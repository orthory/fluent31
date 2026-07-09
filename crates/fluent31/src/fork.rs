//! Named database forks — MVCC-pinned snapshots materialized as hard
//! links. Not PITR: there is no log archiving and no
//! restore-to-arbitrary-time; a fork captures an explicit named cut.
//! Creation copies almost nothing and leaves live readers and writers
//! essentially undisturbed.
//!
//! A fork is a complete database directory under `archive/<name>/`:
//! hard links of the (immutable) tables and sealed vlog files, a bounded
//! copy of the vlog head, and a fresh manifest. Creation is crash-atomic:
//! everything is built in `archive/.tmp-<name>/`, fsynced, then renamed into
//! place. A fork activates with just `Db::open` on its path — writable,
//! copy-on-write with respect to the parent.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::DbPaths;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::identity::{validate_store_name, PendingFork, StoreIdentity};
use crate::io::{self, copy_prefix, hard_link_or_copy};
use crate::manifest::{self, ManifestData, RunMeta};

#[derive(Debug, Clone)]
pub struct ForkInfo {
    pub name: String,
    /// Stable routing handle minted at creation — servers use it to address
    /// the fork as a database instance. An address, not a credential. For a
    /// named lineage this equals the deterministic store identity the fork
    /// mints on first read-write open (`identity::derive_fork`), so routing
    /// and replication share one id; unnamed lineages get a random id so
    /// stale handles can't alias across delete/recreate cycles.
    pub instance_id: String,
    pub created_unix_ms: u64,
    /// Every write with seqno <= this is contained in the archive.
    pub last_seqno: u64,
    pub path: PathBuf,
}

/// Routing id for forks of UNNAMED stores (no lineage to derive from):
/// 128 hex bits from two independently-seeded `RandomState` hashers —
/// process-level OS entropy without adding a rand dependency.
fn mint_instance_id(name: &str, created_unix_ms: u64, seq: u64) -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    let mut out = String::with_capacity(32);
    for salt in 0..2u8 {
        let h = RandomState::new().hash_one((salt, name, created_unix_ms, seq));
        out.push_str(&format!("{h:016x}"));
    }
    out
}

fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
        && !name.starts_with('.');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid fork name {name:?} (use [A-Za-z0-9._-], max 64 chars, no leading dot)"
        )))
    }
}

/// Snapshot registration guard for the fork cut.
struct CutPin {
    db: Arc<crate::db::DbInner>,
    seq: u64,
}

impl Drop for CutPin {
    fn drop(&mut self) {
        self.db.deregister_snapshot(self.seq);
    }
}

/// Removes the in-progress build dir if creation errors (or panics) before
/// the rename publishes it — a failed build must not poison its fork name
/// until the next `Db::open` sweep. Armed only by the caller that created
/// the dir, so a losing concurrent creator can never delete the winner's
/// in-progress build.
struct TmpDirGuard(Option<PathBuf>);

impl TmpDirGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for TmpDirGuard {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_dir_all(&p);
        }
    }
}

pub(crate) fn create(db: &Db, name: &str) -> Result<ForkInfo> {
    validate_name(name)?;
    let inner = &db.inner;
    let final_dir = inner.paths.archive(name);
    if final_dir.exists() {
        return Err(Error::InvalidArgument(format!(
            "fork {name:?} already exists"
        )));
    }

    // 1. everything into tables: freeze the memtable, drain the queue
    db.flush()?;

    // 2. pin an atomic cut. The manifest lock freezes structure (flush &
    //    compaction installs); the registered snapshot at `cut` blocks vlog
    //    GC victims from physical deletion while we link.
    let (mdata, view, cut) = {
        let m = inner.manifest.lock();
        let view = inner.read_view();
        (m.data.clone(), view, m.data.last_flushed_seqno)
    };
    // register at the cut (not at visible): archives contain exactly the
    // flushed history, and the registration blocks vlog victim deletion
    inner.register_snapshot_at(cut);
    let _pin = CutPin {
        db: inner.clone(),
        seq: cut,
    };

    // 3. build the archive in a temp dir. create_dir doubles as the mutual
    //    exclusion for concurrent same-name forks: the second caller
    //    fails here instead of clobbering the first one's in-progress build.
    //    (Stale .tmp-* dirs from crashes are swept at Db::open.)
    let root = inner.paths.archive_root();
    std::fs::create_dir_all(&root)?;
    let tmp_dir = root.join(format!(".tmp-{name}"));
    std::fs::create_dir(&tmp_dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidArgument(format!(
                "fork {name:?} is already being created"
            ))
        } else {
            Error::Io(e)
        }
    })?;
    let mut tmp_guard = TmpDirGuard(Some(tmp_dir.clone()));

    // tables (paths stay live: the pinned view holds their Arc handles and
    // unlink only ever happens in handle Drop)
    for run in view.version.runs_newest_first() {
        for t in &run.tables {
            let file_name = t.path.file_name().expect("table file name");
            hard_link_or_copy(&t.path, &tmp_dir.join(file_name))?;
        }
    }

    // vlog files: link sealed ones; bounded-copy the head
    inner.vlog.sync_head()?;
    let (cur_head_id, cur_head_written, _) = inner.vlog.head_state();
    for (id, h) in &view.version.vlogs {
        let file_name = h.path.file_name().expect("vlog file name");
        if *id == view.version.vlog_head_id {
            // if the head rotated since the view was taken it is sealed and
            // fully durable; otherwise copy the synced prefix we captured
            let len = if *id == cur_head_id {
                cur_head_written
            } else {
                h.file.len()?
            };
            copy_prefix(&h.path, &tmp_dir.join(file_name), len)?;
        } else {
            hard_link_or_copy(&h.path, &tmp_dir.join(file_name))?;
        }
    }

    // archive manifest: structure from the pinned view
    let levels: Vec<Vec<RunMeta>> = view
        .version
        .levels
        .iter()
        .map(|runs| {
            runs.iter()
                .map(|r| RunMeta {
                    id: r.id,
                    table_ids: r.tables.iter().map(|t| t.id).collect(),
                })
                .collect()
        })
        .collect();
    let vlog_live: Vec<u64> = view.version.vlogs.keys().copied().collect();
    let adata = ManifestData {
        next_file_id: mdata.next_file_id,
        last_flushed_seqno: cut,
        // no WALs in an archive: nothing below next_file_id can appear
        wal_floor: mdata.next_file_id,
        levels,
        vlog_live: vlog_live.clone(),
        vlog_head: view.version.vlog_head_id,
        vlog_retired: Vec::new(),
        discard: mdata
            .discard
            .iter()
            .filter(|(id, _)| vlog_live.contains(id))
            .copied()
            .collect(),
        // an archive is not a store instance until opened read-write: it
        // carries no identity of its own, only the fork it would mint (the
        // archive name becomes the child's store name). Unnamed source
        // stores fork namelessly — their archives stay unnamed too.
        identity: None,
        pending_fork: mdata.identity.as_ref().map(|id| PendingFork {
            parent_instance_id: id.instance_id,
            cut_seqno: cut,
            name: name.to_string(),
        }),
    };
    let tmp_paths = DbPaths::new(&tmp_dir);
    manifest::save(&tmp_paths, 1, &adata)?;

    let created_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // routing id: for a named lineage this IS the store identity the fork
    // will mint on first read-write open (derive_fork is pure), so routing
    // and replication agree on one id. Unnamed lineages mint nothing —
    // fall back to a random id so instance routing still works there.
    let instance_id = adata
        .pending_fork
        .as_ref()
        .map(|pf| crate::identity::hex(&pf.mint().instance_id))
        .unwrap_or_else(|| mint_instance_id(name, created_unix_ms, cut));
    let meta = format!(
        "name={name}\ninstance_id={instance_id}\ncreated_unix_ms={created_unix_ms}\nlast_seqno={cut}\n"
    );
    io::atomic_write(&tmp_dir.join("fork.meta"), meta.as_bytes())?;

    // 4. publish atomically. Once the rename lands the fork is complete and
    //    valid; the guard must not touch it even if the root fsync fails.
    io::sync_dir(&tmp_dir)?;
    std::fs::rename(&tmp_dir, &final_dir)?;
    tmp_guard.disarm();
    io::sync_dir(&root)?;

    Ok(ForkInfo {
        name: name.to_string(),
        instance_id,
        created_unix_ms,
        last_seqno: cut,
        path: final_dir,
    })
}

/// List the forks recorded under a database directory by reading
/// `archive/*/fork.meta` — no lock taken, works on databases another
/// process has open. Point-in-time: racing creates/deletes may appear or
/// not.
pub fn list_at(db_dir: &Path) -> Result<Vec<ForkInfo>> {
    list(&DbPaths::new(db_dir))
}

pub(crate) fn list(paths: &DbPaths) -> Result<Vec<ForkInfo>> {
    let root = paths.archive_root();
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(&root) else {
        return Ok(out);
    };
    for entry in rd.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') {
            continue; // .tmp-* build dirs
        }
        let dir = entry.path();
        let Ok(meta) = std::fs::read_to_string(dir.join("fork.meta")) else {
            continue;
        };
        let field = |k: &str| -> Option<String> {
            meta.lines()
                .find_map(|l| l.strip_prefix(&format!("{k}=")).map(|v| v.to_string()))
        };
        out.push(ForkInfo {
            name: name.to_string(),
            // pre-instance-id forks stay addressable by name
            instance_id: field("instance_id").unwrap_or_else(|| name.to_string()),
            created_unix_ms: field("created_unix_ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            last_seqno: field("last_seqno").and_then(|v| v.parse().ok()).unwrap_or(0),
            path: dir,
        });
    }
    out.sort_by_key(|c| c.created_unix_ms);
    Ok(out)
}

pub(crate) fn delete(paths: &DbPaths, name: &str) -> Result<()> {
    validate_name(name)?;
    let dir = paths.archive(name);
    if !dir.exists() {
        return Err(Error::InvalidArgument(format!(
            "no fork named {name:?}"
        )));
    }
    // refuse to delete a fork that is open as a live database
    let lock_path = dir.join("LOCK");
    if lock_path.exists() {
        let f = std::fs::OpenOptions::new().write(true).open(&lock_path)?;
        if f.try_lock().is_err() {
            return Err(Error::InvalidArgument(format!(
                "fork {name:?} is currently open as a database"
            )));
        }
    }
    std::fs::remove_dir_all(&dir)?;
    io::sync_dir(&paths.archive_root())?;
    Ok(())
}

/// Re-link an archive into a fresh standalone directory (promote a fork
/// without mutating the archive).
///
/// `new_name` names the fork the copy will mint on first open. It is
/// REQUIRED when the archive descends from a named store: each restored
/// copy must fork under its own name, or two copies of one archive would
/// mint identical instance ids for histories about to diverge. For an
/// unnamed lineage, `Some(name)` adopts a root identity onto the copy
/// (same as opening it with `Options::store_name`).
pub fn restore_to(archive: &Path, dest: &Path, new_name: Option<&str>) -> Result<()> {
    if dest.exists() {
        return Err(Error::InvalidArgument(format!(
            "destination {} already exists",
            dest.display()
        )));
    }
    if let Some(name) = new_name {
        validate_store_name(name)?;
    }

    // rewrite identity state BEFORE copying: the destination gets a fresh
    // gen-1 manifest instead of a verbatim link of the archive's
    let archive_paths = DbPaths::new(archive);
    let (_, mut mdata) = manifest::load(&archive_paths)?;
    // a minted identity means the archive was already opened read-write —
    // it is a live store now, and copying it would duplicate its instance
    // id across histories about to diverge
    if let Some(id) = &mdata.identity {
        return Err(Error::InvalidArgument(format!(
            "source is a live store (instance {:?} already minted), not a \
             pristine archive; create a fork from it and restore that",
            id.name
        )));
    }
    match (&mut mdata.pending_fork, new_name) {
        (Some(pf), Some(name)) => pf.name = name.to_string(),
        (Some(pf), None) => {
            return Err(Error::InvalidArgument(format!(
                "archive descends from a named store (fork name {:?}); \
                 restore_to requires a new_name so each copy forks uniquely",
                pf.name
            )));
        }
        (None, Some(name)) => mdata.identity = Some(StoreIdentity::root(name)),
        (None, None) => {}
    }

    std::fs::create_dir_all(dest)?;
    for entry in std::fs::read_dir(archive)?.flatten() {
        let name = entry.file_name();
        let is_manifest = name
            .to_str()
            .is_some_and(|n| n == "CURRENT" || n.starts_with("MANIFEST-"));
        if name == "LOCK" || is_manifest {
            continue;
        }
        hard_link_or_copy(&entry.path(), &dest.join(&name))?;
    }
    manifest::save(&DbPaths::new(dest), 1, &mdata)?;
    io::sync_dir(dest)?;
    Ok(())
}
