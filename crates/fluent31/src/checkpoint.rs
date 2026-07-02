//! Manual point-in-time checkpoints (PITR).
//!
//! A checkpoint is a complete database directory under `archive/<name>/`:
//! hard links of the (immutable) tables and sealed vlog files, a bounded
//! copy of the vlog head, and a fresh manifest. Creation is crash-atomic:
//! everything is built in `archive/.tmp-<name>/`, fsynced, then renamed into
//! place. Restore is just `Db::open` on the archive path — opening it
//! read-write forks the database copy-on-write.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::DbPaths;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::io::{self, copy_prefix, hard_link_or_copy};
use crate::manifest::{self, ManifestData, RunMeta};

#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    pub name: String,
    pub created_unix_ms: u64,
    /// Every write with seqno <= this is contained in the archive.
    pub last_seqno: u64,
    pub path: PathBuf,
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
            "invalid checkpoint name {name:?} (use [A-Za-z0-9._-], max 64 chars, no leading dot)"
        )))
    }
}

/// Snapshot registration guard for the checkpoint cut.
struct CutPin {
    db: Arc<crate::db::DbInner>,
    seq: u64,
}

impl Drop for CutPin {
    fn drop(&mut self) {
        self.db.deregister_snapshot(self.seq);
    }
}

pub(crate) fn create(db: &Db, name: &str) -> Result<CheckpointInfo> {
    validate_name(name)?;
    let inner = &db.inner;
    let final_dir = inner.paths.archive(name);
    if final_dir.exists() {
        return Err(Error::InvalidArgument(format!(
            "checkpoint {name:?} already exists"
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
    //    exclusion for concurrent same-name checkpoints: the second caller
    //    fails here instead of clobbering the first one's in-progress build.
    //    (Stale .tmp-* dirs from crashes are swept at Db::open.)
    let root = inner.paths.archive_root();
    std::fs::create_dir_all(&root)?;
    let tmp_dir = root.join(format!(".tmp-{name}"));
    std::fs::create_dir(&tmp_dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidArgument(format!(
                "checkpoint {name:?} is already being created"
            ))
        } else {
            Error::Io(e)
        }
    })?;

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
    };
    let tmp_paths = DbPaths::new(&tmp_dir);
    manifest::save(&tmp_paths, 1, &adata)?;

    let created_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let meta = format!("name={name}\ncreated_unix_ms={created_unix_ms}\nlast_seqno={cut}\n");
    io::atomic_write(&tmp_dir.join("checkpoint.meta"), meta.as_bytes())?;

    // 4. publish atomically
    io::sync_dir(&tmp_dir)?;
    std::fs::rename(&tmp_dir, &final_dir)?;
    io::sync_dir(&root)?;

    Ok(CheckpointInfo {
        name: name.to_string(),
        created_unix_ms,
        last_seqno: cut,
        path: final_dir,
    })
}

pub(crate) fn list(paths: &DbPaths) -> Result<Vec<CheckpointInfo>> {
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
        let Ok(meta) = std::fs::read_to_string(dir.join("checkpoint.meta")) else {
            continue;
        };
        let field = |k: &str| -> Option<String> {
            meta.lines()
                .find_map(|l| l.strip_prefix(&format!("{k}=")).map(|v| v.to_string()))
        };
        out.push(CheckpointInfo {
            name: name.to_string(),
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
            "no checkpoint named {name:?}"
        )));
    }
    // refuse to delete a checkpoint that is open as a live database
    let lock_path = dir.join("LOCK");
    if lock_path.exists() {
        let f = std::fs::OpenOptions::new().write(true).open(&lock_path)?;
        if f.try_lock().is_err() {
            return Err(Error::InvalidArgument(format!(
                "checkpoint {name:?} is currently open as a database"
            )));
        }
    }
    std::fs::remove_dir_all(&dir)?;
    io::sync_dir(&paths.archive_root())?;
    Ok(())
}

/// Re-link an archive into a fresh standalone directory (promote a
/// checkpoint without mutating the archive).
pub fn restore_to(archive: &Path, dest: &Path) -> Result<()> {
    if dest.exists() {
        return Err(Error::InvalidArgument(format!(
            "destination {} already exists",
            dest.display()
        )));
    }
    std::fs::create_dir_all(dest)?;
    for entry in std::fs::read_dir(archive)?.flatten() {
        let name = entry.file_name();
        if name == "LOCK" {
            continue;
        }
        hard_link_or_copy(&entry.path(), &dest.join(&name))?;
    }
    io::sync_dir(dest)?;
    Ok(())
}
