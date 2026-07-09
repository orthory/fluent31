//! Instance registry: routes instance ids to live [`SchemaManager`]s.
//!
//! The filesystem is the source of truth — an instance id is minted at
//! fork creation and lives in the fork's `fork.meta`, so the registry
//! rebuilds from an `archive/` walk at any time; nothing here persists.
//! Resolution never joins the id into a path (it scans metadata and
//! compares), so ids cannot traverse the filesystem.
//!
//! Instances open lazily on first resolve and close on idle-eviction, on
//! the LRU cap, or when their fork is deleted. Each open instance is a
//! full engine (memtable, cache, background threads) — the cap is what
//! keeps "cheap forks" cheap for the server too. All open instances share
//! the primary's [`EnginePermits`] so the count of instances cannot
//! multiply the blocking-pool worst case.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fluent31::{Db, Options};

use crate::SchemaManager;

/// Forks of forks nest their archives; a walk this deep is already a
/// pathological tree, and the cap keeps a corrupted/adversarial layout
/// from turning resolution into an unbounded crawl.
const MAX_FORK_DEPTH: usize = 8;

#[derive(Clone)]
pub struct RegistryConfig {
    /// Open fork instances beyond the primary; LRU-evicted past this.
    pub max_open: usize,
    /// Idle instances are closed by [`InstanceRegistry::evict_idle`].
    pub idle_ttl: Duration,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        RegistryConfig {
            max_open: 8,
            idle_ttl: Duration::from_secs(300),
        }
    }
}

pub enum ResolveError {
    /// No fork under this tree carries the id (or it vanished mid-open).
    UnknownInstance,
    Engine(fluent31::Error),
}

struct Entry {
    mgr: Arc<SchemaManager>,
    last_used: Instant,
}

pub(crate) struct RegistryShared {
    root_dir: PathBuf,
    opts: Options,
    primary: Arc<SchemaManager>,
    /// Guards the map only — never held across engine calls or awaits.
    open: Mutex<HashMap<String, Entry>>,
    cfg: RegistryConfig,
}

impl RegistryShared {
    /// Drop the open entry whose database lives at `path` (idempotent).
    /// Used before fork deletion: the flock check in `delete_fork` would
    /// otherwise refuse because we ourselves hold the fork open. In-flight
    /// requests keep their `Arc<SchemaManager>` alive — if any exist, the
    /// engine's own lock still makes the delete fail cleanly.
    pub(crate) fn close_by_path(&self, path: &Path) {
        self.open
            .lock()
            .unwrap()
            .retain(|_, e| e.mgr.db.path() != path);
    }
}

/// One registry per served tree: the primary database plus every fork
/// (recursively) reachable under its `archive/`.
pub struct InstanceRegistry {
    shared: Arc<RegistryShared>,
    /// Serializes slow-path opens: a concurrent resolve of the same id
    /// must not race two `Db::open`s onto one directory (the second would
    /// fail on the flock and surface a spurious error).
    open_slow: tokio::sync::Mutex<()>,
}

impl InstanceRegistry {
    pub fn new(
        primary: Arc<SchemaManager>,
        root_dir: impl Into<PathBuf>,
        opts: Options,
        cfg: RegistryConfig,
    ) -> Arc<InstanceRegistry> {
        let shared = Arc::new(RegistryShared {
            root_dir: root_dir.into(),
            opts,
            primary,
            open: Mutex::new(HashMap::new()),
            cfg,
        });
        shared.primary.attach_registry(Arc::downgrade(&shared));
        Arc::new(InstanceRegistry {
            shared,
            open_slow: tokio::sync::Mutex::new(()),
        })
    }

    pub fn primary(&self) -> Arc<SchemaManager> {
        self.shared.primary.clone()
    }

    /// Resolve an instance id to its manager, opening the fork if needed.
    pub async fn resolve(&self, id: &str) -> Result<Arc<SchemaManager>, ResolveError> {
        if !is_valid_id(id) {
            return Err(ResolveError::UnknownInstance);
        }
        if let Some(mgr) = self.hit(id) {
            return Ok(mgr);
        }
        let _g = self.open_slow.lock().await;
        if let Some(mgr) = self.hit(id) {
            return Ok(mgr); // raced: another resolve opened it first
        }

        let shared = self.shared.clone();
        let id_owned = id.to_string();
        let mgr = tokio::task::spawn_blocking(move || {
            let path = find_fork_dir(&shared.root_dir, &id_owned)
                .map_err(ResolveError::Engine)?
                .ok_or(ResolveError::UnknownInstance)?;
            let db = Db::open(&path, shared.opts.clone()).map_err(ResolveError::Engine)?;
            SchemaManager::with_permits(Arc::new(db), shared.primary.permit_handles())
                .map_err(ResolveError::Engine)
        })
        .await
        .map_err(|e| ResolveError::Engine(fluent31::Error::Io(std::io::Error::other(e))))??;

        mgr.attach_registry(Arc::downgrade(&self.shared));
        let mut open = self.shared.open.lock().unwrap();
        open.insert(
            id.to_string(),
            Entry {
                mgr: mgr.clone(),
                last_used: Instant::now(),
            },
        );
        // LRU past the cap; the entry just inserted is the most recent
        while open.len() > self.shared.cfg.max_open {
            let Some(oldest) = open
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            open.remove(&oldest);
        }
        Ok(mgr)
    }

    fn hit(&self, id: &str) -> Option<Arc<SchemaManager>> {
        let mut open = self.shared.open.lock().unwrap();
        open.get_mut(id).map(|e| {
            e.last_used = Instant::now();
            e.mgr.clone()
        })
    }

    /// Close instances idle longer than the configured TTL. The server
    /// calls this on a timer; requests in flight hold their own Arcs, so
    /// closing here never severs an executing operation.
    pub fn evict_idle(&self) {
        let ttl = self.shared.cfg.idle_ttl;
        self.shared
            .open
            .lock()
            .unwrap()
            .retain(|_, e| e.last_used.elapsed() < ttl);
    }

    /// Open fork instances (diagnostics/tests).
    pub fn open_count(&self) -> usize {
        self.shared.open.lock().unwrap().len()
    }
}

/// Same character set as fork names/ids; anything else can't exist, so
/// reject before scanning.
fn is_valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// Breadth-first walk of the fork tree under `root`, comparing recorded
/// instance ids. The id is never used as a path component.
fn find_fork_dir(root: &Path, id: &str) -> fluent31::Result<Option<PathBuf>> {
    let mut frontier = vec![root.to_path_buf()];
    for _ in 0..MAX_FORK_DEPTH {
        let mut next = Vec::new();
        for dir in frontier.drain(..) {
            for info in fluent31::list_forks_at(&dir)? {
                if info.instance_id == id {
                    return Ok(Some(info.path));
                }
                next.push(info.path);
            }
        }
        if next.is_empty() {
            return Ok(None);
        }
        frontier = next;
    }
    Ok(None)
}
