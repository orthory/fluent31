//! Edge replica store: a scoped, read-only, ephemeral cache of one remote
//! master instance.
//!
//! An `EdgeStore` holds the slice of a master's flushed tree that overlaps
//! its key scope `[lo, hi)`: fragment files copied locally (index + bloom
//! pinned in memory exactly like a normal open), an in-memory overlay
//! memtable fed by the master's replication stream, and an append-only
//! local cache of fetched vlog records. Values resolve inline → local
//! value cache → the injected [`ValueFetcher`] (reach-back to the master
//! for cold values). Transport lives entirely behind the fetcher and the
//! protocol client (`fluent-replication`); this module never touches a
//! socket.
//!
//! Provenance: an attachment is bound to one master [`StoreIdentity`] for
//! its whole life. Fetched records re-verify CRC + embedded key on every
//! read (local hits included), fragment files re-verify their advertised
//! key bounds at install, and `(file id, offset)` cache keys are unique
//! within one master instance — the protocol client enforces the instance
//! binding per connection, and a changed instance means a fresh `attach`
//! (the directory is wiped; an edge is a cache, not a store of record).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::cache::BlockCache;
use crate::config::IoBackend;
use crate::db::{SliceManifest, StreamEntry};
use crate::error::{corrupt, Error, Result};
use crate::identity::StoreIdentity;
use crate::io::{atomic_write, backend, DbFile, Io};
use crate::iter::{InternalIterator, MergeIterator, MvccForward, MvccReverse};
use crate::memtable::Memtable;
use crate::table::Table;
use crate::types::{
    decode_repr, encode_inline, ikey_seqno, make_ikey, validate_user_key, ReprRef, SeqNo,
    ValueKind, ValuePtr, MAX_SEQNO, USER_KEYSPACE_START,
};
use crate::version::{Run, TableHandle, Version};
use crate::vlog;

/// Fetches raw vlog record bytes (`[crc][klen][vlen][key][value]`) from
/// the master. Implementations own transport and the per-connection
/// instance-id check; the store verifies CRC + key on every returned
/// record before serving or caching it.
pub trait ValueFetcher: Send + Sync {
    fn fetch_record(&self, file: u64, offset: u64, len: u32) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    /// Local cache directory (wiped on attach).
    pub dir: PathBuf,
    /// The master instance this attachment is bound to.
    pub master: StoreIdentity,
    /// Scope `[lo, hi)`; `hi = None` = unbounded above.
    pub scope_lo: Vec<u8>,
    pub scope_hi: Option<Vec<u8>>,
    pub io_backend: IoBackend,
    /// Shared block cache for fragment data blocks.
    pub block_cache_size: usize,
    /// Cap on the local value-cache file; exceeding it resets the cache
    /// (ephemeral data — starting over beats eviction bookkeeping).
    pub value_cache_bytes: u64,
}

impl EdgeConfig {
    pub fn new(
        dir: impl Into<PathBuf>,
        master: StoreIdentity,
        scope_lo: Vec<u8>,
        scope_hi: Option<Vec<u8>>,
    ) -> EdgeConfig {
        EdgeConfig {
            dir: dir.into(),
            master,
            scope_lo,
            scope_hi,
            io_backend: IoBackend::Auto,
            block_cache_size: 32 << 20,
            value_cache_bytes: 256 << 20,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EdgeStats {
    /// Slice watermark: everything at or below is in local fragments.
    pub flushed_seqno: SeqNo,
    /// Highest seqno visible on this edge (slice or stream).
    pub frontier_seqno: SeqNo,
    pub fragments: usize,
    pub overlay_bytes: usize,
    pub value_cache_bytes: u64,
}

struct EdgeState {
    version: Arc<Version>,
    overlay: Arc<Memtable>,
    flushed: SeqNo,
    frontier: SeqNo,
}

/// Local verbatim copies of fetched master vlog records, so every local
/// hit re-verifies CRC + embedded key exactly like a master-side
/// dereference. Keys are master-side `(vlog file id, offset)` — unique
/// within one master instance, which the attachment is bound to.
struct ValueCache {
    io: Arc<dyn Io>,
    path: PathBuf,
    handle: Arc<dyn DbFile>,
    written: u64,
    cap: u64,
    index: HashMap<(u64, u64), (u64, u32)>,
}

impl ValueCache {
    fn create(io: Arc<dyn Io>, path: PathBuf, cap: u64) -> Result<ValueCache> {
        let handle = io.create_new(&path)?;
        Ok(ValueCache {
            io,
            path,
            handle,
            written: 0,
            cap,
            index: HashMap::new(),
        })
    }

    fn read(&self, file: u64, offset: u64) -> Result<Option<Vec<u8>>> {
        let Some(&(local_off, len)) = self.index.get(&(file, offset)) else {
            return Ok(None);
        };
        let mut buf = vec![0u8; len as usize];
        self.handle.read_exact_at(local_off, &mut buf)?;
        Ok(Some(buf))
    }

    fn insert(&mut self, file: u64, offset: u64, record: &[u8]) -> Result<()> {
        if self.written + record.len() as u64 > self.cap {
            self.reset()?;
        }
        let local_off = self.handle.append(record)?;
        self.written = local_off + record.len() as u64;
        self.index.insert((file, offset), (local_off, record.len() as u32));
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.index.clear();
        self.written = 0;
        std::fs::remove_file(&self.path)?;
        self.handle = self.io.create_new(&self.path)?;
        Ok(())
    }
}

/// A scoped, read-only replica cache. See the module docs for the model.
pub struct EdgeStore {
    cfg: EdgeConfig,
    io: Arc<dyn Io>,
    cache: Arc<BlockCache>,
    fetcher: Arc<dyn ValueFetcher>,
    state: RwLock<EdgeState>,
    vcache: Mutex<ValueCache>,
}

impl EdgeStore {
    /// Bind a fresh edge cache to one master instance. The directory is
    /// wiped: an edge is ephemeral, and any previous contents belonged to
    /// an attachment whose stream position is gone anyway.
    pub fn attach(mut cfg: EdgeConfig, fetcher: Arc<dyn ValueFetcher>) -> Result<EdgeStore> {
        if cfg.scope_lo.as_slice() < USER_KEYSPACE_START {
            cfg.scope_lo = USER_KEYSPACE_START.to_vec();
        }
        if let Some(h) = &cfg.scope_hi {
            if h.as_slice() <= cfg.scope_lo.as_slice() {
                return Err(Error::InvalidArgument("empty edge scope [lo, hi)".into()));
            }
        }
        if cfg.dir.exists() {
            std::fs::remove_dir_all(&cfg.dir)?;
        }
        std::fs::create_dir_all(&cfg.dir)?;

        let (io, _) = backend(cfg.io_backend)?;
        // provenance stamp, human-inspectable: what this directory caches
        let meta = format!(
            "master_name={}\nmaster_instance={}\nscope_lo={}\nscope_hi={}\n",
            cfg.master.name,
            cfg.master.instance_hex(),
            hex_bytes(&cfg.scope_lo),
            cfg.scope_hi.as_deref().map_or("-".into(), hex_bytes),
        );
        atomic_write(&cfg.dir.join("EDGE"), meta.as_bytes())?;

        let vcache = ValueCache::create(
            io.clone(),
            cfg.dir.join("edge-values.vcache"),
            cfg.value_cache_bytes,
        )?;
        let cache = Arc::new(BlockCache::new(cfg.block_cache_size));
        Ok(EdgeStore {
            io,
            cache,
            fetcher,
            state: RwLock::new(EdgeState {
                version: Arc::new(Version::empty(1)),
                overlay: Arc::new(Memtable::new(0)),
                flushed: 0,
                frontier: 0,
            }),
            vcache: Mutex::new(vcache),
            cfg,
        })
    }

    pub fn master(&self) -> &StoreIdentity {
        &self.cfg.master
    }

    pub fn scope(&self) -> (&[u8], Option<&[u8]>) {
        (&self.cfg.scope_lo, self.cfg.scope_hi.as_deref())
    }

    /// Where the protocol client must place a fragment's bytes before
    /// [`EdgeStore::install_slice`] references it.
    pub fn fragment_path(&self, id: u64) -> PathBuf {
        self.cfg.dir.join(format!("sst-{id:06}.tbl"))
    }

    /// Fragments already copied by an earlier slice survive refreshes, so
    /// the client only fetches what changed.
    pub fn has_fragment(&self, id: u64) -> bool {
        self.fragment_path(id).exists()
    }

    /// Install a freshly pulled slice: open every referenced fragment
    /// (CRC-verified footer/index/stats; advertised bounds cross-checked),
    /// swap the version, prune overlay entries the slice now covers, and
    /// delete local fragments the slice no longer references.
    pub fn install_slice(&self, slice: &SliceManifest) -> Result<()> {
        let mut levels: Vec<Vec<Run>> = Vec::with_capacity(slice.levels.len());
        let mut referenced: HashSet<u64> = HashSet::new();
        for runs in &slice.levels {
            let mut level = Vec::with_capacity(runs.len());
            for r in runs {
                let mut tables = Vec::with_capacity(r.tables.len());
                for t in &r.tables {
                    let path = self.fragment_path(t.id);
                    if !path.exists() {
                        return Err(Error::InvalidArgument(format!(
                            "fragment {} not fetched before install",
                            t.id
                        )));
                    }
                    let file = self.io.open_read(&path)?;
                    let size = file.len()?;
                    let table = Table::open(file, t.id, self.cache.clone())?;
                    if size != t.size
                        || table.stats.min_ukey() != t.min_ukey.as_slice()
                        || table.stats.max_ukey() != t.max_ukey.as_slice()
                    {
                        return Err(corrupt(format!(
                            "fragment {} does not match its advertised size/bounds",
                            t.id
                        )));
                    }
                    referenced.insert(t.id);
                    tables.push(Arc::new(TableHandle::new(t.id, path, size, table)));
                }
                level.push(Run { id: r.id, tables });
            }
            levels.push(level);
        }
        let mut version = Version::empty(levels.len().max(1));
        version.levels = levels;

        {
            let mut s = self.state.write();
            // entries the slice covers are now served from fragments; keep
            // only the overlay tail past the new watermark
            let overlay = Arc::new(Memtable::new(0));
            let mut it = s.overlay.iter();
            it.seek_to_first()?;
            while it.valid() {
                if ikey_seqno(it.ikey()) > slice.flushed_seqno {
                    overlay.insert(it.ikey().to_vec(), it.value().to_vec());
                }
                it.next()?;
            }
            s.version = Arc::new(version);
            s.overlay = overlay;
            s.flushed = slice.flushed_seqno;
            s.frontier = s.frontier.max(slice.flushed_seqno);
        }

        // superseded local fragments: readers holding the old version keep
        // their open fds; the names can go now
        if let Ok(rd) = std::fs::read_dir(&self.cfg.dir) {
            for entry in rd.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let id = name
                    .strip_prefix("sst-")
                    .and_then(|s| s.strip_suffix(".tbl"))
                    .and_then(|s| s.parse::<u64>().ok());
                if let Some(id) = id {
                    if !referenced.contains(&id) {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply a batch from the master's replication stream. Values arrive
    /// resolved, so overlay entries are always inline.
    pub fn apply_stream(&self, entries: &[StreamEntry]) -> Result<()> {
        let mut s = self.state.write();
        for e in entries {
            if !self.in_scope(&e.key) {
                continue;
            }
            let repr = match (e.kind, &e.value) {
                (ValueKind::Put, Some(v)) => encode_inline(v),
                (ValueKind::Delete, _) => Vec::new(),
                (ValueKind::Put, None) => {
                    return Err(corrupt("streamed Put without a resolved value"))
                }
            };
            s.overlay.insert(make_ikey(&e.key, e.seqno, e.kind), repr);
            s.frontier = s.frontier.max(e.seqno);
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_user_key(key)?;
        if !self.in_scope(key) {
            return Err(Error::InvalidArgument(
                "key outside this replica's scope".into(),
            ));
        }
        let (overlay, version) = {
            let s = self.state.read();
            (s.overlay.clone(), s.version.clone())
        };
        let hit = match overlay.get(key, MAX_SEQNO) {
            Some(h) => Some(h),
            None => version.get(key, MAX_SEQNO)?,
        };
        match hit {
            None | Some((ValueKind::Delete, ..)) => Ok(None),
            Some((ValueKind::Put, _, repr)) => Ok(Some(self.resolve(key, &repr)?)),
        }
    }

    /// Paged scan clamped into the scope: up to `limit` visible pairs and
    /// a has-more flag. Bounds outside the scope are narrowed, so an
    /// out-of-scope range yields an empty page (the caller knows the scope
    /// via [`EdgeStore::scope`]).
    pub fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
        let lo: Vec<u8> = match lo {
            Some(l) if l > self.cfg.scope_lo.as_slice() => l.to_vec(),
            _ => self.cfg.scope_lo.clone(),
        };
        let hi: Option<Vec<u8>> = match (hi, &self.cfg.scope_hi) {
            (Some(h), Some(sh)) => Some(h.min(sh.as_slice()).to_vec()),
            (Some(h), None) => Some(h.to_vec()),
            (None, sh) => sh.clone(),
        };
        if hi.as_ref().is_some_and(|h| h.as_slice() <= lo.as_slice()) {
            return Ok((Vec::new(), false));
        }

        let (overlay, version) = {
            let s = self.state.read();
            (s.overlay.clone(), s.version.clone())
        };
        let mut children: Vec<Box<dyn InternalIterator>> =
            vec![Box::new(overlay.iter()) as Box<dyn InternalIterator>];
        for run in version.runs_newest_first() {
            children.push(Box::new(run.iter()));
        }
        let merge = MergeIterator::new(children, reverse);

        let mut out = Vec::new();
        let mut has_more = false;
        let mut push = |this: &Self, key: Vec<u8>, repr: Vec<u8>| -> Result<bool> {
            if out.len() == limit {
                has_more = true;
                return Ok(true);
            }
            let value = this.resolve(&key, &repr)?;
            out.push((key, value));
            Ok(false)
        };
        if reverse {
            let mut it = MvccReverse::new(merge, MAX_SEQNO, lo, hi.as_deref())?;
            while let Some((key, repr)) = it.next_visible()? {
                if push(self, key, repr)? {
                    break;
                }
            }
        } else {
            let mut it = MvccForward::new(merge, MAX_SEQNO, &lo, hi)?;
            while let Some((key, repr)) = it.next_visible()? {
                if push(self, key, repr)? {
                    break;
                }
            }
        }
        Ok((out, has_more))
    }

    pub fn stats(&self) -> EdgeStats {
        let s = self.state.read();
        EdgeStats {
            flushed_seqno: s.flushed,
            frontier_seqno: s.frontier,
            fragments: s
                .version
                .runs_newest_first()
                .map(|r| r.tables.len())
                .sum(),
            overlay_bytes: s.overlay.approximate_bytes(),
            value_cache_bytes: self.vcache.lock().written,
        }
    }

    fn in_scope(&self, key: &[u8]) -> bool {
        key >= self.cfg.scope_lo.as_slice()
            && self
                .cfg
                .scope_hi
                .as_deref()
                .is_none_or(|h| key < h)
    }

    fn resolve(&self, key: &[u8], repr: &[u8]) -> Result<Vec<u8>> {
        match decode_repr(repr)? {
            ReprRef::Inline(v) => Ok(v.to_vec()),
            ReprRef::Ptr(p) => self.resolve_ptr(key, p),
        }
    }

    /// Local value cache, then reach-back. The record is CRC + key
    /// verified BEFORE it is cached or served, so a bad copy can never
    /// take up residence.
    fn resolve_ptr(&self, key: &[u8], p: ValuePtr) -> Result<Vec<u8>> {
        if let Some(record) = self.vcache.lock().read(p.file, p.offset)? {
            return vlog::record_value_for(&record, key);
        }
        let record = self.fetcher.fetch_record(p.file, p.offset, p.len)?;
        if record.len() != p.len as usize {
            return Err(corrupt(format!(
                "master returned {} bytes for a {}-byte record",
                record.len(),
                p.len
            )));
        }
        let value = vlog::record_value_for(&record, key)?;
        self.vcache.lock().insert(p.file, p.offset, &record)?;
        Ok(value)
    }
}

fn hex_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
