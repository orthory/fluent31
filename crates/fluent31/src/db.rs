//! The engine core: open/recovery, the write path, read views, snapshots,
//! flushes, write stalls, and background thread lifecycle.
//!
//! Locking (fixed global order — always acquire left before right):
//! `write_mu` → `manifest` → `state` → `snapshots`
//! Not every path takes every lock; no path acquires them out of order.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use crate::batch::{decode_batch, encode_batch, BatchOp, EncEntry, WriteBatch};
use crate::cache::BlockCache;
use crate::checkpoint::CheckpointInfo;
use crate::config::{DbPaths, Options, SyncMode};
use crate::error::{corrupt, Error, Result};
use crate::io::{self, Io};
use crate::iter::DbIterator;
use crate::manifest::{self, ManifestData, RunMeta};
use crate::memtable::Memtable;
use crate::table::{Table, TableBuilder};
use crate::txn::Txn;
use crate::types::{
    decode_repr, encode_inline, encode_ptr, make_ikey, validate_user_key, ReprRef, SeqNo,
    ValueKind, MAX_SEQNO, USER_KEYSPACE_START,
};
use crate::version::{ReadView, Run, TableHandle, Version};
use crate::vlog::{self, Vlog, VlogFileHandle};
use crate::wal::{read_wal, WalTail, WalWriter};
#[cfg(feature = "wasm")]
use crate::wasm::WasmRuntime;

pub(crate) struct DbState {
    pub mem: Arc<Memtable>,
    /// Frozen memtables, newest first; flush consumes from the back.
    pub imms: Vec<Arc<Memtable>>,
    pub version: Arc<Version>,
}

pub(crate) struct WriteState {
    pub wal: WalWriter,
}

pub(crate) struct ManifestState {
    pub gen: u64,
    pub data: ManifestData,
}

#[derive(Default)]
pub(crate) struct SnapshotList {
    counts: BTreeMap<SeqNo, usize>,
}

impl SnapshotList {
    fn register(&mut self, seq: SeqNo) {
        *self.counts.entry(seq).or_insert(0) += 1;
    }
    fn deregister(&mut self, seq: SeqNo) {
        if let Some(c) = self.counts.get_mut(&seq) {
            *c -= 1;
            if *c == 0 {
                self.counts.remove(&seq);
            }
        }
    }
    fn min(&self) -> Option<SeqNo> {
        self.counts.keys().next().copied()
    }
}

pub(crate) struct Signal {
    mu: Mutex<()>,
    cv: Condvar,
}

impl Signal {
    fn new() -> Self {
        Signal {
            mu: Mutex::new(()),
            cv: Condvar::new(),
        }
    }
    pub fn notify(&self) {
        let _g = self.mu.lock();
        self.cv.notify_all();
    }
    pub fn wait_timeout(&self, d: Duration) {
        let mut g = self.mu.lock();
        self.cv.wait_for(&mut g, d);
    }
}

/// A retired vlog GC victim awaiting its deletion gates.
pub(crate) struct RetiredVlog {
    pub id: u64,
    pub retired_at: SeqNo,
    pub handle: Arc<VlogFileHandle>,
}

pub(crate) struct DbInner {
    pub opts: Options,
    pub paths: DbPaths,
    pub io: Arc<dyn Io>,
    pub backend_name: &'static str,
    pub cache: Arc<BlockCache>,

    pub state: RwLock<DbState>,
    pub write_mu: Mutex<WriteState>,
    pub visible_seqno: AtomicU64,
    pub next_file_id: AtomicU64,
    pub manifest: Mutex<ManifestState>,
    pub snapshots: Mutex<SnapshotList>,
    pub vlog: Vlog,
    /// Retired GC victims awaiting their deletion gates.
    pub retired: Mutex<Vec<RetiredVlog>>,
    /// Serializes vlog GC passes (manual + automatic).
    pub gc_mu: Mutex<()>,
    /// Serializes compaction jobs: the maintenance thread and user-invoked
    /// `compact_all` must never pick/merge concurrently (both would grab the
    /// same input runs).
    pub compaction_mu: Mutex<()>,

    pub shutdown: AtomicBool,
    pub flush_signal: Signal,
    pub compact_signal: Signal,
    /// Signaled on flush/compaction progress (stall + flush waiters).
    pub progress_signal: Signal,
    pub bg_error: Mutex<Option<String>>,

    #[cfg(feature = "wasm")]
    pub wasm: WasmRuntime,

    /// Held for the process lifetime to exclude concurrent opens.
    _lock_file: std::fs::File,
}

impl DbInner {
    // ---------------------------------------------------------------- reads

    /// Assemble a consistent read view. Ordering is load-bearing twice over:
    /// the Arcs are cloned before the seqno loads (a pinned older structure
    /// still contains whatever GC dropped afterwards), and the seqno load
    /// happens *inside* the state read lock so no structural change that the
    /// seqno could reference — e.g. a vlog head rotation followed by a write
    /// into the new head — can slip in between (rotations take the state
    /// write lock).
    pub fn read_view(&self) -> ReadView {
        let s = self.state.read();
        ReadView {
            mem: s.mem.clone(),
            imms: s.imms.clone(),
            version: s.version.clone(),
            visible: self.visible_seqno.load(Ordering::Acquire),
        }
    }

    pub fn resolve_repr(
        &self,
        view: &ReadView,
        key: &[u8],
        repr: &[u8],
    ) -> Result<Vec<u8>> {
        match decode_repr(repr)? {
            ReprRef::Inline(v) => Ok(v.to_vec()),
            ReprRef::Ptr(p) => {
                let handle = view.version.vlogs.get(&p.file).ok_or_else(|| {
                    corrupt(format!("pointer into unknown vlog file {}", p.file))
                })?;
                vlog::read_value(handle, &p, key, Some(&self.cache))
            }
        }
    }

    pub fn get_at_seq(&self, key: &[u8], seq: SeqNo) -> Result<Option<Vec<u8>>> {
        let view = self.read_view();
        let seq = seq.min(view.visible);
        match view.get_versioned(key, seq)? {
            None => Ok(None),
            Some((ValueKind::Delete, _, _)) => Ok(None),
            Some((ValueKind::Put, _, repr)) => Ok(Some(self.resolve_repr(&view, key, &repr)?)),
        }
    }

    /// User-facing iterator: the lower bound is clamped to the user keyspace
    /// so the reserved 0x00 prefix stays invisible.
    pub fn iter_at_seq(
        &self,
        seq: Option<SeqNo>,
        lo: Option<&[u8]>,
        hi: Option<Vec<u8>>,
        reverse: bool,
    ) -> Result<DbIterator> {
        let lo = match lo {
            Some(l) if l >= USER_KEYSPACE_START => l.to_vec(),
            _ => USER_KEYSPACE_START.to_vec(),
        };
        self.iter_raw(seq, &lo, hi, reverse)
    }

    /// Unclamped iterator — internal use only (system keyspace scans).
    pub fn iter_raw(
        &self,
        seq: Option<SeqNo>,
        lo: &[u8],
        hi: Option<Vec<u8>>,
        reverse: bool,
    ) -> Result<DbIterator> {
        let view = self.read_view();
        let seq = seq.unwrap_or(view.visible).min(view.visible);
        DbIterator::new(view, self.cache.clone(), seq, lo, hi, reverse)
    }

    // ------------------------------------------------------------ snapshots

    pub fn register_snapshot(&self) -> SeqNo {
        let mut g = self.snapshots.lock();
        // the seqno load MUST happen inside this critical section so the
        // watermark can never race past a snapshot mid-creation
        let seq = self.visible_seqno.load(Ordering::Acquire);
        g.register(seq);
        seq
    }

    /// Register a snapshot at an explicit (already-visible) seqno — used by
    /// checkpoints to pin their cut.
    pub fn register_snapshot_at(&self, seq: SeqNo) {
        debug_assert!(seq <= self.visible_seqno.load(Ordering::Acquire));
        self.snapshots.lock().register(seq);
    }

    pub fn deregister_snapshot(&self, seq: SeqNo) {
        self.snapshots.lock().deregister(seq);
    }

    /// GC watermark: versions with seqno <= watermark - 1... precisely: the
    /// smallest seqno any current or future reader may use. Loading
    /// `visible` inside the lock pairs with `register_snapshot`.
    pub fn watermark(&self) -> SeqNo {
        let g = self.snapshots.lock();
        match g.min() {
            Some(s) => s,
            None => self.visible_seqno.load(Ordering::Acquire) + 1,
        }
    }

    // ---------------------------------------------------------- write path

    pub fn check_bg_error(&self) -> Result<()> {
        if let Some(msg) = self.bg_error.lock().as_ref() {
            return Err(Error::Background(msg.clone()));
        }
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn validate_batch(&self, batch: &WriteBatch) -> Result<()> {
        for op in &batch.ops {
            let (key, vlen) = match op {
                BatchOp::Put { key, value } => (key, value.len()),
                BatchOp::Delete { key } => (key, 0),
            };
            validate_user_key(key)?;
            if key.len() > self.opts.max_key_size {
                return Err(Error::InvalidArgument(format!(
                    "key of {} bytes exceeds max_key_size",
                    key.len()
                )));
            }
            if vlen > self.opts.max_value_size {
                return Err(Error::InvalidArgument(format!(
                    "value of {vlen} bytes exceeds max_value_size"
                )));
            }
        }
        Ok(())
    }

    fn stalled(&self) -> bool {
        let s = self.state.read();
        s.imms.len() >= self.opts.max_immutable_memtables
            || s.version.levels[0].len() >= self.opts.l0_stall_trigger
    }

    pub(crate) fn wait_for_space(&self) -> Result<()> {
        while self.stalled() {
            self.check_bg_error()?;
            self.flush_signal.notify();
            self.compact_signal.notify();
            self.progress_signal.wait_timeout(Duration::from_millis(100));
        }
        Ok(())
    }

    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        self.validate_batch(batch)?;
        self.write_batch_unchecked(batch)
    }

    /// Write path without user-key validation (system keys, GC rewrites,
    /// transaction commits that already validated).
    pub fn write_batch_unchecked(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.check_bg_error()?;
        self.wait_for_space()?;
        let mut ws = self.write_mu.lock();
        self.apply_locked(&mut ws, &batch.ops)?;
        Ok(())
    }

    /// Core of the write path; caller holds `write_mu`.
    pub(crate) fn apply_locked(
        &self,
        ws: &mut WriteState,
        ops: &[BatchOp],
    ) -> Result<()> {
        let base = self.visible_seqno.load(Ordering::Acquire) + 1;
        if base + ops.len() as u64 >= MAX_SEQNO {
            return Err(Error::InvalidArgument("seqno space exhausted".into()));
        }

        // size-check BEFORE vlog placement: rejecting afterwards would
        // orphan already-appended vlog records that no discard accounting
        // ever reclaims (pointer reprs only shrink the encoded batch, so
        // this bound is conservative)
        let approx: u64 = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => (key.len() + value.len() + 32) as u64,
                BatchOp::Delete { key } => (key.len() + 16) as u64,
            })
            .sum();
        if approx >= 1 << 30 {
            return Err(Error::InvalidArgument(
                "write batch exceeds WAL record limit".into(),
            ));
        }

        // 1. place large values in the vlog
        let mut entries = Vec::with_capacity(ops.len());
        let mut any_vlog = false;
        for op in ops {
            let e = match op {
                BatchOp::Put { key, value } => {
                    let repr = if value.len() >= self.opts.value_threshold {
                        any_vlog = true;
                        encode_ptr(self.vlog.append(key, value)?)
                    } else {
                        encode_inline(value)
                    };
                    EncEntry {
                        kind: ValueKind::Put,
                        key: key.clone(),
                        repr,
                    }
                }
                BatchOp::Delete { key } => EncEntry {
                    kind: ValueKind::Delete,
                    key: key.clone(),
                    repr: Vec::new(),
                },
            };
            entries.push(e);
        }

        // 2. durability ordering: payload before pointer (vlog fsync before
        //    the WAL record referencing it becomes durable)
        if any_vlog && self.opts.sync == SyncMode::Always {
            self.vlog.sync_head()?;
        }
        let payload = encode_batch(base, &entries);
        if payload.len() as u64 >= 1 << 30 {
            return Err(Error::InvalidArgument(
                "write batch exceeds WAL record limit".into(),
            ));
        }
        ws.wal.append_record(&payload)?;
        if self.opts.sync == SyncMode::Always {
            ws.wal.sync()?;
        }

        // 3. memtable inserts, then publish
        let mem = self.state.read().mem.clone();
        for (i, e) in entries.iter().enumerate() {
            mem.insert(
                make_ikey(&e.key, base + i as u64, e.kind),
                e.repr.clone(),
            );
        }
        self.visible_seqno
            .store(base + entries.len() as u64 - 1, Ordering::Release);

        // 4. rotations
        if mem.approximate_bytes() >= self.opts.memtable_size {
            self.rotate_memtable_locked(ws)?;
            self.flush_signal.notify();
        }
        let (_, head_written, _) = self.vlog.head_state();
        if head_written >= self.opts.vlog_file_size {
            self.rotate_vlog_locked()?;
        }
        Ok(())
    }

    pub fn alloc_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Freeze the active memtable and start a fresh WAL. Caller holds
    /// `write_mu`.
    fn rotate_memtable_locked(&self, ws: &mut WriteState) -> Result<()> {
        // Seal the old WAL: recovery treats damage in a non-newest WAL as
        // corruption, which is only sound if sealed WALs are fully durable.
        ws.wal.sync()?;
        let wal_id = self.alloc_file_id();
        let file = self.io.create_new(&self.paths.wal(wal_id))?;
        // the WAL's directory entry must be durable before any write to it
        // is acknowledged
        io::sync_dir(&self.paths.dir)?;
        ws.wal = WalWriter::new(file);

        let mut s = self.state.write();
        let old = std::mem::replace(&mut s.mem, Arc::new(Memtable::new(wal_id)));
        s.imms.insert(0, old);
        Ok(())
    }

    /// Seal the vlog head and open a fresh one, publishing the new handle.
    /// Caller holds `write_mu`. No manifest write: recovery adopts vlog
    /// files with id >= the manifest's head id (young files).
    fn rotate_vlog_locked(&self) -> Result<()> {
        let id = self.alloc_file_id();
        let (_sealed, new_handle) = self.vlog.rotate(self.io.as_ref(), id, self.paths.vlog(id))?;
        io::sync_dir(&self.paths.dir)?;
        let mut s = self.state.write();
        let mut v = s.version.clone_shape();
        v.vlogs.insert(new_handle.id, new_handle.clone());
        v.vlog_head_id = new_handle.id;
        s.version = Arc::new(v);
        Ok(())
    }

    /// Rotate the memtable even below the size threshold (flush(),
    /// checkpoints). No-op when the memtable is empty.
    pub fn force_rotate(&self) -> Result<()> {
        let mut ws = self.write_mu.lock();
        let non_empty = !self.state.read().mem.is_empty();
        if non_empty {
            self.rotate_memtable_locked(&mut ws)?;
            self.flush_signal.notify();
        }
        Ok(())
    }

    /// Block until every frozen memtable has been flushed.
    pub fn wait_flushed(&self) -> Result<()> {
        loop {
            self.check_bg_error()?;
            if self.state.read().imms.is_empty() {
                return Ok(());
            }
            self.flush_signal.notify();
            self.progress_signal.wait_timeout(Duration::from_millis(50));
        }
    }

    // ---------------------------------------------------------------- flush

    /// Flush the oldest immutable memtable into an L0 run. Runs on the flush
    /// thread (and synchronously during recovery).
    pub(crate) fn flush_one(&self) -> Result<bool> {
        let Some(imm) = self.state.read().imms.last().cloned() else {
            return Ok(false);
        };

        let run = if imm.is_empty() {
            None
        } else {
            // pointers written by this memtable must be durable before the
            // manifest references the table containing them
            self.vlog.sync_head()?;
            Some(self.build_run_from_mem(&imm)?)
        };

        {
            let mut m = self.manifest.lock();
            let mut data = m.data.clone();
            if let Some((run_meta, max_seq)) = run.as_ref().map(|(r, ms)| {
                (
                    RunMeta {
                        id: r.id,
                        table_ids: r.tables.iter().map(|t| t.id).collect(),
                    },
                    *ms,
                )
            }) {
                data.levels[0].insert(0, run_meta);
                data.last_flushed_seqno = data.last_flushed_seqno.max(max_seq);
            }
            data.wal_floor = data.wal_floor.max(imm.wal_id + 1);
            data.next_file_id = self.next_file_id.load(Ordering::SeqCst);
            let gen = m.gen + 1;
            manifest::save(&self.paths, gen, &data)?;
            m.gen = gen;
            m.data = data;

            let mut s = self.state.write();
            let mut v = s.version.clone_shape();
            if let Some((r, _)) = run {
                v.levels[0].insert(0, r);
            }
            s.version = Arc::new(v);
            let popped = s.imms.pop();
            debug_assert!(popped.is_some_and(|p| Arc::ptr_eq(&p, &imm)));

            // old WALs are now fully covered by tables
            let floor = m.data.wal_floor;
            drop(s);
            self.delete_old_wals(floor);
        }
        self.progress_signal.notify();
        self.compact_signal.notify();
        Ok(true)
    }

    fn delete_old_wals(&self, floor: u64) {
        if let Ok(rd) = std::fs::read_dir(&self.paths.dir) {
            for entry in rd.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if let Some(id) = parse_file_id(name, "wal-", ".log") {
                    if id < floor {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    /// Write one memtable out as a (possibly multi-fragment) run.
    fn build_run_from_mem(&self, imm: &Arc<Memtable>) -> Result<(Run, SeqNo)> {
        use crate::iter::InternalIterator;
        let run_id = self.alloc_file_id();
        let mut tables = Vec::new();
        let mut max_seq = 0;
        let mut it = imm.iter();
        it.seek_to_first()?;
        let mut builder: Option<(u64, TableBuilder)> = None;
        let mut last_ukey: Vec<u8> = Vec::new();
        while it.valid() {
            let ikey = it.ikey();
            max_seq = max_seq.max(crate::types::ikey_seqno(ikey));
            let ukey_changed = crate::types::ikey_ukey(ikey) != last_ukey.as_slice();
            if ukey_changed {
                last_ukey = crate::types::ikey_ukey(ikey).to_vec();
            }
            if let Some((_, b)) = &builder {
                // fragments split only at user-key boundaries so a key's
                // versions never straddle fragments
                if ukey_changed && b.estimated_size() >= self.opts.target_file_size {
                    let (id, b) = builder.take().unwrap();
                    tables.push(self.finish_table(id, b)?);
                }
            }
            if builder.is_none() {
                let id = self.alloc_file_id();
                let file = self.io.create_new(&self.paths.table(id))?;
                builder = Some((
                    id,
                    TableBuilder::new(file, self.opts.block_size, self.opts.bloom_bits_per_key),
                ));
            }
            builder.as_mut().unwrap().1.add(ikey, it.value())?;
            it.next()?;
        }
        if let Some((id, b)) = builder.take() {
            tables.push(self.finish_table(id, b)?);
        }
        io::sync_dir(&self.paths.dir)?;
        Ok((
            Run {
                id: run_id,
                tables,
            },
            max_seq,
        ))
    }

    pub(crate) fn finish_table(&self, id: u64, b: TableBuilder) -> Result<Arc<TableHandle>> {
        let (_stats, size) = b.finish()?;
        let path = self.paths.table(id);
        let file = self.io.open_read(&path)?;
        let table = Table::open(file, id, self.cache.clone())?;
        Ok(Arc::new(TableHandle::new(id, path, size, table)))
    }
}

fn parse_file_id(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .parse()
        .ok()
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// An open database. Dropping it shuts down background work and joins the
/// maintenance threads.
pub struct Db {
    pub(crate) inner: Arc<DbInner>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

/// A registered consistent point-in-time view. Reads through it see exactly
/// the state as of creation; compaction preserves whatever it can still see.
pub struct Snapshot {
    db: Arc<DbInner>,
    pub(crate) seq: SeqNo,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.db.deregister_snapshot(self.seq);
    }
}

#[derive(Debug, Clone)]
pub struct DbStats {
    pub backend: &'static str,
    pub visible_seqno: u64,
    pub memtable_bytes: usize,
    pub immutable_memtables: usize,
    /// Per level: (runs, fragment files, bytes).
    pub levels: Vec<(usize, usize, u64)>,
    pub vlog_files: usize,
    pub vlog_retired: usize,
    pub discard_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let inner = open_inner(dir.as_ref(), opts)?;
        let mut threads = Vec::new();
        {
            let i = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("fluent31-flush".into())
                    .spawn(move || flush_thread(i))
                    .expect("spawn flush thread"),
            );
        }
        {
            let i = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("fluent31-compact".into())
                    .spawn(move || compact_thread(i))
                    .expect("spawn compaction thread"),
            );
        }
        Ok(Db { inner, threads })
    }

    // ------------------------------------------------------------- KV API

    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(key.into(), value.into());
        self.write(b)
    }

    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.delete(key.into());
        self.write(b)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.inner.write_batch(&batch)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_user_key(key)?;
        self.inner.get_at_seq(key, MAX_SEQNO)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            db: self.inner.clone(),
            seq: self.inner.register_snapshot(),
        }
    }

    pub fn get_at(&self, key: &[u8], snap: &Snapshot) -> Result<Option<Vec<u8>>> {
        validate_user_key(key)?;
        self.inner.get_at_seq(key, snap.seq)
    }

    /// Forward or reverse iterator over `[lo, hi)` at the current visible
    /// state.
    pub fn iter(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
    ) -> Result<DbIterator> {
        self.inner
            .iter_at_seq(None, lo, hi.map(|h| h.to_vec()), reverse)
    }

    pub fn iter_at(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        snap: &Snapshot,
    ) -> Result<DbIterator> {
        self.inner
            .iter_at_seq(Some(snap.seq), lo, hi.map(|h| h.to_vec()), reverse)
    }

    // ------------------------------------------------------- transactions

    pub fn begin(&self) -> Txn {
        Txn::new(self.inner.clone())
    }

    // --------------------------------------------------------- maintenance

    /// Freeze the active memtable and wait until everything is in tables.
    pub fn flush(&self) -> Result<()> {
        self.inner.force_rotate()?;
        self.inner.wait_flushed()
    }

    /// Run compaction until no trigger fires (test/CLI helper).
    pub fn compact_all(&self) -> Result<()> {
        crate::compaction::compact_until_quiet(&self.inner)
    }

    /// Manually run one value-log GC pass. Returns the reclaimed (retired)
    /// vlog file id, if any victim qualified.
    pub fn gc_vlog(&self) -> Result<Option<u64>> {
        crate::compaction::gc_vlog(&self.inner)
    }

    pub fn stats(&self) -> DbStats {
        let inner = &self.inner;
        // lock order is manifest -> state everywhere; never hold the state
        // guard while acquiring the manifest lock
        let (levels, memtable_bytes, immutable_memtables, vlog_files) = {
            let s = inner.state.read();
            (
                s.version
                    .levels
                    .iter()
                    .map(|runs| {
                        (
                            runs.len(),
                            runs.iter().map(|r| r.tables.len()).sum(),
                            runs.iter().map(|r| r.size()).sum(),
                        )
                    })
                    .collect(),
                s.mem.approximate_bytes(),
                s.imms.len(),
                s.version.vlogs.len(),
            )
        };
        let (hits, misses) = inner.cache.hit_rate();
        let (vlog_retired, discard_bytes) = {
            let m = inner.manifest.lock();
            (
                m.data.vlog_retired.len(),
                m.data.discard.iter().map(|(_, b)| *b).sum(),
            )
        };
        DbStats {
            backend: inner.backend_name,
            visible_seqno: inner.visible_seqno.load(Ordering::Acquire),
            memtable_bytes,
            immutable_memtables,
            levels,
            vlog_files,
            vlog_retired,
            discard_bytes,
            cache_hits: hits,
            cache_misses: misses,
        }
    }

    // -------------------------------------------------------------- wasm

    #[cfg(feature = "wasm")]
    pub fn install_module(&self, name: &str, wasm: &[u8]) -> Result<()> {
        crate::wasm::install_module(&self.inner, name, wasm)
    }

    #[cfg(feature = "wasm")]
    pub fn uninstall_module(&self, name: &str) -> Result<()> {
        crate::wasm::uninstall_module(&self.inner, name)
    }

    #[cfg(feature = "wasm")]
    pub fn list_modules(&self) -> Result<Vec<crate::wasm::ModuleInfo>> {
        crate::wasm::list_modules(&self.inner)
    }

    /// Run a read-only WASM query against the current visible state.
    #[cfg(feature = "wasm")]
    pub fn query(&self, name: &str, input: &[u8]) -> Result<Vec<u8>> {
        crate::wasm::query(&self.inner, name, input, None)
    }

    #[cfg(feature = "wasm")]
    pub fn query_at(&self, name: &str, input: &[u8], snap: &Snapshot) -> Result<Vec<u8>> {
        crate::wasm::query(&self.inner, name, input, Some(snap.seq))
    }

    /// Run a WASM executor inside a transaction; commits on guest exit 0,
    /// retries automatically on conflict.
    #[cfg(feature = "wasm")]
    pub fn execute(&self, name: &str, input: &[u8]) -> Result<Vec<u8>> {
        crate::wasm::execute(&self.inner, name, input)
    }

    // -------------------------------------------------------- checkpoints

    pub fn checkpoint(&self, name: &str) -> Result<CheckpointInfo> {
        crate::checkpoint::create(self, name)
    }

    pub fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        crate::checkpoint::list(&self.inner.paths)
    }

    pub fn delete_checkpoint(&self, name: &str) -> Result<()> {
        crate::checkpoint::delete(&self.inner.paths, name)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.flush_signal.notify();
        self.inner.compact_signal.notify();
        self.inner.progress_signal.notify();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn flush_thread(db: Arc<DbInner>) {
    while !db.shutdown.load(Ordering::Acquire) {
        match db.flush_one() {
            Ok(true) => continue,
            Ok(false) => db.flush_signal.wait_timeout(Duration::from_millis(200)),
            Err(e) => {
                let mut g = db.bg_error.lock();
                if g.is_none() {
                    *g = Some(format!("flush failed: {e}"));
                }
                db.progress_signal.notify();
                db.flush_signal.wait_timeout(Duration::from_millis(500));
            }
        }
    }
}

fn compact_thread(db: Arc<DbInner>) {
    while !db.shutdown.load(Ordering::Acquire) {
        let did = match crate::compaction::maintenance_pass(&db) {
            Ok(did) => did,
            Err(e) => {
                let mut g = db.bg_error.lock();
                if g.is_none() {
                    *g = Some(format!("compaction failed: {e}"));
                }
                db.progress_signal.notify();
                false
            }
        };
        if !did {
            db.compact_signal.wait_timeout(Duration::from_millis(250));
        }
    }
}

// ---------------------------------------------------------------------------
// Open / recovery
// ---------------------------------------------------------------------------

fn open_inner(dir: &Path, opts: Options) -> Result<Arc<DbInner>> {
    let paths = DbPaths::new(dir);
    if !dir.exists() {
        if !opts.create_if_missing {
            return Err(Error::InvalidArgument(format!(
                "{} does not exist",
                dir.display()
            )));
        }
        std::fs::create_dir_all(dir)?;
        if let Some(parent) = dir.parent() {
            io::sync_dir(parent)?;
        }
    }

    // exclusive directory lock for the process lifetime
    let lock_path = dir.join("LOCK");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    if lock_file.try_lock().is_err() {
        return Err(Error::InvalidArgument(format!(
            "{} is locked by another process",
            dir.display()
        )));
    }

    let (io_backend, backend_name) = io::backend(opts.io_backend)?;
    let cache = Arc::new(BlockCache::new(opts.block_cache_size));

    if !manifest::exists(dir) {
        if !opts.create_if_missing {
            return Err(Error::InvalidArgument(format!(
                "no database at {}",
                dir.display()
            )));
        }
        init_fresh(&paths, io_backend.as_ref())?;
    }

    let (gen, mut mdata) = manifest::load(&paths)?;
    // normalize the levels array: flush/compaction index it directly
    let nlevels = opts.max_levels.max(mdata.levels.len()).max(1);
    mdata.levels.resize(nlevels, Vec::new());

    // remove orphaned manifests (older gens and pre-flip crashed newer gens)
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(g) = name
                .strip_prefix("MANIFEST-")
                .and_then(|s| s.parse::<u64>().ok())
            {
                if g != gen {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    let mut next_file_id = mdata.next_file_id.max(1);

    // ---- open tables ------------------------------------------------------
    let mut version = Version::empty(opts.max_levels.max(mdata.levels.len()));
    for (li, level) in mdata.levels.iter().enumerate() {
        for rm in level {
            let mut tables = Vec::with_capacity(rm.table_ids.len());
            for &tid in &rm.table_ids {
                let path = paths.table(tid);
                let file = io_backend.open_read(&path)?;
                let size = file.len()?;
                let table = Table::open(file, tid, cache.clone())?;
                tables.push(Arc::new(TableHandle::new(tid, path, size, table)));
            }
            version.levels[li].push(Run {
                id: rm.id,
                tables,
            });
        }
    }

    // ---- vlog files: manifest live set + adopt young files -----------------
    let mut disk_vlogs: Vec<u64> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(id) = parse_file_id(name, "vlog-", ".vlog") {
                disk_vlogs.push(id);
            }
        }
    }
    let mut live: BTreeMap<u64, Arc<VlogFileHandle>> = BTreeMap::new();
    let open_vlog = |id: u64| -> Result<Arc<VlogFileHandle>> {
        let path = paths.vlog(id);
        let file = io_backend.open_read(&path)?;
        Ok(Arc::new(VlogFileHandle::new(id, path, file)))
    };
    for &id in &mdata.vlog_live {
        live.insert(id, open_vlog(id)?);
    }
    // retired victims still awaiting their deletion gates: they stay
    // RESOLVABLE (in the version map, sharing the same handle Arc) until
    // process_retired passes the gates — old versions in tables may still
    // dereference them
    let retired_ids: std::collections::HashSet<u64> =
        mdata.vlog_retired.iter().map(|(id, _)| *id).collect();
    let mut retired_list = Vec::new();
    for &(id, s) in &mdata.vlog_retired {
        if paths.vlog(id).exists() {
            let handle = open_vlog(id)?;
            live.insert(id, handle.clone());
            retired_list.push(RetiredVlog {
                id,
                retired_at: s,
                handle,
            });
        }
    }
    // young files: created after the manifest's head was recorded (rotation
    // does not flip the manifest); adopt them — never delete pre-replay.
    // Retired ids are NOT young even when id >= head: re-adopting them into
    // the live set would leave a dangling vlog_live entry once the deletion
    // gates pass.
    let mut young: BTreeMap<u64, BTreeMap<u64, (u32, Vec<u8>)>> = BTreeMap::new();
    for &id in disk_vlogs.iter() {
        if id >= mdata.vlog_head && !live.contains_key(&id) && !retired_ids.contains(&id) {
            live.insert(id, open_vlog(id)?);
        }
    }
    // valid-prefix index of every young file (head included) for WAL replay
    // pointer validation
    for (&id, handle) in live.iter() {
        if id >= mdata.vlog_head && !retired_ids.contains(&id) {
            let (records, _valid) = vlog::scan_records(handle.file.as_ref())?;
            let map = records
                .into_iter()
                .map(|(off, len, key, _vlen)| (off, (len, key)))
                .collect();
            young.insert(id, map);
            next_file_id = next_file_id.max(id + 1);
        }
    }

    // ---- WAL replay ---------------------------------------------------------
    let mut wal_ids: Vec<u64> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(id) = parse_file_id(name, "wal-", ".log") {
                if id >= mdata.wal_floor {
                    wal_ids.push(id);
                }
                next_file_id = next_file_id.max(id + 1);
            } else if let Some(id) = parse_file_id(name, "sst-", ".tbl") {
                // orphaned outputs of a crashed flush/compaction: startup_gc
                // deletes them at the END of open, so the id counter must
                // already clear them or the recovery flush collides
                next_file_id = next_file_id.max(id + 1);
            }
        }
    }
    wal_ids.sort_unstable();

    // the recovered memtable is 1:1 with ALL replayed WALs: tagging it with
    // the newest replayed id makes its (synchronous) flush advance the floor
    // past every replayed WAL in the SAME manifest write that records the
    // recovery SST — no window where reopen would re-replay and duplicate
    let recovered = Arc::new(Memtable::new(wal_ids.last().copied().unwrap_or(0)));
    let mut max_seq = mdata.last_flushed_seqno;
    let mut truncate_torn: Option<(u64, u64)> = None; // (wal id, valid_len)
    'wals: for (wi, &wid) in wal_ids.iter().enumerate() {
        let file = io_backend.open_read(&paths.wal(wid))?;
        let is_last = wi == wal_ids.len() - 1;
        let (records, tail) = read_wal(file.as_ref())?;
        for payload in &records {
            let (base, entries) = decode_batch(payload)?;
            // validate pointers into young vlog files (older sealed files
            // were fdatasynced before the WAL records referencing them)
            for e in &entries {
                if e.kind == ValueKind::Put {
                    if let ReprRef::Ptr(p) = decode_repr(&e.repr)? {
                        if let Some(index) = young.get(&p.file) {
                            let ok = index
                                .get(&p.offset)
                                .is_some_and(|(len, key)| *len == p.len && key == &e.key);
                            if !ok {
                                // payload never became durable: everything
                                // from here on is torn-tail loss
                                break 'wals;
                            }
                        } else if !live.contains_key(&p.file)
                            && !retired_list.iter().any(|r| r.id == p.file)
                        {
                            return Err(corrupt(format!(
                                "wal-{wid:06} references unknown vlog file {}",
                                p.file
                            )));
                        }
                    }
                }
            }
            for (i, e) in entries.into_iter().enumerate() {
                let seq = base + i as u64;
                max_seq = max_seq.max(seq);
                recovered.insert(make_ikey(&e.key, seq, e.kind), e.repr);
            }
        }
        match tail {
            WalTail::Clean => {}
            WalTail::Torn { valid_len } if is_last => {
                truncate_torn = Some((wid, valid_len));
                break;
            }
            WalTail::Torn { valid_len } => {
                return Err(corrupt(format!(
                    "sealed wal-{wid:06} damaged at offset {valid_len}"
                )));
            }
        }
    }

    // Neutralize a torn tail NOW: once the fresh WAL below exists, this file
    // is no longer the newest, and a crash before the floor advances would
    // make the next open misread the (legitimate) torn tail as sealed-WAL
    // corruption — permanently. Truncating to the valid prefix makes the
    // file clean under every future classification.
    if let Some((wid, valid_len)) = truncate_torn {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(paths.wal(wid))?;
        f.set_len(valid_len)?;
        f.sync_all()?;
    }

    // ---- fresh head vlog (never append to a file that predates a crash) ---
    let head_id = next_file_id;
    next_file_id += 1;
    let head_path = paths.vlog(head_id);
    let head_file = io_backend.create_new(&head_path)?;
    let head_handle = Arc::new(VlogFileHandle::new(head_id, head_path, head_file));
    live.insert(head_id, head_handle.clone());
    version.vlogs = live;
    version.vlog_head_id = head_id;
    let vlog = Vlog::new(head_handle, 0);

    // ---- fresh WAL + memtable ----------------------------------------------
    let wal_id = next_file_id;
    next_file_id += 1;
    let wal_file = io_backend.create_new(&paths.wal(wal_id))?;
    io::sync_dir(dir)?;
    let wal = WalWriter::new(wal_file);
    let mem = Arc::new(Memtable::new(wal_id));

    #[cfg(feature = "wasm")]
    let wasm = WasmRuntime::new(&opts)?;

    let inner = Arc::new(DbInner {
        opts,
        paths: paths.clone(),
        io: io_backend,
        backend_name,
        cache,
        state: RwLock::new(DbState {
            mem,
            imms: if recovered.is_empty() {
                Vec::new()
            } else {
                vec![recovered.clone()]
            },
            version: Arc::new(version),
        }),
        write_mu: Mutex::new(WriteState { wal }),
        visible_seqno: AtomicU64::new(max_seq),
        next_file_id: AtomicU64::new(next_file_id),
        manifest: Mutex::new(ManifestState {
            gen,
            data: mdata,
        }),
        snapshots: Mutex::new(SnapshotList::default()),
        vlog,
        retired: Mutex::new(retired_list),
        gc_mu: Mutex::new(()),
        compaction_mu: Mutex::new(()),
        shutdown: AtomicBool::new(false),
        flush_signal: Signal::new(),
        compact_signal: Signal::new(),
        progress_signal: Signal::new(),
        bg_error: Mutex::new(None),
        #[cfg(feature = "wasm")]
        wasm,
        _lock_file: lock_file,
    });

    // Flush the recovered memtable synchronously. Its wal_id is the newest
    // replayed WAL, so flush_one's manifest write records the recovery SST
    // AND advances the floor past every replayed WAL atomically — a crash
    // in between can only re-replay, never observe the SST without the
    // floor (which would duplicate data).
    if !inner.state.read().imms.is_empty() {
        inner.flush_one()?;
    }

    // Consolidated open-completion manifest: fresh WAL floor, the adopted
    // vlog set (retired victims are tracked in vlog_retired, never in
    // vlog_live — the in-memory version map holds both for resolution),
    // fresh head, and the final id counter.
    {
        let retired_now: std::collections::HashSet<u64> = {
            let m = inner.manifest.lock();
            m.data.vlog_retired.iter().map(|(id, _)| *id).collect()
        };
        let live_ids: Vec<u64> = inner
            .state
            .read()
            .version
            .vlogs
            .keys()
            .copied()
            .filter(|id| !retired_now.contains(id))
            .collect();
        let mut m = inner.manifest.lock();
        let mut data = m.data.clone();
        data.wal_floor = wal_id;
        data.vlog_live = live_ids;
        data.vlog_head = head_id;
        data.next_file_id = inner.next_file_id.load(Ordering::SeqCst);
        let gen = m.gen + 1;
        manifest::save(&inner.paths, gen, &data)?;
        m.gen = gen;
        m.data = data;
        drop(m);
        inner.delete_old_wals(wal_id);
    }

    // ---- startup GC of unreferenced files ----------------------------------
    startup_gc(&inner)?;

    // sweep checkpoint builds that crashed mid-creation (we hold the LOCK,
    // so nothing can be legitimately building right now)
    if let Ok(rd) = std::fs::read_dir(inner.paths.archive_root()) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".tmp-") {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    Ok(inner)
}

fn init_fresh(paths: &DbPaths, _io: &dyn Io) -> Result<()> {
    let data = ManifestData {
        next_file_id: 1,
        last_flushed_seqno: 0,
        wal_floor: 0,
        levels: Vec::new(),
        vlog_live: Vec::new(),
        vlog_head: 0,
        vlog_retired: Vec::new(),
        discard: Vec::new(),
    };
    manifest::save(paths, 1, &data)?;
    Ok(())
}

/// Delete files no durable state references. Runs once at open, after
/// recovery, before background threads start — nothing is concurrently
/// pinned.
fn startup_gc(inner: &Arc<DbInner>) -> Result<()> {
    let m = inner.manifest.lock();
    let data = &m.data;
    let referenced_tables: std::collections::HashSet<u64> = data
        .levels
        .iter()
        .flat_map(|l| l.iter().flat_map(|r| r.table_ids.iter().copied()))
        .collect();
    let live_vlogs: std::collections::HashSet<u64> = data
        .vlog_live
        .iter()
        .copied()
        .chain(data.vlog_retired.iter().map(|(id, _)| *id))
        .collect();
    let rd = std::fs::read_dir(&inner.paths.dir)?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(id) = parse_file_id(name, "sst-", ".tbl") {
            if !referenced_tables.contains(&id) {
                let _ = std::fs::remove_file(entry.path());
            }
        } else if let Some(id) = parse_file_id(name, "vlog-", ".vlog") {
            if !live_vlogs.contains(&id) {
                let _ = std::fs::remove_file(entry.path());
            }
        } else if let Some(id) = parse_file_id(name, "wal-", ".log") {
            if id < data.wal_floor {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    Ok(())
}
