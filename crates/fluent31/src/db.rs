//! The engine core: open/recovery, the write path, read views, snapshots,
//! flushes, write stalls, and background thread lifecycle.
//!
//! Locking (fixed global order — always acquire left before right):
//! `write_mu` → `manifest` → `state` → `snapshots`
//! Not every path takes every lock; no path acquires them out of order.
//! Group-commit locks are leaves off that chain: `commit_queue` is taken
//! after `write_mu` (leader) or standalone (enqueue/front checks), and
//! `CommitWaiter::inner` nests inside `commit_queue` or standalone —
//! neither is ever held while acquiring a lock from the global chain.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use crate::batch::{decode_batch, encode_batch, BatchOp, EncEntry, WriteBatch};
use crate::cache::BlockCache;
use crate::fork::ForkInfo;
use crate::config::{DbPaths, Options, SyncMode};
use crate::error::{corrupt, Error, Result};
use crate::identity::StoreIdentity;
use crate::io::{self, Io};
use crate::iter::DbIterator;
use crate::manifest::{self, ManifestData, PinInfo, RunMeta};
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
    /// Pending-notify flag: a notify with no waiter parks here instead of
    /// vanishing, so a wake sent between a consumer's work and its next
    /// wait is consumed immediately (no lost-wakeup latency cliff).
    mu: Mutex<bool>,
    cv: Condvar,
}

impl Signal {
    pub(crate) fn new() -> Self {
        Signal {
            mu: Mutex::new(false),
            cv: Condvar::new(),
        }
    }
    pub fn notify(&self) {
        let mut g = self.mu.lock();
        *g = true;
        self.cv.notify_all();
    }
    pub fn wait_timeout(&self, d: Duration) {
        let mut g = self.mu.lock();
        if !*g {
            self.cv.wait_for(&mut g, d);
        }
        *g = false;
    }
}

/// A retired vlog GC victim awaiting its deletion gates.
pub(crate) struct RetiredVlog {
    pub id: u64,
    pub retired_at: SeqNo,
    pub handle: Arc<VlogFileHandle>,
}

/// A commit-queue entry as the committer consumes it: the batch, the
/// optional OCC validation spec (snapshot, conflict keys), and whether the
/// batch is trigger-capture eligible (user-originated logical writes).
type QueuedEntry = (Vec<BatchOp>, Option<(SeqNo, Vec<Vec<u8>>)>, bool);

/// One enqueued write awaiting group commit. `ops` is taken by the leader;
/// `result` is set when the group completes.
pub(crate) struct CommitWaiter {
    inner: Mutex<WaiterInner>,
    cv: Condvar,
}

pub(crate) struct WaiterInner {
    ops: Option<Vec<BatchOp>>,
    bytes: usize,
    /// OCC validation for transactional commits: no key may have a
    /// committed version newer than the snapshot — neither in the store
    /// nor written by an earlier batch of the same group.
    validate: Option<(SeqNo, Vec<Vec<u8>>)>,
    /// Trigger-capture eligibility (user-originated logical writes).
    capture: bool,
    result: Option<Result<()>>,
}

/// Unwind safety net for the committer thread: once waiters are drained
/// from the queue, only the committer can complete them. If it panics
/// mid-group, this guard degrades the store — a panic mid-write leaves
/// WAL/vlog state unknown — and fails every undelivered waiter, so no
/// client thread hangs (parked clients also poll bg_error, covering the
/// committer dying entirely).
struct GroupPanicGuard<'a> {
    db: &'a DbInner,
    group: &'a [Arc<CommitWaiter>],
    armed: bool,
}

impl Drop for GroupPanicGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.db
            .set_bg_error("commit thread panicked; write state unknown");
        for w in self.group {
            let mut g = w.inner.lock();
            if g.result.is_none() {
                g.result = Some(Err(Error::Background(
                    "commit thread panicked; write state unknown".into(),
                )));
            }
            drop(g);
            w.cv.notify_one();
        }
    }
}

pub(crate) struct DbInner {
    pub opts: Options,
    pub paths: DbPaths,
    pub io: Arc<dyn Io>,
    pub backend_name: &'static str,
    pub cache: Arc<BlockCache>,
    /// Resolved at open (verified / adopted / fork-minted), immutable for
    /// the process lifetime; also persisted inside the manifest data.
    pub identity: Option<StoreIdentity>,

    pub state: RwLock<DbState>,
    pub write_mu: Mutex<WriteState>,
    /// Writers waiting for (or leading) a group commit; the front is the
    /// leader. See `write_batch_unchecked`.
    pub commit_queue: Mutex<std::collections::VecDeque<Arc<CommitWaiter>>>,
    pub commit_groups: AtomicU64,
    pub commit_batches: AtomicU64,
    pub wal_syncs: AtomicU64,
    pub visible_seqno: AtomicU64,
    pub next_file_id: AtomicU64,
    pub manifest: Mutex<ManifestState>,
    pub snapshots: Mutex<SnapshotList>,
    pub vlog: Vlog,
    /// Retired GC victims awaiting their deletion gates.
    pub retired: Mutex<Vec<RetiredVlog>>,
    /// Serializes vlog GC passes (manual + automatic).
    pub gc_mu: Mutex<()>,
    /// GC sampling cooldowns: vlog file id -> (visible seqno, vlog head id)
    /// at last sample. A below-ratio file is resampled once enough new
    /// writes happened — by seqno count, OR by the head having rotated
    /// (large-value workloads accrue garbage fast while seqnos crawl).
    pub gc_sampled_at: Mutex<std::collections::HashMap<u64, (SeqNo, u64)>>,
    /// Serializes compaction jobs: the maintenance thread and user-invoked
    /// `compact_all` must never pick/merge concurrently (both would grab the
    /// same input runs).
    pub compaction_mu: Mutex<()>,

    /// Replication stream subscribers (see the replication-surface section
    /// at the bottom of this file). Lock order: a leaf taken under
    /// `write_mu` by the publisher; each subscriber's queue mutex nests
    /// inside it. `subs_active` lets the write path skip the lock entirely
    /// when nothing is subscribed.
    pub subs: Mutex<Vec<Arc<SubShared>>>,
    pub subs_active: AtomicBool,

    pub shutdown: AtomicBool,
    pub commit_signal: Signal,
    pub flush_signal: Signal,
    pub compact_signal: Signal,
    /// Signaled on flush/compaction progress (stall + flush waiters).
    pub progress_signal: Signal,
    pub bg_error: Mutex<Option<String>>,

    #[cfg(feature = "wasm")]
    pub wasm: WasmRuntime,
    /// Trigger registry, runner wake signal, and drain status.
    #[cfg(feature = "wasm")]
    pub triggers: crate::trigger::TriggerState,

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
    /// forks to pin their cut.
    pub fn register_snapshot_at(&self, seq: SeqNo) {
        debug_assert!(seq <= self.visible_seqno.load(Ordering::Acquire));
        self.snapshots.lock().register(seq);
    }

    /// Register a snapshot at `seq` only if the state at `seq` is still
    /// fully materializable, i.e. no GC pass may have dropped versions it
    /// needs. Check and registration are atomic under the snapshots lock,
    /// so the watermark cannot race past `seq` in between. The state at
    /// `seq` is intact iff `seq >= watermark` (compaction keeps everything
    /// above the watermark verbatim plus the newest version at-or-below
    /// it); `seq == visible` is additionally always safe — with no
    /// registered snapshots the watermark sits at `visible + 1`, but the
    /// newest version of every key IS the state at `visible`.
    pub fn try_register_snapshot_at(&self, seq: SeqNo) -> Result<()> {
        let mut g = self.snapshots.lock();
        let visible = self.visible_seqno.load(Ordering::Acquire);
        if seq > visible {
            return Err(Error::InvalidArgument(format!(
                "seqno {seq} has not been committed yet (visible seqno is {visible})"
            )));
        }
        let wm = g.min().unwrap_or(visible + 1);
        if seq < wm && seq != visible {
            return Err(Error::InvalidArgument(format!(
                "state at seqno {seq} is no longer retained (GC watermark is {wm}); \
                 create a pin at the points you want to keep fork-able"
            )));
        }
        g.register(seq);
        Ok(())
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

    /// Degrade the store: every subsequent write is refused until reopen.
    /// Used by background threads on flush/compaction failure and by the
    /// write path when a hard IO failure leaves WAL/vlog state unknown.
    pub fn set_bg_error(&self, msg: impl Into<String>) {
        let mut g = self.bg_error.lock();
        if g.is_none() {
            *g = Some(msg.into());
        }
    }

    /// Durability barrier: everything acked before this call is durable
    /// when it returns. Payload before pointer, as everywhere: the vlog
    /// head syncs before the WAL records referencing it.
    pub fn sync_wal(&self) -> Result<()> {
        self.check_bg_error()?;
        let ws = self.write_mu.lock();
        if let Err(e) = self.vlog.sync_head() {
            self.set_bg_error(format!("vlog sync failed: {e}"));
            return Err(e);
        }
        if let Err(e) = ws.wal.sync() {
            self.set_bg_error(format!("wal sync failed: {e}"));
            return Err(e);
        }
        self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

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

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.validate_batch(&batch)?;
        // capture=true: user-originated logical writes are the only
        // trigger-capture source, and only after user-key validation so
        // just accepted writes can fire. The events themselves materialize
        // inside the apply critical section (see capture_events).
        self.write_batch_inner(batch, true)
    }

    /// Write path without user-key validation and without trigger capture
    /// (system keys, engine-internal rewrites).
    pub fn write_batch_unchecked(&self, batch: WriteBatch) -> Result<()> {
        self.write_batch_inner(batch, false)
    }

    /// Group commit, LevelDB-style: the caller enqueues its batch and the
    /// writer at the queue front becomes the leader — it drains a bounded
    /// group, applies every batch under one `write_mu` critical section
    /// with ONE vlog fsync and ONE WAL fsync for the whole group, then
    /// hands each waiter its result. Concurrent writers therefore amortize
    /// fsync latency instead of serializing on it.
    fn write_batch_inner(&self, batch: WriteBatch, capture: bool) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.check_bg_error()?;
        self.wait_for_space()?;

        // Grouping exists to amortize fsyncs. Writers that don't fsync
        // inline (Never, and Periodic — its fsyncs ride a background timer)
        // gain nothing from the queue's park/unpark handoffs, so they take
        // the direct path.
        if self.opts.sync != SyncMode::Always {
            let mut ws = self.write_mu.lock();
            let r = self.apply_locked(&mut ws, &batch.ops, capture);
            self.commit_groups.fetch_add(1, Ordering::Relaxed);
            self.commit_batches.fetch_add(1, Ordering::Relaxed);
            return r;
        }

        // No uncontended fast path in Always mode, deliberately: a writer
        // that wins write_mu pays a FULL solo fsync (~ms) while blocking
        // the committer from forming a group — measured to fragment groups
        // and halve 4-thread throughput. The queue handoff costs ~µs
        // against an fsync; every sync write goes through the committer.
        let bytes = batch.byte_size();
        self.queue_commit(batch.ops, bytes, None, capture)
    }

    /// Enqueue a batch (optionally OCC-validated) on the commit queue and
    /// park until the committer delivers its result.
    pub(crate) fn queue_commit(
        &self,
        ops: Vec<BatchOp>,
        bytes: usize,
        validate: Option<(SeqNo, Vec<Vec<u8>>)>,
        capture: bool,
    ) -> Result<()> {
        let waiter = Arc::new(CommitWaiter {
            inner: Mutex::new(WaiterInner {
                ops: Some(ops),
                bytes,
                validate,
                capture,
                result: None,
            }),
            cv: Condvar::new(),
        });
        self.commit_queue.lock().push_back(waiter.clone());
        self.commit_signal.notify();

        // park until the committer thread delivers the result. Result is
        // set and read under the same waiter mutex, so no wakeup can be
        // lost; the timeout exists only so a dead committer (panic sets
        // bg_error) or shutdown can't strand this thread.
        let mut g = waiter.inner.lock();
        loop {
            if let Some(r) = g.result.take() {
                return r;
            }
            waiter.cv.wait_for(&mut g, Duration::from_millis(100));
            if g.result.is_some() {
                continue;
            }
            // poll for a degraded store / shutdown — WITHOUT holding the
            // waiter lock (the flush thread may briefly hold bg_error, and
            // the committer needs waiter.inner to deliver)
            drop(g);
            let degraded = self.bg_error.lock().clone();
            let shut = self.shutdown.load(Ordering::Acquire);
            if degraded.is_some() || shut {
                // bail ONLY if the batch can be pulled back out of the
                // queue. If the committer already drained it, a result is
                // guaranteed (delivery or panic guard) — returning an
                // error for a batch that may still commit would hand the
                // caller a false failure and invite duplicating retries.
                let removed = {
                    let mut q = self.commit_queue.lock();
                    match q.iter().position(|w| Arc::ptr_eq(w, &waiter)) {
                        Some(i) => {
                            q.remove(i);
                            true
                        }
                        None => false,
                    }
                };
                if removed {
                    return Err(match degraded {
                        Some(msg) => Error::Background(msg),
                        None => Error::Closed,
                    });
                }
            }
            g = waiter.inner.lock();
        }
    }

    /// Core of the write path for callers that need their own `write_mu`
    /// critical section (transaction commits validating under the mutex,
    /// GC relocations probing liveness under it) and for the single-batch
    /// lanes of `write_batch_unchecked`.
    ///
    /// This is the allocation-lean single-batch twin of
    /// `apply_group_locked` — same phases, same error semantics (hard IO
    /// failures degrade the store; rotation failure after publish keeps
    /// the ack). Any change here must be mirrored there.
    pub(crate) fn apply_locked(
        &self,
        ws: &mut WriteState,
        ops: &[BatchOp],
        capture: bool,
    ) -> Result<()> {
        let base = self.visible_seqno.load(Ordering::Acquire) + 1;
        // trigger events ride the same batch (atomic through WAL, memtable,
        // and crash recovery). Materialized HERE, after the base seqno is
        // fixed and under write_mu: changes-mode queue keys embed per-op
        // commit seqnos, so capture outside the critical section could
        // order the feed differently from the commits it describes.
        let events = self.capture_events(ops, base, capture);
        let total = ops.len() + events.len();
        if base + total as u64 >= MAX_SEQNO {
            return Err(Error::InvalidArgument("seqno space exhausted".into()));
        }
        // size-check BEFORE vlog placement (see apply_group_locked)
        let approx: u64 = ops
            .iter()
            .chain(events.iter())
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
        let mut entries = Vec::with_capacity(total);
        let mut any_vlog = false;
        for op in ops.iter().chain(events.iter()) {
            let e = match op {
                BatchOp::Put { key, value } => {
                    let repr = if value.len() >= self.opts.value_threshold {
                        any_vlog = true;
                        match self.vlog.append(key, value) {
                            Ok(ptr) => encode_ptr(ptr),
                            Err(e) => {
                                self.set_bg_error(format!("vlog append failed: {e}"));
                                return Err(e);
                            }
                        }
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

        // 2. durability ordering: payload before pointer
        if any_vlog && self.opts.sync == SyncMode::Always {
            if let Err(e) = self.vlog.sync_head() {
                self.set_bg_error(format!("vlog sync failed: {e}"));
                return Err(e);
            }
        }
        let payload = encode_batch(base, &entries);
        if payload.len() as u64 >= 1 << 30 {
            return Err(Error::InvalidArgument(
                "write batch exceeds WAL record limit".into(),
            ));
        }
        if let Err(e) = ws.wal.append_record(&payload) {
            self.set_bg_error(format!("wal append failed: {e}"));
            return Err(e);
        }
        if self.opts.sync == SyncMode::Always {
            if let Err(e) = ws.wal.sync() {
                self.set_bg_error(format!("wal sync failed: {e}"));
                return Err(e);
            }
            self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        }

        // 3. memtable inserts, then publish
        let mem = self.state.read().mem.clone();
        for (i, e) in entries.iter().enumerate() {
            mem.insert(make_ikey(&e.key, base + i as u64, e.kind), e.repr.clone());
        }
        self.visible_seqno
            .store(base + entries.len() as u64 - 1, Ordering::Release);
        self.publish_stream(base, &entries);

        // 4. rotations. The write is durable and published: the ack stands
        // even if rotation fails — the store degrades instead.
        let mut rotate = || -> Result<()> {
            if mem.approximate_bytes() >= self.opts.memtable_size {
                self.rotate_memtable_locked(ws)?;
                self.flush_signal.notify();
            }
            let (_, head_written, _) = self.vlog.head_state();
            if head_written >= self.opts.vlog_file_size {
                self.rotate_vlog_locked()?;
            }
            Ok(())
        };
        if let Err(e) = rotate() {
            self.set_bg_error(format!("post-write rotation failed: {e}"));
        }
        // the write is durable and published: wake the trigger runner even
        // though rotation may have degraded the store (harmless wakeup)
        if !events.is_empty() {
            self.notify_triggers();
        }
        Ok(())
    }

    /// Trigger events to append to a capture-eligible batch committing at
    /// `base` (empty without the wasm feature or when no trigger matches).
    /// Must be called under `write_mu` with `base` freshly allocated — see
    /// `TriggerState::capture_ops` for why.
    #[inline]
    fn capture_events(&self, ops: &[BatchOp], base: SeqNo, capture: bool) -> Vec<BatchOp> {
        #[cfg(feature = "wasm")]
        if capture {
            return self.triggers.capture_ops(ops, base, &self.opts);
        }
        let _ = (ops, base, capture);
        Vec::new()
    }

    #[inline]
    fn notify_triggers(&self) {
        #[cfg(feature = "wasm")]
        self.triggers.signal.notify();
    }

    /// Apply a group of batches under one `write_mu` critical section
    /// (multi-batch twin of `apply_locked` — keep their phases in sync):
    /// per-batch validation and WAL records (each batch keeps its own
    /// contiguous seqno range and all-or-nothing atomicity), but one vlog
    /// fsync and one WAL fsync for the whole group.
    ///
    /// Error semantics: batch-local validation failures skip just that
    /// batch (it consumes no seqnos, later batches proceed). A hard IO
    /// failure mid-group fails that batch with the real error and every
    /// LATER batch with `Error::Background` — the already-appended prefix
    /// still completes, so survivors are always a seqno-contiguous prefix
    /// and `visible_seqno` never publishes past a failed batch.
    pub(crate) fn apply_group_locked(
        &self,
        ws: &mut WriteState,
        batches: &[(&[BatchOp], bool)],
    ) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = Vec::new();
        results.resize_with(batches.len(), || None);

        // ---- phase 0: per-batch validation + seqno assignment ----------
        // (validation failures consume no seqnos, so accepted batches form
        // a contiguous seqno range starting at visible+1). Trigger events
        // materialize here — the batch's base seqno is fixed at this point
        // and write_mu is held, which is exactly what commit-ordered
        // changes-mode queue keys require — and each batch's events ride
        // its own WAL record (same crash atomicity as the writes).
        let mut next = self.visible_seqno.load(Ordering::Acquire) + 1;
        let mut accepted: Vec<(usize, u64, Vec<BatchOp>)> = Vec::with_capacity(batches.len());
        for (i, (ops, capture)) in batches.iter().enumerate() {
            let events = self.capture_events(ops, next, *capture);
            // size-check BEFORE vlog placement: rejecting afterwards would
            // orphan already-appended vlog records that no discard
            // accounting ever reclaims (pointer reprs only shrink the
            // encoded batch, so this bound is conservative)
            let approx: u64 = ops
                .iter()
                .chain(events.iter())
                .map(|op| match op {
                    BatchOp::Put { key, value } => (key.len() + value.len() + 32) as u64,
                    BatchOp::Delete { key } => (key.len() + 16) as u64,
                })
                .sum();
            if approx >= 1 << 30 {
                results[i] = Some(Err(Error::InvalidArgument(
                    "write batch exceeds WAL record limit".into(),
                )));
                continue;
            }
            let total = (ops.len() + events.len()) as u64;
            if next + total >= MAX_SEQNO {
                results[i] = Some(Err(Error::InvalidArgument(
                    "seqno space exhausted".into(),
                )));
                continue;
            }
            accepted.push((i, next, events));
            next += total;
        }

        // fail every accepted batch from `from` onward: their writes were
        // NOT applied (retry is safe once the store recovers), so they get
        // an "aborted" IO error, never a fabricated Background — that
        // variant is reserved for a store actually flagged via bg_error
        let aborted = |msg: &str| {
            Error::Io(std::io::Error::other(format!(
                "write group aborted by another batch's failure: {msg}"
            )))
        };
        let fail_tail = |results: &mut Vec<Option<Result<()>>>,
                         accepted: &[(usize, u64, Vec<BatchOp>)],
                         from: usize,
                         msg: &str| {
            for (j, _, _) in &accepted[from..] {
                if results[*j].is_none() {
                    results[*j] = Some(Err(aborted(msg)));
                }
            }
        };
        // a hard failure mid-group leaves WAL/vlog head state unknown:
        // degrade the store (writes refused until reopen) and skip the
        // phase-6 rotation so a WAL with a possibly-torn middle is never
        // sealed — sealed-WAL corruption fails recovery permanently, while
        // an unsealed tail is truncated cleanly on reopen. Vlog values
        // already placed for aborted batches are orphaned (pre-existing
        // leak class, unreclaimed by discard accounting) — bounded by the
        // group cap and moot in practice: the store requires a reopen.
        let mut hard_failure = false;

        // ---- phase 1: place large values in the vlog --------------------
        let mut placed: Vec<(usize, u64, Vec<EncEntry>)> = Vec::with_capacity(accepted.len());
        let mut any_vlog = false;
        'place: for (gi, (i, base, events)) in accepted.iter().enumerate() {
            let (i, base) = (*i, *base);
            let mut entries = Vec::with_capacity(batches[i].0.len() + events.len());
            for op in batches[i].0.iter().chain(events.iter()) {
                let e = match op {
                    BatchOp::Put { key, value } => {
                        let repr = if value.len() >= self.opts.value_threshold {
                            any_vlog = true;
                            match self.vlog.append(key, value) {
                                Ok(ptr) => encode_ptr(ptr),
                                Err(e) => {
                                    let msg = e.to_string();
                                    self.set_bg_error(format!("vlog append failed: {msg}"));
                                    hard_failure = true;
                                    results[i] = Some(Err(e));
                                    fail_tail(&mut results, &accepted, gi + 1, &msg);
                                    break 'place;
                                }
                            }
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
            if results[i].is_none() {
                placed.push((i, base, entries));
            }
        }

        // ---- phase 2: durability ordering — payload before pointer ------
        // (vlog fsync before any WAL record referencing it becomes durable;
        // ONE sync for the whole group)
        if any_vlog && self.opts.sync == SyncMode::Always {
            if let Err(e) = self.vlog.sync_head() {
                let msg = e.to_string();
                self.set_bg_error(format!("vlog sync failed: {msg}"));
                let mut real = Some(e);
                for (i, _, _) in &placed {
                    results[*i] =
                        Some(Err(real.take().unwrap_or_else(|| aborted(&msg))));
                }
                return results.into_iter().map(|r| r.expect("filled")).collect();
            }
        }

        // ---- phase 3: WAL append, one record per batch -------------------
        let mut appended: Vec<(usize, u64, Vec<EncEntry>)> = Vec::with_capacity(placed.len());
        for (i, base, entries) in placed.into_iter() {
            let from_tail = |accepted: &[(usize, u64, Vec<BatchOp>)]| {
                accepted
                    .iter()
                    .position(|(j, _, _)| *j == i)
                    .map(|p| p + 1)
                    .unwrap_or(accepted.len())
            };
            let payload = encode_batch(base, &entries);
            if payload.len() as u64 >= 1 << 30 {
                // unreachable given the conservative phase-0 bound; kept as
                // belt-and-braces. Prefix-fail (not skip) so survivors stay
                // a contiguous seqno prefix, as documented.
                let msg = "write batch exceeds WAL record limit";
                results[i] = Some(Err(Error::InvalidArgument(msg.into())));
                fail_tail(&mut results, &accepted, from_tail(&accepted), msg);
                break;
            }
            if let Err(e) = ws.wal.append_record(&payload) {
                // the WAL tail is now in an unknown state (possibly a torn
                // record mid-file): degrade the store
                let msg = e.to_string();
                self.set_bg_error(format!("wal append failed: {msg}"));
                hard_failure = true;
                results[i] = Some(Err(e));
                fail_tail(&mut results, &accepted, from_tail(&accepted), &msg);
                break;
            }
            appended.push((i, base, entries));
        }

        // ---- phase 4: ONE WAL fsync for the whole group -------------------
        if self.opts.sync == SyncMode::Always && !appended.is_empty() {
            if let Err(e) = ws.wal.sync() {
                // gray zone (same as the old single-writer path): records
                // may or may not be durable; every appended batch reports
                // failure, nothing is published, and the store degrades —
                // continuing to ack writes ordered after unsynced records
                // would risk silent loss on recovery
                let msg = e.to_string();
                self.set_bg_error(format!("wal sync failed: {msg}"));
                let mut real = Some(e);
                for (i, _, _) in &appended {
                    results[*i] =
                        Some(Err(real.take().unwrap_or_else(|| aborted(&msg))));
                }
                return results.into_iter().map(|r| r.expect("filled")).collect();
            }
            self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        }

        // ---- phase 5: memtable inserts, then publish ---------------------
        if let Some((_, last_base, last_entries)) = appended.last() {
            let mem = self.state.read().mem.clone();
            for (i, base, entries) in &appended {
                for (k, e) in entries.iter().enumerate() {
                    mem.insert(make_ikey(&e.key, base + k as u64, e.kind), e.repr.clone());
                }
                results[*i] = Some(Ok(()));
            }
            self.visible_seqno.store(
                last_base + last_entries.len() as u64 - 1,
                Ordering::Release,
            );
            for (_, base, entries) in &appended {
                self.publish_stream(*base, entries);
            }
            // durable + published: wake the trigger runner if any surviving
            // batch enqueued events (spurious wakeups are harmless, so the
            // check is deliberately coarse)
            if accepted.iter().any(|(_, _, events)| !events.is_empty()) {
                self.notify_triggers();
            }

            // ---- phase 6: rotations, once per group ----------------------
            // skipped after a mid-group hard failure: rotating would seal a
            // WAL whose tail may hold a torn record, and sealed-WAL
            // corruption fails recovery permanently (an unsealed tail is
            // truncated cleanly on reopen)
            if !hard_failure {
                let mut rotate = || -> Result<()> {
                    if mem.approximate_bytes() >= self.opts.memtable_size {
                        self.rotate_memtable_locked(ws)?;
                        self.flush_signal.notify();
                    }
                    let (_, head_written, _) = self.vlog.head_state();
                    if head_written >= self.opts.vlog_file_size {
                        self.rotate_vlog_locked()?;
                    }
                    Ok(())
                };
                if let Err(e) = rotate() {
                    // every batch in the group is durable and published:
                    // their acks stand (flipping them to errors would invite
                    // duplicate retries of visible writes — the old path's
                    // false-negative wart, amplified by a group). The store
                    // degrades instead: subsequent writes are refused.
                    self.set_bg_error(format!("post-write rotation failed: {e}"));
                }
            }
        }

        results.into_iter().map(|r| r.expect("filled")).collect()
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
    /// forks). No-op when the memtable is empty.
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
                    TableBuilder::new(
                        file,
                        self.opts.block_size,
                        self.opts.bloom_bits_per_key,
                        self.opts.compression,
                    ),
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

impl Snapshot {
    /// The sequence number this snapshot reads at.
    pub fn seqno(&self) -> SeqNo {
        self.seq
    }
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
    /// Group commits led (leader critical sections on the batch path).
    pub commit_groups: u64,
    /// Batches committed through the group path; `commit_batches -
    /// commit_groups` is how many fsyncs group commit saved.
    pub commit_batches: u64,
    /// WAL fsyncs actually performed (SyncMode::Always only).
    pub wal_syncs: u64,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        Self::spawn_from_inner(open_inner(dir.as_ref(), opts)?)
    }

    /// Open with a caller-supplied IO backend, bypassing `opts.io_backend`.
    /// The **fault-injection test seam** (feature `fault-injection`, off by
    /// default): a custom `Io` can fail/short/corrupt `append`, `read_at`, and
    /// `sync_data` to drive durability-fault paths deterministically. Not for
    /// production use. Note the metadata plane (manifest `atomic_write`,
    /// directory fsyncs, fork hard-links) does not route through this trait.
    #[cfg(feature = "fault-injection")]
    pub fn open_with_io(
        dir: impl AsRef<Path>,
        opts: Options,
        io: Arc<dyn io::Io>,
        backend_name: &'static str,
    ) -> Result<Db> {
        Self::spawn_from_inner(open_inner_with(dir.as_ref(), opts, Some((io, backend_name)))?)
    }

    fn spawn_from_inner(inner: Arc<DbInner>) -> Result<Db> {
        let mut threads = Vec::new();
        {
            let i = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("fluent31-commit".into())
                    .spawn(move || commit_thread(i))
                    .expect("spawn commit thread"),
            );
        }
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
        #[cfg(feature = "wasm")]
        {
            let i = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("fluent31-trigger".into())
                    .spawn(move || crate::trigger::trigger_thread(i))
                    .expect("spawn trigger thread"),
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
        self.inner.write_batch(batch)
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

    /// The current visible sequence number — the address of the latest
    /// committed state. Capture it to cut deterministic forks of one
    /// version (`fork_at(name, seqno)` for the same seqno yields the same
    /// cut) without creating a pin first. Unlike [`pin`], this holds
    /// nothing: the point stays fork-able only while no later write moves
    /// the GC watermark past it — pin it to keep it fork-able.
    ///
    /// [`pin`]: Db::pin
    pub fn seqno(&self) -> SeqNo {
        self.inner.visible_seqno.load(Ordering::Acquire)
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

    // ------------------------------------------------------------ identity

    /// The store's identity (name, deterministic instance id, lineage), or
    /// `None` for an unnamed store. Fixed for the process lifetime.
    pub fn identity(&self) -> Option<StoreIdentity> {
        self.inner.identity.clone()
    }

    // --------------------------------------------------------- maintenance

    /// Durability barrier: every write acked before this call is durable
    /// on return (fsyncs the vlog head, then the WAL). The explicit
    /// companion to `SyncMode::Periodic`; harmless (one extra fsync) under
    /// the other modes.
    pub fn sync_wal(&self) -> Result<()> {
        self.inner.sync_wal()
    }

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
            commit_groups: inner.commit_groups.load(Ordering::Relaxed),
            commit_batches: inner.commit_batches.load(Ordering::Relaxed),
            wal_syncs: inner.wal_syncs.load(Ordering::Relaxed),
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

    /// Run a module's read-only `query` entry against the current visible
    /// state. Rejects modules that export no `query` entry point.
    #[cfg(feature = "wasm")]
    pub fn query(&self, name: &str, input: &[u8]) -> Result<Vec<u8>> {
        crate::wasm::query(&self.inner, name, input, None)
    }

    #[cfg(feature = "wasm")]
    pub fn query_at(&self, name: &str, input: &[u8], snap: &Snapshot) -> Result<Vec<u8>> {
        crate::wasm::query(&self.inner, name, input, Some(snap.seq))
    }

    /// Run a module's `execute` entry inside a transaction; commits on
    /// guest exit 0, retries automatically on conflict. Rejects modules
    /// that export no `execute` entry point.
    #[cfg(feature = "wasm")]
    pub fn execute(&self, name: &str, input: &[u8]) -> Result<Vec<u8>> {
        crate::wasm::execute(&self.inner, name, input)
    }

    /// The role entry points an installed module exports (any of `query`,
    /// `execute`, `on_touch`, `on_apply`) — which invocation paths accept
    /// it.
    #[cfg(feature = "wasm")]
    pub fn module_entries(&self, name: &str) -> Result<Vec<String>> {
        crate::wasm::module_entries(&self.inner, name)
    }

    /// Like [`Db::module_entries`], but for candidate module bytes that are
    /// not (yet) installed.
    #[cfg(feature = "wasm")]
    pub fn wasm_entries(&self, wasm: &[u8]) -> Result<Vec<String>> {
        crate::wasm::wasm_entries(&self.inner, wasm)
    }

    /// Run an installed module's optional `describe` export (read-only,
    /// empty input) and return its output. `Ok(None)` when the module does
    /// not export `describe`.
    #[cfg(feature = "wasm")]
    pub fn describe_module(&self, name: &str) -> Result<Option<Vec<u8>>> {
        crate::wasm::describe_module(&self.inner, name)
    }

    /// Like [`Db::describe_module`], but for candidate module bytes that
    /// are not (yet) installed — used to validate a declared schema before
    /// accepting an install.
    #[cfg(feature = "wasm")]
    pub fn describe_wasm(&self, wasm: &[u8]) -> Result<Option<Vec<u8>>> {
        crate::wasm::describe_wasm(&self.inner, wasm)
    }

    // ------------------------------------------------------------ triggers

    /// Register a write-range trigger: whenever a committed write touches a
    /// user key in `[lo, hi)` (None = unbounded end), the executor module
    /// `module` is asynchronously invoked. The module's exports pick the
    /// consumption mode, returned for confirmation: `on_apply` present →
    /// [`TriggerMode::Changes`], the ordered per-op change feed; `on_touch`
    /// → [`TriggerMode::Keys`], the coalesced touched keys; neither →
    /// rejected.
    /// Events are durable (they commit atomically with the triggering
    /// write) and consumption is exactly-once — see `Db::list_triggers` for
    /// backlog and failure visibility.
    #[cfg(feature = "wasm")]
    pub fn create_trigger(
        &self,
        name: &str,
        module: &str,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Result<crate::trigger::TriggerMode> {
        crate::trigger::create_trigger(&self.inner, name, module, lo, hi)
    }

    /// Unregister a trigger and discard its pending events.
    #[cfg(feature = "wasm")]
    pub fn delete_trigger(&self, name: &str) -> Result<()> {
        crate::trigger::delete_trigger(&self.inner, name)
    }

    /// Registered triggers with queue depth and last drain error.
    #[cfg(feature = "wasm")]
    pub fn list_triggers(&self) -> Result<Vec<crate::trigger::TriggerInfo>> {
        crate::trigger::list_triggers(&self.inner)
    }

    // -------------------------------------------------------- forks & pins

    /// The directory this database was opened at.
    pub fn path(&self) -> &std::path::Path {
        &self.inner.paths.dir
    }

    /// Fork from recent: cut at the current flushed head.
    pub fn fork(&self, name: &str) -> Result<ForkInfo> {
        crate::fork::create(self, name, None)
    }

    /// Fork from a specific point: cut at seqno `at`. The point must still
    /// be materializable — the current head, or a seqno held by a [`pin`]
    /// (or live snapshot); anything the GC watermark has passed is refused.
    ///
    /// [`pin`]: Db::pin
    pub fn fork_at(&self, name: &str, at: SeqNo) -> Result<ForkInfo> {
        crate::fork::create(self, name, Some(at))
    }

    pub fn list_forks(&self) -> Result<Vec<ForkInfo>> {
        crate::fork::list(&self.inner.paths)
    }

    pub fn delete_fork(&self, name: &str) -> Result<()> {
        crate::fork::delete(&self.inner.paths, name)
    }

    /// Create a durable named pin at the current visible seqno: a GC hold
    /// that survives restarts, guaranteeing the pinned state stays
    /// [`fork_at`]-able until [`unpin`]. Costs retention — versions and
    /// vlog values from the pinned seqno forward cannot be reclaimed while
    /// the pin exists (same as holding a snapshot open).
    ///
    /// [`fork_at`]: Db::fork_at
    /// [`unpin`]: Db::unpin
    pub fn pin(&self, name: &str) -> Result<PinInfo> {
        crate::fork::validate_name("pin", name)?;
        self.inner.check_bg_error()?;
        // hold GC first (in-memory), so the state at `seq` stays intact
        // while we make the pin durable
        let seq = self.inner.register_snapshot();
        // rollback guard: a pin is only real once the manifest carrying it
        // is durable; any failure before that must release the GC hold
        struct Hold<'a>(&'a DbInner, Option<SeqNo>);
        impl Drop for Hold<'_> {
            fn drop(&mut self) {
                if let Some(seq) = self.1.take() {
                    self.0.deregister_snapshot(seq);
                }
            }
        }
        let mut hold = Hold(&self.inner, Some(seq));

        // durability barrier: everything at-or-below `seq` must survive a
        // crash before the pin record does, or recovery could land below a
        // persisted pin (rejected at open as history loss)
        self.inner.sync_wal()?;

        let created_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let info = PinInfo {
            name: name.to_string(),
            seqno: seq,
            created_unix_ms,
        };
        let mut m = self.inner.manifest.lock();
        if m.data.pins.iter().any(|p| p.name == name) {
            return Err(Error::InvalidArgument(format!(
                "pin {name:?} already exists"
            )));
        }
        let mut data = m.data.clone();
        data.pins.push(info.clone());
        let gen = m.gen + 1;
        manifest::save(&self.inner.paths, gen, &data)?;
        m.gen = gen;
        m.data = data;
        hold.1 = None; // the pin owns the registration now
        Ok(info)
    }

    /// Delete a pin and release its GC hold.
    pub fn unpin(&self, name: &str) -> Result<()> {
        crate::fork::validate_name("pin", name)?;
        let seq = {
            let mut m = self.inner.manifest.lock();
            let Some(idx) = m.data.pins.iter().position(|p| p.name == name) else {
                return Err(Error::InvalidArgument(format!("no pin named {name:?}")));
            };
            let mut data = m.data.clone();
            let removed = data.pins.remove(idx);
            let gen = m.gen + 1;
            manifest::save(&self.inner.paths, gen, &data)?;
            m.gen = gen;
            m.data = data;
            removed.seqno
        };
        // release AFTER the removal is durable: a crash in between leaves a
        // conservative in-memory hold that the next open won't re-register
        self.inner.deregister_snapshot(seq);
        Ok(())
    }

    /// The store's durable pins, oldest first.
    pub fn pins(&self) -> Vec<PinInfo> {
        self.inner.manifest.lock().data.pins.clone()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.commit_signal.notify();
        self.inner.flush_signal.notify();
        self.inner.compact_signal.notify();
        self.inner.progress_signal.notify();
        #[cfg(feature = "wasm")]
        self.inner.triggers.signal.notify();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// The committer: the single owner of the grouped write path. Drains
/// everything queued, applies it chunked by the group byte cap (one WAL
/// fsync per chunk), delivers results, and immediately drains again — so
/// while an fsync is in flight, every active writer has time to enqueue,
/// and steady-state group size approaches the number of in-flight writers
/// (no leader election, no handoff gap).
fn commit_thread(db: Arc<DbInner>) {
    // In Periodic mode the write path never queues, so this thread is
    // purely the durability timer: fsync the WAL + vlog head every
    // `every`, bounding crash loss to roughly that window.
    let periodic = match db.opts.sync {
        SyncMode::Periodic { every } => Some(every.max(Duration::from_millis(1))),
        _ => None,
    };
    let mut last_sync = std::time::Instant::now();
    loop {
        if let Some(every) = periodic {
            if last_sync.elapsed() >= every {
                // failure degrades the store; the loop keeps running so
                // shutdown still drains and joins cleanly
                let _ = db.sync_wal();
                last_sync = std::time::Instant::now();
            }
        }
        let drained: Vec<Arc<CommitWaiter>> = {
            let mut q = db.commit_queue.lock();
            q.drain(..).collect()
        };
        if drained.is_empty() {
            if db.shutdown.load(Ordering::Acquire) {
                // Periodic: one final barrier so a clean close loses nothing
                if periodic.is_some() {
                    let _ = db.sync_wal();
                }
                return;
            }
            let idle = periodic
                .map(|every| every.saturating_sub(last_sync.elapsed()).max(Duration::from_millis(1)))
                .unwrap_or(Duration::from_millis(100))
                .min(Duration::from_millis(100));
            db.commit_signal.wait_timeout(idle);
            continue;
        }

        let mut start = 0;
        while start < drained.len() {
            // chunk by the group byte cap (LevelDB heuristic) so one huge
            // batch can't hold hostage the latency of small neighbors
            let first_bytes = drained[start].inner.lock().bytes;
            let cap = group_byte_cap(first_bytes);
            let mut end = start + 1;
            let mut total = first_bytes;
            while end < drained.len() {
                let b = drained[end].inner.lock().bytes;
                if total + b > cap {
                    break;
                }
                total += b;
                end += 1;
            }
            let group = &drained[start..end];

            // degradation gate, re-checked per chunk: honors set_bg_error's
            // "writes refused until reopen" contract and stops a chunk from
            // appending into a WAL whose tail a previous chunk's sync
            // failure may have left torn
            if let Some(msg) = db.bg_error.lock().clone() {
                for w in group {
                    let mut g = w.inner.lock();
                    g.result = Some(Err(Error::Background(msg.clone())));
                    drop(g);
                    w.cv.notify_one();
                }
                start = end;
                continue;
            }

            let mut guard = GroupPanicGuard {
                db: &db,
                group,
                armed: true,
            };
            let entries: Vec<QueuedEntry> = group
                .iter()
                .map(|w| {
                    let mut g = w.inner.lock();
                    (
                        g.ops.take().expect("ops present"),
                        g.validate.take(),
                        g.capture,
                    )
                })
                .collect();

            // OCC validation + apply share ONE write_mu critical section:
            // exactly the atomicity Txn::commit had when it held the mutex
            // itself, extended with an in-group written-keys check so two
            // conflicting transactions in the same chunk cannot both pass
            // (the earlier one's writes are not in the view yet).
            let mut results: Vec<Option<Result<()>>> = Vec::new();
            results.resize_with(group.len(), || None);
            {
                let mut ws = db.write_mu.lock();
                let view = db.read_view();
                let mut written: std::collections::HashSet<&[u8]> =
                    std::collections::HashSet::new();
                let mut included: Vec<usize> = Vec::with_capacity(entries.len());
                for (i, (ops, validate, _)) in entries.iter().enumerate() {
                    if let Some((snap, keys)) = validate {
                        let conflict = keys.iter().try_fold(false, |hit, key| {
                            if hit || written.contains(key.as_slice()) {
                                return Ok::<bool, Error>(true);
                            }
                            Ok(match view.latest(key)? {
                                Some((seq, _)) => seq > *snap,
                                None => false,
                            })
                        });
                        match conflict {
                            Ok(false) => {}
                            Ok(true) => {
                                results[i] = Some(Err(Error::Conflict));
                                continue;
                            }
                            Err(e) => {
                                results[i] = Some(Err(e));
                                continue;
                            }
                        }
                    }
                    for op in ops {
                        written.insert(match op {
                            BatchOp::Put { key, .. } => key.as_slice(),
                            BatchOp::Delete { key } => key.as_slice(),
                        });
                    }
                    // a locks-only transaction validates but applies nothing
                    if !ops.is_empty() {
                        included.push(i);
                    } else {
                        results[i] = Some(Ok(()));
                    }
                }
                let refs: Vec<(&[BatchOp], bool)> = included
                    .iter()
                    .map(|&i| (entries[i].0.as_slice(), entries[i].2))
                    .collect();
                if !refs.is_empty() {
                    let applied = db.apply_group_locked(&mut ws, &refs);
                    for (&i, r) in included.iter().zip(applied) {
                        results[i] = Some(r);
                    }
                }
            }
            db.commit_groups.fetch_add(1, Ordering::Relaxed);
            db.commit_batches.fetch_add(group.len() as u64, Ordering::Relaxed);
            for (w, r) in group.iter().zip(results) {
                let mut g = w.inner.lock();
                g.result = Some(r.expect("every group entry resolved"));
                drop(g);
                w.cv.notify_one();
            }
            guard.armed = false;
            start = end;
        }
    }
}

fn flush_thread(db: Arc<DbInner>) {
    while !db.shutdown.load(Ordering::Acquire) {
        match db.flush_one() {
            Ok(true) => continue,
            Ok(false) => db.flush_signal.wait_timeout(Duration::from_millis(200)),
            Err(e) => {
                // set_bg_error releases the lock immediately: holding it
                // across the backoff would stall every bg_error poller
                db.set_bg_error(format!("flush failed: {e}"));
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
                db.set_bg_error(format!("compaction failed: {e}"));
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
    open_inner_with(dir, opts, None)
}

/// `open_inner` with an optional pre-built IO backend. `None` resolves the
/// backend from `opts.io_backend`; `Some` injects a caller-provided one (the
/// feature-gated fault-injection seam — see `Db::open_with_io`).
fn open_inner_with(
    dir: &Path,
    opts: Options,
    io_override: Option<(Arc<dyn io::Io>, &'static str)>,
) -> Result<Arc<DbInner>> {
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

    let (io_backend, backend_name) = match io_override {
        Some(pair) => pair,
        None => io::backend(opts.io_backend)?,
    };
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

    // ---- store identity: verify / adopt / mint a pending fork --------------
    // (persisted by the open-completion manifest write below; deterministic
    // derivation makes a crash before that write harmless — the next open
    // re-mints the exact same id)
    resolve_identity(&mut mdata, opts.store_name.as_deref())?;

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
        identity: mdata.identity.clone(),
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
        commit_queue: Mutex::new(std::collections::VecDeque::new()),
        commit_groups: AtomicU64::new(0),
        commit_batches: AtomicU64::new(0),
        wal_syncs: AtomicU64::new(0),
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
        gc_sampled_at: Mutex::new(std::collections::HashMap::new()),
        compaction_mu: Mutex::new(()),
        subs: Mutex::new(Vec::new()),
        subs_active: AtomicBool::new(false),
        shutdown: AtomicBool::new(false),
        commit_signal: Signal::new(),
        flush_signal: Signal::new(),
        compact_signal: Signal::new(),
        progress_signal: Signal::new(),
        bg_error: Mutex::new(None),
        #[cfg(feature = "wasm")]
        wasm,
        #[cfg(feature = "wasm")]
        triggers: crate::trigger::TriggerState::new(),
        _lock_file: lock_file,
    });

    // Re-register durable pins as snapshots BEFORE any background thread
    // exists (they spawn after open returns), so no GC pass can ever run
    // without the pins' holds in place. A pin above the recovered head is
    // real history loss — pin() persists only after a durability barrier —
    // so refuse to open rather than silently serve a hole.
    {
        let m = inner.manifest.lock();
        for pin in &m.data.pins {
            if pin.seqno > max_seq {
                return Err(corrupt(format!(
                    "pin {:?} at seqno {} is above the recovered head {max_seq}",
                    pin.name, pin.seqno
                )));
            }
            inner.register_snapshot_at(pin.seqno);
        }
    }

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

    // sweep fork builds that crashed mid-creation (we hold the LOCK,
    // so nothing can be legitimately building right now)
    if let Ok(rd) = std::fs::read_dir(inner.paths.archive_root()) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".tmp-") {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    // recovered state is fully readable from here: load persisted trigger
    // definitions so capture is active before the first user write
    #[cfg(feature = "wasm")]
    crate::trigger::load_registry(&inner)?;

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
        identity: None,
        pending_fork: None,
        pins: Vec::new(),
    };
    manifest::save(paths, 1, &data)?;
    Ok(())
}

/// Resolve the store identity at open: verify a persisted name against
/// `Options::store_name`, adopt a name onto an unnamed store, or mint a
/// pending fork (first read-write open of a fork archive or restored
/// copy). See identity.rs for why derivation being deterministic makes this
/// crash-safe without any extra commit ordering.
fn resolve_identity(mdata: &mut ManifestData, requested: Option<&str>) -> Result<()> {
    if let Some(name) = requested {
        crate::identity::validate_store_name(name)?;
    }
    if let Some(pf) = mdata.pending_fork.take() {
        // the fork name was fixed at fork/restore time; an explicit
        // store_name must agree with it
        if requested.is_some_and(|r| r != pf.name) {
            return Err(Error::InvalidArgument(format!(
                "store_name {:?} conflicts with fork name {:?} fixed at fork/restore",
                requested.unwrap_or_default(),
                pf.name
            )));
        }
        mdata.identity = Some(pf.mint());
        return Ok(());
    }
    match (&mdata.identity, requested) {
        (Some(id), Some(req)) if id.name != req => Err(Error::InvalidArgument(format!(
            "store is named {:?}; store_name {req:?} does not match",
            id.name
        ))),
        (None, Some(req)) => {
            // adoption: an existing unnamed store gains its identity once
            mdata.identity = Some(StoreIdentity::root(req));
            Ok(())
        }
        _ => Ok(()),
    }
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

/// Group byte cap, LevelDB's heuristic: 1 MiB max, but when the front
/// batch is small, front + 128 KiB so small writes aren't delayed by huge
/// neighbors. A batch larger than the cap always commits (alone): the
/// leader unconditionally includes the front.
fn group_byte_cap(first_bytes: usize) -> usize {
    if first_bytes <= 128 << 10 {
        first_bytes + (128 << 10)
    } else {
        1 << 20
    }
}

// ---------------------------------------------------------------------------
// Replication surface: range subscriptions (stream tap) + scoped slices
// ---------------------------------------------------------------------------
//
// The master side of edge replication. A `Subscription` delivers committed
// writes in a key range, values resolved; `slice_manifest` +
// `read_table_chunk`/`read_vlog_chunk` let a replica copy the scoped part
// of the flushed tree. Files are NOT pinned for slice readers (a stale
// reference answers `Error::Gone` and the replica re-pulls); the one pin a
// subscription holds is a registered snapshot that advances as batches are
// consumed, which keeps streamed vlog pointers resolvable against GC: a
// victim retired at seqno S can only be deleted once the watermark passes
// S, and the pin sits at-or-below the oldest unconsumed entry's seqno.

/// One committed op as published to subscribers (repr unresolved — pointer
/// resolution happens at consume time, off the write path).
struct RawEntry {
    key: Vec<u8>,
    seqno: SeqNo,
    kind: ValueKind,
    repr: Vec<u8>,
}

struct SubQueue {
    entries: std::collections::VecDeque<RawEntry>,
    bytes: usize,
}

/// Publisher-facing half of a subscription, registered in `DbInner::subs`.
pub(crate) struct SubShared {
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    max_bytes: usize,
    queue: Mutex<SubQueue>,
    signal: Signal,
    /// Set on queue overflow (subscriber lagged) or consumer drop; the
    /// publisher prunes flagged subscriptions on its next pass.
    dropped: AtomicBool,
}

impl SubShared {
    fn in_range(&self, key: &[u8]) -> bool {
        key >= self.lo.as_slice() && self.hi.as_deref().is_none_or(|h| key < h)
    }

    /// Called under `write_mu`, right after the batch became visible.
    fn offer(&self, base: SeqNo, entries: &[EncEntry]) {
        let mut pushed = false;
        let mut q = self.queue.lock();
        for (i, e) in entries.iter().enumerate() {
            if !self.in_range(&e.key) {
                continue;
            }
            q.bytes += e.key.len() + e.repr.len() + 32;
            q.entries.push_back(RawEntry {
                key: e.key.clone(),
                seqno: base + i as u64,
                kind: e.kind,
                repr: e.repr.clone(),
            });
            pushed = true;
        }
        if q.bytes > self.max_bytes {
            // lag policy: cut the subscriber loose, never stall the writer —
            // a gapped stream is useless, so drop everything buffered too
            q.entries.clear();
            q.bytes = 0;
            self.dropped.store(true, Ordering::Release);
        }
        drop(q);
        if pushed {
            self.signal.notify();
        }
    }
}

impl DbInner {
    /// Fan a committed batch out to stream subscribers. Called under
    /// `write_mu` in both apply paths, immediately after `visible_seqno`
    /// publication — subscribers therefore observe batches in seqno order
    /// with no gaps past their start point.
    pub(crate) fn publish_stream(&self, base: SeqNo, entries: &[EncEntry]) {
        if !self.subs_active.load(Ordering::Acquire) {
            return;
        }
        let mut subs = self.subs.lock();
        subs.retain(|s| !s.dropped.load(Ordering::Acquire));
        for s in subs.iter() {
            s.offer(base, entries);
        }
        self.subs_active.store(!subs.is_empty(), Ordering::Release);
    }
}

/// One streamed op, value resolved (`None` for deletes).
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub key: Vec<u8>,
    pub seqno: SeqNo,
    pub kind: ValueKind,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum StreamEvent {
    /// Committed in-range ops, seqno-ascending, gap-free past
    /// `Subscription::start_seqno`.
    Batch(Vec<StreamEntry>),
    /// The subscriber fell behind its queue cap and was cut off; its view
    /// now has a gap — a fresh subscribe + slice pull is required.
    Lagged,
}

/// Consumer half of a range subscription (see `Db::subscribe`).
pub struct Subscription {
    db: Arc<DbInner>,
    shared: Arc<SubShared>,
    start: SeqNo,
    pinned: SeqNo,
}

impl Subscription {
    /// Every committed write with seqno strictly above this flows through
    /// the subscription. Pair with a slice pull (whose `flushed_seqno` is
    /// at least this, guaranteed by subscribing FIRST) for gap-free attach.
    pub fn start_seqno(&self) -> SeqNo {
        self.start
    }

    /// Next batch of streamed entries; `Ok(None)` on timeout. After
    /// `StreamEvent::Lagged` (or any `Err`) the stream is dead and the
    /// consumer must re-sync from a fresh subscription.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<StreamEvent>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.shared.dropped.load(Ordering::Acquire) {
                return Ok(Some(StreamEvent::Lagged));
            }
            self.db.check_bg_error()?;
            let raws: Vec<RawEntry> = {
                let mut q = self.shared.queue.lock();
                q.bytes = 0;
                q.entries.drain(..).collect()
            };
            if !raws.is_empty() {
                return Ok(Some(StreamEvent::Batch(self.resolve(raws)?)));
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            self.shared.signal.wait_timeout(deadline - now);
        }
    }

    fn resolve(&mut self, raws: Vec<RawEntry>) -> Result<Vec<StreamEntry>> {
        let view = self.db.read_view();
        let last = raws.last().map(|r| r.seqno).expect("non-empty batch");
        let mut out = Vec::with_capacity(raws.len());
        for r in raws {
            let value = match r.kind {
                ValueKind::Put => match self.db.resolve_repr(&view, &r.key, &r.repr) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        // the pin makes this unreachable short of real
                        // corruption; kill the stream so the consumer
                        // re-syncs instead of silently missing entries
                        self.shared.dropped.store(true, Ordering::Release);
                        return Err(e);
                    }
                },
                ValueKind::Delete => None,
            };
            out.push(StreamEntry {
                key: r.key,
                seqno: r.seqno,
                kind: r.kind,
                value,
            });
        }
        // advance the GC pin: everything <= last is resolved and delivered.
        // Register the new pin before releasing the old one so the
        // watermark can never leap past both.
        self.db.register_snapshot_at(last);
        self.db.deregister_snapshot(self.pinned);
        self.pinned = last;
        Ok(out)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.shared.dropped.store(true, Ordering::Release);
        let mut subs = self.db.subs.lock();
        subs.retain(|s| !Arc::ptr_eq(s, &self.shared));
        self.db.subs_active.store(!subs.is_empty(), Ordering::Release);
        drop(subs);
        self.db.deregister_snapshot(self.pinned);
    }
}

/// A table fragment as advertised to replicas: id + key bounds + size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceTable {
    pub id: u64,
    pub size: u64,
    pub min_ukey: Vec<u8>,
    pub max_ukey: Vec<u8>,
}

/// A run restricted to the fragments overlapping the requested range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceRun {
    pub id: u64,
    pub tables: Vec<SliceTable>,
}

/// Scoped snapshot of the flushed tree (levels hold runs newest-first,
/// fragments key-ordered — same shape as the live version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceManifest {
    pub flushed_seqno: SeqNo,
    pub levels: Vec<Vec<SliceRun>>,
}

impl Db {
    /// Subscribe to committed writes in `[lo, hi)` (`hi = None` →
    /// unbounded; `lo` is clamped to the user keyspace). Installation
    /// holds the write mutex, so delivery is gap-free past
    /// `start_seqno()`. The subscription's advancing snapshot pin delays
    /// vlog deletion while entries are in flight — an abandoned but
    /// undropped subscription holds that pin, so drop what you stop
    /// consuming.
    pub fn subscribe(&self, lo: &[u8], hi: Option<&[u8]>) -> Result<Subscription> {
        let lo = if lo >= USER_KEYSPACE_START {
            lo.to_vec()
        } else {
            USER_KEYSPACE_START.to_vec()
        };
        if let Some(h) = hi {
            if h <= lo.as_slice() {
                return Err(Error::InvalidArgument(
                    "subscription range [lo, hi) is empty".into(),
                ));
            }
        }
        let inner = self.inner.clone();
        inner.check_bg_error()?;
        let shared = Arc::new(SubShared {
            lo,
            hi: hi.map(|h| h.to_vec()),
            max_bytes: inner.opts.sub_queue_bytes,
            queue: Mutex::new(SubQueue {
                entries: std::collections::VecDeque::new(),
                bytes: 0,
            }),
            signal: Signal::new(),
            dropped: AtomicBool::new(false),
        });
        let start = {
            // excluding writers makes install atomic: no write can commit
            // between the seqno read and the registry insert
            let _ws = inner.write_mu.lock();
            let v0 = inner.visible_seqno.load(Ordering::Acquire);
            inner.register_snapshot_at(v0);
            let mut subs = inner.subs.lock();
            subs.push(shared.clone());
            inner.subs_active.store(true, Ordering::Release);
            v0
        };
        Ok(Subscription {
            db: inner,
            shared,
            start,
            pinned: start,
        })
    }

    /// Scoped snapshot of the flushed tree: per run, only the fragments
    /// overlapping `[lo, hi)`. Flushes first, so every write acked before
    /// this call is covered — subscribe FIRST, then pull the slice, and
    /// the (slice, stream) pair covers everything exactly (overlap is
    /// harmless: entries carry seqnos).
    pub fn slice_manifest(&self, lo: &[u8], hi: Option<&[u8]>) -> Result<SliceManifest> {
        self.flush()?;
        let inner = &self.inner;
        let (flushed, view) = {
            let m = inner.manifest.lock();
            (m.data.last_flushed_seqno, inner.read_view())
        };
        let levels = view
            .version
            .levels
            .iter()
            .map(|runs| {
                runs.iter()
                    .filter_map(|r| {
                        let tables: Vec<SliceTable> = r
                            .tables
                            .iter()
                            .filter(|t| {
                                t.table.stats.max_ukey() >= lo
                                    && hi.is_none_or(|h| t.table.stats.min_ukey() < h)
                            })
                            .map(|t| SliceTable {
                                id: t.id,
                                size: t.size,
                                min_ukey: t.table.stats.min_ukey().to_vec(),
                                max_ukey: t.table.stats.max_ukey().to_vec(),
                            })
                            .collect();
                        (!tables.is_empty()).then_some(SliceRun { id: r.id, tables })
                    })
                    .collect()
            })
            .collect();
        Ok(SliceManifest {
            flushed_seqno: flushed,
            levels,
        })
    }

    /// Raw bytes of a table fragment still referenced by the live version;
    /// `Error::Gone` once compaction dropped it (replica: re-pull the
    /// slice). Nothing is pinned between calls — by design, a replica
    /// never burdens the master's file lifecycle.
    pub fn read_table_chunk(&self, table_id: u64, off: u64, len: usize) -> Result<Vec<u8>> {
        let handle = {
            let s = self.inner.state.read();
            let found = s
                .version
                .runs_newest_first()
                .flat_map(|r| r.tables.iter())
                .find(|t| t.id == table_id)
                .cloned();
            found
        };
        match handle {
            Some(t) => t.table.read_chunk(off, len),
            None => Err(Error::Gone(format!(
                "table {table_id} left the live version"
            ))),
        }
    }

    /// Raw bytes of a vlog record region. Retired-but-gated victims still
    /// resolve (they stay in the version map until their deletion gates
    /// pass); a deleted file is `Gone`.
    pub fn read_vlog_chunk(&self, file_id: u64, off: u64, len: usize) -> Result<Vec<u8>> {
        let handle = {
            let s = self.inner.state.read();
            s.version.vlogs.get(&file_id).cloned()
        };
        let Some(h) = handle else {
            return Err(Error::Gone(format!("vlog {file_id} left the live version")));
        };
        let flen = h.file.len()?;
        if off >= flen {
            return Err(Error::Gone(format!(
                "vlog {file_id} offset {off} beyond end {flen}"
            )));
        }
        let n = (len as u64).min(flen - off) as usize;
        let mut buf = vec![0u8; n];
        h.file.read_exact_at(off, &mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use crate::batch::WriteBatch;

    #[test]
    fn group_cap_matches_leveldb_heuristic() {
        assert_eq!(group_byte_cap(0), 128 << 10);
        assert_eq!(group_byte_cap(100), 100 + (128 << 10));
        assert_eq!(group_byte_cap(128 << 10), (128 << 10) * 2);
        assert_eq!(group_byte_cap((128 << 10) + 1), 1 << 20);
        // a >1MiB front is not capped out of its own group: the leader
        // always includes the front, the cap only stops ADDING neighbors
        assert_eq!(group_byte_cap(2 << 20), 1 << 20);
    }

    /// Two conflicting transactions forced into the SAME fsync group: the
    /// in-group written-set check must fail exactly one (the store view
    /// alone cannot see the earlier one's uncommitted writes).
    #[test]
    fn conflicting_txns_in_one_group_cannot_both_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            crate::Db::open(
                dir.path(),
                Options {
                    sync: SyncMode::Always,
                    ..Options::default()
                },
            )
            .unwrap(),
        );
        db.put("k", 0u64.to_le_bytes().to_vec()).unwrap();

        let mut t1 = db.begin();
        let mut t2 = db.begin();
        for t in [&mut t1, &mut t2] {
            let v = t.get_for_update(b"k").unwrap().unwrap();
            let n = u64::from_le_bytes(v[..8].try_into().unwrap());
            t.put("k", (n + 1).to_le_bytes().to_vec()).unwrap();
        }

        // hold write_mu so both commits land in one committer chunk
        let ws = db.inner.write_mu.lock();
        let h1 = std::thread::spawn(move || t1.commit());
        let h2 = std::thread::spawn(move || t2.commit());
        std::thread::sleep(Duration::from_millis(200));
        drop(ws);

        let (r1, r2) = (h1.join().unwrap(), h2.join().unwrap());
        let ok = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
        assert_eq!(ok, 1, "exactly one may commit: {r1:?} vs {r2:?}");
        let v = db.get(b"k").unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(v[..8].try_into().unwrap()), 1);
    }

    /// A plain batch write queued ahead of a transaction in the same group
    /// conflicts it via the in-group written set.
    #[test]
    fn plain_write_in_same_group_conflicts_later_txn() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            crate::Db::open(
                dir.path(),
                Options {
                    sync: SyncMode::Always,
                    ..Options::default()
                },
            )
            .unwrap(),
        );
        db.put("k", "orig").unwrap();

        let mut txn = db.begin();
        let _ = txn.get_for_update(b"k").unwrap();
        txn.put("k", "from-txn").unwrap();

        let ws = db.inner.write_mu.lock();
        let db1 = db.clone();
        let put = std::thread::spawn(move || db1.put("k", "from-put"));
        std::thread::sleep(Duration::from_millis(100)); // put enqueues first
        let commit = std::thread::spawn(move || txn.commit());
        std::thread::sleep(Duration::from_millis(100));
        drop(ws);

        put.join().unwrap().unwrap();
        let r = commit.join().unwrap();
        assert!(matches!(r, Err(Error::Conflict)), "{r:?}");
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"from-put".as_ref()));
    }

    /// A writer whose batch was already drained by the committer must NOT
    /// bail on a bg_error poll: bailing would report failure for a write
    /// that still commits. It must wait for the real result — and result /
    /// visible state must always agree.
    #[test]
    fn parked_writer_never_gets_false_error_for_an_inflight_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            crate::Db::open(
                dir.path(),
                Options {
                    sync: SyncMode::Always,
                    ..Options::default()
                },
            )
            .unwrap(),
        );

        // stall the committer mid-cycle: it drains the queue, then blocks
        // on write_mu (held here) with the batch already in hand
        let ws = db.inner.write_mu.lock();
        let db2 = db.clone();
        let writer = std::thread::spawn(move || db2.put("inflight", "v"));
        std::thread::sleep(Duration::from_millis(200)); // drained + blocked

        // store degrades while the batch is in flight (mimics a flush
        // failure): the writer's poll fires but must keep waiting — its
        // batch is no longer in the queue
        db.inner.set_bg_error("test: simulated flush failure");
        std::thread::sleep(Duration::from_millis(250)); // several polls

        drop(ws); // committer proceeds
        let result = writer.join().unwrap();
        let present = db.inner.get_at_seq(b"inflight", MAX_SEQNO).unwrap().is_some();
        // either outcome is legal here (the committer's degradation gate
        // may or may not have seen bg_error before this chunk) — but ack
        // and state must AGREE:
        assert_eq!(
            result.is_ok(),
            present,
            "ack and visible state must agree: result={result:?} present={present}"
        );
    }

    /// A writer whose batch is still QUEUED when the store degrades pulls
    /// it back out and errors truthfully: the batch must never be applied
    /// afterwards.
    #[test]
    fn queued_writer_bails_truthfully_when_store_degrades() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            crate::Db::open(
                dir.path(),
                Options {
                    sync: SyncMode::Always,
                    ..Options::default()
                },
            )
            .unwrap(),
        );

        // first batch: drained by the committer, stuck behind write_mu
        let ws = db.inner.write_mu.lock();
        let db2 = db.clone();
        let first = std::thread::spawn(move || db2.put("first", "v"));
        std::thread::sleep(Duration::from_millis(150));

        // second batch: enqueued but NOT yet drained (committer is stuck)
        let db3 = db.clone();
        let second = std::thread::spawn(move || db3.put("second", "v"));
        std::thread::sleep(Duration::from_millis(100));

        db.inner.set_bg_error("test: degraded");
        // second's poll (<=100ms later) finds itself still queued: removes
        // itself and errors; the committer never sees its batch
        let second_res = second.join().unwrap();
        assert!(
            matches!(second_res, Err(Error::Background(_))),
            "{second_res:?}"
        );

        drop(ws);
        let first_res = first.join().unwrap();
        let first_present = db.inner.get_at_seq(b"first", MAX_SEQNO).unwrap().is_some();
        assert_eq!(first_res.is_ok(), first_present, "first: {first_res:?}");
        assert!(
            db.inner.get_at_seq(b"second", MAX_SEQNO).unwrap().is_none(),
            "a truthfully-errored batch must never be applied"
        );
    }

    /// Phase-0 validation failures are batch-local: a mid-group reject
    /// consumes no seqnos, later batches proceed, and every batch gets
    /// exactly one result.
    #[test]
    fn group_validation_failure_is_batch_local_and_results_complete() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .unwrap();
        let inner = &db.inner;

        // park visible_seqno so close to MAX_SEQNO that a 3-op batch fails
        // the seqno-space check while 1-op batches still fit
        inner
            .visible_seqno
            .store(MAX_SEQNO - 4, Ordering::Release);

        let mk = |n: usize| -> Vec<BatchOp> {
            let mut b = WriteBatch::new();
            for i in 0..n {
                b.put(format!("k{n}-{i}"), "v");
            }
            b.ops
        };
        let one_a = mk(1);
        let three = mk(3);
        let one_b = mk(1);

        let mut ws = inner.write_mu.lock();
        let results = inner.apply_group_locked(
            &mut ws,
            &[
                (one_a.as_slice(), false),
                (three.as_slice(), false),
                (one_b.as_slice(), false),
            ],
        );
        drop(ws);

        assert_eq!(results.len(), 3, "one result per batch, always");
        assert!(results[0].is_ok(), "{:?}", results[0]);
        assert!(
            matches!(&results[1], Err(Error::InvalidArgument(m)) if m.contains("seqno")),
            "{:?}",
            results[1]
        );
        assert!(results[2].is_ok(), "validation skip is batch-local: {:?}", results[2]);
        // the skipped batch consumed no seqnos: exactly 2 were used
        assert_eq!(
            inner.visible_seqno.load(Ordering::Acquire),
            MAX_SEQNO - 2
        );
    }
}
