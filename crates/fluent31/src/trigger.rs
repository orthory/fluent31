//! Write-range triggers: durable, async fan-out from committed writes to
//! WASM executor modules — the schema-free "custom index creator".
//!
//! Registration (`Db::create_trigger`) binds an installed executor module
//! to a user-key range `[lo, hi)`. When a committed write touches the
//! range, a compact event record — keyed by the touched user key itself —
//! is appended to the trigger's durable queue *in the same atomic batch*
//! as the write (same WAL record, same crash atomicity). A background
//! runner drains each queue by invoking the module with the batch of
//! touched keys as input and deleting the consumed queue entries inside
//! the module's own transaction: the module's writes and the queue
//! consumption commit together or not at all. Invocation is therefore
//! at-least-once; effects are exactly-once.
//!
//! A trigger consumes committed writes in one of two modes, auto-detected
//! at registration from the module's exports (`on_apply` present → changes
//! mode; `on_touch` → keys mode):
//!
//! **Keys mode** (module entry `on_touch`):
//!
//! - An event means "this key was touched", NOT "here is the op that
//!   touched it". Queue keys are the touched user keys, so a hot key
//!   coalesces to one pending event, and a module must reconcile against
//!   CURRENT state (read the key: present = upsert, absent = remove) —
//!   which makes trigger effects convergent and order-independent.
//! - A re-touch racing a drain is caught by OCC: the queue-entry delete
//!   sits in the transaction's write set, so a newer event record on the
//!   same key conflicts the commit and the drain re-runs against a fresh
//!   snapshot.
//!
//! **Changes mode** (module entry `on_apply`): the post-apply change feed.
//!
//! - One event per committed op — no coalescing. The queue key is the op's
//!   commit seqno and the record value carries the change itself (kind,
//!   key, and the written value up to `trigger_inline_value`; larger
//!   values are elided and read on demand). The module receives the
//!   ordered "list of changes committed", filters it in code, and its
//!   effects are exactly-once per change.
//! - Capture happens inside the commit critical section, so event order
//!   IS commit order — the feed never misrepresents which write won a key.
//! - Drains never conflict with capture (new events always take fresh
//!   seqno keys), so a busy range cannot starve its own feed.
//!
//! Common to both modes:
//!
//! - No stacking: a trigger invocation commits through a *system*
//!   transaction whose writes never generate events, so trigger graphs
//!   cannot cascade or loop.
//! - Value-log GC relocations and other engine-internal rewrites bypass
//!   capture entirely (they change placement, not logical state).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::batch::{BatchOp, WriteBatch};
use crate::coding::{put_len_prefixed, put_uvarint, Reader};
use crate::config::Options;
use crate::db::{DbInner, Signal};
use crate::error::{corrupt, Error, Result};
use crate::types::{
    sys_trigger_change_key, sys_trigger_event_key, sys_trigger_key, validate_user_key, SeqNo,
    SYS_PREFIX,
};

/// `\x00trg\x00` — trigger definition records.
const DEF_PREFIX: &[u8] = &[SYS_PREFIX, b't', b'r', b'g', 0x00];
/// `\x00trgq\x00` — pending-event queue space (all triggers).
const QUEUE_PREFIX: &[u8] = &[SYS_PREFIX, b't', b'r', b'g', b'q', 0x00];

fn def_space_end() -> Vec<u8> {
    let mut end = DEF_PREFIX.to_vec();
    *end.last_mut().unwrap() = 0x01;
    end
}

fn queue_space_end() -> Vec<u8> {
    let mut end = QUEUE_PREFIX.to_vec();
    *end.last_mut().unwrap() = 0x01;
    end
}

/// `\x00trgq\x00<name>\x00` — one trigger's queue.
fn queue_prefix(name: &str) -> Vec<u8> {
    sys_trigger_event_key(name, b"")
}

fn queue_prefix_end(name: &str) -> Vec<u8> {
    let mut end = queue_prefix(name);
    *end.last_mut().unwrap() = 0x01;
    end
}

/// Split a queue key into (trigger name, touched user key).
fn parse_event_key(key: &[u8]) -> Option<(&str, &[u8])> {
    let rest = key.strip_prefix(QUEUE_PREFIX)?;
    let sep = rest.iter().position(|&b| b == 0x00)?;
    let name = std::str::from_utf8(&rest[..sep]).ok()?;
    Some((name, &rest[sep + 1..]))
}

/// How a trigger consumes committed writes (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerMode {
    /// Coalesced touched-key events delivered to the module's `on_touch`.
    Keys,
    /// Ordered per-op change events delivered to the module's `on_apply`.
    Changes,
}

impl TriggerMode {
    /// The module export a drain invokes for this mode.
    pub fn entry(self) -> &'static str {
        match self {
            TriggerMode::Keys => "on_touch",
            TriggerMode::Changes => "on_apply",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TriggerMode::Keys => "keys",
            TriggerMode::Changes => "changes",
        }
    }
}

/// A registered trigger, as listed by `Db::list_triggers`.
#[derive(Debug, Clone)]
pub struct TriggerInfo {
    pub name: String,
    pub module: String,
    /// Inclusive range start; empty = from the start of the user keyspace.
    pub lo: Vec<u8>,
    /// Exclusive range end; empty = unbounded.
    pub hi: Vec<u8>,
    pub mode: TriggerMode,
    /// Queued events not yet consumed by a successful invocation.
    pub pending: u64,
    /// Why the most recent drain attempt failed; None when healthy.
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerDef {
    pub name: String,
    pub module: String,
    pub lo: Vec<u8>,
    pub hi: Vec<u8>,
    pub mode: TriggerMode,
}

impl TriggerDef {
    fn matches(&self, key: &[u8]) -> bool {
        (self.lo.is_empty() || key >= self.lo.as_slice())
            && (self.hi.is_empty() || key < self.hi.as_slice())
    }
}

fn encode_def(d: &TriggerDef) -> Vec<u8> {
    // keys-mode defs keep the v1 layout byte-for-byte, so a store that
    // never uses changes mode stays readable by older binaries
    let mut out = match d.mode {
        TriggerMode::Keys => vec![1u8],
        TriggerMode::Changes => vec![2u8],
    };
    put_len_prefixed(&mut out, d.name.as_bytes());
    put_len_prefixed(&mut out, d.module.as_bytes());
    put_len_prefixed(&mut out, &d.lo);
    put_len_prefixed(&mut out, &d.hi);
    if d.mode == TriggerMode::Changes {
        out.push(1u8); // mode byte, room for future modes
    }
    out
}

fn decode_def(buf: &[u8]) -> Result<TriggerDef> {
    let mut r = Reader::new(buf);
    let ver = r.u8()?;
    if ver != 1 && ver != 2 {
        return Err(corrupt(format!("bad trigger record version {ver}")));
    }
    let name = String::from_utf8(r.len_prefixed()?.to_vec())
        .map_err(|_| corrupt("trigger name is not utf-8"))?;
    let module = String::from_utf8(r.len_prefixed()?.to_vec())
        .map_err(|_| corrupt("trigger module name is not utf-8"))?;
    let lo = r.len_prefixed()?.to_vec();
    let hi = r.len_prefixed()?.to_vec();
    let mode = if ver == 1 {
        TriggerMode::Keys
    } else {
        match r.u8()? {
            0 => TriggerMode::Keys,
            1 => TriggerMode::Changes,
            m => return Err(corrupt(format!("bad trigger mode {m}"))),
        }
    };
    Ok(TriggerDef {
        name,
        module,
        lo,
        hi,
        mode,
    })
}

fn validate_trigger_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid trigger name {name:?} (use [A-Za-z0-9._-], max 64 chars)"
        )))
    }
}

struct RunnerStatus {
    failures: u32,
    not_before: Instant,
    last_error: String,
}

pub(crate) struct TriggerState {
    /// Immutable registry snapshot, swapped whole on create/delete; the
    /// write path pays one read-lock + Arc clone per batch and nothing at
    /// all when no triggers exist.
    defs: RwLock<Arc<Vec<TriggerDef>>>,
    /// Serializes create/delete (persist + registry swap + queue clear).
    admin: Mutex<()>,
    /// Wakes the runner after a commit that enqueued events.
    pub signal: Signal,
    /// Per-trigger drain failure state (backoff + last error); an entry is
    /// cleared by the next successful drain.
    status: Mutex<HashMap<String, RunnerStatus>>,
}

// ---------------------------------------------------------------------------
// changes-mode event records (queue values, persisted — versioned)
// ---------------------------------------------------------------------------

/// `[ver=1][kind][uvarint klen][key][value…]` — value present iff kind
/// is `CHANGE_PUT`.
const CHANGE_RECORD_V1: u8 = 1;
pub(crate) const CHANGE_PUT: u8 = 0;
pub(crate) const CHANGE_DELETE: u8 = 1;
/// A put whose value exceeded `trigger_inline_value`: the event carries the
/// key only and the module reads the value on demand.
pub(crate) const CHANGE_PUT_ELIDED: u8 = 2;

fn encode_change_record(op: &BatchOp, inline_cap: usize) -> Vec<u8> {
    let (kind, key, value): (u8, &[u8], &[u8]) = match op {
        BatchOp::Put { key, value } if value.len() <= inline_cap => (CHANGE_PUT, key, value),
        BatchOp::Put { key, .. } => (CHANGE_PUT_ELIDED, key, &[]),
        BatchOp::Delete { key } => (CHANGE_DELETE, key, &[]),
    };
    let mut out = Vec::with_capacity(12 + key.len() + value.len());
    out.push(CHANGE_RECORD_V1);
    out.push(kind);
    put_uvarint(&mut out, key.len() as u64);
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    out
}

/// Decode a stored change record into (kind, key, inline value).
fn decode_change_record(buf: &[u8]) -> Result<(u8, &[u8], &[u8])> {
    let mut r = Reader::new(buf);
    match r.u8()? {
        CHANGE_RECORD_V1 => {}
        v => return Err(corrupt(format!("bad change record version {v}"))),
    }
    let kind = r.u8()?;
    if kind > CHANGE_PUT_ELIDED {
        return Err(corrupt(format!("bad change record kind {kind}")));
    }
    let klen = usize::try_from(r.uvarint()?).map_err(|_| corrupt("change record key length"))?;
    if r.remaining() < klen {
        return Err(corrupt("change record truncated"));
    }
    let key = r.bytes(klen)?;
    let n = r.remaining();
    let value = r.bytes(n)?;
    if kind != CHANGE_PUT && !value.is_empty() {
        return Err(corrupt("change record has a value but no put kind"));
    }
    Ok((kind, key, value))
}

impl TriggerState {
    pub fn new() -> TriggerState {
        TriggerState {
            defs: RwLock::new(Arc::new(Vec::new())),
            admin: Mutex::new(()),
            signal: Signal::new(),
            status: Mutex::new(HashMap::new()),
        }
    }

    /// Event ops to append to a batch whose logical writes are `ops`,
    /// where the batch's first op will commit at seqno `base`.
    ///
    /// MUST be called inside the commit critical section (`write_mu` held,
    /// `base` freshly allocated): changes-mode queue keys embed each op's
    /// commit seqno, and "events iterate in commit order" is only true if
    /// no other batch can take an earlier seqno afterwards.
    ///
    /// Keys-mode events are sorted + deduped so a key written twice in one
    /// batch enqueues once; changes-mode events are one per op, in batch
    /// order, and never collide (seqnos are unique).
    pub fn capture_ops(&self, ops: &[BatchOp], base: SeqNo, opts: &Options) -> Vec<BatchOp> {
        let defs = self.defs.read().clone();
        if defs.is_empty() {
            return Vec::new();
        }
        // an inlined value must never make a single event undeliverable:
        // one packed change (seqno + kind + framed key + framed value) has
        // to fit a max_wasm_input-sized invocation with room to spare
        let inline_cap = opts
            .trigger_inline_value
            .min(opts.max_wasm_input.saturating_sub(opts.max_key_size + 64));
        let mut keyed: Vec<Vec<u8>> = Vec::new();
        let mut changes: Vec<BatchOp> = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            let key = match op {
                BatchOp::Put { key, .. } => key.as_slice(),
                BatchOp::Delete { key } => key.as_slice(),
            };
            if key.first() == Some(&SYS_PREFIX) {
                continue; // engine-internal writes never fire triggers
            }
            for d in defs.iter() {
                if !d.matches(key) {
                    continue;
                }
                match d.mode {
                    TriggerMode::Keys => keyed.push(sys_trigger_event_key(&d.name, key)),
                    TriggerMode::Changes => changes.push(BatchOp::Put {
                        key: sys_trigger_change_key(&d.name, base + i as u64),
                        value: encode_change_record(op, inline_cap),
                    }),
                }
            }
        }
        keyed.sort_unstable();
        keyed.dedup();
        let mut out: Vec<BatchOp> = keyed
            .into_iter()
            .map(|key| BatchOp::Put {
                key,
                value: Vec::new(),
            })
            .collect();
        out.extend(changes);
        out
    }

    fn lookup(&self, name: &str) -> Option<TriggerDef> {
        self.defs.read().iter().find(|d| d.name == name).cloned()
    }

    fn in_backoff(&self, name: &str) -> bool {
        self.status
            .lock()
            .get(name)
            .is_some_and(|s| Instant::now() < s.not_before)
    }

    fn record_failure(&self, name: &str, err: &Error) {
        let mut msg = err.to_string();
        if msg.len() > 500 {
            let mut cut = 500;
            while !msg.is_char_boundary(cut) {
                cut -= 1;
            }
            msg.truncate(cut);
            msg.push('…');
        }
        let mut g = self.status.lock();
        let s = g.entry(name.to_string()).or_insert(RunnerStatus {
            failures: 0,
            not_before: Instant::now(),
            last_error: String::new(),
        });
        s.failures = s.failures.saturating_add(1);
        // 100ms doubling to a 6.4s ceiling: transient conflicts retry fast,
        // a persistently failing module cannot busy-spin the runner
        let backoff = Duration::from_millis(100 << s.failures.min(6));
        s.not_before = Instant::now() + backoff;
        s.last_error = msg;
    }

    fn clear_status(&self, name: &str) {
        self.status.lock().remove(name);
    }

    fn last_error(&self, name: &str) -> Option<String> {
        self.status.lock().get(name).map(|s| s.last_error.clone())
    }
}

// ---------------------------------------------------------------------------
// admin: create / delete / list / load
// ---------------------------------------------------------------------------

pub(crate) fn create_trigger(
    db: &Arc<DbInner>,
    name: &str,
    module: &str,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Result<TriggerMode> {
    validate_trigger_name(name)?;
    let lo = lo.unwrap_or_default().to_vec();
    let hi = hi.unwrap_or_default().to_vec();
    for bound in [&lo, &hi] {
        if !bound.is_empty() {
            validate_user_key(bound)?;
            if bound.len() > db.opts.max_key_size {
                return Err(Error::InvalidArgument(
                    "trigger range bound exceeds max_key_size".into(),
                ));
            }
        }
    }
    if !lo.is_empty() && !hi.is_empty() && lo >= hi {
        return Err(Error::InvalidArgument(
            "trigger range is empty (lo >= hi)".into(),
        ));
    }

    let _g = db.triggers.admin.lock();
    if db.triggers.lookup(name).is_some() {
        return Err(Error::InvalidArgument(format!(
            "trigger {name:?} already exists (delete it first to change it)"
        )));
    }
    // the module declares its consumption mode through its exports:
    // `on_apply` → the ordered change feed, `on_touch` → coalesced keys
    // (`on_apply` wins when a module exports both — the complete feed is
    // the safer default for a module that can consume it)
    let mode = if crate::wasm::module_exports_func(db, module, "on_apply")? {
        TriggerMode::Changes
    } else if crate::wasm::module_exports_func(db, module, "on_touch")? {
        TriggerMode::Keys
    } else {
        return Err(Error::InvalidArgument(format!(
            "module {module:?} exports neither `on_apply` nor `on_touch` — \
             not a trigger consumer"
        )));
    };
    let def = TriggerDef {
        name: name.to_string(),
        module: module.to_string(),
        lo,
        hi,
        mode,
    };
    // persist first: a trigger only becomes active once it is durable
    let mut b = WriteBatch::new();
    b.put(sys_trigger_key(name), encode_def(&def));
    db.write_batch_unchecked(b)?;
    let mut defs = db.triggers.defs.write();
    let mut next = defs.as_ref().clone();
    next.push(def);
    *defs = Arc::new(next);
    Ok(mode)
}

pub(crate) fn delete_trigger(db: &DbInner, name: &str) -> Result<()> {
    validate_trigger_name(name)?;
    let _g = db.triggers.admin.lock();
    if db.triggers.lookup(name).is_none() {
        return Err(Error::InvalidArgument(format!("no trigger named {name:?}")));
    }
    // deactivate capture first, then erase state. A write that raced the
    // swap can still enqueue a stray event; the runner garbage-collects
    // queue entries whose trigger no longer exists.
    {
        let mut defs = db.triggers.defs.write();
        let next: Vec<TriggerDef> = defs.iter().filter(|d| d.name != name).cloned().collect();
        *defs = Arc::new(next);
    }
    db.triggers.clear_status(name);
    let mut b = WriteBatch::new();
    b.delete(sys_trigger_key(name));
    db.write_batch_unchecked(b)?;
    while clear_queue_chunk(db, name)? {}
    Ok(())
}

pub(crate) fn list_triggers(db: &DbInner) -> Result<Vec<TriggerInfo>> {
    let defs = db.triggers.defs.read().clone();
    let mut out = Vec::with_capacity(defs.len());
    for d in defs.iter() {
        let mut pending: u64 = 0;
        let it = db.iter_raw(
            None,
            &queue_prefix(&d.name),
            Some(queue_prefix_end(&d.name)),
            false,
        )?;
        for kv in it {
            kv?;
            pending += 1;
        }
        out.push(TriggerInfo {
            name: d.name.clone(),
            module: d.module.clone(),
            lo: d.lo.clone(),
            hi: d.hi.clone(),
            mode: d.mode,
            pending,
            last_error: db.triggers.last_error(&d.name),
        });
    }
    Ok(out)
}

/// Load persisted definitions into the in-memory registry (open path).
pub(crate) fn load_registry(db: &DbInner) -> Result<()> {
    let it = db.iter_raw(None, DEF_PREFIX, Some(def_space_end()), false)?;
    let mut defs = Vec::new();
    for kv in it {
        let (_, v) = kv?;
        defs.push(decode_def(&v)?);
    }
    *db.triggers.defs.write() = Arc::new(defs);
    Ok(())
}

/// Delete up to one batch of a trigger's queued events (orphan cleanup and
/// `delete_trigger`). Returns whether anything was deleted.
fn clear_queue_chunk(db: &DbInner, name: &str) -> Result<bool> {
    let it = db.iter_raw(
        None,
        &queue_prefix(name),
        Some(queue_prefix_end(name)),
        false,
    )?;
    let mut b = WriteBatch::new();
    for kv in it {
        let (k, _) = kv?;
        b.delete(k);
        if b.len() >= 512 {
            break;
        }
    }
    if b.is_empty() {
        return Ok(false);
    }
    db.write_batch_unchecked(b)?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// the runner
// ---------------------------------------------------------------------------

/// Background thread: drains every trigger queue until quiet, then parks on
/// the trigger signal. Recovery is implicit — queue entries are ordinary
/// durable keys, so pending events survive restarts and are drained on the
/// next pass.
pub(crate) fn trigger_thread(db: Arc<DbInner>) {
    while !db.shutdown.load(std::sync::atomic::Ordering::Acquire) {
        let did = runner_pass(&db);
        if !did {
            db.triggers.signal.wait_timeout(Duration::from_millis(200));
        }
    }
}

/// One sweep over the queue keyspace. Distinct trigger names are discovered
/// by skip-seeking the shared prefix (one short scan per trigger with a
/// backlog), so the runner needs no side index of "queues with work".
fn runner_pass(db: &Arc<DbInner>) -> bool {
    let mut did = false;
    let mut cursor = QUEUE_PREFIX.to_vec();
    loop {
        if db.shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return did;
        }
        let first = (|| -> Result<Option<Vec<u8>>> {
            let mut it = db.iter_raw(None, &cursor, Some(queue_space_end()), false)?;
            match it.next() {
                None => Ok(None),
                Some(kv) => Ok(Some(kv?.0)),
            }
        })();
        let key = match first {
            Ok(Some(k)) => k,
            // end of queue space, or engine trouble: park and retry later
            Ok(None) | Err(_) => return did,
        };
        let Some((name, _)) = parse_event_key(&key) else {
            // unparseable queue-space key (never written by this code):
            // step past it rather than spinning on it
            cursor = key;
            cursor.push(0);
            continue;
        };
        let name = name.to_string();
        cursor = queue_prefix_end(&name);
        match db.triggers.lookup(&name) {
            None => {
                // deleted trigger with residual events (create/delete race
                // or a crash mid-delete): garbage-collect them
                if clear_queue_chunk(db, &name).unwrap_or(false) {
                    did = true;
                }
            }
            Some(def) => {
                if db.triggers.in_backoff(&name) {
                    continue;
                }
                match drain_one(db, &def) {
                    Ok(true) => {
                        db.triggers.clear_status(&name);
                        did = true;
                    }
                    Ok(false) => {}
                    Err(Error::Closed) => return did,
                    Err(e) => db.triggers.record_failure(&name, &e),
                }
            }
        }
    }
}

/// Drain one batch of a trigger's queue: invoke the module (mode-specific
/// entry point and input packing) with the consumed queue entries deleted
/// inside the module's own transaction (exactly-once effects).
fn drain_one(db: &Arc<DbInner>, def: &TriggerDef) -> Result<bool> {
    let (input, consume) = match def.mode {
        TriggerMode::Keys => pack_touched_keys(db, def)?,
        TriggerMode::Changes => pack_changes(db, def)?,
    };
    if consume.is_empty() {
        return Ok(false);
    }
    crate::wasm::execute_system(db, &def.module, &input, &consume, def.mode.entry())?;
    Ok(true)
}

/// Keys-mode input: the touched keys packed as `[klen uvarint][key]`,
/// repeated (`fluent_guest::trigger_keys()` on the guest side).
fn pack_touched_keys(db: &Arc<DbInner>, def: &TriggerDef) -> Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let prefix = queue_prefix(&def.name);
    let it = db.iter_raw(None, &prefix, Some(queue_prefix_end(&def.name)), false)?;
    let mut consume: Vec<Vec<u8>> = Vec::new();
    let mut input: Vec<u8> = Vec::new();
    for kv in it {
        let (k, _) = kv?;
        if consume.len() >= db.opts.trigger_batch {
            break;
        }
        let ukey = &k[prefix.len()..];
        let mut hdr = Vec::with_capacity(10);
        put_uvarint(&mut hdr, ukey.len() as u64);
        if input.len() + hdr.len() + ukey.len() > db.opts.max_wasm_input {
            if consume.is_empty() {
                return Err(Error::InvalidArgument(
                    "pending trigger event exceeds max_wasm_input".into(),
                ));
            }
            break;
        }
        input.extend_from_slice(&hdr);
        input.extend_from_slice(ukey);
        consume.push(k);
    }
    Ok((input, consume))
}

/// Changes-mode input, wire-format framing (little-endian, `u32` lengths):
/// `[u32 count]` then per change `[u64 seqno][u8 kind][u32 klen][key]` plus
/// `[u32 vlen][value]` when kind = put. Events iterate — and are therefore
/// delivered — in commit order.
fn pack_changes(db: &Arc<DbInner>, def: &TriggerDef) -> Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let prefix = queue_prefix(&def.name);
    let it = db.iter_raw(None, &prefix, Some(queue_prefix_end(&def.name)), false)?;
    let mut consume: Vec<Vec<u8>> = Vec::new();
    let mut input: Vec<u8> = vec![0u8; 4]; // count, patched below
    for kv in it {
        let (k, v) = kv?;
        if consume.len() >= db.opts.trigger_batch {
            break;
        }
        let seq_bytes: [u8; 8] = k[prefix.len()..]
            .try_into()
            .map_err(|_| corrupt("change event key is not a seqno"))?;
        let seqno = u64::from_be_bytes(seq_bytes);
        let (kind, ukey, uval) = decode_change_record(&v)?;
        let packed = 8 + 1 + 4 + ukey.len() + if kind == CHANGE_PUT { 4 + uval.len() } else { 0 };
        if input.len() + packed > db.opts.max_wasm_input {
            if consume.is_empty() {
                return Err(Error::InvalidArgument(
                    "pending trigger event exceeds max_wasm_input".into(),
                ));
            }
            break;
        }
        crate::coding::put_u64(&mut input, seqno);
        input.push(kind);
        crate::coding::put_u32(&mut input, ukey.len() as u32);
        input.extend_from_slice(ukey);
        if kind == CHANGE_PUT {
            crate::coding::put_u32(&mut input, uval.len() as u32);
            input.extend_from_slice(uval);
        }
        consume.push(k);
    }
    let count = consume.len() as u32;
    input[..4].copy_from_slice(&count.to_le_bytes());
    Ok((input, consume))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, lo: &[u8], hi: &[u8]) -> TriggerDef {
        TriggerDef {
            name: name.into(),
            module: "m".into(),
            lo: lo.to_vec(),
            hi: hi.to_vec(),
            mode: TriggerMode::Keys,
        }
    }

    #[test]
    fn def_record_roundtrip() {
        let d = def("orders-idx", b"orders/", b"orders0");
        let enc = encode_def(&d);
        assert_eq!(enc[0], 1, "keys-mode defs keep the v1 layout");
        let got = decode_def(&enc).unwrap();
        assert_eq!(got.name, "orders-idx");
        assert_eq!(got.module, "m");
        assert_eq!(got.lo, b"orders/");
        assert_eq!(got.hi, b"orders0");
        assert_eq!(got.mode, TriggerMode::Keys);
        assert!(decode_def(&[9u8, 0, 0, 0, 0]).is_err(), "bad version");

        let d2 = TriggerDef {
            mode: TriggerMode::Changes,
            ..def("feed", b"", b"")
        };
        let enc2 = encode_def(&d2);
        assert_eq!(enc2[0], 2, "changes-mode defs use the v2 layout");
        let got2 = decode_def(&enc2).unwrap();
        assert_eq!(got2.mode, TriggerMode::Changes);
        assert_eq!(got2.name, "feed");
    }

    #[test]
    fn change_record_roundtrip() {
        let put = BatchOp::Put {
            key: b"orders/1".to_vec(),
            value: b"hello".to_vec(),
        };
        let (kind, k, v) = {
            let enc = encode_change_record(&put, 64);
            let (kind, k, v) = decode_change_record(&enc).unwrap();
            (kind, k.to_vec(), v.to_vec())
        };
        assert_eq!((kind, k.as_slice(), v.as_slice()), (CHANGE_PUT, &b"orders/1"[..], &b"hello"[..]));

        // a value above the inline cap is elided, key retained
        let enc = encode_change_record(&put, 4);
        let (kind, k, v) = decode_change_record(&enc).unwrap();
        assert_eq!(kind, CHANGE_PUT_ELIDED);
        assert_eq!(k, b"orders/1");
        assert!(v.is_empty());

        let del = BatchOp::Delete {
            key: b"orders/1".to_vec(),
        };
        let enc = encode_change_record(&del, 64);
        let (kind, k, v) = decode_change_record(&enc).unwrap();
        assert_eq!(kind, CHANGE_DELETE);
        assert_eq!(k, b"orders/1");
        assert!(v.is_empty());

        assert!(decode_change_record(&[7u8, 0, 0]).is_err(), "bad version");
        assert!(decode_change_record(&[1u8, 9, 0]).is_err(), "bad kind");
        assert!(decode_change_record(&[1u8, 1, 5, b'a']).is_err(), "truncated");
    }

    #[test]
    fn capture_emits_per_op_seqno_keyed_changes_and_coalesced_keys() {
        let state = TriggerState::new();
        *state.defs.write() = Arc::new(vec![
            TriggerDef {
                mode: TriggerMode::Changes,
                ..def("feed", b"orders/", b"orders0")
            },
            def("idx", b"orders/", b"orders0"),
        ]);
        let ops = vec![
            BatchOp::Put {
                key: b"orders/1".to_vec(),
                value: b"a".to_vec(),
            },
            BatchOp::Put {
                key: b"other".to_vec(),
                value: b"x".to_vec(),
            },
            BatchOp::Put {
                key: b"orders/1".to_vec(),
                value: b"b".to_vec(),
            },
            BatchOp::Delete {
                key: b"orders/1".to_vec(),
            },
        ];
        let events = state.capture_ops(&ops, 100, &Options::default());
        // keys mode coalesces the three orders/1 touches into ONE event;
        // changes mode keeps all three, keyed by their op seqnos
        assert_eq!(events.len(), 1 + 3);
        let keys: Vec<&[u8]> = events
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, .. } => key.as_slice(),
                BatchOp::Delete { key } => key.as_slice(),
            })
            .collect();
        assert_eq!(keys[0], sys_trigger_event_key("idx", b"orders/1").as_slice());
        assert_eq!(keys[1], sys_trigger_change_key("feed", 100).as_slice());
        assert_eq!(keys[2], sys_trigger_change_key("feed", 102).as_slice());
        assert_eq!(keys[3], sys_trigger_change_key("feed", 103).as_slice());
        // the change records carry kind + key + inline value
        let BatchOp::Put { value, .. } = &events[1] else {
            panic!("change events are puts")
        };
        let (kind, k, v) = decode_change_record(value).unwrap();
        assert_eq!((kind, k, v), (CHANGE_PUT, &b"orders/1"[..], &b"a"[..]));
        let BatchOp::Put { value, .. } = &events[3] else {
            panic!("change events are puts")
        };
        let (kind, k, v) = decode_change_record(value).unwrap();
        assert_eq!((kind, k, v), (CHANGE_DELETE, &b"orders/1"[..], &[][..]));
    }

    #[test]
    fn capture_skips_system_keys_and_respects_ranges() {
        let state = TriggerState::new();
        *state.defs.write() = Arc::new(vec![TriggerDef {
            mode: TriggerMode::Changes,
            ..def("feed", b"b", b"d")
        }]);
        let ops = vec![
            BatchOp::Put {
                key: vec![SYS_PREFIX, b'x'],
                value: b"sys".to_vec(),
            },
            BatchOp::Put {
                key: b"a".to_vec(),
                value: b"out".to_vec(),
            },
            BatchOp::Put {
                key: b"c".to_vec(),
                value: b"in".to_vec(),
            },
        ];
        let events = state.capture_ops(&ops, 7, &Options::default());
        assert_eq!(events.len(), 1);
        let BatchOp::Put { key, .. } = &events[0] else {
            panic!()
        };
        assert_eq!(key.as_slice(), sys_trigger_change_key("feed", 9).as_slice());
    }

    #[test]
    fn range_matching_and_unbounded_ends() {
        let d = def("t", b"b", b"d");
        assert!(!d.matches(b"a"));
        assert!(d.matches(b"b"));
        assert!(d.matches(b"c"));
        assert!(!d.matches(b"d"), "hi is exclusive");
        let open = def("t", b"", b"");
        assert!(open.matches(b"a"));
        assert!(open.matches(&[0xff; 4]));
        let from = def("t", b"m", b"");
        assert!(!from.matches(b"a"));
        assert!(from.matches(b"z"));
    }

    #[test]
    fn event_key_layout_roundtrip() {
        let k = sys_trigger_event_key("t1", b"user/key");
        let (name, ukey) = parse_event_key(&k).unwrap();
        assert_eq!(name, "t1");
        assert_eq!(ukey, b"user/key");
        // queue keys sort inside [queue_prefix, queue_prefix_end)
        assert!(k.as_slice() >= queue_prefix("t1").as_slice());
        assert!(k.as_slice() < queue_prefix_end("t1").as_slice());
        assert!(k.as_slice() < queue_space_end().as_slice());
    }

    #[test]
    fn trigger_names_validate_like_module_names() {
        assert!(validate_trigger_name("orders-idx.v2").is_ok());
        assert!(validate_trigger_name("").is_err());
        assert!(validate_trigger_name("has space").is_err());
        assert!(validate_trigger_name(&"x".repeat(65)).is_err());
    }
}
