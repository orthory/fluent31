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
//! Semantics, deliberately chosen:
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
//! - No stacking: a trigger invocation commits through a *system*
//!   transaction whose writes never generate events, so trigger graphs
//!   cannot cascade or loop.
//! - Value-log GC relocations and other engine-internal rewrites bypass
//!   capture entirely (they change placement, not logical state).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::batch::WriteBatch;
use crate::coding::{put_len_prefixed, put_uvarint, Reader};
use crate::db::{DbInner, Signal};
use crate::error::{corrupt, Error, Result};
use crate::types::{
    sys_trigger_event_key, sys_trigger_key, sys_wasm_key, validate_user_key, MAX_SEQNO,
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

/// A registered trigger, as listed by `Db::list_triggers`.
#[derive(Debug, Clone)]
pub struct TriggerInfo {
    pub name: String,
    pub module: String,
    /// Inclusive range start; empty = from the start of the user keyspace.
    pub lo: Vec<u8>,
    /// Exclusive range end; empty = unbounded.
    pub hi: Vec<u8>,
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
}

impl TriggerDef {
    fn matches(&self, key: &[u8]) -> bool {
        (self.lo.is_empty() || key >= self.lo.as_slice())
            && (self.hi.is_empty() || key < self.hi.as_slice())
    }
}

fn encode_def(d: &TriggerDef) -> Vec<u8> {
    let mut out = vec![1u8]; // record version
    put_len_prefixed(&mut out, d.name.as_bytes());
    put_len_prefixed(&mut out, d.module.as_bytes());
    put_len_prefixed(&mut out, &d.lo);
    put_len_prefixed(&mut out, &d.hi);
    out
}

fn decode_def(buf: &[u8]) -> Result<TriggerDef> {
    let mut r = Reader::new(buf);
    match r.u8()? {
        1 => {}
        v => return Err(corrupt(format!("bad trigger record version {v}"))),
    }
    let name = String::from_utf8(r.len_prefixed()?.to_vec())
        .map_err(|_| corrupt("trigger name is not utf-8"))?;
    let module = String::from_utf8(r.len_prefixed()?.to_vec())
        .map_err(|_| corrupt("trigger module name is not utf-8"))?;
    let lo = r.len_prefixed()?.to_vec();
    let hi = r.len_prefixed()?.to_vec();
    Ok(TriggerDef {
        name,
        module,
        lo,
        hi,
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

impl TriggerState {
    pub fn new() -> TriggerState {
        TriggerState {
            defs: RwLock::new(Arc::new(Vec::new())),
            admin: Mutex::new(()),
            signal: Signal::new(),
            status: Mutex::new(HashMap::new()),
        }
    }

    /// Event records to append to a batch whose logical writes touch the
    /// given keys. Sorted + deduped so a key written twice in one batch
    /// enqueues once.
    pub fn event_keys<'a>(&self, keys: impl Iterator<Item = &'a [u8]>) -> Vec<Vec<u8>> {
        let defs = self.defs.read().clone();
        if defs.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<Vec<u8>> = Vec::new();
        for key in keys {
            if key.first() == Some(&SYS_PREFIX) {
                continue; // engine-internal writes never fire triggers
            }
            for d in defs.iter() {
                if d.matches(key) {
                    out.push(sys_trigger_event_key(&d.name, key));
                }
            }
        }
        out.sort_unstable();
        out.dedup();
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
    db: &DbInner,
    name: &str,
    module: &str,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Result<()> {
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
    if db.get_at_seq(&sys_wasm_key(module), MAX_SEQNO)?.is_none() {
        return Err(Error::InvalidArgument(format!(
            "no module named {module:?}"
        )));
    }
    let def = TriggerDef {
        name: name.to_string(),
        module: module.to_string(),
        lo,
        hi,
    };
    // persist first: a trigger only becomes active once it is durable
    let mut b = WriteBatch::new();
    b.put(sys_trigger_key(name), encode_def(&def));
    db.write_batch_unchecked(b)?;
    let mut defs = db.triggers.defs.write();
    let mut next = defs.as_ref().clone();
    next.push(def);
    *defs = Arc::new(next);
    Ok(())
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

/// Drain one batch of a trigger's queue: invoke the module with the packed
/// touched keys as input; the consumed queue entries are deleted inside the
/// module's own transaction (exactly-once effects).
fn drain_one(db: &Arc<DbInner>, def: &TriggerDef) -> Result<bool> {
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
    if consume.is_empty() {
        return Ok(false);
    }
    crate::wasm::execute_system(db, &def.module, &input, &consume)?;
    Ok(true)
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
        }
    }

    #[test]
    fn def_record_roundtrip() {
        let d = def("orders-idx", b"orders/", b"orders0");
        let got = decode_def(&encode_def(&d)).unwrap();
        assert_eq!(got.name, "orders-idx");
        assert_eq!(got.module, "m");
        assert_eq!(got.lo, b"orders/");
        assert_eq!(got.hi, b"orders0");
        assert!(decode_def(&[9u8, 0, 0, 0, 0]).is_err(), "bad version");
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
