//! The `fluent` syscall module — the kernel-like API guests import.
//!
//! Conventions ("fluentabi v1"):
//! - all pointers/lengths are u32 (passed as wasm i32; negative values fail
//!   range checks and trap)
//! - memory-safety violations TRAP; semantic misuse returns an errno
//! - errnos (negative i32/i64): NOT_FOUND -1, EROFS -2, EINVAL -3,
//!   ENOSPC -4, EBADF -5, ELIMIT -6, EIO -8
//! - `get(key, off, buf, cap) -> i64` returns the FULL value length and
//!   copies `min(cap, len - off)` bytes from `off` (chunked reads of values
//!   larger than guest memory)
//! - `scan_next` packs `[klen varint][vlen varint][key][value]` repeatedly —
//!   one boundary crossing moves a whole batch
//! - reserved keyspace (0x00 prefix) is invisible: writes are EINVAL, scans
//!   are clamped to the user keyspace
//! - scan handles are per-invocation, never reused, dropped with the Store
//!   on every exit path (traps included)

use std::collections::HashMap;
use std::sync::Arc;

use wasmtime::{Caller, Linker, Memory, StoreLimits, StoreLimitsBuilder};

use crate::coding::put_uvarint;
use crate::db::DbInner;
use crate::error::Error;
use crate::iter::DbIterator;
use crate::txn::{Txn, TxnIter};
use crate::types::{validate_user_key, SeqNo};

pub(crate) const NOT_FOUND: i32 = -1;
pub(crate) const EROFS: i32 = -2;
pub(crate) const EINVAL: i32 = -3;
pub(crate) const ENOSPC: i32 = -4;
pub(crate) const EBADF: i32 = -5;
pub(crate) const ELIMIT: i32 = -6;
pub(crate) const EIO: i32 = -8;

pub(crate) enum Access {
    ReadOnly(SeqNo),
    /// Option so the driver can take the Txn out for commit after `run`.
    Txn(Option<Txn>),
}

enum ScanIter {
    Db(DbIterator),
    Txn(TxnIter),
}

impl ScanIter {
    fn pull(&mut self) -> Option<crate::error::Result<(Vec<u8>, Vec<u8>)>> {
        match self {
            ScanIter::Db(it) => it.next(),
            ScanIter::Txn(it) => it.next(),
        }
    }
}

struct Scan {
    it: ScanIter,
    pending: Option<(Vec<u8>, Vec<u8>)>,
    errored: bool,
}

impl Scan {
    /// Make sure `pending` holds the next entry (or None at end / error).
    fn ensure(&mut self) -> Result<(), ()> {
        if self.errored {
            return Err(());
        }
        if self.pending.is_none() {
            match self.it.pull() {
                None => {}
                Some(Ok(kv)) => self.pending = Some(kv),
                Some(Err(_)) => {
                    self.errored = true;
                    return Err(());
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct HostCtx {
    pub db: Arc<DbInner>,
    pub access: Access,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    pub limits: StoreLimits,
    pub host_error: Option<Error>,
    log_bytes: usize,
    scans: HashMap<i32, Scan>,
    next_scan: i32,
}

impl HostCtx {
    pub fn new(db: Arc<DbInner>, access: Access, input: Vec<u8>) -> HostCtx {
        let limits = StoreLimitsBuilder::new()
            .memory_size(db.opts.wasm_memory_limit)
            // the per-memory size cap only bounds anything if the memory
            // COUNT is bounded too (multi-memory modules)
            .memories(1)
            .tables(4)
            .instances(2)
            .build();
        HostCtx {
            db,
            access,
            input,
            output: Vec::new(),
            limits,
            host_error: None,
            log_bytes: 0,
            scans: HashMap::new(),
            next_scan: 1,
        }
    }
}

type WResult<T> = std::result::Result<T, wasmtime::Error>;

fn memory(caller: &mut Caller<'_, HostCtx>) -> WResult<Memory> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| wasmtime::Error::msg("guest exports no memory"))
}

fn read_guest(caller: &mut Caller<'_, HostCtx>, ptr: i32, len: i32) -> WResult<Vec<u8>> {
    let m = memory(caller)?;
    let (p, l) = (ptr as u32 as usize, len as u32 as usize);
    if l as u64 > m.data_size(&caller) as u64 {
        return Err(wasmtime::Error::msg("guest length out of range"));
    }
    let mut buf = vec![0u8; l];
    m.read(&caller, p, &mut buf)
        .map_err(|_| wasmtime::Error::msg("guest pointer range out of bounds"))?;
    Ok(buf)
}

fn write_guest(caller: &mut Caller<'_, HostCtx>, ptr: i32, data: &[u8]) -> WResult<()> {
    let m = memory(caller)?;
    m.write(&mut *caller, ptr as u32 as usize, data)
        .map_err(|_| wasmtime::Error::msg("guest pointer range out of bounds"))?;
    Ok(())
}

fn packed_len(k: &[u8], v: &[u8]) -> usize {
    let mut hdr = Vec::with_capacity(10);
    put_uvarint(&mut hdr, k.len() as u64);
    put_uvarint(&mut hdr, v.len() as u64);
    hdr.len() + k.len() + v.len()
}

/// Shared body of get / get_for_update.
fn do_get(
    caller: &mut Caller<'_, HostCtx>,
    for_update: bool,
    kptr: i32,
    klen: i32,
    off: i32,
    vbuf: i32,
    vcap: i32,
) -> WResult<i64> {
    let key = read_guest(caller, kptr, klen)?;
    if validate_user_key(&key).is_err() {
        return Ok(EINVAL as i64);
    }
    let got = {
        let ctx = caller.data_mut();
        let db = ctx.db.clone();
        match &mut ctx.access {
            Access::ReadOnly(_) if for_update => return Ok(EROFS as i64),
            Access::ReadOnly(seq) => db.get_at_seq(&key, *seq),
            Access::Txn(t) => {
                let t = t.as_mut().expect("txn present");
                if for_update {
                    t.get_for_update(&key)
                } else {
                    t.get(&key)
                }
            }
        }
    };
    match got {
        Ok(None) => Ok(NOT_FOUND as i64),
        Ok(Some(v)) => {
            let off = off as u32 as usize;
            if off < v.len() {
                let n = (vcap as u32 as usize).min(v.len() - off);
                if n > 0 {
                    write_guest(caller, vbuf, &v[off..off + n])?;
                }
            }
            Ok(v.len() as i64)
        }
        Err(e) => {
            caller.data_mut().host_error = Some(e);
            Ok(EIO as i64)
        }
    }
}

pub(crate) fn register(linker: &mut Linker<HostCtx>) -> WResult<()> {
    linker.func_wrap("fluent", "input_len", |caller: Caller<'_, HostCtx>| -> i32 {
        caller.data().input.len() as i32
    })?;

    linker.func_wrap(
        "fluent",
        "input_read",
        |mut caller: Caller<'_, HostCtx>, dst: i32, cap: i32, off: i32| -> WResult<i32> {
            let off = off as u32 as usize;
            let cap = cap as u32 as usize;
            let input = &caller.data().input;
            if off >= input.len() {
                return Ok(0);
            }
            let n = cap.min(input.len() - off);
            let chunk = input[off..off + n].to_vec();
            write_guest(&mut caller, dst, &chunk)?;
            Ok(n as i32)
        },
    )?;

    linker.func_wrap(
        "fluent",
        "output_write",
        |mut caller: Caller<'_, HostCtx>, ptr: i32, len: i32| -> WResult<i32> {
            let data = read_guest(&mut caller, ptr, len)?;
            let ctx = caller.data_mut();
            if ctx.output.len() + data.len() > ctx.db.opts.max_wasm_output {
                return Ok(ENOSPC);
            }
            ctx.output.extend_from_slice(&data);
            Ok(0)
        },
    )?;

    linker.func_wrap(
        "fluent",
        "log",
        |mut caller: Caller<'_, HostCtx>, level: i32, ptr: i32, len: i32| -> WResult<i32> {
            let data = read_guest(&mut caller, ptr, len)?;
            let ctx = caller.data_mut();
            if ctx.log_bytes + data.len() > ctx.db.opts.max_wasm_log {
                return Ok(ENOSPC);
            }
            ctx.log_bytes += data.len();
            if std::env::var_os("FLUENT31_WASM_LOG").is_some() {
                eprintln!(
                    "[wasm log {level}] {}",
                    String::from_utf8_lossy(&data)
                );
            }
            Ok(0)
        },
    )?;

    linker.func_wrap(
        "fluent",
        "get",
        |mut caller: Caller<'_, HostCtx>,
         kptr: i32,
         klen: i32,
         off: i32,
         vbuf: i32,
         vcap: i32|
         -> WResult<i64> { do_get(&mut caller, false, kptr, klen, off, vbuf, vcap) },
    )?;

    linker.func_wrap(
        "fluent",
        "get_for_update",
        |mut caller: Caller<'_, HostCtx>,
         kptr: i32,
         klen: i32,
         off: i32,
         vbuf: i32,
         vcap: i32|
         -> WResult<i64> { do_get(&mut caller, true, kptr, klen, off, vbuf, vcap) },
    )?;

    linker.func_wrap(
        "fluent",
        "put",
        |mut caller: Caller<'_, HostCtx>,
         kptr: i32,
         klen: i32,
         vptr: i32,
         vlen: i32|
         -> WResult<i32> {
            let key = read_guest(&mut caller, kptr, klen)?;
            let value = read_guest(&mut caller, vptr, vlen)?;
            let ctx = caller.data_mut();
            if validate_user_key(&key).is_err()
                || key.len() > ctx.db.opts.max_key_size
                || value.len() > ctx.db.opts.max_value_size
            {
                return Ok(EINVAL);
            }
            match &mut ctx.access {
                Access::ReadOnly(_) => Ok(EROFS),
                Access::Txn(t) => match t.as_mut().expect("txn").put(key, value) {
                    Ok(()) => Ok(0),
                    Err(_) => Ok(ENOSPC), // only remaining failure: write-set cap
                },
            }
        },
    )?;

    linker.func_wrap(
        "fluent",
        "delete",
        |mut caller: Caller<'_, HostCtx>, kptr: i32, klen: i32| -> WResult<i32> {
            let key = read_guest(&mut caller, kptr, klen)?;
            let ctx = caller.data_mut();
            if validate_user_key(&key).is_err() {
                return Ok(EINVAL);
            }
            match &mut ctx.access {
                Access::ReadOnly(_) => Ok(EROFS),
                Access::Txn(t) => match t.as_mut().expect("txn").delete(key) {
                    Ok(()) => Ok(0),
                    Err(_) => Ok(ENOSPC),
                },
            }
        },
    )?;

    linker.func_wrap(
        "fluent",
        "scan_open",
        |mut caller: Caller<'_, HostCtx>,
         lo_ptr: i32,
         lo_len: i32,
         hi_ptr: i32,
         hi_len: i32,
         flags: i32|
         -> WResult<i32> {
            if flags & !1 != 0 {
                return Ok(EINVAL);
            }
            let reverse = flags & 1 == 1;
            let lo = if lo_len == 0 {
                None
            } else {
                Some(read_guest(&mut caller, lo_ptr, lo_len)?)
            };
            let hi = if hi_len == 0 {
                None
            } else {
                Some(read_guest(&mut caller, hi_ptr, hi_len)?)
            };
            let ctx = caller.data_mut();
            if ctx.scans.len() >= ctx.db.opts.max_wasm_scans {
                return Ok(ELIMIT);
            }
            let db = ctx.db.clone();
            let made = match &mut ctx.access {
                Access::ReadOnly(seq) => db
                    .iter_at_seq(Some(*seq), lo.as_deref(), hi, reverse)
                    .map(ScanIter::Db),
                Access::Txn(t) => t
                    .as_ref()
                    .expect("txn")
                    .iter(lo.as_deref(), hi.as_deref(), reverse)
                    .map(ScanIter::Txn),
            };
            match made {
                Ok(it) => {
                    let h = ctx.next_scan;
                    ctx.next_scan += 1;
                    ctx.scans.insert(
                        h,
                        Scan {
                            it,
                            pending: None,
                            errored: false,
                        },
                    );
                    Ok(h)
                }
                Err(e) => {
                    ctx.host_error = Some(e);
                    Ok(EIO)
                }
            }
        },
    )?;

    linker.func_wrap(
        "fluent",
        "scan_next",
        |mut caller: Caller<'_, HostCtx>, h: i32, buf: i32, cap: i32| -> WResult<i32> {
            // host-side batch ceiling: a huge guest cap must not make the
            // host resolve/buffer gigabytes before the guest write traps
            let cap = (cap as u32 as usize).min(16 << 20);
            let mut out: Vec<u8> = Vec::new();
            {
                let ctx = caller.data_mut();
                let Some(scan) = ctx.scans.get_mut(&h) else {
                    return Ok(EBADF);
                };
                loop {
                    if scan.ensure().is_err() {
                        return Ok(EIO);
                    }
                    let Some((k, v)) = &scan.pending else { break };
                    let entry = packed_len(k, v);
                    if out.len() + entry > cap {
                        if out.is_empty() {
                            return Ok(ENOSPC);
                        }
                        break;
                    }
                    put_uvarint(&mut out, k.len() as u64);
                    put_uvarint(&mut out, v.len() as u64);
                    out.extend_from_slice(k);
                    out.extend_from_slice(v);
                    scan.pending = None;
                }
            }
            if !out.is_empty() {
                write_guest(&mut caller, buf, &out)?;
            }
            Ok(out.len() as i32)
        },
    )?;

    linker.func_wrap(
        "fluent",
        "scan_entry_hint",
        |mut caller: Caller<'_, HostCtx>, h: i32| -> WResult<i64> {
            let ctx = caller.data_mut();
            let Some(scan) = ctx.scans.get_mut(&h) else {
                return Ok(EBADF as i64);
            };
            if scan.ensure().is_err() {
                return Ok(EIO as i64);
            }
            Ok(scan
                .pending
                .as_ref()
                .map(|(k, v)| packed_len(k, v) as i64)
                .unwrap_or(0))
        },
    )?;

    linker.func_wrap(
        "fluent",
        "scan_skip",
        |mut caller: Caller<'_, HostCtx>, h: i32| -> WResult<i32> {
            let ctx = caller.data_mut();
            let Some(scan) = ctx.scans.get_mut(&h) else {
                return Ok(EBADF);
            };
            if scan.ensure().is_err() {
                return Ok(EIO);
            }
            Ok(if scan.pending.take().is_some() { 1 } else { 0 })
        },
    )?;

    linker.func_wrap(
        "fluent",
        "scan_close",
        |mut caller: Caller<'_, HostCtx>, h: i32| -> WResult<i32> {
            let ctx = caller.data_mut();
            Ok(if ctx.scans.remove(&h).is_some() {
                0
            } else {
                EBADF
            })
        },
    )?;

    Ok(())
}
