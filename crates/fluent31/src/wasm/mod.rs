//! In-database WASM execution — the SQL replacement.
//!
//! Modules are installed *into* the store (bytes live at
//! `\x00wasm\x00<name>`, versioned and recovered like any key) and invoked
//! two ways:
//!
//! - **query**: read-only, bound to a registered snapshot; `put`/`delete`
//!   return EROFS.
//! - **executor**: runs inside a fresh optimistic transaction; guest exit 0
//!   commits, anything else aborts. On commit conflict the WHOLE attempt is
//!   discarded and re-run against a fresh snapshot (fresh Store, fresh Txn,
//!   fresh fuel, fresh output) up to `execute_retries` times.
//!
//! Module bytes are resolved at the invocation's snapshot, so `query_at`
//! time-travels code together with data, and each execute attempt sees a
//! consistent module version.

pub(crate) mod abi;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use wasmtime::{Config, Engine, Linker, Module, Store};

use crate::batch::WriteBatch;
use crate::config::Options;
use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::txn::Txn;
use crate::types::{sys_wasm_key, SeqNo};

use abi::{Access, HostCtx};

/// Metadata for an installed module.
#[derive(Debug, Clone)]
pub struct ModuleInfo {
    pub name: String,
    pub size: usize,
    /// Content fingerprint of the stored bytes (cache key, not a security
    /// boundary): lets callers skip re-processing unchanged modules.
    pub content_hash: u128,
}

fn content_hash(bytes: &[u8]) -> u128 {
    // cache key, not a security boundary
    let a = crate::bloom::hash64(bytes);
    let mut salted = Vec::with_capacity(bytes.len().min(4096) + 8);
    salted.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    salted.extend_from_slice(&bytes[..bytes.len().min(4096)]);
    let b = crate::bloom::hash64(&salted);
    ((a as u128) << 64) | b as u128
}

struct ModuleCache {
    map: HashMap<u128, (Module, u64)>,
    tick: u64,
    cap: usize,
}

impl ModuleCache {
    fn get_or_compile(&mut self, engine: &Engine, bytes: &[u8]) -> Result<Module> {
        let h = content_hash(bytes);
        self.tick += 1;
        if let Some((m, t)) = self.map.get_mut(&h) {
            *t = self.tick;
            return Ok(m.clone());
        }
        let module =
            Module::new(engine, bytes).map_err(|e| Error::Wasm(format!("compile: {e}")))?;
        while self.map.len() >= self.cap {
            let Some((&victim, _)) = self.map.iter().min_by_key(|(_, (_, t))| *t) else {
                break;
            };
            self.map.remove(&victim);
        }
        self.map.insert(h, (module.clone(), self.tick));
        Ok(module)
    }
}

pub(crate) struct WasmRuntime {
    engine: Engine,
    linker: Linker<HostCtx>,
    cache: Mutex<ModuleCache>,
}

impl WasmRuntime {
    pub fn new(opts: &Options) -> Result<WasmRuntime> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.cranelift_nan_canonicalization(true);
        // threads support is not compiled in (no `threads` feature);
        // relaxed-simd must be deterministic for re-execution to be sound
        config.relaxed_simd_deterministic(true);
        let engine =
            Engine::new(&config).map_err(|e| Error::Wasm(format!("engine init: {e}")))?;
        let mut linker = Linker::new(&engine);
        abi::register(&mut linker).map_err(|e| Error::Wasm(format!("abi setup: {e}")))?;
        Ok(WasmRuntime {
            engine,
            linker,
            cache: Mutex::new(ModuleCache {
                map: HashMap::new(),
                tick: 0,
                cap: opts.wasm_module_cache.max(1),
            }),
        })
    }
}

fn validate_module_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid module name {name:?} (use [A-Za-z0-9._-], max 64 chars)"
        )))
    }
}

pub(crate) fn install_module(db: &Arc<DbInner>, name: &str, wasm: &[u8]) -> Result<()> {
    validate_module_name(name)?;
    // compile first: refuse to store bytes that can never run. Uncached:
    // rejected candidates must not evict installed modules' artifacts (the
    // shared cache fills at first invocation instead).
    let module = Module::new(&db.wasm.engine, wasm)
        .map_err(|e| Error::Wasm(format!("compile: {e}")))?;
    let has_run = module
        .get_export("run")
        .is_some_and(|e| e.func().is_some());
    let has_memory = module
        .get_export("memory")
        .is_some_and(|e| e.memory().is_some());
    if !has_run || !has_memory {
        return Err(Error::Wasm(
            "module must export `run: () -> i32` and `memory`".into(),
        ));
    }
    let mut b = WriteBatch::new();
    b.put(sys_wasm_key(name), wasm.to_vec());
    db.write_batch_unchecked(b)
}

pub(crate) fn uninstall_module(db: &Arc<DbInner>, name: &str) -> Result<()> {
    validate_module_name(name)?;
    if db.get_at_seq(&sys_wasm_key(name), crate::types::MAX_SEQNO)?.is_none() {
        return Err(Error::InvalidArgument(format!("no module named {name:?}")));
    }
    let mut b = WriteBatch::new();
    b.delete(sys_wasm_key(name));
    db.write_batch_unchecked(b)
}

pub(crate) fn list_modules(db: &Arc<DbInner>) -> Result<Vec<ModuleInfo>> {
    let prefix = sys_wasm_key("");
    let mut hi = prefix.clone();
    *hi.last_mut().unwrap() += 1; // \x00wasm\x01
    let it = db.iter_raw(None, &prefix, Some(hi), false)?;
    let mut out = Vec::new();
    for kv in it {
        let (k, v) = kv?;
        let name = String::from_utf8_lossy(&k[prefix.len()..]).into_owned();
        out.push(ModuleInfo {
            name,
            size: v.len(),
            content_hash: content_hash(&v),
        });
    }
    Ok(out)
}

fn load_module_at(db: &Arc<DbInner>, name: &str, seq: SeqNo) -> Result<Module> {
    validate_module_name(name)?;
    let bytes = db
        .get_at_seq(&sys_wasm_key(name), seq)?
        .ok_or_else(|| Error::InvalidArgument(format!("no module named {name:?}")))?;
    db.wasm.cache.lock().get_or_compile(&db.wasm.engine, &bytes)
}

struct SnapGuard {
    db: Arc<DbInner>,
    seq: SeqNo,
    registered: bool,
}

impl Drop for SnapGuard {
    fn drop(&mut self) {
        if self.registered {
            self.db.deregister_snapshot(self.seq);
        }
    }
}

fn run_instance(
    db: &Arc<DbInner>,
    module: &Module,
    ctx: HostCtx,
    entry: &str,
) -> Result<(i32, HostCtx)> {
    let rt = &db.wasm;
    let mut store = Store::new(&rt.engine, ctx);
    store
        .set_fuel(db.opts.wasm_fuel)
        .map_err(|e| Error::Wasm(format!("fuel: {e}")))?;
    store.limiter(|ctx| &mut ctx.limits);
    let instance = rt
        .linker
        .instantiate(&mut store, module)
        .map_err(|e| Error::Wasm(format!("instantiate: {e}")))?;
    let run = instance
        .get_typed_func::<(), i32>(&mut store, entry)
        .map_err(|e| Error::Wasm(format!("missing {entry}(): {e}")))?;
    match run.call(&mut store, ()) {
        Ok(code) => {
            let mut ctx = store.into_data();
            // a host-side engine error (Corruption/Io surfaced to the guest
            // as EIO) must fail the invocation even if the guest swallowed
            // the errno and exited cleanly
            if let Some(e) = ctx.host_error.take() {
                return Err(e);
            }
            Ok((code, ctx))
        }
        Err(trap) => {
            // surface a host-side error (EIO from a failing read, etc.) in
            // preference to the generic trap text
            let ctx = store.into_data();
            match ctx.host_error {
                Some(e) => Err(e),
                None => Err(Error::Wasm(format!("trap: {trap:#}"))),
            }
        }
    }
}

pub(crate) fn query(
    db: &Arc<DbInner>,
    name: &str,
    input: &[u8],
    at: Option<SeqNo>,
) -> Result<Vec<u8>> {
    if input.len() > db.opts.max_wasm_input {
        return Err(Error::InvalidArgument("input exceeds max_wasm_input".into()));
    }
    // pin a snapshot for the whole invocation so GC cannot outrun the guest
    let guard = match at {
        Some(seq) => SnapGuard {
            db: db.clone(),
            seq,
            registered: false,
        },
        None => {
            let seq = db.register_snapshot();
            SnapGuard {
                db: db.clone(),
                seq,
                registered: true,
            }
        }
    };
    let module = load_module_at(db, name, guard.seq)?;
    let ctx = HostCtx::new(db.clone(), Access::ReadOnly(guard.seq), input.to_vec());
    let (code, ctx) = run_instance(db, &module, ctx, "run")?;
    if code != 0 {
        return Err(Error::GuestFailed {
            code,
            output: ctx.output,
        });
    }
    Ok(ctx.output)
}

pub(crate) fn execute(db: &Arc<DbInner>, name: &str, input: &[u8]) -> Result<Vec<u8>> {
    if input.len() > db.opts.max_wasm_input {
        return Err(Error::InvalidArgument("input exceeds max_wasm_input".into()));
    }
    let attempts = db.opts.execute_retries.max(1);
    for _ in 0..attempts {
        // fresh everything per attempt: snapshot, txn, store, fuel, output
        let txn = Txn::new(db.clone());
        let module = load_module_at(db, name, txn.snapshot_seqno())?;
        let ctx = HostCtx::new(db.clone(), Access::Txn(Some(txn)), input.to_vec());
        let (code, mut ctx) = run_instance(db, &module, ctx, "run")?;
        let txn = match ctx.access {
            Access::Txn(ref mut t) => t.take().expect("txn present"),
            _ => unreachable!(),
        };
        if code != 0 {
            return Err(Error::GuestFailed {
                code,
                output: ctx.output,
            });
        }
        match txn.commit() {
            Ok(()) => return Ok(ctx.output),
            Err(Error::Conflict) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::Conflict)
}

/// Run a compiled module's optional `describe` export — same `() -> i32`
/// ABI as `run`, read-only access at `seq`, empty input — and return its
/// output bytes. `Ok(None)` when the module exports no `describe` function.
fn describe_compiled(db: &Arc<DbInner>, module: &Module, seq: SeqNo) -> Result<Option<Vec<u8>>> {
    let has_describe = module
        .get_export("describe")
        .is_some_and(|e| e.func().is_some());
    if !has_describe {
        return Ok(None);
    }
    let ctx = HostCtx::new(db.clone(), Access::ReadOnly(seq), Vec::new());
    let (code, ctx) = run_instance(db, module, ctx, "describe")?;
    if code != 0 {
        return Err(Error::GuestFailed {
            code,
            output: ctx.output,
        });
    }
    Ok(Some(ctx.output))
}

/// `describe` an installed module by name.
pub(crate) fn describe_module(db: &Arc<DbInner>, name: &str) -> Result<Option<Vec<u8>>> {
    let guard = SnapGuard {
        db: db.clone(),
        seq: db.register_snapshot(),
        registered: true,
    };
    let module = load_module_at(db, name, guard.seq)?;
    describe_compiled(db, &module, guard.seq)
}

/// `describe` candidate module bytes without installing them (install-time
/// validation of the declared schema).
pub(crate) fn describe_wasm(db: &Arc<DbInner>, wasm: &[u8]) -> Result<Option<Vec<u8>>> {
    // compile WITHOUT touching the shared ModuleCache: candidate bytes may
    // be rejected and must not evict installed modules' compiled artifacts
    let module = Module::new(&db.wasm.engine, wasm)
        .map_err(|e| Error::Wasm(format!("compile: {e}")))?;
    let guard = SnapGuard {
        db: db.clone(),
        seq: db.register_snapshot(),
        registered: true,
    };
    describe_compiled(db, &module, guard.seq)
}
