//! WASM reliability under runaway / buggy / malformed guests.
//!
//! This is NOT a security-boundary suite — fluent31 is not public-facing and
//! access control is a separate layer. What these assert is *integrity*: a
//! guest that spins, bombs memory, floods output, exhausts the write set,
//! traps, or was never valid to begin with must fail through a clean typed
//! error, leave the store uncorrupted, and never wedge or crash the engine.
//! Every resource limit fires as an errno/trap the guest can observe, and the
//! engine is fully usable the instant the invocation returns.

#![cfg(feature = "wasm")]

use fluent31::{Db, Error, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 32 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

/// A do-nothing valid module: memory + `run` returning 0.
const NOOP: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32) (i32.const 0)))
"#;

// ---------------------------------------------------------------------------
// Resource limits fire cleanly and leave the engine usable
// ---------------------------------------------------------------------------

/// Grows memory far past the cap (denied), then touches an address it never
/// obtained — an out-of-bounds trap. Whether the limiter denies the grow or
/// the store faults, the result is one clean Wasm error and a bounded host.
const MEMORY_BOMB: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (drop (memory.grow (i32.const 10000)))       ;; ask for ~640 MiB; capped
    (i32.store (i32.const 0x20000000) (i32.const 1)) ;; 512 MiB: OOB -> trap
    (i32.const 0)))
"#;

#[test]
fn memory_growth_is_capped_and_traps_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.wasm_memory_limit = 1 << 20; // 1 MiB ceiling
    let db = Db::open(dir.path(), o).unwrap();
    db.install_module("bomb", MEMORY_BOMB.as_bytes()).unwrap();
    match db.query("bomb", b"") {
        Err(Error::Wasm(_)) => {}
        other => panic!("expected clean Wasm trap, got {other:?}"),
    }
    // engine unharmed
    db.put(b"live".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(db.get(b"live").unwrap().unwrap(), b"1");
}

/// Writes 4 KiB of output in a loop; reports 42 the first time output_write
/// signals ENOSPC (the max_wasm_output ceiling).
const OUTPUT_FLOOD: &str = r#"
(module
  (import "fluent" "output_write" (func $ow (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (local $i i32)
    (loop $l
      (if (i32.ne (call $ow (i32.const 0) (i32.const 4096)) (i32.const 0))
        (then (return (i32.const 42))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 100000))))
    (i32.const 0)))
"#;

#[test]
fn output_cap_stops_a_flood() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_wasm_output = 16 << 10; // 4 writes fit, the 5th trips
    let db = Db::open(dir.path(), o.clone()).unwrap();
    db.install_module("flood", OUTPUT_FLOOD.as_bytes()).unwrap();
    match db.query("flood", b"") {
        Err(Error::GuestFailed { code: 42, output }) => {
            assert!(output.len() <= o.max_wasm_output, "output exceeded cap");
        }
        other => panic!("expected ENOSPC-driven exit 42, got {other:?}"),
    }
    db.put(b"ok".to_vec(), b"1".to_vec()).unwrap();
}

/// Logs 4 KiB in a loop; reports 43 when log signals ENOSPC.
const LOG_FLOOD: &str = r#"
(module
  (import "fluent" "log" (func $log (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (local $i i32)
    (loop $l
      (if (i32.ne (call $log (i32.const 0) (i32.const 0) (i32.const 4096)) (i32.const 0))
        (then (return (i32.const 43))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 100000))))
    (i32.const 0)))
"#;

#[test]
fn log_cap_stops_a_flood() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_wasm_log = 16 << 10;
    let db = Db::open(dir.path(), o).unwrap();
    db.install_module("logflood", LOG_FLOOD.as_bytes()).unwrap();
    match db.query("logflood", b"") {
        Err(Error::GuestFailed { code: 43, .. }) => {}
        other => panic!("expected ENOSPC-driven exit 43, got {other:?}"),
    }
}

/// Opens scans without closing them; reports 44 when scan_open returns ELIMIT.
const SCAN_LEAK: &str = r#"
(module
  (import "fluent" "scan_open"
    (func $so (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (local $i i32)
    (loop $l
      (if (i32.lt_s (call $so (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0))
                    (i32.const 0))
        (then (return (i32.const 44))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 100000))))
    (i32.const 0)))
"#;

#[test]
fn scan_handle_cap_stops_a_leak() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_wasm_scans = 4;
    let db = Db::open(dir.path(), o).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.install_module("scanleak", SCAN_LEAK.as_bytes()).unwrap();
    match db.query("scanleak", b"") {
        Err(Error::GuestFailed { code: 44, .. }) => {}
        other => panic!("expected ELIMIT-driven exit 44, got {other:?}"),
    }
}

/// Executor that puts distinct 256-byte records forever; reports 45 when put
/// returns ENOSPC (the max_txn_write_bytes ceiling). Key = 'k' + i32 counter.
const WRITESET_FLOOD: &str = r#"
(module
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "k")
  (func (export "run") (result i32)
    (local $i i32)
    (loop $l
      (i32.store (i32.const 1) (local.get $i))   ;; counter into key[1..5]
      (if (i32.ne (call $put (i32.const 0) (i32.const 5) (i32.const 16) (i32.const 256))
                  (i32.const 0))
        (then (return (i32.const 45))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $l (i32.lt_u (local.get $i) (i32.const 100000))))
    (i32.const 0)))
"#;

#[test]
fn executor_write_set_cap_stops_runaway_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.max_txn_write_bytes = 8 << 10;
    let db = Db::open(dir.path(), o).unwrap();
    db.install_module("wflood", WRITESET_FLOOD.as_bytes()).unwrap();
    match db.execute("wflood", b"") {
        Err(Error::GuestFailed { code: 45, .. }) => {}
        other => panic!("expected ENOSPC-driven exit 45, got {other:?}"),
    }
    // the aborted executor committed nothing
    assert_eq!(db.iter(None, None, false).unwrap().count(), 0);
    db.put(b"z".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(db.get(b"z").unwrap().unwrap(), b"1");
}

// ---------------------------------------------------------------------------
// Malformed / hostile module bytes never crash the engine
// ---------------------------------------------------------------------------

#[test]
fn garbage_and_truncated_modules_are_rejected_at_install() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    // outright garbage
    assert!(matches!(
        db.install_module("g", b"\x00asm\xff\xff\xff\xff not wasm"),
        Err(Error::Wasm(_))
    ));
    // syntactically truncated WAT
    assert!(matches!(
        db.install_module("t", b"(module (memory (export \"memory\") 1) (func (export \"run\""),
        Err(Error::Wasm(_))
    ));
    // valid module but missing the required exports
    assert!(matches!(
        db.install_module("noexports", b"(module)"),
        Err(Error::Wasm(_))
    ));
    // a rejected install leaves no module behind
    assert!(db.list_modules().unwrap().is_empty());
}

/// Valid shape (run + memory) but imports a host function that does not exist.
/// Install compiles fine; the failure is deferred to instantiation and must be
/// a clean Wasm error, not a panic.
const BOGUS_IMPORT: &str = r#"
(module
  (import "fluent" "does_not_exist" (func $x (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32) (call $x)))
"#;

/// Imports a real host function under the wrong signature.
const WRONG_SIG_IMPORT: &str = r#"
(module
  (import "fluent" "input_len" (func $x (param i32 i32 i32 i32) (result i64)))
  (memory (export "memory") 1)
  (func (export "run") (result i32) (i32.const 0)))
"#;

/// Imports from a module the linker doesn't provide at all.
const FOREIGN_IMPORT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "proc_exit" (func $x (param i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32) (i32.const 0)))
"#;

#[test]
fn modules_with_bad_imports_fail_cleanly_at_invocation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    for (name, wat) in [
        ("bogus", BOGUS_IMPORT),
        ("wrongsig", WRONG_SIG_IMPORT),
        ("foreign", FOREIGN_IMPORT),
    ] {
        // these all compile (imports resolve at instantiate, not compile)
        db.install_module(name, wat.as_bytes()).unwrap();
        match db.query(name, b"") {
            Err(Error::Wasm(_)) => {}
            other => panic!("{name}: expected Wasm error at invocation, got {other:?}"),
        }
    }
    // engine still healthy
    db.put(b"ok".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(db.get(b"ok").unwrap().unwrap(), b"1");
}

// ---------------------------------------------------------------------------
// describe abuse
// ---------------------------------------------------------------------------

/// `describe` traps; `run` is valid.
const DESCRIBE_TRAPS: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32) (i32.const 0))
  (func (export "describe") (result i32) (unreachable)))
"#;

/// `describe` exits non-zero.
const DESCRIBE_FAILS: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32) (i32.const 0))
  (func (export "describe") (result i32) (i32.const 9)))
"#;

#[test]
fn describe_that_traps_or_fails_is_a_clean_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();

    match db.describe_wasm(DESCRIBE_TRAPS.as_bytes()) {
        Err(Error::Wasm(_)) => {}
        other => panic!("expected Wasm error from trapping describe, got {other:?}"),
    }
    match db.describe_wasm(DESCRIBE_FAILS.as_bytes()) {
        Err(Error::GuestFailed { code: 9, .. }) => {}
        other => panic!("expected GuestFailed(9), got {other:?}"),
    }
    // a module with no describe export simply reports None
    assert_eq!(db.describe_wasm(NOOP.as_bytes()).unwrap(), None);
}

// ---------------------------------------------------------------------------
// Reserved keyspace stays engine-owned across guest ops
// ---------------------------------------------------------------------------

/// Executor that deletes a reserved (0x00-prefixed) key; returns the errno.
const DELETE_RESERVED: &str = r#"
(module
  (import "fluent" "delete" (func $del (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "\00wasm\00x")
  (func (export "run") (result i32)
    (local $r i32)
    (local.set $r (call $del (i32.const 0) (i32.const 7)))
    (if (i32.lt_s (local.get $r) (i32.const 0))
      (then (return (i32.sub (i32.const 0) (local.get $r))))) ;; report |errno|
    (i32.const 0)))
"#;

#[test]
fn guest_cannot_delete_reserved_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("delres", DELETE_RESERVED.as_bytes()).unwrap();
    // EINVAL is -3, reported by the guest as exit 3
    match db.execute("delres", b"") {
        Err(Error::GuestFailed { code: 3, .. }) => {}
        other => panic!("expected EINVAL (exit 3), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Integrity survives a barrage of abusive invocations
// ---------------------------------------------------------------------------

#[test]
fn store_integrity_survives_an_abuse_barrage() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.wasm_fuel = 5_000_000;
    o.wasm_memory_limit = 1 << 20;
    o.max_wasm_output = 16 << 10;
    let db = Db::open(dir.path(), o).unwrap();

    // seed good data across memtable + tables + vlog
    for i in 0..300u32 {
        db.put(
            format!("data/{i:05}").into_bytes(),
            format!("value-{i}-{}", "p".repeat(60)).into_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // install every hostile module and hammer them
    let spin = r#"(module (memory (export "memory") 1)
        (func (export "run") (result i32) (loop $l (br $l)) (i32.const 0)))"#;
    let trap = r#"(module (memory (export "memory") 1)
        (func (export "run") (result i32) (unreachable)))"#;
    db.install_module("spin", spin.as_bytes()).unwrap();
    db.install_module("trap", trap.as_bytes()).unwrap();
    db.install_module("bomb", MEMORY_BOMB.as_bytes()).unwrap();
    db.install_module("flood", OUTPUT_FLOOD.as_bytes()).unwrap();

    for _ in 0..25 {
        let _ = db.query("spin", b"");
        let _ = db.query("trap", b"");
        let _ = db.query("bomb", b"");
        let _ = db.query("flood", b"");
        let _ = db.execute("trap", b"");
    }

    // all good data intact, no phantom keys, reopen clean
    let check = |db: &Db| {
        for i in 0..300u32 {
            assert_eq!(
                db.get(&format!("data/{i:05}").into_bytes()).unwrap().unwrap(),
                format!("value-{i}-{}", "p".repeat(60)).into_bytes(),
                "key {i}"
            );
        }
        let n = db
            .iter(Some(b"data/"), Some(b"data0"), false)
            .unwrap()
            .count();
        assert_eq!(n, 300);
    };
    check(&db);
    // a normal invocation still works after all the abuse
    db.install_module("noop", NOOP.as_bytes()).unwrap();
    assert!(db.query("noop", b"").unwrap().is_empty());
    drop(db);
    let db = Db::open(dir.path(), opts()).unwrap();
    check(&db);
}
