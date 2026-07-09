//! Write-range trigger robustness: the guarantees that make triggers safe to
//! lean on — self-recursion is structurally bounded, a runtime-failing module
//! backs off and recovers without losing events, a high-volume storm still
//! converges with coalescing, and a backlog drained across a crash finishes
//! exactly-once.
//!
//! Complements the engine suite (fire/drain, coalesce+reopen, no cross-trigger
//! stacking, admin validation) with the failure and load paths.

#![cfg(feature = "wasm")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fluent31::{Db, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 64 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

/// Trigger target (same contract as the engine suite's): records the packed
/// input at `idx/last` and mirrors each touched key `<k>` to `m/<k>` = `<k>`.
/// Single-byte uvarint key lengths (keys < 128 bytes).
const MIRROR_WAT: &str = r#"
(module
  (import "fluent" "input_len" (func $ilen (result i32)))
  (import "fluent" "input_read" (func $iread (param i32 i32 i32) (result i32)))
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 2)
  (data (i32.const 0) "idx/last")
  (func (export "run") (result i32)
    (local $len i32) (local $off i32) (local $klen i32)
    (local.set $len (call $ilen))
    (drop (call $iread (i32.const 1024) (local.get $len) (i32.const 0)))
    (drop (call $put (i32.const 0) (i32.const 8) (i32.const 1024) (local.get $len)))
    (local.set $off (i32.const 0))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $off) (local.get $len)))
        (local.set $klen (i32.load8_u (i32.add (i32.const 1024) (local.get $off))))
        (local.set $off (i32.add (local.get $off) (i32.const 1)))
        (i32.store8 (i32.const 8192) (i32.const 109)) ;; 'm'
        (i32.store8 (i32.const 8193) (i32.const 47))  ;; '/'
        (memory.copy (i32.const 8194)
                     (i32.add (i32.const 1024) (local.get $off))
                     (local.get $klen))
        (drop (call $put (i32.const 8192) (i32.add (local.get $klen) (i32.const 2))
                         (i32.add (i32.const 1024) (local.get $off)) (local.get $klen)))
        (local.set $off (i32.add (local.get $off) (local.get $klen)))
        (br $next)))
    (i32.const 0)))
"#;

/// Trigger target that always traps.
const TRAP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32) (unreachable)))
"#;

fn wait_until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "not reached within {secs}s: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn pending(db: &Db, name: &str) -> u64 {
    db.list_triggers()
        .unwrap()
        .into_iter()
        .find(|t| t.name == name)
        .map(|t| t.pending)
        .unwrap_or_else(|| panic!("no trigger named {name}"))
}

fn last_error(db: &Db, name: &str) -> Option<String> {
    db.list_triggers()
        .unwrap()
        .into_iter()
        .find(|t| t.name == name)
        .and_then(|t| t.last_error)
}

// ---------------------------------------------------------------------------
// Self-recursion is structurally bounded (the no-stacking rule, direct case)
// ---------------------------------------------------------------------------

/// A trigger whose module writes back INTO its own subscribed range. Trigger
/// commits are system transactions that emit no events, so the module's own
/// output cannot re-fire it: one user write drives exactly one mirror and the
/// queue drains to zero instead of looping forever.
#[test]
fn trigger_writing_into_its_own_range_does_not_loop() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    // range [m/, n) — the very space the module mirrors into
    db.create_trigger("self", "mirror", Some(b"m/"), Some(b"n")).unwrap();

    db.put(b"m/seed".to_vec(), b"1".to_vec()).unwrap();

    // the mirror of the user write lands...
    wait_until("mirror of m/seed", 10, || db.get(b"m/m/seed").unwrap().is_some());
    // ...and the queue settles instead of cascading
    wait_until("queue drains, no cascade", 10, || pending(&db, "self") == 0);

    assert_eq!(db.get(b"m/m/seed").unwrap().unwrap(), b"m/seed");
    // the mirror write (a system write) never re-fired: no second-order mirror
    assert!(db.get(b"m/m/m/seed").unwrap().is_none(), "self-recursion cascaded");
    assert_eq!(last_error(&db, "self"), None);

    // stays quiet: pending is genuinely zero, not momentarily
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(pending(&db, "self"), 0);
    assert!(db.get(b"m/m/m/seed").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// A runtime-failing module backs off, keeps its events, and recovers
// ---------------------------------------------------------------------------

/// Distinct from the missing-module path: the module is present but TRAPS at
/// runtime. The drain fails, the error surfaces on the trigger, the events
/// stay queued (nothing lost), and replacing the module with a working one
/// drains the whole backlog.
#[test]
fn trapping_trigger_module_surfaces_error_then_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("idx", TRAP_WAT.as_bytes()).unwrap();
    db.create_trigger("t", "idx", Some(b"u/"), Some(b"v")).unwrap();

    for i in 0..5u32 {
        db.put(format!("u/{i}").into_bytes(), b"1".to_vec()).unwrap();
    }

    // the trap surfaces as last_error and the backlog is retained, not dropped
    wait_until("trap surfaces on the trigger", 10, || {
        last_error(&db, "t").is_some() && pending(&db, "t") == 5
    });
    let err = last_error(&db, "t").unwrap();
    assert!(err.contains("trap") || err.contains("wasm"), "unexpected: {err}");

    // repair: overwrite the module name with a working one; the runner picks
    // up the new bytes on its next attempt and drains the retained backlog
    db.install_module("idx", MIRROR_WAT.as_bytes()).unwrap();
    wait_until("backlog drains after repair", 15, || pending(&db, "t") == 0);
    for i in 0..5u32 {
        let mk = format!("m/u/{i}").into_bytes();
        assert_eq!(db.get(&mk).unwrap().unwrap(), format!("u/{i}").into_bytes());
    }
    assert_eq!(last_error(&db, "t"), None, "error clears after a clean drain");
}

// ---------------------------------------------------------------------------
// High-volume storm converges with coalescing
// ---------------------------------------------------------------------------

/// Many threads re-touch a shared key space faster than the runner drains,
/// forcing heavy coalescing and multi-batch drains (small trigger_batch). The
/// module converges: every key ends with exactly its mirror, no more, no less.
#[test]
fn trigger_storm_coalesces_and_converges() {
    const KEYS: u32 = 250;
    const THREADS: usize = 8;
    const PASSES: usize = 3;

    let dir = tempfile::tempdir().unwrap();
    let o = Options {
        trigger_batch: 16, // force the chunked drain loop to iterate a lot
        ..opts()
    };
    let db = Arc::new(Db::open(dir.path(), o).unwrap());
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    db.create_trigger("s", "mirror", Some(b"u/"), Some(b"v")).unwrap();

    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..PASSES {
                for i in 0..KEYS {
                    db.put(format!("u/{i:04}").into_bytes(), b"v".to_vec()).unwrap();
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    wait_until("storm backlog drains", 30, || pending(&db, "s") == 0);

    // convergence: every touched key has exactly its mirror
    for i in 0..KEYS {
        let uk = format!("u/{i:04}").into_bytes();
        let mk = format!("m/u/{i:04}").into_bytes();
        assert_eq!(db.get(&mk).unwrap().unwrap(), uk, "mirror {i}");
    }
    // and there are no stray mirrors beyond the touched set
    let mirrors = db.iter(Some(b"m/"), Some(b"m0"), false).unwrap().count();
    assert_eq!(mirrors, KEYS as usize, "exactly one mirror per key");
    assert_eq!(last_error(&db, "s"), None);
}

// ---------------------------------------------------------------------------
// A backlog drained across a crash finishes exactly-once
// ---------------------------------------------------------------------------

/// The queue is a durable key range: dropping the db mid-drain (Drop stops the
/// runner wherever it is) then reopening resumes the drain and finishes every
/// event. Effects are exactly-once — the queue-entry delete commits atomically
/// with the module's write — so the final state is complete and correct.
#[test]
fn backlog_drains_completely_across_reopen_mid_drain() {
    const KEYS: u32 = 300;
    let dir = tempfile::tempdir().unwrap();
    {
        let o = Options {
            trigger_batch: 8, // many small drains => likely to catch it mid-flight
            ..opts()
        };
        let db = Db::open(dir.path(), o).unwrap();
        db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
        db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();
        for i in 0..KEYS {
            db.put(format!("u/{i:04}").into_bytes(), b"v".to_vec()).unwrap();
        }
        // drop immediately — the runner has almost certainly not finished
        drop(db);
    }

    // reopen: the retained backlog resumes and completes
    let db = Db::open(dir.path(), opts()).unwrap();
    wait_until("recovered backlog fully drains", 30, || pending(&db, "t") == 0);
    for i in 0..KEYS {
        let uk = format!("u/{i:04}").into_bytes();
        let mk = format!("m/u/{i:04}").into_bytes();
        assert_eq!(db.get(&mk).unwrap().unwrap(), uk, "mirror {i} after reopen");
    }
    let mirrors = db.iter(Some(b"m/"), Some(b"m0"), false).unwrap().count();
    assert_eq!(mirrors, KEYS as usize);
}

// ---------------------------------------------------------------------------
// Concurrent create/delete churn while writes fire never wedges the runner
// ---------------------------------------------------------------------------

/// Admin churn (repeated create/delete of a trigger) racing live writes must
/// never panic, deadlock, or leave orphaned queue state that spins the runner.
#[test]
fn trigger_admin_churn_races_writes_safely() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts()).unwrap());
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();

    let writes = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let db = db.clone();
        let writes = writes.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Acquire) {
                db.put(format!("u/{i:05}").into_bytes(), b"v".to_vec()).unwrap();
                writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        })
    };

    // churn create/delete of a trigger over the range writes are landing in
    for _ in 0..20 {
        db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        db.delete_trigger("t").unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Release);
    writer.join().unwrap();
    assert!(writes.load(Ordering::Relaxed) > 0);

    // final steady state: create once, let it drain, verify it works and the
    // runner is quiet (no wedge from all the churn)
    db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();
    db.put(b"u/final".to_vec(), b"v".to_vec()).unwrap();
    wait_until("final trigger fires and drains", 15, || {
        db.get(b"m/u/final").unwrap().is_some() && pending(&db, "t") == 0
    });
    assert_eq!(last_error(&db, "t"), None);
}
