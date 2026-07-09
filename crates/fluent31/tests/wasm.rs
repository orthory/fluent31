//! WASM layer tests: WAT-based ABI conformance plus end-to-end runs of the
//! real Rust guests (built for wasm32-unknown-unknown from `guests/`).
#![cfg(feature = "wasm")]

use std::path::PathBuf;
use std::sync::Once;

use fluent31::{Db, Error, Options};

fn opts() -> Options {
    Options {
        sync: fluent31::SyncMode::Never, // macOS F_FULLFSYNC is ~15ms/op
        memtable_size: 32 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------
// WAT conformance
// ---------------------------------------------------------------------------

const ECHO: &str = r#"
(module
  (import "fluent" "input_len" (func $input_len (result i32)))
  (import "fluent" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (local $n i32)
    (local.set $n (call $input_len))
    (drop (call $input_read (i32.const 0) (local.get $n) (i32.const 0)))
    (drop (call $output_write (i32.const 0) (local.get $n)))
    (i32.const 0)))
"#;

/// Puts key "wk" = input bytes (executor use).
const PUT_INPUT: &str = r#"
(module
  (import "fluent" "input_len" (func $input_len (result i32)))
  (import "fluent" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "wk")
  (func (export "run") (result i32)
    (local $n i32)
    (local.set $n (call $input_len))
    (drop (call $input_read (i32.const 0) (local.get $n) (i32.const 0)))
    (call $put (i32.const 1024) (i32.const 2) (i32.const 0) (local.get $n))))
"#;

/// Tries to put from a read-only query; returns the errno as exit code.
const PUT_ALWAYS: &str = r#"
(module
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "k")
  (func (export "run") (result i32)
    (call $put (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1))))
"#;

/// Tries to write into the reserved 0x00 keyspace.
const PUT_RESERVED: &str = r#"
(module
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "\00wasm\00agg")
  (func (export "run") (result i32)
    (call $put (i32.const 0) (i32.const 9) (i32.const 0) (i32.const 4))))
"#;

const INFINITE_LOOP: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (loop $l (br $l))
    (i32.const 0)))
"#;

/// get("gk") and echo the value; exit 1 when missing.
const GET_ECHO: &str = r#"
(module
  (import "fluent" "get" (func $get (param i32 i32 i32 i32 i32) (result i64)))
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "gk")
  (func (export "run") (result i32)
    (local $r i64)
    (local.set $r (call $get (i32.const 0) (i32.const 2) (i32.const 0) (i32.const 128) (i32.const 4096)))
    (if (i64.lt_s (local.get $r) (i64.const 0)) (then (return (i32.const 1))))
    (drop (call $output_write (i32.const 128) (i32.wrap_i64 (local.get $r))))
    (i32.const 0)))
"#;

/// Reads out of bounds — must trap, not corrupt.
const OOB_READ: &str = r#"
(module
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (call $put (i32.const 0) (i32.const -1) (i32.const 0) (i32.const 1))))
"#;

/// Emits "v1" / "v2" — used for module versioning tests.
fn version_module(tag: &str) -> String {
    format!(
        r#"
(module
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "{tag}")
  (func (export "run") (result i32)
    (drop (call $output_write (i32.const 0) (i32.const 2)))
    (i32.const 0)))
"#
    )
}

#[test]
fn wat_echo_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("echo", ECHO.as_bytes()).unwrap();
    let out = db.query("echo", b"hello wasm").unwrap();
    assert_eq!(out, b"hello wasm");
    // executors can run read-only modules too
    let out = db.execute("echo", b"exec path").unwrap();
    assert_eq!(out, b"exec path");
}

#[test]
fn wat_executor_writes_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("putter", PUT_INPUT.as_bytes()).unwrap();
    let out = db.execute("putter", b"stored-value").unwrap();
    assert!(out.is_empty());
    assert_eq!(db.get(b"wk").unwrap().unwrap(), b"stored-value");
}

#[test]
fn wat_query_is_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("w", PUT_ALWAYS.as_bytes()).unwrap();
    // query: put must fail with EROFS (-2), surfaced as the guest exit code
    match db.query("w", b"") {
        Err(Error::GuestFailed { code, .. }) => assert_eq!(code, -2),
        other => panic!("expected GuestFailed(EROFS), got {other:?}"),
    }
    // executor: same module commits fine
    db.execute("w", b"").unwrap();
    assert_eq!(db.get(b"k").unwrap().unwrap(), b"k"); // value is first byte of memory = "k"
}

#[test]
fn wat_reserved_keyspace_is_walled_off() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("evil", PUT_RESERVED.as_bytes()).unwrap();
    match db.execute("evil", b"") {
        Err(Error::GuestFailed { code, .. }) => assert_eq!(code, -3), // EINVAL
        other => panic!("expected GuestFailed(EINVAL), got {other:?}"),
    }
}

#[test]
fn wat_fuel_exhaustion_traps() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.wasm_fuel = 100_000;
    let db = Db::open(dir.path(), o).unwrap();
    db.install_module("spin", INFINITE_LOOP.as_bytes()).unwrap();
    match db.query("spin", b"") {
        Err(Error::Wasm(msg)) => assert!(msg.contains("fuel"), "unexpected trap: {msg}"),
        other => panic!("expected fuel trap, got {other:?}"),
    }
}

#[test]
fn wat_out_of_bounds_pointer_traps() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("oob", OOB_READ.as_bytes()).unwrap();
    match db.execute("oob", b"") {
        Err(Error::Wasm(_)) => {}
        other => panic!("expected trap, got {other:?}"),
    }
    // the engine is fully usable afterwards
    db.put(b"still".to_vec(), b"alive".to_vec()).unwrap();
    assert_eq!(db.get(b"still").unwrap().unwrap(), b"alive");
}

#[test]
fn wat_get_echo_sees_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("g", GET_ECHO.as_bytes()).unwrap();
    match db.query("g", b"") {
        Err(Error::GuestFailed { code, .. }) => assert_eq!(code, 1), // missing
        other => panic!("expected code 1, got {other:?}"),
    }
    db.put(b"gk".to_vec(), b"snap-value".to_vec()).unwrap();
    assert_eq!(db.query("g", b"").unwrap(), b"snap-value");

    // query_at travels back in time (data AND module resolution)
    let snap = db.snapshot();
    db.put(b"gk".to_vec(), b"newer".to_vec()).unwrap();
    assert_eq!(db.query("g", b"").unwrap(), b"newer");
    assert_eq!(db.query_at("g", b"", &snap).unwrap(), b"snap-value");
}

#[test]
fn module_versioning_time_travel() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("ver", version_module("v1").as_bytes()).unwrap();
    let snap = db.snapshot();
    db.install_module("ver", version_module("v2").as_bytes()).unwrap();
    assert_eq!(db.query("ver", b"").unwrap(), b"v2");
    assert_eq!(db.query_at("ver", b"", &snap).unwrap(), b"v1");

    let mods = db.list_modules().unwrap();
    assert_eq!(mods.len(), 1);
    assert_eq!(mods[0].name, "ver");

    db.uninstall_module("ver").unwrap();
    assert!(db.list_modules().unwrap().is_empty());
    assert!(matches!(db.query("ver", b""), Err(Error::InvalidArgument(_))));
    // old snapshot still resolves the uninstalled module
    assert_eq!(db.query_at("ver", b"", &snap).unwrap(), b"v1");
}

#[test]
fn modules_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), opts()).unwrap();
        db.install_module("echo", ECHO.as_bytes()).unwrap();
    }
    let db = Db::open(dir.path(), opts()).unwrap();
    assert_eq!(db.query("echo", b"persisted").unwrap(), b"persisted");
}

#[test]
fn install_rejects_garbage_and_bad_exports() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    assert!(matches!(
        db.install_module("bad", b"not wasm at all"),
        Err(Error::Wasm(_))
    ));
    // valid wasm but no run/memory exports
    let no_exports = "(module)";
    assert!(matches!(
        db.install_module("bad", no_exports.as_bytes()),
        Err(Error::Wasm(_))
    ));
}

// ---------------------------------------------------------------------------
// Real Rust guests
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // crates/fluent31 -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the guest crates once per test-process run. The wasm32 std lives in
/// the rustup toolchain, so point cargo at rustup's rustc when available
/// (PATH may hold a differently-provisioned cargo, e.g. Homebrew's).
fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILD: Once = Once::new();
    let root = workspace_root();
    BUILD.call_once(|| {
        let mut cmd = std::process::Command::new("cargo");
        if let Ok(out) = std::process::Command::new("rustup")
            .args(["which", "rustc"])
            .output()
        {
            if out.status.success() {
                let rustc = String::from_utf8_lossy(&out.stdout).trim().to_string();
                cmd.env("RUSTC", rustc);
            }
        }
        let status = cmd
            .args([
                "build",
                "--manifest-path",
                root.join("guests/Cargo.toml").to_str().unwrap(),
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                // pin the artifact location: a CARGO_TARGET_DIR override in
                // the environment must not move it
                "--target-dir",
                root.join("guests/target").to_str().unwrap(),
            ])
            .env_remove("CARGO_TARGET_DIR")
            .status()
            .expect("cargo build for guests");
        assert!(status.success(), "guest build failed");
    });
    std::fs::read(
        root.join("guests/target/wasm32-unknown-unknown/release")
            .join(format!("{name}.wasm")),
    )
    .expect("guest artifact")
}

#[test]
fn rust_guest_agg_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("agg", &guest_wasm("agg")).unwrap();

    let mut expect_sum = 0u64;
    for i in 0..500u64 {
        let val = i * 7;
        expect_sum += val;
        db.put(
            format!("metric/{i:05}").into_bytes(),
            val.to_le_bytes().to_vec(),
        )
        .unwrap();
    }
    // unrelated keys must not be counted
    db.put(b"other/1".to_vec(), 999u64.to_le_bytes().to_vec()).unwrap();
    // spread across memtable + tables
    db.flush().unwrap();
    for i in 500..600u64 {
        let val = i * 7;
        expect_sum += val;
        db.put(
            format!("metric/{i:05}").into_bytes(),
            val.to_le_bytes().to_vec(),
        )
        .unwrap();
    }

    let out = db.query("agg", b"metric/").unwrap();
    assert_eq!(out.len(), 40);
    let word = |i: usize| u64::from_le_bytes(out[i * 8..(i + 1) * 8].try_into().unwrap());
    assert_eq!(word(0), 600); // count
    assert_eq!(word(1), 600); // summed
    assert_eq!(word(2), expect_sum);
    assert_eq!(word(3), 0); // min
    assert_eq!(word(4), 599 * 7); // max
}

#[test]
fn rust_guest_transfer_concurrent_occ() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.execute_retries = 50; // hot conflicts expected — let OCC grind through
    let db = std::sync::Arc::new(Db::open(dir.path(), o).unwrap());
    db.install_module("transfer", &guest_wasm("transfer")).unwrap();

    let accounts: Vec<Vec<u8>> = (0..4).map(|i| format!("acct/{i}").into_bytes()).collect();
    for a in &accounts {
        db.put(a.clone(), 1_000u64.to_le_bytes().to_vec()).unwrap();
    }

    let input = |from: &[u8], to: &[u8], amount: u64| -> Vec<u8> {
        let mut v = Vec::new();
        v.push(from.len() as u8);
        v.extend_from_slice(from);
        v.push(to.len() as u8);
        v.extend_from_slice(to);
        v.extend_from_slice(&amount.to_le_bytes());
        v
    };

    let mut handles = Vec::new();
    for t in 0..4usize {
        let db = db.clone();
        let accounts = accounts.clone();
        handles.push(std::thread::spawn(move || {
            let mut ok = 0;
            for i in 0..50usize {
                let from = &accounts[(t + i) % 4];
                let to = &accounts[(t + i + 1) % 4];
                match db.execute("transfer", &input(from, to, 3)) {
                    Ok(_) => ok += 1,
                    Err(Error::GuestFailed { code: 1, .. }) => {} // insufficient
                    Err(Error::Conflict) => {}                    // retries exhausted
                    Err(e) => panic!("transfer failed: {e}"),
                }
            }
            ok
        }));
    }
    let succeeded: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(succeeded > 0, "no transfer ever succeeded");

    // conservation law: total balance unchanged whatever interleaving happened
    let total: u64 = accounts
        .iter()
        .map(|a| {
            u64::from_le_bytes(db.get(a).unwrap().unwrap()[..8].try_into().unwrap())
        })
        .sum();
    assert_eq!(total, 4_000);
}

#[test]
fn rust_guest_transfer_insufficient_funds() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("transfer", &guest_wasm("transfer")).unwrap();
    db.put(b"a".to_vec(), 10u64.to_le_bytes().to_vec()).unwrap();
    db.put(b"b".to_vec(), 0u64.to_le_bytes().to_vec()).unwrap();

    let mut input = vec![1u8, b'a', 1u8, b'b'];
    input.extend_from_slice(&100u64.to_le_bytes());
    match db.execute("transfer", &input) {
        Err(Error::GuestFailed { code: 1, .. }) => {}
        other => panic!("expected insufficient-funds exit, got {other:?}"),
    }
    // aborted: no partial writes
    assert_eq!(db.get(b"a").unwrap().unwrap(), 10u64.to_le_bytes());
    assert_eq!(db.get(b"b").unwrap().unwrap(), 0u64.to_le_bytes());
}

/// The `claim` example guest: N concurrent claimers of one username — the
/// get_for_update conflict set guarantees exactly one winner; the losers
/// fail attributably (code 1) and the winner's re-claim is idempotent.
#[test]
fn rust_guest_claim_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.execute_retries = 50;
    let db = std::sync::Arc::new(Db::open(dir.path(), o).unwrap());
    db.install_module("claim", &guest_wasm("claim")).unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let handles: Vec<_> = (0..8usize)
        .map(|i| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let input = format!(r#"{{"username":"neo","owner":"racer-{i}"}}"#);
                barrier.wait();
                (i, db.execute("claim", input.as_bytes()))
            })
        })
        .collect();
    let mut winner = None;
    let mut losses = 0;
    for h in handles {
        match h.join().unwrap() {
            (i, Ok(_)) => assert!(winner.replace(i).is_none(), "two winners"),
            (_, Err(Error::GuestFailed { code: 1, output })) => {
                losses += 1;
                assert!(String::from_utf8_lossy(&output).contains("taken by racer-"));
            }
            (i, Err(e)) => panic!("racer {i}: {e}"),
        }
    }
    let winner = winner.expect("someone wins");
    assert_eq!(losses, 7);
    assert_eq!(
        db.get(b"uname/neo").unwrap().unwrap(),
        format!("racer-{winner}").into_bytes()
    );

    // idempotent re-claim by the holder; still taken for everyone else
    let again = format!(r#"{{"username":"neo","owner":"racer-{winner}"}}"#);
    let out = db.execute("claim", again.as_bytes()).unwrap();
    assert!(String::from_utf8_lossy(&out).contains(r#""already":true"#));
}
