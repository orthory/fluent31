//! Changes-mode trigger (`on_apply`) end-to-end: the ordered, per-op,
//! value-carrying change feed. Exercises the full path — commit-critical-
//! section capture, seqno-keyed durable queue, wire-format delivery to the
//! guest's `on_apply`, exactly-once consumption — through the real
//! `order_feed` Rust guest, so host encoding and `fluent_guest` decoding
//! are proven against each other.
//!
//! The load-bearing assertion lives in `feed_matches_commit_order_*`: many
//! threads hammer ONE key, and the maximum-seqno feed entry must carry the
//! value the database actually ends with. A capture point outside the
//! commit critical section (e.g. a pre-lock counter) passes every
//! "delivered everything" check but fails this one whenever two writers
//! interleave capture and commit in opposite orders.
#![cfg(feature = "wasm")]

use std::path::PathBuf;
use std::sync::{Arc, Barrier, Once};
use std::time::{Duration, Instant};

use fluent31::{Db, Options, StreamEvent, SyncMode, TriggerMode, ValueKind, WriteBatch};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 64 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

fn workspace_root() -> PathBuf {
    // crates/fluent31 -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the guest crates once per test-process run (same recipe as
/// tests/wasm.rs: rustup's rustc so the wasm32 std resolves).
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

/// An `on_apply` module that always traps: events must queue, not drain.
const TRAP_ON_APPLY: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "on_apply") (result i32) (unreachable)))
"#;

/// A classic keys-mode module (exports `on_touch` only).
const ON_TOUCH_ONLY: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "on_touch") (result i32) (i32.const 0)))
"#;

/// Exports nothing callable: must be rejected at install.
const NO_ENTRY: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "helper") (result i32) (i32.const 0)))
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

/// The materialized feed, in key (= zero-padded seqno) order, parsed.
fn feed(db: &Db) -> Vec<serde_json::Value> {
    db.iter(Some(b"feed/"), Some(b"feed0"), false)
        .unwrap()
        .map(|kv| serde_json::from_slice(&kv.unwrap().1).unwrap())
        .collect()
}

fn order_feed_db(dir: &std::path::Path, opts: Options) -> Db {
    let db = Db::open(dir, opts).unwrap();
    db.install_module("order_feed", &guest_wasm("order_feed")).unwrap();
    let mode = db
        .create_trigger("feed", "order_feed", Some(b"orders/"), Some(b"orders0"))
        .unwrap();
    assert_eq!(mode, TriggerMode::Changes, "on_apply export selects changes mode");
    db
}

// ---------------------------------------------------------------------------
// one batch: order, kinds, values, non-coalescing
// ---------------------------------------------------------------------------

/// One batch touching the same key three times (put, put, delete) plus a
/// second key and a filtered-out counter key: the feed must show every op
/// (no coalescing), in batch order, with kinds and inline values intact,
/// under strictly increasing seqnos.
#[test]
fn feed_shows_every_op_in_order_with_kinds_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = order_feed_db(dir.path(), opts());

    let mut b = WriteBatch::new();
    b.put(b"orders/00000001".to_vec(), br#"{"v":1}"#.to_vec());
    b.put(b"orders/00000002".to_vec(), br#"{"v":2}"#.to_vec());
    b.put(b"orders/00000001".to_vec(), br#"{"v":3}"#.to_vec());
    b.delete(b"orders/00000001".to_vec());
    b.put(b"orders/next".to_vec(), b"3".to_vec()); // filtered out by the guest
    db.write(b).unwrap();

    wait_until("feed materialized", 30, || feed(&db).len() == 4);
    wait_until("queue drained", 30, || pending(&db, "feed") == 0);

    let entries = feed(&db);
    let ops: Vec<(&str, &str)> = entries
        .iter()
        .map(|e| (e["op"].as_str().unwrap(), e["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        ops,
        vec![
            ("put", "00000001"),
            ("put", "00000002"),
            ("put", "00000001"),
            ("delete", "00000001"),
        ],
        "every op delivered, batch order preserved, counter key filtered"
    );
    assert_eq!(entries[0]["record"]["v"], 1);
    assert_eq!(entries[1]["record"]["v"], 2);
    assert_eq!(entries[2]["record"]["v"], 3);
    let seqnos: Vec<u64> = entries.iter().map(|e| e["seqno"].as_u64().unwrap()).collect();
    assert!(
        seqnos.windows(2).all(|w| w[0] < w[1]),
        "seqnos strictly increase: {seqnos:?}"
    );
    assert_eq!(last_error(&db, "feed"), None);
}

// ---------------------------------------------------------------------------
// the commit-order mule
// ---------------------------------------------------------------------------

/// N writers hammer ONE key with tagged values, and the replication stream
/// — published inside the same critical section that assigns seqnos — acts
/// as commit-order ground truth. The feed must match it 1:1: same ops, same
/// values, same TRUE seqnos, same order. Deterministic detector: any
/// capture outside the commit critical section cannot know the real per-op
/// seqnos, so it fails this comparison on every run, race or no race. The
/// final-state check stays as a second, independent angle.
fn feed_matches_commit_order(sync: SyncMode) {
    const THREADS: usize = 4;
    const WRITES: usize = 50;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(order_feed_db(
        dir.path(),
        Options {
            sync,
            ..opts()
        },
    ));
    // ground truth: subscribe BEFORE any write so the stream is gap-free
    let mut sub = db.subscribe(b"orders/", Some(b"orders0")).unwrap();

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for j in 0..WRITES {
                    db.put(
                        b"orders/00000042".to_vec(),
                        format!(r#"{{"t":{t},"j":{j}}}"#).into_bytes(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    wait_until("feed complete", 60, || feed(&db).len() == THREADS * WRITES);
    wait_until("queue drained", 60, || pending(&db, "feed") == 0);

    let entries = feed(&db);
    assert_eq!(entries.len(), THREADS * WRITES, "no loss, no dup, no coalescing");
    let seqnos: Vec<u64> = entries.iter().map(|e| e["seqno"].as_u64().unwrap()).collect();
    assert!(seqnos.windows(2).all(|w| w[0] < w[1]), "feed is seqno-ordered");

    // each writer's own commits are sequential, so its j values must appear
    // in order within the feed
    for t in 0..THREADS {
        let js: Vec<u64> = entries
            .iter()
            .filter(|e| e["record"]["t"].as_u64() == Some(t as u64))
            .map(|e| e["record"]["j"].as_u64().unwrap())
            .collect();
        assert_eq!(js.len(), WRITES, "thread {t} fully represented");
        assert!(js.windows(2).all(|w| w[0] < w[1]), "thread {t} in commit order");
    }

    // THE assertion: the feed equals the replication stream — the writes
    // exactly as the engine committed them, seqnos included
    let mut stream: Vec<(u64, serde_json::Value)> = Vec::new();
    while stream.len() < THREADS * WRITES {
        match sub.recv_timeout(Duration::from_secs(10)).unwrap() {
            Some(StreamEvent::Batch(batch)) => {
                for e in batch {
                    assert_eq!(e.kind, ValueKind::Put);
                    stream.push((e.seqno, serde_json::from_slice(&e.value.unwrap()).unwrap()));
                }
            }
            Some(StreamEvent::Lagged) => panic!("ground-truth stream lagged"),
            None => panic!("ground-truth stream stalled"),
        }
    }
    assert_eq!(stream.len(), THREADS * WRITES);
    for (i, ((true_seqno, true_value), entry)) in stream.iter().zip(entries.iter()).enumerate() {
        assert_eq!(
            entry["seqno"].as_u64().unwrap(),
            *true_seqno,
            "feed entry {i} carries the op's true commit seqno"
        );
        assert_eq!(
            &entry["record"], true_value,
            "feed entry {i} carries the committed value, in commit order"
        );
    }

    // independent second angle: the last feed entry is what the store kept
    let final_record: serde_json::Value =
        serde_json::from_slice(&db.get(b"orders/00000042").unwrap().unwrap()).unwrap();
    assert_eq!(
        entries.last().unwrap()["record"], final_record,
        "the feed's last change must be the value the store ends with"
    );
}

#[test]
fn feed_matches_commit_order_direct_path() {
    feed_matches_commit_order(SyncMode::Never);
}

#[test]
fn feed_matches_commit_order_group_commit() {
    feed_matches_commit_order(SyncMode::Always);
}

// ---------------------------------------------------------------------------
// large values, install validation, mode plumbing
// ---------------------------------------------------------------------------

/// Values above trigger_inline_value arrive elided (kind put-large): the
/// feed records the fact instead of the payload; small values stay inline.
#[test]
fn oversized_values_are_elided_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = order_feed_db(
        dir.path(),
        Options {
            trigger_inline_value: 32,
            ..opts()
        },
    );

    let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(64));
    db.put(b"orders/00000001".to_vec(), big.into_bytes()).unwrap();
    db.put(b"orders/00000002".to_vec(), br#"{"v":2}"#.to_vec()).unwrap();

    wait_until("feed materialized", 30, || feed(&db).len() == 2);
    let entries = feed(&db);
    assert_eq!(entries[0]["elided"], true, "oversized value elided");
    assert_eq!(entries[0]["record"], serde_json::Value::Null);
    assert_eq!(entries[1]["record"]["v"], 2, "small value inline");
}

/// An on_apply-only module installs (no other entry required), cannot be
/// invoked as an executor, and selects changes mode; on_touch-only modules
/// stay keys mode; a module with no role entry point at all is rejected.
#[test]
fn install_validation_and_mode_detection() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();

    db.install_module("feedmod", &guest_wasm("order_feed")).unwrap();
    match db.execute("feedmod", b"") {
        Err(fluent31::Error::InvalidArgument(msg)) => {
            assert!(msg.contains("`execute`"), "names the missing entry: {msg}")
        }
        other => panic!("executing an on_apply-only module: {other:?}"),
    }
    // and a consumer-only module is no querier either
    match db.query("feedmod", b"") {
        Err(fluent31::Error::InvalidArgument(msg)) => {
            assert!(msg.contains("`query`"), "names the missing entry: {msg}")
        }
        other => panic!("querying an on_apply-only module: {other:?}"),
    }

    db.install_module("keysmod", ON_TOUCH_ONLY.as_bytes()).unwrap();
    assert_eq!(
        db.create_trigger("k", "keysmod", None, None).unwrap(),
        TriggerMode::Keys
    );
    assert_eq!(
        db.create_trigger("c", "feedmod", None, None).unwrap(),
        TriggerMode::Changes
    );
    let modes: Vec<(String, TriggerMode)> = db
        .list_triggers()
        .unwrap()
        .into_iter()
        .map(|t| (t.name, t.mode))
        .collect();
    assert!(modes.contains(&("k".into(), TriggerMode::Keys)));
    assert!(modes.contains(&("c".into(), TriggerMode::Changes)));

    match db.install_module("broken", NO_ENTRY.as_bytes()) {
        Err(fluent31::Error::Wasm(msg)) => {
            assert!(msg.contains("on_apply"), "rejection names both entries: {msg}")
        }
        other => panic!("module with no entry point installed: {other:?}"),
    }
}

/// The mode is fixed at registration: replacing the module with
/// on_touch-only bytes makes drains fail LOUDLY (lastError names on_apply,
/// events keep queueing); restoring an on_apply module drains the full
/// backlog.
#[test]
fn mode_survives_module_replacement_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let db = order_feed_db(dir.path(), opts());

    db.install_module("order_feed", ON_TOUCH_ONLY.as_bytes()).unwrap();
    db.put(b"orders/00000001".to_vec(), br#"{"v":1}"#.to_vec()).unwrap();
    wait_until("drain failure surfaces", 30, || {
        last_error(&db, "feed").is_some_and(|e| e.contains("on_apply"))
    });
    assert!(pending(&db, "feed") >= 1, "events are retained, not lost");

    db.install_module("order_feed", &guest_wasm("order_feed")).unwrap();
    wait_until("backlog drains after repair", 30, || pending(&db, "feed") == 0);
    assert_eq!(feed(&db).len(), 1);
}

// ---------------------------------------------------------------------------
// the example guests keep their promises (guards guests/{dynamic_index,
// live_stats} — the runnable examples exercise these interactively)
// ---------------------------------------------------------------------------

/// dynamic_index: writing a spec key creates a fully backfilled index,
/// record changes keep it live, spec deletion tears it down.
#[test]
fn dynamic_index_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    db.install_module("dynamic_index", &guest_wasm("dynamic_index")).unwrap();
    db.create_trigger("data", "dynamic_index", Some(b"rec/"), Some(b"rec0")).unwrap();
    db.create_trigger("spec", "dynamic_index", Some(b"idxspec/"), Some(b"idxspec0")).unwrap();
    let idle = |what| {
        wait_until(what, 30, || {
            db.list_triggers().unwrap().iter().all(|t| t.pending == 0)
        })
    };
    let index_keys = |prefix: &str| -> Vec<String> {
        let mut hi = prefix.as_bytes().to_vec();
        *hi.last_mut().unwrap() += 1;
        db.iter(Some(prefix.as_bytes()), Some(&hi), false)
            .unwrap()
            .map(|kv| String::from_utf8(kv.unwrap().0).unwrap())
            .collect()
    };

    db.put(b"rec/1".to_vec(), br#"{"customer":"acme"}"#.to_vec()).unwrap();
    db.put(b"rec/2".to_vec(), br#"{"customer":"bob"}"#.to_vec()).unwrap();
    db.put(b"idxspec/byc".to_vec(), br#"{"field":"customer"}"#.to_vec()).unwrap();
    idle("backfill");
    assert_eq!(index_keys("idx/byc/"), vec!["idx/byc/acme/1", "idx/byc/bob/2"]);

    // live maintenance: move rec/1, delete rec/2, add rec/3
    db.put(b"rec/1".to_vec(), br#"{"customer":"zorg"}"#.to_vec()).unwrap();
    db.delete(b"rec/2".to_vec()).unwrap();
    db.put(b"rec/3".to_vec(), br#"{"customer":"acme"}"#.to_vec()).unwrap();
    idle("live maintenance");
    assert_eq!(index_keys("idx/byc/"), vec!["idx/byc/acme/3", "idx/byc/zorg/1"]);

    // teardown removes the index AND its bookkeeping
    db.delete(b"idxspec/byc".to_vec()).unwrap();
    idle("teardown");
    assert!(index_keys("idx/").is_empty());
    assert!(index_keys("idxptr/").is_empty());
    assert!(index_keys("idxcur/").is_empty());
}

/// live_stats: folded per-group aggregates exactly match a from-scratch
/// recount after a concurrent storm of upserts, moves, and deletes.
#[test]
fn live_stats_fold_is_exact_under_storm() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts()).unwrap());
    db.install_module("live_stats", &guest_wasm("live_stats")).unwrap();
    db.create_trigger("stats", "live_stats", Some(b"ord/"), Some(b"ord0")).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2u64)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for j in 0..50u64 {
                    let key = format!("ord/{:02}", (t * 50 + j * 7) % 10);
                    if (t + j) % 5 == 4 {
                        let _ = db.delete(key.into_bytes());
                    } else {
                        let customer = ["acme", "bob", "zorg"][((t + j) % 3) as usize];
                        let rec = format!(r#"{{"customer":"{customer}","cents":{}}}"#, j + 1);
                        db.put(key.into_bytes(), rec.into_bytes()).unwrap();
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    wait_until("storm drained", 60, || {
        db.list_triggers().unwrap().iter().all(|t| t.pending == 0)
    });
    assert_eq!(last_error(&db, "stats"), None);

    let mut expected: std::collections::BTreeMap<String, (i64, i64)> = Default::default();
    for kv in db.iter(Some(b"ord/"), Some(b"ord0"), false).unwrap() {
        let (_, v) = kv.unwrap();
        let rec: serde_json::Value = serde_json::from_slice(&v).unwrap();
        let e = expected
            .entry(rec["customer"].as_str().unwrap().to_string())
            .or_default();
        e.0 += 1;
        e.1 += rec["cents"].as_i64().unwrap();
    }
    let mut folded: std::collections::BTreeMap<String, (i64, i64)> = Default::default();
    for kv in db.iter(Some(b"stat/"), Some(b"stat0"), false).unwrap() {
        let (k, v) = kv.unwrap();
        let s: serde_json::Value = serde_json::from_slice(&v).unwrap();
        folded.insert(
            String::from_utf8(k[b"stat/".len()..].to_vec()).unwrap(),
            (s["orders"].as_i64().unwrap(), s["cents"].as_i64().unwrap()),
        );
    }
    assert_eq!(folded, expected, "folded stats drifted from ground truth");
}

// ---------------------------------------------------------------------------
// durability: the feed backlog survives a reopen
// ---------------------------------------------------------------------------

/// Change events committed but not yet drained (trapping module) must
/// survive a close/reopen with their order, kinds, and values intact —
/// they are ordinary durable keys riding the triggering batch's WAL record.
#[test]
fn undrained_changes_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options { sync: SyncMode::Always, ..opts() }).unwrap();
        db.install_module("order_feed", TRAP_ON_APPLY.as_bytes()).unwrap();
        let mode = db
            .create_trigger("feed", "order_feed", Some(b"orders/"), Some(b"orders0"))
            .unwrap();
        assert_eq!(mode, TriggerMode::Changes);

        let mut b = WriteBatch::new();
        b.put(b"orders/00000001".to_vec(), br#"{"v":1}"#.to_vec());
        b.delete(b"orders/00000001".to_vec());
        b.put(b"orders/00000002".to_vec(), br#"{"v":2}"#.to_vec());
        db.write(b).unwrap();
        wait_until("events queued", 30, || pending(&db, "feed") == 3);
    }

    let db = Db::open(dir.path(), Options { sync: SyncMode::Always, ..opts() }).unwrap();
    assert_eq!(pending(&db, "feed"), 3, "backlog recovered");
    let listed = db.list_triggers().unwrap();
    assert_eq!(listed[0].mode, TriggerMode::Changes, "mode recovered from disk");

    // repair with the real guest: the recovered backlog drains in order
    db.install_module("order_feed", &guest_wasm("order_feed")).unwrap();
    wait_until("recovered backlog drains", 30, || pending(&db, "feed") == 0);
    let entries = feed(&db);
    let ops: Vec<&str> = entries.iter().map(|e| e["op"].as_str().unwrap()).collect();
    assert_eq!(ops, vec!["put", "delete", "put"]);
    assert_eq!(entries[2]["record"]["v"], 2);
}
