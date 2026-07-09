//! Hard-crash recovery: a real child process writes under load and is SIGKILLed
//! mid-write; the parent reopens the store and verifies it recovered to the last
//! durable watermark. This is the "hard crash → resume from there" contract for
//! a system of record.
//!
//! `Db::drop` joins background threads and flushes cleanly — that is NOT a crash.
//! A SIGKILL leaves the store exactly as the kernel had it: no unwind, no
//! flush-on-exit, no lock release beyond what the OS does. The engine must then
//! reopen to a consistent state with:
//!   - no corruption, and a clean reopen (idempotent across repeats);
//!   - surviving keys forming a GAPLESS prefix per writer (no hole in the
//!     recovered history);
//!   - under SyncMode::Always, every write the child reported as acked present
//!     (acked ⇒ fsynced-before-ack ⇒ durable), i.e. zero acknowledged loss.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fluent31::{Db, Options, SyncMode};

const THREADS: usize = 4;

fn reopen_opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 64 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

/// Spawn the crash_writer child, let it make progress, SIGKILL it. Returns
/// (db_dir, last durable per-thread counts the parent observed on stdout).
fn run_and_kill(mode: &str) -> (tempfile::TempDir, Vec<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_crash_writer"))
        .arg(dir.path())
        .arg(mode)
        .arg(THREADS.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn crash_writer");

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let mut last = vec![0u64; THREADS];
    let deadline = Instant::now() + Duration::from_secs(30);
    for line in reader.lines() {
        let Ok(line) = line else { break };
        let counts: Vec<u64> = line.split(',').filter_map(|s| s.parse().ok()).collect();
        if counts.len() == THREADS {
            last = counts;
        }
        // enough progress to have spanned flushes and a real WAL tail
        if last.iter().sum::<u64>() >= 3000 || Instant::now() > deadline {
            break;
        }
    }
    assert!(
        last.iter().sum::<u64>() > 0,
        "child made no observable progress in {mode} mode"
    );

    // hard crash: SIGKILL (std Child::kill sends SIGKILL on Unix). No unwind,
    // no Db::drop, no flush — exactly a process death mid-write.
    child.kill().expect("kill child");
    child.wait().expect("reap child");
    (dir, last)
}

/// Collect, per thread, the sorted list of surviving indices.
fn survivors(db: &Db) -> BTreeMap<usize, Vec<u64>> {
    let mut out: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for kv in db.iter(Some(b"crash/"), Some(b"crash0"), false).unwrap() {
        let (k, _) = kv.unwrap();
        // key = "crash/<t>/<i>"
        let s = std::str::from_utf8(&k).unwrap();
        let mut parts = s.trim_start_matches("crash/").split('/');
        let t: usize = parts.next().unwrap().parse().unwrap();
        let i: u64 = parts.next().unwrap().parse().unwrap();
        out.entry(t).or_default().push(i);
    }
    out
}

/// Every surviving thread's indices must be exactly [0, N) — no holes.
fn assert_gapless(surv: &BTreeMap<usize, Vec<u64>>) -> Vec<u64> {
    let mut counts = vec![0u64; THREADS];
    for (&t, idxs) in surv {
        for (expected, &got) in idxs.iter().enumerate() {
            assert_eq!(
                got, expected as u64,
                "thread {t} has a hole: index {expected} missing (found {got})"
            );
        }
        counts[t] = idxs.len() as u64;
    }
    counts
}

#[test]
fn always_mode_loses_no_acked_write_on_hard_crash() {
    let (dir, reported) = run_and_kill("always");

    // reopen must succeed on a store left mid-write by SIGKILL
    let db = Db::open(dir.path(), reopen_opts()).unwrap();
    let surv = survivors(&db);
    let recovered = assert_gapless(&surv);

    // zero acknowledged loss: every write the parent saw acked is present.
    // (Under Always, Ok is returned only after the WAL fsync, so a reported
    // count is a hard durable lower bound.)
    for t in 0..THREADS {
        assert!(
            recovered[t] >= reported[t],
            "thread {t}: lost acked writes — recovered {} < reported-durable {}",
            recovered[t],
            reported[t]
        );
    }

    // the store is fully usable after recovery
    db.put(b"post/crash".to_vec(), b"ok".to_vec()).unwrap();
    assert_eq!(db.get(b"post/crash").unwrap().unwrap(), b"ok");
}

#[test]
fn recovery_is_idempotent_across_repeated_reopen() {
    let (dir, _) = run_and_kill("always");

    let first = {
        let db = Db::open(dir.path(), reopen_opts()).unwrap();
        assert_gapless(&survivors(&db))
    };
    // reopening again (and again) must yield the identical recovered state and
    // never reclassify the recovered WAL tail as damage
    for _ in 0..3 {
        let db = Db::open(dir.path(), reopen_opts()).unwrap();
        let again = assert_gapless(&survivors(&db));
        assert_eq!(again, first, "recovery not stable across reopens");
    }
}

#[test]
fn periodic_mode_recovers_consistent_gapless_prefix() {
    // Under Periodic, acks run ahead of durability, so some tail writes may be
    // lost — but the store is never corrupt and the survivors are gapless.
    let (dir, _) = run_and_kill("periodic");
    let db = Db::open(dir.path(), reopen_opts()).unwrap();
    assert_gapless(&survivors(&db));
    // full scan parses cleanly (no corruption anywhere in the tree)
    let n = db.iter(None, None, false).unwrap().map(|r| r.unwrap()).count();
    assert!(n > 0);
    db.put(b"post".to_vec(), b"1".to_vec()).unwrap();
}

#[test]
fn never_mode_recovers_consistent_gapless_prefix() {
    // Never fsyncs inline; recovery still yields a consistent, gapless, non-
    // corrupt prefix (the torn tail is truncated, not resurrected).
    let (dir, _) = run_and_kill("never");
    let db = Db::open(dir.path(), reopen_opts()).unwrap();
    assert_gapless(&survivors(&db));
    let n = db.iter(None, None, false).unwrap().map(|r| r.unwrap()).count();
    assert!(n > 0);
}
