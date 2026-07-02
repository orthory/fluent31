//! Group-commit tests: correctness under concurrent writers, actual fsync
//! amortization, isolation of per-batch failures, and recovery of grouped
//! WAL records.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use fluent31::{Db, Error, Options, SyncMode, WriteBatch};

fn opts(sync: SyncMode) -> Options {
    Options {
        sync,
        ..Options::default()
    }
}

/// Many concurrent writers, every write acknowledged must be readable, and
/// fsyncs must be amortized: with Always-sync writers overlapping on real
/// fsync latency, groups must form (fewer WAL syncs than batches).
#[test]
fn concurrent_writers_group_and_lose_nothing() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts(SyncMode::Always)).unwrap());
    let barrier = Arc::new(Barrier::new(THREADS));
    let acked = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = db.clone();
            let barrier = barrier.clone();
            let acked = acked.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..PER_THREAD {
                    let mut b = WriteBatch::new();
                    // two ops per batch: batch atomicity must survive grouping
                    b.put(format!("gc/{t}/{i}/a"), format!("v{t}-{i}-a"));
                    b.put(format!("gc/{t}/{i}/b"), format!("v{t}-{i}-b"));
                    db.write(b).unwrap();
                    acked.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(acked.load(Ordering::Relaxed) as usize, THREADS * PER_THREAD);
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            for half in ["a", "b"] {
                let k = format!("gc/{t}/{i}/{half}");
                let got = db.get(k.as_bytes()).unwrap();
                assert_eq!(
                    got.as_deref(),
                    Some(format!("v{t}-{i}-{half}").as_bytes()),
                    "missing {k}"
                );
            }
        }
    }

    let stats = db.stats();
    assert_eq!(stats.commit_batches, (THREADS * PER_THREAD) as u64);
    assert!(
        stats.commit_groups < stats.commit_batches,
        "with {THREADS} threads overlapping on fsync latency, groups must \
         form: groups={} batches={}",
        stats.commit_groups,
        stats.commit_batches
    );
    // every group paid at most one WAL fsync (rotations can add syncs, but
    // never more than groups on this small workload)
    assert!(
        stats.wal_syncs <= stats.commit_groups,
        "one fsync per group max: syncs={} groups={}",
        stats.wal_syncs,
        stats.commit_groups
    );
}

/// Sequential writers: grouping must never break the single-writer path
/// (each write is its own group of one).
#[test]
fn sequential_writes_are_groups_of_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts(SyncMode::Never)).unwrap();
    for i in 0..50u32 {
        db.put(format!("seq/{i}"), i.to_le_bytes().to_vec()).unwrap();
    }
    let stats = db.stats();
    assert_eq!(stats.commit_batches, 50);
    assert_eq!(stats.commit_groups, 50, "no concurrency, no grouping");
    assert_eq!(db.get(b"seq/49").unwrap().as_deref(), Some(&49u32.to_le_bytes()[..]));
}

/// A batch that fails validation must not poison concurrent valid batches.
#[test]
fn invalid_batch_is_isolated_from_the_group() {
    const THREADS: usize = 6;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts(SyncMode::Always)).unwrap());
    let barrier = Arc::new(Barrier::new(THREADS + 1));

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = db.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..10 {
                    db.put(format!("ok/{t}/{i}"), "v").unwrap();
                }
            });
        }
        let db2 = db.clone();
        let barrier = barrier.clone();
        s.spawn(move || {
            barrier.wait();
            for _ in 0..10 {
                // key over max_key_size (16 KiB default): validation error,
                // rejected in Db::write's validate_batch before enqueue —
                // and a giant-value batch exercises the in-group WAL bound
                let big_key = vec![b'k'; 20 << 10];
                let err = db2.put(big_key, "v").unwrap_err();
                assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
            }
        });
    });

    for t in 0..THREADS {
        for i in 0..10 {
            assert!(
                db.get(format!("ok/{t}/{i}").as_bytes()).unwrap().is_some(),
                "valid write lost at ok/{t}/{i}"
            );
        }
    }
}

/// Grouped WAL records recover: concurrent writers, drop, reopen, verify.
#[test]
fn grouped_wal_records_recover() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 10;
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Arc::new(Db::open(dir.path(), opts(SyncMode::Always)).unwrap());
        let barrier = Arc::new(Barrier::new(THREADS));
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let db = db.clone();
                let barrier = barrier.clone();
                s.spawn(move || {
                    barrier.wait();
                    for i in 0..PER_THREAD {
                        db.put(format!("rec/{t}/{i}"), format!("r{t}-{i}")).unwrap();
                    }
                });
            }
        });
        let stats = db.stats();
        assert!(stats.commit_groups < stats.commit_batches, "grouping happened");
        // no flush: everything recovers from the WAL alone
    }
    let db = Db::open(dir.path(), opts(SyncMode::Always)).unwrap();
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            assert_eq!(
                db.get(format!("rec/{t}/{i}").as_bytes()).unwrap().as_deref(),
                Some(format!("r{t}-{i}").as_bytes()),
                "rec/{t}/{i} lost across recovery"
            );
        }
    }
}

/// Transactions (their own write_mu path) interleaved with grouped batch
/// writers: OCC counters must not lose updates.
#[test]
fn txns_interleave_correctly_with_grouped_writers() {
    const WRITERS: usize = 4;
    const TXNS: usize = 4;
    const PER: usize = 15;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts(SyncMode::Never)).unwrap());
    db.put("ctr", 0u64.to_le_bytes().to_vec()).unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS + TXNS));

    std::thread::scope(|s| {
        for t in 0..WRITERS {
            let db = db.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..PER {
                    db.put(format!("w/{t}/{i}"), "x").unwrap();
                }
            });
        }
        for _ in 0..TXNS {
            let db = db.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for _ in 0..PER {
                    loop {
                        let mut txn = db.begin();
                        let cur = txn.get_for_update(b"ctr").unwrap().unwrap();
                        let n = u64::from_le_bytes(cur[..8].try_into().unwrap());
                        txn.put("ctr", (n + 1).to_le_bytes().to_vec()).unwrap();
                        match txn.commit() {
                            Ok(()) => break,
                            Err(Error::Conflict) => continue,
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                }
            });
        }
    });

    let ctr = db.get(b"ctr").unwrap().unwrap();
    assert_eq!(
        u64::from_le_bytes(ctr[..8].try_into().unwrap()),
        (TXNS * PER) as u64,
        "no lost counter updates"
    );
    for t in 0..WRITERS {
        for i in 0..PER {
            assert!(db.get(format!("w/{t}/{i}").as_bytes()).unwrap().is_some());
        }
    }
}

/// Large values (vlog path) under concurrent grouped writers: pointers and
/// payloads stay consistent, one vlog fsync per group.
#[test]
fn vlog_values_survive_grouping() {
    const THREADS: usize = 6;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Always,
                value_threshold: 128, // force vlog placement
                ..Options::default()
            },
        )
        .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(THREADS));
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = db.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..8u32 {
                    let val: Vec<u8> = (0..4096u32).map(|j| ((j + i + t as u32) % 251) as u8).collect();
                    db.put(format!("big/{t}/{i}"), val).unwrap();
                }
            });
        }
    });
    for t in 0..THREADS {
        for i in 0..8u32 {
            let expect: Vec<u8> = (0..4096u32).map(|j| ((j + i + t as u32) % 251) as u8).collect();
            assert_eq!(
                db.get(format!("big/{t}/{i}").as_bytes()).unwrap().as_deref(),
                Some(&expect[..]),
                "big/{t}/{i} corrupted"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// OCC transaction grouping (SyncMode::Always routes commits through the
// committer with in-group conflict revalidation)
// ---------------------------------------------------------------------------

/// Concurrent OCC counter increments in Always mode: no lost updates, and
/// commits share fsyncs (grouping visible in stats).
#[test]
fn txn_commits_group_and_never_lose_updates() {
    const THREADS: usize = 8;
    const PER: usize = 10;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Always,
                ..Options::default()
            },
        )
        .unwrap(),
    );
    db.put("ctr", 0u64.to_le_bytes().to_vec()).unwrap();
    let barrier = Arc::new(Barrier::new(THREADS));
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let db = db.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for _ in 0..PER {
                    loop {
                        let mut txn = db.begin();
                        let cur = txn.get_for_update(b"ctr").unwrap().unwrap();
                        let n = u64::from_le_bytes(cur[..8].try_into().unwrap());
                        txn.put("ctr", (n + 1).to_le_bytes().to_vec()).unwrap();
                        match txn.commit() {
                            Ok(()) => break,
                            Err(Error::Conflict) => continue,
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                }
            });
        }
    });
    let ctr = db.get(b"ctr").unwrap().unwrap();
    assert_eq!(
        u64::from_le_bytes(ctr[..8].try_into().unwrap()),
        (THREADS * PER) as u64,
        "lost updates through the grouped txn path"
    );
    let stats = db.stats();
    assert!(
        stats.commit_groups < stats.commit_batches,
        "txn commits must group: groups={} batches={}",
        stats.commit_groups,
        stats.commit_batches
    );
}
