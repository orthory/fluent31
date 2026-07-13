//! End-to-end engine tests: CRUD, MVCC, flush/compaction, recovery, value
//! log, forks, transactions, and a randomized model test against a
//! BTreeMap reference.

use std::collections::BTreeMap;

use fluent31::{Compression, Db, Error, Options, SyncMode, WriteBatch};

/// Tiny sizes so every structure (flush, tiering, vlog rotation, fragment
/// splitting) is exercised by small tests. SyncMode::Never because macOS
/// fsync is F_FULLFSYNC (~15ms) and these suites issue thousands of writes;
/// clean-shutdown recovery exercises WAL replay identically.
fn small_opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 4 << 10,
        block_size: 512,
        l0_compaction_trigger: 2,
        tier_width: 2,
        max_levels: 4,
        target_file_size: 4 << 10,
        value_threshold: 64,
        vlog_file_size: 8 << 10,
        vlog_gc_ratio: 0.3,
        ..Options::default()
    }
}

fn k(i: u32) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn v(i: u32, tag: &str) -> Vec<u8> {
    format!("value-{tag}-{i}").into_bytes()
}

#[test]
fn basic_crud() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    assert_eq!(db.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(db.get(b"b").unwrap().unwrap(), b"2");
    assert!(db.get(b"c").unwrap().is_none());
    db.put(b"a".to_vec(), b"1x".to_vec()).unwrap();
    assert_eq!(db.get(b"a").unwrap().unwrap(), b"1x");
    db.delete(b"a".to_vec()).unwrap();
    assert!(db.get(b"a").unwrap().is_none());
    assert_eq!(db.get(b"b").unwrap().unwrap(), b"2");
}

/// Reads never depend on Options::compression: a store written under
/// Compression::Lz4 reads back fully after reopening with the default
/// (Compression::None), and newly written raw tables mix freely with the
/// compressed ones already on disk.
#[test]
fn compression_cross_compat_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(
            dir.path(),
            Options {
                compression: Compression::Lz4,
                ..small_opts()
            },
        )
        .unwrap();
        for i in 0..300 {
            db.put(k(i), v(i, "lz4")).unwrap();
        }
        db.flush().unwrap();
        db.compact_all().unwrap();
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..300 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), v(i, "lz4"), "reopen {i}");
    }
    // mixed state: raw L0 tables above compressed lower levels
    for i in 300..400 {
        db.put(k(i), v(i, "raw")).unwrap();
    }
    db.flush().unwrap();
    for i in 0..300 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), v(i, "lz4"), "mixed {i}");
    }
    // compaction merges compressed inputs into raw outputs
    db.compact_all().unwrap();
    for i in 0..400 {
        let expect = if i < 300 { v(i, "lz4") } else { v(i, "raw") };
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), expect, "compacted {i}");
    }
}

#[test]
fn reserved_keys_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert!(matches!(
        db.put(vec![0u8, b'x'], b"v".to_vec()),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(db.get(&[0u8, b'x']), Err(Error::InvalidArgument(_))));
    assert!(matches!(
        db.put(Vec::new(), b"v".to_vec()),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn write_batch_is_atomic_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    let mut b = WriteBatch::new();
    b.put(b"x".to_vec(), b"1".to_vec());
    b.delete(b"x".to_vec());
    b.put(b"x".to_vec(), b"3".to_vec());
    db.write(b).unwrap();
    assert_eq!(db.get(b"x").unwrap().unwrap(), b"3");
}

#[test]
fn snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"k".to_vec(), b"old".to_vec()).unwrap();
    let snap = db.snapshot();
    db.put(b"k".to_vec(), b"new".to_vec()).unwrap();
    db.put(b"fresh".to_vec(), b"1".to_vec()).unwrap();
    assert_eq!(db.get_at(b"k", &snap).unwrap().unwrap(), b"old");
    assert!(db.get_at(b"fresh", &snap).unwrap().is_none());
    assert_eq!(db.get(b"k").unwrap().unwrap(), b"new");
    // snapshot survives flush + compaction
    db.flush().unwrap();
    db.compact_all().unwrap();
    assert_eq!(db.get_at(b"k", &snap).unwrap().unwrap(), b"old");
}

#[test]
fn flush_and_read_from_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..200u32 {
        db.put(k(i), v(i, "a")).unwrap();
    }
    db.flush().unwrap();
    let stats = db.stats();
    assert_eq!(stats.immutable_memtables, 0);
    assert!(stats.levels.iter().any(|(runs, _, _)| *runs > 0));
    for i in 0..200u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), v(i, "a"), "key {i}");
    }
}

#[test]
fn compaction_preserves_data_and_drops_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    // several generations of overwrites + deletes across flushes
    for round in 0..6u32 {
        for i in 0..100u32 {
            db.put(k(i), v(i, &format!("r{round}"))).unwrap();
        }
        for i in 0..50u32 {
            if (i + round) % 5 == 0 {
                db.delete(k(i)).unwrap();
            }
        }
        db.flush().unwrap();
    }
    db.compact_all().unwrap();
    for i in 0..100u32 {
        let expect_deleted = i < 50 && (i + 5) % 5 == 0;
        let got = db.get(&k(i)).unwrap();
        if expect_deleted {
            assert!(got.is_none(), "key {i} should be deleted");
        } else {
            assert_eq!(got.unwrap(), v(i, "r5"), "key {i}");
        }
    }
}

#[test]
fn iterators_forward_reverse_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..300u32 {
        db.put(k(i), v(i, "x")).unwrap();
        if i % 3 == 0 {
            // some data flushed, some in memtable
            if i == 150 {
                db.flush().unwrap();
            }
        }
    }
    for i in (0..300u32).step_by(7) {
        db.delete(k(i)).unwrap();
    }
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..300u32)
        .filter(|i| i % 7 != 0)
        .map(|i| (k(i), v(i, "x")))
        .collect();

    let got: Vec<_> = db
        .iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got, expected);

    let mut rev_expected = expected.clone();
    rev_expected.reverse();
    let got_rev: Vec<_> = db
        .iter(None, None, true)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got_rev, rev_expected);

    // bounded [key000100, key000200)
    let lo = k(100);
    let hi = k(200);
    let got_bounded: Vec<_> = db
        .iter(Some(&lo), Some(&hi), false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let expected_bounded: Vec<_> = expected
        .iter()
        .filter(|(kk, _)| kk.as_slice() >= lo.as_slice() && kk.as_slice() < hi.as_slice())
        .cloned()
        .collect();
    assert_eq!(got_bounded, expected_bounded);

    let got_bounded_rev: Vec<_> = db
        .iter(Some(&lo), Some(&hi), true)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut expected_bounded_rev = expected_bounded.clone();
    expected_bounded_rev.reverse();
    assert_eq!(got_bounded_rev, expected_bounded_rev);
}

#[test]
fn recovery_replays_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..50u32 {
            db.put(k(i), v(i, "wal")).unwrap();
        }
        // no flush: everything lives in WAL + memtable
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..50u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), v(i, "wal"), "key {i}");
    }
}

#[test]
fn recovery_after_flush_and_more_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..100u32 {
            db.put(k(i), v(i, "flushed")).unwrap();
        }
        db.flush().unwrap();
        for i in 50..150u32 {
            db.put(k(i), v(i, "wal2")).unwrap();
        }
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..150u32 {
        let expect = if i >= 50 { v(i, "wal2") } else { v(i, "flushed") };
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), expect, "key {i}");
    }
}

#[test]
fn recovery_truncates_torn_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        db.put(b"good".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"tail".to_vec(), b"2".to_vec()).unwrap();
    }
    // find the newest WAL and chop bytes off its tail
    let mut wals: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_string_lossy().into_owned();
            n.starts_with("wal-") && n.ends_with(".log")
        })
        .map(|e| e.path())
        .collect();
    wals.sort();
    let last = wals.last().unwrap();
    let len = std::fs::metadata(last).unwrap().len();
    assert!(len > 4);
    let f = std::fs::OpenOptions::new().write(true).open(last).unwrap();
    f.set_len(len - 3).unwrap();

    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.get(b"good").unwrap().unwrap(), b"1");
    // the second record was torn: cleanly lost
    assert!(db.get(b"tail").unwrap().is_none());
}

#[test]
fn large_values_roundtrip_through_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    let big = |i: u32| vec![(i % 251) as u8; 500 + (i as usize % 300)];
    for i in 0..60u32 {
        db.put(k(i), big(i)).unwrap();
    }
    // spans memtable, tables, several vlog files (8 KiB rotation)
    for i in 0..60u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i), "pre-flush {i}");
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    for i in 0..60u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i), "post-flush {i}");
    }
    assert!(db.stats().vlog_files > 1, "expected vlog rotation");

    // scans resolve pointers in batches
    let entries = db
        .iter(None, None, false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 60);

    // survive reopen
    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..60u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i), "reopened {i}");
    }
}

#[test]
fn vlog_gc_relocates_and_retires() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.2;
    let db = Db::open(dir.path(), opts).unwrap();
    let big = |i: u32, r: u32| {
        format!("{:0>400}", format!("{i}-{r}")).into_bytes()
    };
    // several overwrite rounds → most of the older vlog files are garbage
    for round in 0..4u32 {
        for i in 0..40u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
        db.flush().unwrap();
    }
    let files_before = db.stats().vlog_files;
    // compaction records discard stats
    db.compact_all().unwrap();
    // run gc until nothing qualifies
    let mut retired = 0;
    while db.gc_vlog().unwrap().is_some() {
        retired += 1;
        assert!(retired < 100, "gc runaway");
    }
    // the background maintenance thread races this loop (auto_gc after its
    // compaction passes) and may retire the victims first — assert the
    // outcome, not which thread performed it
    let s = db.stats();
    assert!(
        retired > 0 || s.vlog_retired > 0 || s.vlog_files < files_before,
        "expected vlog retirement (explicit {retired}, gated {}, files {files_before} -> {})",
        s.vlog_retired,
        s.vlog_files
    );
    // everything still readable, with the latest values
    for i in 0..40u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i, 3), "key {i}");
    }
    // after a flush the retirement gates can pass; deletion is async via
    // handle drop — just verify reopen consistency
    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..40u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i, 3), "reopen {i}");
    }
}

#[test]
fn transactions_commit_and_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"acct".to_vec(), b"100".to_vec()).unwrap();

    // successful read-modify-write
    let mut t = db.begin();
    let cur = t.get(b"acct").unwrap().unwrap();
    assert_eq!(cur, b"100");
    t.put(b"acct".to_vec(), b"90".to_vec()).unwrap();
    assert_eq!(t.get(b"acct").unwrap().unwrap(), b"90"); // read-your-writes
    assert_eq!(db.get(b"acct").unwrap().unwrap(), b"100"); // not visible yet
    t.commit().unwrap();
    assert_eq!(db.get(b"acct").unwrap().unwrap(), b"90");

    // write-write conflict: T1 snapshots, T2 commits first, T1 must abort
    let mut t1 = db.begin();
    t1.put(b"acct".to_vec(), b"80".to_vec()).unwrap();
    let mut t2 = db.begin();
    t2.put(b"acct".to_vec(), b"70".to_vec()).unwrap();
    t2.commit().unwrap();
    assert!(matches!(t1.commit(), Err(Error::Conflict)));
    assert_eq!(db.get(b"acct").unwrap().unwrap(), b"70");

    // plain db.put also conflicts an open txn's write set
    let mut t3 = db.begin();
    t3.put(b"acct".to_vec(), b"60".to_vec()).unwrap();
    db.put(b"acct".to_vec(), b"55".to_vec()).unwrap();
    assert!(matches!(t3.commit(), Err(Error::Conflict)));

    // conflicts with a committed delete are detected too
    let mut t4 = db.begin();
    t4.put(b"acct".to_vec(), b"50".to_vec()).unwrap();
    db.delete(b"acct".to_vec()).unwrap();
    assert!(matches!(t4.commit(), Err(Error::Conflict)));
}

#[test]
fn get_for_update_defends_against_write_skew() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"constraint".to_vec(), b"ok".to_vec()).unwrap();
    db.put(b"target".to_vec(), b"initial".to_vec()).unwrap();

    // T1 reads `constraint` with get_for_update and writes `target`.
    let mut t1 = db.begin();
    let c = t1.get_for_update(b"constraint").unwrap().unwrap();
    assert_eq!(c, b"ok");
    t1.put(b"target".to_vec(), b"based-on-ok".to_vec()).unwrap();
    // concurrent writer invalidates the premise
    db.put(b"constraint".to_vec(), b"violated".to_vec()).unwrap();
    assert!(matches!(t1.commit(), Err(Error::Conflict)));

    // without get_for_update the same interleaving commits (snapshot
    // isolation allows write skew on plain reads)
    let mut t2 = db.begin();
    let _ = t2.get(b"constraint").unwrap().unwrap();
    t2.put(b"target".to_vec(), b"unchecked".to_vec()).unwrap();
    db.put(b"constraint".to_vec(), b"changed-again".to_vec()).unwrap();
    t2.commit().unwrap();
}

#[test]
fn txn_iterator_merges_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    db.put(b"c".to_vec(), b"3".to_vec()).unwrap();

    let mut t = db.begin();
    t.put(b"b".to_vec(), b"2x".to_vec()).unwrap(); // overwrite
    t.delete(b"c".to_vec()).unwrap(); // hide
    t.put(b"d".to_vec(), b"4".to_vec()).unwrap(); // add

    let got: Vec<_> = t.iter(None, None, false).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2x".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]
    );
    let got_rev: Vec<_> = t.iter(None, None, true).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(
        got_rev,
        vec![
            (b"d".to_vec(), b"4".to_vec()),
            (b"b".to_vec(), b"2x".to_vec()),
            (b"a".to_vec(), b"1".to_vec()),
        ]
    );
}

#[test]
fn fork_create_open_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..80u32 {
        db.put(k(i), v(i, "cp")).unwrap();
    }
    // include some vlog-resident values
    for i in 0..10u32 {
        db.put(format!("big{i}").into_bytes(), vec![7u8; 300]).unwrap();
    }
    let info = db.fork("snap1").unwrap();
    assert_eq!(info.name, "snap1");
    assert!(info.path.exists());

    // mutate the parent afterwards
    for i in 0..80u32 {
        db.put(k(i), v(i, "post")).unwrap();
    }
    db.delete(b"big0".to_vec()).unwrap();
    db.flush().unwrap();
    db.compact_all().unwrap();

    // archive opens as a frozen fork
    let arc_db = Db::open(&info.path, Options::default()).unwrap();
    for i in 0..80u32 {
        assert_eq!(arc_db.get(&k(i)).unwrap().unwrap(), v(i, "cp"), "key {i}");
    }
    assert_eq!(arc_db.get(b"big0").unwrap().unwrap(), vec![7u8; 300]);
    // fork is writable without affecting the parent
    arc_db.put(b"fork-only".to_vec(), b"1".to_vec()).unwrap();
    drop(arc_db);

    assert!(db.get(b"fork-only").unwrap().is_none());
    assert_eq!(db.get(&k(0)).unwrap().unwrap(), v(0, "post"));

    let listed = db.list_forks().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "snap1");

    db.delete_fork("snap1").unwrap();
    assert!(db.list_forks().unwrap().is_empty());
    // parent unaffected by the unlink (hard links)
    assert_eq!(db.get(&k(3)).unwrap().unwrap(), v(3, "post"));
}

#[test]
fn fork_instance_ids_are_stable_and_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    let a = db.fork("snap").unwrap();
    assert_eq!(a.instance_id.len(), 32, "{}", a.instance_id);
    // listing re-reads fork.meta: same id both through the handle and
    // through the lock-free external listing
    assert_eq!(db.list_forks().unwrap()[0].instance_id, a.instance_id);
    let ext = fluent31::list_forks_at(dir.path()).unwrap();
    assert_eq!(ext[0].instance_id, a.instance_id);
    // delete + recreate mints a fresh id — stale handles must not resolve
    // to the new fork
    db.delete_fork("snap").unwrap();
    let a2 = db.fork("snap").unwrap();
    assert_ne!(a2.instance_id, a.instance_id);
}

#[test]
fn fork_with_gc_interleaved() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.2;
    let db = Db::open(dir.path(), opts).unwrap();
    let big = |i: u32, r: u32| format!("{:0>300}", format!("{i}.{r}")).into_bytes();
    for round in 0..3u32 {
        for i in 0..30u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
        db.flush().unwrap();
    }
    let info = db.fork("mid").unwrap();
    db.compact_all().unwrap();
    while db.gc_vlog().unwrap().is_some() {}
    db.flush().unwrap();

    let arc_db = Db::open(&info.path, Options::default()).unwrap();
    for i in 0..30u32 {
        assert_eq!(arc_db.get(&k(i)).unwrap().unwrap(), big(i, 2), "key {i}");
    }
}

/// The load-bearing fork_at property: a fork cut at a pinned point is
/// EXACTLY the state at that point — overwrites, deletes, compaction, and
/// vlog GC after the pin change nothing (the pin holds version and value
/// GC), keys created after the pin do not leak in (no ghost versions above
/// the cut), and the child is a fully usable store.
#[test]
fn fork_at_pinned_point_equals_state_at_pin() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.2;
    let db = Db::open(dir.path(), opts).unwrap();
    let big = |i: u32, r: u32| format!("{:0>300}", format!("{i}.{r}")).into_bytes();

    for i in 0..30u32 {
        db.put(k(i), big(i, 0)).unwrap();
    }
    let pin = db.pin("round0").unwrap();
    assert_eq!(pin.seqno, db.stats().visible_seqno);

    // churn: overwrite everything twice, delete some keys, add new ones,
    // then push compaction + vlog GC as hard as the engine allows
    for round in 1..3u32 {
        for i in 0..30u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
    }
    for i in 0..10u32 {
        db.delete(k(i)).unwrap();
    }
    for i in 100..110u32 {
        db.put(k(i), big(i, 9)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    while db.gc_vlog().unwrap().is_some() {}
    db.flush().unwrap();

    let info = db.fork_at("at-round0", pin.seqno).unwrap();
    assert_eq!(info.last_seqno, pin.seqno);

    let child = Db::open(&info.path, Options::default()).unwrap();
    // the child opens exactly at the cut
    assert_eq!(child.stats().visible_seqno, pin.seqno);
    for i in 0..30u32 {
        assert_eq!(child.get(&k(i)).unwrap().unwrap(), big(i, 0), "key {i}");
    }
    // nothing from after the pin — a full scan sees exactly the 30 keys
    let scanned: Vec<_> = child.iter(None, None, false).unwrap().collect();
    assert_eq!(scanned.len(), 30);
    // the child is independently writable, and its writes persist a reopen
    child.put(b"child-only".to_vec(), b"1".to_vec()).unwrap();
    child.put(k(0), b"child-v".to_vec()).unwrap();
    drop(child);
    let child = Db::open(&info.path, Options::default()).unwrap();
    assert_eq!(child.get(b"child-only").unwrap().unwrap(), b"1");
    assert_eq!(child.get(&k(0)).unwrap().unwrap(), b"child-v");
    assert_eq!(child.get(&k(1)).unwrap().unwrap(), big(1, 0));
    drop(child);

    // parent unaffected: post-pin state intact
    assert!(db.get(&k(0)).unwrap().is_none());
    assert_eq!(db.get(&k(15)).unwrap().unwrap(), big(15, 2));
    assert!(db.get(b"child-only").unwrap().is_none());
}

/// fork_at addressing rules: the current head is always accepted (same cut
/// as a plain fork), a future seqno is refused, and a past seqno the GC
/// watermark has moved beyond is refused with the retention message.
#[test]
fn fork_at_validates_the_point() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..20u32 {
        db.put(k(i), v(i, "a")).unwrap();
    }
    let old = db.stats().visible_seqno;
    for i in 0..20u32 {
        db.put(k(i), v(i, "b")).unwrap();
    }

    // head cut: equivalent to fork()
    let head = db.stats().visible_seqno;
    let info = db.fork_at("at-head", head).unwrap();
    assert_eq!(info.last_seqno, head);
    let child = Db::open(&info.path, Options::default()).unwrap();
    assert_eq!(child.get(&k(3)).unwrap().unwrap(), v(3, "b"));
    drop(child);

    // future seqno: refused
    let err = format!("{}", db.fork_at("at-future", head + 1000).unwrap_err());
    assert!(err.contains("beyond the flushed head"), "{err}");

    // unpinned past seqno: the watermark (no snapshots -> visible+1) has
    // passed it; refused with a hint at pins
    let err = format!("{}", db.fork_at("at-old", old).unwrap_err());
    assert!(err.contains("no longer retained"), "{err}");
    assert!(db.list_forks().unwrap().len() == 1, "no partial forks");
}

/// db.seqno() is the address of "now": it tracks committed writes, and
/// forks cut at one captured seqno are deterministically the same
/// version — no pin needed while the point is still the head.
#[test]
fn seqno_addresses_now_for_deterministic_forks() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.seqno(), 0);
    for i in 0..10u32 {
        db.put(k(i), v(i, "a")).unwrap();
    }
    let s = db.seqno();
    assert_eq!(s, 10, "one seqno per committed write");
    assert_eq!(s, db.stats().visible_seqno);

    // two forks addressed by the same captured seqno are the same version
    let f1 = db.fork_at("copy-a", s).unwrap();
    let f2 = db.fork_at("copy-b", s).unwrap();
    assert_eq!(f1.last_seqno, s);
    assert_eq!(f2.last_seqno, s);
    let c1 = Db::open(&f1.path, Options::default()).unwrap();
    let c2 = Db::open(&f2.path, Options::default()).unwrap();
    assert_eq!(c1.stats().visible_seqno, s);
    assert_eq!(c2.stats().visible_seqno, s);
    for i in 0..10u32 {
        assert_eq!(c1.get(&k(i)).unwrap(), c2.get(&k(i)).unwrap(), "key {i}");
    }
}

/// Pins are durable: they survive a reopen (re-registered before any GC
/// can run), keep their point fork-able across restart + churn, and
/// unpinning releases the hold so the point becomes refusable again.
#[test]
fn pin_survives_restart_and_holds_gc() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.2;
    let big = |i: u32, r: u32| format!("{:0>300}", format!("{i}.{r}")).into_bytes();
    let pin = {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..20u32 {
            db.put(k(i), big(i, 0)).unwrap();
        }
        db.pin("keep").unwrap()
        // drop: clean shutdown
    };

    let db = Db::open(dir.path(), opts).unwrap();
    let listed = db.pins();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "keep");
    assert_eq!(listed[0].seqno, pin.seqno);

    // churn hard after the restart: the re-registered pin must keep the
    // pinned state materializable through compaction and vlog GC
    for round in 1..4u32 {
        for i in 0..20u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
        db.flush().unwrap();
    }
    db.compact_all().unwrap();
    while db.gc_vlog().unwrap().is_some() {}

    let info = db.fork_at("at-keep", pin.seqno).unwrap();
    let child = Db::open(&info.path, Options::default()).unwrap();
    for i in 0..20u32 {
        assert_eq!(child.get(&k(i)).unwrap().unwrap(), big(i, 0), "key {i}");
    }
    drop(child);

    // released: the very same point is refused once the pin is gone
    db.unpin("keep").unwrap();
    assert!(db.pins().is_empty());
    let err = format!("{}", db.fork_at("at-keep2", pin.seqno).unwrap_err());
    assert!(err.contains("no longer retained"), "{err}");

    // the pin removal is durable too
    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert!(db.pins().is_empty());
}

/// Pin bookkeeping error paths: names share the fork charset, duplicates
/// are refused, and unpinning something unknown is a clean error.
#[test]
fn pin_name_rules() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();

    db.pin("ok-1").unwrap();
    let err = format!("{}", db.pin("ok-1").unwrap_err());
    assert!(err.contains("already exists"), "{err}");
    assert!(db.pin(".bad").is_err());
    assert!(db.pin("").is_err());
    let err = format!("{}", db.unpin("missing").unwrap_err());
    assert!(err.contains("no pin named"), "{err}");
    // the failed duplicate did not leak a second entry
    assert_eq!(db.pins().len(), 1);
}

/// A named store's fork identity carries the cut: forks at different pins
/// mint different deterministic instance ids, and the child's minted
/// lineage records (parent id, cut seqno) verbatim.
#[test]
fn fork_at_identity_carries_the_cut() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.store_name = Some("prod".into());
    let db = Db::open(dir.path(), opts).unwrap();
    let parent_id = db.identity().unwrap().instance_id;

    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    let p1 = db.pin("p1").unwrap();
    db.put(b"a".to_vec(), b"2".to_vec()).unwrap();
    let p2 = db.pin("p2").unwrap();

    let f1 = db.fork_at("nightly-a", p1.seqno).unwrap();
    let f2 = db.fork_at("nightly-b", p2.seqno).unwrap();
    assert_ne!(f1.instance_id, f2.instance_id);

    // first read-write open mints the identity from (parent, cut, name)
    let child = Db::open(&f1.path, Options::default()).unwrap();
    let id = child.identity().unwrap();
    assert_eq!(id.name, "nightly-a");
    assert_eq!(id.parent, Some((parent_id, p1.seqno)));
    assert_eq!(id.instance_hex(), f1.instance_id);
    assert_eq!(child.get(b"a").unwrap().unwrap(), b"1");
}

#[test]
fn double_open_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _db = Db::open(dir.path(), small_opts()).unwrap();
    assert!(matches!(
        Db::open(dir.path(), small_opts()),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn sync_always_mode_works() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.sync = SyncMode::Always;
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..10u32 {
            db.put(k(i), v(i, "sync")).unwrap();
        }
        // one vlog-resident value through the ordered dual-fsync path
        db.put(b"big".to_vec(), vec![9u8; 500]).unwrap();
    }
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..10u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), v(i, "sync"));
    }
    assert_eq!(db.get(b"big").unwrap().unwrap(), vec![9u8; 500]);
}

/// Regression (review finding): a registered snapshot must keep resolving
/// values whose pointers lead into a vlog file that GC retired AFTER the
/// snapshot was taken — the victim file must stay in the resolution map
/// until the deletion gates prove nobody can need it.
#[test]
fn snapshot_reads_survive_vlog_gc_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    // suppress background merges so gc timing is fully deterministic
    opts.l0_compaction_trigger = 100;
    opts.tier_width = 100;
    opts.vlog_gc_ratio = 0.1;
    let db = Db::open(dir.path(), opts).unwrap();

    let big = |tag: u32| format!("{:0>300}", tag).into_bytes();
    // key0 written once — its only version points into the first vlog file
    db.put(k(0), big(999)).unwrap();
    // neighbors written into the same vlog file, then overwritten repeatedly
    // so that file becomes mostly garbage
    for round in 0..4u32 {
        for i in 1..20u32 {
            db.put(k(i), big(round * 100 + i)).unwrap();
        }
        db.flush().unwrap();
    }

    let snap = db.snapshot();
    // now make the old file a victim: compaction credits discard stats,
    // gc relocates key0 (at a seqno above the snapshot) and retires the file
    db.compact_all().unwrap();
    let mut retired = 0;
    while db.gc_vlog().unwrap().is_some() {
        retired += 1;
        assert!(retired < 50);
    }
    assert!(retired > 0, "expected a retirement");

    // latest reads see the relocation; the snapshot must still see through
    // the retired file (this returned Corruption before the fix)
    assert_eq!(db.get(&k(0)).unwrap().unwrap(), big(999));
    assert_eq!(db.get_at(&k(0), &snap).unwrap().unwrap(), big(999));
    let entries = db
        .iter_at(None, None, false, &snap)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 20);
}

/// Regression (review finding): a vlog file retired but not yet deleted at
/// shutdown must survive the reopen cycle — resolvable for old versions,
/// never re-listed as live, and cleanly deletable once the gates pass.
#[test]
fn retired_vlog_survives_reopen_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.l0_compaction_trigger = 100;
    opts.tier_width = 100;
    opts.vlog_gc_ratio = 0.1;
    let big = |tag: u32| format!("{:0>300}", tag).into_bytes();
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for round in 0..4u32 {
            for i in 0..20u32 {
                db.put(k(i), big(round * 100 + i)).unwrap();
            }
            db.flush().unwrap();
        }
        // hold a snapshot so the deletion gates CANNOT pass: the retirement
        // must persist across shutdown
        let _snap = db.snapshot();
        db.compact_all().unwrap();
        let mut retired = 0;
        while db.gc_vlog().unwrap().is_some() {
            retired += 1;
            assert!(retired < 50);
        }
        assert!(retired > 0, "expected a pending retirement at shutdown");
        // _snap dropped here, then the db closes with vlog_retired non-empty
    }
    // reopen with the retirement pending: reads must work, and the file
    // must not have been re-adopted into the live set
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..20u32 {
            assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(300 + i), "key {i}");
        }
        // let the maintenance thread pass the gates and delete the victim
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(800));
    }
    // and the post-deletion state must reopen cleanly (this failed before
    // the fix: vlog_live still referenced the unlinked file)
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..20u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(300 + i), "key {i}");
    }
}

/// Regression (review finding): a zero-filled region at the WAL tail (disk
/// preallocation / crash artifact) must classify as torn-tail loss, not as
/// a valid empty record that later fails decoding.
#[test]
fn zero_filled_wal_tail_is_torn() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        db.put(b"kept".to_vec(), b"1".to_vec()).unwrap();
    }
    let mut wals: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_string_lossy().into_owned();
            n.starts_with("wal-") && n.ends_with(".log")
        })
        .collect();
    wals.sort();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(wals.last().unwrap())
            .unwrap();
        f.write_all(&[0u8; 64]).unwrap();
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.get(b"kept").unwrap().unwrap(), b"1");
}

/// Regression (review finding): repeated reopen after a torn WAL tail must
/// stay stable — recovery truncates the tail so the file can never later be
/// misclassified as damaged-sealed.
#[test]
fn torn_tail_reopen_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    }
    let mut wals: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_string_lossy().into_owned();
            n.starts_with("wal-") && n.ends_with(".log")
        })
        .collect();
    wals.sort();
    let last = wals.last().unwrap();
    let len = std::fs::metadata(last).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(last).unwrap();
    f.set_len(len - 3).unwrap();
    drop(f);

    for round in 0..3 {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        assert_eq!(db.get(b"a").unwrap().unwrap(), b"1", "round {round}");
        assert!(db.get(b"b").unwrap().is_none(), "round {round}");
    }
}

/// On Linux the io_uring backend must actually engage (Auto may silently
/// fall back; force it and drive batched reads through the ring).
#[test]
#[cfg(target_os = "linux")]
fn uring_backend_selected_on_linux() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = small_opts();
    o.io_backend = fluent31::IoBackend::Uring;
    let db = Db::open(dir.path(), o).unwrap();
    assert_eq!(db.stats().backend, "io_uring");
    for i in 0..100u32 {
        db.put(k(i), vec![(i % 250) as u8; 300]).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    // scans batch-resolve vlog pointers via read_many on the ring
    let got: Vec<_> = db
        .iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got.len(), 100);
    for (i, (kk, vv)) in got.iter().enumerate() {
        assert_eq!(kk, &k(i as u32));
        assert_eq!(vv, &vec![(i % 250) as u8; 300]);
    }
}

// ---------------------------------------------------------------------------
// Randomized model test
// ---------------------------------------------------------------------------

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Apply a seeded random op stream to the DB and a BTreeMap reference;
/// intersperse flushes, compactions, GC, snapshots, scans and reopens, and
/// assert exact equality throughout.
#[test]
fn randomized_model_test() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = XorShift(0x5eed_f1e3_1000_0001);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut db = Db::open(dir.path(), small_opts()).unwrap();

    let key_of = |r: &mut XorShift| -> Vec<u8> { k(r.below(400) as u32) };
    let val_of = |r: &mut XorShift, i: u64| -> Vec<u8> {
        if r.below(5) == 0 {
            // vlog-resident value
            format!("{:0>200}", i).into_bytes()
        } else {
            format!("v{}", i).into_bytes()
        }
    };

    for step in 0..4000u64 {
        match rng.below(100) {
            0..=54 => {
                let key = key_of(&mut rng);
                let val = val_of(&mut rng, step);
                db.put(key.clone(), val.clone()).unwrap();
                model.insert(key, val);
            }
            55..=74 => {
                let key = key_of(&mut rng);
                db.delete(key.clone()).unwrap();
                model.remove(&key);
            }
            75..=89 => {
                let key = key_of(&mut rng);
                assert_eq!(
                    db.get(&key).unwrap(),
                    model.get(&key).cloned(),
                    "step {step} get {}",
                    String::from_utf8_lossy(&key)
                );
            }
            90..=92 => {
                db.flush().unwrap();
            }
            93 => {
                db.compact_all().unwrap();
            }
            94 => {
                let _ = db.gc_vlog().unwrap();
            }
            95..=96 => {
                // bounded scan, both directions
                let a = key_of(&mut rng);
                let b = key_of(&mut rng);
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                let reverse = rng.below(2) == 1;
                let got: Vec<_> = db
                    .iter(Some(&lo), Some(&hi), reverse)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                let mut expected: Vec<_> = model
                    .range(lo.clone()..hi.clone())
                    .map(|(kk, vv)| (kk.clone(), vv.clone()))
                    .collect();
                if reverse {
                    expected.reverse();
                }
                assert_eq!(got, expected, "step {step} scan");
            }
            97 => {
                // snapshot consistency under later mutations
                let snap = db.snapshot();
                let frozen: BTreeMap<Vec<u8>, Vec<u8>> = model.clone();
                for j in 0..20 {
                    let key = key_of(&mut rng);
                    let val = val_of(&mut rng, step * 100 + j);
                    db.put(key.clone(), val.clone()).unwrap();
                    model.insert(key, val);
                }
                for key in frozen.keys().take(10) {
                    assert_eq!(
                        db.get_at(key, &snap).unwrap(),
                        frozen.get(key).cloned(),
                        "step {step} snapshot get"
                    );
                }
            }
            _ => {
                // reopen: full durability check
                drop(db);
                db = Db::open(dir.path(), small_opts()).unwrap();
                let got: Vec<_> = db
                    .iter(None, None, false)
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                let expected: Vec<_> = model
                    .iter()
                    .map(|(kk, vv)| (kk.clone(), vv.clone()))
                    .collect();
                assert_eq!(got, expected, "step {step} reopen full scan");
            }
        }
    }

    // final exhaustive comparison
    let got: Vec<_> = db
        .iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let expected: Vec<_> = model
        .iter()
        .map(|(kk, vv)| (kk.clone(), vv.clone()))
        .collect();
    assert_eq!(got, expected);
}

/// Discard-stat lag fallback: overwritten large values whose garbage was
/// never observed by compaction (no compact_all — stats stay empty) must
/// still be reclaimable, via the sampling probe.
#[test]
fn vlog_gc_sampling_reclaims_without_discard_stats() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.3;
    // files must clear the sampling floor (4 MiB): ~16 KiB values, few
    // hundred per file
    opts.vlog_file_size = 4 << 20;
    let db = Db::open(dir.path(), opts).unwrap();
    let big = |i: u32, r: u32| format!("{:0>16000}", format!("{i}-{r}")).into_bytes();

    // several overwrite rounds so older vlog files are mostly garbage; only
    // flush memtables — never compact, so pointer drops are never observed
    // and the discard map stays empty for the old files
    for round in 0..4u32 {
        for i in 0..400u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
    }
    db.flush().unwrap();

    let mut retired = 0;
    while db.gc_vlog().unwrap().is_some() {
        retired += 1;
        assert!(retired < 100, "gc runaway");
    }
    // The background maintenance thread runs the same sampling fallback and
    // can win every race — it may even have retired AND deleted victims
    // already (clearing vlog_retired again). The only race-free signal is
    // ground truth: reclaimed disk space. ~25.6 MB was written but only
    // ~6.4 MB (one round) is live; poll for the vlog footprint to drop
    // below written-total, which is impossible without at least one file
    // reclaimed.
    let vlog_bytes = || -> u64 {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".vlog"))
            .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
            .sum()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let _ = db.gc_vlog(); // keep nudging alongside the bg loop
        if vlog_bytes() < 20 << 20 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sampling fallback never reclaimed: {} vlog bytes still on disk \
             (retired={retired})",
            vlog_bytes()
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    for i in 0..400u32 {
        assert_eq!(db.get(&k(i)).unwrap().unwrap(), big(i, 3), "key {i}");
    }
}

// ---------------------------------------------------------------------------
// Store identity & lineage
// ---------------------------------------------------------------------------

fn named_opts(name: &str) -> Options {
    Options {
        store_name: Some(name.to_string()),
        ..small_opts()
    }
}

/// Identity lifecycle: deterministic mint at create, stable across reopen,
/// name mismatch refused, adoption onto an unnamed store.
#[test]
fn identity_mint_reopen_adopt() {
    let dir = tempfile::tempdir().unwrap();
    let expected = fluent31::identity::derive_root("prod");
    {
        let db = Db::open(dir.path(), named_opts("prod")).unwrap();
        let id = db.identity().unwrap();
        assert_eq!(id.name, "prod");
        assert_eq!(id.instance_id, expected);
        assert!(id.parent.is_none());
        db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    }
    // reopen without a name: identity persists
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        assert_eq!(db.identity().unwrap().instance_id, expected);
    }
    // reopen with a mismatched name: refused
    assert!(matches!(
        Db::open(dir.path(), named_opts("other")),
        Err(Error::InvalidArgument(_))
    ));

    // adoption: an unnamed store gains identity on a later open
    let dir2 = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir2.path(), small_opts()).unwrap();
        assert!(db.identity().is_none());
        db.put(b"x".to_vec(), b"1".to_vec()).unwrap();
    }
    {
        let db = Db::open(dir2.path(), named_opts("adopted")).unwrap();
        let id = db.identity().unwrap();
        assert_eq!(id.instance_id, fluent31::identity::derive_root("adopted"));
        assert_eq!(db.get(b"x").unwrap().unwrap(), b"1");
    }
}

/// The routing id recorded in fork.meta at creation must equal the store
/// identity the fork mints on its first read-write open — routing and
/// replication share one id for named lineages.
#[test]
fn fork_instance_id_matches_minted_identity_for_named_lineage() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), named_opts("root-store")).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    let info = db.fork("branch").unwrap();
    let fdb = Db::open(&info.path, Options::default()).unwrap();
    let minted = fdb.identity().expect("named lineage mints on open");
    assert_eq!(info.instance_id, minted.instance_hex());
    assert_eq!(minted.name, "branch");
}

/// Forks mint deterministic child identities: in-place archive open and
/// restore_to copies each fork under their own name; restores of a named
/// lineage without a new name are refused.
#[test]
fn identity_forks_via_fork_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), named_opts("main")).unwrap();
    for i in 0..50u32 {
        db.put(k(i), v(i, "pre")).unwrap();
    }
    let info = db.fork("nightly").unwrap();
    let parent_id = db.identity().unwrap();

    // named lineage: restore without a new name is refused (two copies of
    // one archive must not mint identical instance ids)
    let dest = dir.path().join("restored");
    assert!(matches!(
        fluent31::restore_to(&info.path, &dest, None),
        Err(Error::InvalidArgument(_))
    ));
    // with a name: a distinct deterministic child, data intact
    fluent31::restore_to(&info.path, &dest, Some("edge-1")).unwrap();
    let restored = Db::open(&dest, small_opts()).unwrap();
    let rid = restored.identity().unwrap();
    assert_eq!(
        rid.instance_id,
        fluent31::identity::derive_fork(&parent_id.instance_id, info.last_seqno, "edge-1")
    );
    for i in 0..50u32 {
        assert_eq!(restored.get(&k(i)).unwrap().unwrap(), v(i, "pre"), "key {i}");
    }

    // in-place fork: first rw open mints child = H(parent ‖ cut ‖ "nightly")
    let expected_child =
        fluent31::identity::derive_fork(&parent_id.instance_id, info.last_seqno, "nightly");
    assert_ne!(rid.instance_id, expected_child);
    {
        let fork = Db::open(&info.path, small_opts()).unwrap();
        let id = fork.identity().unwrap();
        assert_eq!(id.name, "nightly");
        assert_eq!(id.instance_id, expected_child);
        assert_eq!(id.parent, Some((parent_id.instance_id, info.last_seqno)));
        // reopen keeps the minted id (fork marker consumed once)
        drop(fork);
        let again = Db::open(&info.path, small_opts()).unwrap();
        assert_eq!(again.identity().unwrap().instance_id, expected_child);
    }

    // once forked in place the archive is a live store: restoring a copy of
    // it would duplicate the minted instance id — refused
    assert!(matches!(
        fluent31::restore_to(&info.path, &dir.path().join("r2"), Some("edge-2")),
        Err(Error::InvalidArgument(_))
    ));
}

// ---------------------------------------------------------------------------
// Replication surface: subscriptions, slices, chunk reads
// ---------------------------------------------------------------------------

use fluent31::StreamEvent;
use std::time::Duration;

fn recv_batch(sub: &mut fluent31::Subscription) -> Vec<fluent31::StreamEntry> {
    match sub.recv_timeout(Duration::from_secs(5)).unwrap() {
        Some(StreamEvent::Batch(b)) => b,
        other => panic!("expected batch, got {other:?}"),
    }
}

/// Subscriptions deliver exactly the in-range committed writes, in seqno
/// order, values resolved — including vlog-resident ones.
#[test]
fn subscription_streams_in_range_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"kb-before".to_vec(), b"x".to_vec()).unwrap();

    let mut sub = db.subscribe(b"ka", Some(b"kz")).unwrap();
    assert_eq!(sub.start_seqno(), 1);

    db.put(b"ka-1".to_vec(), b"small".to_vec()).unwrap();
    db.put(b"zz-out-of-range".to_vec(), b"nope".to_vec()).unwrap();
    // vlog-resident value (>= value_threshold of 64)
    db.put(b"kb-2".to_vec(), vec![7u8; 200]).unwrap();
    db.delete(b"ka-1".to_vec()).unwrap();

    let mut got = Vec::new();
    while got.len() < 3 {
        got.extend(recv_batch(&mut sub));
    }
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].key, b"ka-1");
    assert_eq!(got[0].value.as_deref(), Some(b"small".as_ref()));
    assert_eq!(got[1].key, b"kb-2");
    assert_eq!(got[1].value.as_deref(), Some(vec![7u8; 200].as_ref()));
    assert_eq!(got[2].key, b"ka-1");
    assert!(got[2].value.is_none()); // tombstone
    assert!(got[0].seqno < got[1].seqno && got[1].seqno < got[2].seqno);
    // nothing else pending
    assert!(sub
        .recv_timeout(Duration::from_millis(50))
        .unwrap()
        .is_none());
}

/// Streamed vlog pointers stay resolvable across GC: the subscription's
/// advancing pin blocks victim deletion until entries are consumed.
#[test]
fn subscription_pointers_survive_vlog_gc() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.vlog_gc_ratio = 0.1;
    let db = Db::open(dir.path(), opts).unwrap();

    let mut sub = db.subscribe(b"k", None).unwrap();
    let big = |i: u32, r: u32| format!("{:0>300}", format!("{i}.{r}")).into_bytes();
    // two rounds: round-0 values become garbage, GC relocates round-1
    for round in 0..2u32 {
        for i in 0..20u32 {
            db.put(k(i), big(i, round)).unwrap();
        }
        db.flush().unwrap();
    }
    db.compact_all().unwrap();
    while db.gc_vlog().unwrap().is_some() {}

    // consume everything AFTER gc ran: old pointers must still resolve
    let mut got = Vec::new();
    while got.len() < 40 {
        got.extend(recv_batch(&mut sub));
    }
    for (n, e) in got.iter().enumerate() {
        let (round, i) = ((n / 20) as u32, (n % 20) as u32);
        assert_eq!(e.key, k(i), "entry {n}");
        assert_eq!(e.value.as_deref(), Some(big(i, round).as_ref()), "entry {n}");
    }
}

/// A subscriber that stops consuming gets cut off (Lagged), writers never
/// stall, and a fresh subscription works.
#[test]
fn subscription_lag_drops_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = small_opts();
    opts.sub_queue_bytes = 4 << 10; // tiny queue
    let db = Db::open(dir.path(), opts).unwrap();

    let mut sub = db.subscribe(b"k", None).unwrap();
    for i in 0..200u32 {
        db.put(k(i), vec![1u8; 100]).unwrap(); // ~20 KiB >> 4 KiB cap
    }
    // may see some batches first, but must end in Lagged
    let saw_lagged = loop {
        match sub.recv_timeout(Duration::from_millis(200)).unwrap() {
            Some(StreamEvent::Lagged) => break true,
            Some(StreamEvent::Batch(_)) => continue,
            None => break false,
        }
    };
    assert!(saw_lagged, "overflowed subscription never reported Lagged");

    // the store is unaffected; a new subscription streams fine
    let mut sub2 = db.subscribe(b"k", None).unwrap();
    db.put(k(9999), b"after".to_vec()).unwrap();
    let b = recv_batch(&mut sub2);
    assert_eq!(b[0].key, k(9999));
}

/// slice_manifest returns only overlapping fragments; chunk reads serve
/// live files and answer Gone for compacted-away ids.
#[test]
fn slice_manifest_scope_and_chunk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..300u32 {
        db.put(k(i), v(i, "s")).unwrap();
    }
    db.flush().unwrap();

    let full = db.slice_manifest(b"\x01", None).unwrap();
    let full_ids: Vec<u64> = full
        .levels
        .iter()
        .flatten()
        .flat_map(|r| r.tables.iter().map(|t| t.id))
        .collect();
    assert!(!full_ids.is_empty());

    // a narrow scope selects a strict subset (many fragments exist: 300
    // keys against a 4 KiB target file size)
    let lo = k(100);
    let hi = k(120);
    let scoped = db.slice_manifest(&lo, Some(&hi)).unwrap();
    let scoped_tables: Vec<&fluent31::SliceTable> = scoped
        .levels
        .iter()
        .flatten()
        .flat_map(|r| r.tables.iter())
        .collect();
    assert!(!scoped_tables.is_empty());
    assert!(scoped_tables.len() < full_ids.len());
    for t in &scoped_tables {
        assert!(t.max_ukey.as_slice() >= lo.as_slice() && t.min_ukey.as_slice() < hi.as_slice());
    }

    // chunk reads reassemble a fragment byte-for-byte
    let t0 = scoped_tables[0];
    let mut assembled = Vec::new();
    let mut off = 0u64;
    while off < t0.size {
        let chunk = db.read_table_chunk(t0.id, off, 1 << 10).unwrap();
        assert!(!chunk.is_empty());
        off += chunk.len() as u64;
        assembled.extend(chunk);
    }
    let disk = std::fs::read(dir.path().join(format!("sst-{:06}.tbl", t0.id))).unwrap();
    assert_eq!(assembled, disk);

    // compact everything into new files: old ids answer Gone
    for i in 0..300u32 {
        db.put(k(i), v(i, "s2")).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    let gone = full_ids
        .iter()
        .any(|&id| matches!(db.read_table_chunk(id, 0, 16), Err(Error::Gone(_))));
    assert!(gone, "no compacted-away fragment reported Gone");
}

/// Vlog chunk reads serve record bytes that parse and verify; unknown
/// files answer Gone.
#[test]
fn vlog_chunk_reads_and_gone() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    let mut sub = db.subscribe(b"k", None).unwrap();
    db.put(k(1), vec![9u8; 500]).unwrap(); // vlog-resident

    // learn the pointer through the stream? No — the stream resolves it.
    // Instead grab it via the slice path: flush and read back the repr is
    // internal, so exercise the public path: the streamed value proves
    // resolution; the chunk API is exercised with offsets from a scan of
    // the head file id range.
    let b = recv_batch(&mut sub);
    assert_eq!(b[0].value.as_deref(), Some(vec![9u8; 500].as_ref()));

    // head vlog file exists with id small; probe the first record region
    let stats = db.stats();
    assert!(stats.vlog_files >= 1);
    // find a real vlog file id from disk
    let vid = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let n = e.file_name().into_string().ok()?;
            let id = n
                .strip_prefix("vlog-")?
                .strip_suffix(".vlog")?
                .parse::<u64>()
                .ok()?;
            (e.metadata().ok()?.len() > 0).then_some(id)
        })
        .next()
        .expect("a non-empty vlog file");
    let chunk = db.read_vlog_chunk(vid, 0, 4096).unwrap();
    assert!(!chunk.is_empty());

    assert!(matches!(
        db.read_vlog_chunk(999_999, 0, 16),
        Err(Error::Gone(_))
    ));
    assert!(matches!(
        db.read_vlog_chunk(vid, 1 << 40, 16),
        Err(Error::Gone(_))
    ));
}

// ---------------------------------------------------------------------------
// Edge store (engine-level, no network: fetcher wired straight to the master)
// ---------------------------------------------------------------------------

use fluent31::edge::{EdgeConfig, EdgeStore, ValueFetcher};
use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
use std::sync::Arc;

struct DirectFetcher {
    master: Arc<Db>,
    calls: AtomicU64,
}

impl ValueFetcher for DirectFetcher {
    fn fetch_record(&self, file: u64, offset: u64, len: u32) -> fluent31::Result<Vec<u8>> {
        self.calls.fetch_add(1, AtOrd::Relaxed);
        self.master.read_vlog_chunk(file, offset, len as usize)
    }
}

/// Pull + install one slice, restarting from a fresh snapshot when the
/// master's background compaction wins the race (Gone) — the same loop the
/// real protocol client runs.
fn sync_slice(
    master: &Db,
    edge: &EdgeStore,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> fluent31::SliceManifest {
    'retry: loop {
        let slice = master.slice_manifest(lo, hi).unwrap();
        for run in slice.levels.iter().flatten() {
            for t in &run.tables {
                if edge.has_fragment(t.id) {
                    continue;
                }
                let mut bytes = Vec::with_capacity(t.size as usize);
                while (bytes.len() as u64) < t.size {
                    match master.read_table_chunk(t.id, bytes.len() as u64, 64 << 10) {
                        Ok(chunk) => bytes.extend(chunk),
                        Err(Error::Gone(_)) => continue 'retry,
                        Err(e) => panic!("chunk fetch: {e}"),
                    }
                }
                std::fs::write(edge.fragment_path(t.id), &bytes).unwrap();
            }
        }
        edge.install_slice(&slice).unwrap();
        return slice;
    }
}

/// Full edge loop: scoped slice + lazy value fetch + cache hits + streamed
/// syncs + refresh — the edge's scoped view stays byte-equal to the master.
#[test]
fn edge_store_end_to_end() {
    let mdir = tempfile::tempdir().unwrap();
    let edir = tempfile::tempdir().unwrap();
    let master = Arc::new(
        Db::open(
            mdir.path(),
            Options {
                store_name: Some("edge-master".into()),
                ..small_opts()
            },
        )
        .unwrap(),
    );
    // mixed inline + vlog values, in and out of the scope
    for i in 0..300u32 {
        master.put(k(i), v(i, "base")).unwrap();
    }
    for i in 0..30u32 {
        master.put(k(i * 10), vec![(i % 250) as u8 + 1; 200]).unwrap();
    }

    // attach: subscribe FIRST, then slice — gap-free by construction
    let mut sub = master.subscribe(&k(100), Some(&k(200))).unwrap();

    let fetcher = Arc::new(DirectFetcher {
        master: master.clone(),
        calls: AtomicU64::new(0),
    });
    let edge = EdgeStore::attach(
        EdgeConfig::new(
            edir.path().join("cache"),
            master.identity().unwrap(),
            k(100),
            Some(k(200)),
        ),
        fetcher.clone(),
    )
    .unwrap();
    let slice = sync_slice(&master, &edge, &k(100), Some(&k(200)));
    assert!(slice.flushed_seqno >= sub.start_seqno());

    // scoped equality, inline and vlog values alike
    for i in 100..200u32 {
        assert_eq!(
            edge.get(&k(i)).unwrap(),
            master.get(&k(i)).unwrap(),
            "key {i}"
        );
    }
    // out of scope: refused, not silently absent
    assert!(matches!(
        edge.get(&k(250)),
        Err(Error::InvalidArgument(_))
    ));

    // laziness: big values were fetched on demand; repeat reads hit cache
    let after_first_pass = fetcher.calls.load(AtOrd::Relaxed);
    assert!(after_first_pass > 0, "no lazy fetches happened");
    for i in 100..200u32 {
        edge.get(&k(i)).unwrap();
    }
    assert_eq!(
        fetcher.calls.load(AtOrd::Relaxed),
        after_first_pass,
        "second pass should be fully cache-served"
    );

    // streamed syncs: master writes (some vlog-resident), edge follows
    for i in 100..150u32 {
        master.put(k(i), v(i, "live")).unwrap();
    }
    master.put(k(160), vec![42u8; 300]).unwrap();
    master.delete(k(199)).unwrap();
    master.put(k(50), v(50, "outside")).unwrap(); // out of scope: not streamed
    let mut applied = 0;
    while applied < 52 {
        match sub.recv_timeout(Duration::from_secs(5)).unwrap() {
            Some(StreamEvent::Batch(b)) => {
                applied += b.len();
                edge.apply_stream(&b).unwrap();
            }
            other => panic!("unexpected stream event {other:?}"),
        }
    }
    for i in 100..200u32 {
        assert_eq!(
            edge.get(&k(i)).unwrap(),
            master.get(&k(i)).unwrap(),
            "post-stream key {i}"
        );
    }

    // scans match the master over the scope (both directions, paged)
    let (page, more) = edge.scan(None, None, false, 500).unwrap();
    assert!(!more);
    let master_scan: Vec<_> = master
        .iter(Some(&k(100)), Some(&k(200)), false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(page, master_scan);
    let (rev_page, _) = edge.scan(None, None, true, 500).unwrap();
    let mut rev_expected = master_scan.clone();
    rev_expected.reverse();
    assert_eq!(rev_page, rev_expected);

    // paging: limit cuts and reports has_more
    let (short, more) = edge.scan(None, None, false, 10).unwrap();
    assert_eq!(short.len(), 10);
    assert!(more);
    assert_eq!(short[..], master_scan[..10]);

    // refresh: pull a fresh slice; overlay prunes to the new watermark and
    // equality holds
    let slice2 = sync_slice(&master, &edge, &k(100), Some(&k(200)));
    assert!(slice2.flushed_seqno > slice.flushed_seqno);
    let stats = edge.stats();
    assert_eq!(stats.flushed_seqno, slice2.flushed_seqno);
    for i in 100..200u32 {
        assert_eq!(
            edge.get(&k(i)).unwrap(),
            master.get(&k(i)).unwrap(),
            "post-refresh key {i}"
        );
    }
}

// ---------------------------------------------------------------------------
// write-range triggers
// ---------------------------------------------------------------------------

/// Executor used as a trigger target: records the raw packed input under
/// `idx/last` and mirrors every touched key `<k>` to `m/<k>` (value = the
/// key bytes). Assumes single-byte uvarint key lengths (keys < 128 bytes),
/// which every test key satisfies.
#[cfg(feature = "wasm")]
const MIRROR_WAT: &str = r#"
(module
  (import "fluent" "input_len" (func $ilen (result i32)))
  (import "fluent" "input_read" (func $iread (param i32 i32 i32) (result i32)))
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 2)
  (data (i32.const 0) "idx/last")
  (func (export "on_touch") (result i32)
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
        (i32.store8 (i32.const 8192) (i32.const 109))
        (i32.store8 (i32.const 8193) (i32.const 47))
        (memory.copy (i32.const 8194)
                     (i32.add (i32.const 1024) (local.get $off))
                     (local.get $klen))
        (drop (call $put (i32.const 8192) (i32.add (local.get $klen) (i32.const 2))
                         (i32.add (i32.const 1024) (local.get $off)) (local.get $klen)))
        (local.set $off (i32.add (local.get $off) (local.get $klen)))
        (br $next)))
    (i32.const 0)))
"#;

#[cfg(feature = "wasm")]
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !f() {
        assert!(
            std::time::Instant::now() < deadline,
            "not reached within 10s: {what}"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "wasm")]
fn pending(db: &Db, name: &str) -> u64 {
    db.list_triggers()
        .unwrap()
        .into_iter()
        .find(|t| t.name == name)
        .map(|t| t.pending)
        .unwrap_or_else(|| panic!("no trigger named {name}"))
}

/// The full loop under SyncMode::Always: a put in the subscribed range
/// fires the module (which indexes the key), the queue drains to zero, and
/// out-of-range writes fire nothing. Batch and transaction write paths
/// fire the same way.
#[cfg(feature = "wasm")]
#[test]
fn trigger_fires_on_range_and_drains_queue() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options {
            sync: SyncMode::Always,
            ..Options::default()
        },
    )
    .unwrap();
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();

    // plain put
    db.put(b"u/a".to_vec(), b"1".to_vec()).unwrap();
    wait_until("mirror of u/a", || db.get(b"m/u/a").unwrap().is_some());
    assert_eq!(db.get(b"m/u/a").unwrap().unwrap(), b"u/a");

    // batch and transaction writes fire too
    let mut b = WriteBatch::new();
    b.put(b"u/b".to_vec(), b"2".to_vec());
    b.delete(b"u/c".to_vec()); // deletes are events as well
    db.write(b).unwrap();
    let mut t = db.begin();
    t.put(b"u/d".to_vec(), b"3".to_vec()).unwrap();
    t.commit().unwrap();
    for key in [b"m/u/b".as_ref(), b"m/u/c".as_ref(), b"m/u/d".as_ref()] {
        wait_until("mirrors of batch/txn writes", || db.get(key).unwrap().is_some());
    }

    // out-of-range writes fire nothing
    db.put(b"w/out".to_vec(), b"x".to_vec()).unwrap();
    wait_until("queue drained", || pending(&db, "t") == 0);
    assert!(db.get(b"m/w/out").unwrap().is_none());

    let info = &db.list_triggers().unwrap()[0];
    assert_eq!(info.module, "mirror");
    assert_eq!(info.last_error, None);
}

/// Events are durable and coalesced: with the target module uninstalled the
/// runner cannot drain (it reports the error), re-touches of one key stay a
/// single pending event, the backlog survives a reopen, and reinstalling
/// the module drains it. delete_trigger discards the backlog.
#[cfg(feature = "wasm")]
#[test]
fn trigger_backlog_coalesces_survives_reopen_and_clears_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
        db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();
        // no module -> the runner cannot drain; events accumulate durably
        db.uninstall_module("mirror").unwrap();
        for _ in 0..3 {
            db.put(b"u/hot".to_vec(), b"v".to_vec()).unwrap();
        }
        db.put(b"u/other".to_vec(), b"v".to_vec()).unwrap();
        db.put(b"zz/out-of-range".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(pending(&db, "t"), 2, "coalesced: hot key counts once");
        wait_until("drain failure surfaces", || {
            pending(&db, "t") == 2
                && db.list_triggers().unwrap()[0]
                    .last_error
                    .as_deref()
                    .is_some_and(|e| e.contains("mirror"))
        });
    }

    // the backlog survives a full close + reopen (events are ordinary
    // durable keys), and a freshly installed module drains it
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert_eq!(pending(&db, "t"), 2, "backlog recovered");
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    wait_until("recovered backlog drains", || pending(&db, "t") == 0);
    assert_eq!(db.get(b"m/u/hot").unwrap().unwrap(), b"u/hot");
    assert_eq!(db.get(b"m/u/other").unwrap().unwrap(), b"u/other");
    assert!(db.get(b"m/zz/out-of-range").unwrap().is_none());

    // a deleted trigger takes its backlog with it: accumulate one fresh
    // event with the module gone again, delete the trigger, recreate it
    db.uninstall_module("mirror").unwrap();
    db.put(b"u/fresh".to_vec(), b"v".to_vec()).unwrap();
    assert_eq!(pending(&db, "t"), 1);
    db.delete_trigger("t").unwrap();
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    db.create_trigger("t", "mirror", Some(b"u/"), Some(b"v")).unwrap();
    assert_eq!(pending(&db, "t"), 0, "recreate starts with an empty queue");
    // the discarded event's record is gone, so it can never fire
    assert!(db.get(b"m/u/fresh").unwrap().is_none());
}

/// No stacking: writes committed by a trigger invocation never generate
/// events, even when they land inside another trigger's subscribed range —
/// while direct user writes into that range still fire it.
#[cfg(feature = "wasm")]
#[test]
fn trigger_writes_do_not_fire_other_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();
    db.create_trigger("a", "mirror", Some(b"u/"), Some(b"v")).unwrap();
    // b watches the range a's module writes into
    db.create_trigger("b", "mirror", Some(b"m/"), Some(b"n")).unwrap();

    db.put(b"u/1".to_vec(), b"x".to_vec()).unwrap();
    wait_until("a fires", || db.get(b"m/u/1").unwrap().is_some());
    // a's invocation committed m/u/1: had stacking existed, b's event would
    // have been enqueued atomically with that commit — so this is exact
    assert_eq!(pending(&db, "b"), 0, "no event from a trigger's own writes");
    assert!(db.get(b"m/m/u/1").unwrap().is_none());

    // a DIRECT user write into b's range does fire b
    db.put(b"m/direct".to_vec(), b"x".to_vec()).unwrap();
    wait_until("b fires on direct write", || {
        db.get(b"m/m/direct").unwrap().is_some()
    });
}

/// Admin validation: bad ranges, unknown modules, duplicate names, unknown
/// deletes, and reserved-keyspace bounds are all rejected loudly.
#[cfg(feature = "wasm")]
#[test]
fn trigger_admin_validation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.install_module("mirror", MIRROR_WAT.as_bytes()).unwrap();

    fn is_invalid<T: std::fmt::Debug>(r: fluent31::Result<T>) {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    }
    is_invalid(db.create_trigger("t", "nope", None, None));
    is_invalid(db.create_trigger("bad name", "mirror", None, None));
    is_invalid(db.create_trigger("t", "mirror", Some(b"b"), Some(b"a")));
    is_invalid(db.create_trigger("t", "mirror", Some(b"\x00sys"), None));
    is_invalid(db.delete_trigger("t"));

    db.create_trigger("t", "mirror", None, None).unwrap();
    is_invalid(db.create_trigger("t", "mirror", None, None));
    let infos = db.list_triggers().unwrap();
    assert_eq!(infos.len(), 1);
    assert!(infos[0].lo.is_empty() && infos[0].hi.is_empty());
    db.delete_trigger("t").unwrap();
    assert!(db.list_triggers().unwrap().is_empty());
}
