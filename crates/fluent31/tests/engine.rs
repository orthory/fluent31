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
