//! Append-log journal: the rebuildability safety net. Proves the core
//! contract a system-of-record needs — when the database directory is lost or
//! damaged beyond self-recovery, replaying the independent journal reconstructs
//! the exact user-key state — plus the surrounding guarantees (survives attach
//! over an existing store, heals a lagged stream, refuses a foreign lineage,
//! tolerates a torn journal tail).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::journal::{self, Journal};
use fluent31::{Db, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 8 << 10,
        value_threshold: 64,
        ..Options::default()
    }
}

fn k(i: u32) -> Vec<u8> {
    format!("key/{i:06}").into_bytes()
}
fn v(i: u32, tag: &str) -> Vec<u8> {
    format!("val-{tag}-{i:06}-{}", "x".repeat(40)).into_bytes()
}

/// Full live contents of a db as a map (user keyspace).
fn dump(db: &Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn wait_until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "not reached within {secs}s: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// The core contract: nuke the DB, rebuild from the journal, identical state
// ---------------------------------------------------------------------------

#[test]
fn rebuild_from_journal_reconstructs_exact_state() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();

        // a workload with overwrites and deletes across memtable + tables
        for i in 0..800u32 {
            db.put(k(i), v(i, "a")).unwrap();
        }
        db.flush().unwrap();
        for i in 0..400u32 {
            db.put(k(i), v(i, "b")).unwrap(); // overwrite
        }
        for i in (0..800u32).step_by(7) {
            db.delete(k(i)).unwrap(); // delete every 7th
        }
        db.flush().unwrap();

        let expected = dump(&db);
        // let the journal drain everything we acked
        let target = db.stats().visible_seqno;
        wait_until("journal catches up", 10, || journal.stats().last_seqno >= target);
        drop(journal); // clean stop + final flush
        expected
    };

    // catastrophe: the entire database directory is gone
    std::fs::remove_dir_all(db_dir.path()).unwrap();

    // rebuild into a fresh directory purely from the journal
    let rebuilt_dir = tempfile::tempdir().unwrap();
    let report = journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    // attached at genesis: the base is empty and every write is a delta
    assert!(report.deltas_applied > 0);

    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "rebuilt state diverged from original");
}

// ---------------------------------------------------------------------------
// Attaching over a non-empty store: the base snapshot captures prior state
// ---------------------------------------------------------------------------

#[test]
fn journal_captures_state_that_predates_attach() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        // write BEFORE the journal exists — only a base snapshot can capture this
        for i in 0..300u32 {
            db.put(k(i), v(i, "pre")).unwrap();
        }
        db.flush().unwrap();

        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        // then more writes that flow through the stream
        for i in 300..500u32 {
            db.put(k(i), v(i, "post")).unwrap();
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain", 10, || journal.stats().last_seqno >= target);
        drop(journal);
        expected
    };

    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected);
}

// ---------------------------------------------------------------------------
// The journal keeps up under sustained write pressure (heals lag if it can't)
// ---------------------------------------------------------------------------

#[test]
fn journal_survives_write_pressure_and_still_rebuilds() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        // a tiny sub-queue makes the stream far more likely to lag, exercising
        // the rebaseline heal path
        let o = Options {
            sub_queue_bytes: 4 << 10,
            ..opts()
        };
        let db = Arc::new(Db::open(db_dir.path(), o).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();

        for round in 0..4u32 {
            for i in 0..500u32 {
                db.put(k(i), v(i, &format!("r{round}"))).unwrap();
            }
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain under pressure", 20, || journal.stats().last_seqno >= target);
        drop(journal);
        expected
    };

    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected);
}

// ---------------------------------------------------------------------------
// Journal survives a full close + reopen (re-attach) of the source db
// ---------------------------------------------------------------------------

#[test]
fn journal_reattaches_across_db_restart() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 0..200u32 {
            db.put(k(i), v(i, "s1")).unwrap();
        }
        let target = db.stats().visible_seqno;
        wait_until("drain s1", 10, || journal.stats().last_seqno >= target);
        drop(journal);
    }

    // restart the db and re-attach the journal to the same directory
    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 200..400u32 {
            db.put(k(i), v(i, "s2")).unwrap();
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain s2", 10, || journal.stats().last_seqno >= target);
        drop(journal);
        expected
    };

    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected);
}

// ---------------------------------------------------------------------------
// Provenance: a journal dir is bound to one store lineage
// ---------------------------------------------------------------------------

#[test]
fn journal_refuses_a_foreign_store() {
    let jrn_dir = tempfile::tempdir().unwrap();

    // store A (named) owns the journal dir
    let dir_a = tempfile::tempdir().unwrap();
    let db_a = Arc::new(
        Db::open(
            dir_a.path(),
            Options {
                store_name: Some("store-a".into()),
                ..opts()
            },
        )
        .unwrap(),
    );
    let j = Journal::attach(db_a.clone(), jrn_dir.path()).unwrap();
    db_a.put(k(1), v(1, "a")).unwrap();
    drop(j);

    // a different named store cannot attach to the same journal dir
    let dir_b = tempfile::tempdir().unwrap();
    let db_b = Arc::new(
        Db::open(
            dir_b.path(),
            Options {
                store_name: Some("store-b".into()),
                ..opts()
            },
        )
        .unwrap(),
    );
    assert!(
        Journal::attach(db_b.clone(), jrn_dir.path()).is_err(),
        "foreign store must be refused by provenance"
    );
}

// ---------------------------------------------------------------------------
// A torn journal tail (crash mid-append) still rebuilds the clean prefix
// ---------------------------------------------------------------------------

#[test]
fn torn_journal_tail_rebuilds_the_clean_prefix() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 0..300u32 {
            db.put(k(i), v(i, "a")).unwrap();
        }
        let target = db.stats().visible_seqno;
        wait_until("drain", 10, || journal.stats().last_seqno >= target);
        drop(journal);
    }

    // simulate a crash mid-append: append garbage that can't frame a record
    let newest = std::fs::read_dir(jrn_dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .max()
        .unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&newest).unwrap();
        f.write_all(&[0xff; 40]).unwrap(); // partial/garbage frame
    }

    // rebuild still succeeds and yields a consistent prefix (all 300 present:
    // they were fully appended before the torn junk)
    let rebuilt_dir = tempfile::tempdir().unwrap();
    let report = journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    let got = dump(&rebuilt);
    assert!(report.deltas_applied > 0);
    for i in 0..300u32 {
        assert_eq!(got.get(&k(i)).cloned(), Some(v(i, "a")), "key {i} after torn tail");
    }
}

// ---------------------------------------------------------------------------
// Empty / malformed journal directories fail loudly
// ---------------------------------------------------------------------------

#[test]
fn rebuild_from_empty_journal_errors() {
    let empty = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    assert!(journal::rebuild(empty.path(), dest.path(), opts()).is_err());
}
