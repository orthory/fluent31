//! Append-log journal: the rebuildability safety net. Proves the core
//! contract a system-of-record needs — when the database directory is lost or
//! damaged beyond self-recovery, replaying the independent journal reconstructs
//! the exact user-key state — plus the surrounding guarantees (survives attach
//! over an existing store, heals a lagged stream, refuses a foreign lineage,
//! tolerates a torn journal tail).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::journal::{self, Journal, JournalConfig};
use fluent31::{Db, Error, Options, SyncMode};

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

/// Tiny thresholds so a small workload forces rotation and compaction.
fn tiny_compaction() -> JournalConfig {
    JournalConfig {
        rotate_bytes: 16 << 10,
        compact_when_deltas_exceed: Some(1.0),
        compact_min_bytes: 8 << 10,
    }
}

/// (lowest file id, file count, total bytes) of the journal-*.log files.
fn journal_disk(dir: &std::path::Path) -> (u64, usize, u64) {
    let mut ids = Vec::new();
    let mut total = 0u64;
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(num) = name.strip_prefix("journal-").and_then(|r| r.strip_suffix(".log")) {
            ids.push(num.parse::<u64>().unwrap());
            total += e.metadata().unwrap().len();
        }
    }
    ids.sort_unstable();
    (*ids.first().expect("journal dir has no log files"), ids.len(), total)
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
// Compaction: journal disk stays bounded while deltas dwarf the live set
// ---------------------------------------------------------------------------

#[test]
fn compaction_bounds_journal_disk_and_rebuilds_exact_state() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        // a small sub queue caps how far one drained batch can overshoot the
        // compaction threshold, keeping the disk bound tight
        let o = Options {
            sub_queue_bytes: 32 << 10,
            ..opts()
        };
        let db = Arc::new(Db::open(db_dir.path(), o).unwrap());
        let journal =
            Journal::attach_with_config(db.clone(), jrn_dir.path(), tiny_compaction()).unwrap();

        // overwrite a fixed live set over and over: total deltas written
        // (~500 KiB) dwarf the live data (~15 KiB), so on-disk size stays
        // small only if compaction actually reclaims superseded files
        for round in 0..45u32 {
            for i in 0..150u32 {
                db.put(k(i), v(i, &format!("r{round:02}"))).unwrap();
            }
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain", 20, || journal.stats().last_seqno >= target);
        let stats = journal.stats();
        assert!(stats.compactions >= 2, "expected repeated compaction, got {}", stats.compactions);
        assert!(stats.files_pruned > 0, "compaction never pruned a file");
        drop(journal);
        expected
    };

    let (min_id, _, total) = journal_disk(jrn_dir.path());
    assert!(min_id > 1, "file 1 should have been pruned, lowest id is {min_id}");
    assert!(total < 160 << 10, "journal disk not bounded: {total} bytes on disk");

    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "rebuilt state diverged after compaction");
}

// ---------------------------------------------------------------------------
// Pruning file 1 must not lose the provenance header (it is re-emitted)
// ---------------------------------------------------------------------------

#[test]
fn pruning_preserves_the_provenance_guard() {
    let jrn_dir = tempfile::tempdir().unwrap();
    let opts_a = || Options {
        store_name: Some("store-a".into()),
        ..opts()
    };

    // store A owns the journal dir; enough churn that compaction prunes file 1
    let dir_a = tempfile::tempdir().unwrap();
    let expected = {
        let db = Arc::new(Db::open(dir_a.path(), opts_a()).unwrap());
        let journal =
            Journal::attach_with_config(db.clone(), jrn_dir.path(), tiny_compaction()).unwrap();
        for round in 0..10u32 {
            for i in 0..100u32 {
                db.put(k(i), v(i, &format!("r{round}"))).unwrap();
            }
        }
        let target = db.stats().visible_seqno;
        wait_until("drain + compact", 20, || {
            let s = journal.stats();
            s.last_seqno >= target && s.compactions >= 1
        });
        let expected = dump(&db);
        drop(journal);
        expected
    };
    let (min_id, _, _) = journal_disk(jrn_dir.path());
    assert!(min_id > 1, "compaction should have pruned file 1");

    // the header lives on in the new anchor file: a foreign store is refused
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
        Journal::attach(db_b, jrn_dir.path()).is_err(),
        "foreign store must still be refused after file 1 was pruned"
    );

    // ...while the owning store still re-attaches, and rebuild still works
    {
        let db = Arc::new(Db::open(dir_a.path(), opts_a()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        drop(journal);
    }
    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected);
}

// ---------------------------------------------------------------------------
// request_checkpoint() is the manual "compact now" hatch; None disables auto
// ---------------------------------------------------------------------------

#[test]
fn manual_checkpoint_compacts_and_prunes() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        let cfg = JournalConfig {
            compact_when_deltas_exceed: None, // manual compaction only
            ..tiny_compaction()
        };
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach_with_config(db.clone(), jrn_dir.path(), cfg).unwrap();
        for round in 0..8u32 {
            for i in 0..100u32 {
                db.put(k(i), v(i, &format!("r{round}"))).unwrap();
            }
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain", 20, || journal.stats().last_seqno >= target);
        assert_eq!(journal.stats().compactions, 0, "auto-compaction must stay off");
        let (min_id, files, _) = journal_disk(jrn_dir.path());
        assert_eq!(min_id, 1, "nothing may be pruned before the manual request");
        assert!(files > 1, "writes should have rotated into several files");

        journal.request_checkpoint();
        wait_until("manual compact", 10, || journal.stats().compactions >= 1);
        assert!(journal.stats().files_pruned > 0);
        drop(journal);
        expected
    };

    let (min_id, _, _) = journal_disk(jrn_dir.path());
    assert!(min_id > 1, "manual checkpoint should have pruned the old files");

    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected);
}

// ---------------------------------------------------------------------------
// Re-attach after a crash tail: post-reattach writes must survive to rebuild
// ---------------------------------------------------------------------------

#[test]
fn reattach_after_torn_tail_still_captures_new_writes() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    // session 1: journal some writes, then a crash leaves a torn tail
    {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 0..100u32 {
            db.put(k(i), v(i, "s1")).unwrap();
        }
        let target = db.stats().visible_seqno;
        wait_until("drain s1", 10, || journal.stats().last_seqno >= target);
        drop(journal);
    }
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

    // session 2: re-attach over the torn journal and keep writing
    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 100..200u32 {
            db.put(k(i), v(i, "s2")).unwrap();
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain s2", 10, || journal.stats().last_seqno >= target);
        drop(journal);
        expected
    };

    // rebuild must reflect session 2, not silently stop at the torn junk
    let rebuilt_dir = tempfile::tempdir().unwrap();
    journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "post-reattach writes lost behind torn tail");
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

// ---------------------------------------------------------------------------
// A base snapshot larger than rotate_bytes must not be split by rotation
// (issue #29: mid-base rotation corrupted the rebuild base span)
// ---------------------------------------------------------------------------

#[test]
fn base_snapshot_larger_than_rotate_bytes_still_rebuilds() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig {
        rotate_bytes: 4 << 10,
        compact_when_deltas_exceed: None,
        ..JournalConfig::default()
    };

    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        // pre-attach keyspace several times rotate_bytes: the attach-time base
        // snapshot alone overflows the rotation threshold
        for i in 0..300u32 {
            db.put(k(i), v(i, "pre")).unwrap();
        }
        db.flush().unwrap();

        let journal = Journal::attach_with_config(db.clone(), jrn_dir.path(), cfg).unwrap();
        // a few post-attach writes so deltas follow the oversized base
        for i in 300..320u32 {
            db.put(k(i), v(i, "post")).unwrap();
        }
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        wait_until("drain", 10, || journal.stats().last_seqno >= target);
        drop(journal);
        expected
    };

    // the whole base span stayed in file 1, which therefore outgrew the
    // rotation threshold instead of splitting
    let f1 = std::fs::metadata(jrn_dir.path().join("journal-000001.log")).unwrap().len();
    assert!(f1 > 4 << 10, "base span should keep file 1 oversized, got {f1} bytes");

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let report = journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(report.base_keys, 300, "attach-time base must carry the pre-attach keys");
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "rebuilt state diverged from original");
}

#[test]
fn compaction_base_larger_than_rotate_bytes_still_rebuilds() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig {
        rotate_bytes: 4 << 10,
        compact_when_deltas_exceed: None, // compact only on request
        ..JournalConfig::default()
    };

    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach_with_config(db.clone(), jrn_dir.path(), cfg).unwrap();
        // grow the live set well past rotate_bytes via the delta stream, then
        // compact: the fresh base into the anchor file overflows the threshold
        for i in 0..300u32 {
            db.put(k(i), v(i, "a")).unwrap();
        }
        let target = db.stats().visible_seqno;
        wait_until("drain", 10, || journal.stats().last_seqno >= target);
        journal.request_checkpoint();
        wait_until("compact", 10, || journal.stats().compactions >= 1);
        let expected = dump(&db);
        drop(journal);
        expected
    };

    let (min_id, _, _) = journal_disk(jrn_dir.path());
    assert!(min_id > 1, "compaction should have pruned the pre-checkpoint files");

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let report = journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(report.base_keys, 300, "anchor base must carry the whole live set");
    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "rebuilt state diverged after oversized compaction");
}

/// Frame one journal record the way the writer does: [len u32][crc u32][body].
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut rec = (payload.len() as u32).to_le_bytes().to_vec();
    rec.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    rec.extend_from_slice(payload);
    rec
}

#[test]
fn split_base_span_from_prior_writer_still_rebuilds() {
    // Journals written before the mid-base rotation fix can hold a base span
    // that straddles a file boundary, with the next file's provenance header
    // interleaved into it. The safety net must still rebuild those.
    let jrn_dir = tempfile::tempdir().unwrap();

    let header = {
        let mut p = vec![0u8, 1u8]; // TAG_HEADER, format 1
        p.extend_from_slice(&0xf115_e731_10c7_0001u64.to_le_bytes());
        p.extend_from_slice(&[0u8; 16]); // source instance id
        p
    };
    let checkpoint = |seq: u64| {
        let mut p = vec![1u8]; // TAG_CHECKPOINT
        p.extend_from_slice(&seq.to_le_bytes());
        p
    };
    let base = |key: &[u8], val: &[u8]| {
        let mut p = vec![2u8]; // TAG_BASE
        p.push(key.len() as u8); // uvarint (short) key len
        p.extend_from_slice(key);
        p.push(val.len() as u8);
        p.extend_from_slice(val);
        p
    };
    let base_end = |seq: u64| {
        let mut p = vec![3u8]; // TAG_BASE_END
        p.extend_from_slice(&seq.to_le_bytes());
        p
    };
    let delta_put = |seq: u64, key: &[u8], val: &[u8]| {
        let mut p = vec![4u8]; // TAG_DELTA
        p.extend_from_slice(&seq.to_le_bytes());
        p.push(1); // kind: Put
        p.push(key.len() as u8);
        p.extend_from_slice(key);
        p.push(val.len() as u8);
        p.extend_from_slice(val);
        p
    };

    // file 1: header, checkpoint, first half of the base — rotation hit here
    let mut f1 = frame(&header);
    f1.extend(frame(&checkpoint(5)));
    f1.extend(frame(&base(b"a", b"1")));
    f1.extend(frame(&base(b"b", b"2")));
    std::fs::write(jrn_dir.path().join("journal-000001.log"), f1).unwrap();

    // file 2: header (from the rotation), rest of the base, then a delta
    let mut f2 = frame(&header);
    f2.extend(frame(&base(b"c", b"3")));
    f2.extend(frame(&base_end(5)));
    f2.extend(frame(&delta_put(6, b"d", b"4")));
    std::fs::write(jrn_dir.path().join("journal-000002.log"), f2).unwrap();

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let report = journal::rebuild(jrn_dir.path(), rebuilt_dir.path(), opts()).unwrap();
    assert_eq!(report.base_keys, 3, "all base records across the split must apply");
    assert_eq!(report.deltas_applied, 1);
    assert_eq!(report.last_seqno, 6);

    let rebuilt = Db::open(rebuilt_dir.path(), opts()).unwrap();
    let want: BTreeMap<Vec<u8>, Vec<u8>> = [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]
        .into_iter()
        .map(|(key, val)| (key.to_vec(), val.to_vec()))
        .collect();
    assert_eq!(dump(&rebuilt), want);
}

// ---------------------------------------------------------------------------
// Continuity: a damaged chain is a loud, classified error — never silent loss
// ---------------------------------------------------------------------------

/// Journal spread over several small files (auto-compaction off so nothing
/// is pruned); returns the id of its lowest file. `min_id + 1` is always a
/// removable middle.
fn build_multi_file_journal(db_dir: &std::path::Path, jrn_dir: &std::path::Path) -> u64 {
    let cfg = JournalConfig {
        rotate_bytes: 4 << 10,
        compact_when_deltas_exceed: None,
        ..JournalConfig::default()
    };
    let db = Arc::new(Db::open(db_dir, opts()).unwrap());
    let journal = Journal::attach_with_config(db.clone(), jrn_dir, cfg).unwrap();
    for i in 0..400u32 {
        db.put(k(i), v(i, "a")).unwrap();
    }
    let target = db.stats().visible_seqno;
    wait_until("drain", 10, || journal.stats().last_seqno >= target);
    drop(journal);
    let (min_id, files, _) = journal_disk(jrn_dir);
    assert!(files >= 3, "need a removable middle file, got {files}");
    min_id
}

#[test]
fn missing_middle_file_is_a_loud_gap_error() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();
    let min_id = build_multi_file_journal(db_dir.path(), jrn_dir.path());

    // a middle segment vanishes (bad disk, incomplete copy of shipped files)
    std::fs::remove_file(jrn_dir.path().join(format!("journal-{:06}.log", min_id + 1))).unwrap();

    // rebuild must refuse — replaying across the hole would silently drop
    // every mutation the missing file held — and must say "gap", so a
    // caller can go fetch the missing segment rather than triage corruption
    let dest = tempfile::tempdir().unwrap();
    let err = journal::rebuild(jrn_dir.path(), dest.path(), opts()).unwrap_err();
    assert!(matches!(err, Error::JournalGap(_)), "want JournalGap, got {err:?}");
}

#[test]
fn torn_middle_file_is_corruption_not_a_gap() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();
    let min_id = build_multi_file_journal(db_dir.path(), jrn_dir.path());

    // a middle file is present but damaged: sealed files never end torn
    let path = jrn_dir.path().join(format!("journal-{:06}.log", min_id + 1));
    let len = std::fs::metadata(&path).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(len - 3).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let err = journal::rebuild(jrn_dir.path(), dest.path(), opts()).unwrap_err();
    assert!(matches!(err, Error::Corruption(_)), "want Corruption, got {err:?}");
}

#[test]
fn delta_seqno_regression_is_corruption() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 0..50u32 {
            db.put(k(i), v(i, "a")).unwrap();
        }
        let target = db.stats().visible_seqno;
        wait_until("drain", 10, || journal.stats().last_seqno >= target);
        drop(journal);
    }

    // hand-append a well-framed delta whose seqno runs backwards — the
    // stream is strictly ascending, so this models a reordered/rewritten log
    let mut payload = vec![4u8]; // TAG_DELTA
    payload.extend_from_slice(&1u64.to_le_bytes()); // seqno 1: far in the past
    payload.push(1); // kind: Put
    payload.extend_from_slice(&[1, b'k']); // uvarint key len + key
    payload.extend_from_slice(&[1, b'v']); // uvarint value len + value
    let mut rec = (payload.len() as u32).to_le_bytes().to_vec();
    rec.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    rec.extend_from_slice(&payload);
    let newest = std::fs::read_dir(jrn_dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .max()
        .unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&newest).unwrap();
        f.write_all(&rec).unwrap();
    }

    let dest = tempfile::tempdir().unwrap();
    let err = journal::rebuild(jrn_dir.path(), dest.path(), opts()).unwrap_err();
    assert!(matches!(err, Error::Corruption(_)), "want Corruption, got {err:?}");
}
