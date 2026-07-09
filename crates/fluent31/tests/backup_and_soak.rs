//! Operational backup/restore drill and an opt-in endurance soak.
//!
//! Backup/restore proves the fork-as-backup path end to end: cut a fork while
//! the store is under write load, relocate it with `restore_to` (as you would
//! copy an archive to another host), and open it as a fully independent,
//! consistent point-in-time database. The soak (ignored by default) runs a
//! long overwrite/delete/GC workload and asserts correctness plus bounded
//! space — the endurance properties unit tests are too short to reveal.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::collections::BTreeMap;

use fluent31::{restore_to, Db, Options, SyncMode};

fn opts() -> Options {
    named_opts(Some("prod"))
}

fn named_opts(name: Option<&str>) -> Options {
    Options {
        sync: SyncMode::Never,
        store_name: name.map(String::from),
        memtable_size: 16 << 10,
        value_threshold: 64,
        vlog_file_size: 32 << 10,
        vlog_gc_ratio: 0.3,
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------
// Backup/restore drill
// ---------------------------------------------------------------------------

#[test]
fn fork_backup_under_load_restores_to_an_independent_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), opts()).unwrap());

    // committed baseline that must appear in any later cut
    for i in 0..500u32 {
        db.put(format!("base/{i:05}").into_bytes(), format!("b{i}").into_bytes()).unwrap();
    }

    // background write load while we take the backup
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Acquire) {
                db.put(format!("churn/{i:08}").into_bytes(), b"c".to_vec()).unwrap();
                i += 1;
            }
        })
    };
    // let the writer get going, then cut a live backup
    std::thread::sleep(std::time::Duration::from_millis(50));
    let fork = db.fork("backup").unwrap();
    stop.store(true, Ordering::Release);
    writer.join().unwrap();

    // relocate the archive to a fresh location (as if copied to another host);
    // restore_to requires a not-yet-existing destination directory
    let restore_parent = tempfile::tempdir().unwrap();
    let restored_path = restore_parent.path().join("restored");
    restore_to(&fork.path, &restored_path, Some("restored")).unwrap();

    // it opens as an independent, consistent database
    let restored = Db::open(&restored_path, named_opts(Some("restored"))).unwrap();

    // the whole committed baseline (written before the cut) is present
    for i in 0..500u32 {
        assert_eq!(
            restored.get(&format!("base/{i:05}").into_bytes()).unwrap().unwrap(),
            format!("b{i}").into_bytes(),
            "restored backup missing base/{i}"
        );
    }
    // a full scan of the restore parses cleanly (no torn values from the cut)
    let n = restored.iter(None, None, false).unwrap().map(|r| r.unwrap()).count();
    assert!(n >= 500);

    // the restore is independently writable and isolated from the parent
    restored.put(b"only/in/restore".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"only/in/parent".to_vec(), b"1".to_vec()).unwrap();
    assert!(restored.get(b"only/in/parent").unwrap().is_none());
    assert!(db.get(b"only/in/restore").unwrap().is_none());

    // and it survives its own reopen
    drop(restored);
    let restored = Db::open(&restored_path, named_opts(Some("restored"))).unwrap();
    assert_eq!(restored.get(b"base/00000").unwrap().unwrap(), b"b0");
    assert_eq!(restored.get(b"only/in/restore").unwrap().unwrap(), b"1");
}

// ---------------------------------------------------------------------------
// Endurance soak — opt in with:  cargo test --test backup_and_soak -- --ignored
// ---------------------------------------------------------------------------

#[test]
#[ignore = "endurance soak; run explicitly with --ignored"]
fn soak_overwrite_delete_gc_stays_correct_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = 0x1234_5678u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    const KEYSPACE: u64 = 20_000; // bounded live set — heavy overwriting
    const ROUNDS: usize = 120;
    const PER_ROUND: usize = 5_000;

    for round in 0..ROUNDS {
        for _ in 0..PER_ROUND {
            let r = next();
            let key = format!("k/{:08}", r % KEYSPACE).into_bytes();
            if r % 9 == 0 {
                db.delete(key.clone()).unwrap();
                model.remove(&key);
            } else {
                let val = format!("v{}-{}", r, "p".repeat((r % 200) as usize)).into_bytes();
                db.put(key.clone(), val.clone()).unwrap();
                model.insert(key, val);
            }
        }
        if round % 5 == 0 {
            db.flush().unwrap();
            db.compact_all().unwrap();
            while db.gc_vlog().unwrap().is_some() {}
        }
        // the flush pipeline keeps up — no unbounded frozen-memtable backlog
        let s = db.stats();
        assert!(s.immutable_memtables <= 3, "flush backlog grew unbounded (round {round})");
    }

    db.flush().unwrap();
    db.compact_all().unwrap();
    while db.gc_vlog().unwrap().is_some() {}

    // full correctness against the model
    let scanned: BTreeMap<Vec<u8>, Vec<u8>> =
        db.iter(None, None, false).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(scanned, model, "soak diverged from reference");

    // bounded space: after GC the on-disk footprint is within a small multiple
    // of the live data (no unbounded garbage accumulation from overwrites)
    let live_bytes: u64 = model.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
    let on_disk = dir_bytes(dir.path());
    assert!(
        on_disk < live_bytes * 8 + (64 << 20),
        "space amplification too high: {on_disk} on disk vs {live_bytes} live"
    );

    // survives a reopen with the full model intact
    drop(db);
    let db = Db::open(dir.path(), opts()).unwrap();
    let after: BTreeMap<Vec<u8>, Vec<u8>> =
        db.iter(None, None, false).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(after, model, "soak state lost across reopen");
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if let Ok(m) = e.metadata() {
            if m.is_file() {
                total += m.len();
            }
        }
    }
    total
}
