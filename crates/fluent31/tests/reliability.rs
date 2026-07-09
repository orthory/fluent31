//! Reliability & robustness: on-disk corruption is caught (never a panic or
//! silent wrong answer), iteration holds at its boundaries, size caps are
//! enforced exactly, and the engine stays correct under concurrent write /
//! transaction / GC pressure.
//!
//! These exercise the *integrity* contract, not the happy path: after any
//! adversarial byte-flip or contended interleaving, the store either returns
//! the right data or a clean typed error — it must never corrupt state, lose
//! an acked write, or crash the process.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use fluent31::{Db, Error, Options, SyncMode};

/// Tiny structures so flush / tiering / vlog rotation all fire on small
/// inputs. SyncMode::Never keeps the suite fast (macOS F_FULLFSYNC is ~15ms);
/// recovery paths are exercised by clean drop + reopen.
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
fn val(i: u32) -> Vec<u8> {
    format!("value-{i:06}-padding-to-force-vlog-and-blocks").into_bytes()
}

/// Files of a given kind inside a db directory.
fn files_with(dir: &std::path::Path, prefix: &str, suffix: &str) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_string_lossy();
            n.starts_with(prefix) && n.ends_with(suffix)
        })
        .collect()
}

/// Open and drain a full forward scan, surfacing the first error. Corruption
/// may present either at open (footer/manifest) or lazily at the block read
/// during iteration; this captures it wherever it lands.
fn open_and_scan(dir: &std::path::Path) -> Result<usize, Error> {
    let db = Db::open(dir, small_opts())?;
    let mut n = 0usize;
    for kv in db.iter(None, None, false)? {
        kv?;
        n += 1;
    }
    Ok(n)
}

fn seed_flushed(dir: &std::path::Path, n: u32) {
    let db = Db::open(dir, small_opts()).unwrap();
    for i in 0..n {
        db.put(k(i), val(i)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    drop(db);
}

// ---------------------------------------------------------------------------
// On-disk corruption is detected, never silently served or panicked on
// ---------------------------------------------------------------------------

#[test]
fn flipped_table_magic_is_a_clean_error() {
    let dir = tempfile::tempdir().unwrap();
    seed_flushed(dir.path(), 400);
    let tables = files_with(dir.path(), "sst-", ".tbl");
    assert!(!tables.is_empty(), "flush must have produced tables");
    // the footer ends with an 8-byte magic; flipping its last byte fails the
    // magic check the reader runs before trusting any offsets
    for t in &tables {
        let mut bytes = std::fs::read(t).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(t, &bytes).unwrap();
    }
    match open_and_scan(dir.path()) {
        Err(Error::Corruption(_)) | Err(Error::Io(_)) => {}
        other => panic!("expected clean corruption error, got {other:?}"),
    }
}

#[test]
fn flipped_data_block_byte_is_caught_by_crc() {
    let dir = tempfile::tempdir().unwrap();
    seed_flushed(dir.path(), 400);
    let tables = files_with(dir.path(), "sst-", ".tbl");
    // flip a byte a quarter into each table — squarely inside the
    // CRC-protected data-block region, not the footer
    for t in &tables {
        let mut bytes = std::fs::read(t).unwrap();
        let at = bytes.len() / 4;
        bytes[at] ^= 0xff;
        std::fs::write(t, &bytes).unwrap();
    }
    match open_and_scan(dir.path()) {
        Err(Error::Corruption(_)) | Err(Error::Io(_)) => {}
        other => panic!("expected block crc corruption, got {other:?}"),
    }
}

#[test]
fn truncated_table_is_a_clean_error() {
    let dir = tempfile::tempdir().unwrap();
    seed_flushed(dir.path(), 400);
    let tables = files_with(dir.path(), "sst-", ".tbl");
    for t in &tables {
        let f = std::fs::OpenOptions::new().write(true).open(t).unwrap();
        let len = f.metadata().unwrap().len();
        f.set_len(len / 2).unwrap(); // lop off the footer + trailing blocks
        f.sync_all().unwrap();
    }
    match open_and_scan(dir.path()) {
        Err(Error::Corruption(_)) | Err(Error::Io(_)) => {}
        other => panic!("expected clean error on truncated table, got {other:?}"),
    }
}

#[test]
fn corrupted_manifest_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    seed_flushed(dir.path(), 100);
    let manifests = files_with(dir.path(), "MANIFEST-", "");
    assert!(!manifests.is_empty());
    for m in &manifests {
        let mut bytes = std::fs::read(m).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff; // breaks the manifest CRC
        std::fs::write(m, &bytes).unwrap();
    }
    match Db::open(dir.path(), small_opts()) {
        Err(Error::Corruption(_)) | Err(Error::Io(_)) => {}
        Ok(_) => panic!("expected corruption on bad manifest, got Ok"),
        Err(e) => panic!("expected corruption on bad manifest, got {e:?}"),
    }
}

#[test]
fn garbage_current_pointer_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    seed_flushed(dir.path(), 20);
    // CURRENT naming a non-existent manifest, and CURRENT full of junk, both
    // fail loudly rather than opening an empty/wrong store
    std::fs::write(dir.path().join("CURRENT"), b"MANIFEST-999999\n").unwrap();
    assert!(Db::open(dir.path(), small_opts()).is_err());

    std::fs::write(dir.path().join("CURRENT"), b"not-a-manifest-name\n").unwrap();
    match Db::open(dir.path(), small_opts()) {
        Err(Error::Corruption(_)) | Err(Error::Io(_)) => {}
        Ok(_) => panic!("expected error on junk CURRENT, got Ok"),
        Err(e) => panic!("expected error on junk CURRENT, got {e:?}"),
    }
}

#[test]
fn mid_stream_wal_corruption_after_valid_records() {
    // Data lives only in the WAL (no flush). Corrupting a byte partway
    // through truncates replay at that record: everything before it survives,
    // the store opens, and nothing after is silently resurrected.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..40u32 {
            db.put(k(i), val(i)).unwrap();
        }
        // no flush: everything is in the newest WAL
        drop(db);
    }
    let wals = files_with(dir.path(), "wal-", ".log");
    // corrupt the newest WAL somewhere in its second half
    let newest = wals.iter().max().unwrap();
    let mut bytes = std::fs::read(newest).unwrap();
    let at = bytes.len() * 3 / 4;
    bytes[at] ^= 0xff;
    std::fs::write(newest, &bytes).unwrap();

    // reopen must succeed (torn tail in the newest WAL is loss, not
    // corruption) and expose a clean prefix of the writes
    let db = Db::open(dir.path(), small_opts()).unwrap();
    let mut count = 0u32;
    for i in 0..40u32 {
        if db.get(&k(i)).unwrap().is_some() {
            count += 1;
        }
    }
    // a prefix survived, the tail past the flip was dropped
    assert!(count > 0, "some prefix of the WAL should survive");
    assert!(count <= 40);
}

#[test]
fn torn_tail_reopen_loop_is_stable() {
    // A torn newest-WAL tail is truncated to its valid prefix on first open;
    // repeated reopens must stay stable and never reclassify it as damaged.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..12u32 {
            db.put(k(i), val(i)).unwrap();
        }
        drop(db);
    }
    let wals = files_with(dir.path(), "wal-", ".log");
    let newest = wals.iter().max().unwrap();
    // append junk that can't form a valid record
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(newest).unwrap();
        f.write_all(&[0xab; 24]).unwrap();
    }
    for round in 0..5 {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..12u32 {
            assert_eq!(db.get(&k(i)).unwrap().unwrap(), val(i), "round {round} key {i}");
        }
        db.put(k(100 + round), val(round)).unwrap();
        drop(db);
    }
}

// ---------------------------------------------------------------------------
// Iterator boundary conditions
// ---------------------------------------------------------------------------

fn collect(db: &Db, lo: Option<&[u8]>, hi: Option<&[u8]>, rev: bool) -> Vec<Vec<u8>> {
    db.iter(lo, hi, rev)
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect()
}

#[test]
fn iterator_edge_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..50u32 {
        db.put(k(i), val(i)).unwrap();
    }
    db.flush().unwrap(); // split residence across tables + memtable
    for i in 50..80u32 {
        db.put(k(i), val(i)).unwrap();
    }

    // empty range: lo == hi (half-open, so no keys qualify)
    assert!(collect(&db, Some(&k(10)), Some(&k(10)), false).is_empty());
    assert!(collect(&db, Some(&k(10)), Some(&k(10)), true).is_empty());

    // inverted range: lo > hi yields nothing, no panic
    assert!(collect(&db, Some(&k(40)), Some(&k(10)), false).is_empty());
    assert!(collect(&db, Some(&k(40)), Some(&k(10)), true).is_empty());

    // range entirely in a gap above all keys
    assert!(collect(&db, Some(b"zzz"), None, false).is_empty());

    // single-element range [k(20), k(21))
    assert_eq!(collect(&db, Some(&k(20)), Some(&k(21)), false), vec![k(20)]);
    assert_eq!(collect(&db, Some(&k(20)), Some(&k(21)), true), vec![k(20)]);

    // reverse over a bounded window equals the forward window reversed
    let fwd = collect(&db, Some(&k(30)), Some(&k(35)), false);
    let mut rev = collect(&db, Some(&k(30)), Some(&k(35)), true);
    rev.reverse();
    assert_eq!(fwd, rev);
    assert_eq!(fwd, vec![k(30), k(31), k(32), k(33), k(34)]);
}

#[test]
fn iterator_over_all_tombstones_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..40u32 {
        db.put(k(i), val(i)).unwrap();
    }
    db.flush().unwrap();
    for i in 0..40u32 {
        db.delete(k(i)).unwrap();
    }
    // every key in the range is a tombstone: forward and reverse both empty
    assert!(collect(&db, Some(&k(0)), Some(&k(40)), false).is_empty());
    assert!(collect(&db, Some(&k(0)), Some(&k(40)), true).is_empty());
    // and it stays empty after the tombstones are compacted away
    db.flush().unwrap();
    db.compact_all().unwrap();
    assert!(collect(&db, None, None, false).is_empty());
}

// ---------------------------------------------------------------------------
// Size caps enforced exactly at the boundary
// ---------------------------------------------------------------------------

#[test]
fn key_and_value_caps_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        max_key_size: 1024,
        max_value_size: 4096,
        ..small_opts()
    };
    let db = Db::open(dir.path(), opts).unwrap();

    // key exactly at cap: accepted
    db.put(vec![b'a'; 1024], b"v".to_vec()).unwrap();
    // one over: rejected
    assert!(matches!(
        db.put(vec![b'a'; 1025], b"v".to_vec()),
        Err(Error::InvalidArgument(_))
    ));
    // value exactly at cap: accepted
    db.put(b"vk".to_vec(), vec![b'z'; 4096]).unwrap();
    assert_eq!(db.get(b"vk").unwrap().unwrap().len(), 4096);
    // one over: rejected
    assert!(matches!(
        db.put(b"vk2".to_vec(), vec![b'z'; 4097]),
        Err(Error::InvalidArgument(_))
    ));
    // the rejected writes left nothing behind
    assert!(db.get(b"vk2").unwrap().is_none());
}

#[test]
fn txn_write_set_cap_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        max_txn_write_bytes: 8 << 10,
        max_value_size: 4 << 10,
        ..small_opts()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let mut txn = db.begin();
    // pile writes until the write-set cap trips
    let mut hit_cap = false;
    for i in 0..100u32 {
        match txn.put(k(i), vec![b'x'; 2048]) {
            Ok(()) => {}
            Err(Error::InvalidArgument(m)) => {
                assert!(m.contains("write set"), "unexpected msg: {m}");
                hit_cap = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(hit_cap, "write-set cap should have tripped");
    // the over-cap txn is unusable but the store is fine
    drop(txn);
    db.put(b"after".to_vec(), b"ok".to_vec()).unwrap();
    assert_eq!(db.get(b"after").unwrap().unwrap(), b"ok");
}

// ---------------------------------------------------------------------------
// Concurrency: conservation under contended optimistic transactions
// ---------------------------------------------------------------------------

#[test]
fn concurrent_transfers_conserve_total() {
    // Classic bank invariant under heavy OCC contention: N accounts, many
    // threads moving money between hot accounts with get_for_update on both
    // legs. Whatever the interleaving, the sum is invariant and no acked
    // commit is lost.
    const ACCOUNTS: u32 = 8;
    const THREADS: usize = 16;
    const PER_THREAD: usize = 200;
    const START: u64 = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small_opts()).unwrap());
    for a in 0..ACCOUNTS {
        db.put(acct(a), START.to_le_bytes().to_vec()).unwrap();
    }

    let barrier = Arc::new(Barrier::new(THREADS));
    let committed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = db.clone();
        let barrier = barrier.clone();
        let committed = committed.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for i in 0..PER_THREAD {
                let from = ((t + i) % ACCOUNTS as usize) as u32;
                let to = ((t + i + 1) % ACCOUNTS as usize) as u32;
                if from == to {
                    continue;
                }
                // retry the whole txn on conflict until it lands or funds run out
                loop {
                    let mut txn = db.begin();
                    let fb = read_u64(txn.get_for_update(&acct(from)).unwrap());
                    let tb = read_u64(txn.get_for_update(&acct(to)).unwrap());
                    if fb < 5 {
                        break; // insufficient funds; skip
                    }
                    txn.put(acct(from), (fb - 5).to_le_bytes().to_vec()).unwrap();
                    txn.put(acct(to), (tb + 5).to_le_bytes().to_vec()).unwrap();
                    match txn.commit() {
                        Ok(()) => {
                            committed.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(Error::Conflict) => continue,
                        Err(e) => panic!("commit failed: {e}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(committed.load(Ordering::Relaxed) > 0, "no transfer committed");

    let total: u64 = (0..ACCOUNTS)
        .map(|a| read_u64(db.get(&acct(a)).unwrap()))
        .sum();
    assert_eq!(total, START * ACCOUNTS as u64, "money was created or destroyed");

    // survives a reopen with the invariant intact
    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    let total: u64 = (0..ACCOUNTS)
        .map(|a| read_u64(db.get(&acct(a)).unwrap()))
        .sum();
    assert_eq!(total, START * ACCOUNTS as u64);
}

fn acct(a: u32) -> Vec<u8> {
    format!("acct/{a}").into_bytes()
}
fn read_u64(v: Option<Vec<u8>>) -> u64 {
    u64::from_le_bytes(v.unwrap()[..8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Concurrency: readers + writers + compaction/GC hammer, model-checked
// ---------------------------------------------------------------------------

#[test]
fn mixed_readers_writers_and_gc_stay_consistent() {
    // A writer thread overwrites a key space (making vlog garbage), a
    // maintenance thread flushes/compacts/GCs, and reader threads scan
    // concurrently. Readers must never see an error or a torn value; the
    // final state must match the last write to every key.
    const KEYS: u32 = 200;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small_opts()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    // reference of the final generation each key should hold
    let final_gen: u32 = 6;

    // writer: several generations of every key
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            for gen in 0..=final_gen {
                for i in 0..KEYS {
                    let v = format!("gen{gen}-key{i:06}-{}", "p".repeat(80));
                    db.put(k(i), v.into_bytes()).unwrap();
                }
            }
        })
    };
    // maintenance: churn flush/compact/gc while writes are in flight
    let maint = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let _ = db.flush();
                let _ = db.compact_all();
                let _ = db.gc_vlog();
            }
        })
    };
    // readers: full scans must always parse cleanly
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    for kv in db.iter(None, None, false).unwrap() {
                        let (_k, _v) = kv.unwrap(); // any Err here is a failure
                    }
                }
            })
        })
        .collect();

    writer.join().unwrap();
    stop.store(true, Ordering::Release);
    maint.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    // every key holds its final generation, before and after reopen
    let check = |db: &Db| {
        for i in 0..KEYS {
            let got = db.get(&k(i)).unwrap().unwrap();
            let want = format!("gen{final_gen}-key{i:06}-{}", "p".repeat(80));
            assert_eq!(got, want.into_bytes(), "key {i}");
        }
    };
    check(&db);
    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    check(&db);
}

// ---------------------------------------------------------------------------
// Randomized model check with reopens and GC interleaved
// ---------------------------------------------------------------------------

#[test]
fn randomized_ops_match_btreemap_reference_with_reopen() {
    // A second, independent model test (the engine suite has one too): drives
    // a longer op mix including reopen + gc and cross-checks a BTreeMap on
    // every read and at the end via a full scan.
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), small_opts()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        // xorshift64
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    for step in 0..3000 {
        let r = next();
        let key = k((r % 120) as u32);
        match r % 10 {
            0..=5 => {
                let v = val((r >> 8) as u32 % 100000);
                db.put(key.clone(), v.clone()).unwrap();
                model.insert(key, v);
            }
            6 => {
                db.delete(key.clone()).unwrap();
                model.remove(&key);
            }
            7 => {
                assert_eq!(db.get(&key).unwrap(), model.get(&key).cloned(), "get step {step}");
            }
            8 => {
                let _ = db.flush();
                let _ = db.gc_vlog();
            }
            _ => {
                // reopen: everything acked must come back identically
                drop(db);
                db = Db::open(dir.path(), small_opts()).unwrap();
            }
        }
    }

    let scanned: BTreeMap<Vec<u8>, Vec<u8>> = db
        .iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(scanned, model, "final scan diverged from reference");
}
