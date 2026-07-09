//! Fork creation under concurrent load. Writers, flush, compaction, and
//! vlog GC all run while forks are cut; each fork is then opened and
//! verified: every key acked before the cut is present at a round within
//! its [floor, ceiling] bounds, and a full scan surfaces any dangling
//! vlog pointers left by a GC/link race.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use fluent31::{Db, Options, SyncMode};

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

const WRITERS: usize = 4;
const KEYS_PER_WRITER: usize = 48;
const FORKS: usize = 6;

fn key(w: usize, i: usize) -> String {
    format!("w{w}-k{i:04}")
}

fn counters() -> Arc<Vec<Vec<AtomicU32>>> {
    Arc::new(
        (0..WRITERS)
            .map(|_| (0..KEYS_PER_WRITER).map(|_| AtomicU32::new(0)).collect())
            .collect(),
    )
}

fn round_of(val: &[u8]) -> u32 {
    let s = std::str::from_utf8(val).expect("utf8 value");
    s.split("-r")
        .nth(1)
        .and_then(|t| t.split('-').next())
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("malformed value {s:?}"))
}

#[test]
fn fork_under_concurrent_load() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small_opts()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));

    // acked[w][i] = highest round whose put() has returned. started[w][i] =
    // highest round whose put() has been submitted; a group-committed write
    // can land in a fork's cut before its caller is woken to ack it, so the
    // sound upper bound for fork contents is `started`, not `acked`.
    let acked = counters();
    let started = counters();

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let db = db.clone();
        let stop = stop.clone();
        let acked = acked.clone();
        let started = started.clone();
        handles.push(std::thread::spawn(move || {
            let mut round = 1u32;
            while !stop.load(Ordering::Relaxed) {
                for i in 0..KEYS_PER_WRITER {
                    // every third key large enough to live in the vlog
                    let val = if i % 3 == 0 {
                        format!("{}-r{round}-{}", key(w, i), "x".repeat(200))
                    } else {
                        format!("{}-r{round}", key(w, i))
                    };
                    started[w][i].store(round, Ordering::Release);
                    db.put(key(w, i).into_bytes(), val.into_bytes()).unwrap();
                    acked[w][i].store(round, Ordering::Release);
                }
                round += 1;
            }
        }));
    }

    // keep flush/compaction/vlog GC constantly racing the fork cuts
    {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.flush();
                let _ = db.compact_all();
                while let Ok(Some(_)) = db.gc_vlog() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }));
    }

    let snap = |c: &[Vec<AtomicU32>]| -> Vec<Vec<u32>> {
        c.iter()
            .map(|ws| ws.iter().map(|a| a.load(Ordering::Acquire)).collect())
            .collect()
    };

    let mut forks = Vec::new();
    for f in 0..FORKS {
        // everything acked before the call must be in the fork; nothing
        // submitted after the call returns can be
        let floor = snap(&acked);
        let info = db.fork(&format!("stress-{f}")).unwrap();
        let ceil = snap(&started);
        forks.push((info, floor, ceil));
        std::thread::sleep(std::time::Duration::from_millis(25));
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(db.list_forks().unwrap().len(), FORKS);

    for (info, floor, ceil) in &forks {
        let fdb = Db::open(&info.path, small_opts()).unwrap();
        for w in 0..WRITERS {
            for i in 0..KEYS_PER_WRITER {
                let k = key(w, i);
                let got = fdb.get(k.as_bytes()).unwrap();
                let f = floor[w][i];
                let c = ceil[w][i];
                if f == 0 {
                    continue;
                }
                let val = got.unwrap_or_else(|| {
                    panic!("fork {}: {k} missing (acked round {f} before cut)", info.name)
                });
                let r = round_of(&val);
                assert!(
                    r >= f && r <= c,
                    "fork {}: {k} at round {r}, expected within [{f}, {c}]",
                    info.name
                );
            }
        }
        // full scan: any vlog pointer left dangling by a GC race fails here
        let mut n = 0usize;
        for item in fdb.iter(None, None, false).unwrap() {
            item.unwrap();
            n += 1;
        }
        // the fork holds every key acked before the cut, nothing beyond the
        // keyspace the writers ever touch
        let acked_before_cut = floor.iter().flatten().filter(|f| **f > 0).count();
        assert!(
            n >= acked_before_cut && n <= WRITERS * KEYS_PER_WRITER,
            "fork {}: scan saw {n} keys, expected within [{acked_before_cut}, {}]",
            info.name,
            WRITERS * KEYS_PER_WRITER
        );
        // the fork is writable and isolated from the parent
        fdb.put(b"fork-local".to_vec(), b"1".to_vec()).unwrap();
        drop(fdb);
    }
    assert!(db.get(b"fork-local").unwrap().is_none());

    // fork of a fork: an opened fork is a full database
    let (info0, floor0, _) = &forks[0];
    let fdb = Db::open(&info0.path, small_opts()).unwrap();
    let sub = fdb.fork("sub").unwrap();
    let sdb = Db::open(&sub.path, small_opts()).unwrap();
    for (w, rounds) in floor0.iter().enumerate() {
        for (i, f) in rounds.iter().enumerate() {
            if *f > 0 {
                assert!(sdb.get(key(w, i).as_bytes()).unwrap().is_some());
            }
        }
    }
}
