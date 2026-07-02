//! Concurrent sync-write benchmark: THREADS writers x PER ops, SyncMode::Always.
use std::sync::{Arc, Barrier};
use std::time::Instant;
use fluent31::{Db, Options, SyncMode};

fn main() {
    let threads: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(8);
    let sync = match std::env::args().nth(2).as_deref() {
        Some("never") => SyncMode::Never,
        _ => SyncMode::Always,
    };
    let per: usize = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(50);
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), Options { sync, ..Options::default() }).unwrap());
    let barrier = Arc::new(Barrier::new(threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for t in 0..threads {
            let db = db.clone(); let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..per { db.put(format!("b/{t}/{i}"), "value-payload-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").unwrap(); }
            });
        }
    });
    let el = start.elapsed();
    let total = threads * per;
    let st = db.stats();
    println!("{threads} threads x {per}: {total} writes in {:?} = {:.0} writes/s | groups={} batches={} wal_syncs={}",
        el, total as f64 / el.as_secs_f64(), st.commit_groups, st.commit_batches, st.wal_syncs);
}
