//! SyncMode::Periodic and Db::sync_wal: acks at memory speed, background
//! fsyncs on a timer, explicit barrier on demand.

use std::sync::Arc;
use std::time::Duration;

use fluent31::{Db, Options, SyncMode};

fn periodic(ms: u64) -> Options {
    Options {
        sync: SyncMode::Periodic {
            every: Duration::from_millis(ms),
        },
        ..Options::default()
    }
}

/// Periodic writes never pay a per-write fsync; the timer syncs in the
/// background.
#[test]
fn periodic_writes_ack_without_inline_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), periodic(20)).unwrap();
    for i in 0..200u32 {
        db.put(format!("p/{i}"), i.to_le_bytes().to_vec()).unwrap();
    }
    let stats = db.stats();
    assert!(
        stats.wal_syncs < 200,
        "periodic mode must not fsync per write: {} syncs for 200 writes",
        stats.wal_syncs
    );
    // the timer fires: within a generous window at least one sync lands
    std::thread::sleep(Duration::from_millis(200));
    assert!(db.stats().wal_syncs >= 1, "timer never fired");
    assert_eq!(
        db.get(b"p/199").unwrap().as_deref(),
        Some(&199u32.to_le_bytes()[..])
    );
}

/// sync_wal is a barrier: it bumps the sync counter and everything acked
/// before it survives reopen.
#[test]
fn sync_wal_is_an_explicit_barrier() {
    let dir = tempfile::tempdir().unwrap();
    {
        // long interval: the timer effectively never fires during the test,
        // so durability comes from the explicit barrier alone
        let db = Db::open(dir.path(), periodic(60_000)).unwrap();
        db.put("k", "must-survive").unwrap();
        let before = db.stats().wal_syncs;
        db.sync_wal().unwrap();
        assert!(db.stats().wal_syncs > before);
    } // drop also syncs once (clean-close barrier)
    let db = Db::open(dir.path(), periodic(60_000)).unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"must-survive".as_ref()));
}

/// Concurrent periodic writers: correctness identical to the other modes.
#[test]
fn periodic_concurrent_writers_lose_nothing() {
    const THREADS: usize = 8;
    const PER: usize = 200;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), periodic(5)).unwrap());
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = db.clone();
            s.spawn(move || {
                for i in 0..PER {
                    db.put(format!("c/{t}/{i}"), "v").unwrap();
                }
            });
        }
    });
    for t in 0..THREADS {
        for i in 0..PER {
            assert!(
                db.get(format!("c/{t}/{i}").as_bytes()).unwrap().is_some(),
                "lost c/{t}/{i}"
            );
        }
    }
}

/// sync_wal works (and is harmless) in the other modes too.
#[test]
fn sync_wal_available_in_all_modes() {
    for sync in [SyncMode::Always, SyncMode::Never] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), Options { sync, ..Options::default() }).unwrap();
        db.put("x", "y").unwrap();
        db.sync_wal().unwrap();
        assert_eq!(db.get(b"x").unwrap().as_deref(), Some(b"y".as_ref()));
    }
}
