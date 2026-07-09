//! End-to-end replication tests over real TCP: master server + edge
//! replica in-process. Covers scoped equality, lazy value fetch, streamed
//! syncs, wire-v1 serving off the edge, resync after a master restart
//! (same instance: caches retained), and full re-attach after the master
//! is replaced by a restored fork (provenance mismatch).

use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::{Db, Options, SyncMode};
use fluent_replication::{EdgeReplica, EdgeReplicaConfig, ReplServer, ReplServerConfig};
use fluent_wire::{ServerConfig, WireClient, WireServer};

fn small_opts(name: &str) -> Options {
    Options {
        sync: SyncMode::Never,
        store_name: Some(name.to_string()),
        memtable_size: 4 << 10,
        block_size: 512,
        l0_compaction_trigger: 2,
        tier_width: 2,
        max_levels: 4,
        target_file_size: 4 << 10,
        value_threshold: 64,
        vlog_file_size: 8 << 10,
        ..Options::default()
    }
}

fn k(i: u32) -> Vec<u8> {
    format!("key{i:06}").into_bytes()
}

fn v(i: u32, tag: &str) -> Vec<u8> {
    format!("value-{tag}-{i}").into_bytes()
}

/// A replication server on its own runtime; dropping it kills the server
/// and every connection (the edge sees a disconnect and re-syncs).
struct MasterHarness {
    rt: Option<tokio::runtime::Runtime>,
    pub addr: String,
}

impl MasterHarness {
    fn serve(db: Arc<Db>, addr: Option<&str>) -> MasterHarness {
        let srv = ReplServer::new(
            db,
            ReplServerConfig {
                ping_every: Duration::from_millis(200),
                ..ReplServerConfig::default()
            },
        )
        .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bind = addr.unwrap_or("127.0.0.1:0").to_string();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind(&bind))
            .unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        rt.spawn(srv.serve(listener));
        MasterHarness {
            rt: Some(rt),
            addr,
        }
    }
}

impl Drop for MasterHarness {
    fn drop(&mut self) {
        // shutdown_background: dropping a runtime inside a test thread that
        // might hold blocking tasks must not hang
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

fn wait_until(what: &str, deadline: Duration, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

/// Reopen a just-stopped master. The old server's subscription tasks hold
/// the directory flock for up to a ping interval after shutdown — exactly
/// like a real restart waiting for the old process to exit.
fn open_retrying(dir: &std::path::Path, opts: Options) -> Arc<Db> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        match Db::open(dir, opts.clone()) {
            Ok(db) => return Arc::new(db),
            Err(fluent31::Error::InvalidArgument(m))
                if m.contains("locked") && Instant::now() < end =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("reopen failed: {e}"),
        }
    }
}

fn edge_cfg(addr: &str, dir: &std::path::Path, lo: u32, hi: u32) -> EdgeReplicaConfig {
    let mut cfg = EdgeReplicaConfig::new(addr, dir, k(lo), Some(k(hi)));
    cfg.refresh_every = None; // deterministic tests refresh explicitly via resync
    cfg
}

/// The core loop over real TCP: attach, equality, laziness, streaming,
/// and wire-v1 serving with writes refused.
#[test]
fn edge_replica_over_tcp() {
    let mdir = tempfile::tempdir().unwrap();
    let edir = tempfile::tempdir().unwrap();
    let master = Arc::new(Db::open(mdir.path(), small_opts("tcp-master")).unwrap());
    for i in 0..300u32 {
        master.put(k(i), v(i, "base")).unwrap();
    }
    for i in (100..200u32).step_by(10) {
        master.put(k(i), vec![(i % 251) as u8; 300]).unwrap(); // vlog-resident
    }
    let harness = MasterHarness::serve(master.clone(), None);

    let replica =
        Arc::new(EdgeReplica::start(edge_cfg(&harness.addr, &edir.path().join("c"), 100, 200)).unwrap());
    let store = replica.store();

    // scoped equality (values fetched lazily over TCP as needed)
    for i in 100..200u32 {
        assert_eq!(store.get(&k(i)).unwrap(), master.get(&k(i)).unwrap(), "key {i}");
    }
    assert!(store.stats().value_cache_bytes > 0, "no lazy fetch happened");

    // streamed syncs become visible without any slice refresh
    for i in 100..140u32 {
        master.put(k(i), v(i, "live")).unwrap();
    }
    master.delete(k(150)).unwrap();
    wait_until("streamed writes to land", Duration::from_secs(10), || {
        store.get(&k(139)).unwrap() == Some(v(139, "live")) && store.get(&k(150)).unwrap().is_none()
    });
    for i in 100..200u32 {
        assert_eq!(store.get(&k(i)).unwrap(), master.get(&k(i)).unwrap(), "post-stream {i}");
    }

    // the edge serves standard wire v1: reads work, writes are refused
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = listener.local_addr().unwrap().to_string();
        let wsrv = WireServer::with_backend(replica.clone(), ServerConfig::default());
        tokio::spawn(wsrv.serve(listener));

        let c = WireClient::connect(&waddr).await.unwrap();
        assert_eq!(c.get(&k(139)).await.unwrap().unwrap(), v(139, "live"));
        assert!(c.get(&k(150)).await.unwrap().is_none());
        let (pairs, _after) = c
            .scan(Some(&k(100)), Some(&k(110)), None, false, 100)
            .await
            .unwrap();
        assert_eq!(pairs.len(), 10);
        // out-of-scope get and any write answer INVALID
        assert!(c.get(&k(250)).await.is_err());
        assert!(c.put(&k(120), b"nope").await.is_err());
    });
    rt.shutdown_background();
}

/// A master restart is a disconnect, not an identity change: the edge
/// re-syncs onto the same instance and keeps its caches.
#[test]
fn master_restart_same_instance_resyncs() {
    let mdir = tempfile::tempdir().unwrap();
    let edir = tempfile::tempdir().unwrap();
    let master = Arc::new(Db::open(mdir.path(), small_opts("restart-master")).unwrap());
    for i in 0..200u32 {
        master.put(k(i), v(i, "one")).unwrap();
    }
    let harness = MasterHarness::serve(master.clone(), None);
    let addr = harness.addr.clone();

    let replica =
        Arc::new(EdgeReplica::start(edge_cfg(&addr, &edir.path().join("c"), 0, 200)).unwrap());
    let store = replica.store();
    assert_eq!(store.get(&k(5)).unwrap().unwrap(), v(5, "one"));
    let instance_before = replica.master().instance_id;

    // hard-stop the server AND the store, then reopen the same directory:
    // same store lifetime, same instance id
    drop(harness);
    drop(master);
    let master = open_retrying(mdir.path(), small_opts("restart-master"));
    for i in 0..50u32 {
        master.put(k(i), v(i, "two")).unwrap();
    }
    let _harness2 = MasterHarness::serve(master.clone(), Some(&addr));

    wait_until("edge to resync after restart", Duration::from_secs(15), || {
        replica.store().get(&k(5)).unwrap() == Some(v(5, "two"))
    });
    assert_eq!(replica.master().instance_id, instance_before);
    let store = replica.store();
    for i in 0..200u32 {
        assert_eq!(store.get(&k(i)).unwrap(), master.get(&k(i)).unwrap(), "key {i}");
    }
}

/// Replacing the master with a restored fork re-mints the instance id;
/// the edge must detect the mismatch, wipe, and re-attach — never serve
/// the old instance's cache against the new one.
#[test]
fn replaced_master_forces_full_reattach() {
    let mdir = tempfile::tempdir().unwrap();
    let edir = tempfile::tempdir().unwrap();
    let master = Arc::new(Db::open(mdir.path(), small_opts("prov-master")).unwrap());
    for i in 0..100u32 {
        master.put(k(i), v(i, "orig")).unwrap();
    }
    // fork point: everything after this exists only on the original
    let info = master.fork("cut").unwrap();
    for i in 0..100u32 {
        master.put(k(i), v(i, "post-cut")).unwrap();
    }
    let harness = MasterHarness::serve(master.clone(), None);
    let addr = harness.addr.clone();

    let replica =
        Arc::new(EdgeReplica::start(edge_cfg(&addr, &edir.path().join("c"), 0, 100)).unwrap());
    wait_until("initial sync", Duration::from_secs(10), || {
        replica.store().get(&k(1)).unwrap() == Some(v(1, "post-cut"))
    });
    let instance_before = replica.master().instance_id;

    // replace the master wholesale with a restored copy of the fork
    let fork_dir = mdir.path().join("fork");
    fluent31::restore_to(&info.path, &fork_dir, Some("prov-fork")).unwrap();
    drop(harness);
    drop(master);
    let fork = open_retrying(
        &fork_dir,
        Options {
            store_name: None,
            ..small_opts("x")
        },
    );
    let _harness2 = MasterHarness::serve(fork.clone(), Some(&addr));

    // the edge must converge onto the fork: pre-cut data only, new identity
    wait_until("full re-attach onto the fork", Duration::from_secs(20), || {
        let s = replica.store();
        s.master().instance_id != instance_before
            && s.get(&k(1)).ok() == Some(Some(v(1, "orig")))
    });
    let store = replica.store();
    assert_eq!(store.master().name, "prov-fork");
    for i in 0..100u32 {
        assert_eq!(store.get(&k(i)).unwrap(), fork.get(&k(i)).unwrap(), "key {i}");
    }
}
