//! End-to-end server-mode tests over real TCP: one process, one store,
//! all three planes. A write over the wire pipe is read back over
//! GraphQL, an edge cache joins with a key-range scope, a full replica
//! joins unbounded, and both see streamed writes. An unnamed store serves
//! graphql + wire but keeps the replication join point closed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::{Db, Options, SyncMode};
use fluent_replication::{EdgeReplica, EdgeReplicaConfig};
use fluent_server::{Server, ServerConfig};
use fluent_wire::WireClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ephemeral_cfg() -> ServerConfig {
    ServerConfig {
        graphql_addr: "127.0.0.1:0".into(),
        wire_addr: "127.0.0.1:0".into(),
        replication_addr: "127.0.0.1:0".into(),
        ..ServerConfig::default()
    }
}

async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Minimal HTTP/1.1 POST — enough to hit the GraphQL plane without
/// pulling an HTTP client into the dev-dependencies.
async fn graphql_post(addr: SocketAddr, body: &str) -> String {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /graphql HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

fn edge_cfg(addr: SocketAddr, dir: &std::path::Path, lo: &[u8], hi: Option<&[u8]>) -> EdgeReplicaConfig {
    EdgeReplicaConfig::new(addr.to_string(), dir, lo.to_vec(), hi.map(<[u8]>::to_vec))
}

async fn attach(cfg: EdgeReplicaConfig) -> Arc<EdgeReplica> {
    let replica = tokio::task::spawn_blocking(move || EdgeReplica::start(cfg))
        .await
        .unwrap()
        .unwrap();
    Arc::new(replica)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_planes_over_one_store() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        store_name: Some("srv-test".to_string()),
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let server = Server::start(db, dir.path(), opts, ephemeral_cfg())
        .await
        .unwrap();
    let repl_addr = server
        .replication_addr
        .expect("named store must open the join point");

    // write over the wire pipe
    let wc = WireClient::connect(&server.wire_addr.to_string()).await.unwrap();
    wc.put(b"user/1", b"ada").await.unwrap();
    assert_eq!(wc.get(b"user/1").await.unwrap().unwrap(), b"ada");

    // read the same key back over GraphQL — both planes serve one store
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"{ get(key: {text: \"user/1\"}) { text } }"}"#,
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    assert!(resp.contains(r#""text":"ada""#), "{resp}");

    // an edge cache joins the replication plane with a key-range scope
    let edir = tempfile::tempdir().unwrap();
    let edge = attach(edge_cfg(repl_addr, &edir.path().join("e"), b"user/", Some(b"user0"))).await;
    assert_eq!(edge.master().name, "srv-test");
    assert_eq!(edge.store().get(b"user/1").unwrap().unwrap(), b"ada");

    // a full replica joins the same point with an unbounded scope
    let rdir = tempfile::tempdir().unwrap();
    let replica = attach(edge_cfg(repl_addr, &rdir.path().join("r"), b"", None)).await;
    assert_eq!(replica.store().get(b"user/1").unwrap().unwrap(), b"ada");

    // a write over the pipe streams to both attached nodes
    wc.put(b"user/2", b"grace").await.unwrap();
    wait_for("edge to stream user/2", || {
        edge.store().get(b"user/2").unwrap() == Some(b"grace".to_vec())
    })
    .await;
    wait_for("replica to stream user/2", || {
        replica.store().get(b"user/2").unwrap() == Some(b"grace".to_vec())
    })
    .await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unnamed_store_keeps_join_point_closed() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let server = Server::start(db, dir.path(), opts, ephemeral_cfg())
        .await
        .unwrap();
    assert!(server.replication_addr.is_none());

    // graphql + wire still serve
    let wc = WireClient::connect(&server.wire_addr.to_string()).await.unwrap();
    wc.put(b"k", b"v").await.unwrap();
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"{ get(key: {text: \"k\"}) { text } }"}"#,
    )
    .await;
    assert!(resp.contains(r#""text":"v""#), "{resp}");

    server.shutdown().await;
}

/// A plane tunable set through ServerConfig must reach the running
/// plane: with a tiny wire max-frame, an oversized request is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plane_tunables_flow_through() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let mut cfg = ephemeral_cfg();
    cfg.wire.max_frame = 64;
    let server = Server::start(db, dir.path(), opts, cfg).await.unwrap();

    let wc = WireClient::connect(&server.wire_addr.to_string()).await.unwrap();
    wc.put(b"k", b"v").await.unwrap();
    assert!(
        wc.put(b"big", &[0u8; 128]).await.is_err(),
        "frame above the configured cap must be refused"
    );

    server.shutdown().await;
}

/// Drive the real binary: every setting — including the db dir — sourced
/// from a TOML file via `--config`, no other arguments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_sources_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("server.toml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"
dir = "{}"
store-name = "cfg-test"
sync = "never"

[listen]
graphql = "127.0.0.1:0"
wire = "127.0.0.1:0"
replication = "127.0.0.1:0"

[graphql]
max-body-bytes = 1048576

[engine]
io-backend = "std"
memtable-size = 4194304
"#,
            dir.path().join("db").display()
        ),
    )
    .unwrap();

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-server"))
        .arg("--config")
        .arg(&cfg_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // the binary announces each plane's bound address on stdout
    let mut graphql: Option<SocketAddr> = None;
    let mut replication_line = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while graphql.is_none() || replication_line.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = rx.recv_timeout(left) else {
            child.kill().ok();
            panic!("binary did not announce its planes in time");
        };
        if let Some(rest) = line.strip_prefix("fluent-server: graphql") {
            let addr = rest.trim_start().strip_prefix("http://").unwrap();
            graphql = Some(addr[..addr.find("/graphql").unwrap()].parse().unwrap());
        } else if line.starts_with("fluent-server: replication") {
            replication_line = line;
        }
    }
    assert!(
        replication_line.contains("\"cfg-test\""),
        "store name not sourced from the config file: {replication_line}"
    );

    let resp = graphql_post(
        graphql.unwrap(),
        r#"{"query":"mutation { put(key: {text: \"cfg\"}, value: {text: \"file\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");

    child.kill().unwrap();
    child.wait().unwrap();
}
