//! End-to-end server-mode tests over real TCP: one process, one store,
//! both planes. A GraphQL write streams to an edge cache joined with a
//! key-range scope and to a full replica joined unbounded. An unnamed
//! store serves graphql but keeps the replication join point closed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::{journal, Db, Options, SyncMode};
use fluent_replication::{EdgeReplica, EdgeReplicaConfig};
use fluent_server::{Server, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ephemeral_cfg() -> ServerConfig {
    ServerConfig {
        graphql_addr: "127.0.0.1:0".into(),
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

    // write and read back over GraphQL
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"mutation { put(key: {text: \"user/1\"}, value: {text: \"ada\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");
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

    // a committed write streams to both attached nodes
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"mutation { put(key: {text: \"user/2\"}, value: {text: \"grace\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");
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

    // graphql still serves
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"mutation { put(key: {text: \"k\"}, value: {text: \"v\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"{ get(key: {text: \"k\"}) { text } }"}"#,
    )
    .await;
    assert!(resp.contains(r#""text":"v""#), "{resp}");

    server.shutdown().await;
}

/// A plane tunable set through ServerConfig must reach the running
/// plane: with a tiny GraphQL body cap, an oversized request is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plane_tunables_flow_through() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let mut cfg = ephemeral_cfg();
    cfg.max_body_bytes = 64;
    let server = Server::start(db, dir.path(), opts, cfg).await.unwrap();

    let small = graphql_post(server.graphql_addr, r#"{"query":"{ seqno }"}"#).await;
    assert!(small.starts_with("HTTP/1.1 200"), "{small}");
    let big_padding = " ".repeat(128);
    let big = graphql_post(
        server.graphql_addr,
        &format!(r#"{{"query":"{{ seqno }}{big_padding}"}}"#),
    )
    .await;
    assert!(
        !big.starts_with("HTTP/1.1 200"),
        "body above the configured cap must be refused: {big}"
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

/// Rebuild the journal into a fresh directory and look `key` up there.
/// `None` covers "rebuild failed" (journal mid-write), "key not journaled
/// yet", and a transiently locked dest — a concurrently spawned test
/// child can hold the fresh store's flock for the moment between fork and
/// exec — so callers just poll until the value appears.
fn rebuilt_value(jrn: &std::path::Path, key: &[u8]) -> Option<Vec<u8>> {
    let dest = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    journal::rebuild(jrn, dest.path(), opts.clone()).ok()?;
    let db = Db::open(dest.path(), opts).ok()?;
    db.get(key).unwrap()
}

/// `[journal]` in the config file attaches the opt-in mutation journal:
/// the attach-time base captures state that predates the server, a live
/// GraphQL write streams in as a delta, and a SIGTERM shutdown drains
/// cleanly — each proven by rebuilding a fresh store from the journal
/// directory alone.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_attaches_journal_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let db_dir = dir.path().join("db");
    let jrn_dir = dir.path().join("journal");

    // state that predates the journal: only the attach-time base carries it
    {
        let opts = Options {
            sync: SyncMode::Never,
            ..Options::default()
        };
        let db = Db::open(&db_dir, opts).unwrap();
        db.put(b"pre".to_vec(), b"base".to_vec()).unwrap();
    }

    let cfg_path = dir.path().join("server.toml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"
dir = "{}"
sync = "never"

[listen]
graphql = "127.0.0.1:0"
replication = "127.0.0.1:0"

[journal]
dir = "{}"
"#,
            db_dir.display(),
            jrn_dir.display()
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

    // the binary announces the journal and each plane's bound address
    let mut graphql: Option<SocketAddr> = None;
    let mut journal_line = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while graphql.is_none() || journal_line.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = rx.recv_timeout(left) else {
            child.kill().ok();
            panic!("binary did not announce journal + graphql in time");
        };
        if let Some(rest) = line.strip_prefix("fluent-server: graphql") {
            let addr = rest.trim_start().strip_prefix("http://").unwrap();
            graphql = Some(addr[..addr.find("/graphql").unwrap()].parse().unwrap());
        } else if line.starts_with("fluent-server: journal") {
            journal_line = line;
        }
    }
    assert!(
        journal_line.contains(&jrn_dir.display().to_string()),
        "journal dir not sourced from the config file: {journal_line}"
    );

    // the attach-time base snapshot covers the pre-existing key
    wait_for("base snapshot to cover pre-attach state", || {
        rebuilt_value(&jrn_dir, b"pre") == Some(b"base".to_vec())
    })
    .await;

    // a live write flows through the delta stream (fsynced per batch)
    let resp = graphql_post(
        graphql.unwrap(),
        r#"{"query":"mutation { put(key: {text: \"live\"}, value: {text: \"delta\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");
    wait_for("delta to reach the journal", || {
        rebuilt_value(&jrn_dir, b"live") == Some(b"delta".to_vec())
    })
    .await;

    // graceful shutdown: the journal drains and flushes before the Db drops
    let killed = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap()
        .success();
    assert!(killed, "kill -TERM failed");
    let status = child.wait().unwrap();
    assert!(status.success(), "clean shutdown must exit 0, got {status}");
    assert_eq!(rebuilt_value(&jrn_dir, b"pre"), Some(b"base".to_vec()));
    assert_eq!(rebuilt_value(&jrn_dir, b"live"), Some(b"delta".to_vec()));
}

/// Echo module (query + execute) for the wasm-disabled test.
const ECHO_WAT: &str = r#"
(module
  (import "fluent" "input_len" (func $input_len (result i32)))
  (import "fluent" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "query") (export "execute") (result i32)
    (local $n i32)
    (local.set $n (call $input_len))
    (drop (call $input_read (i32.const 0) (local.get $n) (i32.const 0)))
    (drop (call $output_write (i32.const 0) (local.get $n)))
    (i32.const 0)))
"#;

/// A server with `Options::wasm_enabled = false` boots fine on a store
/// that has modules installed: the KV planes are unaffected, the module
/// stays listed (inert), and wasm invocations answer with an error
/// instead of running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wasm_disabled_server_serves_inert_layer() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.install_module("echo", ECHO_WAT.as_bytes()).unwrap();
    }
    let opts = Options {
        wasm_enabled: false,
        ..opts
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let server = Server::start(db, dir.path(), opts, ephemeral_cfg())
        .await
        .unwrap();

    // the KV surface is unaffected
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"mutation { put(key: {text: \"k\"}, value: {text: \"v\"}) }"}"#,
    )
    .await;
    assert!(resp.contains(r#""put":true"#), "{resp}");

    // the installed module stays visible (inert)...
    let resp = graphql_post(server.graphql_addr, r#"{"query":"{ modules { name } }"}"#).await;
    assert!(resp.contains(r#""name":"echo""#), "{resp}");

    // ...but invoking it answers with the disabled error
    let resp = graphql_post(
        server.graphql_addr,
        r#"{"query":"{ wasm(module: \"echo\", input: {text: \"x\"}) { hex } }"}"#,
    )
    .await;
    assert!(resp.contains("disabled"), "{resp}");

    server.shutdown().await;
}
