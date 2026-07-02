//! End-to-end wire-protocol tests: a real server on an ephemeral port, the
//! reference client, real TCP.

use std::sync::Arc;
use std::time::Duration;

use fluent31::{Db, Options, SyncMode};
use fluent_wire::proto::*;
use fluent_wire::{ServerConfig, WireClient, WireServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn start_server() -> (Arc<Db>, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let srv = WireServer::new(db.clone(), ServerConfig::default());
    tokio::spawn(srv.serve(listener));
    (db, addr, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roundtrip_all_ops() {
    let (_db, addr, _dir) = start_server().await;
    let c = WireClient::connect(&addr).await.unwrap();

    // hello
    let hello = c.call(OP_HELLO, &[]).await.unwrap();
    assert_eq!(u32::from_le_bytes(hello[..4].try_into().unwrap()), 1);

    // put/get/del
    c.put(b"k1", b"v1").await.unwrap();
    assert_eq!(c.get(b"k1").await.unwrap().as_deref(), Some(b"v1".as_ref()));
    assert_eq!(c.get(b"missing").await.unwrap(), None);
    c.del(b"k1").await.unwrap();
    assert_eq!(c.get(b"k1").await.unwrap(), None);

    // batch: 2 puts + 1 del, atomic
    c.put(b"b3", b"old").await.unwrap();
    let mut p = bytes::BytesMut::new();
    use bytes::BufMut;
    p.put_u32_le(3);
    p.put_u8(0);
    put_blob(&mut p, b"b1");
    put_blob(&mut p, b"x");
    p.put_u8(0);
    put_blob(&mut p, b"b2");
    put_blob(&mut p, b"y");
    p.put_u8(1);
    put_blob(&mut p, b"b3");
    let n = c.call(OP_BATCH, &p).await.unwrap();
    assert_eq!(u32::from_le_bytes(n[..4].try_into().unwrap()), 3);
    assert_eq!(c.get(b"b1").await.unwrap().as_deref(), Some(b"x".as_ref()));
    assert_eq!(c.get(b"b3").await.unwrap(), None);

    // scan with pagination
    for i in 0..7u32 {
        c.put(format!("s/{i}").as_bytes(), b"v").await.unwrap();
    }
    let (page1, next) = c.scan(None, None, None, false, 3).await.unwrap();
    assert_eq!(page1.len(), 3);
    assert_eq!(page1[0].0, b"b1");
    let next = next.expect("more pages");
    let (page2, _) = c.scan(None, None, Some(&next), false, 100).await.unwrap();
    assert_eq!(page2.first().unwrap().0, b"s/1");
    assert_eq!(page2.len(), 6, "s/1..s/6");

    // reverse
    let (rev, _) = c.scan(None, None, None, true, 2).await.unwrap();
    assert_eq!(rev[0].0, b"s/6");

    // sync_wal barrier
    c.sync_wal().await.unwrap();
}

/// Guest that busy-loops ~120M iterations (~840M fuel of the 1e9 budget):
/// slow enough to prove out-of-order completion, cheap enough not to trap.
const SLOW_WAT: &str = r#"
(module
  (import "fluent" "output_write" (func $ow (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "slow-done")
  (func (export "run") (result i32)
    (local $i i32)
    (local.set $i (i32.const 120000000))
    (block $out
      (loop $l
        (br_if $out (i32.eqz (local.get $i)))
        (local.set $i (i32.sub (local.get $i) (i32.const 1)))
        (br $l)))
    (drop (call $ow (i32.const 0) (i32.const 9)))
    (i32.const 0)))
"#;

/// THE property this protocol exists for: a slow EXEC on a connection must
/// not delay GETs pipelined behind it — responses correlate by id, not
/// position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_exec_does_not_block_gets_on_same_connection() {
    let (db, addr, _dir) = start_server().await;
    db.install_module("slow", SLOW_WAT.as_bytes()).unwrap();
    db.put("fast", "answer").unwrap();

    let c = WireClient::connect(&addr).await.unwrap();
    let c2 = c.clone();
    let exec = tokio::spawn(async move { c2.exec("slow", &[]).await });

    // give the exec frame a head start on the wire
    tokio::time::sleep(Duration::from_millis(10)).await;

    let t0 = std::time::Instant::now();
    let got = c.get(b"fast").await.unwrap();
    let get_latency = t0.elapsed();
    assert_eq!(got.as_deref(), Some(b"answer".as_ref()));
    let finished_early = exec.is_finished();
    let out = exec.await.unwrap().unwrap();
    assert!(
        !finished_early,
        "GET must complete while the slow EXEC is still running (get took {get_latency:?})"
    );
    assert_eq!(out, b"slow-done");
}

/// Large values take the read-buffer bypass path and must survive intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_values_roundtrip_via_bypass() {
    let (_db, addr, _dir) = start_server().await;
    let c = WireClient::connect(&addr).await.unwrap();
    let big: Vec<u8> = (0..8 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    c.put(b"big", &big).await.unwrap();
    let got = c.get(b"big").await.unwrap().unwrap();
    assert_eq!(got.len(), big.len());
    assert!(got == big, "large payload corrupted in transit");
}

/// 200 mixed pipelined requests on one connection: every response pairs
/// with its request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heavy_pipelining_pairs_every_response() {
    let (_db, addr, _dir) = start_server().await;
    let c = WireClient::connect(&addr).await.unwrap();
    let mut handles = Vec::new();
    for i in 0..200u32 {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("p/{i}");
            c.put(key.as_bytes(), &i.to_le_bytes()).await.unwrap();
            let got = c.get(key.as_bytes()).await.unwrap().unwrap();
            assert_eq!(got, i.to_le_bytes());
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

/// Unknown opcodes answer ST_BAD_FRAME; an oversized frame length gets
/// ST_TOO_LARGE and the connection is torn down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_violations_are_rejected() {
    let (_db, addr, _dir) = start_server().await;
    let c = WireClient::connect(&addr).await.unwrap();
    let err = c.call(0x7f, &[]).await.unwrap_err();
    assert_eq!(err.status, ST_BAD_FRAME);

    // raw socket: absurd frame_len
    let mut raw = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&u32::MAX.to_le_bytes());
    frame.extend_from_slice(&7u64.to_le_bytes());
    frame.push(OP_GET);
    raw.write_all(&frame).await.unwrap();
    let mut hdr = [0u8; HEADER_LEN];
    raw.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[12], ST_TOO_LARGE);
    assert_eq!(u64::from_le_bytes(hdr[4..12].try_into().unwrap()), 7);
    // then EOF: connection closed
    let mut body = vec![0u8; u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize - FRAME_OVERHEAD];
    raw.read_exact(&mut body).await.unwrap();
    let n = raw.read(&mut [0u8; 16]).await.unwrap();
    assert_eq!(n, 0, "server must close after TOO_LARGE");
}
