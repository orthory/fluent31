//! Reference client: allocates per-connection request ids, pipelines
//! freely, and pairs out-of-order responses back to their callers.
//!
//! One task owns the read half and completes oneshots out of a pending
//! map; senders write frames directly under a write-half mutex (frames are
//! written atomically via a single `write_all`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};

use crate::proto::*;

#[derive(Debug)]
pub struct WireError {
    pub status: u8,
    pub body: Vec<u8>,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "wire status {:#04x}: {}",
            self.status,
            String::from_utf8_lossy(&self.body)
        )
    }
}

impl std::error::Error for WireError {}

pub type WireResult<T> = std::result::Result<T, WireError>;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<(u8, Vec<u8>)>>>>;

pub struct WireClient {
    wr: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    pending: Pending,
    next_id: AtomicU64,
}

impl WireClient {
    pub async fn connect(addr: &str) -> std::io::Result<Arc<WireClient>> {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        let (mut rd, wr) = sock.into_split();
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

        let map = pending.clone();
        tokio::spawn(async move {
            let mut hdr = [0u8; HEADER_LEN];
            loop {
                if rd.read_exact(&mut hdr).await.is_err() {
                    break;
                }
                let frame_len = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
                let id = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
                let status = hdr[12];
                let mut body = vec![0u8; frame_len - FRAME_OVERHEAD];
                if rd.read_exact(&mut body).await.is_err() {
                    break;
                }
                if let Some(tx) = map.lock().await.remove(&id) {
                    let _ = tx.send((status, body));
                }
            }
            // connection gone: fail everything still pending
            map.lock().await.clear();
        });

        Ok(Arc::new(WireClient {
            wr: Mutex::new(wr),
            pending,
            next_id: AtomicU64::new(1), // 0 is reserved
        }))
    }

    /// Send one request and await its (possibly out-of-order) response.
    pub async fn call(&self, opcode: u8, payload: &[u8]) -> WireResult<Vec<u8>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        {
            let frame = request(id, opcode, payload);
            let mut wr = self.wr.lock().await;
            if wr.write_all(&frame).await.is_err() {
                self.pending.lock().await.remove(&id);
                return Err(WireError {
                    status: ST_IO,
                    body: b"connection write failed".to_vec(),
                });
            }
        }
        match rx.await {
            Ok((ST_OK, body)) => Ok(body),
            Ok((status, body)) => Err(WireError { status, body }),
            Err(_) => Err(WireError {
                status: ST_IO,
                body: b"connection closed with request in flight".to_vec(),
            }),
        }
    }

    // ---- typed convenience wrappers -----------------------------------

    pub async fn get(&self, key: &[u8]) -> WireResult<Option<Vec<u8>>> {
        match self.call(OP_GET, key).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.status == ST_NOT_FOUND => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> WireResult<()> {
        let mut p = BytesMut::with_capacity(4 + key.len() + value.len());
        put_blob(&mut p, key);
        p.put_slice(value);
        self.call(OP_PUT, &p).await.map(|_| ())
    }

    pub async fn del(&self, key: &[u8]) -> WireResult<()> {
        self.call(OP_DEL, key).await.map(|_| ())
    }

    pub async fn exec(&self, module: &str, input: &[u8]) -> WireResult<Vec<u8>> {
        let mut p = BytesMut::new();
        put_blob(&mut p, module.as_bytes());
        p.put_slice(input);
        self.call(OP_EXEC, &p).await
    }

    pub async fn query(&self, module: &str, input: &[u8]) -> WireResult<Vec<u8>> {
        let mut p = BytesMut::new();
        put_blob(&mut p, module.as_bytes());
        p.put_slice(input);
        self.call(OP_QUERY, &p).await
    }

    /// One scan page; returns (pairs, next_after) — feed `next_after` back
    /// in to continue.
    #[allow(clippy::type_complexity)]
    pub async fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        after: Option<&[u8]>,
        reverse: bool,
        limit: u32,
    ) -> WireResult<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let mut p = BytesMut::new();
        p.put_u8(reverse as u8);
        for f in [lo, hi, after] {
            match f {
                Some(b) => {
                    p.put_u8(1);
                    put_blob(&mut p, b);
                }
                None => p.put_u8(0),
            }
        }
        p.put_u32_le(limit);
        let body = self.call(OP_SCAN, &p).await?;
        let mut rd = Rd(&body);
        let bad = |m: String| WireError {
            status: ST_BAD_FRAME,
            body: m.into_bytes(),
        };
        let count = rd.u32().map_err(bad)?;
        let mut pairs = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let k = rd.blob().map_err(bad)?.to_vec();
            let v = rd.blob().map_err(bad)?.to_vec();
            pairs.push((k, v));
        }
        let _has_more = rd.u8().map_err(bad)?;
        let next_after = match rd.u8().map_err(bad)? {
            0 => None,
            _ => Some(rd.blob().map_err(bad)?.to_vec()),
        };
        Ok((pairs, next_after))
    }

    pub async fn sync_wal(&self) -> WireResult<()> {
        self.call(OP_SYNC_WAL, &[]).await.map(|_| ())
    }
}
