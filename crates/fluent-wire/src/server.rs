//! Connection handling: capped refcounted read buffer with a large-frame
//! bypass, per-connection in-flight gating (backpressure by pausing reads),
//! one writer task per connection with a response byte budget, and
//! out-of-order completion.

use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use fluent31::Db;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};

use crate::backend::WireBackend;
use crate::dispatch;
use crate::proto::*;

/// Soft size of the per-connection read buffer; frames with payloads at or
/// below this ride the refcounted buffer (zero-copy handoff to the request
/// task). Bigger frames bypass it entirely.
const READ_BUF: usize = 256 << 10;
/// If churn grows the buffer past this, it is replaced with a fresh one so
/// steady-state memory stays bounded (the old allocation frees once the
/// last in-flight payload referencing it drops).
const READ_BUF_HARD: usize = 1 << 20;
/// Max concurrently executing requests per connection. When in flight is
/// full the read loop pauses — TCP backpressure carries the signal to the
/// client. No queue exists to grow.
const MAX_IN_FLIGHT: usize = 128;
/// Response-bytes budget per connection, in 64 KiB permits: request tasks
/// holding large response payloads wait here, so a slow reader cannot make
/// the server materialize unbounded response data.
const RESP_BUDGET_UNITS: usize = 1024; // 64 MiB
const RESP_UNIT: usize = 64 << 10;

pub struct ServerConfig {
    /// Hard cap on a frame's payload; larger requests are refused with
    /// ST_TOO_LARGE and the connection is closed (the stream stays framed,
    /// but a client this far out of contract is better torn down).
    pub max_frame: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            // engine max_value_size (256 MiB) + key + payload overhead
            max_frame: (256 << 20) + (1 << 20),
        }
    }
}

/// Engine-call gates shared by every connection: same rationale as the
/// GraphQL server's permits — the engine parks stalled writers on condvar
/// waits, and unbounded spawn_blocking would starve the pool.
pub(crate) struct EngineGate {
    pub read: Arc<Semaphore>,
    pub write: Arc<Semaphore>,
}

pub struct WireServer {
    pub(crate) backend: Arc<dyn WireBackend>,
    pub(crate) gate: EngineGate,
    cfg: ServerConfig,
}

impl WireServer {
    pub fn new(db: Arc<Db>, cfg: ServerConfig) -> Arc<WireServer> {
        Self::with_backend(db, cfg)
    }

    /// Serve the wire protocol over any engine backend — the full `Db` or
    /// a read-only `EdgeStore` (writes answer INVALID there).
    pub fn with_backend(backend: Arc<impl WireBackend>, cfg: ServerConfig) -> Arc<WireServer> {
        Arc::new(WireServer {
            backend,
            gate: EngineGate {
                read: Arc::new(Semaphore::new(128)),
                write: Arc::new(Semaphore::new(32)),
            },
            cfg,
        })
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (sock, _peer) = listener.accept().await?;
            let srv = self.clone();
            tokio::spawn(async move {
                // per-connection errors end that connection only
                let _ = srv.run_conn(sock).await;
            });
        }
    }

    async fn run_conn(self: Arc<Self>, sock: TcpStream) -> std::io::Result<()> {
        sock.set_nodelay(true)?;
        let (mut rd, mut wr) = sock.into_split();

        // ---- writer task: sole owner of the write half -------------------
        // Bounded channel: if the client stops reading, the writer stalls,
        // the channel fills, and request tasks wait to send — backpressure
        // end to end.
        let (tx, mut rx) = mpsc::channel::<(Bytes, usize)>(64);
        let resp_budget = Arc::new(Semaphore::new(RESP_BUDGET_UNITS));
        let budget_for_writer = resp_budget.clone();
        let writer = tokio::spawn(async move {
            while let Some((frame, units)) = rx.recv().await {
                let r = wr.write_all(&frame).await;
                budget_for_writer.add_permits(units);
                if r.is_err() {
                    break;
                }
            }
            let _ = wr.shutdown().await;
        });

        let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let mut buf = BytesMut::with_capacity(READ_BUF);

        // ---- read loop ---------------------------------------------------
        'conn: loop {
            // header first
            while buf.len() < HEADER_LEN {
                if rd.read_buf(&mut buf).await? == 0 {
                    break 'conn; // clean EOF between frames
                }
            }
            let frame_len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            if frame_len < FRAME_OVERHEAD || frame_len > self.cfg.max_frame {
                let id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
                let msg = format!("frame_len {frame_len} out of bounds");
                let _ = tx
                    .send((response(id, ST_TOO_LARGE, msg.as_bytes()), 1))
                    .await;
                break 'conn;
            }
            let request_id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
            let opcode = buf[12];
            let payload_len = frame_len - FRAME_OVERHEAD;

            // ---- payload: buffered (zero-copy) or bypass ----------------
            let payload: Bytes = if payload_len > READ_BUF {
                // large-frame bypass: exact-size allocation, filled straight
                // from the socket — the payload never transits `buf`
                let mut big = Vec::with_capacity(payload_len);
                let have = (buf.len() - HEADER_LEN).min(payload_len);
                big.extend_from_slice(&buf[HEADER_LEN..HEADER_LEN + have]);
                buf.advance(HEADER_LEN + have);
                big.resize(payload_len, 0);
                rd.read_exact(&mut big[have..]).await?;
                Bytes::from(big)
            } else {
                while buf.len() < HEADER_LEN + payload_len {
                    if rd.read_buf(&mut buf).await? == 0 {
                        return Ok(()); // torn mid-frame: peer vanished
                    }
                }
                buf.advance(HEADER_LEN);
                let payload = buf.split_to(payload_len).freeze();
                // churn control: a fresh buffer once fragmentation piles up
                // (freed when the last in-flight payload drops its ref)
                if buf.capacity() > READ_BUF_HARD {
                    let mut fresh = BytesMut::with_capacity(READ_BUF);
                    fresh.extend_from_slice(&buf);
                    buf = fresh;
                }
                payload
            };

            // ---- gate + dispatch ----------------------------------------
            // waiting here (not queueing) is the flow control: reads pause,
            // the kernel buffer fills, the client feels backpressure
            let permit = in_flight.clone().acquire_owned().await.expect("open");
            let srv = self.clone();
            let tx = tx.clone();
            let budget = resp_budget.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let (status, body) = dispatch::handle(&srv, opcode, &payload).await;
                let frame = response(request_id, status, &body);
                // budget units proportional to response size; capped at the
                // whole budget so one giant response serializes rather than
                // deadlocks
                let units = (frame.len() / RESP_UNIT + 1).min(RESP_BUDGET_UNITS);
                let Ok(bp) = budget.acquire_many_owned(units as u32).await else {
                    return;
                };
                bp.forget(); // writer returns these permits after the write
                let _ = tx.send((frame, units)).await;
            });
        }

        drop(tx);
        let _ = writer.await;
        Ok(())
    }
}
