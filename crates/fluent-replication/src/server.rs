//! Master-side replication server.
//!
//! Request/response connections serve HELLO / SNAPSHOT / FETCH_* ; a
//! SUBSCRIBE flips its connection into push-only mode: a blocking task
//! consumes the engine `Subscription` and forwards batches, pinging on
//! idle so the edge can detect a dead master. Backpressure is end to end:
//! a slow edge stalls its own bounded channel, the blocking forwarder
//! stalls on it, the subscription queue fills on the master, and the
//! engine cuts the subscriber loose (`Lagged`) rather than stalling any
//! writer.

use std::sync::Arc;
use std::time::Duration;

use fluent31::{Db, StoreIdentity, StreamEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};

use crate::proto::*;

pub struct ReplServerConfig {
    /// Hard cap on a request frame's payload.
    pub max_frame: usize,
    /// Idle interval after which a subscribed connection gets a PING.
    pub ping_every: Duration,
}

impl Default for ReplServerConfig {
    fn default() -> Self {
        ReplServerConfig {
            // a FETCH_VALUE response can carry one whole record
            // (max_value_size 256 MiB + key + header); requests stay tiny
            max_frame: 1 << 20,
            ping_every: Duration::from_secs(2),
        }
    }
}

pub struct ReplServer {
    db: Arc<Db>,
    identity: StoreIdentity,
    /// Bounds concurrent blocking engine calls across all connections.
    gate: Arc<Semaphore>,
    cfg: ReplServerConfig,
}

impl ReplServer {
    /// Fails on an unnamed store: replication requires the deterministic
    /// store identity (`Options::store_name`).
    pub fn new(db: Arc<Db>, cfg: ReplServerConfig) -> fluent31::Result<Arc<ReplServer>> {
        let identity = db.identity().ok_or_else(|| {
            fluent31::Error::InvalidArgument(
                "replication requires a named store (set Options::store_name)".into(),
            )
        })?;
        Ok(Arc::new(ReplServer {
            db,
            identity,
            gate: Arc::new(Semaphore::new(64)),
            cfg,
        }))
    }

    pub fn identity(&self) -> &StoreIdentity {
        &self.identity
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (sock, _peer) = listener.accept().await?;
            let srv = self.clone();
            tokio::spawn(async move {
                let _ = srv.run_conn(sock).await;
            });
        }
    }

    async fn run_conn(self: Arc<Self>, mut sock: TcpStream) -> std::io::Result<()> {
        sock.set_nodelay(true)?;
        loop {
            let Some((request_id, opcode, payload)) = read_frame(&mut sock, self.cfg.max_frame).await?
            else {
                return Ok(()); // clean EOF between frames
            };

            if opcode == OP_SUBSCRIBE {
                // reply then flip to push-only mode; this call never returns
                // to request handling
                return self.run_subscription(sock, request_id, &payload).await;
            }

            let (status, body) = self.handle(opcode, &payload).await;
            sock.write_all(&response(request_id, status, &body)).await?;
        }
    }

    async fn handle(&self, opcode: u8, payload: &[u8]) -> (u8, Vec<u8>) {
        match self.dispatch(opcode, payload).await {
            Ok(ok) => (ST_OK, ok),
            Err(DispatchErr::Bad(m)) => (ST_BAD_FRAME, m.into_bytes()),
            Err(DispatchErr::Engine(e)) => (status_for(&e), e.to_string().into_bytes()),
        }
    }

    async fn dispatch(&self, opcode: u8, payload: &[u8]) -> Result<Vec<u8>, DispatchErr> {
        let db = self.db.clone();
        match opcode {
            OP_HELLO => Ok(encode_hello(
                &self.identity.name,
                &self.identity.instance_id,
                self.db.stats().visible_seqno,
            )),

            OP_SNAPSHOT => {
                let (lo, hi) = range_args(payload)?;
                let slice = self
                    .engine_call(move || db.slice_manifest(&lo, hi.as_deref()))
                    .await?;
                Ok(encode_slice(&slice))
            }

            OP_FETCH_TABLE => {
                let mut rd = Rd(payload);
                let (id, off, len) = (rd.u64()?, rd.u64()?, rd.u32()?);
                rd.done()?;
                Ok(self
                    .engine_call(move || db.read_table_chunk(id, off, len as usize))
                    .await?)
            }

            OP_FETCH_VALUE => {
                let mut rd = Rd(payload);
                let (file, off, len) = (rd.u64()?, rd.u64()?, rd.u32()?);
                rd.done()?;
                Ok(self
                    .engine_call(move || db.read_vlog_chunk(file, off, len as usize))
                    .await?)
            }

            other => Err(format!("unknown opcode {other:#04x}").into()),
        }
    }

    async fn engine_call<T, F>(&self, f: F) -> Result<T, DispatchErr>
    where
        F: FnOnce() -> fluent31::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let _p = self.gate.clone().acquire_owned().await.expect("open");
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| DispatchErr::Bad(format!("engine worker failed: {e}")))?
            .map_err(DispatchErr::Engine)
    }

    /// Push-only mode: consume the engine subscription on a blocking task,
    /// forward batches/pings/lagged over a bounded channel, and write them
    /// out until either side goes away.
    async fn run_subscription(
        self: Arc<Self>,
        mut sock: TcpStream,
        request_id: u64,
        payload: &[u8],
    ) -> std::io::Result<()> {
        let (lo, hi) = match range_args(payload) {
            Ok(r) => r,
            Err(DispatchErr::Bad(m)) | Err(DispatchErr::Engine(fluent31::Error::InvalidArgument(m))) => {
                sock.write_all(&response(request_id, ST_BAD_FRAME, m.as_bytes()))
                    .await?;
                return Ok(());
            }
            Err(DispatchErr::Engine(e)) => {
                sock.write_all(&response(request_id, status_for(&e), e.to_string().as_bytes()))
                    .await?;
                return Ok(());
            }
        };
        let db = self.db.clone();
        let sub = match self
            .engine_call(move || db.subscribe(&lo, hi.as_deref()))
            .await
        {
            Ok(s) => s,
            Err(DispatchErr::Bad(m)) => {
                sock.write_all(&response(request_id, ST_BAD_FRAME, m.as_bytes()))
                    .await?;
                return Ok(());
            }
            Err(DispatchErr::Engine(e)) => {
                sock.write_all(&response(request_id, status_for(&e), e.to_string().as_bytes()))
                    .await?;
                return Ok(());
            }
        };
        sock.write_all(&response(
            request_id,
            ST_OK,
            &sub.start_seqno().to_le_bytes(),
        ))
        .await?;

        // bounded: a slow edge fills this, the forwarder stalls, the
        // engine-side queue overflows, and the subscription reports Lagged
        let (tx, mut rx) = mpsc::channel::<bytes::Bytes>(8);
        let ping_every = self.cfg.ping_every;
        let forwarder = tokio::task::spawn_blocking(move || {
            let mut sub = sub;
            loop {
                let frame = match sub.recv_timeout(ping_every) {
                    Ok(Some(StreamEvent::Batch(b))) => {
                        response(0, PUSH_STREAM, &encode_stream_batch(&b))
                    }
                    Ok(Some(StreamEvent::Lagged)) => {
                        let _ = tx.blocking_send(response(0, PUSH_LAGGED, &[]));
                        return;
                    }
                    Ok(None) => response(0, PUSH_PING, &[]),
                    Err(_) => return, // store degraded/closed: drop the conn
                };
                if tx.blocking_send(frame).is_err() {
                    return; // connection gone; dropping `sub` releases its pin
                }
            }
        });

        while let Some(frame) = rx.recv().await {
            if sock.write_all(&frame).await.is_err() {
                break;
            }
        }
        drop(rx); // unblocks the forwarder, which drops the subscription
        let _ = forwarder.await;
        Ok(())
    }
}

enum DispatchErr {
    Bad(String),
    Engine(fluent31::Error),
}

impl From<String> for DispatchErr {
    fn from(s: String) -> Self {
        DispatchErr::Bad(s)
    }
}

impl From<fluent31::Error> for DispatchErr {
    fn from(e: fluent31::Error) -> Self {
        DispatchErr::Engine(e)
    }
}

fn range_args(payload: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>), DispatchErr> {
    let mut rd = Rd(payload);
    let lo = rd.opt_blob()?.map(<[u8]>::to_vec).unwrap_or_default();
    let hi = rd.opt_blob()?.map(<[u8]>::to_vec);
    rd.done()?;
    Ok((lo, hi))
}

/// Read one `[len][id][op][payload]` frame; `None` on clean EOF at a frame
/// boundary.
async fn read_frame(
    sock: &mut TcpStream,
    max_frame: usize,
) -> std::io::Result<Option<(u64, u8, Vec<u8>)>> {
    let mut len_buf = [0u8; 4];
    match sock.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if !(FRAME_OVERHEAD..=max_frame + FRAME_OVERHEAD).contains(&frame_len) {
        return Err(std::io::Error::other(format!(
            "frame_len {frame_len} out of bounds"
        )));
    }
    let mut rest = vec![0u8; frame_len];
    sock.read_exact(&mut rest).await?;
    let request_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
    let opcode = rest[8];
    rest.drain(..9);
    Ok(Some((request_id, opcode, rest)))
}
