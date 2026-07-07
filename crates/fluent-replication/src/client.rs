//! Edge-side replication client and replica driver.
//!
//! [`ReplClient`] is the blocking request/response half (HELLO, SNAPSHOT,
//! FETCH_*): it binds to one master instance id at first contact and
//! re-verifies it on every reconnect — a changed id surfaces as
//! `Error::ProvenanceMismatch`, never as silently different data.
//!
//! [`EdgeReplica`] is the driver: it attaches an [`EdgeStore`], subscribes
//! BEFORE pulling the slice (so the pair is gap-free), applies streamed
//! batches from a background thread, refreshes the slice periodically to
//! prune the overlay, and re-syncs on `Lagged`/`Gone`/disconnect. Same
//! instance ⇒ local caches stay valid across re-syncs; a re-minted master
//! ⇒ full re-attach (wiped cache) behind an atomically swapped store.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use fluent31::edge::{EdgeConfig, EdgeStore, ValueFetcher};
use fluent31::{Error, InstanceId, Result, SliceManifest, StoreIdentity};

use crate::proto::*;

const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Fragment bytes fetched per FETCH_TABLE request.
const TABLE_CHUNK: u32 = 1 << 20;

fn io_err(msg: impl Into<String>) -> Error {
    Error::Io(std::io::Error::other(msg.into()))
}

// ---------------------------------------------------------------------------
// Blocking request/response client
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub name: String,
    pub instance_id: InstanceId,
    pub visible_seqno: u64,
}

pub struct ReplClient {
    addr: String,
    conn: Mutex<Option<TcpStream>>,
    /// Instance this client is bound to; verified on every (re)connect.
    bound: Mutex<Option<InstanceId>>,
    next_id: AtomicU64,
}

impl ReplClient {
    /// Connect and bind to whatever instance the master reports.
    pub fn connect(addr: &str) -> Result<(Arc<ReplClient>, MasterInfo)> {
        let client = Arc::new(ReplClient {
            addr: addr.to_string(),
            conn: Mutex::new(None),
            bound: Mutex::new(None),
            next_id: AtomicU64::new(1),
        });
        let info = client.ensure_connected()?;
        Ok((client, info))
    }

    /// Dial + HELLO + instance check, reusing the cached socket when live.
    fn ensure_connected(&self) -> Result<MasterInfo> {
        let mut guard = self.conn.lock().expect("poisoned");
        if guard.is_none() {
            let sock = TcpStream::connect(&self.addr)?;
            sock.set_nodelay(true)?;
            sock.set_read_timeout(Some(IO_TIMEOUT))?;
            sock.set_write_timeout(Some(IO_TIMEOUT))?;
            *guard = Some(sock);
        }
        let sock = guard.as_mut().expect("just set");
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let hello = match roundtrip(sock, id, OP_HELLO, &[]) {
            Ok((ST_OK, body)) => decode_hello(&body).map_err(io_err)?,
            Ok((ST_NO_IDENTITY, m)) => {
                return Err(Error::InvalidArgument(String::from_utf8_lossy(&m).into()))
            }
            Ok((st, m)) => {
                return Err(io_err(format!(
                    "hello failed (status {st}): {}",
                    String::from_utf8_lossy(&m)
                )))
            }
            Err(e) => {
                *guard = None;
                return Err(e);
            }
        };
        let mut bound = self.bound.lock().expect("poisoned");
        match *bound {
            None => *bound = Some(hello.instance_id),
            Some(expect) if expect != hello.instance_id => {
                *guard = None;
                return Err(Error::ProvenanceMismatch(format!(
                    "master {:?} is instance {}, expected {}",
                    hello.name,
                    fluent31::identity::hex(&hello.instance_id),
                    fluent31::identity::hex(&expect),
                )));
            }
            Some(_) => {}
        }
        Ok(MasterInfo {
            name: hello.name,
            instance_id: hello.instance_id,
            visible_seqno: hello.visible_seqno,
        })
    }

    /// One request with a single transparent reconnect on IO failure.
    fn request(&self, opcode: u8, payload: &[u8]) -> Result<(u8, Vec<u8>)> {
        for attempt in 0..2 {
            self.ensure_connected()?;
            let mut guard = self.conn.lock().expect("poisoned");
            let Some(sock) = guard.as_mut() else { continue };
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            match roundtrip(sock, id, opcode, payload) {
                Ok(r) => return Ok(r),
                Err(e) => {
                    *guard = None; // dead socket; retry reconnects + re-verifies
                    if attempt == 1 {
                        return Err(e);
                    }
                }
            }
        }
        Err(io_err("request retries exhausted"))
    }

    fn expect_ok(&self, opcode: u8, payload: &[u8]) -> Result<Vec<u8>> {
        match self.request(opcode, payload)? {
            (ST_OK, body) => Ok(body),
            (ST_GONE, m) => Err(Error::Gone(String::from_utf8_lossy(&m).into())),
            (st, m) => Err(io_err(format!(
                "status {st}: {}",
                String::from_utf8_lossy(&m)
            ))),
        }
    }

    pub fn snapshot(&self, lo: &[u8], hi: Option<&[u8]>) -> Result<SliceManifest> {
        let body = self.expect_ok(OP_SNAPSHOT, &encode_range(lo, hi))?;
        decode_slice(&body).map_err(io_err)
    }

    pub fn fetch_table_chunk(&self, id: u64, off: u64, len: u32) -> Result<Vec<u8>> {
        let mut p = Vec::with_capacity(20);
        p.extend_from_slice(&id.to_le_bytes());
        p.extend_from_slice(&off.to_le_bytes());
        p.extend_from_slice(&len.to_le_bytes());
        self.expect_ok(OP_FETCH_TABLE, &p)
    }

    pub fn fetch_value(&self, file: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut p = Vec::with_capacity(20);
        p.extend_from_slice(&file.to_le_bytes());
        p.extend_from_slice(&offset.to_le_bytes());
        p.extend_from_slice(&len.to_le_bytes());
        self.expect_ok(OP_FETCH_VALUE, &p)
    }
}

/// The edge store's reach-back path for cold values.
impl ValueFetcher for ReplClient {
    fn fetch_record(&self, file: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.fetch_value(file, offset, len)
    }
}

fn encode_range(lo: &[u8], hi: Option<&[u8]>) -> Vec<u8> {
    let mut out = bytes::BytesMut::new();
    out.extend_from_slice(&[1]);
    put_blob(&mut out, lo);
    match hi {
        Some(h) => {
            out.extend_from_slice(&[1]);
            put_blob(&mut out, h);
        }
        None => out.extend_from_slice(&[0]),
    }
    out.to_vec()
}

fn write_request(sock: &mut TcpStream, id: u64, opcode: u8, payload: &[u8]) -> Result<()> {
    sock.write_all(&request(id, opcode, payload))?;
    Ok(())
}

fn read_frame_blocking(sock: &mut TcpStream) -> Result<(u64, u8, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len < FRAME_OVERHEAD {
        return Err(io_err(format!("bad frame_len {frame_len}")));
    }
    let mut rest = vec![0u8; frame_len];
    sock.read_exact(&mut rest)?;
    let id = u64::from_le_bytes(rest[..8].try_into().unwrap());
    let op = rest[8];
    rest.drain(..9);
    Ok((id, op, rest))
}

fn roundtrip(sock: &mut TcpStream, id: u64, opcode: u8, payload: &[u8]) -> Result<(u8, Vec<u8>)> {
    write_request(sock, id, opcode, payload)?;
    let (echo, status, body) = read_frame_blocking(sock)?;
    if echo != id {
        return Err(io_err(format!("response id {echo} != request id {id}")));
    }
    Ok((status, body))
}

// ---------------------------------------------------------------------------
// Stream connection (push-only after SUBSCRIBE)
// ---------------------------------------------------------------------------

enum PushFrame {
    Batch(Vec<fluent31::StreamEntry>),
    Ping,
    Lagged,
}

struct StreamConn {
    sock: TcpStream,
}

impl StreamConn {
    /// Dial, verify the instance, subscribe. Returns the subscription
    /// start seqno (stream covers everything strictly above it).
    fn open(
        addr: &str,
        expect: &InstanceId,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<(u64, StreamConn)> {
        let mut sock = TcpStream::connect(addr)?;
        sock.set_nodelay(true)?;
        sock.set_write_timeout(Some(IO_TIMEOUT))?;
        sock.set_read_timeout(Some(IO_TIMEOUT))?;
        let (st, body) = roundtrip(&mut sock, 1, OP_HELLO, &[])?;
        if st != ST_OK {
            return Err(io_err(format!("stream hello failed (status {st})")));
        }
        let hello = decode_hello(&body).map_err(io_err)?;
        if hello.instance_id != *expect {
            return Err(Error::ProvenanceMismatch(format!(
                "stream master is instance {}, expected {}",
                fluent31::identity::hex(&hello.instance_id),
                fluent31::identity::hex(expect),
            )));
        }
        let (st, body) = roundtrip(&mut sock, 2, OP_SUBSCRIBE, &encode_range(lo, hi))?;
        if st != ST_OK || body.len() != 8 {
            return Err(io_err(format!(
                "subscribe failed (status {st}): {}",
                String::from_utf8_lossy(&body)
            )));
        }
        let start = u64::from_le_bytes(body.try_into().expect("8 bytes"));
        Ok((start, StreamConn { sock }))
    }

    /// Blocking read of the next pushed frame. The server pings on idle,
    /// so a read timeout here means the master is unreachable.
    fn next(&mut self) -> Result<PushFrame> {
        let (id, op, payload) = read_frame_blocking(&mut self.sock)?;
        if id != 0 {
            return Err(io_err(format!("push frame with request id {id}")));
        }
        match op {
            PUSH_STREAM => Ok(PushFrame::Batch(
                decode_stream_batch(&payload).map_err(io_err)?,
            )),
            PUSH_PING => Ok(PushFrame::Ping),
            PUSH_LAGGED => Ok(PushFrame::Lagged),
            other => Err(io_err(format!("unknown push opcode {other:#04x}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Replica driver
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EdgeReplicaConfig {
    pub master_addr: String,
    /// Local cache directory (wiped on attach / re-attach).
    pub dir: PathBuf,
    pub scope_lo: Vec<u8>,
    pub scope_hi: Option<Vec<u8>>,
    /// Periodic slice refresh (prunes the stream overlay); `None` refreshes
    /// only on re-sync.
    pub refresh_every: Option<Duration>,
    pub value_cache_bytes: u64,
    pub block_cache_size: usize,
}

impl EdgeReplicaConfig {
    pub fn new(master_addr: impl Into<String>, dir: impl Into<PathBuf>, lo: Vec<u8>, hi: Option<Vec<u8>>) -> Self {
        EdgeReplicaConfig {
            master_addr: master_addr.into(),
            dir: dir.into(),
            scope_lo: lo,
            scope_hi: hi,
            refresh_every: Some(Duration::from_secs(300)),
            value_cache_bytes: 256 << 20,
            block_cache_size: 32 << 20,
        }
    }
}

/// Client and store always swap together: the client is bound to the same
/// master instance the store caches, so a re-attach replaces both.
#[derive(Clone)]
struct ReplicaState {
    client: Arc<ReplClient>,
    store: Arc<EdgeStore>,
}

struct ReplicaInner {
    cfg: EdgeReplicaConfig,
    state: RwLock<ReplicaState>,
    shutdown: AtomicBool,
}

impl ReplicaInner {
    fn state(&self) -> ReplicaState {
        self.state.read().expect("poisoned").clone()
    }
}

/// A running edge replica: an [`EdgeStore`] kept in sync by a background
/// thread. Serve reads through it directly (it implements
/// `fluent_wire::WireBackend`) or via [`EdgeReplica::store`].
pub struct EdgeReplica {
    inner: Arc<ReplicaInner>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl EdgeReplica {
    /// Attach, do the initial gap-free sync (subscribe → slice pull), and
    /// start the streaming thread. Returns only once the replica serves a
    /// complete scoped view.
    pub fn start(cfg: EdgeReplicaConfig) -> Result<EdgeReplica> {
        let (client, info) = ReplClient::connect(&cfg.master_addr)?;
        let store = attach_store(&cfg, &info, client.clone())?;

        // subscribe FIRST: the slice pulled afterwards is guaranteed to
        // cover everything at or below the subscription start
        let (_start, stream) = StreamConn::open(
            &cfg.master_addr,
            &info.instance_id,
            &cfg.scope_lo,
            cfg.scope_hi.as_deref(),
        )?;
        pull_slice_with_retries(&client, &store, &cfg, || false)?;

        let inner = Arc::new(ReplicaInner {
            cfg,
            state: RwLock::new(ReplicaState { client, store }),
            shutdown: AtomicBool::new(false),
        });
        let run_inner = inner.clone();
        let thread = std::thread::Builder::new()
            .name("fluent-repl-edge".into())
            .spawn(move || run_loop(run_inner, stream))
            .expect("spawn replica thread");
        Ok(EdgeReplica {
            inner,
            thread: Some(thread),
        })
    }

    pub fn store(&self) -> Arc<EdgeStore> {
        self.inner.state().store
    }

    pub fn master(&self) -> StoreIdentity {
        self.store().master().clone()
    }
}

impl Drop for EdgeReplica {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Read delegation so the standard wire server can front a replica; the
/// inner store is re-read per call, so a re-attach swap is invisible to
/// connected readers.
impl fluent_wire::WireBackend for EdgeReplica {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store().get(key)
    }

    fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
        self.store().scan(lo, hi, reverse, limit)
    }

    fn stats_text(&self) -> Result<String> {
        Ok(format!("{:#?}", self.store().stats()))
    }
}

fn attach_store(
    cfg: &EdgeReplicaConfig,
    info: &MasterInfo,
    fetcher: Arc<ReplClient>,
) -> Result<Arc<EdgeStore>> {
    // lineage is not carried over the wire; the instance id alone is the
    // provenance anchor
    let master = StoreIdentity {
        name: info.name.clone(),
        instance_id: info.instance_id,
        parent: None,
    };
    let mut ecfg = EdgeConfig::new(
        &cfg.dir,
        master,
        cfg.scope_lo.clone(),
        cfg.scope_hi.clone(),
    );
    ecfg.value_cache_bytes = cfg.value_cache_bytes;
    ecfg.block_cache_size = cfg.block_cache_size;
    Ok(Arc::new(EdgeStore::attach(ecfg, fetcher)?))
}

/// Snapshot + fetch missing fragments + install. `Gone` mid-fetch means
/// the master compacted underneath us — the caller retries with a fresh
/// snapshot.
fn pull_slice(client: &ReplClient, store: &EdgeStore, cfg: &EdgeReplicaConfig) -> Result<()> {
    let slice = client.snapshot(&cfg.scope_lo, cfg.scope_hi.as_deref())?;
    for run in slice.levels.iter().flatten() {
        for t in &run.tables {
            if store.has_fragment(t.id) {
                continue;
            }
            let mut bytes = Vec::with_capacity(t.size as usize);
            while (bytes.len() as u64) < t.size {
                let chunk = client.fetch_table_chunk(t.id, bytes.len() as u64, TABLE_CHUNK)?;
                if chunk.is_empty() {
                    return Err(io_err(format!("empty chunk for fragment {}", t.id)));
                }
                bytes.extend(chunk);
            }
            // write then rename would be overkill: install_slice re-verifies
            // size + bounds + block CRCs before the fragment is referenced
            std::fs::write(store.fragment_path(t.id), &bytes)?;
        }
    }
    store.install_slice(&slice)
}

fn pull_slice_retrying(inner: &ReplicaInner, state: &ReplicaState) -> Result<()> {
    pull_slice_with_retries(&state.client, &state.store, &inner.cfg, || {
        inner.shutdown.load(Ordering::Acquire)
    })
}

/// `Gone` means the master compacted underneath the pull: retry promptly
/// with a fresh snapshot (compaction goes quiet, so this converges). Other
/// errors back off and count against the attempt budget.
fn pull_slice_with_retries(
    client: &ReplClient,
    store: &EdgeStore,
    cfg: &EdgeReplicaConfig,
    stop: impl Fn() -> bool,
) -> Result<()> {
    let mut last = None;
    for _ in 0..20 {
        if stop() {
            return Ok(());
        }
        match pull_slice(client, store, cfg) {
            Ok(()) => return Ok(()),
            Err(Error::Gone(_)) => continue,
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    Err(last.unwrap_or_else(|| io_err("slice pull kept racing compaction")))
}

/// The replica's life after the initial sync: apply pushed batches,
/// refresh periodically, and re-sync (or fully re-attach) when the stream
/// breaks.
fn run_loop(inner: Arc<ReplicaInner>, mut stream: StreamConn) {
    let mut next_refresh = inner.cfg.refresh_every.map(|d| Instant::now() + d);
    loop {
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        let event = stream.next();
        match event {
            Ok(PushFrame::Batch(entries)) => {
                if inner.state().store.apply_stream(&entries).is_err() {
                    // a malformed batch is a broken stream: re-sync
                    if !resync(&inner, &mut stream) {
                        return;
                    }
                }
            }
            Ok(PushFrame::Ping) => {}
            Ok(PushFrame::Lagged) | Err(_) => {
                if !resync(&inner, &mut stream) {
                    return;
                }
                next_refresh = inner.cfg.refresh_every.map(|d| Instant::now() + d);
            }
        }
        if next_refresh.is_some_and(|t| Instant::now() >= t) {
            // refresh failures are non-fatal: the stream keeps the replica
            // fresh, the overlay just stays bigger until the next attempt
            let _ = pull_slice_retrying(&inner, &inner.state());
            next_refresh = inner.cfg.refresh_every.map(|d| Instant::now() + d);
        }
    }
}

/// Re-establish the (stream, slice) pair. Same master instance keeps every
/// local cache; a re-minted instance forces a full re-attach (fresh client
/// + wiped store, swapped in together). Returns false only on shutdown.
fn resync(inner: &ReplicaInner, stream: &mut StreamConn) -> bool {
    loop {
        if inner.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let attempt = || -> Result<StreamConn> {
            let state = inner.state();
            let expect = state.store.master().instance_id;
            match StreamConn::open(
                &inner.cfg.master_addr,
                &expect,
                &inner.cfg.scope_lo,
                inner.cfg.scope_hi.as_deref(),
            ) {
                Ok((_start, s)) => {
                    pull_slice_retrying(inner, &state)?;
                    Ok(s)
                }
                Err(Error::ProvenanceMismatch(_)) => {
                    // the master was restored/forked: everything cached is
                    // dead. Re-attach from scratch and swap client + store.
                    let (client, info) = ReplClient::connect(&inner.cfg.master_addr)?;
                    let store = attach_store(&inner.cfg, &info, client.clone())?;
                    let (_start, s) = StreamConn::open(
                        &inner.cfg.master_addr,
                        &info.instance_id,
                        &inner.cfg.scope_lo,
                        inner.cfg.scope_hi.as_deref(),
                    )?;
                    let fresh = ReplicaState { client, store };
                    pull_slice_retrying(inner, &fresh)?;
                    *inner.state.write().expect("poisoned") = fresh;
                    Ok(s)
                }
                Err(e) => Err(e),
            }
        };
        match attempt() {
            Ok(s) => {
                *stream = s;
                return true;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(500)),
        }
    }
}
