//! fluent replication (protocol version 2): frame codec, payload codecs,
//! and opcode space.
//!
//! Frame, both directions: `[u32 frame_len][u64 request_id][u8
//! opcode|status][payload…]`, little-endian, blob = `[u32 len][bytes]`.
//! After `SUBSCRIBE` succeeds, a connection becomes push-only: the server
//! sends `PUSH_*` frames with `request_id = 0` and the client sends
//! nothing further. Full specification: REPLICATION.md.

use bytes::{BufMut, Bytes, BytesMut};
use fluent31::{SliceManifest, SliceRun, SliceTable, StreamEntry, ValueKind};

pub const REPL_VERSION: u32 = 2;

// ------------------------------------------------------------- frame codec

/// Bytes of [request_id][opcode] after the length field.
pub const FRAME_OVERHEAD: usize = 8 + 1;
/// Full header: length field + overhead.
pub const HEADER_LEN: usize = 4 + FRAME_OVERHEAD;

/// Minimal, allocation-free payload reader.
pub struct Rd<'a>(pub &'a [u8]);

impl<'a> Rd<'a> {
    pub fn u8(&mut self) -> Result<u8, String> {
        let (&b, rest) = self.0.split_first().ok_or("truncated payload")?;
        self.0 = rest;
        Ok(b)
    }
    pub fn u32(&mut self) -> Result<u32, String> {
        if self.0.len() < 4 {
            return Err("truncated payload".into());
        }
        let (a, rest) = self.0.split_at(4);
        self.0 = rest;
        Ok(u32::from_le_bytes(a.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, String> {
        if self.0.len() < 8 {
            return Err("truncated payload".into());
        }
        let (a, rest) = self.0.split_at(8);
        self.0 = rest;
        Ok(u64::from_le_bytes(a.try_into().unwrap()))
    }
    /// A `[u32 len][bytes]` field.
    pub fn blob(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        if self.0.len() < n {
            return Err("truncated payload".into());
        }
        let (a, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(a)
    }
    /// An OPTIONAL `[u8 present][u32 len][bytes]` field.
    pub fn opt_blob(&mut self) -> Result<Option<&'a [u8]>, String> {
        Ok(match self.u8()? {
            0 => None,
            _ => Some(self.blob()?),
        })
    }
    pub fn rest(self) -> &'a [u8] {
        self.0
    }
    pub fn done(&self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err("trailing bytes in payload".into())
        }
    }
}

pub fn put_blob(out: &mut BytesMut, b: &[u8]) {
    out.put_u32_le(b.len() as u32);
    out.put_slice(b);
}

/// Encode one response frame.
pub fn response(request_id: u64, status: u8, payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(HEADER_LEN + payload.len());
    out.put_u32_le((FRAME_OVERHEAD + payload.len()) as u32);
    out.put_u64_le(request_id);
    out.put_u8(status);
    out.put_slice(payload);
    out.freeze()
}

/// Encode one request frame (client side; also used by tests).
pub fn request(request_id: u64, opcode: u8, payload: &[u8]) -> Bytes {
    // same layout as a response — the direction gives the byte its meaning
    response(request_id, opcode, payload)
}

// ---------------------------------------------------------------- opcodes

pub const OP_HELLO: u8 = 0x00;
pub const OP_SNAPSHOT: u8 = 0x01;
pub const OP_FETCH_TABLE: u8 = 0x02;
pub const OP_FETCH_VALUE: u8 = 0x03;
pub const OP_SUBSCRIBE: u8 = 0x04;

/// Server-pushed frames after SUBSCRIBE (request_id = 0).
pub const PUSH_STREAM: u8 = 0x10;
pub const PUSH_PING: u8 = 0x11;
pub const PUSH_LAGGED: u8 = 0x12;

// ----------------------------------------------------------------- status

pub const ST_OK: u8 = 0x00;
/// Engine/server failure; payload: UTF-8 message.
pub const ST_ERR: u8 = 0x01;
/// The referenced file left the live version; re-pull the slice.
pub const ST_GONE: u8 = 0x02;
/// The master store is unnamed — replication requires a store identity.
pub const ST_NO_IDENTITY: u8 = 0x03;
/// Unknown opcode / malformed payload; payload: UTF-8 message.
pub const ST_BAD_FRAME: u8 = 0x04;

pub fn status_for(e: &fluent31::Error) -> u8 {
    match e {
        fluent31::Error::Gone(_) => ST_GONE,
        _ => ST_ERR,
    }
}

// --------------------------------------------------------------- payloads

/// HELLO response: `[u32 version][blob name][16B instance_id][u64 visible]`.
pub fn encode_hello(name: &str, instance: &fluent31::InstanceId, visible: u64) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.extend_from_slice(&REPL_VERSION.to_le_bytes());
    put_blob(&mut out, name.as_bytes());
    out.extend_from_slice(instance);
    out.extend_from_slice(&visible.to_le_bytes());
    out.to_vec()
}

pub struct Hello {
    pub version: u32,
    pub name: String,
    pub instance_id: fluent31::InstanceId,
    pub visible_seqno: u64,
}

pub fn decode_hello(payload: &[u8]) -> Result<Hello, String> {
    let mut rd = Rd(payload);
    let version = rd.u32()?;
    let name = String::from_utf8(rd.blob()?.to_vec()).map_err(|_| "name not UTF-8".to_string())?;
    let id = rd.blob_exact(16)?;
    let instance_id: fluent31::InstanceId = id.try_into().expect("16 bytes");
    let visible_seqno = rd.u64()?;
    rd.done()?;
    Ok(Hello {
        version,
        name,
        instance_id,
        visible_seqno,
    })
}

/// Slice manifest: `[u64 flushed][u32 nlevels]` then per level `[u32
/// nruns]`, per run `[u64 id][u32 ntables]`, per table `[u64 id][u64
/// size][blob min][blob max]`.
pub fn encode_slice(m: &SliceManifest) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.extend_from_slice(&m.flushed_seqno.to_le_bytes());
    out.extend_from_slice(&(m.levels.len() as u32).to_le_bytes());
    for runs in &m.levels {
        out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
        for r in runs {
            out.extend_from_slice(&r.id.to_le_bytes());
            out.extend_from_slice(&(r.tables.len() as u32).to_le_bytes());
            for t in &r.tables {
                out.extend_from_slice(&t.id.to_le_bytes());
                out.extend_from_slice(&t.size.to_le_bytes());
                put_blob(&mut out, &t.min_ukey);
                put_blob(&mut out, &t.max_ukey);
            }
        }
    }
    out.to_vec()
}

pub fn decode_slice(payload: &[u8]) -> Result<SliceManifest, String> {
    let mut rd = Rd(payload);
    let flushed_seqno = rd.u64()?;
    let nlevels = rd.u32()? as usize;
    let mut levels = Vec::with_capacity(nlevels.min(64));
    for _ in 0..nlevels {
        let nruns = rd.u32()? as usize;
        let mut runs = Vec::with_capacity(nruns.min(1024));
        for _ in 0..nruns {
            let id = rd.u64()?;
            let ntables = rd.u32()? as usize;
            let mut tables = Vec::with_capacity(ntables.min(1024));
            for _ in 0..ntables {
                tables.push(SliceTable {
                    id: rd.u64()?,
                    size: rd.u64()?,
                    min_ukey: rd.blob()?.to_vec(),
                    max_ukey: rd.blob()?.to_vec(),
                });
            }
            runs.push(SliceRun { id, tables });
        }
        levels.push(runs);
    }
    rd.done()?;
    Ok(SliceManifest {
        flushed_seqno,
        levels,
    })
}

/// Stream batch: `[u32 count]` then per entry `[u8 kind][u64 seqno]
/// [u64 commit_seqno][blob key]` plus `[blob value]` for puts (deletes
/// carry no value).
pub fn encode_stream_batch(entries: &[StreamEntry]) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&[e.kind as u8]);
        out.extend_from_slice(&e.seqno.to_le_bytes());
        out.extend_from_slice(&e.commit_seqno.to_le_bytes());
        put_blob(&mut out, &e.key);
        if e.kind == ValueKind::Put {
            put_blob(&mut out, e.value.as_deref().unwrap_or_default());
        }
    }
    out.to_vec()
}

pub fn decode_stream_batch(payload: &[u8]) -> Result<Vec<StreamEntry>, String> {
    let mut rd = Rd(payload);
    let count = rd.u32()? as usize;
    let mut out = Vec::with_capacity(count.min(65_536));
    for _ in 0..count {
        let kind = match rd.u8()? {
            0 => ValueKind::Delete,
            1 => ValueKind::Put,
            k => return Err(format!("bad stream entry kind {k}")),
        };
        let seqno = rd.u64()?;
        let commit_seqno = rd.u64()?;
        let key = rd.blob()?.to_vec();
        let value = match kind {
            ValueKind::Put => Some(rd.blob()?.to_vec()),
            ValueKind::Delete => None,
        };
        out.push(StreamEntry {
            key,
            seqno,
            commit_seqno,
            kind,
            value,
        });
    }
    rd.done()?;
    Ok(out)
}

/// Extension for fixed-width byte fields on the shared payload reader.
pub trait RdExt<'a> {
    fn blob_exact(&mut self, n: usize) -> Result<&'a [u8], String>;
}

impl<'a> RdExt<'a> for Rd<'a> {
    fn blob_exact(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.0.len() < n {
            return Err("truncated payload".into());
        }
        let (a, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(a)
    }
}

/// A key's leading bytes as hex: enough to identify a range in a log line
/// without dumping the whole key.
pub(crate) fn hex_prefix(key: &[u8]) -> String {
    key.iter().take(16).map(|b| format!("{b:02x}")).collect()
}
