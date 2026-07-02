//! Fluent wire v1: frame layout, opcodes, status codes, and payload
//! codecs. See WIRE.md at the repository root for the full specification.
//!
//! Frame (both directions):
//!   [u32 frame_len LE]  length of everything AFTER this field
//!   [u64 request_id LE] client-allocated, per-connection scope, echoed
//!                       verbatim on the response; 0 is reserved for
//!                       future server-initiated frames
//!   [u8  opcode / status]
//!   [payload…]
//!
//! Integers are little-endian; lengths inside payloads are u32 LE (keys
//! and values are engine-capped far below 4 GiB). Responses may arrive in
//! ANY order — correlate by request_id.

use bytes::{BufMut, Bytes, BytesMut};

pub const PROTOCOL_VERSION: u32 = 1;

/// Bytes of [request_id][opcode] after the length field.
pub const FRAME_OVERHEAD: usize = 8 + 1;
/// Full header: length field + overhead.
pub const HEADER_LEN: usize = 4 + FRAME_OVERHEAD;

// ---------------------------------------------------------------- opcodes

pub const OP_HELLO: u8 = 0x00;
pub const OP_GET: u8 = 0x01;
pub const OP_PUT: u8 = 0x02;
pub const OP_DEL: u8 = 0x03;
pub const OP_BATCH: u8 = 0x04;
pub const OP_SCAN: u8 = 0x05;
pub const OP_QUERY: u8 = 0x06;
pub const OP_EXEC: u8 = 0x07;
pub const OP_SYNC_WAL: u8 = 0x08;
pub const OP_STATS: u8 = 0x09;

// ----------------------------------------------------------------- status

pub const ST_OK: u8 = 0x00;
/// GET/scan miss (not an error; payload empty).
pub const ST_NOT_FOUND: u8 = 0x01;
pub const ST_INVALID: u8 = 0x02;
pub const ST_CONFLICT: u8 = 0x03;
/// Payload: [i32 exit_code LE][guest output…].
pub const ST_GUEST_FAILED: u8 = 0x04;
/// Store degraded (bg_error); reopen required.
pub const ST_BACKGROUND: u8 = 0x05;
pub const ST_CLOSED: u8 = 0x06;
pub const ST_IO: u8 = 0x07;
/// Frame exceeded the server's max_frame; payload: UTF-8 message.
pub const ST_TOO_LARGE: u8 = 0x08;
/// Unknown opcode / malformed payload; payload: UTF-8 message.
pub const ST_BAD_FRAME: u8 = 0x09;
pub const ST_CORRUPTION: u8 = 0x0a;
pub const ST_WASM: u8 = 0x0b;

pub fn status_for(e: &fluent31::Error) -> u8 {
    use fluent31::Error as E;
    match e {
        E::Io(_) => ST_IO,
        E::Corruption(_) => ST_CORRUPTION,
        E::InvalidArgument(_) => ST_INVALID,
        E::Conflict => ST_CONFLICT,
        E::Closed => ST_CLOSED,
        E::Background(_) => ST_BACKGROUND,
        E::Wasm(_) => ST_WASM,
        E::GuestFailed { .. } => ST_GUEST_FAILED,
    }
}

// -------------------------------------------------------- payload cursors

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
