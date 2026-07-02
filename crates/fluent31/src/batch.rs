//! Write batches: the public mutation unit and its WAL payload encoding.
//!
//! WAL payload: `[base_seqno u64][count u32]` then per entry
//! `[kind u8][klen varint][key][rlen varint][repr]` where `repr` is the
//! already-placed value representation (inline bytes or vlog pointer) — the
//! vlog placement happens *before* WAL encoding so recovery replays pointers
//! verbatim.

use crate::coding::{put_len_prefixed, put_u32, put_u64, Reader};
use crate::error::Result;
use crate::types::{SeqNo, ValueKind};

/// A set of writes applied atomically (all-or-nothing in WAL and memtable,
/// single seqno range).
#[derive(Default, Clone)]
pub struct WriteBatch {
    pub(crate) ops: Vec<BatchOp>,
    bytes: usize,
}

#[derive(Clone)]
pub(crate) enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        let (key, value) = (key.into(), value.into());
        self.bytes += key.len() + value.len();
        self.ops.push(BatchOp::Put { key, value });
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        let key = key.into();
        self.bytes += key.len();
        self.ops.push(BatchOp::Delete { key });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Approximate payload size (keys + values), used for stall accounting
    /// and size caps.
    pub fn byte_size(&self) -> usize {
        self.bytes
    }
}

/// An entry after value placement: what actually lands in WAL + memtable.
pub(crate) struct EncEntry {
    pub kind: ValueKind,
    pub key: Vec<u8>,
    /// Encoded value representation (types::encode_inline / encode_ptr);
    /// empty for deletes.
    pub repr: Vec<u8>,
}

pub(crate) fn encode_batch(base_seqno: SeqNo, entries: &[EncEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        16 + entries
            .iter()
            .map(|e| e.key.len() + e.repr.len() + 12)
            .sum::<usize>(),
    );
    put_u64(&mut out, base_seqno);
    put_u32(&mut out, entries.len() as u32);
    for e in entries {
        out.push(e.kind as u8);
        put_len_prefixed(&mut out, &e.key);
        put_len_prefixed(&mut out, &e.repr);
    }
    out
}

pub(crate) fn decode_batch(payload: &[u8]) -> Result<(SeqNo, Vec<EncEntry>)> {
    let mut r = Reader::new(payload);
    let base = r.u64()?;
    let count = r.u32()?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let kind = ValueKind::from_u8(r.u8()?)?;
        let key = r.len_prefixed()?.to_vec();
        let repr = r.len_prefixed()?.to_vec();
        entries.push(EncEntry { kind, key, repr });
    }
    Ok((base, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::encode_inline;

    #[test]
    fn encode_decode_roundtrip() {
        let entries = vec![
            EncEntry {
                kind: ValueKind::Put,
                key: b"alpha".to_vec(),
                repr: encode_inline(b"one"),
            },
            EncEntry {
                kind: ValueKind::Delete,
                key: b"beta".to_vec(),
                repr: Vec::new(),
            },
        ];
        let payload = encode_batch(42, &entries);
        let (base, got) = decode_batch(&payload).unwrap();
        assert_eq!(base, 42);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].key, b"alpha");
        assert_eq!(got[0].repr, encode_inline(b"one"));
        assert!(matches!(got[1].kind, ValueKind::Delete));
    }

    #[test]
    fn truncated_payload_errors() {
        let entries = vec![EncEntry {
            kind: ValueKind::Put,
            key: b"k".to_vec(),
            repr: encode_inline(b"v"),
        }];
        let payload = encode_batch(1, &entries);
        assert!(decode_batch(&payload[..payload.len() - 1]).is_err());
    }
}
