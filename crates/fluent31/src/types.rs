//! Internal key encoding and MVCC value representation.
//!
//! An *internal key* is `user_key ++ trailer` where the 8-byte big-endian
//! trailer packs `(seqno << 8) | kind`. Ordering is `(user_key asc, seqno
//! desc, kind desc)` so that seeking `(k, s)` lands on the newest version of
//! `k` with `seqno <= s`.

use std::cmp::Ordering;

use crate::coding::{put_uvarint, Reader};
use crate::error::{corrupt, Error, Result};

pub type SeqNo = u64;

/// Seqnos must leave 8 bits for the kind byte in the trailer.
pub const MAX_SEQNO: SeqNo = (1 << 56) - 1;

pub const TRAILER_LEN: usize = 8;

/// Kind byte used only in seek targets; sorts before every real kind at the
/// same seqno (trailer compares descending), so `lower_bound(seek(k, s))`
/// yields the newest real entry of `k` with `seqno <= s`.
const SEEK_KIND: u8 = 0xff;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Delete = 0,
    Put = 1,
}

impl ValueKind {
    pub fn from_u8(b: u8) -> Result<ValueKind> {
        match b {
            0 => Ok(ValueKind::Delete),
            1 => Ok(ValueKind::Put),
            other => Err(corrupt(format!("bad value kind {other}"))),
        }
    }
}

fn pack_trailer(seq: SeqNo, kind: u8) -> u64 {
    debug_assert!(seq <= MAX_SEQNO);
    (seq << 8) | u64::from(kind)
}

/// Build an internal key for a real entry.
pub fn make_ikey(user_key: &[u8], seq: SeqNo, kind: ValueKind) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&pack_trailer(seq, kind as u8).to_be_bytes());
    out
}

/// Build a seek target: positions at the newest version of `user_key` with
/// `seqno <= seq` (or the next user key if none qualifies).
pub fn make_seek_ikey(user_key: &[u8], seq: SeqNo) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&pack_trailer(seq, SEEK_KIND).to_be_bytes());
    out
}

pub fn ikey_ukey(ikey: &[u8]) -> &[u8] {
    debug_assert!(ikey.len() >= TRAILER_LEN);
    &ikey[..ikey.len() - TRAILER_LEN]
}

fn ikey_trailer(ikey: &[u8]) -> u64 {
    let n = ikey.len();
    u64::from_be_bytes(ikey[n - TRAILER_LEN..].try_into().unwrap())
}

pub fn ikey_seqno(ikey: &[u8]) -> SeqNo {
    ikey_trailer(ikey) >> 8
}

pub fn ikey_kind(ikey: &[u8]) -> Result<ValueKind> {
    ValueKind::from_u8((ikey_trailer(ikey) & 0xff) as u8)
}

/// The internal-key comparator: user key ascending, then trailer descending
/// (newest seqno first).
pub fn cmp_ikey(a: &[u8], b: &[u8]) -> Ordering {
    ikey_ukey(a)
        .cmp(ikey_ukey(b))
        .then_with(|| ikey_trailer(b).cmp(&ikey_trailer(a)))
}

/// Owned internal key ordered by `cmp_ikey`; the memtable's skiplist key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey(pub Vec<u8>);

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_ikey(&self.0, &other.0)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// Value representation (inline vs value-log pointer)
// ---------------------------------------------------------------------------

const REPR_INLINE: u8 = 0;
const REPR_PTR: u8 = 1;

/// Location of a whole record inside a value-log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValuePtr {
    pub file: u64,
    /// Byte offset of the record header within the file.
    pub offset: u64,
    /// Total record length (header + key + value).
    pub len: u32,
}

/// What an LSM `Put` actually stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReprRef<'a> {
    Inline(&'a [u8]),
    Ptr(ValuePtr),
}

pub fn encode_inline(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + value.len());
    out.push(REPR_INLINE);
    out.extend_from_slice(value);
    out
}

pub fn encode_ptr(ptr: ValuePtr) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 10 * 3);
    out.push(REPR_PTR);
    put_uvarint(&mut out, ptr.file);
    put_uvarint(&mut out, ptr.offset);
    put_uvarint(&mut out, u64::from(ptr.len));
    out
}

pub fn decode_repr(repr: &[u8]) -> Result<ReprRef<'_>> {
    let mut r = Reader::new(repr);
    match r.u8()? {
        REPR_INLINE => Ok(ReprRef::Inline(&repr[1..])),
        REPR_PTR => {
            let file = r.uvarint()?;
            let offset = r.uvarint()?;
            let len = r.uvarint()?;
            if len > u32::MAX as u64 {
                return Err(corrupt("vlog pointer length overflow"));
            }
            Ok(ReprRef::Ptr(ValuePtr {
                file,
                offset,
                len: len as u32,
            }))
        }
        other => Err(corrupt(format!("bad value repr tag {other}"))),
    }
}

// ---------------------------------------------------------------------------
// Reserved keyspace
// ---------------------------------------------------------------------------

/// Every key beginning with 0x00 belongs to the engine (installed WASM
/// modules live at `\x00wasm\x00<name>`).
pub const SYS_PREFIX: u8 = 0x00;

/// First possible user key; user-facing unbounded iteration starts here.
pub const USER_KEYSPACE_START: &[u8] = &[0x01];

pub fn validate_user_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument("empty keys are not allowed".into()));
    }
    if key[0] == SYS_PREFIX {
        return Err(Error::InvalidArgument(
            "keys starting with 0x00 are reserved for the engine".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "wasm")]
pub fn sys_wasm_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(6 + name.len());
    k.push(SYS_PREFIX);
    k.extend_from_slice(b"wasm");
    k.push(0);
    k.extend_from_slice(name.as_bytes());
    k
}

/// Registered trigger definition record: `\x00trg\x00<name>`.
#[cfg(feature = "wasm")]
pub fn sys_trigger_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(5 + name.len());
    k.push(SYS_PREFIX);
    k.extend_from_slice(b"trg");
    k.push(0);
    k.extend_from_slice(name.as_bytes());
    k
}

/// Keys-mode pending-event record: `\x00trgq\x00<name>\x00<user key>`. The
/// touched user key IS the queue key, so re-touches coalesce to one pending
/// event. (Trigger names cannot contain 0x00, so the layout is unambiguous.)
#[cfg(feature = "wasm")]
pub fn sys_trigger_event_key(name: &str, user_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + name.len() + user_key.len());
    k.push(SYS_PREFIX);
    k.extend_from_slice(b"trgq");
    k.push(0);
    k.extend_from_slice(name.as_bytes());
    k.push(0);
    k.extend_from_slice(user_key);
    k
}

/// Changes-mode pending-event record: `\x00trgq\x00<name>\x00<be64 seqno>`.
/// The queue key is the triggering op's commit seqno, so events are unique
/// per committed op and iterate in commit order — the opposite of keys-mode
/// coalescing, by design (the record value carries the change itself).
#[cfg(feature = "wasm")]
pub fn sys_trigger_change_key(name: &str, seqno: SeqNo) -> Vec<u8> {
    let mut k = Vec::with_capacity(15 + name.len());
    k.push(SYS_PREFIX);
    k.extend_from_slice(b"trgq");
    k.push(0);
    k.extend_from_slice(name.as_bytes());
    k.push(0);
    k.extend_from_slice(&seqno.to_be_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_user_key_then_seq_desc() {
        let a = make_ikey(b"a", 5, ValueKind::Put);
        let a_newer = make_ikey(b"a", 9, ValueKind::Put);
        let b = make_ikey(b"b", 1, ValueKind::Put);
        assert_eq!(cmp_ikey(&a_newer, &a), Ordering::Less); // newer first
        assert_eq!(cmp_ikey(&a, &b), Ordering::Less);
        assert_eq!(cmp_ikey(&a_newer, &b), Ordering::Less);
        // prefix user keys are ordered by user key, not merged bytes
        let ab = make_ikey(b"ab", 1, ValueKind::Put);
        assert_eq!(cmp_ikey(&a_newer, &ab), Ordering::Less);
        assert_eq!(cmp_ikey(&ab, &b), Ordering::Less);
    }

    #[test]
    fn seek_lands_on_newest_visible() {
        // entries for key "k": seq 9, 5, 3 (sorted: 9 first)
        let e9 = make_ikey(b"k", 9, ValueKind::Put);
        let e5 = make_ikey(b"k", 5, ValueKind::Delete);
        let e3 = make_ikey(b"k", 3, ValueKind::Put);
        let seek7 = make_seek_ikey(b"k", 7);
        // seek(7) must sort after e9, before e5 and e3
        assert_eq!(cmp_ikey(&e9, &seek7), Ordering::Less);
        assert_eq!(cmp_ikey(&seek7, &e5), Ordering::Less);
        assert_eq!(cmp_ikey(&seek7, &e3), Ordering::Less);
        // seek at exactly 5 includes the seq-5 entry
        let seek5 = make_seek_ikey(b"k", 5);
        assert_eq!(cmp_ikey(&seek5, &e5), Ordering::Less);
        assert_eq!(cmp_ikey(&e9, &seek5), Ordering::Less);
    }

    #[test]
    fn repr_roundtrip() {
        let inline = encode_inline(b"v");
        assert_eq!(decode_repr(&inline).unwrap(), ReprRef::Inline(b"v"));
        let p = ValuePtr {
            file: 7,
            offset: 123456789,
            len: 4096,
        };
        let enc = encode_ptr(p);
        assert_eq!(decode_repr(&enc).unwrap(), ReprRef::Ptr(p));
    }

    #[test]
    fn user_key_validation() {
        assert!(validate_user_key(b"ok").is_ok());
        assert!(validate_user_key(b"").is_err());
        assert!(validate_user_key(&[0x00, b'x']).is_err());
    }
}
