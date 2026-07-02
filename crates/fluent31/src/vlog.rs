//! Value log (WiscKey-style key-value separation).
//!
//! Record: `[crc32c u32][klen varint][vlen varint][key][value]` — the CRC
//! covers everything after itself. Records embed their key so that (a) GC can
//! interrogate the LSM for liveness and (b) **every dereference verifies the
//! key**, which converts cross-file/offset aliasing after crashes into a
//! clean corruption error instead of silently returning another key's value.
//!
//! Lifetime rules (see DESIGN.md §13): live vlog files are pinned by
//! `Arc<VlogFileHandle>` ownership inside `Version`; a GC'd victim is only
//! marked obsolete (and thus deleted on last Arc drop) once the snapshot
//! watermark AND the flushed-seqno watermark pass its retirement seqno.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::cache::BlockCache;
use crate::coding::{crc32, put_uvarint, Reader};
use crate::error::{corrupt, Result};
use crate::io::{DbFile, Io, ReadReq};
use crate::types::ValuePtr;

pub(crate) struct VlogFileHandle {
    pub id: u64,
    pub path: PathBuf,
    pub file: Arc<dyn DbFile>,
    obsolete: AtomicBool,
}

impl VlogFileHandle {
    pub fn new(id: u64, path: PathBuf, file: Arc<dyn DbFile>) -> Self {
        VlogFileHandle {
            id,
            path,
            file,
            obsolete: AtomicBool::new(false),
        }
    }

    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for VlogFileHandle {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn encode_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(20 + key.len() + value.len());
    put_uvarint(&mut body, key.len() as u64);
    put_uvarint(&mut body, value.len() as u64);
    body.extend_from_slice(key);
    body.extend_from_slice(value);
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&crc32(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Parse a full record buffer; verifies CRC. Returns (key, value).
pub(crate) fn parse_record(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut r = Reader::new(bytes);
    let crc = r.u32()?;
    if crc32(&bytes[4..]) != crc {
        return Err(corrupt("vlog record crc mismatch"));
    }
    let klen = r.uvarint()? as usize;
    let vlen = r.uvarint()? as usize;
    let key = r.bytes(klen)?.to_vec();
    let value = r.bytes(vlen)?.to_vec();
    if !r.is_empty() {
        return Err(corrupt("vlog record trailing bytes"));
    }
    Ok((key, value))
}

/// Extract the value for `expect_key`, failing on key mismatch (dangling or
/// aliased pointer — must never surface another key's bytes).
pub(crate) fn record_value_for(bytes: &[u8], expect_key: &[u8]) -> Result<Vec<u8>> {
    let (key, value) = parse_record(bytes)?;
    if key != expect_key {
        return Err(corrupt("vlog record key mismatch (dangling pointer)"));
    }
    Ok(value)
}

/// Read + verify + slice a pointed-to value through the optional cache.
pub(crate) fn read_value(
    handle: &VlogFileHandle,
    ptr: &ValuePtr,
    expect_key: &[u8],
    cache: Option<&BlockCache>,
) -> Result<Vec<u8>> {
    if let Some(c) = cache {
        if let Some(hit) = c.get(handle.id, ptr.offset) {
            return record_value_for(&hit, expect_key);
        }
    }
    let mut buf = vec![0u8; ptr.len as usize];
    handle.file.read_exact_at(ptr.offset, &mut buf)?;
    let value = record_value_for(&buf, expect_key)?;
    if let Some(c) = cache {
        // Only cache small records; huge values would thrash the cache.
        if buf.len() <= 64 << 10 {
            c.insert(handle.id, ptr.offset, Arc::new(buf));
        }
    }
    Ok(value)
}

/// Batch-resolve pointers against one file (callers group by file id). One
/// io_uring submission on the uring backend.
pub(crate) fn read_values_batch(
    handle: &VlogFileHandle,
    items: &[(ValuePtr, &[u8])],
) -> Result<Vec<Vec<u8>>> {
    let mut reqs: Vec<ReadReq> = items
        .iter()
        .map(|(p, _)| ReadReq {
            off: p.offset,
            buf: vec![0u8; p.len as usize],
        })
        .collect();
    handle.file.read_many(&mut reqs)?;
    items
        .iter()
        .zip(reqs)
        .map(|((_, key), req)| record_value_for(&req.buf, key))
        .collect()
}

/// Scan a vlog file from the start, yielding (offset, record_len, key, vlen)
/// for every valid record; stops cleanly at the first invalid one. Used by GC
/// (liveness scan) and recovery (valid-prefix of young files).
/// One scanned record: (offset, record len, key, value len).
pub(crate) type ScannedRecord = (u64, u32, Vec<u8>, u32);

pub(crate) fn scan_records(file: &dyn DbFile) -> Result<(Vec<ScannedRecord>, u64)> {
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    if len > 0 {
        file.read_exact_at(0, &mut buf)?;
    }
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < buf.len() {
        let rest = &buf[pos..];
        let mut r = Reader::new(rest);
        let Ok(crc) = r.u32() else { break };
        let Ok(klen) = r.uvarint() else { break };
        let Ok(vlen) = r.uvarint() else { break };
        let header = rest.len() - r.remaining();
        // corruption-controlled varints: checked math so a wrapped sum stops
        // the scan cleanly instead of panicking on the slice
        let Some(total) = (header as u64)
            .checked_add(klen)
            .and_then(|t| t.checked_add(vlen))
            .filter(|&t| t <= rest.len() as u64 && t <= u32::MAX as u64)
        else {
            break;
        };
        if crc32(&rest[4..total as usize]) != crc {
            break;
        }
        let key = rest[header..header + klen as usize].to_vec();
        out.push((pos as u64, total as u32, key, vlen as u32));
        pos += total as usize;
    }
    Ok((out, pos as u64))
}

pub(crate) struct VlogHead {
    pub handle: Arc<VlogFileHandle>,
    pub written: u64,
    pub synced: u64,
}

/// Append side of the value log. All appends happen under the DB write mutex;
/// the internal mutex just keeps the bookkeeping consistent for readers of
/// head state (checkpoint, rotation).
pub(crate) struct Vlog {
    head: Mutex<VlogHead>,
}

impl Vlog {
    pub fn new(handle: Arc<VlogFileHandle>, written: u64) -> Self {
        Vlog {
            head: Mutex::new(VlogHead {
                handle,
                written,
                synced: written,
            }),
        }
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<ValuePtr> {
        let rec = encode_record(key, value);
        if rec.len() as u64 > u32::MAX as u64 {
            return Err(crate::error::Error::InvalidArgument(
                "vlog record exceeds 4 GiB pointer limit".into(),
            ));
        }
        let mut head = self.head.lock();
        let off = head.handle.file.append(&rec)?;
        head.written = off + rec.len() as u64;
        Ok(ValuePtr {
            file: head.handle.id,
            offset: off,
            len: rec.len() as u32,
        })
    }

    /// fdatasync the head if it has unsynced appends. Returns the synced
    /// length (== written length at the time of the call).
    pub fn sync_head(&self) -> Result<u64> {
        let mut head = self.head.lock();
        if head.synced < head.written {
            head.handle.file.sync_data()?;
            head.synced = head.written;
        }
        Ok(head.synced)
    }

    pub fn head_state(&self) -> (u64, u64, u64) {
        let head = self.head.lock();
        (head.handle.id, head.written, head.synced)
    }

    /// Seal the current head (fdatasync) and swap in a fresh head file.
    /// Returns the sealed handle. Caller publishes the new handle in the next
    /// Version and manifest.
    pub fn rotate(
        &self,
        io: &dyn Io,
        new_id: u64,
        new_path: PathBuf,
    ) -> Result<(Arc<VlogFileHandle>, Arc<VlogFileHandle>)> {
        let file = io.create_new(&new_path)?;
        let new_handle = Arc::new(VlogFileHandle::new(new_id, new_path, file));
        let mut head = self.head.lock();
        if head.synced < head.written {
            head.handle.file.sync_data()?;
        }
        let sealed = std::mem::replace(&mut head.handle, new_handle.clone());
        head.written = 0;
        head.synced = 0;
        Ok((sealed, new_handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IoBackend;
    use crate::io::backend;

    #[test]
    fn record_roundtrip_and_key_check() {
        let rec = encode_record(b"key", b"value-bytes");
        let (k, v) = parse_record(&rec).unwrap();
        assert_eq!(k, b"key");
        assert_eq!(v, b"value-bytes");
        assert!(record_value_for(&rec, b"key").is_ok());
        assert!(record_value_for(&rec, b"other").is_err());
        let mut bad = rec.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(parse_record(&bad).is_err());
    }

    #[test]
    fn append_read_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        let path = dir.path().join("vlog-000001.vlog");
        let file = io.create_new(&path).unwrap();
        let handle = Arc::new(VlogFileHandle::new(1, path.clone(), file));
        let vlog = Vlog::new(handle.clone(), 0);

        let p1 = vlog.append(b"a", &vec![1u8; 100]).unwrap();
        let p2 = vlog.append(b"b", &vec![2u8; 200]).unwrap();
        vlog.sync_head().unwrap();

        assert_eq!(read_value(&handle, &p1, b"a", None).unwrap(), vec![1u8; 100]);
        assert_eq!(read_value(&handle, &p2, b"b", None).unwrap(), vec![2u8; 200]);
        // wrong expected key must fail
        assert!(read_value(&handle, &p1, b"b", None).is_err());

        let (recs, valid) = scan_records(handle.file.as_ref()).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].2, b"a");
        assert_eq!(recs[1].0, p2.offset);
        assert_eq!(valid, p2.offset + p2.len as u64);

        let vals =
            read_values_batch(&handle, &[(p2, b"b" as &[u8]), (p1, b"a" as &[u8])]).unwrap();
        assert_eq!(vals[0], vec![2u8; 200]);
        assert_eq!(vals[1], vec![1u8; 100]);
    }

    #[test]
    fn scan_stops_at_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        let path = dir.path().join("v");
        let file = io.create_new(&path).unwrap();
        let handle = Arc::new(VlogFileHandle::new(1, path.clone(), file));
        let vlog = Vlog::new(handle.clone(), 0);
        vlog.append(b"a", b"11111").unwrap();
        let p2 = vlog.append(b"b", b"22222").unwrap();
        vlog.sync_head().unwrap();
        // truncate mid-second-record
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(p2.offset + 3).unwrap();

        let file = io.open_read(&path).unwrap();
        let (recs, valid) = scan_records(file.as_ref()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(valid, p2.offset);
    }

    #[test]
    fn obsolete_handle_deletes_file_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        let path = dir.path().join("v");
        let file = io.create_new(&path).unwrap();
        let handle = Arc::new(VlogFileHandle::new(1, path.clone(), file));
        handle.mark_obsolete();
        drop(handle);
        assert!(!path.exists());
    }
}
