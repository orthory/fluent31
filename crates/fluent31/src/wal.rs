//! Write-ahead log: length + CRC framed records, one file per memtable
//! generation.
//!
//! Framing: `[len u32][crc32c(payload) u32][payload]`.
//!
//! Recovery semantics (critical, see DESIGN.md §5): a truncated or
//! CRC-invalid record is *torn-tail loss* only in the newest WAL; sealed WALs
//! were fdatasynced at rotation, so damage there is real corruption and
//! recovery must fail loudly rather than resurrect a non-prefix history.

use std::sync::Arc;

use crate::coding::{crc32, Reader};
use crate::error::Result;
use crate::io::DbFile;

const HEADER_LEN: usize = 8;
/// Sanity bound; a record is one encoded write batch and batches are size-
/// capped well below this at the write path.
const MAX_RECORD: u32 = 1 << 30;

pub(crate) struct WalWriter {
    file: Arc<dyn DbFile>,
}

impl WalWriter {
    pub fn new(file: Arc<dyn DbFile>) -> Self {
        WalWriter { file }
    }

    pub fn append_record(&self, payload: &[u8]) -> Result<()> {
        debug_assert!(payload.len() < MAX_RECORD as usize);
        let mut rec = Vec::with_capacity(HEADER_LEN + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&crc32(payload).to_le_bytes());
        rec.extend_from_slice(payload);
        self.file.append(&rec)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WalTail {
    /// File ended exactly on a record boundary.
    Clean,
    /// Trailing bytes did not form a valid record; `valid_len` is the byte
    /// length of the valid prefix.
    Torn { valid_len: u64 },
}

/// Read every valid record from the head of the file. Never errors on a bad
/// tail — classification of Torn as "fine" vs "corruption" is the caller's
/// job (it depends on whether this is the newest WAL).
pub(crate) fn read_wal(file: &dyn DbFile) -> Result<(Vec<Vec<u8>>, WalTail)> {
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact_at(0, &mut buf)?;

    let mut records = Vec::new();
    let mut pos = 0usize;
    loop {
        if buf.len() - pos == 0 {
            return Ok((records, WalTail::Clean));
        }
        if buf.len() - pos < HEADER_LEN {
            return Ok((records, WalTail::Torn { valid_len: pos as u64 }));
        }
        let mut r = Reader::new(&buf[pos..pos + HEADER_LEN]);
        let rec_len = r.u32().expect("sized") as usize;
        let crc = r.u32().expect("sized");
        // an all-zero header would pass the CRC check (crc32 of an empty
        // payload is 0), but the engine never writes empty records — treat a
        // zero-filled region as a torn tail, not data
        if rec_len == 0 && crc == 0 {
            return Ok((records, WalTail::Torn { valid_len: pos as u64 }));
        }
        if rec_len as u32 > MAX_RECORD || buf.len() - pos - HEADER_LEN < rec_len {
            return Ok((records, WalTail::Torn { valid_len: pos as u64 }));
        }
        let payload = &buf[pos + HEADER_LEN..pos + HEADER_LEN + rec_len];
        if crc32(payload) != crc {
            return Ok((records, WalTail::Torn { valid_len: pos as u64 }));
        }
        records.push(payload.to_vec());
        pos += HEADER_LEN + rec_len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IoBackend;
    use crate::io::backend;

    fn setup() -> (tempfile::TempDir, Arc<dyn crate::io::Io>) {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        (dir, io)
    }

    #[test]
    fn roundtrip_multiple_records() {
        let (dir, io) = setup();
        let path = dir.path().join("wal");
        let w = WalWriter::new(io.create_new(&path).unwrap());
        w.append_record(b"first").unwrap();
        w.append_record(b"second").unwrap();
        w.append_record(&vec![7u8; 100_000]).unwrap();
        w.sync().unwrap();

        let f = io.open_read(&path).unwrap();
        let (recs, tail) = read_wal(f.as_ref()).unwrap();
        assert_eq!(tail, WalTail::Clean);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0], b"first");
        assert_eq!(recs[1], b"second");
        assert_eq!(recs[2], vec![7u8; 100_000]);
    }

    #[test]
    fn zero_filled_tail_is_torn_not_empty_record() {
        // an empty record's header is indistinguishable from zero fill, so
        // the reader treats it as a torn tail — the engine never writes
        // empty payloads (a batch always encodes at least its header)
        let (dir, io) = setup();
        let path = dir.path().join("wal");
        let w = WalWriter::new(io.create_new(&path).unwrap());
        w.append_record(b"real").unwrap();
        w.sync().unwrap();
        let valid = std::fs::metadata(&path).unwrap().len();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 32]).unwrap();
        }
        let f = io.open_read(&path).unwrap();
        let (recs, tail) = read_wal(f.as_ref()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(tail, WalTail::Torn { valid_len: valid });
    }

    #[test]
    fn torn_tail_is_detected_and_prefix_kept() {
        let (dir, io) = setup();
        let path = dir.path().join("wal");
        let w = WalWriter::new(io.create_new(&path).unwrap());
        w.append_record(b"keep-me").unwrap();
        w.sync().unwrap();
        let keep_len = std::fs::metadata(&path).unwrap().len();
        // simulate a torn append: header + partial payload
        w.append_record(b"torn-record-payload").unwrap();
        let full = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 5).unwrap();

        let f = io.open_read(&path).unwrap();
        let (recs, tail) = read_wal(f.as_ref()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0], b"keep-me");
        assert_eq!(
            tail,
            WalTail::Torn {
                valid_len: keep_len
            }
        );
    }

    #[test]
    fn corrupt_crc_stops_replay() {
        let (dir, io) = setup();
        let path = dir.path().join("wal");
        let w = WalWriter::new(io.create_new(&path).unwrap());
        w.append_record(b"aaaa").unwrap();
        w.append_record(b"bbbb").unwrap();
        w.sync().unwrap();
        // flip a payload byte of the first record
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let f = io.open_read(&path).unwrap();
        let (recs, tail) = read_wal(f.as_ref()).unwrap();
        assert!(recs.is_empty());
        assert_eq!(tail, WalTail::Torn { valid_len: 0 });
    }
}
