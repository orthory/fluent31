//! Sorted-run table files.
//!
//! Layout: `[data block]* [filter block] [index block] [stats block] [footer]`
//! Every block carries a trailer `[compression u8][crc32c u32]` where the
//! CRC covers payload + compression byte (0 = raw, 1 = lz4 with the
//! uncompressed size prepended). The footer is fixed-width:
//!
//! `[filter_off u64][filter_len u32][index_off u64][index_len u32]
//!  [stats_off u64][stats_len u32][format u32][magic u64]` (48 bytes)

pub(crate) mod builder;
pub(crate) mod reader;

pub(crate) use builder::TableBuilder;
pub(crate) use reader::{Table, TableIter};

use crate::coding::{crc32, Reader};
use crate::error::{corrupt, Result};
use crate::io::DbFile;

pub(crate) const MAGIC: u64 = 0xf115_e731_ab1e_0001;
/// Base format: every block stored raw (codec byte 0).
pub(crate) const FORMAT: u32 = 1;
/// At least one block is lz4-compressed (codec byte 1). Written only when a
/// table actually contains a compressed block, so stores that never enable
/// compression stay readable by format-1 binaries.
pub(crate) const FORMAT_COMPRESSED: u32 = 2;
pub(crate) const FOOTER_LEN: usize = 48;
pub(crate) const BLOCK_TRAILER_LEN: usize = 5;

/// Block codec bytes (the trailer's `compression u8`).
pub(crate) const CODEC_NONE: u8 = 0;
pub(crate) const CODEC_LZ4: u8 = 1;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockRef {
    pub off: u64,
    /// Stored length including the 5-byte trailer.
    pub len: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Footer {
    pub filter: BlockRef,
    pub index: BlockRef,
    pub stats: BlockRef,
    pub format: u32,
}

impl Footer {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(FOOTER_LEN);
        out.extend_from_slice(&self.filter.off.to_le_bytes());
        out.extend_from_slice(&self.filter.len.to_le_bytes());
        out.extend_from_slice(&self.index.off.to_le_bytes());
        out.extend_from_slice(&self.index.len.to_le_bytes());
        out.extend_from_slice(&self.stats.off.to_le_bytes());
        out.extend_from_slice(&self.stats.len.to_le_bytes());
        out.extend_from_slice(&self.format.to_le_bytes());
        out.extend_from_slice(&MAGIC.to_le_bytes());
        debug_assert_eq!(out.len(), FOOTER_LEN);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Footer> {
        let mut r = Reader::new(buf);
        let filter = BlockRef {
            off: r.u64()?,
            len: r.u32()?,
        };
        let index = BlockRef {
            off: r.u64()?,
            len: r.u32()?,
        };
        let stats = BlockRef {
            off: r.u64()?,
            len: r.u32()?,
        };
        let format = r.u32()?;
        let magic = r.u64()?;
        if magic != MAGIC {
            return Err(corrupt("bad table magic"));
        }
        if !(FORMAT..=FORMAT_COMPRESSED).contains(&format) {
            return Err(corrupt(format!("unsupported table format {format}")));
        }
        Ok(Footer {
            filter,
            index,
            stats,
            format,
        })
    }
}

/// Read a block (payload without trailer, decompressed) directly from the
/// file, verifying its CRC.
pub(crate) fn read_block_verified(file: &dyn DbFile, r: BlockRef) -> Result<Vec<u8>> {
    if (r.len as usize) < BLOCK_TRAILER_LEN {
        return Err(corrupt("block shorter than trailer"));
    }
    let mut buf = vec![0u8; r.len as usize];
    file.read_exact_at(r.off, &mut buf)?;
    let payload_end = buf.len() - 4;
    let stored_crc = u32::from_le_bytes(buf[payload_end..].try_into().unwrap());
    if crc32(&buf[..payload_end]) != stored_crc {
        return Err(corrupt("block crc mismatch"));
    }
    match buf[payload_end - 1] {
        CODEC_NONE => {
            buf.truncate(payload_end - 1);
            Ok(buf)
        }
        CODEC_LZ4 => lz4_flex::block::decompress_size_prepended(&buf[..payload_end - 1])
            .map_err(|e| corrupt(format!("lz4 block decode: {e}"))),
        codec => Err(corrupt(format!("unsupported compression {codec}"))),
    }
}

/// Per-table statistics persisted in the stats block.
#[derive(Debug, Clone, Default)]
pub(crate) struct TableStats {
    pub entries: u64,
    pub tombstones: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub first_ikey: Vec<u8>,
    pub last_ikey: Vec<u8>,
}

impl TableStats {
    pub fn encode(&self) -> Vec<u8> {
        use crate::coding::{put_len_prefixed, put_u64};
        let mut out = Vec::new();
        put_u64(&mut out, self.entries);
        put_u64(&mut out, self.tombstones);
        put_u64(&mut out, self.min_seq);
        put_u64(&mut out, self.max_seq);
        put_len_prefixed(&mut out, &self.first_ikey);
        put_len_prefixed(&mut out, &self.last_ikey);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<TableStats> {
        let mut r = Reader::new(buf);
        let stats = TableStats {
            entries: r.u64()?,
            tombstones: r.u64()?,
            min_seq: r.u64()?,
            max_seq: r.u64()?,
            first_ikey: r.len_prefixed()?.to_vec(),
            last_ikey: r.len_prefixed()?.to_vec(),
        };
        if stats.first_ikey.len() < crate::types::TRAILER_LEN
            || stats.last_ikey.len() < crate::types::TRAILER_LEN
        {
            return Err(corrupt("stats keys shorter than trailer"));
        }
        Ok(stats)
    }

    pub fn min_ukey(&self) -> &[u8] {
        crate::types::ikey_ukey(&self.first_ikey)
    }

    pub fn max_ukey(&self) -> &[u8] {
        crate::types::ikey_ukey(&self.last_ikey)
    }
}
