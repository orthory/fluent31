//! Streaming SSTable writer. Entries must arrive in internal-key order.

use std::sync::Arc;

use super::{BlockRef, Footer, TableStats, CODEC_LZ4, CODEC_NONE, FORMAT, FORMAT_COMPRESSED};
use crate::block::BlockBuilder;
use crate::bloom;
use crate::coding::{crc32, put_len_prefixed, put_uvarint};
use crate::config::Compression;
use crate::error::Result;
use crate::io::DbFile;
use crate::types::{ikey_kind, ikey_seqno, ikey_ukey, ValueKind};

pub(crate) struct TableBuilder {
    file: Arc<dyn DbFile>,
    block_size: usize,
    bloom_bits_per_key: usize,
    compression: Compression,

    block: BlockBuilder,
    /// (last internal key of block, block ref)
    index: Vec<(Vec<u8>, BlockRef)>,
    offset: u64,
    /// Whether any block was actually stored compressed — gates the footer
    /// format bump (old readers keep opening tables that stayed all-raw).
    wrote_compressed: bool,

    key_hashes: Vec<u64>,
    last_hashed_ukey: Vec<u8>,

    stats: TableStats,
    started: bool,
}

impl TableBuilder {
    pub fn new(
        file: Arc<dyn DbFile>,
        block_size: usize,
        bloom_bits_per_key: usize,
        compression: Compression,
    ) -> Self {
        TableBuilder {
            file,
            block_size,
            bloom_bits_per_key,
            compression,
            block: BlockBuilder::default(),
            index: Vec::new(),
            offset: 0,
            wrote_compressed: false,
            key_hashes: Vec::new(),
            last_hashed_ukey: Vec::new(),
            stats: TableStats {
                min_seq: u64::MAX,
                ..Default::default()
            },
            started: false,
        }
    }

    /// Bytes written to disk so far plus the in-progress block — used by
    /// compaction to split output into bounded fragments.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.block.size_estimate() as u64
    }

    /// User key of the most recently added entry (empty before the first
    /// add). Compaction splits fragments only at user-key boundaries.
    pub fn last_ukey(&self) -> &[u8] {
        if self.started {
            ikey_ukey(&self.stats.last_ikey)
        } else {
            &[]
        }
    }

    pub fn add(&mut self, ikey: &[u8], repr: &[u8]) -> Result<()> {
        debug_assert!(
            !self.started
                || crate::types::cmp_ikey(&self.stats.last_ikey, ikey)
                    == std::cmp::Ordering::Less,
            "keys must be added in strictly increasing internal-key order"
        );
        if !self.started {
            self.stats.first_ikey = ikey.to_vec();
            self.started = true;
        }
        self.stats.last_ikey = ikey.to_vec();
        self.stats.entries += 1;
        if ikey_kind(ikey)? == ValueKind::Delete {
            self.stats.tombstones += 1;
        }
        let seq = ikey_seqno(ikey);
        self.stats.min_seq = self.stats.min_seq.min(seq);
        self.stats.max_seq = self.stats.max_seq.max(seq);

        let ukey = ikey_ukey(ikey);
        if self.key_hashes.is_empty() || self.last_hashed_ukey != ukey {
            self.key_hashes.push(bloom::hash64(ukey));
            self.last_hashed_ukey = ukey.to_vec();
        }

        self.block.add(ikey, repr);
        if self.block.size_estimate() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn write_block(&mut self, payload: Vec<u8>, compression: Compression) -> Result<BlockRef> {
        let (codec, mut buf) = match compression {
            Compression::Lz4 => {
                let compressed = lz4_flex::block::compress_prepend_size(&payload);
                if compressed.len() < payload.len() {
                    (CODEC_LZ4, compressed)
                } else {
                    // incompressible: store raw so a block never grows; the
                    // codec byte keeps each block self-describing
                    (CODEC_NONE, payload)
                }
            }
            Compression::None => (CODEC_NONE, payload),
        };
        self.wrote_compressed |= codec != CODEC_NONE;
        buf.push(codec);
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let off = self.file.append(&buf)?;
        debug_assert_eq!(off, self.offset);
        self.offset += buf.len() as u64;
        Ok(BlockRef {
            off,
            len: buf.len() as u32,
        })
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let last_ikey = self.stats.last_ikey.clone();
        let payload = self.block.finish();
        let r = self.write_block(payload, self.compression)?;
        self.index.push((last_ikey, r));
        Ok(())
    }

    /// Finalize: filter + index + stats + footer, then fdatasync. Returns the
    /// stats and total file size. The table must contain at least one entry.
    pub fn finish(mut self) -> Result<(TableStats, u64)> {
        assert!(self.started, "cannot finish an empty table");
        self.flush_block()?;

        // Only the key-bearing payloads (data + index) go through the
        // configured codec: bloom filters are high-entropy by construction,
        // and the stats block is ~50 bytes of metadata — "compressing" either
        // would bump the table format for no real win.
        let filter = bloom::build(&self.key_hashes, self.bloom_bits_per_key);
        let filter_ref = self.write_block(filter, Compression::None)?;

        let mut index_payload = Vec::new();
        for (last_ikey, r) in &self.index {
            put_len_prefixed(&mut index_payload, last_ikey);
            put_uvarint(&mut index_payload, r.off);
            put_uvarint(&mut index_payload, u64::from(r.len));
        }
        let index_ref = self.write_block(index_payload, self.compression)?;

        let stats_ref = self.write_block(self.stats.encode(), Compression::None)?;

        let footer = Footer {
            filter: filter_ref,
            index: index_ref,
            stats: stats_ref,
            format: if self.wrote_compressed {
                FORMAT_COMPRESSED
            } else {
                FORMAT
            },
        };
        self.file.append(&footer.encode())?;
        self.offset += super::FOOTER_LEN as u64;

        // Durability: the table's contents must be stable before any manifest
        // references it (DESIGN.md §5).
        self.file.sync_data()?;
        Ok((self.stats, self.offset))
    }
}
