//! SST data block: a flat run of entries plus an offset array enabling
//! in-block binary search.
//!
//! Payload layout:
//! `([iklen varint][rlen varint][ikey][repr])*  [u32 entry_off]* [u32 count]`

use std::sync::Arc;

use crate::coding::{put_u32, put_uvarint, Reader};
use crate::error::{corrupt, Result};
use crate::iter::InternalIterator;
use crate::types::cmp_ikey;

#[derive(Default)]
pub(crate) struct BlockBuilder {
    buf: Vec<u8>,
    offsets: Vec<u32>,
}

impl BlockBuilder {
    pub fn add(&mut self, ikey: &[u8], repr: &[u8]) {
        self.offsets.push(self.buf.len() as u32);
        put_uvarint(&mut self.buf, ikey.len() as u64);
        put_uvarint(&mut self.buf, repr.len() as u64);
        self.buf.extend_from_slice(ikey);
        self.buf.extend_from_slice(repr);
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn size_estimate(&self) -> usize {
        self.buf.len() + self.offsets.len() * 4 + 4
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for off in &self.offsets {
            put_u32(&mut out, *off);
        }
        put_u32(&mut out, self.offsets.len() as u32);
        self.offsets.clear();
        out
    }
}

/// Parsed view over an immutable block payload.
pub(crate) struct Block {
    data: Arc<Vec<u8>>,
    /// Byte range of the entry region (offset array excluded).
    entries_end: usize,
    count: usize,
}

impl Block {
    pub fn new(data: Arc<Vec<u8>>) -> Result<Self> {
        if data.len() < 4 {
            return Err(corrupt("block too small"));
        }
        let count =
            u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let array_len = count
            .checked_mul(4)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| corrupt("block count overflow"))?;
        if array_len > data.len() {
            return Err(corrupt("block offset array beyond payload"));
        }
        let entries_end = data.len() - array_len;
        Ok(Block {
            data,
            entries_end,
            count,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    fn entry_off(&self, i: usize) -> usize {
        let at = self.entries_end + i * 4;
        u32::from_le_bytes(self.data[at..at + 4].try_into().unwrap()) as usize
    }

    /// (internal_key, repr) of entry `i`.
    pub fn entry(&self, i: usize) -> Result<(&[u8], &[u8])> {
        debug_assert!(i < self.count);
        let off = self.entry_off(i);
        if off >= self.entries_end {
            return Err(corrupt("entry offset beyond entry region"));
        }
        let mut r = Reader::new(&self.data[off..self.entries_end]);
        let iklen = r.uvarint()? as usize;
        let rlen = r.uvarint()? as usize;
        if iklen < crate::types::TRAILER_LEN {
            // file-sourced key too short to carry a trailer: corrupt data,
            // not a panic in the trailer arithmetic
            return Err(corrupt("internal key shorter than trailer"));
        }
        let ikey = r.bytes(iklen)?;
        let repr = r.bytes(rlen)?;
        Ok((ikey, repr))
    }

    /// Index of the first entry with `ikey >= target` (== count when none).
    pub fn lower_bound(&self, target: &[u8]) -> Result<usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (k, _) = self.entry(mid)?;
            if cmp_ikey(k, target) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }
}

/// Iterator over one block; position is an index into the offset array.
pub(crate) struct BlockIter {
    block: Arc<Block>,
    /// `count` means invalid.
    pos: usize,
}

impl BlockIter {
    pub fn new(block: Arc<Block>) -> Self {
        let pos = block.count(); // start invalid
        BlockIter { block, pos }
    }
}

impl InternalIterator for BlockIter {
    fn seek_to_first(&mut self) -> Result<()> {
        self.pos = 0;
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<()> {
        self.pos = if self.block.count() == 0 {
            0
        } else {
            self.block.count() - 1
        };
        Ok(())
    }

    fn seek(&mut self, ikey: &[u8]) -> Result<()> {
        self.pos = self.block.lower_bound(ikey)?;
        Ok(())
    }

    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()> {
        // last entry <= ikey == (first entry > ikey) - 1; entries are unique
        // so first-entry->=-then-adjust works: if lb points at an entry equal
        // to ikey keep it, else step back.
        let lb = self.block.lower_bound(ikey)?;
        if lb < self.block.count() {
            let (k, _) = self.block.entry(lb)?;
            if cmp_ikey(k, ikey) == std::cmp::Ordering::Equal {
                self.pos = lb;
                return Ok(());
            }
        }
        self.pos = if lb == 0 { self.block.count() } else { lb - 1 };
        Ok(())
    }

    fn valid(&self) -> bool {
        self.pos < self.block.count()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.valid());
        self.pos += 1;
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        debug_assert!(self.valid());
        self.pos = if self.pos == 0 {
            self.block.count()
        } else {
            self.pos - 1
        };
        Ok(())
    }

    fn ikey(&self) -> &[u8] {
        self.block.entry(self.pos).expect("validated entry").0
    }

    fn value(&self) -> &[u8] {
        self.block.entry(self.pos).expect("validated entry").1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{make_ikey, make_seek_ikey, ValueKind, MAX_SEQNO};

    fn build(keys: &[(&[u8], u64)]) -> Arc<Block> {
        let mut b = BlockBuilder::default();
        for (k, s) in keys {
            b.add(&make_ikey(k, *s, ValueKind::Put), b"v");
        }
        Arc::new(Block::new(Arc::new(b.finish())).unwrap())
    }

    #[test]
    fn build_and_search() {
        let block = build(&[(b"a", 9), (b"a", 5), (b"c", 7), (b"e", 1)]);
        assert_eq!(block.count(), 4);
        // seek (a, 6) -> entry (a,5)
        let i = block.lower_bound(&make_seek_ikey(b"a", 6)).unwrap();
        let (k, _) = block.entry(i).unwrap();
        assert_eq!(crate::types::ikey_seqno(k), 5);
        // seek (b, max) -> first c entry
        let i = block.lower_bound(&make_seek_ikey(b"b", MAX_SEQNO)).unwrap();
        let (k, _) = block.entry(i).unwrap();
        assert_eq!(crate::types::ikey_ukey(k), b"c");
        // beyond the end
        let i = block.lower_bound(&make_seek_ikey(b"z", MAX_SEQNO)).unwrap();
        assert_eq!(i, 4);
    }

    #[test]
    fn iter_forward_and_reverse() {
        let block = build(&[(b"a", 2), (b"b", 2), (b"c", 2)]);
        let mut it = BlockIter::new(block);
        it.seek_to_first().unwrap();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(crate::types::ikey_ukey(it.ikey()).to_vec());
            it.next().unwrap();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        it.seek_for_prev(&make_seek_ikey(b"bb", MAX_SEQNO)).unwrap();
        assert_eq!(crate::types::ikey_ukey(it.ikey()), b"b");
        it.prev().unwrap();
        assert_eq!(crate::types::ikey_ukey(it.ikey()), b"a");
        it.prev().unwrap();
        assert!(!it.valid());
    }

    #[test]
    fn empty_block() {
        let mut b = BlockBuilder::default();
        let block = Arc::new(Block::new(Arc::new(b.finish())).unwrap());
        assert_eq!(block.count(), 0);
        let mut it = BlockIter::new(block);
        it.seek_to_first().unwrap();
        assert!(!it.valid());
    }
}
