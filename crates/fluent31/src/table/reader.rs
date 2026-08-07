//! SSTable reader: pinned index + bloom, data blocks through the shared
//! block cache.

use std::sync::Arc;

use super::{read_block_verified, BlockRef, Footer, TableStats, FOOTER_LEN};
use crate::block::{Block, BlockIter};
use crate::bloom;
use crate::cache::BlockCache;
use crate::coding::Reader;
use crate::error::{corrupt, Result};
use crate::io::DbFile;
use crate::iter::InternalIterator;
use crate::types::{cmp_ikey, ikey_kind, ikey_seqno, ikey_ukey, make_seek_ikey, SeqNo, ValueKind};

/// The pinned block index, flat: one keys arena + one offsets vector instead of a
/// heap `Vec<u8>` per entry. The index lives for the table's whole life and there is
/// one entry per data block, so per-entry allocator overhead (Vec header + malloc
/// header + size-class rounding, ~60-80 bytes against ~40-byte keys) roughly doubled
/// its residency. Same lookups, half the heap, no per-entry allocations to churn.
struct TableIndex {
    /// Concatenated last-ikeys, in index order.
    keys: Vec<u8>,
    /// `ends[i]` = exclusive end of entry i's key in `keys` (entry i starts at `ends[i-1]`).
    ends: Vec<u32>,
    blocks: Vec<BlockRef>,
}

impl TableIndex {
    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn key(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.keys[start..self.ends[i] as usize]
    }

    fn block(&self, i: usize) -> BlockRef {
        self.blocks[i]
    }

    /// First entry whose key is `>= target` (the partition point of `< target`).
    fn lower_bound(&self, target: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp_ikey(self.key(mid), target) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

pub(crate) struct Table {
    pub id: u64,
    file: Arc<dyn DbFile>,
    cache: Arc<BlockCache>,
    index: TableIndex,
    /// Where the bloom filter lives in the file. The filter itself rides the shared
    /// block cache under `(id, filter.off)` like any data block: at ~10 bits/key it
    /// is the single largest pinned population on a big store (measured 2.76 GB
    /// across one production deployment's tables), it grows with data rather than
    /// load, and a cold table's filter is pure dead weight. Hot filters stay
    /// resident by being used; cold ones fall out with the LRU.
    filter: BlockRef,
    pub stats: TableStats,
}

impl Table {
    pub fn open(file: Arc<dyn DbFile>, id: u64, cache: Arc<BlockCache>) -> Result<Table> {
        let flen = file.len()?;
        if flen < FOOTER_LEN as u64 {
            return Err(corrupt("table smaller than footer"));
        }
        let mut fbuf = vec![0u8; FOOTER_LEN];
        file.read_exact_at(flen - FOOTER_LEN as u64, &mut fbuf)?;
        let footer = Footer::decode(&fbuf)?;

        // Verify the filter block is readable now (corruption should surface at open,
        // as it always has), but do not keep the bytes — queries reload it through the
        // block cache on demand.
        read_block_verified(file.as_ref(), footer.filter)?;
        let stats = TableStats::decode(&read_block_verified(file.as_ref(), footer.stats)?)?;

        let index_payload = read_block_verified(file.as_ref(), footer.index)?;
        let mut index = TableIndex {
            keys: Vec::new(),
            ends: Vec::new(),
            blocks: Vec::new(),
        };
        let mut r = Reader::new(&index_payload);
        while !r.is_empty() {
            let last_ikey = r.len_prefixed()?;
            if last_ikey.len() < crate::types::TRAILER_LEN {
                return Err(corrupt("index key shorter than trailer"));
            }
            let off = r.uvarint()?;
            let len = r.uvarint()?;
            index.keys.extend_from_slice(last_ikey);
            let end = u32::try_from(index.keys.len())
                .map_err(|_| corrupt("index keys exceed u32 arena"))?;
            index.ends.push(end);
            index.blocks.push(BlockRef {
                off,
                len: len as u32,
            });
        }
        if index.blocks.is_empty() {
            return Err(corrupt("table has no data blocks"));
        }
        index.keys.shrink_to_fit();
        Ok(Table {
            id,
            file,
            cache,
            index,
            filter: footer.filter,
            stats,
        })
    }

    #[allow(dead_code)] // debugging/inspection helper, kept intentionally
    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    /// Raw byte range of the table file (replication chunk serving); `len`
    /// is clamped to the file end.
    pub fn read_chunk(&self, off: u64, len: usize) -> Result<Vec<u8>> {
        let flen = self.file.len()?;
        if off >= flen {
            return Err(corrupt(format!(
                "chunk offset {off} beyond table end {flen}"
            )));
        }
        let n = (len as u64).min(flen - off) as usize;
        let mut buf = vec![0u8; n];
        self.file.read_exact_at(off, &mut buf)?;
        Ok(buf)
    }

    fn load_block(&self, idx: usize) -> Result<Arc<Block>> {
        let r = self.index.block(idx);
        let payload = match self.cache.get(self.id, r.off) {
            Some(p) => p,
            None => {
                let p = Arc::new(read_block_verified(self.file.as_ref(), r)?);
                self.cache.insert(self.id, r.off, p.clone());
                p
            }
        };
        Ok(Arc::new(Block::new(payload)?))
    }

    /// First block whose last key is `>= target` — the only block that can
    /// contain the lower bound for `target`.
    fn index_lower_bound(&self, target: &[u8]) -> usize {
        self.index.lower_bound(target)
    }

    fn load_filter(&self) -> Result<Arc<Vec<u8>>> {
        match self.cache.get(self.id, self.filter.off) {
            Some(p) => Ok(p),
            None => {
                let p = Arc::new(read_block_verified(self.file.as_ref(), self.filter)?);
                self.cache.insert(self.id, self.filter.off, p.clone());
                Ok(p)
            }
        }
    }

    /// Fallible since the filter rides the block cache: a miss re-reads it from
    /// the file, and that read can fail. The key-range check stays free.
    pub fn may_contain_ukey(&self, ukey: &[u8]) -> Result<bool> {
        if ukey < self.stats.min_ukey() || ukey > self.stats.max_ukey() {
            return Ok(false);
        }
        Ok(bloom::may_contain(
            &self.load_filter()?,
            bloom::hash64(ukey),
        ))
    }

    /// Newest version of `ukey` with `seqno <= seq` in this table.
    pub fn get(&self, ukey: &[u8], seq: SeqNo) -> Result<Option<(ValueKind, SeqNo, Vec<u8>)>> {
        if !self.may_contain_ukey(ukey)? {
            return Ok(None);
        }
        let target = make_seek_ikey(ukey, seq);
        let bi = self.index_lower_bound(&target);
        if bi >= self.index.len() {
            return Ok(None);
        }
        let block = self.load_block(bi)?;
        let i = block.lower_bound(&target)?;
        if i >= block.count() {
            return Ok(None);
        }
        let (ik, repr) = block.entry(i)?;
        if ikey_ukey(ik) != ukey {
            return Ok(None);
        }
        Ok(Some((ikey_kind(ik)?, ikey_seqno(ik), repr.to_vec())))
    }

    pub fn iter(self: &Arc<Self>) -> TableIter {
        TableIter {
            t: self.clone(),
            idx: 0,
            bi: None,
        }
    }
}

pub(crate) struct TableIter {
    t: Arc<Table>,
    idx: usize,
    bi: Option<BlockIter>,
}

impl TableIter {
    fn load(&mut self, idx: usize) -> Result<()> {
        self.idx = idx;
        self.bi = Some(BlockIter::new(self.t.load_block(idx)?));
        Ok(())
    }

    fn invalidate(&mut self) {
        self.bi = None;
    }
}

impl InternalIterator for TableIter {
    fn seek_to_first(&mut self) -> Result<()> {
        self.load(0)?;
        self.bi.as_mut().unwrap().seek_to_first()
    }

    fn seek_to_last(&mut self) -> Result<()> {
        let last = self.t.index.len() - 1;
        self.load(last)?;
        self.bi.as_mut().unwrap().seek_to_last()
    }

    fn seek(&mut self, ikey: &[u8]) -> Result<()> {
        let bi = self.t.index_lower_bound(ikey);
        if bi >= self.t.index.len() {
            self.invalidate();
            return Ok(());
        }
        self.load(bi)?;
        self.bi.as_mut().unwrap().seek(ikey)?;
        // last_ikey(bi) >= ikey guarantees a hit, but stay robust:
        if !self.bi.as_ref().unwrap().valid() {
            if bi + 1 < self.t.index.len() {
                self.load(bi + 1)?;
                self.bi.as_mut().unwrap().seek_to_first()?;
            } else {
                self.invalidate();
            }
        }
        Ok(())
    }

    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()> {
        let bi = self.t.index_lower_bound(ikey);
        if bi >= self.t.index.len() {
            // every entry < ikey: last entry of the table
            return self.seek_to_last();
        }
        self.load(bi)?;
        self.bi.as_mut().unwrap().seek_for_prev(ikey)?;
        if !self.bi.as_ref().unwrap().valid() {
            // every entry in this block > ikey; previous block (if any) is
            // entirely <= ikey.
            if bi == 0 {
                self.invalidate();
            } else {
                self.load(bi - 1)?;
                self.bi.as_mut().unwrap().seek_to_last()?;
            }
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.bi.as_ref().is_some_and(|b| b.valid())
    }

    fn next(&mut self) -> Result<()> {
        let bi = self.bi.as_mut().expect("valid");
        bi.next()?;
        if !bi.valid() {
            if self.idx + 1 < self.t.index.len() {
                let idx = self.idx + 1;
                self.load(idx)?;
                self.bi.as_mut().unwrap().seek_to_first()?;
            } else {
                self.invalidate();
            }
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        let bi = self.bi.as_mut().expect("valid");
        bi.prev()?;
        if !bi.valid() {
            if self.idx > 0 {
                let idx = self.idx - 1;
                self.load(idx)?;
                self.bi.as_mut().unwrap().seek_to_last()?;
            } else {
                self.invalidate();
            }
        }
        Ok(())
    }

    fn ikey(&self) -> &[u8] {
        self.bi.as_ref().expect("valid").ikey()
    }

    fn value(&self) -> &[u8] {
        self.bi.as_ref().expect("valid").value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, IoBackend};
    use crate::io::backend;
    use crate::table::{TableBuilder, FORMAT, FORMAT_COMPRESSED};
    use crate::types::{encode_inline, make_ikey, MAX_SEQNO};

    fn build_table_sized(
        entries: &[(&[u8], u64, ValueKind, &[u8])],
        block_size: usize,
        compression: Compression,
    ) -> (tempfile::TempDir, Arc<Table>, u64) {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        let path = dir.path().join("t");
        let f = io.create_new(&path).unwrap();
        let mut b = TableBuilder::new(f, block_size, 10, compression);
        for (k, s, kind, v) in entries {
            let repr = if *kind == ValueKind::Put {
                encode_inline(v)
            } else {
                Vec::new()
            };
            b.add(&make_ikey(k, *s, *kind), &repr).unwrap();
        }
        let (stats, size) = b.finish().unwrap();
        assert_eq!(stats.entries as usize, entries.len());
        let f = io.open_read(&path).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let t = Arc::new(Table::open(f, 1, cache).unwrap());
        (dir, t, size)
    }

    fn build_table(
        entries: &[(&[u8], u64, ValueKind, &[u8])],
        block_size: usize,
    ) -> (tempfile::TempDir, Arc<Table>) {
        let (dir, t, _) = build_table_sized(entries, block_size, Compression::None);
        (dir, t)
    }

    fn footer_format(t: &Table) -> u32 {
        let flen = t.file.len().unwrap();
        let mut fbuf = vec![0u8; FOOTER_LEN];
        t.file
            .read_exact_at(flen - FOOTER_LEN as u64, &mut fbuf)
            .unwrap();
        Footer::decode(&fbuf).unwrap().format
    }

    fn many() -> Vec<(Vec<u8>, u64, ValueKind, Vec<u8>)> {
        (0..500u32)
            .map(|i| {
                (
                    format!("key{i:05}").into_bytes(),
                    (i as u64) + 1,
                    if i % 7 == 0 {
                        ValueKind::Delete
                    } else {
                        ValueKind::Put
                    },
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect()
    }

    #[test]
    fn build_open_get_small_blocks() {
        let data = many();
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = data
            .iter()
            .map(|(k, s, kind, v)| (k.as_slice(), *s, *kind, v.as_slice()))
            .collect();
        // tiny blocks to force many of them
        let (_dir, t) = build_table(&refs, 256);
        assert!(t.block_count() > 10);

        for (k, s, kind, v) in &data {
            let got = t.get(k, MAX_SEQNO).unwrap().unwrap();
            assert_eq!(got.0, *kind);
            assert_eq!(got.1, *s);
            if *kind == ValueKind::Put {
                assert_eq!(got.2, encode_inline(v));
            }
        }
        assert!(t.get(b"key99999x", MAX_SEQNO).unwrap().is_none());
        // seq-bounded lookup: nothing visible below the entry's seqno
        assert!(t.get(b"key00010", 5).unwrap().is_none());
    }

    #[test]
    fn table_iter_forward_reverse_and_seeks() {
        let data = many();
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = data
            .iter()
            .map(|(k, s, kind, v)| (k.as_slice(), *s, *kind, v.as_slice()))
            .collect();
        let (_dir, t) = build_table(&refs, 256);

        let mut it = t.iter();
        it.seek_to_first().unwrap();
        let mut n = 0;
        let mut prev: Option<Vec<u8>> = None;
        while it.valid() {
            let k = it.ikey().to_vec();
            if let Some(p) = &prev {
                assert_eq!(cmp_ikey(p, &k), std::cmp::Ordering::Less);
            }
            prev = Some(k);
            n += 1;
            it.next().unwrap();
        }
        assert_eq!(n, 500);

        it.seek_to_last().unwrap();
        let mut m = 0;
        while it.valid() {
            m += 1;
            it.prev().unwrap();
        }
        assert_eq!(m, 500);

        // seek to a mid key
        it.seek(&make_seek_ikey(b"key00250", MAX_SEQNO)).unwrap();
        assert!(it.valid());
        assert_eq!(ikey_ukey(it.ikey()), b"key00250");

        // seek_for_prev between keys
        it.seek_for_prev(&make_seek_ikey(b"key00250a", MAX_SEQNO))
            .unwrap();
        assert!(it.valid());
        assert_eq!(ikey_ukey(it.ikey()), b"key00250");

        // seek beyond the end / before the start
        it.seek(&make_seek_ikey(b"zzz", MAX_SEQNO)).unwrap();
        assert!(!it.valid());
        it.seek_for_prev(&make_seek_ikey(b"aaa", MAX_SEQNO))
            .unwrap();
        assert!(!it.valid());
    }

    #[test]
    fn bloom_filters_absent_keys() {
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = vec![(b"only", 1, ValueKind::Put, b"v")];
        let (_dir, t) = build_table(&refs, 4096);
        assert!(t.may_contain_ukey(b"only").unwrap());
        assert!(!t.may_contain_ukey(b"absent").unwrap()); // outside key range
    }

    /// The filter rides the block cache: it must land there after use, and a query
    /// must still answer correctly after the cache forgets it (reload path).
    #[test]
    fn bloom_filter_rides_the_block_cache() {
        let data = many();
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = data
            .iter()
            .map(|(k, s, kind, v)| (k.as_slice(), *s, *kind, v.as_slice()))
            .collect();
        let (_dir, t) = build_table(&refs, 256);
        assert!(t.cache.get(t.id, t.filter.off).is_none(), "not pre-warmed");
        assert!(t.may_contain_ukey(b"key00042").unwrap());
        assert!(
            t.cache.get(t.id, t.filter.off).is_some(),
            "filter must be charged to the cache after use"
        );
        // Flood the 1 MiB test cache with far more junk than its capacity — without
        // touching the filter, so its LRU slot ages out in whichever shard holds it —
        // then query again: the filter must reload from the file and answer identically.
        for i in 0..2048u64 {
            t.cache.insert(u64::MAX, i, Arc::new(vec![0u8; 8 << 10]));
        }
        assert!(
            t.cache.get(t.id, t.filter.off).is_none(),
            "flood must evict the filter"
        );
        assert!(t.may_contain_ukey(b"key00042").unwrap());
        assert!(!t.may_contain_ukey(b"nope-not-here").unwrap());
    }

    /// Lz4 tables round-trip every read path, shrink the file, and carry the
    /// bumped format version.
    #[test]
    fn lz4_round_trip_and_shrinks() {
        let data = many();
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = data
            .iter()
            .map(|(k, s, kind, v)| (k.as_slice(), *s, *kind, v.as_slice()))
            .collect();
        let (_d1, plain, plain_size) = build_table_sized(&refs, 256, Compression::None);
        let (_d2, lz4, lz4_size) = build_table_sized(&refs, 256, Compression::Lz4);
        assert!(
            lz4_size < plain_size,
            "lz4 table ({lz4_size}) not smaller than raw ({plain_size})"
        );
        assert_eq!(footer_format(&plain), FORMAT);
        assert_eq!(footer_format(&lz4), FORMAT_COMPRESSED);

        for (k, s, kind, v) in &data {
            let got = lz4.get(k, MAX_SEQNO).unwrap().unwrap();
            assert_eq!(got.0, *kind);
            assert_eq!(got.1, *s);
            if *kind == ValueKind::Put {
                assert_eq!(got.2, encode_inline(v));
            }
        }
        assert!(lz4.get(b"key99999x", MAX_SEQNO).unwrap().is_none());

        let mut it = lz4.iter();
        it.seek_to_first().unwrap();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next().unwrap();
        }
        assert_eq!(n, 500);
    }

    /// A table written with compression enabled but where no block shrinks
    /// stays format 1 — readable by binaries that predate compression.
    #[test]
    fn incompressible_lz4_table_stays_format_1() {
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> = vec![(b"only", 1, ValueKind::Put, b"v")];
        let (_dir, t, _) = build_table_sized(&refs, 4096, Compression::Lz4);
        assert_eq!(footer_format(&t), FORMAT);
        let got = t.get(b"only", MAX_SEQNO).unwrap().unwrap();
        assert_eq!(got.2, encode_inline(b"v"));
    }
}
