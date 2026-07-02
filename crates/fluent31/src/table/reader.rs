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
use crate::types::{
    cmp_ikey, ikey_kind, ikey_seqno, ikey_ukey, make_seek_ikey, SeqNo, ValueKind,
};

struct IndexEntry {
    last_ikey: Vec<u8>,
    block: BlockRef,
}

pub(crate) struct Table {
    pub id: u64,
    file: Arc<dyn DbFile>,
    cache: Arc<BlockCache>,
    index: Vec<IndexEntry>,
    filter: Vec<u8>,
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

        let filter = read_block_verified(file.as_ref(), footer.filter)?;
        let stats = TableStats::decode(&read_block_verified(file.as_ref(), footer.stats)?)?;

        let index_payload = read_block_verified(file.as_ref(), footer.index)?;
        let mut index = Vec::new();
        let mut r = Reader::new(&index_payload);
        while !r.is_empty() {
            let last_ikey = r.len_prefixed()?.to_vec();
            if last_ikey.len() < crate::types::TRAILER_LEN {
                return Err(corrupt("index key shorter than trailer"));
            }
            let off = r.uvarint()?;
            let len = r.uvarint()?;
            index.push(IndexEntry {
                last_ikey,
                block: BlockRef {
                    off,
                    len: len as u32,
                },
            });
        }
        if index.is_empty() {
            return Err(corrupt("table has no data blocks"));
        }
        Ok(Table {
            id,
            file,
            cache,
            index,
            filter,
            stats,
        })
    }

    #[allow(dead_code)] // debugging/inspection helper, kept intentionally
    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    fn load_block(&self, idx: usize) -> Result<Arc<Block>> {
        let r = self.index[idx].block;
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
        self.index
            .partition_point(|e| cmp_ikey(&e.last_ikey, target) == std::cmp::Ordering::Less)
    }

    pub fn may_contain_ukey(&self, ukey: &[u8]) -> bool {
        if ukey < self.stats.min_ukey() || ukey > self.stats.max_ukey() {
            return false;
        }
        bloom::may_contain(&self.filter, bloom::hash64(ukey))
    }

    /// Newest version of `ukey` with `seqno <= seq` in this table.
    pub fn get(&self, ukey: &[u8], seq: SeqNo) -> Result<Option<(ValueKind, SeqNo, Vec<u8>)>> {
        if !self.may_contain_ukey(ukey) {
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
    use crate::config::IoBackend;
    use crate::io::backend;
    use crate::table::TableBuilder;
    use crate::types::{encode_inline, make_ikey, MAX_SEQNO};

    fn build_table(
        entries: &[(&[u8], u64, ValueKind, &[u8])],
        block_size: usize,
    ) -> (tempfile::TempDir, Arc<Table>) {
        let dir = tempfile::tempdir().unwrap();
        let (io, _) = backend(IoBackend::Std).unwrap();
        let path = dir.path().join("t");
        let f = io.create_new(&path).unwrap();
        let mut b = TableBuilder::new(f, block_size, 10);
        for (k, s, kind, v) in entries {
            let repr = if *kind == ValueKind::Put {
                encode_inline(v)
            } else {
                Vec::new()
            };
            b.add(&make_ikey(k, *s, *kind), &repr).unwrap();
        }
        let (stats, _) = b.finish().unwrap();
        assert_eq!(stats.entries as usize, entries.len());
        let f = io.open_read(&path).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let t = Arc::new(Table::open(f, 1, cache).unwrap());
        (dir, t)
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
        it.seek_for_prev(&make_seek_ikey(b"aaa", MAX_SEQNO)).unwrap();
        assert!(!it.valid());
    }

    #[test]
    fn bloom_filters_absent_keys() {
        let refs: Vec<(&[u8], u64, ValueKind, &[u8])> =
            vec![(b"only", 1, ValueKind::Put, b"v")];
        let (_dir, t) = build_table(&refs, 4096);
        assert!(t.may_contain_ukey(b"only"));
        assert!(!t.may_contain_ukey(b"absent")); // outside key range
    }
}
