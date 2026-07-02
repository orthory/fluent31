//! In-memory write buffer: a concurrent skiplist ordered by internal key.

use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use crate::error::Result;
use crate::iter::InternalIterator;
use crate::types::{
    ikey_kind, ikey_seqno, ikey_ukey, make_seek_ikey, InternalKey, SeqNo, ValueKind,
};

/// Rough per-entry bookkeeping overhead added to key+value bytes when
/// tracking memtable size (skiplist node, tower pointers, allocation slop).
const ENTRY_OVERHEAD: usize = 64;

pub(crate) struct Memtable {
    map: SkipMap<InternalKey, Vec<u8>>,
    bytes: AtomicUsize,
    /// The WAL file backing this memtable (1:1).
    pub wal_id: u64,
}

impl Memtable {
    pub fn new(wal_id: u64) -> Self {
        Memtable {
            map: SkipMap::new(),
            bytes: AtomicUsize::new(0),
            wal_id,
        }
    }

    pub fn insert(&self, ikey: Vec<u8>, repr: Vec<u8>) {
        let sz = ikey.len() + repr.len() + ENTRY_OVERHEAD;
        self.map.insert(InternalKey(ikey), repr);
        self.bytes.fetch_add(sz, Ordering::Relaxed);
    }

    pub fn approximate_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Newest version of `ukey` with `seqno <= seq`, if any.
    pub fn get(&self, ukey: &[u8], seq: SeqNo) -> Option<(ValueKind, SeqNo, Vec<u8>)> {
        let target = InternalKey(make_seek_ikey(ukey, seq));
        let entry = self.map.lower_bound(Bound::Included(&target))?;
        let ik = &entry.key().0;
        if ikey_ukey(ik) != ukey {
            return None;
        }
        let kind = ikey_kind(ik).ok()?;
        Some((kind, ikey_seqno(ik), entry.value().clone()))
    }

    pub fn iter(self: &Arc<Self>) -> MemIter {
        MemIter {
            cur: None,
            mt: self.clone(),
        }
    }
}

type StaticEntry = Entry<'static, InternalKey, Vec<u8>>;

/// Owning memtable iterator.
///
/// SAFETY: `cur` borrows `mt`'s skiplist but is stored with a forged 'static
/// lifetime. This is sound because (a) `mt` is an Arc kept alive by this
/// struct, (b) `cur` is declared before `mt` so it drops first, and (c) no
/// entry (or borrow of it) with the forged lifetime is ever handed out —
/// `ikey`/`value` return slices reborrowed at `&self`'s lifetime.
pub(crate) struct MemIter {
    cur: Option<StaticEntry>,
    mt: Arc<Memtable>,
}

impl MemIter {
    /// Associated fn (not a method) so the returned 'static entry carries no
    /// borrow of `self`, letting callers assign it into `self.cur`.
    /// SAFETY: see struct docs.
    fn grab(e: Option<Entry<'_, InternalKey, Vec<u8>>>) -> Option<StaticEntry> {
        e.map(|e| unsafe {
            std::mem::transmute::<Entry<'_, InternalKey, Vec<u8>>, StaticEntry>(e)
        })
    }
}

impl InternalIterator for MemIter {
    fn seek_to_first(&mut self) -> Result<()> {
        self.cur = Self::grab(self.mt.map.front());
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<()> {
        self.cur = Self::grab(self.mt.map.back());
        Ok(())
    }

    fn seek(&mut self, ikey: &[u8]) -> Result<()> {
        let target = InternalKey(ikey.to_vec());
        self.cur = Self::grab(self.mt.map.lower_bound(Bound::Included(&target)));
        Ok(())
    }

    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()> {
        let target = InternalKey(ikey.to_vec());
        self.cur = Self::grab(self.mt.map.upper_bound(Bound::Included(&target)));
        Ok(())
    }

    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.cur = Self::grab(self.cur.as_ref().and_then(|c| c.next()));
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        self.cur = Self::grab(self.cur.as_ref().and_then(|c| c.prev()));
        Ok(())
    }

    fn ikey(&self) -> &[u8] {
        &self.cur.as_ref().expect("valid").key().0
    }

    fn value(&self) -> &[u8] {
        self.cur.as_ref().expect("valid").value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{encode_inline, make_ikey, MAX_SEQNO};

    fn mt_with(entries: &[(&[u8], SeqNo, ValueKind, &[u8])]) -> Arc<Memtable> {
        let mt = Arc::new(Memtable::new(0));
        for (k, s, kind, v) in entries {
            let repr = if *kind == ValueKind::Put {
                encode_inline(v)
            } else {
                Vec::new()
            };
            mt.insert(make_ikey(k, *s, *kind), repr);
        }
        mt
    }

    #[test]
    fn get_respects_snapshot() {
        let mt = mt_with(&[
            (b"k", 3, ValueKind::Put, b"v3"),
            (b"k", 7, ValueKind::Put, b"v7"),
            (b"k", 9, ValueKind::Delete, b""),
        ]);
        let (kind, seq, repr) = mt.get(b"k", 8).unwrap();
        assert_eq!((kind, seq), (ValueKind::Put, 7));
        assert_eq!(repr, encode_inline(b"v7"));
        let (kind, seq, _) = mt.get(b"k", MAX_SEQNO).unwrap();
        assert_eq!((kind, seq), (ValueKind::Delete, 9));
        let (_, seq, _) = mt.get(b"k", 3).unwrap();
        assert_eq!(seq, 3);
        assert!(mt.get(b"k", 2).is_none());
        assert!(mt.get(b"other", 100).is_none());
    }

    #[test]
    fn iter_orders_by_key_then_seq_desc() {
        let mt = mt_with(&[
            (b"a", 1, ValueKind::Put, b"x"),
            (b"b", 5, ValueKind::Put, b"y1"),
            (b"b", 2, ValueKind::Put, b"y2"),
        ]);
        let mut it = mt.iter();
        it.seek_to_first().unwrap();
        let mut seen = Vec::new();
        while it.valid() {
            seen.push((ikey_ukey(it.ikey()).to_vec(), ikey_seqno(it.ikey())));
            it.next().unwrap();
        }
        assert_eq!(
            seen,
            vec![
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 5),
                (b"b".to_vec(), 2)
            ]
        );
        // reverse from the end
        it.seek_to_last().unwrap();
        assert_eq!(ikey_seqno(it.ikey()), 2);
        it.prev().unwrap();
        assert_eq!(ikey_seqno(it.ikey()), 5);
    }

    #[test]
    fn seek_for_prev_lands_at_or_before() {
        let mt = mt_with(&[
            (b"a", 1, ValueKind::Put, b"x"),
            (b"c", 1, ValueKind::Put, b"z"),
        ]);
        let mut it = mt.iter();
        it.seek_for_prev(&make_seek_ikey(b"b", MAX_SEQNO)).unwrap();
        assert!(it.valid());
        assert_eq!(ikey_ukey(it.ikey()), b"a");
    }
}
