//! Iterator stack: merge across sources in internal-key order, apply MVCC
//! visibility, resolve value-log pointers in batches.

use std::collections::{HashMap, VecDeque};

use std::sync::Arc;

use crate::cache::BlockCache;
use crate::error::{corrupt, Error, Result};
use crate::types::{
    decode_repr, ikey_kind, ikey_seqno, ikey_ukey, make_seek_ikey, ReprRef, SeqNo, ValueKind,
    ValuePtr, MAX_SEQNO,
};
use crate::version::ReadView;
use crate::vlog;

/// Cursor over internal-key/repr entries. All sources (memtable, SST block,
/// SST table, run, merge) speak this.
pub(crate) trait InternalIterator: Send {
    fn seek_to_first(&mut self) -> Result<()>;
    fn seek_to_last(&mut self) -> Result<()>;
    /// Position at the first entry `>= ikey`.
    fn seek(&mut self, ikey: &[u8]) -> Result<()>;
    /// Position at the last entry `<= ikey`.
    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()>;
    fn valid(&self) -> bool;
    fn next(&mut self) -> Result<()>;
    fn prev(&mut self) -> Result<()>;
    /// Requires `valid()`.
    fn ikey(&self) -> &[u8];
    /// Requires `valid()`.
    fn value(&self) -> &[u8];
}

/// K-way merge by linear scan over children — K is small (memtables + runs)
/// and this avoids self-referential heap keys. A given instance is used in
/// one direction only.
pub(crate) struct MergeIterator {
    children: Vec<Box<dyn InternalIterator>>,
    cur: Option<usize>,
    reverse: bool,
}

impl MergeIterator {
    pub fn new(children: Vec<Box<dyn InternalIterator>>, reverse: bool) -> Self {
        MergeIterator {
            children,
            cur: None,
            reverse,
        }
    }

    fn pick(&mut self) {
        let mut best: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            best = match best {
                None => Some(i),
                Some(b) => {
                    let ord = crate::types::cmp_ikey(c.ikey(), self.children[b].ikey());
                    let better = if self.reverse {
                        ord == std::cmp::Ordering::Greater
                    } else {
                        ord == std::cmp::Ordering::Less
                    };
                    if better {
                        Some(i)
                    } else {
                        Some(b)
                    }
                }
            };
        }
        self.cur = best;
    }
}

impl InternalIterator for MergeIterator {
    fn seek_to_first(&mut self) -> Result<()> {
        debug_assert!(!self.reverse);
        for c in &mut self.children {
            c.seek_to_first()?;
        }
        self.pick();
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<()> {
        debug_assert!(self.reverse);
        for c in &mut self.children {
            c.seek_to_last()?;
        }
        self.pick();
        Ok(())
    }

    fn seek(&mut self, ikey: &[u8]) -> Result<()> {
        debug_assert!(!self.reverse);
        for c in &mut self.children {
            c.seek(ikey)?;
        }
        self.pick();
        Ok(())
    }

    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()> {
        debug_assert!(self.reverse);
        for c in &mut self.children {
            c.seek_for_prev(ikey)?;
        }
        self.pick();
        Ok(())
    }

    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(!self.reverse);
        let i = self.cur.expect("valid");
        self.children[i].next()?;
        self.pick();
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        debug_assert!(self.reverse);
        let i = self.cur.expect("valid");
        self.children[i].prev()?;
        self.pick();
        Ok(())
    }

    fn ikey(&self) -> &[u8] {
        self.children[self.cur.expect("valid")].ikey()
    }

    fn value(&self) -> &[u8] {
        self.children[self.cur.expect("valid")].value()
    }
}

// ---------------------------------------------------------------------------
// MVCC visibility
// ---------------------------------------------------------------------------

/// Forward stream of the newest visible Put per user key within
/// `[lo, hi)` at snapshot `snap`.
pub(crate) struct MvccForward {
    it: MergeIterator,
    snap: SeqNo,
    hi: Option<Vec<u8>>,
    last_ukey: Option<Vec<u8>>,
    done: bool,
}

impl MvccForward {
    pub fn new(mut it: MergeIterator, snap: SeqNo, lo: &[u8], hi: Option<Vec<u8>>) -> Result<Self> {
        it.seek(&make_seek_ikey(lo, MAX_SEQNO))?;
        Ok(MvccForward {
            it,
            snap,
            hi,
            last_ukey: None,
            done: false,
        })
    }

    /// Next visible (user_key, repr) pair.
    pub fn next_visible(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.done {
            return Ok(None);
        }
        loop {
            if !self.it.valid() {
                self.done = true;
                return Ok(None);
            }
            let ik = self.it.ikey();
            let uk = ikey_ukey(ik);
            if let Some(hi) = &self.hi {
                if uk >= hi.as_slice() {
                    self.done = true;
                    return Ok(None);
                }
            }
            if self.last_ukey.as_deref() == Some(uk) {
                self.it.next()?;
                continue;
            }
            if ikey_seqno(ik) > self.snap {
                self.it.next()?;
                continue;
            }
            let kind = ikey_kind(ik)?;
            self.last_ukey = Some(uk.to_vec());
            if kind == ValueKind::Delete {
                self.it.next()?;
                continue;
            }
            let out = (uk.to_vec(), self.it.value().to_vec());
            self.it.next()?;
            return Ok(Some(out));
        }
    }
}

/// Reverse stream: newest visible Put per user key, descending user keys,
/// within `[lo, hi)`.
///
/// In reverse internal order a key's versions arrive oldest-first, so we keep
/// overwriting a candidate while `seqno <= snap` and emit it when the user
/// key changes underneath us.
pub(crate) struct MvccReverse {
    it: MergeIterator,
    snap: SeqNo,
    lo: Vec<u8>,
    group: Option<Vec<u8>>,
    /// Some(Some(v)) = visible Put; Some(None) = visible tombstone.
    cand: Option<Option<Vec<u8>>>,
    done: bool,
}

impl MvccReverse {
    pub fn new(
        mut it: MergeIterator,
        snap: SeqNo,
        lo: Vec<u8>,
        hi: Option<&[u8]>,
    ) -> Result<Self> {
        match hi {
            // Seek target (hi, MAX) sorts before every real entry of `hi`,
            // so seek_for_prev lands on the last entry with ukey < hi.
            Some(hi) => it.seek_for_prev(&make_seek_ikey(hi, MAX_SEQNO))?,
            None => it.seek_to_last()?,
        }
        Ok(MvccReverse {
            it,
            snap,
            lo,
            group: None,
            cand: None,
            done: false,
        })
    }

    fn take_emittable(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let g = self.group.take()?;
        match self.cand.take() {
            Some(Some(v)) => Some((g, v)),
            _ => None,
        }
    }

    pub fn next_visible(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.done {
            return Ok(None);
        }
        loop {
            if !self.it.valid() {
                self.done = true;
                return Ok(self.take_emittable());
            }
            let (uk, seq) = {
                let ik = self.it.ikey();
                (ikey_ukey(ik).to_vec(), ikey_seqno(ik))
            };
            if uk.as_slice() < self.lo.as_slice() {
                self.done = true;
                return Ok(self.take_emittable());
            }
            if self.group.as_deref() != Some(uk.as_slice()) {
                // crossing into a new (smaller) user key; flush the previous
                // group first if it produced a visible Put. Note: do not
                // advance the iterator — the current entry belongs to the new
                // group and is processed on the next call.
                let emit = self.take_emittable();
                self.group = Some(uk);
                self.cand = None;
                if emit.is_some() {
                    return Ok(emit);
                }
            }
            if seq <= self.snap {
                let kind = ikey_kind(self.it.ikey())?;
                self.cand = Some(if kind == ValueKind::Put {
                    Some(self.it.value().to_vec())
                } else {
                    None
                });
            }
            self.it.prev()?;
        }
    }
}

pub(crate) enum MvccIter {
    Fwd(MvccForward),
    Rev(MvccReverse),
}

impl MvccIter {
    fn next_visible(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self {
            MvccIter::Fwd(f) => f.next_visible(),
            MvccIter::Rev(r) => r.next_visible(),
        }
    }
}

// ---------------------------------------------------------------------------
// Public iterator with batched vlog resolution
// ---------------------------------------------------------------------------

/// Entries pulled ahead per fill; pointer loads for a window are grouped by
/// vlog file and issued as one batched read per file (one io_uring submission
/// on Linux).
const PREFETCH_ENTRIES: usize = 32;
const PREFETCH_BYTES: usize = 256 << 10;

enum Pending {
    Ready(Vec<u8>),
    Ptr(ValuePtr),
}

/// Ordered key-value iterator over a consistent snapshot of the database.
/// Yields owned pairs; errors end iteration.
pub struct DbIterator {
    mvcc: MvccIter,
    view: ReadView,
    cache: Arc<BlockCache>,
    ready: VecDeque<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
    errored: bool,
}

impl DbIterator {
    pub(crate) fn new(
        view: ReadView,
        cache: Arc<BlockCache>,
        snap: SeqNo,
        lo: &[u8],
        hi: Option<Vec<u8>>,
        reverse: bool,
    ) -> Result<DbIterator> {
        let children = view.internal_children();
        let merge = MergeIterator::new(children, reverse);
        let mvcc = if reverse {
            MvccIter::Rev(MvccReverse::new(merge, snap, lo.to_vec(), hi.as_deref())?)
        } else {
            MvccIter::Fwd(MvccForward::new(merge, snap, lo, hi)?)
        };
        Ok(DbIterator {
            mvcc,
            view,
            cache,
            ready: VecDeque::new(),
            exhausted: false,
            errored: false,
        })
    }

    fn fill(&mut self) -> Result<()> {
        if self.exhausted {
            return Ok(());
        }
        let mut window: Vec<(Vec<u8>, Pending)> = Vec::with_capacity(PREFETCH_ENTRIES);
        let mut bytes = 0usize;
        while window.len() < PREFETCH_ENTRIES && bytes < PREFETCH_BYTES {
            match self.mvcc.next_visible()? {
                None => {
                    self.exhausted = true;
                    break;
                }
                Some((key, repr)) => {
                    let pending = match decode_repr(&repr)? {
                        ReprRef::Inline(v) => {
                            bytes += key.len() + v.len();
                            Pending::Ready(v.to_vec())
                        }
                        ReprRef::Ptr(p) => {
                            bytes += key.len() + p.len as usize;
                            Pending::Ptr(p)
                        }
                    };
                    window.push((key, pending));
                }
            }
        }

        // Group unresolved pointers by vlog file; serve cache hits directly.
        let mut by_file: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, (key, pending)) in window.iter_mut().enumerate() {
            if let Pending::Ptr(p) = pending {
                if let Some(hit) = self.cache.get(p.file, p.offset) {
                    *pending = Pending::Ready(vlog::record_value_for(&hit, key)?);
                } else {
                    by_file.entry(p.file).or_default().push(i);
                }
            }
        }
        for (file_id, idxs) in by_file {
            let handle = self
                .view
                .version
                .vlogs
                .get(&file_id)
                .ok_or_else(|| corrupt(format!("pointer into unknown vlog file {file_id}")))?
                .clone();
            let items: Vec<(ValuePtr, &[u8])> = idxs
                .iter()
                .map(|&i| {
                    let (key, pending) = &window[i];
                    let Pending::Ptr(p) = pending else {
                        unreachable!()
                    };
                    (*p, key.as_slice())
                })
                .collect();
            let values = vlog::read_values_batch(&handle, &items)?;
            for (&i, v) in idxs.iter().zip(values) {
                window[i].1 = Pending::Ready(v);
            }
        }

        for (key, pending) in window {
            let Pending::Ready(v) = pending else {
                unreachable!()
            };
            self.ready.push_back((key, v));
        }
        Ok(())
    }
}

impl Iterator for DbIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        if self.ready.is_empty() {
            if let Err(e) = self.fill() {
                self.errored = true;
                return Some(Err(e));
            }
        }
        self.ready.pop_front().map(Ok)
    }
}

// keep Error in the public signature without unused-import churn
const _: fn(Error) -> Error = |e| e;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::types::{encode_inline, make_ikey};

    fn mt(entries: &[(&[u8], SeqNo, ValueKind, &[u8])]) -> Arc<Memtable> {
        let m = Arc::new(Memtable::new(0));
        for (k, s, kind, v) in entries {
            let repr = if *kind == ValueKind::Put {
                encode_inline(v)
            } else {
                Vec::new()
            };
            m.insert(make_ikey(k, *s, *kind), repr);
        }
        m
    }

    fn collect_fwd(
        sources: Vec<Arc<Memtable>>,
        snap: SeqNo,
        lo: &[u8],
        hi: Option<Vec<u8>>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let children: Vec<Box<dyn InternalIterator>> = sources
            .iter()
            .map(|m| Box::new(m.iter()) as Box<dyn InternalIterator>)
            .collect();
        let merge = MergeIterator::new(children, false);
        let mut f = MvccForward::new(merge, snap, lo, hi).unwrap();
        let mut out = Vec::new();
        while let Some((k, r)) = f.next_visible().unwrap() {
            let ReprRef::Inline(v) = decode_repr(&r).unwrap() else {
                panic!()
            };
            out.push((k, v.to_vec()));
        }
        out
    }

    fn collect_rev(
        sources: Vec<Arc<Memtable>>,
        snap: SeqNo,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let children: Vec<Box<dyn InternalIterator>> = sources
            .iter()
            .map(|m| Box::new(m.iter()) as Box<dyn InternalIterator>)
            .collect();
        let merge = MergeIterator::new(children, true);
        let mut f = MvccReverse::new(merge, snap, lo.to_vec(), hi).unwrap();
        let mut out = Vec::new();
        while let Some((k, r)) = f.next_visible().unwrap() {
            let ReprRef::Inline(v) = decode_repr(&r).unwrap() else {
                panic!()
            };
            out.push((k, v.to_vec()));
        }
        out
    }

    #[test]
    fn forward_visibility_and_tombstones() {
        let m1 = mt(&[
            (b"a", 10, ValueKind::Put, b"a10"),
            (b"b", 12, ValueKind::Delete, b""),
            (b"c", 5, ValueKind::Put, b"c5"),
        ]);
        let m2 = mt(&[
            (b"a", 3, ValueKind::Put, b"a3"),
            (b"b", 4, ValueKind::Put, b"b4"),
            (b"d", 20, ValueKind::Put, b"d20"),
        ]);
        // snap 11: a=a10, b=b4 (delete@12 invisible), c=c5; d@20 invisible
        let got = collect_fwd(vec![m1.clone(), m2.clone()], 11, b"", None);
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"a10".to_vec()),
                (b"b".to_vec(), b"b4".to_vec()),
                (b"c".to_vec(), b"c5".to_vec()),
            ]
        );
        // snap 12: b deleted
        let got = collect_fwd(vec![m1.clone(), m2.clone()], 12, b"", None);
        assert_eq!(
            got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
        // bounds [b, d)
        let got = collect_fwd(vec![m1, m2], 11, b"b", Some(b"d".to_vec()));
        assert_eq!(
            got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn reverse_matches_forward_reversed() {
        let m1 = mt(&[
            (b"a", 10, ValueKind::Put, b"a10"),
            (b"a", 2, ValueKind::Put, b"a2"),
            (b"b", 12, ValueKind::Delete, b""),
            (b"b", 4, ValueKind::Put, b"b4"),
            (b"c", 5, ValueKind::Put, b"c5"),
            (b"e", 7, ValueKind::Put, b"e7"),
        ]);
        for snap in [1u64, 3, 4, 5, 9, 11, 12, 100] {
            let mut fwd = collect_fwd(vec![m1.clone()], snap, b"", None);
            fwd.reverse();
            let rev = collect_rev(vec![m1.clone()], snap, b"", None);
            assert_eq!(fwd, rev, "snap={snap}");
        }
        // bounded reverse [b, e): includes c (and b when visible), not e
        let rev = collect_rev(vec![m1.clone()], 11, b"b", Some(b"e"));
        assert_eq!(
            rev.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![b"c".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn newest_source_wins_across_sources() {
        // same key in two sources at different seqnos
        let newer = mt(&[(b"k", 9, ValueKind::Put, b"new")]);
        let older = mt(&[(b"k", 3, ValueKind::Put, b"old")]);
        let got = collect_fwd(vec![newer, older], 100, b"", None);
        assert_eq!(got, vec![(b"k".to_vec(), b"new".to_vec())]);
    }
}
