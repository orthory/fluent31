//! Immutable snapshots of the LSM shape.
//!
//! A `Run` is a sequence of key-ordered, non-overlapping table *fragments*
//! (bounded-size files), which keeps per-file blooms/indexes small and bottom
//! merges resumable. A `Version` is the levels array plus the live value-log
//! file set; both tables and vlog files are lifetime-managed by Arc handles —
//! **file unlink happens exclusively in handle Drop** (after `mark_obsolete`),
//! so pinned versions (readers, forks) keep paths alive for hard-links.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::Result;
use crate::iter::InternalIterator;
use crate::memtable::Memtable;
use crate::table::{Table, TableIter};
use crate::types::{cmp_ikey, SeqNo, ValueKind, MAX_SEQNO};
use crate::vlog::VlogFileHandle;

pub(crate) struct TableHandle {
    pub id: u64,
    pub path: PathBuf,
    pub size: u64,
    pub table: Arc<Table>,
    obsolete: AtomicBool,
}

impl TableHandle {
    pub fn new(id: u64, path: PathBuf, size: u64, table: Table) -> Self {
        TableHandle {
            id,
            path,
            size,
            table: Arc::new(table),
            obsolete: AtomicBool::new(false),
        }
    }

    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for TableHandle {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A sorted run: fragments ordered by key, pairwise non-overlapping.
#[derive(Clone)]
pub(crate) struct Run {
    pub id: u64,
    pub tables: Vec<Arc<TableHandle>>,
}

impl Run {
    pub fn size(&self) -> u64 {
        self.tables.iter().map(|t| t.size).sum()
    }

    /// The single fragment that may contain `ukey`, if any.
    fn fragment_for(&self, ukey: &[u8]) -> Option<&Arc<TableHandle>> {
        let i = self
            .tables
            .partition_point(|t| t.table.stats.max_ukey() < ukey);
        let t = self.tables.get(i)?;
        (t.table.stats.min_ukey() <= ukey).then_some(t)
    }

    pub fn get(&self, ukey: &[u8], seq: SeqNo) -> Result<Option<(ValueKind, SeqNo, Vec<u8>)>> {
        match self.fragment_for(ukey) {
            Some(t) => t.table.get(ukey, seq),
            None => Ok(None),
        }
    }

    /// Can this run possibly contain `ukey`? Bloom-backed; false means
    /// provably absent (used by the tombstone-drop predicate).
    pub fn may_contain_ukey(&self, ukey: &[u8]) -> bool {
        self.fragment_for(ukey)
            .is_some_and(|t| t.table.may_contain_ukey(ukey))
    }

    pub fn iter(&self) -> RunIter {
        RunIter {
            tables: self.tables.clone(),
            idx: 0,
            ti: None,
        }
    }
}

/// Concatenating iterator over a run's fragments.
pub(crate) struct RunIter {
    tables: Vec<Arc<TableHandle>>,
    idx: usize,
    ti: Option<TableIter>,
}

impl RunIter {
    fn load(&mut self, idx: usize) -> &mut TableIter {
        self.idx = idx;
        self.ti = Some(self.tables[idx].table.iter());
        self.ti.as_mut().unwrap()
    }
}

impl InternalIterator for RunIter {
    fn seek_to_first(&mut self) -> Result<()> {
        if self.tables.is_empty() {
            self.ti = None;
            return Ok(());
        }
        self.load(0).seek_to_first()
    }

    fn seek_to_last(&mut self) -> Result<()> {
        if self.tables.is_empty() {
            self.ti = None;
            return Ok(());
        }
        let last = self.tables.len() - 1;
        self.load(last).seek_to_last()
    }

    fn seek(&mut self, ikey: &[u8]) -> Result<()> {
        let i = self.tables.partition_point(|t| {
            cmp_ikey(&t.table.stats.last_ikey, ikey) == std::cmp::Ordering::Less
        });
        if i >= self.tables.len() {
            self.ti = None;
            return Ok(());
        }
        self.load(i).seek(ikey)?;
        if !self.ti.as_ref().unwrap().valid() && i + 1 < self.tables.len() {
            self.load(i + 1).seek_to_first()?;
        }
        Ok(())
    }

    fn seek_for_prev(&mut self, ikey: &[u8]) -> Result<()> {
        let i = self.tables.partition_point(|t| {
            cmp_ikey(&t.table.stats.last_ikey, ikey) == std::cmp::Ordering::Less
        });
        if i >= self.tables.len() {
            return self.seek_to_last();
        }
        self.load(i).seek_for_prev(ikey)?;
        if !self.ti.as_ref().unwrap().valid() {
            if i == 0 {
                self.ti = None;
            } else {
                self.load(i - 1).seek_to_last()?;
            }
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.ti.as_ref().is_some_and(|t| t.valid())
    }

    fn next(&mut self) -> Result<()> {
        self.ti.as_mut().expect("valid").next()?;
        if !self.ti.as_ref().unwrap().valid() && self.idx + 1 < self.tables.len() {
            let idx = self.idx + 1;
            self.load(idx).seek_to_first()?;
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        self.ti.as_mut().expect("valid").prev()?;
        if !self.ti.as_ref().unwrap().valid() && self.idx > 0 {
            let idx = self.idx - 1;
            self.load(idx).seek_to_last()?;
        }
        Ok(())
    }

    fn ikey(&self) -> &[u8] {
        self.ti.as_ref().expect("valid").ikey()
    }

    fn value(&self) -> &[u8] {
        self.ti.as_ref().expect("valid").value()
    }
}

/// Immutable view of the tree structure. Levels hold runs newest-first.
pub(crate) struct Version {
    pub levels: Vec<Vec<Run>>,
    /// Every live vlog file (sealed + head), pinned by Arc.
    pub vlogs: BTreeMap<u64, Arc<VlogFileHandle>>,
    pub vlog_head_id: u64,
}

impl Version {
    pub fn empty(max_levels: usize) -> Self {
        Version {
            levels: vec![Vec::new(); max_levels],
            vlogs: BTreeMap::new(),
            vlog_head_id: 0,
        }
    }

    /// Structural clone (Arcs shared) for building a successor version.
    pub fn clone_shape(&self) -> Version {
        Version {
            levels: self.levels.clone(),
            vlogs: self.vlogs.clone(),
            vlog_head_id: self.vlog_head_id,
        }
    }

    pub fn runs_newest_first(&self) -> impl Iterator<Item = &Run> {
        self.levels.iter().flat_map(|l| l.iter())
    }

    pub fn get(&self, ukey: &[u8], seq: SeqNo) -> Result<Option<(ValueKind, SeqNo, Vec<u8>)>> {
        for run in self.runs_newest_first() {
            if let Some(hit) = run.get(ukey, seq)? {
                return Ok(Some(hit));
            }
        }
        Ok(None)
    }
}

/// A consistent read view: memtables + version pinned by Arc, plus the
/// visible seqno loaded *after* the Arcs were cloned (see DESIGN.md §6 —
/// this ordering is what makes unregistered reads GC-safe).
pub(crate) struct ReadView {
    pub mem: Arc<Memtable>,
    pub imms: Vec<Arc<Memtable>>,
    pub version: Arc<Version>,
    pub visible: SeqNo,
}

impl ReadView {
    /// Children for a merge iterator, newest source first (merge ties are
    /// impossible — seqnos are unique — but ordering keeps things sane).
    pub fn internal_children(&self) -> Vec<Box<dyn InternalIterator>> {
        let mut out: Vec<Box<dyn InternalIterator>> = Vec::new();
        out.push(Box::new(self.mem.iter()));
        for imm in &self.imms {
            out.push(Box::new(imm.iter()));
        }
        for run in self.version.runs_newest_first() {
            out.push(Box::new(run.iter()));
        }
        out
    }

    /// Newest version of `ukey` with `seqno <= seq` across the whole view.
    pub fn get_versioned(
        &self,
        ukey: &[u8],
        seq: SeqNo,
    ) -> Result<Option<(ValueKind, SeqNo, Vec<u8>)>> {
        if let Some(hit) = self.mem.get(ukey, seq) {
            return Ok(Some(hit));
        }
        for imm in &self.imms {
            if let Some(hit) = imm.get(ukey, seq) {
                return Ok(Some(hit));
            }
        }
        self.version.get(ukey, seq)
    }

    /// Newest committed (seqno, kind) for `ukey`, tombstones included — the
    /// OCC validation primitive (DESIGN.md §6).
    pub fn latest(&self, ukey: &[u8]) -> Result<Option<(SeqNo, ValueKind)>> {
        Ok(self
            .get_versioned(ukey, MAX_SEQNO)?
            .map(|(kind, seq, _)| (seq, kind)))
    }
}
