//! Optimistic transactions: snapshot reads, buffered writes, and
//! first-committer-wins validation.
//!
//! Commit takes the write mutex and performs validation AND application
//! inside that one critical section, so it is atomic against *every* writer
//! — other transactions and plain `db.put` alike. Validation checks the
//! newest committed version of each written (and `get_for_update`-ed) key
//! **including tombstones**; any version newer than the transaction's
//! snapshot aborts with `Error::Conflict`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use crate::batch::BatchOp;
use crate::config::SyncMode;
use crate::db::DbInner;
use crate::error::{Error, Result};
use crate::iter::DbIterator;
use crate::types::{validate_user_key, SeqNo};

pub struct Txn {
    db: Arc<DbInner>,
    snap: SeqNo,
    registered: bool,
    /// key -> Some(value) for puts, None for deletes.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Keys whose reads must still be the newest committed version at commit
    /// time (get_for_update).
    locks: BTreeSet<Vec<u8>>,
    bytes: usize,
    /// Engine-internal transaction (the trigger runner): its commit fires
    /// no triggers, so trigger invocations can never cascade.
    #[cfg(feature = "wasm")]
    system: bool,
}

impl Txn {
    pub(crate) fn new(db: Arc<DbInner>) -> Txn {
        let snap = db.register_snapshot();
        Txn {
            db,
            snap,
            registered: true,
            writes: BTreeMap::new(),
            locks: BTreeSet::new(),
            bytes: 0,
            #[cfg(feature = "wasm")]
            system: false,
        }
    }

    /// Mark this transaction as engine-internal: its commit generates no
    /// trigger events (the no-stacking rule).
    #[cfg(feature = "wasm")]
    pub(crate) fn mark_system(&mut self) {
        self.system = true;
    }

    /// Buffer a reserved-keyspace delete (trigger queue consumption).
    /// Bypasses user-key validation — engine-internal callers only. The key
    /// joins the commit's conflict set like any write, which is what makes
    /// consuming an event race-safe against a concurrent re-touch.
    #[cfg(feature = "wasm")]
    pub(crate) fn sys_delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.track_bytes(key.len())?;
        self.writes.insert(key, None);
        Ok(())
    }

    pub fn snapshot_seqno(&self) -> SeqNo {
        self.snap
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_user_key(key)?;
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        self.db.get_at_seq(key, self.snap)
    }

    /// Read a key and add it to the conflict set: commit fails if anyone
    /// else writes (or deletes) it after this transaction's snapshot.
    pub fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_user_key(key)?;
        self.locks.insert(key.to_vec());
        self.get(key)
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let (key, value) = (key.into(), value.into());
        validate_user_key(&key)?;
        if key.len() > self.db.opts.max_key_size || value.len() > self.db.opts.max_value_size {
            return Err(Error::InvalidArgument("key or value too large".into()));
        }
        self.track_bytes(key.len() + value.len())?;
        self.writes.insert(key, Some(value));
        Ok(())
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.into();
        validate_user_key(&key)?;
        self.track_bytes(key.len())?;
        self.writes.insert(key, None);
        Ok(())
    }

    fn track_bytes(&mut self, add: usize) -> Result<()> {
        self.bytes += add;
        if self.bytes > self.db.opts.max_txn_write_bytes {
            return Err(Error::InvalidArgument(
                "transaction write set exceeds max_txn_write_bytes".into(),
            ));
        }
        Ok(())
    }

    pub fn write_set_len(&self) -> usize {
        self.writes.len()
    }

    /// Iterator over `[lo, hi)` merging the snapshot with this transaction's
    /// own writes. The overlay is captured at creation: writes made after
    /// the iterator is opened are not observed by it.
    pub fn iter(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        reverse: bool,
    ) -> Result<TxnIter> {
        let base = self
            .db
            .iter_at_seq(Some(self.snap), lo, hi.map(|h| h.to_vec()), reverse)?;
        let lo_v = lo.map(|l| l.to_vec());
        let hi_v = hi.map(|h| h.to_vec());
        let mut overlay: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .writes
            .iter()
            .filter(|(k, _)| {
                lo_v.as_ref().is_none_or(|lo| *k >= lo)
                    && hi_v.as_ref().is_none_or(|hi| *k < hi)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if reverse {
            overlay.reverse();
        }
        Ok(TxnIter {
            base,
            base_peek: None,
            overlay: overlay.into(),
            reverse,
            done: false,
        })
    }

    /// Validate + apply atomically. Consumes the transaction; on `Conflict`
    /// nothing was written.
    ///
    /// Under `SyncMode::Always` the commit rides the group-commit queue:
    /// the committer performs the validation and the application in one
    /// `write_mu` critical section (checking the store AND the writes of
    /// earlier batches in the same fsync group), so concurrent transactions
    /// share fsyncs exactly like plain batch writers. Relaxed sync modes
    /// keep the direct path.
    pub fn commit(self) -> Result<()> {
        if self.writes.is_empty() && self.locks.is_empty() {
            return Ok(());
        }
        self.db.check_bg_error()?;
        // commits are writes: honor the same memtable/L0 backpressure as the
        // plain write path
        self.db.wait_for_space()?;
        let ops: Vec<BatchOp> = self
            .writes
            .iter()
            .map(|(k, v)| match v {
                Some(v) => BatchOp::Put {
                    key: k.clone(),
                    value: v.clone(),
                },
                None => BatchOp::Delete { key: k.clone() },
            })
            .collect();

        // Trigger events materialize inside the apply critical section and
        // ride the same batch as the writes that caused them (atomic
        // through WAL, memtable, and crash recovery). They are NOT
        // validation keys: the queue moving underneath us must not conflict
        // this commit. System transactions (the trigger runner itself)
        // never capture — the no-stacking rule.
        let capture = !self.system;

        if self.db.opts.sync == SyncMode::Always {
            let keys: Vec<Vec<u8>> = self
                .writes
                .keys()
                .chain(self.locks.iter())
                .cloned()
                .collect();
            self.db
                .queue_commit(ops, self.bytes, Some((self.snap, keys)), capture)
        } else {
            let mut ws = self.db.write_mu.lock();
            // validation inside the write mutex: no writer can slip a
            // version in between validation and application
            let view = self.db.read_view();
            let mut conflicted = false;
            for key in self.writes.keys().chain(self.locks.iter()) {
                if let Some((seq, _kind)) = view.latest(key)? {
                    if seq > self.snap {
                        conflicted = true;
                        break;
                    }
                }
            }
            if conflicted {
                Err(Error::Conflict)
            } else if !ops.is_empty() {
                self.db.apply_locked(&mut ws, &ops, capture)
            } else {
                Ok(())
            }
        }
    }

    /// Discard all buffered writes (also what `Drop` does).
    pub fn rollback(self) {}
}

impl Drop for Txn {
    fn drop(&mut self) {
        if self.registered {
            self.db.deregister_snapshot(self.snap);
            self.registered = false;
        }
    }
}

/// Merges the transaction's write overlay with a snapshot iterator.
pub struct TxnIter {
    base: DbIterator,
    base_peek: Option<(Vec<u8>, Vec<u8>)>,
    overlay: VecDeque<(Vec<u8>, Option<Vec<u8>>)>,
    reverse: bool,
    done: bool,
}

impl TxnIter {
    fn fill_base(&mut self) -> Result<()> {
        if self.base_peek.is_none() {
            self.base_peek = match self.base.next() {
                None => None,
                Some(Ok(kv)) => Some(kv),
                Some(Err(e)) => return Err(e),
            };
        }
        Ok(())
    }
}

impl Iterator for TxnIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Err(e) = self.fill_base() {
                self.done = true;
                return Some(Err(e));
            }
            let use_overlay = match (self.overlay.front(), &self.base_peek) {
                (None, None) => {
                    self.done = true;
                    return None;
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((ok, _)), Some((bk, _))) => {
                    if self.reverse {
                        ok >= bk
                    } else {
                        ok <= bk
                    }
                }
            };
            if use_overlay {
                let (ok, ov) = self.overlay.pop_front().unwrap();
                // overlay shadows an equal base key
                if self
                    .base_peek
                    .as_ref()
                    .is_some_and(|(bk, _)| *bk == ok)
                {
                    self.base_peek = None;
                }
                match ov {
                    Some(v) => return Some(Ok((ok, v))),
                    None => continue, // overlay delete hides the key
                }
            } else {
                return Some(Ok(self.base_peek.take().unwrap()));
            }
        }
    }
}
