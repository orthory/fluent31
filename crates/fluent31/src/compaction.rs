//! Background structure maintenance: lazy-leveling compaction, value-log GC,
//! and retired-victim reclamation.
//!
//! Lazy leveling: levels 0..last are tiered — when a level accumulates
//! `tier_width` runs (l0 has its own trigger) ALL of its runs merge into one
//! run placed at the FRONT (newest position) of the next level. The last
//! level is leveled, maintained INCREMENTALLY: one newer run at a time
//! merges into the base run, touching only the base tables its key range
//! overlaps — untouched fragments are spliced through by identity. When the
//! bottom outgrows its byte budget (`level_target_bytes`) the tree deepens by
//! moving its runs to a new level without rewriting their tables.
//!
//! One compaction executor owns the picker. Rewrite jobs run in bounded input
//! slices; after every slice the scheduler may suspend the job and service any
//! newly eligible higher level. Installation remains atomic at job completion,
//! and removes exactly the pinned inputs, so flushes and higher-priority output
//! installed while a job is suspended are preserved.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::batch::BatchOp;
use crate::config::Options;
use crate::db::{DbInner, RetiredVlog};
use crate::error::Result;
use crate::iter::{InternalIterator, MergeIterator};
use crate::manifest::{self, RunMeta};
use crate::table::TableBuilder;
use crate::types::{
    decode_repr, ikey_kind, ikey_seqno, ikey_ukey, ReprRef, SeqNo, ValueKind, MAX_SEQNO,
};
use crate::version::{Run, TableHandle};
use crate::vlog::{self, VlogFileHandle};

pub(crate) struct Job {
    level: usize,
    target: usize,
    inputs: Vec<Run>,
    /// Runs that will sit *behind* (older than) the compaction output:
    /// existing runs of the target level plus everything deeper. A tombstone
    /// may only be dropped if none of these can contain its key.
    older: Vec<Run>,
    kind: JobKind,
    /// Output lands in (or creates) the deepest level, where
    /// `Options::bottom_compression` applies.
    bottom: bool,
}

enum JobKind {
    /// Tiered merge: output run lands at the FRONT (newest) of the target
    /// level.
    Tier,
    /// Move the current bottom runs into a new, deeper level by manifest edit.
    /// Table encoding is self-describing, so every file remains byte-for-byte.
    Deepen,
    /// Incremental bottom merge: `inputs` are the newest bottom run plus
    /// ONLY the base run's tables overlapping its key range; the output is
    /// spliced between the base run's untouched fragments to form the new
    /// single leveled base run at the BACK of the level.
    BottomSplice {
        /// The (old) base run consumed by the splice.
        base_id: u64,
        /// Base tables strictly before / after the merged range, key order.
        keep_left: Vec<Arc<TableHandle>>,
        keep_right: Vec<Arc<TableHandle>>,
        new_run_id: u64,
    },
}

struct RunningJob {
    job: Job,
    watermark: SeqNo,
    merge: Option<MergeIterator>,
    run_id: u64,
    tables: Vec<Arc<TableHandle>>,
    builder: Option<(u64, TableBuilder)>,
    discard: HashMap<u64, u64>,
    cur_ukey: Vec<u8>,
    have_key: bool,
    kept_le_w: bool,
}

struct Scheduler {
    active: Option<RunningJob>,
    suspended: Vec<RunningJob>,
    force: bool,
    did_work: bool,
}

/// Levels can grow (deepening) but never past this: 16 tiers at any sane
/// `tier_width` is more data than a single node stores.
const MAX_DYNAMIC_LEVELS: usize = 16;

/// Per-level byte budget for deepening decisions: the volume of one
/// L0->L1 merge is the unit; each level down multiplies by `tier_width`.
fn level_target_bytes(opts: &Options, level: usize) -> u64 {
    let unit = (opts.memtable_size as u64)
        .saturating_mul(opts.l0_compaction_trigger as u64)
        .max(1);
    let width = opts.tier_width.max(2) as u64;
    (0..level).fold(unit, |acc, _| acc.saturating_mul(width))
}

/// Return a level count whose bottom budget can hold `bottom_bytes` without
/// immediately deepening the tree. Fresh-store bulk loading uses this to
/// install its only run directly at a stable bottom level.
pub(crate) fn level_count_for_bottom_bytes(
    opts: &Options,
    current: usize,
    bottom_bytes: u64,
) -> usize {
    let mut levels = current.max(1);
    while levels < MAX_DYNAMIC_LEVELS && bottom_bytes > level_target_bytes(opts, levels - 1) {
        levels += 1;
    }
    levels
}

/// One pass of the maintenance loop; returns whether any work happened.
pub(crate) fn maintenance_pass(db: &Arc<DbInner>) -> Result<bool> {
    let mut did = {
        let _guard = db.compaction_mu.lock();
        Scheduler::new(false).run(db)?
    };
    did |= process_retired(db)?;
    did |= auto_gc(db)?;
    Ok(did)
}

/// Manual full compaction: merges regardless of the configured triggers
/// until every level holds at most one run.
pub(crate) fn compact_until_quiet(db: &Arc<DbInner>) -> Result<()> {
    db.check_bg_error()?;
    let _guard = db.compaction_mu.lock();
    Scheduler::new(true).run(db)?;
    Ok(())
}

impl Scheduler {
    fn new(force: bool) -> Self {
        Self {
            active: None,
            suspended: Vec::new(),
            force,
            did_work: false,
        }
    }

    fn run(mut self, db: &Arc<DbInner>) -> Result<bool> {
        while self.step(db)? {
            if db.shutdown.load(Ordering::Acquire) {
                break;
            }
        }
        Ok(self.did_work)
    }

    /// Run one bounded slice. Newly eligible upper levels preempt a deeper
    /// rewrite; suspended jobs retain immutable inputs and install later by
    /// exact run id, so outputs created above them are preserved.
    fn step(&mut self, db: &Arc<DbInner>) -> Result<bool> {
        if let Some(level) = self.active.as_ref().map(|job| job.job.level) {
            if let Some(job) = pick_before(db, level) {
                self.suspended.push(self.active.take().unwrap());
                self.active = Some(RunningJob::new(db, job)?);
            }
        }

        if self.active.is_none() {
            if let Some(level) = self.suspended.last().map(|job| job.job.level) {
                if let Some(job) = pick_before(db, level) {
                    self.active = Some(RunningJob::new(db, job)?);
                } else {
                    self.active = self.suspended.pop();
                }
            } else if let Some(job) = pick(db, self.force) {
                self.active = Some(RunningJob::new(db, job)?);
            } else {
                return Ok(false);
            }
        }

        self.did_work = true;
        if self.active.as_mut().unwrap().run_slice(db)? {
            self.active = None;
        }
        Ok(true)
    }
}

fn pick_tier(v: &crate::version::Version, level: usize) -> Job {
    let mut older = v.levels[level + 1].clone();
    for deeper in &v.levels[level + 2..] {
        older.extend(deeper.iter().cloned());
    }
    Job {
        level,
        target: level + 1,
        inputs: v.levels[level].clone(),
        older,
        kind: JobKind::Tier,
        bottom: level + 1 == v.levels.len() - 1,
    }
}

/// Normal-trigger work strictly above `before`, in picker priority order.
fn pick_before(db: &Arc<DbInner>, before: usize) -> Option<Job> {
    let s = db.state.read();
    let v = &s.version;
    let last = v.levels.len() - 1;
    for level in 0..last.min(before) {
        let trigger = if level == 0 {
            db.opts.l0_compaction_trigger
        } else {
            db.opts.tier_width
        };
        if v.levels[level].len() >= trigger {
            return Some(pick_tier(v, level));
        }
    }
    None
}

fn pick(db: &Arc<DbInner>, force: bool) -> Option<Job> {
    let s = db.state.read();
    let v = &s.version;
    let last = v.levels.len() - 1;
    for i in 0..last {
        let trigger = if force {
            2
        } else if i == 0 {
            db.opts.l0_compaction_trigger
        } else {
            db.opts.tier_width
        };
        if v.levels[i].len() >= trigger {
            return Some(pick_tier(v, i));
        }
    }
    // Deepen before merging in place. A one-level tree first grows a real L1
    // so future L0 work can preempt bottom maintenance. Otherwise only normal
    // maintenance deepens, when the current bottom crosses its byte budget.
    let bottom_bytes: u64 = v.levels[last].iter().map(|r| r.size()).sum();
    if v.levels.len() < MAX_DYNAMIC_LEVELS
        && !v.levels[last].is_empty()
        && ((last == 0 && v.levels[last].len() >= 2)
            || (!force && bottom_bytes > level_target_bytes(&db.opts, last)))
    {
        return Some(Job {
            level: last,
            target: last + 1,
            inputs: v.levels[last].clone(),
            older: Vec::new(),
            kind: JobKind::Deepen,
            bottom: true,
        });
    }
    if v.levels[last].len() >= 2 {
        // Incremental, not wholesale: merge ONE newer run into the base,
        // touching only the base tables its key range overlaps. Work per
        // job is bounded by that run + overlap, never the whole bottom
        // level. The run merged MUST be the one ADJACENT to the base
        // (oldest non-base) — never the front: any runs left between the
        // merged run and the spliced output would be OLDER than data now
        // positioned behind them, breaking the newest-first order that
        // point reads resolve by (stale reads), and their old Puts would
        // outlive tombstones the merge legally dropped (permanent
        // resurrection). With the adjacent run, everything left above the
        // splice is strictly newer, and for every key in the merged range
        // ALL older data is in the inputs — so older=[] stays sound.
        let n = v.levels[last].len();
        let upper = v.levels[last][n - 2].clone();
        let base = v.levels[last][n - 1].clone();
        let lo = upper
            .tables
            .iter()
            .map(|t| t.table.stats.min_ukey())
            .min()
            .expect("runs are non-empty")
            .to_vec();
        let hi = upper
            .tables
            .iter()
            .map(|t| t.table.stats.max_ukey())
            .max()
            .expect("runs are non-empty")
            .to_vec();
        let mut keep_left = Vec::new();
        let mut overlapped = Vec::new();
        let mut keep_right = Vec::new();
        for t in &base.tables {
            if t.table.stats.max_ukey() < lo.as_slice() {
                keep_left.push(t.clone());
            } else if t.table.stats.min_ukey() > hi.as_slice() {
                keep_right.push(t.clone());
            } else {
                overlapped.push(t.clone());
            }
        }
        let overlap_run = Run {
            id: base.id,
            tables: overlapped,
        };
        return Some(Job {
            level: last,
            target: last,
            inputs: vec![upper, overlap_run],
            older: Vec::new(),
            kind: JobKind::BottomSplice {
                base_id: base.id,
                keep_left,
                keep_right,
                new_run_id: db.alloc_file_id(),
            },
            bottom: true,
        });
    }
    None
}

impl RunningJob {
    fn new(db: &Arc<DbInner>, job: Job) -> Result<Self> {
        let deepen = matches!(job.kind, JobKind::Deepen);
        let merge = if deepen {
            None
        } else {
            let children: Vec<Box<dyn InternalIterator>> = job
                .inputs
                .iter()
                .map(|r| Box::new(r.iter()) as Box<dyn InternalIterator>)
                .collect();
            let mut merge = MergeIterator::new(children, false);
            merge.seek_to_first()?;
            Some(merge)
        };
        Ok(Self {
            job,
            watermark: db.watermark(),
            merge,
            run_id: if deepen { 0 } else { db.alloc_file_id() },
            tables: Vec::new(),
            builder: None,
            discard: HashMap::new(),
            cur_ukey: Vec::new(),
            have_key: false,
            kept_le_w: false,
        })
    }

    /// Process at most one scheduling quantum, finishing the current user
    /// key so its versions never straddle priority boundaries. Returns true
    /// once the job is durably installed.
    fn run_slice(&mut self, db: &Arc<DbInner>) -> Result<bool> {
        if matches!(self.job.kind, JobKind::Deepen) {
            install_deepen(db, &self.job)?;
            db.progress_signal.notify();
            return Ok(true);
        }

        let budget = db.opts.compaction_slice_bytes.max(1);
        let mut processed = 0u64;
        while self.merge.as_ref().unwrap().valid() {
            let new_user_key = {
                let merge = self.merge.as_ref().unwrap();
                !self.have_key || ikey_ukey(merge.ikey()) != self.cur_ukey.as_slice()
            };
            if processed >= budget && new_user_key {
                return Ok(false);
            }

            let entry_bytes = {
                let merge = self.merge.as_ref().unwrap();
                (merge.ikey().len() as u64).saturating_add(merge.value().len() as u64)
            };
            let (keep, is_ptr_drop) = {
                let merge = self.merge.as_ref().unwrap();
                let ik = merge.ikey();
                let uk = ikey_ukey(ik);
                if !self.have_key || uk != self.cur_ukey.as_slice() {
                    self.cur_ukey = uk.to_vec();
                    self.have_key = true;
                    self.kept_le_w = false;
                }
                let seq = ikey_seqno(ik);
                let kind = ikey_kind(ik)?;
                if seq > self.watermark {
                    // still visible to some possible snapshot: keep verbatim
                    (true, false)
                } else if !self.kept_le_w {
                    self.kept_le_w = true;
                    // the newest version at-or-below the watermark: keep, unless
                    // it is a tombstone provably shadowing nothing older
                    let mut shadows_older = false;
                    if kind == ValueKind::Delete {
                        for r in &self.job.older {
                            if r.may_contain_ukey(&self.cur_ukey)? {
                                shadows_older = true;
                                break;
                            }
                        }
                    }
                    if kind == ValueKind::Delete && !shadows_older {
                        (false, false)
                    } else {
                        (true, false)
                    }
                } else {
                    // shadowed by a kept newer version for every live snapshot
                    (false, kind == ValueKind::Put)
                }
            };

            if keep {
                let ukey_changed_boundary = {
                    // fragments split only between user keys
                    match &self.builder {
                        Some((_, b)) => {
                            b.estimated_size() >= db.opts.target_file_size
                                && ikey_ukey(self.merge.as_ref().unwrap().ikey()) != b.last_ukey()
                        }
                        None => false,
                    }
                };
                if ukey_changed_boundary {
                    let (id, b) = self.builder.take().unwrap();
                    self.tables.push(db.finish_table(id, b)?);
                }
                if self.builder.is_none() {
                    let id = db.alloc_file_id();
                    let file = db.io.create_new(&db.paths.table(id))?;
                    self.builder = Some((
                        id,
                        TableBuilder::new(
                            file,
                            db.opts.block_size,
                            db.opts.bloom_bits_per_key,
                            if self.job.bottom {
                                db.opts.bottom_compression.unwrap_or(db.opts.compression)
                            } else {
                                db.opts.compression
                            },
                        ),
                    ));
                }
                let merge = self.merge.as_ref().unwrap();
                self.builder
                    .as_mut()
                    .unwrap()
                    .1
                    .add(merge.ikey(), merge.value())?;
            } else if is_ptr_drop {
                if let ReprRef::Ptr(p) = decode_repr(self.merge.as_ref().unwrap().value())? {
                    *self.discard.entry(p.file).or_insert(0) += u64::from(p.len);
                }
            }
            self.merge.as_mut().unwrap().next()?;
            processed = processed.saturating_add(entry_bytes);
        }

        if let Some((id, b)) = self.builder.take() {
            self.tables.push(db.finish_table(id, b)?);
        }
        crate::io::sync_dir(&db.paths.dir)?;

        let output = if self.tables.is_empty() {
            None
        } else {
            Some(Run {
                id: self.run_id,
                tables: std::mem::take(&mut self.tables),
            })
        };
        install(db, &self.job, output, std::mem::take(&mut self.discard))?;
        db.progress_signal.notify();
        Ok(true)
    }
}

#[cfg(test)]
fn run_job(db: &Arc<DbInner>, job: Job) -> Result<()> {
    let mut running = RunningJob::new(db, job)?;
    while !running.run_slice(db)? {}
    Ok(())
}

fn install_deepen(db: &Arc<DbInner>, job: &Job) -> Result<()> {
    debug_assert!(matches!(job.kind, JobKind::Deepen));
    let input_ids: Vec<u64> = job.inputs.iter().map(|r| r.id).collect();

    let mut manifest = db.manifest.lock();
    let mut data = manifest.data.clone();
    debug_assert_eq!(job.target, data.levels.len());
    data.levels[job.level].retain(|run| !input_ids.contains(&run.id));
    data.levels.push(
        job.inputs
            .iter()
            .map(|run| RunMeta {
                id: run.id,
                table_ids: run.tables.iter().map(|table| table.id).collect(),
            })
            .collect(),
    );
    data.next_file_id = db.next_file_id.load(Ordering::SeqCst);
    let gen = manifest.gen + 1;
    manifest::save(&db.paths, gen, &data)?;
    manifest.gen = gen;
    manifest.data = data;

    let mut state = db.state.write();
    let mut version = state.version.clone_shape();
    debug_assert_eq!(job.target, version.levels.len());
    version.levels[job.level].retain(|run| !input_ids.contains(&run.id));
    version.levels.push(job.inputs.clone());
    state.version = Arc::new(version);
    Ok(())
}

fn install(
    db: &Arc<DbInner>,
    job: &Job,
    output: Option<Run>,
    discard: HashMap<u64, u64>,
) -> Result<()> {
    let input_ids: Vec<u64> = job.inputs.iter().map(|r| r.id).collect();

    let mut m = db.manifest.lock();
    let mut data = m.data.clone();
    data.levels[job.level].retain(|r| !input_ids.contains(&r.id));
    match &job.kind {
        JobKind::Tier => {
            if let Some(run) = &output {
                data.levels[job.target].insert(
                    0,
                    RunMeta {
                        id: run.id,
                        table_ids: run.tables.iter().map(|t| t.id).collect(),
                    },
                );
            }
        }
        JobKind::Deepen => unreachable!("deepening has no rewrite output"),
        JobKind::BottomSplice {
            base_id,
            keep_left,
            keep_right,
            new_run_id,
        } => {
            // base run is consumed too (its overlapped tables were inputs
            // under the base id; retain above already removed it)
            data.levels[job.level].retain(|r| r.id != *base_id);
            let mut table_ids: Vec<u64> = keep_left.iter().map(|t| t.id).collect();
            if let Some(run) = &output {
                table_ids.extend(run.tables.iter().map(|t| t.id));
            }
            table_ids.extend(keep_right.iter().map(|t| t.id));
            if !table_ids.is_empty() {
                // the leveled base run lives at the BACK (oldest position)
                data.levels[job.level].push(RunMeta {
                    id: *new_run_id,
                    table_ids,
                });
            }
        }
    }
    if !discard.is_empty() {
        // only files still in the resolution map (and not already retired)
        // accumulate stats: pointers into retired/deleted files would pin
        // dead entries in the manifest forever. NB: manifest vlog_live lags
        // rotation (new heads are only persisted at reopen/GC), so the
        // version map — not vlog_live — is the live-set authority here.
        let resolvable: std::collections::HashSet<u64> =
            db.state.read().version.vlogs.keys().copied().collect();
        let mut map = data.discard_map();
        for (f, b) in &discard {
            let is_retired = data.vlog_retired.iter().any(|(id, _)| id == f);
            if resolvable.contains(f) && !is_retired {
                *map.entry(*f).or_insert(0) += *b;
            }
        }
        data.discard = map.into_iter().collect();
    }
    data.next_file_id = db.next_file_id.load(Ordering::SeqCst);
    let gen = m.gen + 1;
    manifest::save(&db.paths, gen, &data)?;
    m.gen = gen;
    m.data = data;

    let mut s = db.state.write();
    let mut v = s.version.clone_shape();
    v.levels[job.level].retain(|r| !input_ids.contains(&r.id));
    match job.kind {
        JobKind::Tier => {
            if let Some(run) = output {
                v.levels[job.target].insert(0, run);
            }
        }
        JobKind::Deepen => unreachable!("deepening has no rewrite output"),
        JobKind::BottomSplice {
            base_id,
            ref keep_left,
            ref keep_right,
            new_run_id,
        } => {
            v.levels[job.level].retain(|r| r.id != base_id);
            let mut tables = keep_left.clone();
            if let Some(run) = output {
                tables.extend(run.tables);
            }
            tables.extend(keep_right.iter().cloned());
            if !tables.is_empty() {
                v.levels[job.level].push(Run {
                    id: new_run_id,
                    tables,
                });
            }
        }
    }
    s.version = Arc::new(v);
    drop(s);
    drop(m);

    for r in &job.inputs {
        for t in &r.tables {
            t.mark_obsolete();
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Value-log GC
// ---------------------------------------------------------------------------

/// Retired victims become deletable once BOTH gates pass:
/// - snapshot watermark strictly above the retirement seqno (no reader can
///   resolve a version that dereferences the victim), and
/// - flush watermark at/above it (the relocations are durable in tables, so
///   recovery can never resurrect pointers into the deleted file).
fn process_retired(db: &Arc<DbInner>) -> Result<bool> {
    let watermark = db.watermark();
    let flushed = db.manifest.lock().data.last_flushed_seqno;
    let ready: Vec<RetiredVlog> = {
        let mut list = db.retired.lock();
        let mut ready = Vec::new();
        let mut keep = Vec::new();
        for r in list.drain(..) {
            if watermark > r.retired_at && flushed >= r.retired_at {
                ready.push(r);
            } else {
                keep.push(r);
            }
        }
        *list = keep;
        ready
    };
    if ready.is_empty() {
        return Ok(false);
    }
    {
        let mut m = db.manifest.lock();
        let mut data = m.data.clone();
        data.vlog_retired
            .retain(|(id, _)| !ready.iter().any(|r| r.id == *id));
        let gen = m.gen + 1;
        manifest::save(&db.paths, gen, &data)?;
        m.gen = gen;
        m.data = data;

        // only now drop the victims from the resolution map: the gates
        // guarantee no current or future reader can resolve a version that
        // dereferences them (older pinned Versions keep their own Arcs)
        let mut s = db.state.write();
        let mut v = s.version.clone_shape();
        for r in &ready {
            v.vlogs.remove(&r.id);
        }
        s.version = Arc::new(v);
    }
    for r in ready {
        // actual unlink happens when the last pinned Version drops its Arc
        r.handle.mark_obsolete();
    }
    Ok(true)
}

fn auto_gc(db: &Arc<DbInner>) -> Result<bool> {
    Ok(gc_vlog(db)?.is_some())
}

/// Bytes of each vlog file to sample when estimating liveness.
const GC_SAMPLE_BYTES: u64 = 1 << 20;
/// Skip files smaller than this: relocating them wholesale is cheap enough
/// that sampling adds nothing.
const GC_SAMPLE_MIN_FILE: u64 = 4 << 20;
/// Don't resample a below-ratio file until this many new writes have
/// happened — its dead ratio can only change with writes. A vlog head
/// rotation also expires the cooldown: for large-value workloads a sealed
/// ~vlog_file_size of new data is massive garbage potential while the
/// seqno counter barely moves (the maintenance loop samples files EARLY,
/// while they are still mostly live — without the rotation escape those
/// early verdicts would blind GC on low-seqno-volume stores).
const GC_RESAMPLE_SEQ_DELTA: u64 = 10_000;

/// Discard-stat fallback: estimate one sealed vlog file's dead ratio by
/// probing a bounded oldest-first sample of its records against the LSM
/// (the same liveness test relocation uses). Returns a victim when the
/// estimate clears the configured GC ratio.
///
/// The oldest-first sample biases toward dead data, which makes the probe
/// eager rather than blind: a false positive costs one relocation pass
/// whose live records are simply rewritten (relocation itself is ground
/// truth), while the old behavior — waiting for compaction to happen to
/// observe the garbage — could defer reclaiming a mostly-dead file
/// indefinitely under lazy leveling.
fn sample_victim(db: &Arc<DbInner>) -> Result<Option<(u64, Arc<VlogFileHandle>)>> {
    let visible = db.visible_seqno.load(std::sync::atomic::Ordering::Acquire);

    // candidates: sealed, not retired, big enough, not on cooldown —
    // lowest id first (oldest data is the most likely to have died)
    let candidate = {
        let m = db.manifest.lock();
        let s = db.state.read();
        let head = s.version.vlog_head_id;
        let sampled = db.gc_sampled_at.lock();
        let mut ids: Vec<u64> = s
            .version
            .vlogs
            .keys()
            .copied()
            .filter(|&id| id != head)
            .filter(|id| !m.data.vlog_retired.iter().any(|(r, _)| r == id))
            .filter(|id| {
                sampled
                    .get(id)
                    .map(|&(at_seq, at_head)| {
                        visible.saturating_sub(at_seq) >= GC_RESAMPLE_SEQ_DELTA
                            || at_head != head
                    })
                    .unwrap_or(true)
            })
            .collect();
        ids.sort_unstable();
        let mut picked = None;
        for id in ids {
            let h = s.version.vlogs.get(&id).unwrap();
            if h.file.len()? >= GC_SAMPLE_MIN_FILE {
                picked = Some((id, h.clone()));
                break;
            }
        }
        picked
    };
    let Some((id, handle)) = candidate else {
        return Ok(None);
    };

    let head_now = db.state.read().version.vlog_head_id;
    let (records, sampled_len) = vlog::sample_records(handle.file.as_ref(), GC_SAMPLE_BYTES)?;
    if sampled_len == 0 {
        db.gc_sampled_at.lock().insert(id, (visible, head_now));
        return Ok(None);
    }
    let view = db.read_view();
    let mut dead: u64 = 0;
    for (off, len, key, _vlen) in &records {
        let live = match view.get_versioned(key, MAX_SEQNO)? {
            Some((ValueKind::Put, _, repr)) => match decode_repr(&repr)? {
                ReprRef::Ptr(p) => p.file == id && p.offset == *off && p.len == *len,
                ReprRef::Inline(_) => false,
            },
            _ => false,
        };
        if !live {
            dead += u64::from(*len);
        }
    }
    let ratio = dead as f64 / sampled_len as f64;
    if ratio >= db.opts.vlog_gc_ratio {
        Ok(Some((id, handle)))
    } else {
        db.gc_sampled_at.lock().insert(id, (visible, head_now));
        Ok(None)
    }
}

/// One GC pass: pick the most-garbage sealed vlog file above the configured
/// ratio, relocate its still-live values through the write path (batched
/// under the write mutex — atomic against every writer, no OCC needed), and
/// retire it at S = the visible seqno after the last relocation.
pub(crate) fn gc_vlog(db: &Arc<DbInner>) -> Result<Option<u64>> {
    let _g = db.gc_mu.lock();
    db.check_bg_error()?;

    // ---- pick a victim ----------------------------------------------------
    let (victim_id, handle) = {
        let m = db.manifest.lock();
        let s = db.state.read();
        let head = s.version.vlog_head_id;
        let discard = m.data.discard_map();
        let mut best: Option<(u64, f64)> = None;
        for (&id, h) in &s.version.vlogs {
            // the version map also carries gated-retired victims (kept
            // resolvable for old snapshots) — never re-pick those
            if id == head || m.data.vlog_retired.iter().any(|(r, _)| *r == id) {
                continue;
            }
            let Some(&dead) = discard.get(&id) else {
                continue;
            };
            let size = h.file.len()?.max(1);
            let ratio = dead as f64 / size as f64;
            if ratio >= db.opts.vlog_gc_ratio
                && best.map(|(_, r)| ratio > r).unwrap_or(true)
            {
                best = Some((id, ratio));
            }
        }
        match best {
            None => {
                drop(s);
                drop(m);
                // discard stats only accumulate when compaction happens to
                // rewrite pointers — under lazy leveling they lag far
                // behind reality. Fall back to sampling one candidate
                // file's actual liveness per pass.
                match sample_victim(db)? {
                    None => return Ok(None),
                    Some(v) => v,
                }
            }
            Some((id, _)) => (id, s.version.vlogs.get(&id).unwrap().clone()),
        }
    };

    // ---- relocate live records --------------------------------------------
    let (mut records, _valid) = vlog::scan_records(handle.file.as_ref())?;
    // key order gives the LSM liveness probes locality
    records.sort_by(|a, b| a.2.cmp(&b.2));

    for chunk in records.chunks(256) {
        let mut ws = db.write_mu.lock();
        let view = db.read_view();
        let mut ops: Vec<BatchOp> = Vec::new();
        for (off, len, key, _vlen) in chunk {
            let Some((ValueKind::Put, _, repr)) = view.get_versioned(key, MAX_SEQNO)? else {
                continue;
            };
            let ReprRef::Ptr(p) = decode_repr(&repr)? else {
                continue;
            };
            if p.file != victim_id || p.offset != *off || p.len != *len {
                continue; // relocated or overwritten already — garbage now
            }
            let value = vlog::read_value(&handle, &p, key, None)?;
            ops.push(BatchOp::Put {
                key: key.clone(),
                value,
            });
        }
        if !ops.is_empty() {
            // relocations change placement, not logical state: never capture
            db.apply_locked(&mut ws, &ops, false)?;
        }
    }

    // The retirement manifest flip below is DURABLE; the relocations it
    // depends on must be durable first (in relaxed sync modes they so far
    // live only in the unsynced WAL / vlog head). Payload before pointer,
    // pointer before the metadata that disowns the old copy.
    {
        let ws = db.write_mu.lock();
        db.vlog.sync_head()?;
        ws.wal.sync()?;
    }

    // S: sampled after every relocation is committed. Any key we skipped was
    // already shadowed by a write with seqno <= S, so every reader at
    // snapshot > S resolves victim-free versions.
    let retired_at: SeqNo = db.visible_seqno.load(Ordering::Acquire);

    {
        let mut m = db.manifest.lock();
        let mut data = m.data.clone();
        data.vlog_live.retain(|&id| id != victim_id);
        data.vlog_retired.push((victim_id, retired_at));
        data.discard.retain(|(id, _)| *id != victim_id);
        let gen = m.gen + 1;
        manifest::save(&db.paths, gen, &data)?;
        m.gen = gen;
        m.data = data;
        // NOTE: the victim deliberately STAYS in Version::vlogs — registered
        // snapshots at/below S still resolve old versions that point into
        // it. process_retired removes it from the resolution map only once
        // the gates prove no reader can ever need it again.
    }
    db.retired.lock().push(RetiredVlog {
        id: victim_id,
        retired_at,
        handle,
    });
    Ok(Some(victim_id))
}

#[cfg(test)]
mod shape_tests {
    use super::*;
    use crate::config::{Options, SyncMode};

    fn tiny_opts() -> Options {
        Options {
            sync: SyncMode::Never,
            memtable_size: 32 << 10,
            l0_compaction_trigger: 2,
            tier_width: 2,
            max_levels: 2,
            compaction_slice_bytes: 1,
            value_threshold: 4096,
            ..Options::default()
        }
    }

    fn flush_run(db: &crate::Db, prefix: &str, count: u32) {
        for i in 0..count {
            db.put(format!("{prefix}/{i:06}"), vec![i as u8; 32])
                .unwrap();
        }
        db.flush().unwrap();
    }

    #[test]
    fn scheduler_preempts_a_sliced_deep_job_for_l0() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 3;
        opts.memtable_size = 16 << 20;
        opts.l0_stall_trigger = 2;
        opts.compaction_slice_bytes = 1;
        let reopen_opts = opts.clone();
        let db = Arc::new(crate::Db::open(dir.path(), opts).unwrap());
        let inner = db.inner.clone();
        let hold = inner.compaction_mu.lock();

        // Build two L1 runs, then begin (but do not finish) their L1->L2 job.
        for generation in 0..2 {
            flush_run(&db, &format!("old/{generation}/a"), 200);
            flush_run(&db, &format!("old/{generation}/b"), 200);
            let job = pick(&inner, false).expect("L0 tier job");
            assert_eq!(job.level, 0);
            run_job(&inner, job).unwrap();
        }
        let deep = pick(&inner, false).expect("L1 tier job");
        assert_eq!(deep.level, 1);
        let mut scheduler = Scheduler::new(false);
        scheduler.active = Some(RunningJob::new(&inner, deep).unwrap());
        assert!(scheduler.step(&inner).unwrap());
        assert_eq!(scheduler.active.as_ref().unwrap().job.level, 1);

        // Fill L0 to the write-stall threshold while the deep job is suspended.
        flush_run(&db, "new/a", 40);
        flush_run(&db, "new/b", 40);
        assert_eq!(inner.state.read().version.levels[0].len(), 2);

        let (sent, received) = std::sync::mpsc::channel();
        let writer_db = db.clone();
        let writer = std::thread::spawn(move || {
            sent.send(writer_db.put("writer/after-stall", "ok"))
                .unwrap();
        });
        assert!(received
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());

        // The next slice is L0, not another slice of the active deep job.
        assert!(scheduler.step(&inner).unwrap());
        assert_eq!(scheduler.active.as_ref().unwrap().job.level, 0);
        assert_eq!(scheduler.suspended.len(), 1);
        for _ in 0..10_000 {
            if inner.state.read().version.levels[0].is_empty() {
                break;
            }
            assert!(scheduler.step(&inner).unwrap());
        }
        received
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer stayed stalled behind the deep job")
            .unwrap();
        writer.join().unwrap();

        while scheduler.step(&inner).unwrap() {}
        assert_eq!(
            db.get(b"old/1/b/000199").unwrap().as_deref(),
            Some(&[199; 32][..])
        );
        assert_eq!(
            db.get(b"writer/after-stall").unwrap().as_deref(),
            Some(&b"ok"[..])
        );

        drop(hold);
        drop(inner);
        drop(db);
        let db = crate::Db::open(dir.path(), reopen_opts).unwrap();
        assert_eq!(
            db.get(b"writer/after-stall").unwrap().as_deref(),
            Some(&b"ok"[..])
        );
    }

    #[test]
    fn scheduler_preempts_bottom_splice_without_losing_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 2;
        opts.memtable_size = 16 << 20;
        opts.l0_stall_trigger = 2;
        opts.compaction_slice_bytes = 1;
        let reopen_opts = opts.clone();
        let db = Arc::new(crate::Db::open(dir.path(), opts).unwrap());
        let inner = db.inner.clone();
        let hold = inner.compaction_mu.lock();

        db.put("victim", "alive").unwrap();
        flush_run(&db, "base/a", 200);
        flush_run(&db, "base/b", 200);
        let base = pick(&inner, false).expect("base L0 tier job");
        assert_eq!(base.level, 0);
        run_job(&inner, base).unwrap();

        db.delete("victim").unwrap();
        flush_run(&db, "upper/a", 200);
        flush_run(&db, "upper/b", 200);
        let upper = pick(&inner, false).expect("upper L0 tier job");
        assert_eq!(upper.level, 0);
        run_job(&inner, upper).unwrap();

        let splice = pick(&inner, false).expect("bottom splice job");
        assert!(matches!(splice.kind, JobKind::BottomSplice { .. }));
        let mut scheduler = Scheduler::new(false);
        scheduler.active = Some(RunningJob::new(&inner, splice).unwrap());
        assert!(scheduler.step(&inner).unwrap());

        flush_run(&db, "newer/a", 40);
        flush_run(&db, "newer/b", 40);
        let (sent, received) = std::sync::mpsc::channel();
        let writer_db = db.clone();
        let writer = std::thread::spawn(move || {
            sent.send(writer_db.put("writer/through-bottom", "ok"))
                .unwrap();
        });
        assert!(received
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());

        assert!(scheduler.step(&inner).unwrap());
        assert_eq!(scheduler.active.as_ref().unwrap().job.level, 0);
        for _ in 0..10_000 {
            if inner.state.read().version.levels[0].is_empty() {
                break;
            }
            assert!(scheduler.step(&inner).unwrap());
        }
        received
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer stayed stalled behind the bottom splice")
            .unwrap();
        writer.join().unwrap();
        while scheduler.step(&inner).unwrap() {}

        assert_eq!(db.get(b"victim").unwrap(), None);
        assert_eq!(
            db.get(b"newer/a/000000").unwrap().as_deref(),
            Some(&[0; 32][..])
        );
        assert_eq!(
            db.get(b"writer/through-bottom").unwrap().as_deref(),
            Some(&b"ok"[..])
        );

        drop(hold);
        db.compact_all().unwrap();
        drop(inner);
        drop(db);
        let db = crate::Db::open(dir.path(), reopen_opts).unwrap();
        assert_eq!(db.get(b"victim").unwrap(), None);
        assert_eq!(
            db.get(b"writer/through-bottom").unwrap().as_deref(),
            Some(&b"ok"[..])
        );
    }

    #[test]
    fn deepening_moves_existing_tables_by_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 1;
        opts.memtable_size = 16 << 20;
        let db = crate::Db::open(dir.path(), opts).unwrap();
        let inner = db.inner.clone();
        let _hold = inner.compaction_mu.lock();

        flush_run(&db, "first", 40);
        flush_run(&db, "second", 40);
        let before: Vec<u64> = inner.state.read().version.levels[0]
            .iter()
            .flat_map(|run| run.tables.iter().map(|table| table.id))
            .collect();
        let job = pick(&inner, false).expect("one-level tree must deepen");
        assert!(matches!(job.kind, JobKind::Deepen));
        run_job(&inner, job).unwrap();

        let state = inner.state.read();
        assert!(state.version.levels[0].is_empty());
        let after: Vec<u64> = state.version.levels[1]
            .iter()
            .flat_map(|run| run.tables.iter().map(|table| table.id))
            .collect();
        assert_eq!(after, before, "deepening rewrote an existing table");
    }

    /// Regression (review finding): a 3+-run bottom must read the NEWEST
    /// value after splicing. The broken version merged the FRONT run into
    /// the base, leaving middle runs positioned "newer" than newer data.
    #[test]
    fn three_run_bottom_reads_newest_after_splices() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 1; // L0 IS the bottom: flushes build bottom runs
        let db = crate::Db::open(dir.path(), opts).unwrap();

        // build bottom = [r3(k=v3), r2(k=v2), r1(k=v1)] with the background
        // compactor held off so all three runs coexist
        {
            let _hold = db.inner.compaction_mu.lock();
            for v in ["v1", "v2", "v3"] {
                db.put("k", v).unwrap();
                db.put(format!("pad/{v}"), v).unwrap();
                db.inner.force_rotate().unwrap();
                db.inner.wait_flushed().unwrap();
            }
            assert!(
                db.inner.state.read().version.levels[0].len() >= 3,
                "need a 3-run bottom"
            );
        }
        // splice to quiet
        for _ in 0..50 {
            if !maintenance_pass(&db.inner).unwrap() {
                break;
            }
        }
        assert_eq!(
            db.get(b"k").unwrap().as_deref(),
            Some(b"v3".as_ref()),
            "newest value must win after incremental bottom merges"
        );
    }

    /// Regression (review finding): a tombstone in a newer bottom run must
    /// NOT be dropped while a stranded middle run still holds an older Put
    /// — the broken version resurrected deleted keys permanently.
    #[test]
    fn tombstone_survives_multi_run_bottom_splices() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 1;
        let db = crate::Db::open(dir.path(), opts).unwrap();

        {
            let _hold = db.inner.compaction_mu.lock();
            db.put("k", "v1").unwrap();
            db.put("keep/1", "x").unwrap();
            db.inner.force_rotate().unwrap();
            db.inner.wait_flushed().unwrap();
            db.put("k", "v2").unwrap();
            db.put("keep/2", "x").unwrap();
            db.inner.force_rotate().unwrap();
            db.inner.wait_flushed().unwrap();
            db.delete("k").unwrap();
            db.put("keep/3", "x").unwrap();
            db.inner.force_rotate().unwrap();
            db.inner.wait_flushed().unwrap();
            assert!(db.inner.state.read().version.levels[0].len() >= 3);
        }
        for _ in 0..50 {
            if !maintenance_pass(&db.inner).unwrap() {
                break;
            }
        }
        assert_eq!(
            db.get(b"k").unwrap(),
            None,
            "deleted key resurrected through bottom splices"
        );
        for i in 1..=3 {
            assert!(db.get(format!("keep/{i}").as_bytes()).unwrap().is_some());
        }
        // and it stays dead across recovery
        drop(db);
        let mut opts = tiny_opts();
        opts.max_levels = 1;
        let db = crate::Db::open(dir.path(), opts).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None, "resurrected after reopen");
    }

    /// A bottom level past its byte budget grows a NEW level instead of
    /// being rewritten in place forever; the deeper manifest reopens fine
    /// even though it exceeds Options::max_levels.
    #[test]
    fn bottom_overflow_deepens_the_tree() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Db::open(dir.path(), tiny_opts()).unwrap();
        // unit = 32KiB * 2 = 64KiB; level-1 budget = 128KiB. Write ~2MiB.
        let val = vec![7u8; 1000];
        for i in 0..2000u32 {
            db.put(format!("deep/{i:06}"), val.clone()).unwrap();
        }
        db.flush().unwrap();
        // let maintenance chew until quiet
        for _ in 0..200 {
            if !maintenance_pass(&db.inner).unwrap() {
                break;
            }
        }
        let depth = db.inner.state.read().version.levels.len();
        assert!(depth > 2, "bottom overflow must deepen: depth={depth}");

        for i in (0..2000u32).step_by(97) {
            assert!(db.get(format!("deep/{i:06}").as_bytes()).unwrap().is_some());
        }
        drop(db);
        // reopen: manifest is deeper than Options::max_levels
        let db = crate::Db::open(dir.path(), tiny_opts()).unwrap();
        assert!(db.inner.state.read().version.levels.len() > 2);
        for i in (0..2000u32).step_by(97) {
            assert!(db.get(format!("deep/{i:06}").as_bytes()).unwrap().is_some());
        }
    }

    /// A newer bottom run disjoint from most of the base must splice: base
    /// tables outside the overlap survive by identity (no rewrite).
    #[test]
    fn bottom_merge_keeps_untouched_base_tables()  {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = tiny_opts();
        opts.max_levels = 2;
        opts.target_file_size = 16 << 10; // many small fragments in the base
        let db = crate::Db::open(dir.path(), opts).unwrap();

        // phase 1: a wide base at the bottom
        let val = vec![3u8; 500];
        for i in 0..1500u32 {
            db.put(format!("a/{i:06}"), val.clone()).unwrap();
        }
        db.flush().unwrap();
        db.compact_all().unwrap();
        let base_ids: Vec<u64> = {
            let s = db.inner.state.read();
            let last = s.version.levels.len() - 1;
            let base = s.version.levels[last].last().unwrap();
            assert!(base.tables.len() > 3, "need several base fragments");
            base.tables.iter().map(|t| t.id).collect()
        };

        // phase 2: a narrow update touching only the very start of the
        // keyspace, pushed down to the bottom by the tiered merges
        for i in 0..40u32 {
            db.put(format!("a/{i:06}"), vec![9u8; 500]).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..200 {
            if !maintenance_pass(&db.inner).unwrap() {
                break;
            }
        }

        let survived: usize = {
            let s = db.inner.state.read();
            let last = s.version.levels.len() - 1;
            let bottom_ids: Vec<u64> = s.version.levels[last]
                .iter()
                .flat_map(|r| r.tables.iter().map(|t| t.id))
                .collect();
            base_ids.iter().filter(|id| bottom_ids.contains(id)).count()
        };
        assert!(
            survived > 0,
            "an incremental bottom merge must keep base tables outside the \
             overlap by identity (all {} were rewritten)",
            base_ids.len()
        );

        // and the data is right: updated head, untouched tail
        assert_eq!(
            db.get(b"a/000000").unwrap().as_deref(),
            Some(&vec![9u8; 500][..])
        );
        assert_eq!(
            db.get(b"a/001400").unwrap().as_deref(),
            Some(&vec![3u8; 500][..])
        );
    }

    /// VERIFICATION REPRO: a BottomSplice (front + base only, older = [])
    /// must not drop a front-run tombstone whose key a MIDDLE bottom run
    /// still shadows. Steps jobs by hand under compaction_mu.
    #[test]
    fn bottom_splice_must_not_resurrect_deleted_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Db::open(dir.path(), tiny_opts()).unwrap();
        let inner = db.inner.clone();

        // Block the background maintenance thread; we drive jobs manually.
        let guard = inner.compaction_mu.lock();

        let bottom_len = |inner: &Arc<DbInner>| -> usize {
            let s = inner.state.read();
            let last = s.version.levels.len() - 1;
            s.version.levels[last].len()
        };

        // Step A: base run B at the bottom (holds put k@v0 among a..z).
        for i in 0..26u8 {
            db.put(vec![b'a' + i], vec![0u8; 16]).unwrap();
        }
        db.flush().unwrap();
        db.put("zz", vec![0u8; 16]).unwrap();
        db.flush().unwrap();
        let job = pick(&inner, false).expect("tier L0->bottom (B)");
        run_job(&inner, job).unwrap();
        assert_eq!(bottom_len(&inner), 1, "bottom = [B]");

        // Step B: middle run M with put k = "resurrected".
        db.put("k", b"resurrected".to_vec()).unwrap();
        db.flush().unwrap();
        db.put("zz", vec![1u8; 16]).unwrap();
        db.flush().unwrap();
        let job = pick(&inner, false).expect("tier L0->bottom (M)");
        run_job(&inner, job).unwrap();
        assert_eq!(bottom_len(&inner), 2, "bottom = [M, B]");

        // Step C: front run F with delete k. pick() prefers the L0 tier
        // trigger over the splice, so the bottom reaches 3 runs.
        db.delete("k").unwrap();
        db.flush().unwrap();
        db.put("zz", vec![2u8; 16]).unwrap();
        db.flush().unwrap();
        let job = pick(&inner, false).expect("tier L0->bottom (F)");
        assert!(matches!(job.kind, JobKind::Tier), "L0 tier must win over splice");
        run_job(&inner, job).unwrap();
        assert_eq!(bottom_len(&inner), 3, "bottom = [F(del k), M(put k), B]");
        assert_eq!(db.get(b"k").unwrap(), None, "delete visible pre-splice");

        // Step D: the incremental splice merges F + B's overlap only.
        let job = pick(&inner, false).expect("bottom splice");
        assert!(matches!(job.kind, JobKind::BottomSplice { .. }));
        assert!(job.older.is_empty(), "splice job carries no older runs");
        run_job(&inner, job).unwrap();

        let after_splice = db.get(b"k").unwrap();
        drop(guard);

        // Permanence: full compaction, then reopen.
        db.compact_all().unwrap();
        let after_full = db.get(b"k").unwrap();
        // the DbInner clone owns the process lock file — release it too
        drop(inner);
        drop(db);
        let db = crate::Db::open(dir.path(), tiny_opts()).unwrap();
        let after_reopen = db.get(b"k").unwrap();

        assert!(
            after_splice.is_none() && after_full.is_none() && after_reopen.is_none(),
            "deleted key resurrected: after_splice={:?} after_full_compaction={:?} after_reopen={:?}",
            after_splice.as_deref().map(String::from_utf8_lossy),
            after_full.as_deref().map(String::from_utf8_lossy),
            after_reopen.as_deref().map(String::from_utf8_lossy),
        );
    }
}
