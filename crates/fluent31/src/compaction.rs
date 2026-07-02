//! Background structure maintenance: lazy-leveling compaction, value-log GC,
//! and retired-victim reclamation.
//!
//! Lazy leveling: levels 0..last are tiered — when a level accumulates
//! `tier_width` runs (l0 has its own trigger) ALL of its runs merge into one
//! run placed at the FRONT (newest position) of the next level. The last
//! level is leveled: whenever it holds more than one run, everything there
//! merges into a single run. Inputs are pinned at pick time and installation
//! removes exactly the pinned runs — flush can concurrently prepend to L0.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::batch::BatchOp;
use crate::db::{DbInner, RetiredVlog};
use crate::error::Result;
use crate::iter::{InternalIterator, MergeIterator};
use crate::manifest::{self, RunMeta};
use crate::table::TableBuilder;
use crate::types::{
    decode_repr, ikey_kind, ikey_seqno, ikey_ukey, ReprRef, SeqNo, ValueKind, MAX_SEQNO,
};
use crate::version::Run;
use crate::vlog;

pub(crate) struct Job {
    level: usize,
    target: usize,
    inputs: Vec<Run>,
    /// Runs that will sit *behind* (older than) the compaction output:
    /// existing runs of the target level plus everything deeper. A tombstone
    /// may only be dropped if none of these can contain its key.
    older: Vec<Run>,
}

/// One pass of the maintenance loop; returns whether any work happened.
pub(crate) fn maintenance_pass(db: &Arc<DbInner>) -> Result<bool> {
    let mut did = false;
    {
        let _guard = db.compaction_mu.lock();
        while let Some(job) = pick(db) {
            run_job(db, job)?;
            did = true;
            if db.shutdown.load(Ordering::Acquire) {
                return Ok(did);
            }
        }
    }
    did |= process_retired(db)?;
    did |= auto_gc(db)?;
    Ok(did)
}

pub(crate) fn compact_until_quiet(db: &Arc<DbInner>) -> Result<()> {
    db.check_bg_error()?;
    let _guard = db.compaction_mu.lock();
    while let Some(job) = pick(db) {
        run_job(db, job)?;
    }
    Ok(())
}

fn pick(db: &Arc<DbInner>) -> Option<Job> {
    let s = db.state.read();
    let v = &s.version;
    let last = v.levels.len() - 1;
    for i in 0..last {
        let trigger = if i == 0 {
            db.opts.l0_compaction_trigger
        } else {
            db.opts.tier_width
        };
        if v.levels[i].len() >= trigger {
            let mut older: Vec<Run> = v.levels[i + 1].clone();
            for deeper in &v.levels[i + 2..] {
                older.extend(deeper.iter().cloned());
            }
            return Some(Job {
                level: i,
                target: i + 1,
                inputs: v.levels[i].clone(),
                older,
            });
        }
    }
    if v.levels[last].len() >= 2 {
        return Some(Job {
            level: last,
            target: last,
            inputs: v.levels[last].clone(),
            older: Vec::new(),
        });
    }
    None
}

fn run_job(db: &Arc<DbInner>, job: Job) -> Result<()> {
    let watermark = db.watermark();

    let children: Vec<Box<dyn InternalIterator>> = job
        .inputs
        .iter()
        .map(|r| Box::new(r.iter()) as Box<dyn InternalIterator>)
        .collect();
    let mut merge = MergeIterator::new(children, false);
    merge.seek_to_first()?;

    let run_id = db.alloc_file_id();
    let mut tables = Vec::new();
    let mut builder: Option<(u64, TableBuilder)> = None;
    let mut discard: HashMap<u64, u64> = HashMap::new();

    let mut cur_ukey: Vec<u8> = Vec::new();
    let mut have_key = false;
    let mut kept_le_w = false;

    while merge.valid() {
        let (keep, is_ptr_drop) = {
            let ik = merge.ikey();
            let uk = ikey_ukey(ik);
            if !have_key || uk != cur_ukey.as_slice() {
                cur_ukey = uk.to_vec();
                have_key = true;
                kept_le_w = false;
            }
            let seq = ikey_seqno(ik);
            let kind = ikey_kind(ik)?;
            if seq > watermark {
                // still visible to some possible snapshot: keep verbatim
                (true, false)
            } else if !kept_le_w {
                kept_le_w = true;
                // the newest version at-or-below the watermark: keep, unless
                // it is a tombstone provably shadowing nothing older
                if kind == ValueKind::Delete
                    && !job.older.iter().any(|r| r.may_contain_ukey(&cur_ukey))
                {
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
                match &builder {
                    Some((_, b)) => {
                        b.estimated_size() >= db.opts.target_file_size
                            && ikey_ukey(merge.ikey()) != b.last_ukey()
                    }
                    None => false,
                }
            };
            if ukey_changed_boundary {
                let (id, b) = builder.take().unwrap();
                tables.push(db.finish_table(id, b)?);
            }
            if builder.is_none() {
                let id = db.alloc_file_id();
                let file = db.io.create_new(&db.paths.table(id))?;
                builder = Some((
                    id,
                    TableBuilder::new(file, db.opts.block_size, db.opts.bloom_bits_per_key),
                ));
            }
            builder.as_mut().unwrap().1.add(merge.ikey(), merge.value())?;
        } else if is_ptr_drop {
            if let ReprRef::Ptr(p) = decode_repr(merge.value())? {
                *discard.entry(p.file).or_insert(0) += u64::from(p.len);
            }
        }
        merge.next()?;
    }
    if let Some((id, b)) = builder.take() {
        tables.push(db.finish_table(id, b)?);
    }
    crate::io::sync_dir(&db.paths.dir)?;

    let output = if tables.is_empty() {
        None
    } else {
        Some(Run {
            id: run_id,
            tables,
        })
    };

    install(db, &job, output, discard)?;
    db.progress_signal.notify();
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
    if let Some(run) = &output {
        data.levels[job.target].insert(
            0,
            RunMeta {
                id: run.id,
                table_ids: run.tables.iter().map(|t| t.id).collect(),
            },
        );
    }
    if !discard.is_empty() {
        let mut map = data.discard_map();
        for (f, b) in &discard {
            *map.entry(*f).or_insert(0) += *b;
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
    if let Some(run) = output {
        v.levels[job.target].insert(0, run);
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
            if id == head {
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
            None => return Ok(None),
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
            db.apply_locked(&mut ws, &ops)?;
        }
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

        let mut s = db.state.write();
        let mut v = s.version.clone_shape();
        v.vlogs.remove(&victim_id);
        s.version = Arc::new(v);
    }
    db.retired.lock().push(RetiredVlog {
        id: victim_id,
        retired_at,
        handle,
    });
    Ok(Some(victim_id))
}
