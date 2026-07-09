//! Opt-in append-only mutation journal — a catastrophe-recovery safety net.
//!
//! This is **not** part of the engine's durability path and is off unless you
//! construct it. The store's own WAL + manifest are the source of truth; the
//! journal is a *separate*, independent record of every logical mutation from
//! which a fresh database can be rebuilt from zero — for the day a disk block
//! goes bad, a file is truncated, or the directory is lost, i.e. damage past
//! what the engine can self-recover.
//!
//! Design (deliberate):
//!
//! - **External and async.** A [`Journal`] attaches to a live `Db`, subscribes
//!   to the committed-write stream ([`Db::subscribe`]), and appends each
//!   mutation to its own log on a background thread. It never sits on the
//!   commit path, never fsyncs inside a `put`, and never stalls a writer — the
//!   DB stays the fast source of truth and the journal trails it slightly. If
//!   you ever need it, losing the journal's last few unsynced records is
//!   acceptable: you only reach for it when the DB itself is gone.
//! - **Gap-free by construction.** The subscription is installed under the
//!   write mutex, so every commit past `start_seqno()` is delivered in
//!   seqno order with no gaps. The journal writes a base snapshot at attach
//!   (a consistent cut ≥ `start_seqno()`), then streams deltas; any small
//!   overlap between the base cut and the first deltas replays idempotently.
//! - **Self-healing.** If the consumer ever falls behind its queue cap the
//!   stream reports `Lagged`; the journal responds by writing a fresh base
//!   snapshot (a new checkpoint) and resuming, so the log is never left with
//!   a hole. Rebuild always anchors on the *last complete* checkpoint.
//! - **Provenance-guarded.** The log header records the source store's
//!   instance id; re-attaching a different store's log into the same
//!   directory is refused, so two lineages can't interleave into one journal.
//!
//! **Scope:** the journal records the **user keyspace only** — the actual
//! key/value data. Engine-reserved state (`0x00`-prefixed: installed WASM
//! modules, trigger definitions) is *not* journaled; it is code/config that is
//! re-deployed, not data that must be reconstructed. This is the same boundary
//! `Db::subscribe` draws. A rebuilt database therefore holds all data; reinstall
//! modules and recreate triggers separately.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::coding::{crc32, put_len_prefixed, put_u64, Reader};
use crate::config::Options;
use crate::db::{Db, StreamEvent};
use crate::error::{corrupt, Error, Result};
use crate::identity::{hex, InstanceId, INSTANCE_ID_LEN};
use crate::types::{SeqNo, ValueKind, USER_KEYSPACE_START};

const MAGIC: u64 = 0xf115_e731_10c7_0001;
const FORMAT: u8 = 1;

/// Record tags.
const TAG_HEADER: u8 = 0;
const TAG_CHECKPOINT: u8 = 1;
const TAG_BASE: u8 = 2;
const TAG_BASE_END: u8 = 3;
const TAG_DELTA: u8 = 4;

/// Rotate the active log file once it passes this size.
const DEFAULT_ROTATE_BYTES: u64 = 128 << 20;

/// How often the background drainer parks waiting for new commits.
const DRAIN_POLL: Duration = Duration::from_millis(200);

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

/// Snapshot of a running journal's progress.
#[derive(Debug, Clone, Default)]
pub struct JournalStats {
    /// Delta records appended since attach.
    pub deltas_written: u64,
    /// Base-snapshot records written (across all checkpoints).
    pub base_records_written: u64,
    /// Highest committed seqno durably appended to the log.
    pub last_seqno: SeqNo,
    /// Times the stream lagged and forced a fresh base snapshot.
    pub rebaselines: u64,
    /// Set if the drainer stopped on an error; the journal is then stale.
    pub last_error: Option<String>,
}

/// What a rebuild reconstructed.
#[derive(Debug, Clone)]
pub struct RebuildReport {
    /// Source store instance id (hex) recorded in the journal header.
    pub source_instance: String,
    /// Keys written from the anchoring base snapshot.
    pub base_keys: u64,
    /// Delta records applied after the base.
    pub deltas_applied: u64,
    /// Highest seqno reflected in the rebuilt database.
    pub last_seqno: SeqNo,
}

/// A live journal attached to a `Db`. Drop stops the drainer and flushes.
pub struct Journal {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

struct Shared {
    stop: AtomicBool,
    stats: Mutex<JournalStats>,
    /// Force a fresh base snapshot on the next drainer wake (checkpoint()).
    force_checkpoint: AtomicBool,
}

impl Journal {
    /// Attach a journal in `dir` to `db`, capturing all user-key mutations.
    /// Writes an initial base snapshot, then streams committed writes on a
    /// background thread. `db` is held for the journal's lifetime.
    pub fn attach(db: Arc<Db>, dir: impl AsRef<Path>) -> Result<Journal> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let source = db.identity().map(|id| id.instance_id).unwrap_or([0u8; INSTANCE_ID_LEN]);
        let mut writer = LogWriter::open(&dir, source)?;

        // Subscribe FIRST so no commit slips between the base cut and the
        // stream (delivery is gap-free past start_seqno).
        let sub = db.subscribe(USER_KEYSPACE_START, None)?;

        let (base_records, seq) = write_base_snapshot(&db, &mut writer)?;
        let stats = JournalStats {
            base_records_written: base_records,
            last_seqno: seq,
            ..JournalStats::default()
        };

        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            stats: Mutex::new(stats),
            force_checkpoint: AtomicBool::new(false),
        });

        let thread = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("fluent31-journal".into())
                .spawn(move || drain_loop(db, sub, writer, shared))
                .map_err(Error::Io)?
        };

        Ok(Journal {
            shared,
            thread: Some(thread),
        })
    }

    /// Current progress snapshot.
    pub fn stats(&self) -> JournalStats {
        self.shared.stats.lock().unwrap().clone()
    }

    /// Force a fresh base snapshot (a new checkpoint) on the next drainer
    /// pass. Bounds log growth and captures any state the incremental stream
    /// does not — call after installing modules / changing triggers if you
    /// want those reflected at the next checkpoint's base.
    pub fn request_checkpoint(&self) {
        self.shared.force_checkpoint.store(true, Ordering::Release);
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Log file writer (append-only, framed like the WAL: [len u32][crc u32][body])
// ---------------------------------------------------------------------------

struct LogWriter {
    dir: PathBuf,
    file: File,
    file_id: u64,
    bytes_in_file: u64,
    rotate_at: u64,
}

fn log_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("journal-{id:06}.log"))
}

fn list_log_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("journal-") {
            if let Some(num) = rest.strip_suffix(".log") {
                if let Ok(id) = num.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

impl LogWriter {
    /// Open the journal directory: reuse the newest log if a header for this
    /// store is already present, else start a fresh header. A header for a
    /// *different* store is a provenance error.
    fn open(dir: &Path, source: InstanceId) -> Result<LogWriter> {
        let existing = list_log_ids(dir)?;
        if let Some(&first) = existing.first() {
            let recorded = read_header(&log_path(dir, first))?;
            if recorded != source {
                return Err(Error::InvalidArgument(format!(
                    "journal dir belongs to store {} but this store is {}",
                    hex(&recorded),
                    hex(&source)
                )));
            }
            let last = *existing.last().unwrap();
            let file = OpenOptions::new().append(true).open(log_path(dir, last))?;
            let bytes_in_file = file.metadata()?.len();
            return Ok(LogWriter {
                dir: dir.to_path_buf(),
                file,
                file_id: last,
                bytes_in_file,
                rotate_at: DEFAULT_ROTATE_BYTES,
            });
        }
        // fresh journal: file 1 opens with a header record
        let mut w = LogWriter {
            dir: dir.to_path_buf(),
            file: File::create(log_path(dir, 1))?,
            file_id: 1,
            bytes_in_file: 0,
            rotate_at: DEFAULT_ROTATE_BYTES,
        };
        let mut hdr = vec![TAG_HEADER, FORMAT];
        put_u64(&mut hdr, MAGIC);
        hdr.extend_from_slice(&source);
        w.append(&hdr)?;
        w.sync()?;
        Ok(w)
    }

    fn append(&mut self, payload: &[u8]) -> Result<()> {
        let mut rec = Vec::with_capacity(8 + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&crc32(payload).to_le_bytes());
        rec.extend_from_slice(payload);
        self.file.write_all(&rec)?;
        self.bytes_in_file += rec.len() as u64;
        if self.bytes_in_file >= self.rotate_at {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        self.file.sync_all()?;
        self.file_id += 1;
        self.file = File::create(log_path(&self.dir, self.file_id))?;
        self.bytes_in_file = 0;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_all().map_err(Error::Io)
    }
}

/// Read and validate the header record of a journal file, returning the
/// recorded source instance id.
fn read_header(path: &Path) -> Result<InstanceId> {
    let bytes = std::fs::read(path)?;
    let (records, _) = read_records(&bytes);
    let first = records
        .first()
        .ok_or_else(|| corrupt("journal file has no header"))?;
    let mut r = Reader::new(first);
    if r.u8()? != TAG_HEADER {
        return Err(corrupt("journal does not start with a header"));
    }
    if r.u8()? != FORMAT {
        return Err(corrupt("unsupported journal format"));
    }
    if r.u64()? != MAGIC {
        return Err(corrupt("bad journal magic"));
    }
    let id: InstanceId = r.bytes(INSTANCE_ID_LEN)?.try_into().unwrap();
    Ok(id)
}

// ---------------------------------------------------------------------------
// Record encoding
// ---------------------------------------------------------------------------

fn encode_checkpoint(seq: SeqNo) -> Vec<u8> {
    let mut p = vec![TAG_CHECKPOINT];
    put_u64(&mut p, seq);
    p
}

fn encode_base(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = vec![TAG_BASE];
    put_len_prefixed(&mut p, key);
    put_len_prefixed(&mut p, value);
    p
}

fn encode_base_end(seq: SeqNo) -> Vec<u8> {
    let mut p = vec![TAG_BASE_END];
    put_u64(&mut p, seq);
    p
}

fn encode_delta(seq: SeqNo, kind: ValueKind, key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let mut p = vec![TAG_DELTA];
    put_u64(&mut p, seq);
    p.push(kind as u8);
    put_len_prefixed(&mut p, key);
    if let Some(v) = value {
        put_len_prefixed(&mut p, v);
    }
    p
}

// ---------------------------------------------------------------------------
// Base snapshot + drain loop
// ---------------------------------------------------------------------------

/// Write a checkpoint + a full base snapshot of the current user keyspace,
/// terminated by a base-end record carrying the snapshot's seqno. Returns
/// (base record count, snapshot seqno).
fn write_base_snapshot(db: &Db, writer: &mut LogWriter) -> Result<(u64, SeqNo)> {
    let snap = db.snapshot();
    let seq = snap.seqno();
    writer.append(&encode_checkpoint(seq))?;
    let mut base_records = 0u64;
    for kv in db.iter_at(None, None, false, &snap)? {
        let (k, v) = kv?;
        writer.append(&encode_base(&k, &v))?;
        base_records += 1;
    }
    writer.append(&encode_base_end(seq))?;
    writer.sync()?;
    Ok((base_records, seq))
}

fn drain_loop(db: Arc<Db>, mut sub: crate::db::Subscription, mut writer: LogWriter, shared: Arc<Shared>) {
    while !shared.stop.load(Ordering::Acquire) {
        if shared.force_checkpoint.swap(false, Ordering::AcqRel) {
            if let Err(e) = rebaseline(&db, &mut sub, &mut writer, &shared) {
                record_error(&shared, e);
                return;
            }
        }
        match sub.recv_timeout(DRAIN_POLL) {
            Ok(Some(StreamEvent::Batch(entries))) => {
                if let Err(e) = write_deltas(&mut writer, &entries, &shared) {
                    record_error(&shared, e);
                    return;
                }
            }
            Ok(Some(StreamEvent::Lagged)) => {
                // the stream has a hole; heal it with a fresh base snapshot
                if let Err(e) = rebaseline(&db, &mut sub, &mut writer, &shared) {
                    record_error(&shared, e);
                    return;
                }
            }
            Ok(None) => {} // timeout, loop and re-check stop
            Err(e) => {
                record_error(&shared, e);
                return;
            }
        }
    }
    // final flush on clean shutdown
    let _ = writer.sync();
}

fn write_deltas(writer: &mut LogWriter, entries: &[crate::db::StreamEntry], shared: &Arc<Shared>) -> Result<()> {
    let mut last = 0u64;
    for e in entries {
        writer.append(&encode_delta(e.seqno, e.kind, &e.key, e.value.as_deref()))?;
        last = last.max(e.seqno);
    }
    writer.sync()?;
    let mut s = shared.stats.lock().unwrap();
    s.deltas_written += entries.len() as u64;
    s.last_seqno = s.last_seqno.max(last);
    Ok(())
}

/// Re-subscribe and write a fresh base snapshot, healing a lagged/holed
/// stream. The old subscription is dropped; the new one is installed
/// gap-free, and the fresh base supersedes everything before it at rebuild.
fn rebaseline(
    db: &Arc<Db>,
    sub: &mut crate::db::Subscription,
    writer: &mut LogWriter,
    shared: &Arc<Shared>,
) -> Result<()> {
    let fresh = db.subscribe(USER_KEYSPACE_START, None)?;
    *sub = fresh;
    let (base_records, seq) = write_base_snapshot(db, writer)?;
    let mut s = shared.stats.lock().unwrap();
    s.rebaselines += 1;
    s.base_records_written += base_records;
    s.last_seqno = s.last_seqno.max(seq);
    Ok(())
}

fn record_error(shared: &Arc<Shared>, e: Error) {
    let mut s = shared.stats.lock().unwrap();
    s.last_error = Some(e.to_string());
}

// ---------------------------------------------------------------------------
// Rebuild
// ---------------------------------------------------------------------------

/// Rebuild a fresh database at `dest` from a journal directory. Anchors on the
/// last complete base snapshot and replays every delta after it, reconstructing
/// the source's user keyspace as of the journal's last durable record.
///
/// `dest` must not already be an open/live database; a fresh directory is
/// expected. Returns what was reconstructed.
pub fn rebuild(journal_dir: impl AsRef<Path>, dest: impl AsRef<Path>, opts: Options) -> Result<RebuildReport> {
    let journal_dir = journal_dir.as_ref();
    let records = read_all_records(journal_dir)?;
    if records.is_empty() {
        return Err(corrupt("journal is empty"));
    }

    // header + provenance
    let mut r0 = Reader::new(&records[0]);
    if r0.u8()? != TAG_HEADER {
        return Err(corrupt("journal does not start with a header"));
    }
    r0.u8()?; // format
    if r0.u64()? != MAGIC {
        return Err(corrupt("bad journal magic"));
    }
    let source: InstanceId = r0.bytes(INSTANCE_ID_LEN)?.try_into().unwrap();

    // Find the last CHECKPOINT that is followed by a BASE_END: its base is
    // complete. Records between it and its base-end are the base; everything
    // after the base-end is replayable deltas.
    let anchor = find_last_complete_checkpoint(&records)?;

    let db = Db::open(dest, opts)?;
    let mut base_keys = 0u64;
    let mut deltas_applied = 0u64;
    let mut last_seqno = 0u64;

    // apply the base
    for rec in &records[anchor.base_start..anchor.base_end] {
        let mut r = Reader::new(rec);
        if r.u8()? != TAG_BASE {
            return Err(corrupt("expected base record in base span"));
        }
        let key = r.len_prefixed()?.to_vec();
        let value = r.len_prefixed()?.to_vec();
        db.put(key, value)?;
        base_keys += 1;
    }
    last_seqno = last_seqno.max(anchor.base_seqno);

    // replay deltas after the base-end, in file (== seqno) order
    for rec in &records[anchor.base_end + 1..] {
        let mut r = Reader::new(rec);
        match r.u8()? {
            TAG_DELTA => {
                let seq = r.u64()?;
                let kind = ValueKind::from_u8(r.u8()?)?;
                let key = r.len_prefixed()?.to_vec();
                match kind {
                    ValueKind::Put => {
                        let value = r.len_prefixed()?.to_vec();
                        db.put(key, value)?;
                    }
                    ValueKind::Delete => db.delete(key)?,
                }
                deltas_applied += 1;
                last_seqno = last_seqno.max(seq);
            }
            // a later checkpoint/base can appear if the anchor wasn't the very
            // last checkpoint (shouldn't happen — we pick the last complete
            // one) — skip base scaffolding defensively
            TAG_CHECKPOINT | TAG_BASE | TAG_BASE_END => {}
            other => return Err(corrupt(format!("unexpected journal record tag {other}"))),
        }
    }

    db.sync_wal()?;
    Ok(RebuildReport {
        source_instance: hex(&source),
        base_keys,
        deltas_applied,
        last_seqno,
    })
}

struct Anchor {
    base_seqno: SeqNo,
    /// index of the first BASE record (checkpoint index + 1)
    base_start: usize,
    /// index of the BASE_END record
    base_end: usize,
}

fn find_last_complete_checkpoint(records: &[Vec<u8>]) -> Result<Anchor> {
    // walk forward, tracking the most recent (checkpoint, base_end) pair
    let mut cur_checkpoint: Option<(usize, SeqNo)> = None;
    let mut best: Option<Anchor> = None;
    for (i, rec) in records.iter().enumerate() {
        match rec.first().copied() {
            Some(TAG_CHECKPOINT) => {
                let mut r = Reader::new(rec);
                r.u8()?;
                cur_checkpoint = Some((i, r.u64()?));
            }
            Some(TAG_BASE_END) => {
                if let Some((ci, cseq)) = cur_checkpoint.take() {
                    let mut r = Reader::new(rec);
                    r.u8()?;
                    let end_seq = r.u64()?;
                    best = Some(Anchor {
                        base_seqno: cseq.max(end_seq),
                        base_start: ci + 1,
                        base_end: i,
                    });
                }
            }
            _ => {}
        }
    }
    best.ok_or_else(|| corrupt("journal has no complete base snapshot"))
}

// ---------------------------------------------------------------------------
// Framed-record reading (shared by header read, rebuild, tests)
// ---------------------------------------------------------------------------

/// Parse framed records from one file's bytes, stopping at the first torn or
/// CRC-invalid record (a journal crash tail). Returns (records, clean_end).
fn read_records(bytes: &[u8]) -> (Vec<Vec<u8>>, bool) {
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos + 8 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let body_start = pos + 8;
        if len == 0 || body_start + len > bytes.len() {
            return (records, false); // torn tail
        }
        let body = &bytes[body_start..body_start + len];
        if crc32(body) != crc {
            return (records, false); // corrupt/torn tail
        }
        records.push(body.to_vec());
        pos = body_start + len;
    }
    (records, pos == bytes.len())
}

/// Read every journal file in id order, concatenating their records. A file
/// that ends torn must be the last one written; later files after a torn file
/// signal corruption.
fn read_all_records(dir: &Path) -> Result<Vec<Vec<u8>>> {
    let ids = list_log_ids(dir)?;
    if ids.is_empty() {
        return Err(corrupt("no journal files found"));
    }
    let mut out = Vec::new();
    let last = *ids.last().unwrap();
    for id in ids {
        let bytes = std::fs::read(log_path(dir, id))?;
        let (recs, clean) = read_records(&bytes);
        out.extend(recs);
        if !clean && id != last {
            return Err(corrupt(format!(
                "journal file {id} ends torn but is not the newest"
            )));
        }
    }
    Ok(out)
}
