//! Corruption fuzz sweep: a system of record must never turn a bad byte on
//! disk into a crash. This builds a populated store (data across SSTs, the
//! value log, an unflushed WAL, and the manifest), then deterministically
//! mutates every file type at many offsets — bit flips, truncation, zero runs,
//! random overwrites, whole-file garbage — and asserts that `Db::open` + a full
//! scan always returns a clean `Result` (Ok or a typed Error) and **never
//! panics, hangs, or reads out of bounds**.
//!
//! This drives the WAL, table (footer + block CRC), manifest, and vlog decoders
//! through the real recovery/read pipeline, not in isolation.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use fluent31::{Db, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 8 << 10,
        block_size: 512,
        target_file_size: 8 << 10,
        value_threshold: 64,
        vlog_file_size: 16 << 10,
        ..Options::default()
    }
}

/// Deterministic PRNG (xorshift64) so failures reproduce from the seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Build a store exercising every on-disk structure, then close it.
fn build_pristine(dir: &Path) {
    let db = Db::open(dir, opts()).unwrap();
    for i in 0..600u32 {
        // mix of small (inline) and large (vlog) values
        let val = if i % 3 == 0 {
            vec![b'L'; 300] // -> value log
        } else {
            format!("v{i}").into_bytes()
        };
        db.put(format!("key/{i:05}").into_bytes(), val).unwrap();
        if i == 300 {
            db.flush().unwrap(); // some data into SSTs + manifest
        }
    }
    db.compact_all().unwrap();
    // more writes left only in the WAL (unflushed)
    for i in 600..680u32 {
        db.put(format!("key/{i:05}").into_bytes(), format!("w{i}").into_bytes()).unwrap();
    }
    drop(db);
}

fn files_in(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_file() && p.file_name().unwrap().to_string_lossy() != "LOCK")
        .collect()
}

fn copy_dir_flat(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for f in files_in(src) {
        std::fs::copy(&f, dst.join(f.file_name().unwrap())).unwrap();
    }
}

/// Apply one random mutation to `bytes`; returns a description for diagnostics.
fn mutate(bytes: &mut Vec<u8>, rng: &mut Rng) -> String {
    if bytes.is_empty() {
        bytes.push(0);
    }
    match rng.below(5) {
        0 => {
            let at = rng.below(bytes.len());
            bytes[at] ^= 1 << (rng.below(8));
            format!("flip bit @ {at}")
        }
        1 => {
            let at = rng.below(bytes.len());
            bytes.truncate(at);
            format!("truncate @ {at}")
        }
        2 => {
            let at = rng.below(bytes.len());
            let len = (rng.below(64) + 1).min(bytes.len() - at);
            for b in &mut bytes[at..at + len] {
                *b = 0;
            }
            format!("zero {len} @ {at}")
        }
        3 => {
            let at = rng.below(bytes.len());
            let len = (rng.below(64) + 1).min(bytes.len() - at);
            for b in &mut bytes[at..at + len] {
                *b = (rng.next() & 0xff) as u8;
            }
            format!("garble {len} @ {at}")
        }
        _ => {
            let n = rng.below(256) + 1;
            *bytes = (0..n).map(|_| (rng.next() & 0xff) as u8).collect();
            format!("replace whole file ({n} rand bytes)")
        }
    }
}

/// Open + fully drain the store; returns Ok(()) whether the engine succeeded
/// or returned a typed error — the point is that neither panics.
fn open_and_drain(dir: &Path) {
    if let Ok(db) = Db::open(dir, opts()) {
        if let Ok(it) = db.iter(None, None, false) {
            for kv in it {
                if kv.is_err() {
                    break; // a clean scan error is fine
                }
            }
        }
        // a few point lookups too (bloom + block read paths)
        for i in [0u32, 42, 350, 679] {
            let _ = db.get(&format!("key/{i:05}").into_bytes());
        }
    }
}

#[test]
fn random_on_disk_corruption_never_panics() {
    let pristine = tempfile::tempdir().unwrap();
    build_pristine(pristine.path());
    let file_count = files_in(pristine.path()).len();
    assert!(file_count >= 3, "expected SST + vlog + WAL + manifest, got {file_count}");

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let iterations = 500;
    for iter in 0..iterations {
        let work = tempfile::tempdir().unwrap();
        copy_dir_flat(pristine.path(), work.path());

        // mutate a randomly chosen file
        let files = files_in(work.path());
        let target = &files[rng.below(files.len())];
        let mut bytes = std::fs::read(target).unwrap();
        let desc = mutate(&mut bytes, &mut rng);
        std::fs::write(target, &bytes).unwrap();
        let fname = target.file_name().unwrap().to_string_lossy().into_owned();

        // opening/scanning a corrupted store must never panic or hang
        let res = catch_unwind(AssertUnwindSafe(|| open_and_drain(work.path())));
        assert!(
            res.is_ok(),
            "PANIC on iter {iter}: file {fname}, mutation: {desc}"
        );
    }
}

/// A pristine store must, of course, still open and read back everything —
/// guards against the harness silently testing an already-broken store.
#[test]
fn pristine_store_reads_back_fully() {
    let dir = tempfile::tempdir().unwrap();
    build_pristine(dir.path());
    let db = Db::open(dir.path(), opts()).unwrap();
    for i in 0..680u32 {
        assert!(
            db.get(&format!("key/{i:05}").into_bytes()).unwrap().is_some(),
            "key {i} missing from pristine store"
        );
    }
}
