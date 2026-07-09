//! IO fault injection (feature `fault-injection`, run with
//! `cargo test -p fluent31 --features fault-injection`).
//!
//! A SIGKILL crash test proves the engine recovers what was *fsynced*; this
//! proves what happens when the disk itself *fails or lies*. A custom `Io`
//! wraps positioned file IO and, on command, fails `sync_data` (fsync error),
//! `append` (ENOSPC), or `read_at` (EIO). The system-of-record invariant under
//! test: a failed fsync must **never** be reported as a successful commit
//! (no false ack / silent loss), a full disk fails the write cleanly, a read
//! fault surfaces a typed error instead of a panic, and the store always
//! reopens consistent with everything that WAS durably acked intact.
//!
//! Scope: the injected `Io` covers WAL / vlog / table data-plane IO (append +
//! both fsyncs + reads). Manifest `atomic_write` and directory fsyncs use
//! `std::fs` directly and are not faulted here.
#![cfg(all(feature = "fault-injection", unix))]

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fluent31::{Db, DbFile, Error, Io, Options, Result, SyncMode};

// ---------------------------------------------------------------------------
// A fault-injecting positioned-IO backend
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Control {
    fail_sync: AtomicBool,
    fail_append: AtomicBool,
    fail_read: AtomicBool,
    syncs: AtomicU64,
    appends: AtomicU64,
    reads: AtomicU64,
}

fn io_err(msg: &str) -> Error {
    Error::Io(std::io::Error::other(msg))
}

struct FaultIo {
    ctl: Arc<Control>,
}

impl Io for FaultIo {
    fn open_read(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Arc::new(FaultFile {
            file,
            ctl: self.ctl.clone(),
            append_off: Mutex::new(len),
        }))
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Arc::new(FaultFile {
            file,
            ctl: self.ctl.clone(),
            append_off: Mutex::new(0),
        }))
    }
}

struct FaultFile {
    file: File,
    ctl: Arc<Control>,
    append_off: Mutex<u64>,
}

impl DbFile for FaultFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize> {
        self.ctl.reads.fetch_add(1, Ordering::Relaxed);
        if self.ctl.fail_read.load(Ordering::Acquire) {
            return Err(io_err("injected read fault"));
        }
        Ok(self.file.read_at(buf, off)?)
    }

    fn append(&self, data: &[u8]) -> Result<u64> {
        self.ctl.appends.fetch_add(1, Ordering::Relaxed);
        if self.ctl.fail_append.load(Ordering::Acquire) {
            return Err(io_err("injected ENOSPC on append"));
        }
        let mut off = self.append_off.lock().unwrap();
        self.file.write_all_at(data, *off)?;
        let at = *off;
        *off += data.len() as u64;
        Ok(at)
    }

    fn sync_data(&self) -> Result<()> {
        self.ctl.syncs.fetch_add(1, Ordering::Relaxed);
        if self.ctl.fail_sync.load(Ordering::Acquire) {
            return Err(io_err("injected fsync failure"));
        }
        Ok(self.file.sync_data()?)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

fn opts() -> Options {
    Options {
        sync: SyncMode::Always,
        memtable_size: 32 << 10,
        value_threshold: 128,
        ..Options::default()
    }
}

fn open_faulted(dir: &Path, ctl: Arc<Control>) -> Db {
    Db::open_with_io(dir, opts(), Arc::new(FaultIo { ctl }), "fault").unwrap()
}

// ---------------------------------------------------------------------------
// The invariant: a failed fsync is never a successful commit
// ---------------------------------------------------------------------------

#[test]
fn failed_fsync_is_never_a_false_ack() {
    let dir = tempfile::tempdir().unwrap();
    let ctl = Arc::new(Control::default());
    {
        let db = open_faulted(dir.path(), ctl.clone());
        // durable baseline
        for i in 0..50u32 {
            db.put(format!("k/{i:04}").into_bytes(), b"durable".to_vec()).unwrap();
        }

        // arm fsync failure: the next commit's WAL fsync fails
        ctl.fail_sync.store(true, Ordering::Release);
        let r = db.put(b"doomed".to_vec(), b"x".to_vec());
        assert!(
            r.is_err(),
            "a write whose fsync failed must return Err, not a false Ok"
        );
        assert!(ctl.syncs.load(Ordering::Relaxed) > 0, "fsync was actually exercised");
        drop(db);
    }

    // reopen with a healthy backend: the durable baseline is fully intact and
    // the store is not corrupt (the doomed write may be absent, never partial)
    let db = Db::open(dir.path(), Options { sync: SyncMode::Never, ..opts() }).unwrap();
    for i in 0..50u32 {
        assert_eq!(
            db.get(&format!("k/{i:04}").into_bytes()).unwrap().unwrap(),
            b"durable",
            "durable baseline lost after an fsync fault"
        );
    }
    // full scan parses cleanly — no corruption anywhere
    let n = db.iter(None, None, false).unwrap().map(|r| r.unwrap()).count();
    assert!(n >= 50);
}

#[test]
fn enospc_on_append_fails_the_write_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let ctl = Arc::new(Control::default());
    {
        let db = open_faulted(dir.path(), ctl.clone());
        for i in 0..30u32 {
            db.put(format!("k/{i:04}").into_bytes(), b"ok".to_vec()).unwrap();
        }
        // disk full: the next append (WAL or vlog) errors
        ctl.fail_append.store(true, Ordering::Release);
        let r = db.put(b"nospace".to_vec(), vec![b'z'; 400]);
        assert!(r.is_err(), "append ENOSPC must fail the write, not fake success");
        drop(db);
    }
    // reopen clean; the pre-fault writes survive
    let db = Db::open(dir.path(), Options { sync: SyncMode::Never, ..opts() }).unwrap();
    for i in 0..30u32 {
        assert_eq!(db.get(&format!("k/{i:04}").into_bytes()).unwrap().unwrap(), b"ok");
    }
    assert!(db.get(b"nospace").unwrap().is_none());
}

#[test]
fn read_fault_surfaces_a_clean_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let ctl = Arc::new(Control::default());
    {
        // seed and flush so reads must hit disk (through the faulted backend)
        let db = open_faulted(dir.path(), ctl.clone());
        for i in 0..200u32 {
            db.put(format!("k/{i:05}").into_bytes(), vec![b'v'; 200]).unwrap();
        }
        db.flush().unwrap();
        drop(db);
    }

    // reopen with the faulted backend (cold cache), then arm read failures
    let db = open_faulted(dir.path(), ctl.clone());
    ctl.fail_read.store(true, Ordering::Release);
    // a cold-cache get forces a disk read → clean typed error, no panic/UB
    let mut saw_err = false;
    for i in 0..200u32 {
        if db.get(&format!("k/{i:05}").into_bytes()).is_err() {
            saw_err = true;
            break;
        }
    }
    assert!(saw_err, "a read fault should surface as an error");

    // the engine is still alive: disarm and it serves correctly again
    ctl.fail_read.store(false, Ordering::Release);
    assert_eq!(db.get(b"k/00000").unwrap().unwrap(), vec![b'v'; 200]);
}
