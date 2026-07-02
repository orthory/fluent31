//! io_uring backend (Linux).
//!
//! Design note (deliberate, after weighing a shared-reactor design): a single
//! shared ring serializing every read behind one mutex would make foreground
//! point reads wait on background batches, and a reactor dispatching foreign
//! completions is out of scope for v1. Instead:
//!
//! - single positioned reads, appends and fsyncs use plain syscalls (pread /
//!   pwrite / fdatasync are as fast as uring for single ops and never contend);
//! - `read_many` — compaction readahead, scan block prefetch, batched vlog
//!   value resolution — grabs a ring from a small pool and submits the whole
//!   batch in one `io_uring_enter`, reaping all completions before releasing
//!   the ring. Each ring serves exactly one batch at a time, so no dispatcher
//!   is needed and batches from different threads proceed in parallel.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;

use super::std_io::StdFile;
use super::{DbFile, Io, ReadReq};
use crate::error::{Error, Result};

const RING_ENTRIES: u32 = 128;
const POOL_SIZE: usize = 4;

pub(super) struct RingPool {
    rings: Vec<Mutex<IoUring>>,
    next: AtomicUsize,
}

impl RingPool {
    fn new() -> Result<Self> {
        let mut rings = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            rings.push(Mutex::new(IoUring::new(RING_ENTRIES).map_err(Error::Io)?));
        }
        Ok(RingPool {
            rings,
            next: AtomicUsize::new(0),
        })
    }

    fn acquire(&self) -> parking_lot::MutexGuard<'_, IoUring> {
        // Try each ring once without blocking, then block on our round-robin
        // slot. Batches are short-lived, so contention resolves quickly.
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..self.rings.len() {
            if let Some(g) = self.rings[(start + i) % self.rings.len()].try_lock() {
                return g;
            }
        }
        self.rings[start % self.rings.len()].lock()
    }
}

pub(super) struct UringFile {
    inner: StdFile,
    file: File,
    pool: Arc<RingPool>,
}

impl DbFile for UringFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize> {
        self.inner.read_at(off, buf)
    }

    fn read_exact_at(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact_at(off, buf)
    }

    fn append(&self, data: &[u8]) -> Result<u64> {
        self.inner.append(data)
    }

    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }

    fn len(&self) -> Result<u64> {
        self.inner.len()
    }

    fn read_many(&self, reqs: &mut [ReadReq]) -> Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        let fd = types::Fd(self.file.as_raw_fd());
        let mut ring = self.pool.acquire();

        let mut start = 0usize;
        while start < reqs.len() {
            let chunk_len = (reqs.len() - start).min(RING_ENTRIES as usize);
            let chunk = &mut reqs[start..start + chunk_len];

            // SAFETY: the SQEs hold raw pointers into `chunk`'s buffers. The
            // buffers live in the caller's slice, which cannot move or drop
            // while this function holds `&mut` to it, and we do not return
            // before reaping exactly `chunk_len` completions.
            {
                let mut sq = ring.submission();
                for (i, r) in chunk.iter_mut().enumerate() {
                    let sqe = opcode::Read::new(fd, r.buf.as_mut_ptr(), r.buf.len() as u32)
                        .offset(r.off)
                        .build()
                        .user_data(i as u64);
                    unsafe {
                        sq.push(&sqe).expect("sq sized to chunk");
                    }
                }
            }

            let mut completed = 0usize;
            let mut short: Vec<(usize, usize)> = Vec::new(); // (idx, bytes already read)
            while completed < chunk_len {
                match ring.submit_and_wait(1) {
                    Ok(_) => {}
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(e) => return Err(Error::Io(e)),
                }
                let cq = ring.completion();
                for cqe in cq {
                    completed += 1;
                    let idx = cqe.user_data() as usize;
                    let res = cqe.result();
                    if res < 0 {
                        return Err(Error::Io(std::io::Error::from_raw_os_error(-res)));
                    }
                    let got = res as usize;
                    if got < chunk[idx].buf.len() {
                        short.push((idx, got));
                    }
                }
            }
            // Finish any short reads synchronously; simpler than resubmission
            // and vanishingly rare on regular files.
            for (idx, got) in short {
                let off = chunk[idx].off + got as u64;
                let buf = &mut chunk[idx].buf[got..];
                self.inner.read_exact_at(off, buf)?;
            }
            start += chunk_len;
        }
        Ok(())
    }
}

pub(super) struct UringIo {
    pool: Arc<RingPool>,
}

impl UringIo {
    pub(super) fn new() -> Result<Self> {
        // Constructing the pool is itself the support probe: kernels without
        // io_uring (or seccomp profiles blocking it) fail here and Auto falls
        // back to the portable backend.
        Ok(UringIo {
            pool: Arc::new(RingPool::new()?),
        })
    }
}

impl Io for UringIo {
    fn open_read(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let f = File::open(path)?;
        let len = f.metadata()?.len();
        let dup = f.try_clone()?;
        Ok(Arc::new(UringFile {
            inner: StdFile::new(dup, len),
            file: f,
            pool: self.pool.clone(),
        }))
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let dup = f.try_clone()?;
        Ok(Arc::new(UringFile {
            inner: StdFile::new(dup, 0),
            file: f,
            pool: self.pool.clone(),
        }))
    }
}
