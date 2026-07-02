//! Portable positioned-IO backend (pread/pwrite via std).

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{DbFile, Io};
use crate::error::Result;

pub(super) struct StdFile {
    f: File,
    append_off: AtomicU64,
}

impl StdFile {
    pub(super) fn new(f: File, len: u64) -> Self {
        StdFile {
            f,
            append_off: AtomicU64::new(len),
        }
    }
}

impl DbFile for StdFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize> {
        Ok(self.f.read_at(buf, off)?)
    }

    fn read_exact_at(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        Ok(self.f.read_exact_at(buf, off)?)
    }

    fn append(&self, data: &[u8]) -> Result<u64> {
        let off = self.append_off.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.f.write_all_at(data, off)?;
        Ok(off)
    }

    fn sync_data(&self) -> Result<()> {
        self.f.sync_data()?;
        Ok(())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.f.metadata()?.len())
    }
}

pub(super) struct StdIo;

impl Io for StdIo {
    fn open_read(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let f = File::open(path)?;
        let len = f.metadata()?.len();
        Ok(Arc::new(StdFile::new(f, len)))
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn DbFile>> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Arc::new(StdFile::new(f, 0)))
    }
}
