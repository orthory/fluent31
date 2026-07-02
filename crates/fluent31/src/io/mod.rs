//! IO abstraction. The data plane (file reads/appends) sits behind traits so
//! Linux gets an io_uring backend while everything else uses positioned IO.
//! Metadata operations (rename, link, dir fsync) are plain std::fs helpers —
//! they are never on a hot path.

mod std_io;
#[cfg(target_os = "linux")]
mod uring_io;

use std::path::Path;
use std::sync::Arc;

use crate::config::IoBackend;
use crate::error::{Error, Result};

/// One batched positioned read: fill `buf` completely from `off`.
pub struct ReadReq {
    pub off: u64,
    pub buf: Vec<u8>,
}

pub trait DbFile: Send + Sync {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize>;

    fn read_exact_at(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        let mut done = 0;
        while done < buf.len() {
            let n = self.read_at(off + done as u64, &mut buf[done..])?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                )));
            }
            done += n;
        }
        Ok(())
    }

    /// Fill every request completely. Backends may batch (io_uring submits the
    /// whole slice in one go); the default is a serial loop.
    fn read_many(&self, reqs: &mut [ReadReq]) -> Result<()> {
        for r in reqs.iter_mut() {
            let off = r.off;
            self.read_exact_at(off, &mut r.buf)?;
        }
        Ok(())
    }

    /// Sequential append; returns the offset the data landed at. A file has at
    /// most one appender at a time (enforced by the callers' locking).
    fn append(&self, data: &[u8]) -> Result<u64>;

    fn sync_data(&self) -> Result<()>;

    fn len(&self) -> Result<u64>;
}

pub trait Io: Send + Sync {
    fn open_read(&self, path: &Path) -> Result<Arc<dyn DbFile>>;
    /// Create a brand-new file for appending; fails if the path exists (file
    /// ids are never reused, so collision means a bug or a foreign process).
    fn create_new(&self, path: &Path) -> Result<Arc<dyn DbFile>>;
}

/// Resolve the configured backend. `Auto` probes io_uring on Linux and falls
/// back to portable IO if the kernel or sandbox refuses it.
pub fn backend(kind: IoBackend) -> Result<(Arc<dyn Io>, &'static str)> {
    match kind {
        IoBackend::Std => Ok((Arc::new(std_io::StdIo), "std")),
        IoBackend::Uring => {
            #[cfg(target_os = "linux")]
            {
                Ok((Arc::new(uring_io::UringIo::new()?), "io_uring"))
            }
            #[cfg(not(target_os = "linux"))]
            {
                Err(Error::InvalidArgument(
                    "io_uring backend requires Linux".into(),
                ))
            }
        }
        IoBackend::Auto => {
            #[cfg(target_os = "linux")]
            {
                if let Ok(u) = uring_io::UringIo::new() {
                    return Ok((Arc::new(u), "io_uring"));
                }
            }
            Ok((Arc::new(std_io::StdIo), "std"))
        }
    }
}

// ---------------------------------------------------------------------------
// Metadata helpers (control plane)
// ---------------------------------------------------------------------------

pub fn sync_dir(dir: &Path) -> Result<()> {
    // Directory fsync is how directory entries (created/renamed/linked files)
    // become durable on POSIX.
    let d = std::fs::File::open(dir)?;
    d.sync_all()?;
    Ok(())
}

/// Durable small-file replacement: write tmp, fsync tmp, rename over target,
/// fsync directory.
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument("path has no parent".into()))?;
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        use std::io::Write;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_dir(dir)?;
    Ok(())
}

/// Hard-link `src` to `dst`; fall back to a full copy (+fsync) when the
/// filesystem refuses links. Used by checkpoints — sources are immutable.
pub fn hard_link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(src, dst)?;
            let f = std::fs::File::open(dst)?;
            f.sync_all()?;
            Ok(())
        }
    }
}

/// Copy exactly `len` bytes of `src` into a new file `dst` and fsync it.
/// Used to snapshot the actively-appended vlog head into an archive.
pub fn copy_prefix(src: &Path, dst: &Path, len: u64) -> Result<()> {
    use std::io::{Read, Write};
    let mut from = std::fs::File::open(src)?;
    let mut to = std::fs::File::create(dst)?;
    let mut remaining = len;
    let mut buf = vec![0u8; 1 << 20];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = from.read(&mut buf[..want])?;
        if n == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "source shorter than copy length",
            )));
        }
        to.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    to.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn std_append_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let (io, _) = backend(IoBackend::Std).unwrap();
        let f = io.create_new(&path).unwrap();
        assert_eq!(f.append(b"hello").unwrap(), 0);
        assert_eq!(f.append(b" world").unwrap(), 5);
        f.sync_data().unwrap();
        assert_eq!(f.len().unwrap(), 11);

        let r = io.open_read(&path).unwrap();
        let mut buf = vec![0u8; 5];
        r.read_exact_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"world");

        let mut reqs = vec![
            ReadReq {
                off: 0,
                buf: vec![0; 5],
            },
            ReadReq {
                off: 6,
                buf: vec![0; 5],
            },
        ];
        r.read_many(&mut reqs).unwrap();
        assert_eq!(&reqs[0].buf, b"hello");
        assert_eq!(&reqs[1].buf, b"world");
    }

    #[test]
    fn create_new_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let (io, _) = backend(IoBackend::Std).unwrap();
        io.create_new(&path).unwrap();
        assert!(io.create_new(&path).is_err());
    }

    #[test]
    fn atomic_write_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CURRENT");
        atomic_write(&path, b"one").unwrap();
        atomic_write(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
    }
}
