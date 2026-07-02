//! Guest-side SDK for fluent31 WASM modules ("fluentabi v1").
//!
//! Write a function returning an exit code (0 = success/commit) and export
//! it with [`fluent_main!`]:
//!
//! ```ignore
//! fn run() -> i32 {
//!     let who = fluent_guest::input();
//!     match fluent_guest::get(&who) {
//!         Some(v) => { fluent_guest::output(&v); 0 }
//!         None => 1,
//!     }
//! }
//! fluent_guest::fluent_main!(run);
//! ```
//!
//! Build with `--target wasm32-unknown-unknown` as a `cdylib`, then install
//! the artifact with `db.install_module(name, bytes)`.

pub mod errno {
    pub const NOT_FOUND: i32 = -1;
    pub const EROFS: i32 = -2;
    pub const EINVAL: i32 = -3;
    pub const ENOSPC: i32 = -4;
    pub const EBADF: i32 = -5;
    pub const ELIMIT: i32 = -6;
    pub const EIO: i32 = -8;
}

#[cfg(target_arch = "wasm32")]
mod sys {
    #[link(wasm_import_module = "fluent")]
    extern "C" {
        pub fn input_len() -> i32;
        pub fn input_read(dst: *mut u8, cap: i32, off: i32) -> i32;
        pub fn output_write(ptr: *const u8, len: i32) -> i32;
        pub fn log(level: i32, ptr: *const u8, len: i32) -> i32;
        pub fn get(kptr: *const u8, klen: i32, off: i32, vbuf: *mut u8, vcap: i32) -> i64;
        pub fn get_for_update(
            kptr: *const u8,
            klen: i32,
            off: i32,
            vbuf: *mut u8,
            vcap: i32,
        ) -> i64;
        pub fn put(kptr: *const u8, klen: i32, vptr: *const u8, vlen: i32) -> i32;
        pub fn delete(kptr: *const u8, klen: i32) -> i32;
        pub fn scan_open(
            lo_ptr: *const u8,
            lo_len: i32,
            hi_ptr: *const u8,
            hi_len: i32,
            flags: i32,
        ) -> i32;
        pub fn scan_next(h: i32, buf: *mut u8, cap: i32) -> i32;
        pub fn scan_entry_hint(h: i32) -> i64;
        pub fn scan_skip(h: i32) -> i32;
        pub fn scan_close(h: i32) -> i32;
    }
}

/// Host stubs so guest crates still type-check on the host.
#[cfg(not(target_arch = "wasm32"))]
mod sys {
    #![allow(unused_variables)]
    const MSG: &str = "fluent ABI is only available inside the database";
    pub unsafe fn input_len() -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn input_read(dst: *mut u8, cap: i32, off: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn output_write(ptr: *const u8, len: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn log(level: i32, ptr: *const u8, len: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn get(kptr: *const u8, klen: i32, off: i32, vbuf: *mut u8, vcap: i32) -> i64 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn get_for_update(
        kptr: *const u8,
        klen: i32,
        off: i32,
        vbuf: *mut u8,
        vcap: i32,
    ) -> i64 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn put(kptr: *const u8, klen: i32, vptr: *const u8, vlen: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn delete(kptr: *const u8, klen: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn scan_open(
        lo_ptr: *const u8,
        lo_len: i32,
        hi_ptr: *const u8,
        hi_len: i32,
        flags: i32,
    ) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn scan_next(h: i32, buf: *mut u8, cap: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn scan_entry_hint(h: i32) -> i64 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn scan_skip(h: i32) -> i32 {
        unimplemented!("{MSG}")
    }
    pub unsafe fn scan_close(h: i32) -> i32 {
        unimplemented!("{MSG}")
    }
}

/// The input blob this invocation was called with.
pub fn input() -> Vec<u8> {
    unsafe {
        let len = sys::input_len();
        if len <= 0 {
            return Vec::new();
        }
        let mut buf = vec![0u8; len as usize];
        let n = sys::input_read(buf.as_mut_ptr(), len, 0);
        buf.truncate(n.max(0) as usize);
        buf
    }
}

/// Append bytes to the invocation's result.
pub fn output(bytes: &[u8]) {
    unsafe {
        sys::output_write(bytes.as_ptr(), bytes.len() as i32);
    }
}

/// Emit a log line (host-side, size limited).
pub fn log(msg: &str) {
    unsafe {
        sys::log(0, msg.as_ptr(), msg.len() as i32);
    }
}

fn get_impl(key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>, i32> {
    unsafe {
        let call = |off: i32, buf: &mut [u8]| -> i64 {
            if for_update {
                sys::get_for_update(
                    key.as_ptr(),
                    key.len() as i32,
                    off,
                    buf.as_mut_ptr(),
                    buf.len() as i32,
                )
            } else {
                sys::get(
                    key.as_ptr(),
                    key.len() as i32,
                    off,
                    buf.as_mut_ptr(),
                    buf.len() as i32,
                )
            }
        };
        let mut buf = vec![0u8; 4096];
        let full = call(0, &mut buf);
        if full == errno::NOT_FOUND as i64 {
            return Ok(None);
        }
        if full < 0 {
            return Err(full as i32);
        }
        let full = full as usize;
        if full <= buf.len() {
            buf.truncate(full);
            return Ok(Some(buf));
        }
        // chunked read of a value larger than the probe buffer
        let mut out = Vec::with_capacity(full);
        out.extend_from_slice(&buf);
        while out.len() < full {
            let want = (full - out.len()).min(1 << 20);
            let mut chunk = vec![0u8; want];
            let r = call(out.len() as i32, &mut chunk);
            if r < 0 {
                return Err(r as i32);
            }
            let copied = want.min(full - out.len());
            chunk.truncate(copied);
            out.extend_from_slice(&chunk);
        }
        Ok(Some(out))
    }
}

/// Read a key at this invocation's snapshot (executors see their own writes
/// overlaid).
pub fn get(key: &[u8]) -> Option<Vec<u8>> {
    get_impl(key, false).unwrap_or(None)
}

/// Like [`get`], but the key joins the transaction's conflict set — commit
/// fails if anyone else writes it (executors only).
pub fn get_for_update(key: &[u8]) -> Result<Option<Vec<u8>>, i32> {
    get_impl(key, true)
}

/// Buffer a write into the transaction (executors only).
pub fn put(key: &[u8], value: &[u8]) -> Result<(), i32> {
    let r = unsafe {
        sys::put(
            key.as_ptr(),
            key.len() as i32,
            value.as_ptr(),
            value.len() as i32,
        )
    };
    if r == 0 {
        Ok(())
    } else {
        Err(r)
    }
}

/// Buffer a delete into the transaction (executors only).
pub fn delete(key: &[u8]) -> Result<(), i32> {
    let r = unsafe { sys::delete(key.as_ptr(), key.len() as i32) };
    if r == 0 {
        Ok(())
    } else {
        Err(r)
    }
}

/// Ordered scan over `[lo, hi)`. Entries arrive in host-packed batches — one
/// boundary crossing per buffer, not per pair.
pub struct Scan {
    h: i32,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    done: bool,
}

impl Scan {
    fn open(lo: Option<&[u8]>, hi: Option<&[u8]>, flags: i32) -> Result<Scan, i32> {
        let (lp, ll) = lo
            .map(|b| (b.as_ptr(), b.len() as i32))
            .unwrap_or((std::ptr::null(), 0));
        let (hp, hl) = hi
            .map(|b| (b.as_ptr(), b.len() as i32))
            .unwrap_or((std::ptr::null(), 0));
        let h = unsafe { sys::scan_open(lp, ll, hp, hl, flags) };
        if h < 0 {
            return Err(h);
        }
        Ok(Scan {
            h,
            buf: vec![0u8; 16 << 10],
            pos: 0,
            filled: 0,
            done: false,
        })
    }

    fn refill(&mut self) -> bool {
        loop {
            let r =
                unsafe { sys::scan_next(self.h, self.buf.as_mut_ptr(), self.buf.len() as i32) };
            if r == errno::ENOSPC {
                // one entry larger than the buffer: grow to the exact size
                let hint = unsafe { sys::scan_entry_hint(self.h) };
                if hint <= 0 {
                    self.done = true;
                    return false;
                }
                self.buf = vec![0u8; hint as usize];
                continue;
            }
            if r <= 0 {
                self.done = true;
                return false;
            }
            self.pos = 0;
            self.filled = r as usize;
            return true;
        }
    }

    /// Skip the entry the host is about to deliver — the escape hatch for
    /// values too large to buffer. Returns false when the scan is exhausted.
    pub fn skip_pending(&mut self) -> bool {
        unsafe { sys::scan_skip(self.h) == 1 }
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = *self.buf.get(self.pos)?;
            self.pos += 1;
            v |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Some(v);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }
}

impl Iterator for Scan {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.filled {
            if self.done || !self.refill() {
                return None;
            }
        }
        let klen = self.read_varint()? as usize;
        let vlen = self.read_varint()? as usize;
        if self.pos + klen + vlen > self.filled {
            self.done = true;
            return None;
        }
        let key = self.buf[self.pos..self.pos + klen].to_vec();
        self.pos += klen;
        let value = self.buf[self.pos..self.pos + vlen].to_vec();
        self.pos += vlen;
        Some((key, value))
    }
}

impl Drop for Scan {
    fn drop(&mut self) {
        unsafe {
            sys::scan_close(self.h);
        }
    }
}

pub fn scan(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32> {
    Scan::open(lo, hi, 0)
}

pub fn scan_rev(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32> {
    Scan::open(lo, hi, 1)
}

/// Scan every key with the given prefix.
pub fn scan_prefix(prefix: &[u8]) -> Result<Scan, i32> {
    let mut hi = prefix.to_vec();
    // smallest byte string greater than every prefixed key
    loop {
        match hi.last_mut() {
            None => return Scan::open(Some(prefix), None, 0),
            Some(0xff) => {
                hi.pop();
            }
            Some(b) => {
                *b += 1;
                break;
            }
        }
    }
    Scan::open(Some(prefix), Some(&hi), 0)
}

/// Export `$f` as the module entry point.
#[macro_export]
macro_rules! fluent_main {
    ($f:path) => {
        #[no_mangle]
        pub extern "C" fn run() -> i32 {
            $f()
        }
    };
}
