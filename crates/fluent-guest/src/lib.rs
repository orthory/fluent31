//! Guest-side SDK for fluent31 WASM modules ("fluentabi v2").
//!
//! A module declares its role(s) by which entry points it exports — the
//! export name IS the role. Write a typed function returning `Result` and
//! annotate it with the role — the attribute macro exports the entry point
//! and maps `Ok` to exit 0 (output encoded via [`IntoOutput`]) and
//! `Err(Fail)` to a non-zero exit with the message in the output buffer:
//!
//! ```ignore
//! #[fluent_guest::query]                      // exports `query` (read-only)
//! fn lookup(who: Vec<u8>) -> Result<Vec<u8>, fluent_guest::Fail> {
//!     fluent_guest::get(&who).ok_or(fluent_guest::Fail::new(1, "no such key"))
//! }
//!
//! #[fluent_guest::execute]                    // exports `execute` (transactional)
//! fn claim(input: Vec<u8>) -> Result<String, fluent_guest::Fail> { /* ... */ }
//! ```
//!
//! Trigger consumers take the trigger input instead: a keys-mode module
//! receives the coalesced touched keys, a changes-mode module the ordered
//! list of committed changes (see [`Change`]):
//!
//! ```ignore
//! #[fluent_guest::on_touch]                   // exports `on_touch`
//! fn index(keys: Vec<Vec<u8>>) -> Result<(), fluent_guest::Fail> {
//!     for key in keys { /* reconcile against current state */ }
//!     Ok(())
//! }
//!
//! #[fluent_guest::on_apply]                   // exports `on_apply`
//! fn feed(changes: Vec<fluent_guest::Change>) -> Result<(), fluent_guest::Fail> {
//!     for c in changes { /* filter, then index/materialize */ }
//!     Ok(())
//! }
//! ```
//!
//! The declarative [`fluent_query!`] / [`fluent_execute!`] /
//! [`fluent_on_touch!`] / [`fluent_on_apply!`] macros remain as the raw
//! layer for modules that want to speak exit codes directly.
//!
//! Build with `--target wasm32-unknown-unknown` as a `cdylib`, then install
//! the artifact with `db.install_module(name, bytes)`.

pub use fluent_guest_macros::{execute, on_apply, on_touch, query};

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

/// The touched keys of a keys-mode trigger invocation: parses the input
/// blob's packing (`[klen uvarint][key bytes]` repeated). A trigger event
/// means "this key was touched", not "here is what happened to it" — read
/// the key to decide (present = upsert your derived state, absent = remove
/// it). Returns None on malformed input (not invoked as a trigger).
pub fn trigger_keys() -> Option<Vec<Vec<u8>>> {
    parse_trigger_keys(&input())
}

/// [`trigger_keys`] over an explicit buffer (what `Vec<Vec<u8>>`'s
/// [`FromInput`] uses — a typed `#[fluent_guest::on_touch]` function
/// receives the parsed keys as its argument directly).
pub fn parse_trigger_keys(input: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    let mut pos = 0usize;
    while pos < input.len() {
        let (mut len, mut shift) = (0u64, 0u32);
        loop {
            let b = *input.get(pos)?;
            pos += 1;
            len |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
        let len = usize::try_from(len).ok()?;
        keys.push(input.get(pos..pos + len)?.to_vec());
        pos += len;
    }
    Some(keys)
}

// ---------------------------------------------------------------------------
// typed entry points (the #[fluent_guest::query] / #[execute] / #[on_touch]
// / #[on_apply] layer): Result-returning functions over FromInput/IntoOutput
// values
// ---------------------------------------------------------------------------

/// A guest failure: a non-zero exit code plus a human-readable message
/// (written to the output buffer, where callers surface it as
/// `guestOutputText`). Use distinct codes per failure class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fail {
    pub code: i32,
    pub message: String,
}

impl Fail {
    pub fn new(code: i32, message: impl Into<String>) -> Fail {
        Fail {
            code,
            message: message.into(),
        }
    }
}

/// `?` on string errors: exit code 1.
impl From<String> for Fail {
    fn from(message: String) -> Fail {
        Fail { code: 1, message }
    }
}

impl From<&str> for Fail {
    fn from(message: &str) -> Fail {
        Fail::new(1, message)
    }
}

/// Decode a typed value from the invocation's input blob.
pub trait FromInput: Sized {
    fn from_input(bytes: Vec<u8>) -> Result<Self, Fail>;
}

impl FromInput for Vec<u8> {
    fn from_input(bytes: Vec<u8>) -> Result<Self, Fail> {
        Ok(bytes)
    }
}

impl FromInput for String {
    fn from_input(bytes: Vec<u8>) -> Result<Self, Fail> {
        String::from_utf8(bytes).map_err(|_| Fail::new(3, "input is not utf-8"))
    }
}

/// One committed change, as delivered to a changes-mode trigger's
/// `on_apply` in commit order (see the engine's WASM.md, section 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A committed put. `value` is the written value, or `None` when it
    /// exceeded the engine's `trigger_inline_value` — read the key if you
    /// need it (the read reflects CURRENT state, which may already be
    /// newer than this change).
    Put {
        seqno: u64,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    /// A committed delete of `key`.
    Delete { seqno: u64, key: Vec<u8> },
}

impl Change {
    /// The commit seqno of the op this change describes: unique, and
    /// strictly increasing across the feed.
    pub fn seqno(&self) -> u64 {
        match self {
            Change::Put { seqno, .. } | Change::Delete { seqno, .. } => *seqno,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Change::Put { key, .. } | Change::Delete { key, .. } => key,
        }
    }
}

/// Parse a changes-mode trigger input: little-endian wire framing,
/// `[u32 count]` then per change `[u64 seqno][u8 kind][u32 klen][key]`
/// plus `[u32 vlen][value]` when kind = put (0). Kinds: 0 = put with
/// inline value, 1 = delete, 2 = put with the value elided.
/// Returns None on malformed input (not an on_apply invocation).
pub fn parse_changes(input: &[u8]) -> Option<Vec<Change>> {
    let u32_at = |pos: usize| -> Option<(u32, usize)> {
        let b: [u8; 4] = input.get(pos..pos + 4)?.try_into().ok()?;
        Some((u32::from_le_bytes(b), pos + 4))
    };
    let (count, mut pos) = u32_at(0)?;
    let mut out = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        let seq: [u8; 8] = input.get(pos..pos + 8)?.try_into().ok()?;
        let seqno = u64::from_le_bytes(seq);
        let kind = *input.get(pos + 8)?;
        let (klen, kstart) = u32_at(pos + 9)?;
        let key = input.get(kstart..kstart + klen as usize)?.to_vec();
        pos = kstart + klen as usize;
        out.push(match kind {
            0 => {
                let (vlen, vstart) = u32_at(pos)?;
                let value = input.get(vstart..vstart + vlen as usize)?.to_vec();
                pos = vstart + vlen as usize;
                Change::Put {
                    seqno,
                    key,
                    value: Some(value),
                }
            }
            2 => Change::Put {
                seqno,
                key,
                value: None,
            },
            1 => Change::Delete { seqno, key },
            _ => return None,
        });
    }
    (pos == input.len()).then_some(out)
}

/// The change list of an `on_apply` invocation; None when the input is not
/// one (symmetric with [`trigger_keys`] for keys-mode modules).
pub fn changes() -> Option<Vec<Change>> {
    parse_changes(&input())
}

impl FromInput for Vec<Change> {
    fn from_input(bytes: Vec<u8>) -> Result<Self, Fail> {
        parse_changes(&bytes).ok_or_else(|| Fail::new(3, "input is not a fluent change list"))
    }
}

/// The touched keys of a keys-mode (`on_touch`) trigger invocation.
impl FromInput for Vec<Vec<u8>> {
    fn from_input(bytes: Vec<u8>) -> Result<Self, Fail> {
        parse_trigger_keys(&bytes).ok_or_else(|| Fail::new(3, "input is not packed trigger keys"))
    }
}

/// Encode a typed value into the invocation's output bytes.
pub trait IntoOutput {
    fn into_output(self) -> Vec<u8>;
}

impl IntoOutput for Vec<u8> {
    fn into_output(self) -> Vec<u8> {
        self
    }
}

impl IntoOutput for String {
    fn into_output(self) -> Vec<u8> {
        self.into_bytes()
    }
}

/// No output (trigger modules usually have nothing to say on success).
impl IntoOutput for () {
    fn into_output(self) -> Vec<u8> {
        Vec::new()
    }
}

/// Glue behind the attribute macros: decode, call, encode, exit.
/// Not part of the public contract.
#[doc(hidden)]
pub fn __entry<T: FromInput, O: IntoOutput>(f: fn(T) -> Result<O, Fail>) -> i32 {
    let value = match T::from_input(input()) {
        Ok(value) => value,
        Err(fail) => return report(fail),
    };
    match f(value) {
        Ok(out) => {
            let bytes = out.into_output();
            if !bytes.is_empty() {
                output(&bytes);
            }
            0
        }
        Err(fail) => report(fail),
    }
}

fn report(fail: Fail) -> i32 {
    output(fail.message.as_bytes());
    // exit 0 means success/commit — a Fail must never map to it
    if fail.code == 0 {
        1
    } else {
        fail.code
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
        if self.pos >= self.filled
            && (self.done || !self.refill()) {
                return None;
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

/// Export `$f` as the module's raw read-only `query` entry point. Prefer
/// `#[fluent_guest::query]` on a typed function; this is the
/// exit-code-speaking escape hatch.
#[macro_export]
macro_rules! fluent_query {
    ($f:path) => {
        #[no_mangle]
        pub extern "C" fn query() -> i32 {
            $f()
        }
    };
}

/// Export `$f` as the module's raw transactional `execute` entry point.
/// Prefer `#[fluent_guest::execute]` on a typed function; this is the
/// exit-code-speaking escape hatch.
#[macro_export]
macro_rules! fluent_execute {
    ($f:path) => {
        #[no_mangle]
        pub extern "C" fn execute() -> i32 {
            $f()
        }
    };
}

/// Export `$f` as the module's raw `on_touch` entry point (keys-mode
/// triggers). Prefer `#[fluent_guest::on_touch]` on a typed function; this
/// is the exit-code-speaking escape hatch, paired with [`trigger_keys`].
#[macro_export]
macro_rules! fluent_on_touch {
    ($f:path) => {
        #[no_mangle]
        pub extern "C" fn on_touch() -> i32 {
            $f()
        }
    };
}

/// Export `$f` as the module's raw `on_apply` entry point (changes-mode
/// triggers). Prefer `#[fluent_guest::on_apply]` on a typed function;
/// this is the exit-code-speaking escape hatch, paired with [`changes`].
#[macro_export]
macro_rules! fluent_on_apply {
    ($f:path) => {
        #[no_mangle]
        pub extern "C" fn on_apply() -> i32 {
            $f()
        }
    };
}

/// Export a static JSON schema descriptor as the module's `describe`
/// entry point. Hosts that understand "fluentabi describe" (e.g. the
/// GraphQL server) call it at install/schema-build time to generate typed
/// API surface for the module.
#[macro_export]
macro_rules! fluent_describe {
    ($json:expr) => {
        #[no_mangle]
        pub extern "C" fn describe() -> i32 {
            $crate::output(($json).as_bytes());
            0
        }
    };
}
