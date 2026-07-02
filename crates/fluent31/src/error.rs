use std::fmt;

/// Unified error type for all engine operations.
#[derive(Debug)]
pub enum Error {
    /// Underlying IO failure.
    Io(std::io::Error),
    /// On-disk data failed validation (bad CRC, truncated structure, bad magic).
    Corruption(String),
    /// Caller misuse: reserved key prefix, bad checkpoint name, unknown module...
    InvalidArgument(String),
    /// Optimistic transaction lost a first-committer-wins race.
    Conflict,
    /// The database has been shut down.
    Closed,
    /// A background thread (flush/compaction) failed; the store is
    /// read-only-degraded until reopened.
    Background(String),
    /// WASM engine/module failure (compile error, trap, limit exhaustion).
    Wasm(String),
    /// A WASM executor/query ran fine but the guest returned a non-zero code.
    GuestFailed { code: i32, output: Vec<u8> },
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Conflict => write!(f, "transaction conflict"),
            Error::Closed => write!(f, "database closed"),
            Error::Background(m) => write!(f, "background error: {m}"),
            Error::Wasm(m) => write!(f, "wasm: {m}"),
            Error::GuestFailed { code, .. } => write!(f, "guest module failed with code {code}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub(crate) fn corrupt(msg: impl Into<String>) -> Error {
    Error::Corruption(msg.into())
}
