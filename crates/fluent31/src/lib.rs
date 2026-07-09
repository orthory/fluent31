//! fluent31 — an embedded LSM key-value engine with MVCC snapshots and
//! transactions, io_uring-backed IO on Linux, WiscKey-style key-value
//! separation, copy-on-write database forks, and an in-database WASM
//! execution layer that replaces SQL.
//!
//! See DESIGN.md at the repository root for the full architecture.

mod batch;
mod block;
mod bloom;
mod cache;
mod coding;
mod compaction;
mod config;
mod db;
mod error;
mod fork;
mod io;
mod iter;
mod manifest;
mod memtable;
mod table;
mod txn;
mod types;
mod version;
mod vlog;
mod wal;
#[cfg(feature = "wasm")]
mod wasm;

pub use batch::WriteBatch;
pub use config::{Compression, IoBackend, Options, SyncMode};
pub use db::{Db, DbStats, Snapshot};
pub use error::{Error, Result};
pub use iter::DbIterator;
pub use txn::Txn;
#[cfg(feature = "wasm")]
pub use wasm::ModuleInfo;

pub use fork::{clone_to, list_at as list_forks_at, ForkInfo};
