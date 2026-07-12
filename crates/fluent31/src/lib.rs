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
pub mod edge;
mod error;
mod fork;
pub mod identity;
mod io;
mod iter;
pub mod journal;
mod manifest;
mod memtable;
mod table;
#[cfg(feature = "wasm")]
mod trigger;
mod txn;
mod types;
mod version;
mod vlog;
mod wal;
#[cfg(feature = "wasm")]
mod wasm;

pub use batch::WriteBatch;
pub use config::{Compression, IoBackend, Options, SyncMode};
pub use db::{
    Db, DbStats, SliceManifest, SliceRun, SliceTable, Snapshot, StreamEntry, StreamEvent,
    Subscription,
};
pub use error::{Error, Result};
pub use identity::{InstanceId, StoreIdentity};
pub use iter::DbIterator;
pub use journal::{Journal, JournalConfig, JournalStats, RebuildReport};
pub use manifest::PinInfo;

/// IO backend traits, exposed only under the `fault-injection` feature so
/// durability tests can supply a custom `Io` to `Db::open_with_io`.
#[cfg(feature = "fault-injection")]
pub use io::{DbFile, Io, ReadReq};
#[cfg(feature = "wasm")]
pub use trigger::{TriggerInfo, TriggerMode};
pub use txn::Txn;
pub use types::{SeqNo, ValueKind};
#[cfg(feature = "wasm")]
pub use wasm::ModuleInfo;

pub use fork::{list_at as list_forks_at, restore_to, ForkInfo};
