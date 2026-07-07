//! fluent31 — an embedded LSM key-value engine with MVCC snapshots and
//! transactions, io_uring-backed IO on Linux, WiscKey-style key-value
//! separation, manual point-in-time checkpoints, and an in-database WASM
//! execution layer that replaces SQL.
//!
//! See DESIGN.md at the repository root for the full architecture.

mod batch;
mod block;
mod bloom;
mod cache;
mod checkpoint;
mod coding;
mod compaction;
mod config;
mod db;
pub mod edge;
mod error;
pub mod identity;
mod io;
mod iter;
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
#[cfg(feature = "wasm")]
pub use trigger::TriggerInfo;
pub use txn::Txn;
pub use types::{SeqNo, ValueKind};
#[cfg(feature = "wasm")]
pub use wasm::ModuleInfo;

pub use checkpoint::{restore_to, CheckpointInfo};
