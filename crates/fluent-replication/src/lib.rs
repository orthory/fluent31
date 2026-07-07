//! fluent-replication — the edge replication channel for fluent31.
//!
//! A separate binary protocol (own port, own opcode space; frame layout
//! shared with wire v1) that lets a small ephemeral edge replica hold a
//! scoped slice of one master: the LSM index fragments overlapping its key
//! range copied locally, values fetched lazily and cached, and committed
//! writes streamed in. Provenance is anchored on the master's
//! deterministic instance id — every (re)connect verifies it, and a
//! re-minted master (restore/fork) invalidates the edge wholesale.
//!
//! Full specification: REPLICATION.md at the repository root.

mod client;
pub mod proto;
mod server;

pub use client::{EdgeReplica, EdgeReplicaConfig, MasterInfo, ReplClient};
pub use server::{ReplServer, ReplServerConfig};
