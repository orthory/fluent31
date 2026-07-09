//! fluent-wire — the binary data-plane protocol for fluent31.
//!
//! Correlated request/response frames over TCP with out-of-order
//! completion: each request carries a client-allocated, per-connection
//! `request_id` echoed on its response, so one connection can hold many
//! in-flight operations (a slow `EXEC` never blocks the `GET`s behind it).
//! GraphQL (`fluent-graphql`) remains the general/typed/admin plane; this
//! is the heat lane: raw bytes, no encoding tax, stateless per request.
//!
//! Full specification: WIRE.md at the repository root.

mod backend;
mod client;
mod dispatch;
pub mod proto;
mod server;

pub use backend::WireBackend;
pub use client::{WireClient, WireError, WireResult};
pub use server::{ServerConfig, WireServer};
