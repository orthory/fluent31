//! GraphQL interface for fluent31.
//!
//! One schema exposes both the direct engine operations (`get`, `scan`,
//! `put`, `delete`, `writeBatch`, module/checkpoint/maintenance admin) and
//! the registered in-database WASM programs: read-only modules run through
//! `Query.wasm`, transactional executors through `Mutation.wasmExecute`.
//!
//! Consistency model: every GraphQL *query* operation lazily pins one MVCC
//! [`fluent31::Snapshot`] the first time a read field needs it, and every
//! read field of that operation (`get`, `scan`, `wasm`, `snapshotSeqno`)
//! executes against that same snapshot — one request, one point-in-time
//! view. Mutation fields run serially in document order, each against the
//! then-current state, per the GraphQL spec.
//!
//! All engine calls are synchronous and may block on IO or write stalls, so
//! resolvers hop onto the blocking thread pool via
//! [`tokio::task::spawn_blocking`] — gated by [`EnginePermits`] so stalled
//! writers cannot exhaust the pool and starve reads (or vice versa).

mod bytes;
mod error;
mod mutation;
mod query;
mod types;

use std::sync::{Arc, OnceLock};

use async_graphql::{Context, EmptySubscription, Request, Schema, SchemaBuilder};
use fluent31::{Db, Snapshot};
use tokio::sync::Semaphore;

pub use bytes::{Bytes, BytesInput};
pub use error::engine_err;
pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use types::U64;

pub type FluentSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// The engine parks stalled writers on 100ms condvar waits when flush or
/// compaction falls behind (`wait_for_space`), holding their blocking-pool
/// thread the whole time. Unbounded, ~512 stalled writers would saturate
/// tokio's blocking pool and queue every read behind them. These caps keep
/// the pool's worst case at READ_PERMITS + WRITE_PERMITS threads, so one
/// class can never starve the other; excess calls queue on the semaphore.
pub(crate) struct EnginePermits {
    read: Semaphore,
    write: Semaphore,
}

const READ_PERMITS: usize = 128;
const WRITE_PERMITS: usize = 32;

impl EnginePermits {
    fn new() -> Self {
        EnginePermits {
            read: Semaphore::new(READ_PERMITS),
            write: Semaphore::new(WRITE_PERMITS),
        }
    }
}

/// Per-request cell holding the query operation's pinned MVCC snapshot.
/// Created lazily by the first read field, shared by the rest, released
/// (deregistered from the engine) when the request's data map drops.
#[derive(Default)]
pub struct SnapCell(OnceLock<Arc<Snapshot>>);

impl SnapCell {
    pub(crate) fn pin(&self, db: &Db) -> Arc<Snapshot> {
        self.0.get_or_init(|| Arc::new(db.snapshot())).clone()
    }
}

/// Attach the per-request data every execution needs. All entry points
/// (the HTTP handler, tests) must route requests through this; read fields
/// fail loudly if it was skipped.
pub fn prepare(req: impl Into<Request>) -> Request {
    req.into().data(SnapCell::default())
}

fn builder() -> SchemaBuilder<QueryRoot, MutationRoot, EmptySubscription> {
    // The permit semaphores are the primary defense against alias
    // amplification; the limits below just reject absurd documents outright
    // while leaving room for GraphiQL's introspection query.
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .limit_depth(32)
        .limit_complexity(5_000)
}

/// Build the schema over an open database handle.
pub fn build_schema(db: Arc<Db>) -> FluentSchema {
    builder()
        .data(db)
        .data(Arc::new(EnginePermits::new()))
        .finish()
}

/// Schema SDL, independent of any database.
pub fn sdl() -> String {
    builder().finish().sdl()
}

pub(crate) fn db(ctx: &Context<'_>) -> async_graphql::Result<Arc<Db>> {
    Ok(ctx.data::<Arc<Db>>()?.clone())
}

pub(crate) fn snap(ctx: &Context<'_>) -> async_graphql::Result<Arc<Snapshot>> {
    let db = ctx.data::<Arc<Db>>()?;
    Ok(ctx.data::<SnapCell>()?.pin(db))
}

enum Class {
    Read,
    Write,
}

async fn blocking<T, F>(ctx: &Context<'_>, class: Class, f: F) -> async_graphql::Result<T>
where
    F: FnOnce() -> fluent31::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let permits = ctx.data::<Arc<EnginePermits>>()?;
    let sem = match class {
        Class::Read => &permits.read,
        Class::Write => &permits.write,
    };
    let _permit = sem
        .acquire()
        .await
        .map_err(|e| async_graphql::Error::new(format!("engine gate closed: {e}")))?;
    match tokio::task::spawn_blocking(f).await {
        Ok(r) => r.map_err(engine_err),
        Err(e) => Err(async_graphql::Error::new(format!(
            "engine worker failed: {e}"
        ))),
    }
}

/// Run a blocking read-path engine call off the async executor.
pub(crate) async fn blocking_read<T, F>(ctx: &Context<'_>, f: F) -> async_graphql::Result<T>
where
    F: FnOnce() -> fluent31::Result<T> + Send + 'static,
    T: Send + 'static,
{
    blocking(ctx, Class::Read, f).await
}

/// Run a blocking write-path engine call (anything that can hit the
/// engine's write stall or a maintenance lock) off the async executor.
pub(crate) async fn blocking_write<T, F>(ctx: &Context<'_>, f: F) -> async_graphql::Result<T>
where
    F: FnOnce() -> fluent31::Result<T> + Send + 'static,
    T: Send + 'static,
{
    blocking(ctx, Class::Write, f).await
}
