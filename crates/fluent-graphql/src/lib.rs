//! GraphQL interface for fluent31.
//!
//! One dynamically-built schema exposes three layers:
//!
//! 1. **Direct operations** — `get`, `scan`, `put`, `delete`, `writeBatch`,
//!    module/maintenance admin (see `builtins.rs`).
//! 2. **Generic WASM access** — `Query.wasm` / `Mutation.wasmExecute` run
//!    any installed module with raw byte input/output.
//! 3. **Typed module fields** — a module exporting `describe` (fluentabi v1
//!    describe, see `descriptor.rs`) becomes its own typed root field:
//!    `kind: "query"` modules on Query, `kind: "execute"` on Mutation. The
//!    schema is rebuilt and hot-swapped whenever `installModule` /
//!    `uninstallModule` changes the module set.
//!
//! Consistency model: every GraphQL *query* operation lazily pins one MVCC
//! [`fluent31::Snapshot`] the first time a read field needs it, and every
//! read field of that operation — direct or WASM-typed — executes against
//! that same snapshot. Mutation fields run serially in document order.
//!
//! All engine calls are synchronous and may block on IO or write stalls, so
//! resolvers hop onto the blocking thread pool via
//! [`tokio::task::spawn_blocking`] — gated by [`EnginePermits`] so stalled
//! writers cannot exhaust the pool and starve reads (or vice versa).

mod builtins;
mod bytes;
mod descriptor;
mod error;
mod modules;
mod registry;
mod router;
mod schema;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock, Weak};

use async_graphql::dynamic::Schema;
use async_graphql::Request;
use fluent31::{Db, Snapshot};
use tokio::sync::Semaphore;

pub use descriptor::{parse_descriptor, ModuleSchema};
pub use error::engine_err;
pub use registry::{InstanceRegistry, RegistryConfig, ResolveError};
pub use router::router;

/// The engine parks stalled writers on 100ms condvar waits when flush or
/// compaction falls behind (`wait_for_space`), holding their blocking-pool
/// thread the whole time. Unbounded, ~512 stalled writers would saturate
/// tokio's blocking pool and queue every read behind them. These caps keep
/// the pool's worst case at READ_PERMITS + WRITE_PERMITS threads, so one
/// class can never starve the other; excess calls queue on the semaphore.
///
/// One pool per served *tree*: fork instances share the primary's permits
/// (see [`InstanceRegistry`]), so the number of open instances cannot
/// multiply the blocking-pool worst case.
#[derive(Clone)]
pub(crate) struct EnginePermits {
    pub(crate) read: Arc<Semaphore>,
    pub(crate) write: Arc<Semaphore>,
}

const READ_PERMITS: usize = 128;
const WRITE_PERMITS: usize = 32;

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

/// A module's schema status as of the last rebuild: a validated typed
/// declaration, a validation failure (module still reachable via the
/// generic `wasm`/`wasmExecute` fields), or untyped (no `describe` export).
#[derive(Clone)]
pub(crate) enum ModuleStatus {
    Typed(Arc<ModuleSchema>),
    Invalid(String),
    Untyped,
}

/// What a module's `describe` export produced, cached by content hash so a
/// rebuild only re-executes describe for modules whose bytes changed
/// (rebuilds are otherwise O(modules) untrusted WASM runs).
#[derive(Clone)]
pub(crate) enum DescribeOutcome {
    Described(Arc<ModuleSchema>),
    DescribeError(String),
    NoDescribe,
}

/// Owns the database handle and the current dynamically-built schema;
/// rebuilds and hot-swaps the schema when the module set changes.
pub struct SchemaManager {
    pub(crate) db: Arc<Db>,
    pub(crate) permits: EnginePermits,
    schema: RwLock<Schema>,
    pub(crate) statuses: RwLock<BTreeMap<String, ModuleStatus>>,
    /// Per-module describe results keyed by content hash (see
    /// [`DescribeOutcome`]); guarded by `rebuild_lock` callers.
    describe_cache: RwLock<BTreeMap<String, (u128, DescribeOutcome)>>,
    /// Serializes install/uninstall + rebuild so two concurrent installs
    /// cannot swap in a schema that misses one of them.
    pub(crate) rebuild_lock: tokio::sync::Mutex<()>,
    /// Handed to resolvers via schema data (Weak: the schema itself lives
    /// inside this manager — a strong Arc would be a reference cycle).
    weak: Weak<SchemaManager>,
    /// Back-reference to the registry serving this manager, when one is
    /// (Weak: the registry holds this manager). `deleteFork` uses it to
    /// close a served fork before deleting it.
    registry: RwLock<Option<Weak<registry::RegistryShared>>>,
}

impl SchemaManager {
    /// Open the manager over a database handle: runs every installed
    /// module's `describe` once and builds the initial schema.
    pub fn new(db: Arc<Db>) -> Result<Arc<SchemaManager>, fluent31::Error> {
        Self::with_permits(
            db,
            EnginePermits {
                read: Arc::new(Semaphore::new(READ_PERMITS)),
                write: Arc::new(Semaphore::new(WRITE_PERMITS)),
            },
        )
    }

    /// As [`SchemaManager::new`] but sharing an existing permit pool —
    /// used by the instance registry so every instance of one served tree
    /// draws from the primary's caps.
    pub(crate) fn with_permits(
        db: Arc<Db>,
        permits: EnginePermits,
    ) -> Result<Arc<SchemaManager>, fluent31::Error> {
        let cache = schema::collect_outcomes(&db, &BTreeMap::new())?;
        let statuses = schema::statuses_from_outcomes(&cache);
        Ok(Arc::new_cyclic(|weak| SchemaManager {
            db,
            permits,
            schema: RwLock::new(schema::build(weak.clone(), &statuses)),
            statuses: RwLock::new(statuses),
            describe_cache: RwLock::new(cache),
            rebuild_lock: tokio::sync::Mutex::new(()),
            weak: weak.clone(),
            registry: RwLock::new(None),
        }))
    }

    pub(crate) fn permit_handles(&self) -> EnginePermits {
        self.permits.clone()
    }

    pub(crate) fn attach_registry(&self, reg: Weak<registry::RegistryShared>) {
        *self.registry.write().unwrap() = Some(reg);
    }

    pub(crate) fn registry_shared(&self) -> Option<Arc<registry::RegistryShared>> {
        self.registry.read().unwrap().as_ref().and_then(Weak::upgrade)
    }

    /// The underlying database handle (tests and embedders; GraphQL-side
    /// installs should go through the mutations so the schema rebuilds).
    pub fn db_handle(&self) -> &Db {
        &self.db
    }

    /// The current schema (cheap Arc-backed clone).
    pub fn schema(&self) -> Schema {
        self.schema.read().unwrap().clone()
    }

    /// Re-describe changed modules (cached by content hash) and hot-swap
    /// the schema. Called under `rebuild_lock`.
    pub(crate) fn rebuild(&self) -> Result<(), fluent31::Error> {
        let prev = self.describe_cache.read().unwrap().clone();
        let cache = schema::collect_outcomes(&self.db, &prev)?;
        let statuses = schema::statuses_from_outcomes(&cache);
        let next = schema::build(self.weak.clone(), &statuses);
        *self.describe_cache.write().unwrap() = cache;
        *self.statuses.write().unwrap() = statuses;
        *self.schema.write().unwrap() = next;
        Ok(())
    }

    /// Execute a request against the current schema with per-request data
    /// attached. All entry points (HTTP handler, tests) go through this.
    pub async fn execute(&self, req: impl Into<Request>) -> async_graphql::Response {
        let req = req.into().data(SnapCell::default());
        self.schema().execute(req).await
    }
}

/// Schema SDL for the built-in surface (no database, no modules).
pub fn base_sdl() -> String {
    schema::build(Weak::new(), &BTreeMap::new()).sdl()
}
