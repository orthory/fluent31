//! Server mode for fluent31: one process, one of two roles.
//!
//! **Store server** ([`Server`]) — every network plane over one store.
//! The engine flocks its directory, so GraphQL and the replication master
//! cannot run as separate processes against the same data. This crate is
//! the one-process composition: a single [`Db`] handle shared by
//!
//! - **GraphQL** (HTTP, default `:8317`) — the typed/admin plane: direct
//!   operations, per-module typed WASM root fields, forks at
//!   `/graphql/<instanceId>`;
//! - **replication** (TCP, default `:8428`) — the join point where full
//!   replicas and key-range edge caches attach (see `REPLICATION.md`).
//!   Replication's provenance model needs the deterministic store
//!   identity, so this plane is served only when the store is named
//!   (`Options::store_name`, persisted after first adoption).
//!
//! **Edge server** ([`EdgeServer`]) — a networked edge: one
//! [`EdgeReplica`] attached to a master's replication plane, serving the
//! read-only edge GraphQL surface (`get`/`scan`, scope-clamped — see
//! `fluent_graphql::edge_router`) over HTTP. No store of record, no
//! replication join point, no journal.
//!
//! Each plane keeps its own blocking-pool gate (GraphQL 128 read + 32
//! write, replication 64); the combined worst case of 224 parked engine
//! calls stays under tokio's default 512 blocking threads.

mod config;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use fluent31::edge::EdgeStore;
use fluent31::{Db, Options};
use fluent_graphql::{InstanceRegistry, RegistryConfig, SchemaManager};
use fluent_replication::{EdgeReplica, ReplServer, ReplServerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub use config::{
    parse_sync, BytesSpec, CompressionKey, ConfigError, EdgeSection, EngineSection, FileConfig,
    GraphqlSection, IoBackendKey, JournalSection, ListenSection, LogSection, ReplicationSection,
};

/// Listen addresses plus each composed plane's tunables. Every plane is
/// always served; replication additionally needs a named store and is
/// skipped (leaving [`Server::replication_addr`] `None`) when the store
/// is anonymous.
pub struct ServerConfig {
    pub graphql_addr: String,
    pub replication_addr: String,
    /// GraphQL HTTP request body cap in bytes.
    pub max_body_bytes: usize,
    /// Fork-instance registry tuning (GraphQL plane).
    pub registry: RegistryConfig,
    /// Replication plane limits.
    pub replication: ReplServerConfig,
    /// Period of the stats heartbeat (an INFO line per open store plus
    /// the fork registry's occupancy); zero turns it off.
    pub stats_every: std::time::Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            graphql_addr: "127.0.0.1:8317".into(),
            replication_addr: "127.0.0.1:8428".into(),
            max_body_bytes: 32 << 20,
            registry: RegistryConfig::default(),
            replication: ReplServerConfig::default(),
            stats_every: std::time::Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
pub enum StartError {
    Engine(fluent31::Error),
    Bind {
        plane: &'static str,
        addr: String,
        err: std::io::Error,
    },
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::Engine(e) => write!(f, "{e}"),
            StartError::Bind { plane, addr, err } => {
                write!(f, "cannot listen on {addr} ({plane} plane): {err}")
            }
        }
    }
}

impl std::error::Error for StartError {}

/// A running server: the bound addresses plus the tasks serving them.
/// All planes answer against the one `Db` passed to [`Server::start`].
pub struct Server {
    db: Arc<Db>,
    pub graphql_addr: SocketAddr,
    /// `None` when the store is unnamed (replication plane not served).
    pub replication_addr: Option<SocketAddr>,
    graphql_task: JoinHandle<()>,
    graphql_stop: tokio::sync::oneshot::Sender<()>,
    accept_tasks: Vec<JoinHandle<()>>,
}

async fn bind(plane: &'static str, addr: &str) -> Result<TcpListener, StartError> {
    TcpListener::bind(addr).await.map_err(|err| StartError::Bind {
        plane,
        addr: addr.to_string(),
        err,
    })
}

impl Server {
    /// Bind every plane, then start serving on the current runtime.
    /// Nothing is served unless all binds (and the replication identity
    /// check, when applicable) succeed. `root_dir`/`opts` mirror the
    /// arguments `db` was opened with — the fork registry needs them to
    /// open instances on demand.
    pub async fn start(
        db: Arc<Db>,
        root_dir: impl Into<PathBuf>,
        opts: Options,
        cfg: ServerConfig,
    ) -> Result<Server, StartError> {
        // runs every installed module's `describe`: blocking WASM work
        let mgr = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || SchemaManager::new(db))
                .await
                .expect("schema init panicked")
                .map_err(StartError::Engine)?
        };
        // forks carry their own identity, fixed at fork time; opening them
        // with the primary's store_name would fail the identity check
        let fork_opts = Options {
            store_name: None,
            ..opts
        };
        let registry = InstanceRegistry::new(mgr, root_dir, fork_opts, cfg.registry.clone());

        let repl = match db.identity() {
            Some(_) => Some(ReplServer::new(db.clone(), cfg.replication).map_err(StartError::Engine)?),
            None => None,
        };

        let graphql_listener = bind("graphql", &cfg.graphql_addr).await?;
        let repl_listener = match &repl {
            Some(_) => Some(bind("replication", &cfg.replication_addr).await?),
            None => None,
        };
        let local = |plane: &'static str, addr: &str, l: &TcpListener| {
            l.local_addr().map_err(|err| StartError::Bind {
                plane,
                addr: addr.to_string(),
                err,
            })
        };
        let graphql_addr = local("graphql", &cfg.graphql_addr, &graphql_listener)?;
        let replication_addr = match &repl_listener {
            Some(l) => Some(local("replication", &cfg.replication_addr, l)?),
            None => None,
        };

        let mut accept_tasks = Vec::new();

        let app = fluent_graphql::router(registry.clone(), cfg.max_body_bytes);
        let (graphql_stop, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let graphql_task = tokio::spawn(async move {
            let shutdown = async move {
                stop_rx.await.ok();
            };
            if let Err(e) = axum::serve(graphql_listener, app)
                .with_graceful_shutdown(shutdown)
                .await
            {
                error!(error = %e, "graphql plane failed");
            }
        });

        // close fork instances nobody has touched in a while
        accept_tasks.push(tokio::spawn({
            let registry = registry.clone();
            async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    tick.tick().await;
                    registry.evict_idle();
                }
            }
        }));
        if !cfg.stats_every.is_zero() {
            accept_tasks.push(tokio::spawn(fluent_graphql::stats_heartbeat(
                registry.clone(),
                cfg.stats_every,
            )));
        }

        if let (Some(repl), Some(listener)) = (repl, repl_listener) {
            accept_tasks.push(tokio::spawn(async move {
                if let Err(e) = repl.serve(listener).await {
                    error!(error = %e, "replication plane failed");
                }
            }));
        }

        Ok(Server {
            db,
            graphql_addr,
            replication_addr,
            graphql_task,
            graphql_stop,
            accept_tasks,
        })
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Stop accepting on every plane and drain in-flight GraphQL
    /// requests. In-flight replication connections are severed when the
    /// process (or runtime) goes down — the WAL keeps the store
    /// consistent on reopen.
    pub async fn shutdown(self) {
        for t in &self.accept_tasks {
            t.abort();
        }
        let _ = self.graphql_stop.send(());
        let _ = self.graphql_task.await;
    }
}

// ---------------------------------------------------------------------------
// Edge role
// ---------------------------------------------------------------------------

/// The edge plane's listen address and limits — the edge role's
/// counterpart to [`ServerConfig`], carrying only what an edge serves
/// (one GraphQL plane; no replication join point, no fork registry).
pub struct EdgeServerConfig {
    pub graphql_addr: String,
    /// GraphQL HTTP request body cap in bytes.
    pub max_body_bytes: usize,
    /// Period of the stats heartbeat (an INFO line with the replica's
    /// [`fluent31::edge::EdgeStats`]); zero turns it off.
    pub stats_every: std::time::Duration,
}

impl Default for EdgeServerConfig {
    fn default() -> Self {
        EdgeServerConfig {
            graphql_addr: "127.0.0.1:8317".into(),
            max_body_bytes: 32 << 20,
            stats_every: std::time::Duration::from_secs(60),
        }
    }
}

/// fluent-graphql reads through this so every request sees the replica's
/// CURRENT store — a re-sync (lag cutoff, master swap) replaces the
/// [`EdgeStore`] under a live server.
struct ReplicaStores(Arc<EdgeReplica>);

impl fluent_graphql::EdgeStoreProvider for ReplicaStores {
    fn store(&self) -> Arc<EdgeStore> {
        self.0.store()
    }
}

/// A running edge server: the read-only edge GraphQL plane over one
/// replica attachment (see `fluent_graphql::edge_router` for what the
/// plane serves and refuses).
pub struct EdgeServer {
    replica: Arc<EdgeReplica>,
    pub graphql_addr: SocketAddr,
    graphql_task: JoinHandle<()>,
    graphql_stop: tokio::sync::oneshot::Sender<()>,
    stats_task: Option<JoinHandle<()>>,
}

impl EdgeServer {
    /// Bind and start serving on the current runtime. The replica is
    /// attached (and its initial sync complete) before this is called —
    /// nothing binds until the edge can answer with a complete scoped
    /// view.
    pub async fn start(
        replica: Arc<EdgeReplica>,
        cfg: EdgeServerConfig,
    ) -> Result<EdgeServer, StartError> {
        let listener = bind("graphql", &cfg.graphql_addr).await?;
        let graphql_addr = listener.local_addr().map_err(|err| StartError::Bind {
            plane: "graphql",
            addr: cfg.graphql_addr.clone(),
            err,
        })?;

        let provider = Arc::new(ReplicaStores(replica.clone()));
        let app = fluent_graphql::edge_router(provider, cfg.max_body_bytes);
        let (graphql_stop, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let graphql_task = tokio::spawn(async move {
            let shutdown = async move {
                stop_rx.await.ok();
            };
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
            {
                error!(error = %e, "edge graphql plane failed");
            }
        });

        let stats_task = (!cfg.stats_every.is_zero())
            .then(|| tokio::spawn(edge_stats_heartbeat(replica.clone(), cfg.stats_every)));

        Ok(EdgeServer {
            replica,
            graphql_addr,
            graphql_task,
            graphql_stop,
            stats_task,
        })
    }

    pub fn replica(&self) -> &Arc<EdgeReplica> {
        &self.replica
    }

    /// Stop accepting and drain in-flight GraphQL requests. The replica
    /// itself is stopped by dropping the last [`EdgeReplica`] handle.
    pub async fn shutdown(self) {
        if let Some(t) = &self.stats_task {
            t.abort();
        }
        let _ = self.graphql_stop.send(());
        let _ = self.graphql_task.await;
    }
}

/// One INFO line with the replica's [`fluent31::edge::EdgeStats`] every
/// `every` — the edge role's counterpart to
/// [`fluent_graphql::stats_heartbeat`]. The first line comes after one
/// period, not at start (the attach already reported the opening state).
async fn edge_stats_heartbeat(replica: Arc<EdgeReplica>, every: std::time::Duration) {
    let mut tick = tokio::time::interval(every);
    tick.tick().await; // an interval's first tick is immediate
    loop {
        tick.tick().await;
        let s = replica.store().stats();
        info!(
            frontier_seqno = s.frontier_seqno,
            flushed_seqno = s.flushed_seqno,
            fragments = s.fragments,
            overlay_bytes = s.overlay_bytes,
            value_cache_bytes = s.value_cache_bytes,
            "edge stats"
        );
    }
}
