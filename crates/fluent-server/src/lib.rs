//! Server mode for fluent31: every network plane over one store, in one
//! process.
//!
//! The engine flocks its directory, so GraphQL, the wire pipe, and the
//! replication master cannot run as separate processes against the same
//! data. This crate is the one-process composition: a single [`Db`] handle
//! shared by
//!
//! - **GraphQL** (HTTP, default `:8317`) — the typed/admin plane: direct
//!   operations, per-module typed WASM root fields, forks at
//!   `/graphql/<instanceId>`;
//! - **wire v1** (TCP, default `:8427`) — the data-plane pipe: raw bytes,
//!   correlated frames, out-of-order completion (see `WIRE.md`);
//! - **replication v1** (TCP, default `:8428`) — the join point where full
//!   replicas and key-range edge caches attach (see `REPLICATION.md`).
//!   Replication's provenance model needs the deterministic store
//!   identity, so this plane is served only when the store is named
//!   (`Options::store_name`, persisted after first adoption).
//!
//! Each plane keeps its own blocking-pool gate (GraphQL 128 read + 32
//! write, wire 128 + 32, replication 64); the combined worst case of 384
//! parked engine calls stays under tokio's default 512 blocking threads.

mod config;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use fluent31::{Db, Options};
use fluent_graphql::{InstanceRegistry, RegistryConfig, SchemaManager};
use fluent_replication::{ReplServer, ReplServerConfig};
use fluent_wire::{ServerConfig as WireConfig, WireServer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use config::{
    parse_sync, CompressionKey, ConfigError, EngineSection, FileConfig, GraphqlSection,
    IoBackendKey, ListenSection, ReplicationSection, WireSection,
};

/// Listen addresses plus each composed plane's tunables. Every plane is
/// always served; replication additionally needs a named store and is
/// skipped (leaving [`Server::replication_addr`] `None`) when the store
/// is anonymous.
pub struct ServerConfig {
    pub graphql_addr: String,
    pub wire_addr: String,
    pub replication_addr: String,
    /// GraphQL HTTP request body cap in bytes.
    pub max_body_bytes: usize,
    /// Fork-instance registry tuning (GraphQL plane).
    pub registry: RegistryConfig,
    /// Wire plane limits.
    pub wire: WireConfig,
    /// Replication plane limits.
    pub replication: ReplServerConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            graphql_addr: "127.0.0.1:8317".into(),
            wire_addr: "127.0.0.1:8427".into(),
            replication_addr: "127.0.0.1:8428".into(),
            max_body_bytes: 32 << 20,
            registry: RegistryConfig::default(),
            wire: WireConfig::default(),
            replication: ReplServerConfig::default(),
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
    pub wire_addr: SocketAddr,
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
        let wire_listener = bind("wire", &cfg.wire_addr).await?;
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
        let wire_addr = local("wire", &cfg.wire_addr, &wire_listener)?;
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
                eprintln!("fluent-server: graphql plane failed: {e}");
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

        let wire = WireServer::new(db.clone(), cfg.wire);
        accept_tasks.push(tokio::spawn(async move {
            if let Err(e) = wire.serve(wire_listener).await {
                eprintln!("fluent-server: wire plane failed: {e}");
            }
        }));

        if let (Some(repl), Some(listener)) = (repl, repl_listener) {
            accept_tasks.push(tokio::spawn(async move {
                if let Err(e) = repl.serve(listener).await {
                    eprintln!("fluent-server: replication plane failed: {e}");
                }
            }));
        }

        Ok(Server {
            db,
            graphql_addr,
            wire_addr,
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
    /// requests. In-flight wire/replication connections are severed when
    /// the process (or runtime) goes down — the WAL keeps the store
    /// consistent on reopen.
    pub async fn shutdown(self) {
        for t in &self.accept_tasks {
            t.abort();
        }
        let _ = self.graphql_stop.send(());
        let _ = self.graphql_task.await;
    }
}
