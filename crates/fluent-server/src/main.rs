//! fluent-server binary: formal server mode — one process, one store,
//! every network plane.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Journal, Options, SyncMode};
use fluent_replication::EdgeReplica;
use fluent_server::{
    parse_sync, BytesSpec, EdgeSection, EdgeServer, EdgeServerConfig, FileConfig, GraphqlSection,
    JournalSection, ListenSection, Server, ServerConfig,
};
use tracing::{error, info, warn};

// The process allocates in bursts spread across many short-lived threads
// (per-request blocking work, each open fork's engine threads, WASM
// invocations), and must hand freed memory back to the OS afterwards.
// glibc malloc cannot: it grows one arena per concurrently allocating
// thread and keeps each arena at its high-water mark, so resident memory
// ratchets up with every burst that lands on a fresh arena. mimalloc
// returns freed pages to the OS regardless of where they sit, keeping RSS
// tied to live data rather than to the history of peaks.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "\
usage: fluent-server <db-dir> [--config FILE] [--store-name NAME]
                     [--graphql ADDR:PORT] [--replication ADDR:PORT]
                     [--sync always|never|periodic:<ms>] [--max-body-bytes N]
                     [--journal DIR]
       fluent-server <cache-dir> --edge-master ADDR:PORT
                     [--edge-scope-lo TEXT] [--edge-scope-hi TEXT]
                     [--config FILE] [--graphql ADDR:PORT] [--max-body-bytes N]
       fluent-server --print-schema | --print-edge-schema

store server — serves every plane of one store in one process:
  graphql      HTTP, default 127.0.0.1:8317 — typed/admin plane, GraphiQL at /
  replication  TCP,  default 127.0.0.1:8428 — join point for replicas and
               key-range edge caches (REPLICATION.md); needs a named store:
               pass --store-name once, the name persists

edge server (--edge-master, or an [edge] config section) — attaches an
  edge replica to a master's replication plane and serves the read-only
  edge GraphQL surface (get/scan, clamped to the scope) on the graphql
  address. <cache-dir> is the replica's local cache (wiped on attach).
  --edge-scope-lo/--edge-scope-hi take text keys; hex keys go in the
  [edge] section. Store-of-record settings are refused in this role.

--config FILE reads TOML settings, kebab-case: top-level dir / store-name /
  sync, [listen] graphql/replication, and the file-only tuning
  sections [graphql] [replication] [journal] [engine] [log] [edge] —
  [engine] covers every fluent31::Options tunable, [journal] dir attaches
  the opt-in mutation journal (rebuild: fluent-cli journal-rebuild), [log]
  sets the stats heartbeat period, [edge] selects and tunes the edge
  role. Explicit flags override the file.
  Annotated example: crates/fluent-server/src/config.rs
--journal DIR attaches the journal at DIR; its tuning stays in [journal].
--print-schema prints the base GraphQL SDL (built-ins only) and exits.
--print-edge-schema prints the edge surface's SDL and exits.

logs go to stderr; RUST_LOG sets the level (default info).";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

/// Diagnostics go to stderr as structured lines; `RUST_LOG` overrides the
/// default level (`info`), per crate if wanted: `RUST_LOG=fluent31=debug`.
fn init_logging() {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
}

/// The `[listen]` slots the address flags write into.
fn listen(cli: &mut FileConfig) -> &mut ListenSection {
    cli.listen.get_or_insert_with(ListenSection::default)
}

/// The `[edge]` slots the edge flags write into.
fn edge(cli: &mut FileConfig) -> &mut EdgeSection {
    cli.edge.get_or_insert_with(EdgeSection::default)
}

fn main() -> ExitCode {
    init_logging();
    let mut cli = FileConfig::default();
    let mut config_path: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => match args.next() {
                Some(v) => config_path = Some(v),
                None => return usage(),
            },
            "--graphql" => match args.next() {
                Some(v) => listen(&mut cli).graphql = Some(v),
                None => return usage(),
            },
            "--replication" => match args.next() {
                Some(v) => listen(&mut cli).replication = Some(v),
                None => return usage(),
            },
            "--store-name" => match args.next() {
                Some(v) => cli.store_name = Some(v),
                None => return usage(),
            },
            "--sync" => match args.next() {
                Some(v) if parse_sync(&v).is_some() => cli.sync = Some(v),
                _ => return usage(),
            },
            "--max-body-bytes" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => {
                    cli.graphql
                        .get_or_insert_with(GraphqlSection::default)
                        .max_body_bytes = Some(v)
                }
                None => return usage(),
            },
            "--journal" => match args.next() {
                Some(v) => cli.journal.get_or_insert_with(JournalSection::default).dir = Some(v),
                None => return usage(),
            },
            "--edge-master" => match args.next() {
                Some(v) => edge(&mut cli).master_addr = Some(v),
                None => return usage(),
            },
            "--edge-scope-lo" => match args.next() {
                Some(v) => edge(&mut cli).scope_lo = Some(BytesSpec::Text { text: v }),
                None => return usage(),
            },
            "--edge-scope-hi" => match args.next() {
                Some(v) => edge(&mut cli).scope_hi = Some(BytesSpec::Text { text: v }),
                None => return usage(),
            },
            "--print-schema" => {
                print!("{}", fluent_graphql::base_sdl());
                return ExitCode::SUCCESS;
            }
            "--print-edge-schema" => {
                print!("{}", fluent_graphql::edge_sdl());
                return ExitCode::SUCCESS;
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ if cli.dir.is_none() && !a.starts_with('-') => cli.dir = Some(a),
            _ => return usage(),
        }
    }

    // both sources validate sync at intake, so provenance stays in the
    // error message; after overlay the value is known-good
    let file = match &config_path {
        Some(path) => match FileConfig::load(std::path::Path::new(path)) {
            Ok(f) => f,
            Err(e) => {
                error!(config = path, error = %e, "cannot load config");
                return ExitCode::FAILURE;
            }
        },
        None => FileConfig::default(),
    };
    if let Some(s) = &file.sync {
        if parse_sync(s).is_none() {
            let path = config_path.as_deref().unwrap_or_default();
            error!(
                config = path,
                sync = s,
                "invalid sync mode (always | never | periodic:<ms>)"
            );
            return ExitCode::FAILURE;
        }
    }
    let eff = cli.overlay(file);

    // [edge] present = the process is an edge server, not a store server
    if eff.edge.is_some() {
        return edge_main(eff);
    }

    let Some(dir) = eff.dir.clone() else {
        eprintln!("fluent-server: missing <db-dir> (positional argument, or `dir` in the --config file)\n");
        return usage();
    };
    let sync = eff
        .sync
        .as_deref()
        .map(|s| parse_sync(s).expect("sync validated at intake"))
        .unwrap_or(SyncMode::Always);
    let cfg = eff.server_config();
    let opts = eff.engine_options(sync);
    // [journal] is opt-in; once present it must name a destination — a
    // section that journals nowhere would be a silent no-op
    let journal = match &eff.journal {
        Some(j) => match &j.dir {
            Some(d) => Some((d.clone(), j.config())),
            None => {
                error!("[journal] section needs dir");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    let db = match Db::open(&dir, opts.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            error!(dir, error = %e, "cannot open store");
            return ExitCode::FAILURE;
        }
    };
    // Attached before serving, so the base snapshot precedes every streamed
    // request. Held to the end of main — its Drop (drainer join + final
    // flush) runs after serve returns, before the last Db handle goes down.
    let _journal = match journal {
        Some((jdir, jcfg)) => match Journal::attach_with_config(db.clone(), &jdir, jcfg) {
            Ok(j) => Some(j),
            Err(e) => {
                error!(dir = jdir, error = %e, "cannot attach journal");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    serve(db, dir, opts, cfg)
}

/// The edge role: attach a replica to the configured master, then serve
/// the read-only edge GraphQL plane over it.
fn edge_main(eff: FileConfig) -> ExitCode {
    let conflicts = eff.edge_conflicts();
    if !conflicts.is_empty() {
        error!(
            settings = conflicts.join(", "),
            "edge mode: these settings configure a store of record, which an edge server has none of"
        );
        return ExitCode::FAILURE;
    }
    let Some(dir) = eff.dir.clone() else {
        eprintln!("fluent-server: missing <cache-dir> (positional argument, or `dir` in the --config file)\n");
        return usage();
    };
    let section = eff
        .edge
        .as_ref()
        .expect("edge_main called with [edge] present");
    let rcfg = match section.replica_config(&dir) {
        Ok(c) => c,
        Err(e) => {
            error!("{e}");
            return ExitCode::FAILURE;
        }
    };
    // blocking: connects, wipes the cache dir, completes the initial
    // gap-free sync — the plane binds only once the edge can answer
    let replica = match EdgeReplica::start(rcfg) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            error!(dir, error = %e, "cannot attach edge replica");
            return ExitCode::FAILURE;
        }
    };
    serve_edge(replica, eff.edge_server_config())
}

#[tokio::main]
async fn serve_edge(replica: Arc<EdgeReplica>, cfg: EdgeServerConfig) -> ExitCode {
    let server = match EdgeServer::start(replica, cfg).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "cannot start");
            return ExitCode::FAILURE;
        }
    };
    let master = server.replica().master();
    info!(
        listen = %server.graphql_addr,
        master_store = %master.name,
        master_instance = %master.instance_hex(),
        "serving edge graphql: read-only get/scan at /graphql (GraphiQL at /)"
    );

    // an edge is a cache — a severed connection or dropped overlay costs
    // nothing; the next attach starts clean
    shutdown_on_signals(server.shutdown()).await;
    ExitCode::SUCCESS
}

/// Resolves on SIGINT (ctrl-C) or, on unix, SIGTERM.
async fn any_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

/// First signal: stop accepting and run `drain` (in-flight requests
/// finish). Second signal: exit immediately.
async fn shutdown_on_signals(drain: impl std::future::Future<Output = ()>) {
    any_signal().await;
    info!("shutting down: draining in-flight requests (signal again to exit immediately)");
    tokio::spawn(async {
        any_signal().await;
        warn!("forced exit");
        std::process::exit(130);
    });
    drain.await;
}

#[tokio::main]
async fn serve(db: Arc<Db>, dir: String, opts: Options, cfg: ServerConfig) -> ExitCode {
    let server = match Server::start(db.clone(), &dir, opts, cfg).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "cannot start");
            return ExitCode::FAILURE;
        }
    };
    info!(
        listen = %server.graphql_addr,
        "serving graphql: /graphql (GraphiQL at /, forks at /graphql/<instanceId>)"
    );
    match (server.replication_addr, db.identity()) {
        (Some(addr), Some(id)) => info!(
            listen = %addr,
            store = %id.name,
            instance = %id.instance_hex(),
            "serving replication: replicas and edge caches join here (REPLICATION.md)"
        ),
        _ => info!("replication off: unnamed store; pass --store-name NAME to open the join point"),
    }

    // in-flight replication connections are severed at exit; the WAL
    // keeps the store consistent
    shutdown_on_signals(server.shutdown()).await;
    ExitCode::SUCCESS
}
