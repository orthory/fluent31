//! fluent-graphql server binary: the primary database at POST /graphql,
//! forks at POST /graphql/<instanceId>, GraphiQL IDE on GET at each
//! endpoint.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Journal, JournalConfig, Options, SyncMode};
use fluent_graphql::{InstanceRegistry, RegistryConfig, SchemaManager};
use tracing::{error, info, warn};

const USAGE: &str = "usage: fluent-graphql <db-dir> [--listen ADDR:PORT] [--sync always|never|periodic:<ms>] [--max-body-bytes N]\n                      [--journal DIR] [--journal-rotate-bytes N] [--journal-compact-when-deltas-exceed R|off] [--journal-compact-min-bytes N]\n                      [--stats-every-secs N]\n       fluent-graphql --print-schema\n\nlogs go to stderr; RUST_LOG sets the level (default info). --stats-every-secs\nlogs an engine stats line on that period (default 60, 0 = off).";
const DEFAULT_MAX_BODY: usize = 32 << 20;
const DEFAULT_STATS_EVERY_SECS: u64 = 60;

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

fn main() -> ExitCode {
    init_logging();
    let mut dir: Option<String> = None;
    let mut listen = "127.0.0.1:8317".to_string();
    let mut sync = SyncMode::Always;
    let mut max_body = DEFAULT_MAX_BODY;
    let mut stats_every_secs = DEFAULT_STATS_EVERY_SECS;
    let mut journal: Option<String> = None;
    let mut journal_rotate_bytes: Option<u64> = None;
    // Some(None) is `off`: auto-compaction disabled (lag healing still compacts)
    let mut journal_compact_ratio: Option<Option<f64>> = None;
    let mut journal_compact_min_bytes: Option<u64> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--listen" => match args.next() {
                Some(v) => listen = v,
                None => return usage(),
            },
            "--sync" => match args.next().as_deref() {
                Some("always") => sync = SyncMode::Always,
                Some("never") => sync = SyncMode::Never,
                Some(v) if v.starts_with("periodic:") => {
                    let Some(ms) = v["periodic:".len()..].parse::<u64>().ok().filter(|ms| *ms > 0)
                    else {
                        return usage();
                    };
                    sync = SyncMode::Periodic {
                        every: std::time::Duration::from_millis(ms),
                    };
                }
                _ => return usage(),
            },
            "--max-body-bytes" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => max_body = v,
                None => return usage(),
            },
            "--journal" => match args.next() {
                Some(v) => journal = Some(v),
                None => return usage(),
            },
            "--journal-rotate-bytes" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => journal_rotate_bytes = Some(v),
                None => return usage(),
            },
            "--journal-compact-when-deltas-exceed" => match args.next().as_deref() {
                Some("off") => journal_compact_ratio = Some(None),
                Some(v) => match v.parse::<f64>() {
                    Ok(r) => journal_compact_ratio = Some(Some(r)),
                    Err(_) => return usage(),
                },
                None => return usage(),
            },
            "--journal-compact-min-bytes" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => journal_compact_min_bytes = Some(v),
                None => return usage(),
            },
            "--stats-every-secs" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => stats_every_secs = v,
                None => return usage(),
            },
            "--print-schema" => {
                print!("{}", fluent_graphql::base_sdl());
                return ExitCode::SUCCESS;
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ if dir.is_none() && !a.starts_with('-') => dir = Some(a),
            _ => return usage(),
        }
    }
    let Some(dir) = dir else { return usage() };

    // tuning without a journal would be a silent no-op; refuse it, the way
    // fluent-server refuses a [journal] section that names no dir
    let journal_tuning_given = journal_rotate_bytes.is_some()
        || journal_compact_ratio.is_some()
        || journal_compact_min_bytes.is_some();
    if journal_tuning_given && journal.is_none() {
        error!("--journal-* tuning flags need --journal DIR");
        return ExitCode::FAILURE;
    }

    let opts = Options {
        sync,
        ..Options::default()
    };
    let db = match Db::open(&dir, opts.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            error!(dir, error = %e, "cannot open store");
            return ExitCode::FAILURE;
        }
    };
    // Opt-in mutation journal (fluent31::journal): a base snapshot at attach,
    // then streamed deltas on a background thread for the life of the
    // process. Held to the end of main — its Drop (drainer join + final
    // flush) runs after serve returns, before the last Db handle goes down.
    // Flag overrides apply over JournalConfig::default, mirroring
    // fluent-server's [journal] section; value validation (rotate > 0,
    // ratio finite and > 0) lives in attach_with_config.
    let mut journal_cfg = JournalConfig::default();
    if let Some(v) = journal_rotate_bytes {
        journal_cfg.rotate_bytes = v;
    }
    if let Some(v) = journal_compact_ratio {
        journal_cfg.compact_when_deltas_exceed = v;
    }
    if let Some(v) = journal_compact_min_bytes {
        journal_cfg.compact_min_bytes = v;
    }
    let _journal = match &journal {
        Some(jdir) => match Journal::attach_with_config(db.clone(), jdir, journal_cfg) {
            Ok(j) => Some(j),
            Err(e) => {
                error!(dir = jdir, error = %e, "cannot attach journal");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    // runs every installed module's `describe` and builds the schema
    let mgr = match SchemaManager::new(db) {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "schema init failed");
            return ExitCode::FAILURE;
        }
    };
    let registry = InstanceRegistry::new(mgr, &dir, opts, RegistryConfig::default());
    serve(registry, listen, max_body, std::time::Duration::from_secs(stats_every_secs))
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

/// First signal: stop accepting and drain in-flight requests. Second
/// signal: exit immediately — in-flight requests are severed, but the WAL
/// keeps the store consistent on reopen.
async fn shutdown_signal() {
    any_signal().await;
    info!("shutting down: draining in-flight requests (signal again to exit immediately)");
    tokio::spawn(async {
        any_signal().await;
        warn!("forced exit");
        std::process::exit(130);
    });
}

#[tokio::main]
async fn serve(
    registry: Arc<InstanceRegistry>,
    listen: String,
    max_body: usize,
    stats_every: std::time::Duration,
) -> ExitCode {
    let app = fluent_graphql::router(registry.clone(), max_body);
    // close fork instances nobody has touched in a while
    tokio::spawn({
        let registry = registry.clone();
        async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                registry.evict_idle();
            }
        }
    });
    if !stats_every.is_zero() {
        tokio::spawn(fluent_graphql::stats_heartbeat(registry.clone(), stats_every));
    }
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            error!(listen, error = %e, "cannot listen");
            return ExitCode::FAILURE;
        }
    };
    info!(listen, "serving graphql: /graphql (GraphiQL at /, forks at /graphql/<instanceId>)");
    match axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "graphql plane failed");
            ExitCode::FAILURE
        }
    }
}
