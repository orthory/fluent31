//! fluent-graphql server binary: the primary database at POST /graphql,
//! forks at POST /graphql/<instanceId>, GraphiQL IDE on GET at each
//! endpoint.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options, SyncMode};
use fluent_graphql::{InstanceRegistry, RegistryConfig, SchemaManager};

const USAGE: &str = "usage: fluent-graphql <db-dir> [--listen ADDR:PORT] [--sync always|never|periodic:<ms>] [--max-body-bytes N]\n       fluent-graphql --print-schema";
const DEFAULT_MAX_BODY: usize = 32 << 20;

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let mut dir: Option<String> = None;
    let mut listen = "127.0.0.1:8317".to_string();
    let mut sync = SyncMode::Always;
    let mut max_body = DEFAULT_MAX_BODY;
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

    let opts = Options {
        sync,
        ..Options::default()
    };
    let db = match Db::open(&dir, opts.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("fluent-graphql: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    // runs every installed module's `describe` and builds the schema
    let mgr = match SchemaManager::new(db) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("fluent-graphql: schema init failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let registry = InstanceRegistry::new(mgr, &dir, opts, RegistryConfig::default());
    serve(registry, listen, max_body)
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
    eprintln!("fluent-graphql: shutting down — draining in-flight requests (signal again to exit immediately)");
    tokio::spawn(async {
        any_signal().await;
        eprintln!("fluent-graphql: forced exit");
        std::process::exit(130);
    });
}

#[tokio::main]
async fn serve(registry: Arc<InstanceRegistry>, listen: String, max_body: usize) -> ExitCode {
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
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fluent-graphql: cannot listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("fluent-graphql: http://{listen}/graphql (GraphiQL at /, forks at /graphql/<instanceId>)");
    match axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fluent-graphql: server error: {e}");
            ExitCode::FAILURE
        }
    }
}
