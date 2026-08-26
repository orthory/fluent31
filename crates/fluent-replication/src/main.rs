//! fluent-replication binary: the master-side replication server — the
//! join point full replicas and key-range edge caches attach to. Edge
//! replicas are library components (`fluent_replication::EdgeReplica`):
//! the process that needs the scoped reads embeds one and reads through
//! `EdgeReplica::store()`.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options};
use fluent_replication::{ReplServer, ReplServerConfig};
use tracing::{error, info};

const USAGE: &str = "\
usage: fluent-replication <db-dir> [--store-name NAME] [--listen ADDR:PORT]

serves the replication join point (default 127.0.0.1:8428) for one store.
Replication needs a named store: pass --store-name once, the name persists.";

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
    let mut listen = "127.0.0.1:8428".to_string();
    let mut store_name: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--listen" => match args.next() {
                Some(v) => listen = v,
                None => return usage(),
            },
            "--store-name" => match args.next() {
                Some(v) => store_name = Some(v),
                None => return usage(),
            },
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ if dir.is_none() && !a.starts_with('-') => dir = Some(a),
            _ => return usage(),
        }
    }
    let Some(dir) = dir else { return usage() };
    let db = match Db::open(
        &dir,
        Options {
            store_name,
            ..Options::default()
        },
    ) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            error!(dir, error = %e, "cannot open store");
            return ExitCode::FAILURE;
        }
    };
    let srv = match ReplServer::new(db, ReplServerConfig::default()) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "cannot serve replication");
            return ExitCode::FAILURE;
        }
    };
    serve(srv, listen)
}

#[tokio::main]
async fn serve(srv: Arc<ReplServer>, listen: String) -> ExitCode {
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            error!(listen, error = %e, "cannot listen");
            return ExitCode::FAILURE;
        }
    };
    info!(
        listen,
        store = %srv.identity().name,
        instance = %srv.identity().instance_hex(),
        "serving replication"
    );
    tokio::select! {
        r = srv.serve(listener) => match r {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!(error = %e, "replication plane failed");
                ExitCode::FAILURE
            }
        },
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down");
            ExitCode::SUCCESS
        }
    }
}
