//! fluent-server binary: formal server mode — one process, one store,
//! all three network planes.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options, SyncMode};
use fluent_server::{Server, ServerConfig};

const USAGE: &str = "\
usage: fluent-server <db-dir> [--store-name NAME]
                     [--graphql ADDR:PORT] [--wire ADDR:PORT] [--replication ADDR:PORT]
                     [--sync always|never|periodic:<ms>] [--max-body-bytes N]

serves every plane of one store in one process:
  graphql      HTTP, default 127.0.0.1:8317 — typed/admin plane, GraphiQL at /
  wire         TCP,  default 127.0.0.1:8427 — binary data-plane pipe (WIRE.md)
  replication  TCP,  default 127.0.0.1:8428 — join point for replicas and
               key-range edge caches (REPLICATION.md); needs a named store:
               pass --store-name once, the name persists";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let mut dir: Option<String> = None;
    let mut store_name: Option<String> = None;
    let mut sync = SyncMode::Always;
    let mut cfg = ServerConfig::default();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--graphql" => match args.next() {
                Some(v) => cfg.graphql = v,
                None => return usage(),
            },
            "--wire" => match args.next() {
                Some(v) => cfg.wire = v,
                None => return usage(),
            },
            "--replication" => match args.next() {
                Some(v) => cfg.replication = v,
                None => return usage(),
            },
            "--store-name" => match args.next() {
                Some(v) => store_name = Some(v),
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
                Some(v) => cfg.max_body_bytes = v,
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

    let opts = Options {
        sync,
        store_name,
        ..Options::default()
    };
    let db = match Db::open(&dir, opts.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("fluent-server: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    serve(db, dir, opts, cfg)
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

#[tokio::main]
async fn serve(db: Arc<Db>, dir: String, opts: Options, cfg: ServerConfig) -> ExitCode {
    let server = match Server::start(db.clone(), &dir, opts, cfg).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("fluent-server: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "fluent-server: graphql      http://{}/graphql (GraphiQL at /, forks at /graphql/<instanceId>)",
        server.graphql_addr
    );
    println!(
        "fluent-server: wire         {} (protocol v1, WIRE.md)",
        server.wire_addr
    );
    match (server.replication_addr, db.identity()) {
        (Some(addr), Some(id)) => println!(
            "fluent-server: replication  {addr} — store {:?} instance {} (replicas and edge caches join here, REPLICATION.md)",
            id.name,
            id.instance_hex()
        ),
        _ => println!(
            "fluent-server: replication  off — unnamed store; pass --store-name NAME to open the join point"
        ),
    }

    // First signal: stop accepting and drain in-flight GraphQL requests
    // (in-flight wire connections are severed at exit; the WAL keeps the
    // store consistent). Second signal: exit immediately.
    any_signal().await;
    eprintln!("fluent-server: shutting down — draining in-flight requests (signal again to exit immediately)");
    tokio::spawn(async {
        any_signal().await;
        eprintln!("fluent-server: forced exit");
        std::process::exit(130);
    });
    server.shutdown().await;
    ExitCode::SUCCESS
}
