//! fluent-replication binary: the master-side replication server — the
//! join point full replicas and key-range edge caches attach to. Edge
//! replicas are library components (`fluent_replication::EdgeReplica`):
//! the process that needs the scoped reads embeds one and reads through
//! `EdgeReplica::store()`.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options};
use fluent_replication::{ReplServer, ReplServerConfig};

const USAGE: &str = "\
usage: fluent-replication <db-dir> [--store-name NAME] [--listen ADDR:PORT]

serves the replication join point (default 127.0.0.1:8428) for one store.
Replication needs a named store: pass --store-name once, the name persists.";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
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
            eprintln!("fluent-replication: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let srv = match ReplServer::new(db, ReplServerConfig::default()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("fluent-replication: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "fluent-replication: {} instance {} on {listen}",
        srv.identity().name,
        srv.identity().instance_hex()
    );
    serve(srv, listen)
}

#[tokio::main]
async fn serve(srv: Arc<ReplServer>, listen: String) -> ExitCode {
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fluent-replication: cannot listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    tokio::select! {
        r = srv.serve(listener) => match r {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("fluent-replication: {e}");
                ExitCode::FAILURE
            }
        },
        _ = tokio::signal::ctrl_c() => {
            eprintln!("fluent-replication: shutting down");
            ExitCode::SUCCESS
        }
    }
}
