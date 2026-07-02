//! fluent-wire server binary.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options, SyncMode};
use fluent_wire::{ServerConfig, WireServer};

const USAGE: &str =
    "usage: fluent-wire <db-dir> [--listen ADDR:PORT] [--sync always|never|periodic:<ms>]";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let mut dir: Option<String> = None;
    let mut listen = "127.0.0.1:8427".to_string();
    let mut sync = SyncMode::Always;
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
                    let Some(ms) = v["periodic:".len()..].parse::<u64>().ok().filter(|m| *m > 0)
                    else {
                        return usage();
                    };
                    sync = SyncMode::Periodic {
                        every: std::time::Duration::from_millis(ms),
                    };
                }
                _ => return usage(),
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
    let db = match Db::open(&dir, Options { sync, ..Options::default() }) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("fluent-wire: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    serve(db, listen)
}

#[tokio::main]
async fn serve(db: Arc<Db>, listen: String) -> ExitCode {
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fluent-wire: cannot listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("fluent-wire: {listen} (protocol v1, see WIRE.md)");
    let srv = WireServer::new(db, ServerConfig::default());
    tokio::select! {
        r = srv.serve(listener) => match r {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("fluent-wire: {e}");
                ExitCode::FAILURE
            }
        },
        _ = tokio::signal::ctrl_c() => {
            eprintln!("fluent-wire: shutting down");
            ExitCode::SUCCESS
        }
    }
}
