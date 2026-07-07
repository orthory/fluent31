//! fluent-replication binary: master-side replication server, or an edge
//! replica that serves wire v1 reads over its scoped slice.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Options};
use fluent_replication::{EdgeReplica, EdgeReplicaConfig, ReplServer, ReplServerConfig};
use fluent_wire::{ServerConfig, WireServer};

const USAGE: &str = "\
usage:
  fluent-replication master <db-dir> --store-name NAME [--listen ADDR:PORT]
  fluent-replication edge --master ADDR:PORT --dir DIR [--lo KEY] [--hi KEY]
                          [--serve ADDR:PORT] [--refresh-secs N]

keys accept raw text or hex:<bytes>; scope is [lo, hi), hi omitted = unbounded";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

fn parse_key(s: &str) -> Option<Vec<u8>> {
    match s.strip_prefix("hex:") {
        None => Some(s.as_bytes().to_vec()),
        Some(h) if h.len() % 2 == 0 => h
            .as_bytes()
            .chunks_exact(2)
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
            .collect(),
        Some(_) => None,
    }
}

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--help") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    if args.is_empty() {
        return usage();
    }
    match args.remove(0).as_str() {
        "master" => master_main(args),
        "edge" => edge_main(args),
        _ => usage(),
    }
}

fn master_main(args: Vec<String>) -> ExitCode {
    let mut dir: Option<String> = None;
    let mut listen = "127.0.0.1:8428".to_string();
    let mut store_name: Option<String> = None;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--listen" => match it.next() {
                Some(v) => listen = v,
                None => return usage(),
            },
            "--store-name" => match it.next() {
                Some(v) => store_name = Some(v),
                None => return usage(),
            },
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
        "fluent-replication master: {} instance {} on {listen}",
        srv.identity().name,
        srv.identity().instance_hex()
    );
    serve_master(srv, listen)
}

#[tokio::main]
async fn serve_master(srv: Arc<ReplServer>, listen: String) -> ExitCode {
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

fn edge_main(args: Vec<String>) -> ExitCode {
    let mut master: Option<String> = None;
    let mut dir: Option<String> = None;
    let mut lo: Vec<u8> = Vec::new();
    let mut hi: Option<Vec<u8>> = None;
    let mut serve: Option<String> = None;
    let mut refresh_secs: u64 = 300;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        let mut val = || it.next().ok_or(());
        let r = match a.as_str() {
            "--master" => val().map(|v| master = Some(v)),
            "--dir" => val().map(|v| dir = Some(v)),
            "--lo" => match val().as_deref().map(parse_key) {
                Ok(Some(k)) => {
                    lo = k;
                    Ok(())
                }
                _ => Err(()),
            },
            "--hi" => match val().as_deref().map(parse_key) {
                Ok(Some(k)) => {
                    hi = Some(k);
                    Ok(())
                }
                _ => Err(()),
            },
            "--serve" => val().map(|v| serve = Some(v)),
            "--refresh-secs" => match val().ok().and_then(|v| v.parse().ok()) {
                Some(n) => {
                    refresh_secs = n;
                    Ok(())
                }
                None => Err(()),
            },
            _ => Err(()),
        };
        if r.is_err() {
            return usage();
        }
    }
    let (Some(master), Some(dir)) = (master, dir) else {
        return usage();
    };
    let serve = serve.unwrap_or_else(|| "127.0.0.1:8427".to_string());
    let mut cfg = EdgeReplicaConfig::new(master, dir, lo, hi);
    cfg.refresh_every = (refresh_secs > 0).then(|| std::time::Duration::from_secs(refresh_secs));
    let replica = match EdgeReplica::start(cfg) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("fluent-replication: edge attach failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let id = replica.master();
    println!(
        "fluent-replication edge: master {} instance {}, serving wire v1 reads on {serve}",
        id.name,
        id.instance_hex()
    );
    serve_edge(replica, serve)
}

#[tokio::main]
async fn serve_edge(replica: Arc<EdgeReplica>, listen: String) -> ExitCode {
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fluent-replication: cannot listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let srv = WireServer::with_backend(replica, ServerConfig::default());
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
