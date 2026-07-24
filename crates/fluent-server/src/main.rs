//! fluent-server binary: formal server mode — one process, one store,
//! all three network planes.

use std::process::ExitCode;
use std::sync::Arc;

use fluent31::{Db, Journal, Options, SyncMode};
use fluent_server::{parse_sync, FileConfig, GraphqlSection, ListenSection, Server, ServerConfig};

const USAGE: &str = "\
usage: fluent-server <db-dir> [--config FILE] [--store-name NAME]
                     [--graphql ADDR:PORT] [--wire ADDR:PORT] [--replication ADDR:PORT]
                     [--sync always|never|periodic:<ms>] [--max-body-bytes N]

serves every plane of one store in one process:
  graphql      HTTP, default 127.0.0.1:8317 — typed/admin plane, GraphiQL at /
  wire         TCP,  default 127.0.0.1:8427 — binary data-plane pipe (WIRE.md)
  replication  TCP,  default 127.0.0.1:8428 — join point for replicas and
               key-range edge caches (REPLICATION.md); needs a named store:
               pass --store-name once, the name persists

--config FILE reads TOML settings, kebab-case: top-level dir / store-name /
  sync, [listen] graphql/wire/replication, and the file-only tuning
  sections [graphql] [wire] [replication] [journal] [engine] — [engine]
  covers every fluent31::Options tunable, [journal] dir attaches the
  opt-in mutation journal (rebuild: fluent-cli journal-rebuild). Explicit
  flags override the file. Annotated example:
  crates/fluent-server/src/config.rs";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

/// The `[listen]` slots the address flags write into.
fn listen(cli: &mut FileConfig) -> &mut ListenSection {
    cli.listen.get_or_insert_with(ListenSection::default)
}

fn main() -> ExitCode {
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
            "--wire" => match args.next() {
                Some(v) => listen(&mut cli).wire = Some(v),
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
                eprintln!("fluent-server: --config {path}: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => FileConfig::default(),
    };
    if let Some(s) = &file.sync {
        if parse_sync(s).is_none() {
            let path = config_path.as_deref().unwrap_or_default();
            eprintln!("fluent-server: --config {path}: invalid sync {s:?} (always | never | periodic:<ms>)");
            return ExitCode::FAILURE;
        }
    }
    let eff = cli.overlay(file);

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
                eprintln!("fluent-server: [journal] section needs dir");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    let db = match Db::open(&dir, opts.clone()) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("fluent-server: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Attached before serving, so the base snapshot precedes every streamed
    // request. Held to the end of main — its Drop (drainer join + final
    // flush) runs after serve returns, before the last Db handle goes down.
    let _journal = match journal {
        Some((jdir, jcfg)) => match Journal::attach_with_config(db.clone(), &jdir, jcfg) {
            Ok(j) => {
                println!("fluent-server: journal      {jdir} (mutation journal — rebuild: fluent-cli journal-rebuild)");
                Some(j)
            }
            Err(e) => {
                eprintln!("fluent-server: cannot attach journal at {jdir}: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
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
