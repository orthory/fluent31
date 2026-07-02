//! fluent-graphql server binary: GraphQL at POST /graphql, GraphiQL IDE at
//! GET / and GET /graphql.

use std::process::ExitCode;
use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use fluent31::{Db, Options, SyncMode};
use fluent_graphql::{build_schema, prepare, FluentSchema};
use tower_http::limit::RequestBodyLimitLayer;

const USAGE: &str = "usage: fluent-graphql <db-dir> [--listen ADDR:PORT] [--sync always|never] [--max-body-bytes N]\n       fluent-graphql --print-schema";
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
                _ => return usage(),
            },
            "--max-body-bytes" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => max_body = v,
                None => return usage(),
            },
            "--print-schema" => {
                print!("{}", fluent_graphql::sdl());
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
    let db = match Db::open(&dir, opts) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("fluent-graphql: cannot open {dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    serve(build_schema(db), listen, max_body)
}

async fn graphql_handler(
    State(schema): State<FluentSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(prepare(req.into_inner())).await.into()
}

async fn graphiql() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
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
async fn serve(schema: FluentSchema, listen: String, max_body: usize) -> ExitCode {
    let app = Router::new()
        .route("/", get(graphiql))
        .route("/graphql", get(graphiql).post(graphql_handler))
        // the async-graphql extractor bypasses axum's DefaultBodyLimit, so
        // cap the body itself
        .layer(RequestBodyLimitLayer::new(max_body))
        .with_state(schema);
    let listener = match tokio::net::TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fluent-graphql: cannot listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("fluent-graphql: http://{listen}/graphql (GraphiQL at /)");
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
