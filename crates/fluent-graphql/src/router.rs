//! HTTP wiring: the primary instance at `/graphql`, every fork at
//! `/graphql/<instanceId>` (GraphiQL on GET at each endpoint). The path
//! is the routing seam — each instance carries its own hot-swapped
//! schema, so the request must pick its instance before GraphQL
//! execution starts. Physical topology (ports, NAT, discovery) stays out
//! of scope by construction: one listener serves the whole instance tree.
//!
//! Subscriptions ride the same endpoints: a GET carrying a WebSocket
//! upgrade with a graphql-ws subprotocol is served as a subscription
//! transport, a plain GET stays GraphiQL. A connection executes against
//! its instance's live [`crate::SchemaManager`] (via
//! [`crate::ManagerExecutor`]), so operations sent later on one connection
//! see hot-swapped schemas; already-running subscription streams keep the
//! schema they started with until re-subscribed.

use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::{InstanceRegistry, ManagerExecutor, ResolveError, SchemaManager};

pub fn router(registry: Arc<InstanceRegistry>, max_body: usize) -> Router {
    Router::new()
        .route("/", get(graphiql_primary))
        .route("/graphql", get(primary_get).post(primary_handler))
        .route(
            "/graphql/{instance}",
            get(instance_get).post(instance_handler),
        )
        // the async-graphql extractor bypasses axum's DefaultBodyLimit, so
        // cap the body itself
        .layer(RequestBodyLimitLayer::new(max_body))
        .with_state(registry)
}

async fn primary_handler(
    State(reg): State<Arc<InstanceRegistry>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    reg.primary().execute(req.into_inner()).await.into()
}

async fn instance_handler(
    State(reg): State<Arc<InstanceRegistry>>,
    Path(instance): Path<String>,
    req: GraphQLRequest,
) -> Response {
    match reg.resolve(&instance).await {
        Ok(mgr) => GraphQLResponse::from(mgr.execute(req.into_inner()).await).into_response(),
        Err(e) => resolve_error(&instance, e),
    }
}

/// GET on a GraphQL endpoint: a WebSocket upgrade with a graphql-ws
/// subprotocol becomes the subscription transport, anything else GraphiQL.
/// (Extracted by hand: axum 0.8's `Option<T>` extractors require
/// `OptionalFromRequestParts`, which these types don't implement.)
async fn ws_intent(req: axum::extract::Request) -> Option<(GraphQLProtocol, WebSocketUpgrade)> {
    let (mut parts, _body) = req.into_parts();
    let protocol = GraphQLProtocol::from_request_parts(&mut parts, &()).await.ok()?;
    let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &()).await.ok()?;
    Some((protocol, upgrade))
}

async fn primary_get(
    State(reg): State<Arc<InstanceRegistry>>,
    req: axum::extract::Request,
) -> Response {
    let Some((protocol, upgrade)) = ws_intent(req).await else {
        return graphiql("/graphql");
    };
    serve_ws(reg.primary(), upgrade, protocol)
}

async fn instance_get(
    State(reg): State<Arc<InstanceRegistry>>,
    Path(instance): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let Some((protocol, upgrade)) = ws_intent(req).await else {
        return graphiql(&format!("/graphql/{instance}"));
    };
    match reg.resolve(&instance).await {
        Ok(mgr) => serve_ws(mgr, upgrade, protocol),
        Err(e) => resolve_error(&instance, e),
    }
}

fn serve_ws(mgr: Arc<SchemaManager>, upgrade: WebSocketUpgrade, protocol: GraphQLProtocol) -> Response {
    upgrade
        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| {
            GraphQLWebSocket::new(socket, ManagerExecutor(mgr), protocol).serve()
        })
        .into_response()
}

fn resolve_error(instance: &str, e: ResolveError) -> Response {
    match e {
        ResolveError::UnknownInstance => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!("unknown instance {instance:?}")
            })),
        )
            .into_response(),
        ResolveError::Engine(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("cannot open instance {instance:?}: {e}")
            })),
        )
            .into_response(),
    }
}

async fn graphiql_primary() -> Response {
    graphiql("/graphql")
}

fn graphiql(endpoint: &str) -> Response {
    Html(
        GraphiQLSource::build()
            .endpoint(endpoint)
            .subscription_endpoint(endpoint)
            .finish(),
    )
    .into_response()
}
