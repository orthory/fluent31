//! HTTP wiring: the primary instance at `/graphql`, every fork at
//! `/graphql/<instanceId>` (GraphiQL on GET at each endpoint). The path
//! is the routing seam — each instance carries its own hot-swapped
//! schema, so the request must pick its instance before GraphQL
//! execution starts. Physical topology (ports, NAT, discovery) stays out
//! of scope by construction: one listener serves the whole instance tree.

use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use crate::{InstanceRegistry, ResolveError};

pub fn router(registry: Arc<InstanceRegistry>, max_body: usize) -> Router {
    Router::new()
        .route("/", get(graphiql_primary))
        .route("/graphql", get(graphiql_primary).post(primary_handler))
        .route(
            "/graphql/{instance}",
            get(graphiql_instance).post(instance_handler),
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
        Err(ResolveError::UnknownInstance) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!("unknown instance {instance:?}")
            })),
        )
            .into_response(),
        Err(ResolveError::Engine(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("cannot open instance {instance:?}: {e}")
            })),
        )
            .into_response(),
    }
}

async fn graphiql_primary() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn graphiql_instance(Path(instance): Path<String>) -> Html<String> {
    Html(
        GraphiQLSource::build()
            .endpoint(&format!("/graphql/{instance}"))
            .finish(),
    )
}
