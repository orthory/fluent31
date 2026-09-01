//! The edge read plane: the primary plane's `get`/`scan` grammar served
//! over an [`EdgeStore`] — one schema, query root only.
//!
//! An edge replica is a scoped, read-only, ephemeral cache of one master
//! instance; served over the network it speaks the SAME protocol as every
//! other plane (the shared `Bytes`/`BytesInput`/`Pair`/`ScanPage` types
//! and the shared scan grammar — see [`crate::schema::ScanArgs`]), so a
//! client moving between a primary and an edge changes an address, never
//! a dialect. What the edge does NOT serve is equally load-bearing: no
//! mutation root, no admin fields, no WASM, no subscriptions, no fork
//! instances.
//!
//! Consistency: an edge has no MVCC snapshots (it is a cache, not a store
//! of record) — every field reads the edge's current replication frontier
//! at its own execution, unlike the primary plane's per-operation pinned
//! snapshot. Scope: reads clamp exactly as the embedded surface does — an
//! out-of-scope `get` refuses loudly, an out-of-scope `scan` range yields
//! an empty page.

use std::sync::Arc;

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, Schema, TypeRef};
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use fluent31::edge::EdgeStore;
use tokio::sync::Semaphore;
use tower_http::limit::RequestBodyLimitLayer;

use crate::builtins::arg_bytes;
use crate::schema::{
    bytes_input, bytes_object, gated_blocking, pair_object, scan_page_object, BytesVal, ScanArgs,
    ScanPageVal, LIMIT_COMPLEXITY, LIMIT_DEPTH,
};

/// Hands resolvers the replica's CURRENT store. A re-sync (after a lag
/// cutoff or a master swap) replaces the [`EdgeStore`] under a live
/// server, so the store is looked up per call — never captured at schema
/// build.
pub trait EdgeStoreProvider: Send + Sync + 'static {
    fn store(&self) -> Arc<EdgeStore>;
}

/// Schema-data context for the edge resolvers: the store lookup plus the
/// read gate (edge reads may reach back to the master for cold values,
/// so they hold blocking-pool threads like any engine call).
struct EdgeCtx {
    provider: Arc<dyn EdgeStoreProvider>,
    read_gate: Arc<Semaphore>,
}

/// Build the edge schema over `provider`. Static for the server's life:
/// the edge has no module layer, so there is nothing to hot-swap.
pub fn edge_schema(provider: Arc<dyn EdgeStoreProvider>) -> Schema {
    let query = Object::new("Query")
        .field(
            Field::new("get", TypeRef::named("Bytes"), |ctx| {
                FieldFuture::new(async move {
                    let key = arg_bytes(&ctx, "key")?;
                    let edge = ctx.data::<EdgeCtx>()?;
                    let store = edge.provider.store();
                    Ok(gated_blocking(&edge.read_gate, move || store.get(&key))
                        .await?
                        .map(|v| FieldValue::owned_any(BytesVal(v))))
                })
            })
            .argument(InputValue::new("key", TypeRef::named_nn("BytesInput")))
            .description(
                "Point lookup at the edge's current replication frontier. Null when \
                 the key is absent; refused when the key is outside the edge's scope.",
            ),
        )
        .field(
            // argument list declared per plane; the SDL-equality test below
            // pins it byte-for-byte to the primary plane's declaration
            Field::new("scan", TypeRef::named("ScanPage"), |ctx| {
                FieldFuture::new(async move {
                    let args = ScanArgs::parse(&ctx)?;
                    let edge = ctx.data::<EdgeCtx>()?;
                    let store = edge.provider.store();
                    let (pairs, has_more) = gated_blocking(&edge.read_gate, move || {
                        store.scan(
                            args.lo.as_deref(),
                            args.hi.as_deref(),
                            args.reverse,
                            args.limit,
                        )
                    })
                    .await?;
                    Ok(Some(FieldValue::owned_any(ScanPageVal::new(
                        pairs, has_more,
                    ))))
                })
            })
            .argument(InputValue::new("lo", TypeRef::named("BytesInput")))
            .argument(InputValue::new("hi", TypeRef::named("BytesInput")))
            .argument(InputValue::new("prefix", TypeRef::named("BytesInput")))
            .argument(InputValue::new("after", TypeRef::named("BytesInput")))
            .argument(InputValue::new("reverse", TypeRef::named(TypeRef::BOOLEAN)))
            .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
            .description(
                "Range scan over [lo, hi) — or over a key prefix — clamped into the \
                 edge's scope (an out-of-scope range yields an empty page), at the \
                 edge's current replication frontier, forward or reverse (default \
                 forward), paginated with limit (default 100, max 10000) plus the \
                 returned nextAfter cursor.",
            ),
        );

    Schema::build("Query", None, None)
        .register(bytes_object())
        .register(bytes_input())
        .register(pair_object())
        .register(scan_page_object())
        .register(query)
        .limit_depth(LIMIT_DEPTH)
        .limit_complexity(LIMIT_COMPLEXITY)
        .data(EdgeCtx {
            provider,
            read_gate: Arc::new(Semaphore::new(crate::READ_PERMITS)),
        })
        .finish()
        .expect("edge schema build: internal invariant")
}

/// HTTP wiring for an edge server: GraphiQL on GET at `/` and `/graphql`,
/// execution on POST at `/graphql` — the same paths as the primary plane,
/// minus fork instances (an edge serves exactly one attachment) and minus
/// the WebSocket transport (the edge schema has no subscriptions).
pub fn edge_router(provider: Arc<dyn EdgeStoreProvider>, max_body: usize) -> Router {
    let schema = edge_schema(provider);
    Router::new()
        .route("/", get(graphiql_edge))
        .route("/graphql", get(graphiql_edge).post(edge_handler))
        // the async-graphql extractor bypasses axum's DefaultBodyLimit, so
        // cap the body itself
        .layer(RequestBodyLimitLayer::new(max_body))
        .with_state(schema)
}

async fn edge_handler(State(schema): State<Schema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphiql_edge() -> Response {
    Html(GraphiQLSource::build().endpoint("/graphql").finish()).into_response()
}

/// Schema SDL for the edge surface (no store attached).
pub fn edge_sdl() -> String {
    /// SDL rendering never resolves a field, so the provider is never called.
    struct NoStore;
    impl EdgeStoreProvider for NoStore {
        fn store(&self) -> Arc<EdgeStore> {
            unreachable!("SDL build never resolves fields")
        }
    }
    edge_schema(Arc::new(NoStore)).sdl()
}

#[cfg(test)]
mod tests {
    /// The field's one-line SDL signature, docs stripped.
    fn field_line<'a>(sdl: &'a str, field: &str) -> &'a str {
        let stripped: Vec<&str> = sdl.split("\"\"\"").step_by(2).collect();
        for part in stripped {
            for line in part.lines() {
                if line.trim_start().starts_with(field) {
                    return line.trim();
                }
            }
        }
        panic!("no field {field:?} in SDL");
    }

    /// The edge serves exactly the primary plane's read grammar — same
    /// `get`/`scan` signatures, byte for byte — and nothing else: no
    /// mutation root, no subscription root, no admin or WASM fields.
    #[test]
    fn edge_sdl_is_read_only_and_matches_the_primary_grammar() {
        let edge = super::edge_sdl();
        let primary = crate::base_sdl();
        assert!(!edge.contains("type Mutation"));
        assert!(!edge.contains("type Subscription"));
        assert!(!edge.contains("wasm"));
        assert!(!edge.contains("modules"));
        assert!(!edge.contains("fork"));
        assert_eq!(field_line(&edge, "get("), field_line(&primary, "get("));
        assert_eq!(field_line(&edge, "scan("), field_line(&primary, "scan("));
    }
}
