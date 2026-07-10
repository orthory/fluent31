//! Instance routing end-to-end: fork via the primary's GraphQL surface,
//! resolve the returned instanceId through the registry, and operate on
//! the fork as its own database — isolation, fork-of-fork, restart
//! rediscovery, LRU/idle eviction, deleteFork closing served instances,
//! and the HTTP path layer.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Request, Variables};
use fluent31::{Db, Options, SyncMode};
use fluent_graphql::{InstanceRegistry, RegistryConfig, ResolveError, SchemaManager};
use serde_json::{json, Value};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        ..Options::default()
    }
}

fn open_registry_with(
    cfg: RegistryConfig,
) -> (Arc<InstanceRegistry>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    let mgr = SchemaManager::new(Arc::new(db)).unwrap();
    let reg = InstanceRegistry::new(mgr, dir.path(), opts(), cfg);
    (reg, dir)
}

fn open_registry() -> (Arc<InstanceRegistry>, tempfile::TempDir) {
    open_registry_with(RegistryConfig::default())
}

async fn run(mgr: &SchemaManager, query: &str) -> Value {
    let req = Request::new(query).variables(Variables::from_json(json!({})));
    let resp = mgr.execute(req).await;
    assert!(
        resp.errors.is_empty(),
        "unexpected errors for {query}: {:?}",
        resp.errors
    );
    resp.data.into_json().unwrap()
}

async fn get_text(mgr: &SchemaManager, key: &str) -> Option<String> {
    let d = run(mgr, &format!(r#"{{ get(key: {{text: "{key}"}}) {{ text }} }}"#)).await;
    d["get"]["text"].as_str().map(|s| s.to_string())
}

async fn put_text(mgr: &SchemaManager, key: &str, value: &str) {
    run(
        mgr,
        &format!(r#"mutation {{ put(key: {{text: "{key}"}}, value: {{text: "{value}"}}) }}"#),
    )
    .await;
}

/// Cut a fork through the GraphQL surface and return its instanceId.
async fn fork(mgr: &SchemaManager, name: &str) -> String {
    let d = run(
        mgr,
        &format!(r#"mutation {{ fork(name: "{name}") {{ name instanceId }} }}"#),
    )
    .await;
    assert_eq!(d["fork"]["name"], json!(name));
    let id = d["fork"]["instanceId"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 32, "{id}");
    id
}

#[tokio::test]
async fn fork_resolves_and_is_isolated() {
    let (reg, _dir) = open_registry();
    let primary = reg.primary();
    put_text(&primary, "shared", "v1").await;

    let id = fork(&primary, "branch").await;
    let inst = reg.resolve(&id).await.map_err(|_| "resolve").unwrap();

    // the fork sees the cut...
    assert_eq!(get_text(&inst, "shared").await.as_deref(), Some("v1"));
    // ...and diverges in both directions
    put_text(&inst, "fork-only", "f").await;
    put_text(&primary, "shared", "v2").await;
    assert_eq!(get_text(&primary, "fork-only").await, None);
    assert_eq!(get_text(&inst, "shared").await.as_deref(), Some("v1"));
    assert_eq!(get_text(&primary, "shared").await.as_deref(), Some("v2"));

    // repeated resolve returns the same live instance
    let again = reg.resolve(&id).await.map_err(|_| "resolve").unwrap();
    assert!(Arc::ptr_eq(&inst, &again));
    assert_eq!(reg.open_count(), 1);
}

/// The from-a-specific-point flow end to end over GraphQL: pin, keep
/// writing, fork at the pin's seqno (U64 travels as a decimal string),
/// and the resolved instance serves exactly the pinned state.
#[tokio::test]
async fn pin_then_fork_at_serves_the_pinned_state() {
    let (reg, _dir) = open_registry();
    let primary = reg.primary();
    put_text(&primary, "k", "v1").await;

    let d = run(&primary, r#"mutation { pin(name: "p1") { name seqno } }"#).await;
    assert_eq!(d["pin"]["name"], json!("p1"));
    let seqno = d["pin"]["seqno"].as_str().unwrap().to_string();

    put_text(&primary, "k", "v2").await;

    let d = run(&primary, r#"{ pins { name seqno } }"#).await;
    assert_eq!(d["pins"], json!([{"name": "p1", "seqno": seqno}]));

    let d = run(
        &primary,
        &format!(r#"mutation {{ fork(name: "at-p1", at: "{seqno}") {{ instanceId lastSeqno }} }}"#),
    )
    .await;
    assert_eq!(d["fork"]["lastSeqno"].as_str().unwrap(), seqno);
    let id = d["fork"]["instanceId"].as_str().unwrap().to_string();

    let inst = reg.resolve(&id).await.map_err(|_| "resolve").unwrap();
    assert_eq!(get_text(&inst, "k").await.as_deref(), Some("v1"));
    assert_eq!(get_text(&primary, "k").await.as_deref(), Some("v2"));

    let d = run(&primary, r#"mutation { unpin(name: "p1") }"#).await;
    assert_eq!(d["unpin"], json!(true));
    let d = run(&primary, r#"{ pins { name } }"#).await;
    assert_eq!(d["pins"], json!([]));
}

/// `{ seqno }` addresses "now" without a pin: two fork(at:) cuts from one
/// captured seqno are the same version.
#[tokio::test]
async fn seqno_addresses_now_over_graphql() {
    let (reg, _dir) = open_registry();
    let primary = reg.primary();
    put_text(&primary, "k", "v1").await;

    let d = run(&primary, r#"{ seqno }"#).await;
    let s = d["seqno"].as_str().unwrap().to_string();

    for name in ["det-a", "det-b"] {
        let d = run(
            &primary,
            &format!(r#"mutation {{ fork(name: "{name}", at: "{s}") {{ lastSeqno }} }}"#),
        )
        .await;
        assert_eq!(d["fork"]["lastSeqno"].as_str().unwrap(), s);
    }
}

#[tokio::test]
async fn fork_of_fork_resolves_recursively() {
    let (reg, _dir) = open_registry();
    let primary = reg.primary();
    put_text(&primary, "k", "root").await;

    let id1 = fork(&primary, "level1").await;
    let inst1 = reg.resolve(&id1).await.map_err(|_| "resolve").unwrap();
    put_text(&inst1, "k", "level1").await;

    let id2 = fork(&inst1, "level2").await;
    let inst2 = reg.resolve(&id2).await.map_err(|_| "resolve").unwrap();
    assert_eq!(get_text(&inst2, "k").await.as_deref(), Some("level1"));
    put_text(&inst2, "k", "level2").await;
    assert_eq!(get_text(&inst1, "k").await.as_deref(), Some("level1"));
    assert_eq!(get_text(&primary, "k").await.as_deref(), Some("root"));
}

#[tokio::test]
async fn registry_rediscovers_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (id1, id2) = {
        let db = Db::open(dir.path(), opts()).unwrap();
        let mgr = SchemaManager::new(Arc::new(db)).unwrap();
        let reg = InstanceRegistry::new(mgr, dir.path(), opts(), RegistryConfig::default());
        let primary = reg.primary();
        put_text(&primary, "k", "v").await;
        let id1 = fork(&primary, "outer").await;
        let inst1 = reg.resolve(&id1).await.map_err(|_| "resolve").unwrap();
        let id2 = fork(&inst1, "inner").await;
        (id1, id2)
        // registry, managers, and Dbs all drop here — "process exit"
    };

    let db = Db::open(dir.path(), opts()).unwrap();
    let mgr = SchemaManager::new(Arc::new(db)).unwrap();
    let reg = InstanceRegistry::new(mgr, dir.path(), opts(), RegistryConfig::default());
    // both instance ids resolve from fork.meta alone, including the nested one
    let inst1 = reg.resolve(&id1).await.map_err(|_| "resolve").unwrap();
    assert_eq!(get_text(&inst1, "k").await.as_deref(), Some("v"));
    let inst2 = reg.resolve(&id2).await.map_err(|_| "resolve").unwrap();
    assert_eq!(get_text(&inst2, "k").await.as_deref(), Some("v"));
}

#[tokio::test]
async fn unknown_and_malformed_ids_do_not_resolve() {
    let (reg, _dir) = open_registry();
    for id in [
        "00000000000000000000000000000000",
        "../evil",
        "",
        "no/slashes",
    ] {
        assert!(
            matches!(reg.resolve(id).await, Err(ResolveError::UnknownInstance)),
            "{id:?} should not resolve"
        );
    }
    assert_eq!(reg.open_count(), 0);
}

#[tokio::test]
async fn delete_fork_closes_the_served_instance() {
    let (reg, _dir) = open_registry();
    let primary = reg.primary();
    put_text(&primary, "k", "v").await;
    let id = fork(&primary, "doomed").await;
    reg.resolve(&id).await.map_err(|_| "resolve").unwrap();
    assert_eq!(reg.open_count(), 1);

    // the registry holds the fork open; deleteFork must close it first
    // (otherwise the engine's flock check refuses on our own account)
    let d = run(&primary, r#"mutation { deleteFork(name: "doomed") }"#).await;
    assert_eq!(d["deleteFork"], json!(true));
    assert_eq!(reg.open_count(), 0);
    let d = run(&primary, r#"{ forks { name } }"#).await;
    assert_eq!(d["forks"].as_array().unwrap().len(), 0);
    assert!(matches!(
        reg.resolve(&id).await,
        Err(ResolveError::UnknownInstance)
    ));
}

#[tokio::test]
async fn lru_cap_and_idle_ttl_evict() {
    let (reg, _dir) = open_registry_with(RegistryConfig {
        max_open: 1,
        idle_ttl: Duration::from_secs(0),
    });
    let primary = reg.primary();
    put_text(&primary, "k", "v").await;
    let id_a = fork(&primary, "a").await;
    let id_b = fork(&primary, "b").await;

    reg.resolve(&id_a).await.map_err(|_| "resolve").unwrap();
    reg.resolve(&id_b).await.map_err(|_| "resolve").unwrap();
    assert_eq!(reg.open_count(), 1, "LRU cap must hold");
    // the evicted instance reopens transparently
    let a = reg.resolve(&id_a).await.map_err(|_| "resolve").unwrap();
    assert_eq!(get_text(&a, "k").await.as_deref(), Some("v"));

    reg.evict_idle(); // ttl 0: everything idle
    assert_eq!(reg.open_count(), 0);
    reg.resolve(&id_b).await.map_err(|_| "resolve").unwrap();
    assert_eq!(reg.open_count(), 1);
}

// ---------------------------------------------------------------------------
// HTTP path layer
// ---------------------------------------------------------------------------

mod http {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn post(router: &axum::Router, path: &str, query: &str) -> (StatusCode, Value) {
        let req = HttpRequest::post(path)
            .header("content-type", "application/json")
            .body(Body::from(json!({ "query": query }).to_string()))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, v)
    }

    #[tokio::test]
    async fn routes_primary_and_instances() {
        let (reg, _dir) = open_registry();
        let router = fluent_graphql::router(reg.clone(), 1 << 20);

        let (st, v) = post(
            &router,
            "/graphql",
            r#"mutation { put(key: {text: "k"}, value: {text: "v"}) fork(name: "b") { instanceId } }"#,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let id = v["data"]["fork"]["instanceId"].as_str().unwrap().to_string();

        // the fork answers on its own path
        let (st, v) = post(
            &router,
            &format!("/graphql/{id}"),
            r#"{ get(key: {text: "k"}) { text } }"#,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["data"]["get"]["text"], json!("v"));

        // unknown instance: 404, not a GraphQL error envelope
        let (st, _) = post(
            &router,
            "/graphql/00000000000000000000000000000000",
            r#"{ snapshotSeqno }"#,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
    }
}
