//! Adversarial GraphQL: query-shape bombs (depth, complexity / alias
//! amplification), malformed byte inputs, oversized values, oversized request
//! bodies, and large-but-legitimate batches. The server must reject the
//! pathological shapes without executing them and without crashing, while
//! still serving the large-legitimate ones.

use std::sync::Arc;

use async_graphql::{Request, Variables};
use fluent31::{Db, Options, SyncMode};
use fluent_graphql::SchemaManager;
use serde_json::{json, Value};

fn open_schema_with(opts: Options) -> (Arc<SchemaManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts).unwrap();
    (SchemaManager::new(Arc::new(db)).unwrap(), dir)
}

fn open_schema() -> (Arc<SchemaManager>, tempfile::TempDir) {
    open_schema_with(Options {
        sync: SyncMode::Never,
        ..Options::default()
    })
}

async fn exec(schema: &SchemaManager, query: String) -> async_graphql::Response {
    let req = Request::new(query).variables(Variables::from_json(json!({})));
    schema.execute(req).await
}

fn ext_code(errs: &[async_graphql::ServerError]) -> Option<String> {
    let ext = errs[0].extensions.as_ref()?;
    let v = ext.get("code")?;
    Some(format!("{v}").trim_matches('"').to_string())
}

// ---------------------------------------------------------------------------
// Query-shape bombs are rejected at validation, before execution
// ---------------------------------------------------------------------------

/// Depth cap (`limit_depth(32)`): a deeply nested selection is rejected. We
/// nest introspection's recursive `ofType` well past the limit — validation
/// runs on shape, so it never matters that the leaves resolve to null.
#[tokio::test]
async fn deeply_nested_query_is_rejected() {
    let (schema, _dir) = open_schema();
    let mut q = String::from(r#"{ __type(name: "Bytes") { "#);
    let depth = 60;
    for _ in 0..depth {
        q.push_str("ofType { ");
    }
    q.push_str("name ");
    for _ in 0..depth {
        q.push_str("} ");
    }
    q.push_str("} }");

    let resp = exec(&schema, q).await;
    assert!(!resp.errors.is_empty(), "deep query should be rejected");
    assert!(resp.data.into_json().unwrap().is_null(), "must not execute");
    let msg = resp.errors[0].message.to_lowercase();
    assert!(msg.contains("depth"), "expected a depth error, got: {msg}");
}

/// Complexity cap (`limit_complexity(5000)`) is the alias-amplification
/// defense: 6000 aliased fields exceed the budget and are rejected wholesale.
#[tokio::test]
async fn alias_flood_exceeding_complexity_is_rejected() {
    let (schema, _dir) = open_schema();
    let mut q = String::from("{ ");
    for i in 0..6000 {
        q.push_str(&format!("a{i}: snapshotSeqno "));
    }
    q.push('}');

    let resp = exec(&schema, q).await;
    assert!(!resp.errors.is_empty(), "alias flood should be rejected");
    assert!(resp.data.into_json().unwrap().is_null());
    let msg = resp.errors[0].message.to_lowercase();
    assert!(msg.contains("complex"), "expected a complexity error, got: {msg}");
}

/// A wide-but-legal fan-out (under the complexity budget) still executes: the
/// caps gate abuse without breaking legitimately broad queries.
#[tokio::test]
async fn moderate_alias_fan_out_still_executes() {
    let (schema, _dir) = open_schema();
    let n = 1000;
    let mut q = String::from("{ ");
    for i in 0..n {
        q.push_str(&format!("a{i}: snapshotSeqno "));
    }
    q.push('}');

    let resp = exec(&schema, q).await;
    assert!(resp.errors.is_empty(), "under-budget query errored: {:?}", resp.errors);
    let data = resp.data.into_json().unwrap();
    assert_eq!(data.as_object().unwrap().len(), n, "all aliases resolved");
}

// ---------------------------------------------------------------------------
// Malformed and oversized inputs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn malformed_byte_inputs_are_rejected() {
    let (schema, _dir) = open_schema();
    let cases = [
        // odd-length hex
        r#"mutation { put(key: {hex: "abc"}, value: {text: "v"}) }"#,
        // non-hex digit
        r#"mutation { put(key: {hex: "zz"}, value: {text: "v"}) }"#,
        // invalid base64
        r#"mutation { put(key: {base64: "@@@@"}, value: {text: "v"}) }"#,
        // oneof violation: two representations at once
        r#"mutation { put(key: {text: "a", hex: "61"}, value: {text: "v"}) }"#,
        // oneof violation: none present
        r#"mutation { put(key: {}, value: {text: "v"}) }"#,
    ];
    for q in cases {
        let resp = exec(&schema, q.to_string()).await;
        assert!(!resp.errors.is_empty(), "should reject: {q}");
    }
    // the store stayed empty — no half-applied writes from any rejected input
    let resp = exec(&schema, r#"{ scan(limit: 10) { pairs { key { text } } } }"#.to_string()).await;
    let d = resp.data.into_json().unwrap();
    assert_eq!(d["scan"]["pairs"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn oversized_value_is_rejected_with_engine_code() {
    // a small value cap surfaces the engine's INVALID_ARGUMENT through GraphQL
    let (schema, _dir) = open_schema_with(Options {
        sync: SyncMode::Never,
        max_value_size: 1024,
        ..Options::default()
    });
    // 2048 bytes of value, supplied as 4096 hex chars
    let big_hex = "61".repeat(2048);
    let q = format!(r#"mutation {{ put(key: {{text: "k"}}, value: {{hex: "{big_hex}"}}) }}"#);
    let resp = exec(&schema, q).await;
    assert!(!resp.errors.is_empty());
    assert_eq!(ext_code(&resp.errors).as_deref(), Some("INVALID_ARGUMENT"));
    // nothing was written
    let resp = exec(&schema, r#"{ get(key: {text: "k"}) { len } }"#.to_string()).await;
    assert_eq!(resp.data.into_json().unwrap()["get"], Value::Null);
}

/// A large but well-formed value (multi-hundred-KB) roundtrips fine under the
/// default caps: "big" is not "hostile".
#[tokio::test]
async fn large_legitimate_value_roundtrips() {
    let (schema, _dir) = open_schema();
    let big_hex = "ab".repeat(400 * 1024); // 400 KiB value
    let q = format!(r#"mutation {{ put(key: {{text: "big"}}, value: {{hex: "{big_hex}"}}) }}"#);
    let resp = exec(&schema, q).await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    let resp = exec(&schema, r#"{ get(key: {text: "big"}) { len } }"#.to_string()).await;
    assert_eq!(resp.data.into_json().unwrap()["get"]["len"], json!(400 * 1024));
}

/// A large `writeBatch` (thousands of ops) applies atomically — the batch API
/// is not artificially capped, and a legitimately big batch is served.
#[tokio::test]
async fn large_write_batch_applies_atomically() {
    let (schema, _dir) = open_schema();
    let n = 2000;
    let mut ops = String::new();
    for i in 0..n {
        ops.push_str(&format!(
            r#"{{put: {{key: {{text: "k{i:05}"}}, value: {{text: "v{i}"}}}}}}, "#
        ));
    }
    let q = format!("mutation {{ writeBatch(ops: [{ops}]) }}");
    let resp = exec(&schema, q).await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    assert_eq!(resp.data.into_json().unwrap()["writeBatch"], json!(n));

    // spot-check a few and the total count
    let resp = exec(
        &schema,
        r#"{ scan(prefix: {text: "k"}, limit: 10000) { pairs { key { text } } } }"#.to_string(),
    )
    .await;
    let d = resp.data.into_json().unwrap();
    assert_eq!(d["scan"]["pairs"].as_array().unwrap().len(), n);
}

// ---------------------------------------------------------------------------
// HTTP request-body ceiling
// ---------------------------------------------------------------------------

mod http {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use fluent_graphql::{InstanceRegistry, RegistryConfig};
    use tower::ServiceExt;

    fn open_router(max_body: usize) -> (axum::Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync: SyncMode::Never,
            ..Options::default()
        };
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        let mgr = SchemaManager::new(Arc::new(db)).unwrap();
        let reg = InstanceRegistry::new(mgr, dir.path(), opts, RegistryConfig::default());
        (fluent_graphql::router(reg, max_body), dir)
    }

    async fn post(router: &axum::Router, body: String) -> StatusCode {
        let req = HttpRequest::post("/graphql")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router.clone().oneshot(req).await.unwrap().status()
    }

    /// A body past the configured ceiling is refused at the HTTP layer before
    /// any parsing/execution — the primary guard against multi-MB documents.
    #[tokio::test]
    async fn oversized_request_body_is_rejected() {
        let (router, _dir) = open_router(4096);

        // a comfortably-under-limit request is served
        let small = json!({ "query": "{ snapshotSeqno }" }).to_string();
        assert_eq!(post(&router, small).await, StatusCode::OK);

        // a request whose body exceeds the ceiling is rejected, not executed
        let filler = "x".repeat(64 * 1024);
        let big = json!({ "query": format!("# {filler}\n{{ snapshotSeqno }}") }).to_string();
        let st = post(&router, big).await;
        assert!(
            st == StatusCode::PAYLOAD_TOO_LARGE || st == StatusCode::BAD_REQUEST,
            "oversized body should be refused, got {st}"
        );
    }
}
