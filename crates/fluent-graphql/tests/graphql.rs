//! End-to-end tests: execute GraphQL operations against a real temp-dir Db
//! through the public schema, exactly as the HTTP handler does (via
//! `prepare`).

use std::sync::Arc;

use async_graphql::{Request, Variables};
use fluent31::{Db, Options, SyncMode};
use fluent_graphql::{build_schema, prepare, FluentSchema};
use serde_json::{json, Value};

fn open_schema_with(opts: Options) -> (FluentSchema, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts).unwrap();
    (build_schema(Arc::new(db)), dir)
}

fn open_schema() -> (FluentSchema, tempfile::TempDir) {
    open_schema_with(Options {
        sync: SyncMode::Never, // macOS F_FULLFSYNC is ~15ms/op
        ..Options::default()
    })
}

/// U64 scalar fields arrive as decimal strings.
fn u64_field(v: &Value) -> u64 {
    v.as_str().unwrap().parse().unwrap()
}

async fn run(schema: &FluentSchema, query: &str, vars: Value) -> Value {
    let req = Request::new(query).variables(Variables::from_json(vars));
    let resp = schema.execute(prepare(req)).await;
    assert!(
        resp.errors.is_empty(),
        "unexpected errors for {query}: {:?}",
        resp.errors
    );
    resp.data.into_json().unwrap()
}

async fn run_err(schema: &FluentSchema, query: &str, vars: Value) -> Vec<async_graphql::ServerError> {
    let req = Request::new(query).variables(Variables::from_json(vars));
    let resp = schema.execute(prepare(req)).await;
    assert!(!resp.errors.is_empty(), "expected errors for {query}");
    resp.errors
}

fn ext_code(errs: &[async_graphql::ServerError]) -> Option<String> {
    let ext = errs[0].extensions.as_ref()?;
    let v = ext.get("code")?;
    Some(format!("{v}").trim_matches('"').to_string())
}

// ---------------------------------------------------------------------------
// direct operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_roundtrip_and_missing() {
    let (schema, _dir) = open_schema();
    let d = run(
        &schema,
        r#"mutation { put(key: {text: "k1"}, value: {text: "hello"}) }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["put"], json!(true));

    let d = run(
        &schema,
        r#"{ get(key: {text: "k1"}) { text base64 hex len } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["get"]["text"], json!("hello"));
    assert_eq!(d["get"]["base64"], json!("aGVsbG8="));
    assert_eq!(d["get"]["hex"], json!("68656c6c6f"));
    assert_eq!(d["get"]["len"], json!(5));

    let d = run(&schema, r#"{ get(key: {text: "nope"}) { text } }"#, json!({})).await;
    assert_eq!(d["get"], Value::Null);
}

#[tokio::test]
async fn encodings_are_interchangeable() {
    let (schema, _dir) = open_schema();
    // "k1" as base64 ("azE=") and hex ("6b31") must address the same key.
    run(
        &schema,
        r#"mutation { put(key: {base64: "azE="}, value: {hex: "00ff"}) }"#,
        json!({}),
    )
    .await;
    let d = run(
        &schema,
        r#"{ get(key: {hex: "6b31"}) { text hex len } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["get"]["hex"], json!("00ff"));
    assert_eq!(d["get"]["text"], Value::Null); // 0x00 0xff is not UTF-8
    assert_eq!(d["get"]["len"], json!(2));
}

#[tokio::test]
async fn bad_encodings_are_rejected() {
    let (schema, _dir) = open_schema();
    run_err(
        &schema,
        r#"mutation { put(key: {hex: "xyz"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    run_err(
        &schema,
        r#"mutation { put(key: {base64: "!!!"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    // oneof: exactly one representation
    run_err(
        &schema,
        r#"mutation { put(key: {text: "a", hex: "61"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
}

#[tokio::test]
async fn delete_removes_key() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation { put(key: {text: "dk"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    let d = run(&schema, r#"mutation { delete(key: {text: "dk"}) }"#, json!({})).await;
    assert_eq!(d["delete"], json!(true));
    let d = run(&schema, r#"{ get(key: {text: "dk"}) { text } }"#, json!({})).await;
    assert_eq!(d["get"], Value::Null);
}

#[tokio::test]
async fn write_batch_applies_atomically() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation { put(key: {text: "b3"}, value: {text: "old"}) }"#,
        json!({}),
    )
    .await;
    let d = run(
        &schema,
        r#"mutation {
            writeBatch(ops: [
                {put: {key: {text: "b1"}, value: {text: "v1"}}},
                {put: {key: {text: "b2"}, value: {text: "v2"}}},
                {delete: {text: "b3"}}
            ])
        }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["writeBatch"], json!(3));
    let d = run(
        &schema,
        r#"{ a: get(key: {text: "b1"}) { text }
             b: get(key: {text: "b2"}) { text }
             c: get(key: {text: "b3"}) { text } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["a"]["text"], json!("v1"));
    assert_eq!(d["b"]["text"], json!("v2"));
    assert_eq!(d["c"], Value::Null);
}

#[tokio::test]
async fn invalid_key_rejects_whole_batch() {
    let (schema, _dir) = open_schema();
    // Reserved 0x00-prefixed key must reject the batch; the valid op in the
    // same batch must not land.
    let errs = run_err(
        &schema,
        r#"mutation {
            writeBatch(ops: [
                {put: {key: {text: "good"}, value: {text: "v"}}},
                {put: {key: {hex: "0001"}, value: {text: "v"}}}
            ])
        }"#,
        json!({}),
    )
    .await;
    assert_eq!(ext_code(&errs).as_deref(), Some("INVALID_ARGUMENT"));
    let d = run(&schema, r#"{ get(key: {text: "good"}) { text } }"#, json!({})).await;
    assert_eq!(d["get"], Value::Null);
}

// ---------------------------------------------------------------------------
// scans
// ---------------------------------------------------------------------------

async fn seed_scan_data(schema: &FluentSchema) {
    for i in 0..5 {
        run(
            schema,
            r#"mutation Put($k: BytesInput!, $v: BytesInput!) { put(key: $k, value: $v) }"#,
            json!({"k": {"text": format!("scan/{i}")}, "v": {"text": format!("v{i}")}}),
        )
        .await;
    }
    run(
        schema,
        r#"mutation { put(key: {text: "other/x"}, value: {text: "ov"}) }"#,
        json!({}),
    )
    .await;
}

#[tokio::test]
async fn scan_prefix_limit_and_pagination() {
    let (schema, _dir) = open_schema();
    seed_scan_data(&schema).await;

    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "scan/"}, limit: 2) {
            pairs { key { text } value { text } } hasMore nextAfter { text } } }"#,
        json!({}),
    )
    .await;
    let s = &d["scan"];
    assert_eq!(s["pairs"].as_array().unwrap().len(), 2);
    assert_eq!(s["pairs"][0]["key"]["text"], json!("scan/0"));
    assert_eq!(s["pairs"][1]["key"]["text"], json!("scan/1"));
    assert_eq!(s["hasMore"], json!(true));
    assert_eq!(s["nextAfter"]["text"], json!("scan/1"));

    // follow the cursor to the end
    let d = run(
        &schema,
        r#"query Next($after: BytesInput) {
            scan(prefix: {text: "scan/"}, after: $after, limit: 10) {
            pairs { key { text } } hasMore nextAfter { text } } }"#,
        json!({"after": {"text": "scan/1"}}),
    )
    .await;
    let s = &d["scan"];
    let keys: Vec<&str> = s["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["scan/2", "scan/3", "scan/4"]);
    assert_eq!(s["hasMore"], json!(false));
    assert_eq!(s["nextAfter"], Value::Null);
}

#[tokio::test]
async fn scan_reverse_and_range() {
    let (schema, _dir) = open_schema();
    seed_scan_data(&schema).await;

    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "scan/"}, reverse: true, limit: 3) {
            pairs { key { text } } nextAfter { text } hasMore } }"#,
        json!({}),
    )
    .await;
    let s = &d["scan"];
    let keys: Vec<&str> = s["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["scan/4", "scan/3", "scan/2"]);
    assert_eq!(s["hasMore"], json!(true));

    // reverse pagination continues below the cursor
    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "scan/"}, reverse: true, after: {text: "scan/2"}, limit: 10) {
            pairs { key { text } } hasMore } }"#,
        json!({}),
    )
    .await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["scan/1", "scan/0"]);

    // half-open [lo, hi)
    let d = run(
        &schema,
        r#"{ scan(lo: {text: "scan/1"}, hi: {text: "scan/3"}) { pairs { key { text } } } }"#,
        json!({}),
    )
    .await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["scan/1", "scan/2"]);
}

#[tokio::test]
async fn scan_argument_validation() {
    let (schema, _dir) = open_schema();
    run_err(
        &schema,
        r#"{ scan(prefix: {text: "p"}, lo: {text: "a"}) { hasMore } }"#,
        json!({}),
    )
    .await;
    run_err(&schema, r#"{ scan(limit: 0) { hasMore } }"#, json!({})).await;
    run_err(&schema, r#"{ scan(limit: 10001) { hasMore } }"#, json!({})).await;
}

#[tokio::test]
async fn unbounded_scan_hides_system_keyspace() {
    let (schema, _dir) = open_schema();
    // install a module (stored under the reserved 0x00 keyspace)...
    run(
        &schema,
        r#"mutation Install($w: BytesInput!) { installModule(name: "echo", wasm: $w) { name } }"#,
        json!({"w": {"text": ECHO_WAT}}),
    )
    .await;
    run(
        &schema,
        r#"mutation { put(key: {text: "user-key"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    // ...then a fully unbounded scan must only see user keys.
    let d = run(&schema, r#"{ scan { pairs { key { text } } } }"#, json!({})).await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["user-key"]);
}

// ---------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_seqno_advances_between_requests() {
    let (schema, _dir) = open_schema();
    let d1 = run(&schema, r#"{ snapshotSeqno }"#, json!({})).await;
    run(
        &schema,
        r#"mutation { put(key: {text: "s"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    let d2 = run(&schema, r#"{ snapshotSeqno }"#, json!({})).await;
    assert!(
        u64_field(&d2["snapshotSeqno"]) > u64_field(&d1["snapshotSeqno"]),
        "snapshot must move forward after a write: {d1} -> {d2}"
    );
}

// ---------------------------------------------------------------------------
// wasm
// ---------------------------------------------------------------------------

/// Echoes its input (read-only query module).
const ECHO_WAT: &str = r#"
(module
  (import "fluent" "input_len" (func $input_len (result i32)))
  (import "fluent" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run") (result i32)
    (local $n i32)
    (local.set $n (call $input_len))
    (drop (call $input_read (i32.const 0) (local.get $n) (i32.const 0)))
    (drop (call $output_write (i32.const 0) (local.get $n)))
    (i32.const 0)))
"#;

/// Puts key "wk" = input bytes (executor module).
const PUT_INPUT_WAT: &str = r#"
(module
  (import "fluent" "input_len" (func $input_len (result i32)))
  (import "fluent" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "wk")
  (func (export "run") (result i32)
    (local $n i32)
    (local.set $n (call $input_len))
    (drop (call $input_read (i32.const 0) (local.get $n) (i32.const 0)))
    (call $put (i32.const 1024) (i32.const 2) (i32.const 0) (local.get $n))))
"#;

/// Always exits 7 with output "boom".
const FAIL_WAT: &str = r#"
(module
  (import "fluent" "output_write" (func $output_write (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "boom")
  (func (export "run") (result i32)
    (drop (call $output_write (i32.const 0) (i32.const 4)))
    (i32.const 7)))
"#;

#[tokio::test]
async fn wasm_install_list_query_uninstall() {
    let (schema, _dir) = open_schema();
    let d = run(
        &schema,
        r#"mutation Install($w: BytesInput!) { installModule(name: "echo", wasm: $w) { name size } }"#,
        json!({"w": {"text": ECHO_WAT}}),
    )
    .await;
    assert_eq!(d["installModule"]["name"], json!("echo"));

    let d = run(&schema, r#"{ modules { name } }"#, json!({})).await;
    assert_eq!(d["modules"][0]["name"], json!("echo"));

    let d = run(
        &schema,
        r#"{ wasm(module: "echo", input: {text: "ping"}) { text } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["wasm"]["text"], json!("ping"));

    // no input → empty input bytes
    let d = run(&schema, r#"{ wasm(module: "echo") { len } }"#, json!({})).await;
    assert_eq!(d["wasm"]["len"], json!(0));

    run(
        &schema,
        r#"mutation { uninstallModule(name: "echo") }"#,
        json!({}),
    )
    .await;
    let errs = run_err(
        &schema,
        r#"{ wasm(module: "echo") { len } }"#,
        json!({}),
    )
    .await;
    assert!(ext_code(&errs).is_some(), "engine error must carry a code");
}

#[tokio::test]
async fn wasm_executor_writes_transactionally() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation Install($w: BytesInput!) { installModule(name: "writer", wasm: $w) { name } }"#,
        json!({"w": {"text": PUT_INPUT_WAT}}),
    )
    .await;
    let d = run(
        &schema,
        r#"mutation { wasmExecute(module: "writer", input: {text: "hello-txn"}) { len } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["wasmExecute"]["len"], json!(0));
    let d = run(&schema, r#"{ get(key: {text: "wk"}) { text } }"#, json!({})).await;
    assert_eq!(d["get"]["text"], json!("hello-txn"));
}

#[tokio::test]
async fn wasm_guest_failure_carries_exit_code_and_output() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation Install($w: BytesInput!) { installModule(name: "boom", wasm: $w) { name } }"#,
        json!({"w": {"text": FAIL_WAT}}),
    )
    .await;
    let errs = run_err(&schema, r#"{ wasm(module: "boom") { len } }"#, json!({})).await;
    assert_eq!(ext_code(&errs).as_deref(), Some("GUEST_FAILED"));
    let ext = errs[0].extensions.as_ref().unwrap();
    assert_eq!(format!("{}", ext.get("guestExitCode").unwrap()), "7");
    assert_eq!(
        format!("{}", ext.get("guestOutputText").unwrap()).trim_matches('"'),
        "boom"
    );
}

#[tokio::test]
async fn installing_garbage_module_fails_with_wasm_code() {
    let (schema, _dir) = open_schema();
    let errs = run_err(
        &schema,
        r#"mutation { installModule(name: "junk", wasm: {hex: "deadbeef"}) { name } }"#,
        json!({}),
    )
    .await;
    assert_eq!(ext_code(&errs).as_deref(), Some("WASM"));
}

// ---------------------------------------------------------------------------
// admin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stats_and_maintenance() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation { put(key: {text: "st"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    let d = run(
        &schema,
        r#"{ stats { backend visibleSeqno memtableBytes levels { runs tables bytes } } }"#,
        json!({}),
    )
    .await;
    assert!(u64_field(&d["stats"]["visibleSeqno"]) >= 1);
    assert!(d["stats"]["backend"].as_str().is_some());

    let d = run(
        &schema,
        r#"mutation { flush compactAll gcVlog { retired } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["flush"], json!(true));
    assert_eq!(d["compactAll"], json!(true));
    // fresh store: gc ran but no vlog victim qualified
    assert_eq!(d["gcVlog"], json!({ "retired": Value::Null }));
}

#[tokio::test]
async fn checkpoint_lifecycle() {
    let (schema, _dir) = open_schema();
    run(
        &schema,
        r#"mutation { put(key: {text: "ck"}, value: {text: "v"}) }"#,
        json!({}),
    )
    .await;
    let d = run(
        &schema,
        r#"mutation { checkpoint(name: "snap1") { name lastSeqno } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["checkpoint"]["name"], json!("snap1"));
    assert!(u64_field(&d["checkpoint"]["lastSeqno"]) >= 1);

    let d = run(&schema, r#"{ checkpoints { name } }"#, json!({})).await;
    assert_eq!(d["checkpoints"][0]["name"], json!("snap1"));

    run(
        &schema,
        r#"mutation { deleteCheckpoint(name: "snap1") }"#,
        json!({}),
    )
    .await;
    let d = run(&schema, r#"{ checkpoints { name } }"#, json!({})).await;
    assert_eq!(d["checkpoints"].as_array().unwrap().len(), 0);

    let errs = run_err(
        &schema,
        r#"mutation { checkpoint(name: "../evil") { name } }"#,
        json!({}),
    )
    .await;
    assert_eq!(ext_code(&errs).as_deref(), Some("INVALID_ARGUMENT"));
}

// ---------------------------------------------------------------------------
// review-workflow coverage: cursor/bound clamps, prefix carries, vlog path,
// pagination under mutation, concurrent executors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn omitted_nullable_variables_apply_defaults() {
    let (schema, _dir) = open_schema();
    seed_scan_data(&schema).await;
    // the common client pattern: optional $limit/$reverse variables, omitted
    let d = run(
        &schema,
        r#"query S($l: Int, $r: Boolean) {
            scan(prefix: {text: "scan/"}, limit: $l, reverse: $r) { pairs { key { text } } hasMore } }"#,
        json!({}),
    )
    .await;
    let pairs = d["scan"]["pairs"].as_array().unwrap();
    assert_eq!(pairs.len(), 5, "default limit 100, forward order");
    assert_eq!(pairs[0]["key"]["text"], json!("scan/0"));
}

#[tokio::test]
async fn failed_mutation_field_keeps_siblings_visible() {
    let (schema, _dir) = open_schema();
    let req = async_graphql::Request::new(
        r#"mutation {
            a: put(key: {text: "x"}, value: {text: "v"})
            b: delete(key: {hex: "0001"})
            c: put(key: {text: "y"}, value: {text: "v"})
        }"#,
    );
    let resp = schema.execute(prepare(req)).await;
    assert_eq!(resp.errors.len(), 1, "only field b fails: {:?}", resp.errors);
    let data = resp.data.into_json().unwrap();
    // nullable fields: the failed one is null, committed siblings visible
    assert_eq!(data["a"], json!(true));
    assert_eq!(data["b"], Value::Null);
    assert_eq!(data["c"], json!(true));
    let d = run(&schema, r#"{ y: get(key: {text: "y"}) { text } }"#, json!({})).await;
    assert_eq!(d["y"]["text"], json!("v"));
}

#[tokio::test]
async fn scan_after_clamps_against_explicit_bounds() {
    let (schema, _dir) = open_schema();
    seed_scan_data(&schema).await;

    // forward: lo wins over after-successor when lo is higher
    let d = run(
        &schema,
        r#"{ scan(lo: {text: "scan/2"}, hi: {text: "scan/9"}, after: {text: "scan/0"}, limit: 1) {
            pairs { key { text } } } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"][0]["key"]["text"], json!("scan/2"));

    // forward: after-successor wins when it is higher than lo
    let d = run(
        &schema,
        r#"{ scan(lo: {text: "scan/2"}, hi: {text: "scan/9"}, after: {text: "scan/3"}, limit: 10) {
            pairs { key { text } } hasMore } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"][0]["key"]["text"], json!("scan/4"));
    assert_eq!(d["scan"]["pairs"].as_array().unwrap().len(), 1);
    assert_eq!(d["scan"]["hasMore"], json!(false));

    // forward: cursor past hi -> empty terminal page
    let d = run(
        &schema,
        r#"{ scan(lo: {text: "scan/0"}, hi: {text: "scan/2"}, after: {text: "scan/3"}, limit: 10) {
            pairs { key { text } } hasMore nextAfter { text } } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"].as_array().unwrap().len(), 0);
    assert_eq!(d["scan"]["hasMore"], json!(false));
    assert_eq!(d["scan"]["nextAfter"], Value::Null);

    // reverse: hi wins over after when hi is lower
    let d = run(
        &schema,
        r#"{ scan(hi: {text: "scan/3"}, after: {text: "scan/4"}, reverse: true, limit: 1) {
            pairs { key { text } } } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"][0]["key"]["text"], json!("scan/2"));

    // reverse: after wins when it is lower than hi
    let d = run(
        &schema,
        r#"{ scan(lo: {text: "scan/"}, hi: {text: "scan/9"}, after: {text: "scan/2"}, reverse: true, limit: 10) {
            pairs { key { text } } } }"#,
        json!({}),
    )
    .await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["scan/1", "scan/0"]);
}

#[tokio::test]
async fn prefix_carry_over_ff_bytes() {
    let (schema, _dir) = open_schema();
    for k in ["fe", "ff", "ff00", "ffff", "abff01", "ac00"] {
        run(
            &schema,
            r#"mutation P($k: BytesInput!) { put(key: $k, value: {text: "v"}) }"#,
            json!({"k": {"hex": k}}),
        )
        .await;
    }
    // all-0xFF prefix: hi is unbounded above
    let d = run(
        &schema,
        r#"{ scan(prefix: {hex: "ff"}) { pairs { key { hex } } } }"#,
        json!({}),
    )
    .await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["hex"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["ff", "ff00", "ffff"]);

    // trailing-0xFF prefix carries into the earlier byte: hi = [0xac]
    let d = run(
        &schema,
        r#"{ scan(prefix: {hex: "abff"}) { pairs { key { hex } } } }"#,
        json!({}),
    )
    .await;
    let keys: Vec<&str> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["hex"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["abff01"]);
}

#[tokio::test]
async fn vlog_resident_values_roundtrip_and_paginate() {
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;
    // force the value-log path for modest values
    let (schema, _dir) = open_schema_with(Options {
        sync: SyncMode::Never,
        value_threshold: 128,
        ..Options::default()
    });
    let big_a: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
    let big_b: Vec<u8> = (0..8192u32).map(|i| (i % 241) as u8).collect();
    for (k, v) in [("vlog/a", &big_a), ("vlog/b", &big_b)] {
        run(
            &schema,
            r#"mutation P($k: BytesInput!, $v: BytesInput!) { put(key: $k, value: $v) }"#,
            json!({"k": {"text": k}, "v": {"base64": B64.encode(v)}}),
        )
        .await;
    }
    // flush so reads go through tables + vlog pointers, not the memtable
    run(&schema, r#"mutation { flush }"#, json!({})).await;

    let d = run(
        &schema,
        r#"{ get(key: {text: "vlog/a"}) { base64 len } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["get"]["len"], json!(8192));
    assert_eq!(d["get"]["base64"].as_str().unwrap(), B64.encode(&big_a));

    // paginated scan: the limit+1 read-ahead materializes a pointer value
    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "vlog/"}, limit: 1) {
            pairs { key { text } value { len } } hasMore nextAfter { text } } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"][0]["value"]["len"], json!(8192));
    assert_eq!(d["scan"]["hasMore"], json!(true));
    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "vlog/"}, after: {text: "vlog/a"}) {
            pairs { key { text } value { base64 } } hasMore } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["scan"]["pairs"][0]["key"]["text"], json!("vlog/b"));
    assert_eq!(
        d["scan"]["pairs"][0]["value"]["base64"].as_str().unwrap(),
        B64.encode(&big_b)
    );
    assert_eq!(d["scan"]["hasMore"], json!(false));
}

#[tokio::test]
async fn pagination_under_mutation_uses_fresh_snapshot_and_exact_successor() {
    let (schema, _dir) = open_schema();
    seed_scan_data(&schema).await;

    let d = run(
        &schema,
        r#"{ scan(prefix: {text: "scan/"}, limit: 2) { nextAfter { hex } } }"#,
        json!({}),
    )
    .await;
    let cursor = d["scan"]["nextAfter"]["hex"].as_str().unwrap().to_string();
    assert_eq!(cursor, hex::encode(b"scan/1"));

    // between pages: insert the cursor's exact successor and delete scan/2
    run(
        &schema,
        r#"mutation P($k: BytesInput!) { put(key: $k, value: {text: "wedge"}) }"#,
        json!({"k": {"hex": format!("{cursor}00")}}),
    )
    .await;
    run(&schema, r#"mutation { delete(key: {text: "scan/2"}) }"#, json!({})).await;

    // page 2 pins a fresh snapshot: sees the wedge, not the deleted key,
    // and never repeats the cursor
    let d = run(
        &schema,
        r#"query N($a: BytesInput) { scan(prefix: {text: "scan/"}, after: $a) {
            pairs { key { hex } value { text } } hasMore } }"#,
        json!({"a": {"hex": cursor}}),
    )
    .await;
    let keys: Vec<String> = d["scan"]["pairs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["key"]["hex"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        keys,
        vec![
            hex::encode(b"scan/1\0"),
            hex::encode(b"scan/3"),
            hex::encode(b"scan/4"),
        ]
    );
    assert_eq!(d["scan"]["pairs"][0]["value"]["text"], json!("wedge"));
}

/// Transactional counter: get_for_update("ctr"), +1 (LE u64), put. Exits 0
/// on success so the engine commits; conflicts retry via execute_retries.
const COUNTER_WAT: &str = r#"
(module
  (import "fluent" "get_for_update" (func $gfu (param i32 i32 i32 i32 i32) (result i64)))
  (import "fluent" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "ctr")
  (func (export "run") (result i32)
    (local $r i64)
    (local.set $r (call $gfu (i32.const 0) (i32.const 3) (i32.const 0) (i32.const 16) (i32.const 8)))
    (if (i64.lt_s (local.get $r) (i64.const 0))
      (then
        (if (i64.ne (local.get $r) (i64.const -1))
          (then (return (i32.const 9))))
        (i64.store (i32.const 16) (i64.const 0))))
    (i64.store (i32.const 16) (i64.add (i64.load (i32.const 16)) (i64.const 1)))
    (call $put (i32.const 0) (i32.const 3) (i32.const 16) (i32.const 8))))
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_wasm_executors_do_not_lose_updates() {
    const N: usize = 8;
    let (schema, _dir) = open_schema_with(Options {
        sync: SyncMode::Never,
        execute_retries: 64, // absorb worst-case OCC conflict cascades
        ..Options::default()
    });
    run(
        &schema,
        r#"mutation I($w: BytesInput!) { installModule(name: "counter", wasm: $w) { name } }"#,
        json!({"w": {"text": COUNTER_WAT}}),
    )
    .await;

    let mut handles = Vec::new();
    for _ in 0..N {
        let s = schema.clone();
        handles.push(tokio::spawn(async move {
            s.execute(prepare(async_graphql::Request::new(
                r#"mutation { wasmExecute(module: "counter") { len } }"#,
            )))
            .await
        }));
    }
    for h in handles {
        let resp = h.await.unwrap();
        assert!(resp.errors.is_empty(), "executor failed: {:?}", resp.errors);
    }

    let d = run(&schema, r#"{ get(key: {text: "ctr"}) { hex } }"#, json!({})).await;
    let mut expect = [0u8; 8];
    expect[0] = N as u8;
    assert_eq!(d["get"]["hex"].as_str().unwrap(), hex::encode(expect));
}

mod hex {
    pub fn encode(b: impl AsRef<[u8]>) -> String {
        b.as_ref().iter().map(|x| format!("{x:02x}")).collect()
    }
}
