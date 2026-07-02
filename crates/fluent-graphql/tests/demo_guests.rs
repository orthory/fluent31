//! End-to-end run of the demo guest pair (`placeOrder` / `topCustomers`):
//! real wasm32 builds installed through GraphQL, exercised through their
//! typed root fields.

use std::path::PathBuf;
use std::sync::{Arc, Once};

use async_graphql::{Request, Variables};
use fluent31::{Db, Options, SyncMode};
use fluent_graphql::SchemaManager;
use serde_json::{json, Value};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the guest crates once per test-process run (same recipe as the
/// engine's wasm tests: rustup's rustc owns the wasm32 std).
fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILD: Once = Once::new();
    let root = workspace_root();
    BUILD.call_once(|| {
        let mut cmd = std::process::Command::new("cargo");
        if let Ok(out) = std::process::Command::new("rustup")
            .args(["which", "rustc"])
            .output()
        {
            if out.status.success() {
                let rustc = String::from_utf8_lossy(&out.stdout).trim().to_string();
                cmd.env("RUSTC", rustc);
            }
        }
        let status = cmd
            .args([
                "build",
                "--manifest-path",
                root.join("guests/Cargo.toml").to_str().unwrap(),
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--target-dir",
                root.join("guests/target").to_str().unwrap(),
            ])
            .env_remove("CARGO_TARGET_DIR")
            .status()
            .expect("cargo build for guests");
        assert!(status.success(), "guest build failed");
    });
    std::fs::read(
        root.join("guests/target/wasm32-unknown-unknown/release")
            .join(format!("{name}.wasm")),
    )
    .expect("guest artifact")
}

async fn run(mgr: &SchemaManager, query: &str, vars: Value) -> Value {
    let req = Request::new(query).variables(Variables::from_json(vars));
    let resp = mgr.execute(req).await;
    assert!(
        resp.errors.is_empty(),
        "unexpected errors for {query}: {:?}",
        resp.errors
    );
    resp.data.into_json().unwrap()
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demo_pair_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options {
            sync: SyncMode::Never,
            execute_retries: 64, // absorb OCC conflict cascades in the concurrent phase
            ..Options::default()
        },
    )
    .unwrap();
    let mgr = SchemaManager::new(Arc::new(db)).unwrap();

    // install both demo modules through GraphQL; both must come back typed
    let d = run(
        &mgr,
        r#"mutation I($po: BytesInput!, $tc: BytesInput!) {
            po: installModule(name: "placeOrder", wasm: $po) { typed schemaError }
            tc: installModule(name: "topCustomers", wasm: $tc) { typed schemaError }
        }"#,
        json!({
            "po": {"base64": b64(&guest_wasm("place_order"))},
            "tc": {"base64": b64(&guest_wasm("top_customers"))},
        }),
    )
    .await;
    assert_eq!(d["po"], json!({"typed": true, "schemaError": Value::Null}));
    assert_eq!(d["tc"], json!({"typed": true, "schemaError": Value::Null}));

    // typed writer: ids allocate sequentially, stats fold in
    let d = run(
        &mgr,
        r#"mutation {
            a: placeOrder(customer: "acme", amountCents: "5000", note: "first!") {
                id customer amountCents customerOrders customerTotalCents }
            b: placeOrder(customer: "acme", amountCents: "2500") {
                id customerOrders customerTotalCents }
            c: placeOrder(customer: "zenith", amountCents: "9000") {
                id customerTotalCents }
        }"#,
        json!({}),
    )
    .await;
    assert_eq!(
        d["a"],
        json!({"id": "1", "customer": "acme", "amountCents": "5000",
               "customerOrders": "1", "customerTotalCents": "5000"})
    );
    assert_eq!(
        d["b"],
        json!({"id": "2", "customerOrders": "2", "customerTotalCents": "7500"})
    );
    assert_eq!(d["c"], json!({"id": "3", "customerTotalCents": "9000"}));

    // typed reader: ranked, averaged, floored — plus direct ops in the
    // same operation against the same snapshot
    let d = run(
        &mgr,
        r#"{
            top: topCustomers(limit: 5) { customer orders totalCents avgCents }
            floor: topCustomers(minTotalCents: "8000") { customer }
            order: get(key: {text: "orders/00000001"}) { text }
        }"#,
        json!({}),
    )
    .await;
    assert_eq!(
        d["top"],
        json!([
            {"customer": "zenith", "orders": "1", "totalCents": "9000", "avgCents": "9000"},
            {"customer": "acme", "orders": "2", "totalCents": "7500", "avgCents": "3750"},
        ])
    );
    assert_eq!(d["floor"], json!([{"customer": "zenith"}]));
    let order: Value =
        serde_json::from_str(d["order"]["text"].as_str().unwrap()).unwrap();
    assert_eq!(order["customer"], json!("acme"));
    assert_eq!(order["note"], json!("first!"));

    // guest-side validation surfaces as GUEST_FAILED with the message
    let resp = mgr
        .execute(Request::new(
            r#"mutation { placeOrder(customer: "not/ok", amountCents: "1") { id } }"#,
        ))
        .await;
    assert_eq!(resp.errors.len(), 1);
    let ext = resp.errors[0].extensions.as_ref().unwrap();
    assert_eq!(format!("{}", ext.get("code").unwrap()).trim_matches('"'), "GUEST_FAILED");
    assert!(
        format!("{}", ext.get("guestOutputText").unwrap()).contains("customer must be"),
        "{:?}",
        resp.errors
    );

    // concurrent orders: OCC retries keep ids unique and totals exact
    const N: usize = 8;
    let mut handles = Vec::new();
    for _ in 0..N {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            mgr.execute(Request::new(
                r#"mutation { placeOrder(customer: "burst", amountCents: "100") { id } }"#,
            ))
            .await
        }));
    }
    let mut ids = std::collections::BTreeSet::new();
    for h in handles {
        let resp = h.await.unwrap();
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        ids.insert(data["placeOrder"]["id"].as_str().unwrap().to_string());
    }
    assert_eq!(ids.len(), N, "order ids must be unique: {ids:?}");
    let d = run(
        &mgr,
        r#"{ topCustomers(limit: 1) { customer totalCents orders } }"#,
        json!({}),
    )
    .await;
    assert_eq!(
        d["topCustomers"][0],
        json!({"customer": "zenith", "totalCents": "9000", "orders": "1"})
    );
    let d = run(
        &mgr,
        r#"{ topCustomers(minTotalCents: "800", limit: 10) { customer totalCents } }"#,
        json!({}),
    )
    .await;
    let burst = d["topCustomers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["customer"] == json!("burst"))
        .unwrap();
    assert_eq!(burst["totalCents"], json!("800"), "no lost updates");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_over_existing_modules_restores_typed_fields_and_state() {
    let dir = tempfile::tempdir().unwrap();
    let opts = || Options {
        sync: SyncMode::Never,
        ..Options::default()
    };

    // session 1: install through GraphQL, place one order
    {
        let mgr = SchemaManager::new(Arc::new(Db::open(dir.path(), opts()).unwrap())).unwrap();
        run(
            &mgr,
            r#"mutation I($po: BytesInput!, $tc: BytesInput!) {
                po: installModule(name: "placeOrder", wasm: $po) { typed }
                tc: installModule(name: "topCustomers", wasm: $tc) { typed }
            }"#,
            json!({
                "po": {"base64": b64(&guest_wasm("place_order"))},
                "tc": {"base64": b64(&guest_wasm("top_customers"))},
            }),
        )
        .await;
        let d = run(
            &mgr,
            r#"mutation { placeOrder(customer: "acme", amountCents: "100") { id } }"#,
            json!({}),
        )
        .await;
        assert_eq!(d["placeOrder"]["id"], json!("1"));
    } // mgr + Db drop: engine shuts down cleanly

    // session 2 (restart): typed fields must exist immediately from
    // collect_outcomes at construction, and state must carry over
    let mgr = SchemaManager::new(Arc::new(Db::open(dir.path(), opts()).unwrap())).unwrap();
    let sdl = mgr.schema().sdl();
    assert!(sdl.contains("placeOrder"), "typed field after restart: {sdl}");
    assert!(sdl.contains("type CustomerStat"), "{sdl}");
    let d = run(
        &mgr,
        r#"mutation { placeOrder(customer: "acme", amountCents: "50") { id customerTotalCents } }"#,
        json!({}),
    )
    .await;
    assert_eq!(d["placeOrder"]["id"], json!("2"), "id sequence continues");
    assert_eq!(d["placeOrder"]["customerTotalCents"], json!("150"));
    let d = run(&mgr, r#"{ topCustomers(limit: 1) { customer orders } }"#, json!({})).await;
    assert_eq!(d["topCustomers"][0], json!({"customer": "acme", "orders": "2"}));
}
