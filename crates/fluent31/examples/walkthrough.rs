//! The whole path in one program: install modules, bind a trigger, drive an
//! executor under contention, wait for derived state to catch up, and
//! rehearse a change on a fork before it touches the real store.
//!
//! Every fluent31 pattern a driver needs is written out here rather than
//! imported. Two of them are the ones a reader has to copy — the trigger
//! drain loop and the caller-side conflict retry — and neither is longer
//! than it looks below. Building the guest workspace is not one of them, so
//! that stays in the helper the other examples share.
//!
//! ```sh
//! cargo run -p fluent31 --example walkthrough
//! ```

#[path = "util/mod.rs"]
mod util;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fluent31::{Db, Error, Options};
use serde_json::{json, Value};
use util::guest_wasm;

const THREADS: u64 = 4;
const PER_THREAD: u64 = 6;
const CUSTOMERS: [&str; 3] = ["acme", "globex", "initech"];

/// Conflicts the engine's own retries did not absorb, counted so the run
/// reports whether the caller-side loop below actually did any work.
static RETRIES: AtomicU64 = AtomicU64::new(0);

/// Block until every named trigger has consumed its backlog.
///
/// Trigger effects land after the write that caused them has already
/// committed, so a read taken the instant a write returns sees the state
/// before the trigger ran. There is no synchronous "run the triggers now":
/// poll `list_triggers` until `pending` reaches zero. `last_error` is fatal
/// rather than transient — a module that fails holds its batch instead of
/// dropping it, so a queue that stops moving never restarts on its own.
fn drain(db: &Db, names: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let all = db.list_triggers().expect("list_triggers");
        let mine: Vec<_> = all.iter().filter(|t| names.contains(&t.name.as_str())).collect();
        assert_eq!(mine.len(), names.len(), "every named trigger is registered");
        if let Some(t) = mine.iter().find(|t| t.last_error.is_some()) {
            panic!("trigger {} is stuck: {:?}", t.name, t.last_error);
        }
        if mine.iter().all(|t| t.pending == 0) {
            return;
        }
        assert!(Instant::now() < deadline, "triggers did not drain in 30s: {mine:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One executor call, with the retry that belongs to the caller.
///
/// `execute` runs the module inside a transaction and re-runs it on a commit
/// conflict, but only `execute_retries` times (3 by default). Under real
/// contention those attempts get spent and `Conflict` arrives here, having
/// written nothing. This loop is what survives the storm; without it, a
/// busy range turns into lost work.
fn place_order(db: &Db, customer: &str, cents: u64) -> Value {
    let input = json!({"customer": customer, "amountCents": cents}).to_string();
    loop {
        match db.execute("place_order", input.as_bytes()) {
            Ok(out) => return serde_json::from_slice(&out).expect("executor output is JSON"),
            Err(Error::Conflict) => {
                RETRIES.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(e) => panic!("place_order: {e}"),
        }
    }
}

/// Every key/value in `[prefix, prefix+1)` — the prefix scan spelled out.
///
/// There is no prefix argument at this layer. The upper bound is the prefix
/// with its last byte incremented, which is why `orders/` ends at `orders0`.
fn scan_prefix(db: &Db, prefix: &str) -> Vec<(String, Vec<u8>)> {
    let mut hi = prefix.as_bytes().to_vec();
    *hi.last_mut().expect("a prefix is never empty") += 1;
    db.iter(Some(prefix.as_bytes()), Some(&hi), false)
        .expect("iter")
        .map(|kv| {
            let (k, v) = kv.expect("scan");
            (String::from_utf8(k).expect("utf-8 key"), v)
        })
        .collect()
}

fn json_at(db: &Db, key: &str) -> Option<Value> {
    db.get(key.as_bytes())
        .expect("get")
        .map(|v| serde_json::from_slice(&v).expect("stored JSON"))
}

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Options::default() is SyncMode::Always: every commit is durable before
    // it returns. Nothing here weakens it.
    let db = Arc::new(Db::open(dir.path(), Options::default()).expect("open"));

    println!("== install the modules");
    db.install_module("place_order", &guest_wasm("place_order")).expect("install executor");
    db.install_module("customer_index", &guest_wasm("customer_index")).expect("install trigger");
    let installed: Vec<_> =
        db.list_modules().expect("list_modules").into_iter().map(|m| m.name).collect();
    println!("   modules: {}", installed.join(", "));

    // A module that exports `describe` carries its own typed surface, which
    // is what lets the GraphQL plane mint a root field for it. Read it back
    // without a server in sight.
    let descriptor = db.describe_module("place_order").expect("describe").expect("has describe");
    let descriptor: Value = serde_json::from_slice(&descriptor).expect("descriptor JSON");
    println!(
        "   place_order describes itself as kind={} output={}",
        descriptor["kind"], descriptor["output"]
    );

    println!("\n== bind the trigger over [orders/, orders0)");
    // The mode is not a parameter. customer_index exports on_touch, so the
    // engine registers it in keys mode and hands it touched keys to
    // reconcile; a module exporting on_apply would get changes mode instead.
    let mode = db
        .create_trigger("customerIndex", "customer_index", Some(b"orders/"), Some(b"orders0"))
        .expect("create_trigger");
    println!("   mode detected from the module's exports: {}", mode.as_str());

    println!("\n== one order, through the executor");
    let first = place_order(&db, "acme", 1_250);
    println!("   {first}");
    assert_eq!(first["customer"], "acme");

    // The index is derived state, so it trails the write until the trigger
    // has run. This is the read that would be wrong without the drain.
    drain(&db, &["customerIndex"]);
    let index = scan_prefix(&db, "idx/customer/acme/");
    assert_eq!(index.len(), 1, "one order indexed under acme");
    println!("   index entry: {}", index[0].0);

    println!("\n== {THREADS} threads x {PER_THREAD} orders, all retrying on conflict");
    let barrier = Arc::new(Barrier::new(THREADS as usize));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                (0..PER_THREAD)
                    .map(|i| {
                        let customer = CUSTOMERS[(t + i) as usize % CUSTOMERS.len()];
                        place_order(&db, customer, 100 * (i + 1))
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let placed: Vec<Value> = handles.into_iter().flat_map(|h| h.join().expect("thread")).collect();

    // Every order got its own id even though the counter is one hot key:
    // the executor allocates it inside the transaction, so a conflicting
    // attempt is discarded whole rather than leaving a gap or a duplicate.
    let ids: BTreeSet<u64> = placed.iter().map(|o| o["id"].as_u64().expect("id")).collect();
    assert_eq!(ids.len(), placed.len(), "every order id is distinct");
    println!(
        "   {} orders placed, {} distinct ids, {} reached the caller as Conflict",
        placed.len(),
        ids.len(),
        RETRIES.load(Ordering::Relaxed)
    );

    println!("\n== the derived state agrees with the records");
    drain(&db, &["customerIndex"]);
    let orders = scan_prefix(&db, "orders/");
    let records = orders.iter().filter(|(k, _)| k != "orders/next").count();
    assert_eq!(records, placed.len() + 1, "one record per order, plus the first");

    for customer in CUSTOMERS {
        let indexed = scan_prefix(&db, &format!("idx/customer/{customer}/")).len();
        let stats = json_at(&db, &format!("customers/{customer}")).expect("customer stats");
        assert_eq!(
            stats["orders"].as_u64().expect("orders"),
            indexed as u64,
            "{customer}: the executor's own count and the trigger's index agree"
        );
        println!("   {customer}: {indexed} orders, {} cents", stats["totalCents"]);
    }

    println!("\n== rehearse a destructive change on a fork");
    // A fork is a complete database directory, published by hard-linking
    // what is already immutable. Opening it gives a writable copy whose
    // divergence costs only what actually diverges.
    let fork = db.fork("rehearsal").expect("fork");
    println!("   forked at seqno {} -> {}", fork.last_seqno, fork.path.display());

    let rehearsal = Db::open(&fork.path, Options::default()).expect("open fork");
    for (key, _) in scan_prefix(&rehearsal, "orders/") {
        if key != "orders/next" {
            rehearsal.delete(key.into_bytes()).expect("delete on the fork");
        }
    }
    let left_on_fork = scan_prefix(&rehearsal, "orders/").len();
    drop(rehearsal);

    let left_on_parent = scan_prefix(&db, "orders/").len();
    assert_eq!(left_on_fork, 1, "the fork keeps only the counter");
    assert_eq!(left_on_parent, records + 1, "the parent never saw the deletes");
    println!("   fork: {left_on_fork} keys left, parent: {left_on_parent} — untouched");

    println!("\ndone: installed, bound, drained, contended and rehearsed.");
}
