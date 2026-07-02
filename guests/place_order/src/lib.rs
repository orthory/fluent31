//! `placeOrder` — the demo *writer* executor, typed via fluentabi v1
//! describe.
//!
//! One transaction does three coordinated writes (the point of an executor
//! over plain `put`): allocate a monotonically increasing order id from a
//! counter key, append the order record, and fold the amount into the
//! customer's running stats. Conflicting concurrent orders retry via the
//! engine's OCC loop, so ids stay unique and stats never lose an update.
//!
//! Keyspace:
//!   orders/next             decimal ASCII counter (next unassigned id)
//!   orders/<id, 8-digit>    order record JSON (zero-padded => scan-sorted)
//!   customers/<name>        {"orders": u64, "totalCents": u64}
//!
//! Input (from the typed GraphQL field): {"customer", "amountCents", "note"}.
//! Non-zero exits surface as GUEST_FAILED with the message in the output.

use serde::Deserialize;
use serde_json::json;

fluent_guest::fluent_main!(place_order_main);
fluent_guest::fluent_describe!(
    r#"{
  "kind": "execute",
  "description": "Place an order: allocates an id, records it, and folds the amount into the customer's running stats - one transaction, retried on conflict.",
  "args": [
    {"name": "customer", "type": "String!", "description": "Customer handle ([a-z0-9_-], max 64)"},
    {"name": "amountCents", "type": "U64!"},
    {"name": "note", "type": "String", "description": "Free-form note stored on the order (max 256 bytes)"}
  ],
  "types": [
    {"name": "PlacedOrder", "fields": [
      {"name": "id", "type": "U64!"},
      {"name": "customer", "type": "String!"},
      {"name": "amountCents", "type": "U64!"},
      {"name": "customerOrders", "type": "U64!", "description": "Customer's order count after this one"},
      {"name": "customerTotalCents", "type": "U64!", "description": "Customer's lifetime spend after this one"}
    ]}
  ],
  "output": "PlacedOrder!"
}"#
);

#[derive(Deserialize)]
struct Input {
    customer: String,
    #[serde(rename = "amountCents")]
    amount_cents: u64,
    note: Option<String>,
}

fn fail(msg: &str, code: i32) -> i32 {
    fluent_guest::output(msg.as_bytes());
    code
}

fn place_order_main() -> i32 {
    let Ok(input) = serde_json::from_slice::<Input>(&fluent_guest::input()) else {
        return fail("input is not valid placeOrder JSON", 2);
    };
    // customer becomes a key segment: reject empties, key injection ('/'),
    // and unbounded names
    let name_ok = !input.customer.is_empty()
        && input.customer.len() <= 64
        && input
            .customer
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if !name_ok {
        return fail("customer must be [a-z0-9_-], 1..=64 chars", 3);
    }
    if input.amount_cents == 0 {
        return fail("amountCents must be positive", 4);
    }
    if input.note.as_deref().is_some_and(|n| n.len() > 256) {
        return fail("note exceeds 256 bytes", 5);
    }

    // allocate the next order id (locked for update: OCC retries keep it
    // unique under concurrency). A present-but-unparseable counter is
    // corruption, NOT a fresh start — resetting to 1 would overwrite
    // existing orders.
    let id = match fluent_guest::get_for_update(b"orders/next") {
        Ok(None) => 1,
        Ok(Some(bytes)) => match String::from_utf8(bytes).ok().and_then(|s| s.parse::<u64>().ok())
        {
            Some(n) => n,
            None => return fail("orders/next counter is corrupt", 7),
        },
        Err(_) => return fail("counter read failed", 6),
    };
    let Some(next) = id.checked_add(1) else {
        return fail("order id space exhausted", 7);
    };
    if fluent_guest::put(b"orders/next", next.to_string().as_bytes()).is_err() {
        return fail("counter write failed", 6);
    }

    let record = json!({
        "id": id,
        "customer": input.customer,
        "amountCents": input.amount_cents,
        "note": input.note,
    });
    let order_key = format!("orders/{id:08}");
    if fluent_guest::put(order_key.as_bytes(), record.to_string().as_bytes()).is_err() {
        return fail("order write failed", 6);
    }

    // fold into the customer's running stats. Wrong-shape JSON is
    // corruption too — folding into unwrap_or(0) would silently erase the
    // customer's history.
    let stat_key = format!("customers/{}", input.customer);
    let (orders, total) = match fluent_guest::get_for_update(stat_key.as_bytes()) {
        Ok(Some(v)) => {
            let parsed = serde_json::from_slice::<serde_json::Value>(&v)
                .ok()
                .and_then(|s| Some((s.get("orders")?.as_u64()?, s.get("totalCents")?.as_u64()?)));
            match parsed {
                Some(pair) => pair,
                None => return fail("customer stats corrupt", 7),
            }
        }
        Ok(None) => (0, 0),
        Err(_) => return fail("customer stats read failed", 6),
    };
    let (Some(orders), Some(total)) = (
        orders.checked_add(1),
        total.checked_add(input.amount_cents),
    ) else {
        return fail("customer stats overflow", 7);
    };
    let stats = json!({"orders": orders, "totalCents": total});
    if fluent_guest::put(stat_key.as_bytes(), stats.to_string().as_bytes()).is_err() {
        return fail("customer stats write failed", 6);
    }

    let out = json!({
        "id": id,
        "customer": input.customer,
        "amountCents": input.amount_cents,
        "customerOrders": orders,
        "customerTotalCents": total,
    });
    fluent_guest::output(out.to_string().as_bytes());
    0
}
