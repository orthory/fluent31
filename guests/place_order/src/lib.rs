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
//! Failures surface as GUEST_FAILED with the `Fail` message in the output
//! and its code as the exit code (distinct per failure class).

use fluent_guest::Fail;
use serde::Deserialize;
use serde_json::json;

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

#[fluent_guest::main]
fn place_order(raw: Vec<u8>) -> Result<String, Fail> {
    let input = serde_json::from_slice::<Input>(&raw)
        .map_err(|_| Fail::new(2, "input is not valid placeOrder JSON"))?;
    // customer becomes a key segment: reject empties, key injection ('/'),
    // and unbounded names
    let name_ok = !input.customer.is_empty()
        && input.customer.len() <= 64
        && input
            .customer
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if !name_ok {
        return Err(Fail::new(3, "customer must be [a-z0-9_-], 1..=64 chars"));
    }
    if input.amount_cents == 0 {
        return Err(Fail::new(4, "amountCents must be positive"));
    }
    if input.note.as_deref().is_some_and(|n| n.len() > 256) {
        return Err(Fail::new(5, "note exceeds 256 bytes"));
    }

    // allocate the next order id (locked for update: OCC retries keep it
    // unique under concurrency). A present-but-unparseable counter is
    // corruption, NOT a fresh start — resetting to 1 would overwrite
    // existing orders.
    let id = match fluent_guest::get_for_update(b"orders/next") {
        Ok(None) => 1,
        Ok(Some(bytes)) => String::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(Fail::new(7, "orders/next counter is corrupt"))?,
        Err(_) => return Err(Fail::new(6, "counter read failed")),
    };
    let next = id
        .checked_add(1)
        .ok_or(Fail::new(7, "order id space exhausted"))?;
    fluent_guest::put(b"orders/next", next.to_string().as_bytes())
        .map_err(|_| Fail::new(6, "counter write failed"))?;

    let record = json!({
        "id": id,
        "customer": input.customer,
        "amountCents": input.amount_cents,
        "note": input.note,
    });
    let order_key = format!("orders/{id:08}");
    fluent_guest::put(order_key.as_bytes(), record.to_string().as_bytes())
        .map_err(|_| Fail::new(6, "order write failed"))?;

    // fold into the customer's running stats. Wrong-shape JSON is
    // corruption too — folding into unwrap_or(0) would silently erase the
    // customer's history.
    let stat_key = format!("customers/{}", input.customer);
    let (orders, total) = match fluent_guest::get_for_update(stat_key.as_bytes()) {
        Ok(Some(v)) => serde_json::from_slice::<serde_json::Value>(&v)
            .ok()
            .and_then(|s| Some((s.get("orders")?.as_u64()?, s.get("totalCents")?.as_u64()?)))
            .ok_or(Fail::new(7, "customer stats corrupt"))?,
        Ok(None) => (0, 0),
        Err(_) => return Err(Fail::new(6, "customer stats read failed")),
    };
    let (Some(orders), Some(total)) = (
        orders.checked_add(1),
        total.checked_add(input.amount_cents),
    ) else {
        return Err(Fail::new(7, "customer stats overflow"));
    };
    let stats = json!({"orders": orders, "totalCents": total});
    fluent_guest::put(stat_key.as_bytes(), stats.to_string().as_bytes())
        .map_err(|_| Fail::new(6, "customer stats write failed"))?;

    let out = json!({
        "id": id,
        "customer": input.customer,
        "amountCents": input.amount_cents,
        "customerOrders": orders,
        "customerTotalCents": total,
    });
    Ok(out.to_string())
}
