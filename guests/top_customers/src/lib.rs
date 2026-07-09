//! `topCustomers` — the demo *reader* query, typed via fluentabi v1
//! describe.
//!
//! Aggregates the `customers/` index written by `placeOrder` at the GraphQL
//! operation's pinned snapshot: ranks customers by lifetime spend, computes
//! a per-customer average, and applies an optional spend floor — the kind
//! of "ORDER BY ... LIMIT with a HAVING clause" a SQL layer would do,
//! running inside the database instead.
//!
//! Input: {"limit": n|null, "minTotalCents": n|null}.

use fluent_guest::Fail;
use serde_json::{json, Value};

fluent_guest::fluent_describe!(
    r#"{
  "kind": "query",
  "description": "Customers ranked by lifetime spend (from the customers/ index maintained by placeOrder), computed at this operation's snapshot.",
  "args": [
    {"name": "limit", "type": "Int", "description": "Max rows (default 10; 0 or negative returns none; capped at 100)"},
    {"name": "minTotalCents", "type": "U64", "description": "Only customers at or above this lifetime spend"}
  ],
  "types": [
    {"name": "CustomerStat", "fields": [
      {"name": "customer", "type": "String!"},
      {"name": "orders", "type": "U64!"},
      {"name": "totalCents", "type": "U64!"},
      {"name": "avgCents", "type": "U64!", "description": "totalCents / orders, rounded down"}
    ]}
  ],
  "output": "[CustomerStat!]!"
}"#
);

const PREFIX: &[u8] = b"customers/";

#[fluent_guest::main]
fn top_customers(raw: Vec<u8>) -> Result<String, Fail> {
    let input: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    let limit = input["limit"].as_i64().unwrap_or(10);
    if limit <= 0 {
        // an explicit request for zero (or negative) rows means zero rows
        return Ok("[]".into());
    }
    let limit = limit.min(100) as usize;
    let min_total = input["minTotalCents"].as_u64().unwrap_or(0);

    let scan = fluent_guest::scan_prefix(PREFIX).map_err(|_| Fail::new(2, "customers scan failed"))?;
    let mut rows: Vec<(String, u64, u64)> = Vec::new();
    for (key, value) in scan {
        let Ok(name) = String::from_utf8(key[PREFIX.len()..].to_vec()) else {
            continue;
        };
        let Ok(stats) = serde_json::from_slice::<Value>(&value) else {
            fluent_guest::log(&format!("topCustomers: corrupt stats for {name}"));
            continue;
        };
        let orders = stats["orders"].as_u64().unwrap_or(0);
        let total = stats["totalCents"].as_u64().unwrap_or(0);
        if orders > 0 && total >= min_total {
            rows.push((name, orders, total));
        }
    }
    rows.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(limit);

    let out: Vec<Value> = rows
        .into_iter()
        .map(|(customer, orders, total)| {
            json!({
                "customer": customer,
                "orders": orders,
                "totalCents": total,
                "avgCents": total / orders,
            })
        })
        .collect();
    Ok(Value::Array(out).to_string())
}
