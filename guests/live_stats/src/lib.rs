//! `live_stats` — always-fresh per-group aggregates, folded incrementally
//! from the change feed: the `SELECT count(*), sum(cents) GROUP BY
//! customer` that never runs a query. Every committed order change adjusts
//! the group's running totals by exactly its delta — updates move a record
//! between groups, deletes subtract it — and the trigger contract makes the
//! arithmetic EXACT: effects are exactly-once (the fold commits atomically
//! with the events' consumption), so totals cannot drift under retries,
//! crashes, or concurrency. Demo: `cargo run -p fluent31 --example
//! live_stats` (it proves stats == a full recount after a write storm).
//!
//! Keyspace:
//!   watched: ord/<id>          {"customer": "...", "cents": n}
//!   stats:   stat/<customer>   {"orders": k, "cents": total}
//!   folded:  fold/<id>         the (customer, cents) this record last
//!                              contributed — the back-pointer that makes
//!                              updates and deletes subtractable
//!
//! Why changes mode: the delta needs "what is this record NOW" (the event
//! value, inline — no read) and "what did it contribute BEFORE" (the fold
//! key). Keys mode would coalesce an update+delete burst into one
//! reconcile; here every op folds once, in order.

use fluent_guest::{Change, Fail};
use serde_json::json;

const ORD: &[u8] = b"ord/";

#[fluent_guest::on_apply]
fn live_stats(changes: Vec<Change>) -> Result<(), Fail> {
    for change in changes {
        let Some(id) = change.key().strip_prefix(ORD) else {
            continue; // not an order: filter in code
        };
        let new = match &change {
            Change::Put { value: Some(v), .. } => Some(parse_order(v)?),
            // elided value: read current state; later changes re-fold
            Change::Put { value: None, .. } => match fluent_guest::get(change.key()) {
                Some(v) => Some(parse_order(&v)?),
                None => None,
            },
            Change::Delete { .. } => None,
        };
        let fold_key = [b"fold/".as_ref(), id].concat();
        let old = match fluent_guest::get(&fold_key) {
            Some(v) => Some(parse_order(&v)?),
            None => None,
        };
        if old == new {
            continue;
        }
        if let Some((customer, cents)) = &old {
            adjust(customer, -1, -(*cents as i128))?;
        }
        match &new {
            Some((customer, cents)) => {
                adjust(customer, 1, *cents as i128)?;
                let rec = json!({"customer": customer, "cents": cents}).to_string();
                fluent_guest::put(&fold_key, rec.as_bytes())
                    .map_err(|_| Fail::new(5, "fold write failed"))?;
            }
            None => {
                fluent_guest::delete(&fold_key)
                    .map_err(|_| Fail::new(5, "fold delete failed"))?;
            }
        }
    }
    Ok(())
}

/// Apply a (count, cents) delta to a customer's stat record; a group whose
/// count reaches zero is removed rather than left as a zero row.
fn adjust(customer: &str, dorders: i64, dcents: i128) -> Result<(), Fail> {
    let key = format!("stat/{customer}");
    let (orders, cents) = match fluent_guest::get(key.as_bytes()) {
        None => (0i64, 0i128),
        Some(v) => {
            let s = serde_json::from_slice::<serde_json::Value>(&v)
                .map_err(|_| Fail::new(3, "stat record corrupt"))?;
            let orders = s["orders"].as_i64().ok_or(Fail::new(3, "stat record corrupt"))?;
            let cents = s["cents"]
                .as_i64()
                .ok_or(Fail::new(3, "stat record corrupt"))? as i128;
            (orders, cents)
        }
    };
    let (orders, cents) = (orders + dorders, cents + dcents);
    if orders < 0 || cents < 0 {
        // exactly-once folding makes this unreachable; if it fires, state
        // was corrupted out-of-band — fail loudly, never clamp
        return Err(Fail::new(3, "stat underflow: fold state corrupt"));
    }
    if orders == 0 {
        fluent_guest::delete(key.as_bytes()).map_err(|_| Fail::new(5, "stat delete failed"))
    } else {
        let rec = json!({"orders": orders, "cents": cents as i64}).to_string();
        fluent_guest::put(key.as_bytes(), rec.as_bytes())
            .map_err(|_| Fail::new(5, "stat write failed"))
    }
}

/// (customer, cents) of an order record; malformed records are corruption.
fn parse_order(bytes: &[u8]) -> Result<(String, u64), Fail> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| {
            let customer = v["customer"].as_str()?.to_string();
            let cents = v["cents"].as_u64()?;
            (!customer.is_empty() && !customer.contains('/')).then_some((customer, cents))
        })
        .ok_or(Fail::new(3, "order record is not {customer, cents} JSON"))
}
