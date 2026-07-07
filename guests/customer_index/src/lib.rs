//! `customer_index` — the reference *trigger* module: maintains a secondary
//! index over `place_order`'s records, updated asynchronously by the engine
//! whenever an order key is touched.
//!
//! Register it as a write-range trigger (CLI: `mktrig customerIndex
//! customer_index orders/ orders0`, or the `createTrigger` GraphQL
//! mutation). No writer changes: plain puts, batches, transactions, and
//! other executors all keep this index current.
//!
//! Keyspace:
//!   watched:  orders/<id, 8 digits>       order record JSON (place_order)
//!   index:    idx/customer/<name>/<id>    "" — scan a customer's orders
//!   backptr:  idx/order/<id>              customer currently indexed
//!
//! The trigger contract this demonstrates:
//! - input is `trigger_keys()`: the touched keys, nothing else. An event
//!   means "reconcile this key", so the module reads CURRENT state and
//!   converges — replays and coalesced re-touches are harmless.
//! - deletes/updates need the module's own back-pointer (`idx/order/<id>`)
//!   to find the stale index entry: the event does not carry the old value.
//! - everything written here commits atomically with the event's
//!   consumption, and never fires further triggers.

fn fail(msg: &str, code: i32) -> i32 {
    fluent_guest::output(msg.as_bytes());
    code
}

fn customer_index_main() -> i32 {
    let Some(keys) = fluent_guest::trigger_keys() else {
        return fail("input is not packed trigger keys", 2);
    };
    for key in keys {
        // record keys are orders/<8 digits>; skip anything else the range
        // catches (the orders/next counter)
        let Some(id) = key.strip_prefix(b"orders/".as_ref()) else {
            continue;
        };
        if id.len() != 8 || !id.iter().all(|b| b.is_ascii_digit()) {
            continue;
        }

        let back_key = [b"idx/order/".as_ref(), id].concat();
        let old = fluent_guest::get(&back_key);
        // reconcile against the record's CURRENT state at this snapshot
        let cur: Option<Vec<u8>> = match fluent_guest::get(&key) {
            None => None,
            Some(bytes) => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                    return fail("order record is not JSON", 3);
                };
                // present-but-malformed state is corruption: fail loudly
                // (the runner reports it on the trigger) rather than
                // silently dropping the order from the index
                let Some(c) = v["customer"].as_str() else {
                    return fail("order record has no customer", 3);
                };
                if c.is_empty() || c.contains('/') {
                    return fail("order record customer is not key-safe", 3);
                }
                Some(c.as_bytes().to_vec())
            }
        };

        if old == cur {
            continue; // replay or no-op touch: the index is already right
        }
        if let Some(o) = &old {
            let stale = [b"idx/customer/".as_ref(), o, b"/", id].concat();
            if fluent_guest::delete(&stale).is_err() {
                return fail("unindex write failed", 5);
            }
        }
        match &cur {
            Some(c) => {
                let entry = [b"idx/customer/".as_ref(), c, b"/", id].concat();
                if fluent_guest::put(&entry, b"").is_err()
                    || fluent_guest::put(&back_key, c).is_err()
                {
                    return fail("index write failed", 5);
                }
            }
            None => {
                if fluent_guest::delete(&back_key).is_err() {
                    return fail("backptr delete failed", 5);
                }
            }
        }
    }
    0
}

fluent_guest::fluent_main!(customer_index_main);
