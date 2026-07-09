//! `order_feed` — the reference *changes-mode trigger* module: materializes
//! an ordered, durable changefeed of the `orders/` range. Where
//! `customer_index` (keys mode) reconciles coalesced "key was touched"
//! events against current state, this module consumes the actual list of
//! committed changes — one event per op, in commit order, values included —
//! which is exactly what an audit log or event-sourced projection needs and
//! what coalescing would destroy.
//!
//! Register: `mktrig orderFeed order_feed orders/ orders0` (the engine
//! detects `on_apply` and picks changes mode automatically).
//!
//! Keyspace:
//!   watched:  orders/<id, 8 digits>    order record JSON (place_order)
//!   feed:     feed/<seqno, 20 digits>  one JSON line per committed change
//!
//! The changes-mode contract this demonstrates:
//! - input is the ordered change list: seqno (the op's commit seqno —
//!   unique, strictly increasing), kind (put/delete), key, and the written
//!   value (inline up to the engine's `trigger_inline_value`, elided above
//!   it).
//! - the module is the post-apply FILTER: the trigger range does the
//!   coarse cut, the code drops what it doesn't care about (here: the
//!   orders/next counter that shares the range).
//! - feed keys are derived from the seqno, so replays after a conflict or
//!   crash overwrite the same entries instead of duplicating them, and the
//!   feed's own writes never re-fire triggers (no stacking).

use fluent_guest::{Change, Fail};
use serde_json::json;

#[fluent_guest::on_apply]
fn order_feed(changes: Vec<Change>) -> Result<(), Fail> {
    for change in changes {
        // the post-apply filter: only real order records enter the feed
        let Some(id) = change.key().strip_prefix(b"orders/".as_ref()) else {
            continue;
        };
        if id.len() != 8 || !id.iter().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let id = String::from_utf8(id.to_vec()).expect("ascii digits");

        let entry = match &change {
            Change::Put {
                seqno,
                value: Some(bytes),
                ..
            } => {
                // present-but-malformed records are corruption: fail loudly
                // (surfaces in `triggers { lastError }`) instead of feeding
                // garbage downstream
                let record = serde_json::from_slice::<serde_json::Value>(bytes)
                    .map_err(|_| Fail::new(3, "order record is not JSON"))?;
                json!({"seqno": seqno, "op": "put", "id": id, "record": record})
            }
            Change::Put {
                seqno, value: None, ..
            } => {
                // value exceeded trigger_inline_value: record the fact; a
                // consumer that needs the payload reads the key (current
                // state, possibly newer than this change)
                json!({"seqno": seqno, "op": "put", "id": id, "record": null, "elided": true})
            }
            Change::Delete { seqno, .. } => {
                json!({"seqno": seqno, "op": "delete", "id": id})
            }
        };
        let feed_key = format!("feed/{:020}", change.seqno());
        fluent_guest::put(feed_key.as_bytes(), entry.to_string().as_bytes())
            .map_err(|_| Fail::new(5, "feed write failed"))?;
    }
    Ok(())
}
