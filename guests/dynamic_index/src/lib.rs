//! `dynamic_index` — event-driven DYNAMIC index generation: index
//! definitions are ordinary keys, so writing a spec record creates a fully
//! backfilled secondary index at runtime, updating one keeps it live, and
//! deleting one tears it down — no reinstall, no schema, no writer
//! cooperation. The end-to-end demo lives at
//! `cargo run -p fluent31 --example dynamic_index`.
//!
//! Keyspace:
//!   data:     rec/<id>              JSON records (what gets indexed)
//!   specs:    idxspec/<name>        {"field": "<top-level JSON field>"}
//!   active:   idxcur/<name>         field this index currently materializes
//!   index:    idx/<name>/<value>/<id>   ""
//!   backptr:  idxptr/<name>/<id>    value currently indexed for the record
//!
//! ONE module backs TWO changes-mode triggers (the engine detects
//! `on_apply` and picks changes mode for both):
//!   mktrig dynIdxData dynamic_index rec/ rec0
//!   mktrig dynIdxSpec dynamic_index idxspec/ idxspec0
//!
//! What this demonstrates about the changes-mode contract:
//! - **Data changes fold, they don't reconcile.** Each committed op arrives
//!   once, in commit order, value included — so record puts index straight
//!   from the event payload (no read per key), updates unindex via the
//!   back-pointer, deletes need no old value re-read. Elided values (above
//!   `trigger_inline_value`) fall back to reading current state, which
//!   later events re-fold — convergent either way.
//! - **Spec changes reconcile.** The spec queue is separate from the data
//!   queue (no cross-trigger ordering), so the spec path compares desired
//!   state (`idxspec/`) against materialized state (`idxcur/`) and swaps
//!   generations: teardown + scan-backfill inside the SAME transaction that
//!   consumes the event — the index appears atomically, already complete.
//!   Data events racing a backfill are harmless: both paths write the same
//!   entries (idempotent puts), and the data path maintains whatever
//!   `idxcur/` says is materialized.
//!
//! Demo simplifications, deliberate: indexed values must be JSON strings or
//! numbers, key-safe (no '/', non-empty, ≤ 128 bytes) — anything else is
//! skipped, not indexed. Present-but-malformed records or specs FAIL the
//! invocation loudly (surfacing in `triggers { lastError }`, holding the
//! queue) rather than silently dropping entries. Backfill/teardown scans
//! the whole range in one transaction: fine at demo scale, chunk it for
//! real datasets.

use fluent_guest::{Change, Fail};

const REC: &[u8] = b"rec/";
const SPEC: &[u8] = b"idxspec/";

#[fluent_guest::on_apply]
fn dynamic_index(changes: Vec<Change>) -> Result<(), Fail> {
    for change in changes {
        if let Some(name) = change.key().strip_prefix(SPEC) {
            reconcile_spec(name)?;
        } else if let Some(id) = change.key().strip_prefix(REC) {
            fold_record(id, &change)?;
        }
        // anything else the ranges catch is not ours: filter in code
    }
    Ok(())
}

/// Swap an index generation: compare the spec's desired field against the
/// materialized one, tearing down and/or backfilling as needed. Reconcile
/// (not fold) because spec events live in their own queue: reading current
/// state makes replays and put/delete races converge.
fn reconcile_spec(name: &[u8]) -> Result<(), Fail> {
    let desired = match fluent_guest::get(&key(SPEC, name)) {
        None => None,
        Some(bytes) => Some(spec_field(&bytes)?),
    };
    let active = fluent_guest::get(&key(b"idxcur/", name));
    if desired == active {
        return Ok(()); // replay or no-op touch
    }
    if active.is_some() {
        delete_prefix(&index_prefix(name))?;
        delete_prefix(&key(b"idxptr/", &[name, b"/"].concat()))?;
        put_or_fail(&key(b"idxcur/", name), None)?;
    }
    if let Some(field) = &desired {
        let recs = fluent_guest::scan_prefix(REC).map_err(|_| Fail::new(4, "backfill scan failed"))?;
        for (k, v) in recs {
            let id = k[REC.len()..].to_vec();
            if let Some(value) = extract(&v, field)? {
                put_or_fail(&index_key(name, &value, &id), Some(b""))?;
                put_or_fail(&ptr_key(name, &id), Some(&value))?;
            }
        }
        put_or_fail(&key(b"idxcur/", name), Some(field))?;
    }
    Ok(())
}

/// Fold one record change into every materialized index — straight from the
/// event: inline value → extract and index, elided → read current state,
/// delete → unindex via the back-pointer. No reconciliation read needed for
/// the common (inline) case.
fn fold_record(id: &[u8], change: &Change) -> Result<(), Fail> {
    let record: Option<Vec<u8>> = match change {
        Change::Put {
            value: Some(v), ..
        } => Some(v.clone()),
        // value exceeded trigger_inline_value: read current state; a later
        // change to the same key re-folds, so this converges
        Change::Put { value: None, .. } => fluent_guest::get(change.key()),
        Change::Delete { .. } => None,
    };
    let specs = fluent_guest::scan_prefix(b"idxcur/")
        .map_err(|_| Fail::new(4, "spec scan failed"))?;
    for (k, field) in specs {
        let name = k[b"idxcur/".len()..].to_vec();
        let new = match &record {
            Some(bytes) => extract(bytes, &field)?,
            None => None,
        };
        let old = fluent_guest::get(&ptr_key(&name, id));
        if old == new {
            continue;
        }
        if let Some(old) = &old {
            put_or_fail(&index_key(&name, old, id), None)?;
        }
        match &new {
            Some(value) => {
                put_or_fail(&index_key(&name, value, id), Some(b""))?;
                put_or_fail(&ptr_key(&name, id), Some(value))?;
            }
            None => put_or_fail(&ptr_key(&name, id), None)?,
        }
    }
    Ok(())
}

/// The field a spec asks for. A present-but-malformed spec is corruption:
/// fail loudly instead of quietly indexing nothing.
fn spec_field(bytes: &[u8]) -> Result<Vec<u8>, Fail> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|s| Some(s.get("field")?.as_str()?.as_bytes().to_vec()))
        .filter(|f| !f.is_empty())
        .ok_or(Fail::new(3, "index spec is not {\"field\": \"...\"}"))
}

/// The indexable value of `field` in a JSON record: strings as-is, numbers
/// by their decimal form; missing/other types or key-unsafe values mean
/// "not indexed" (None). A record that isn't JSON at all is corruption.
fn extract(record: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, Fail> {
    let parsed = serde_json::from_slice::<serde_json::Value>(record)
        .map_err(|_| Fail::new(3, "record is not JSON"))?;
    let field = std::str::from_utf8(field).map_err(|_| Fail::new(3, "spec field not utf-8"))?;
    let value = match parsed.get(field) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        _ => return Ok(None),
    };
    let ok = !value.is_empty() && value.len() <= 128 && !value.contains('/');
    Ok(ok.then(|| value.into_bytes()))
}

fn key(prefix: &[u8], rest: &[u8]) -> Vec<u8> {
    [prefix, rest].concat()
}

fn index_prefix(name: &[u8]) -> Vec<u8> {
    [b"idx/", name, b"/"].concat()
}

fn index_key(name: &[u8], value: &[u8], id: &[u8]) -> Vec<u8> {
    [b"idx/", name, b"/", value, b"/", id].concat()
}

fn ptr_key(name: &[u8], id: &[u8]) -> Vec<u8> {
    [b"idxptr/", name, b"/", id].concat()
}

/// Buffer a put (Some) or delete (None), converting errnos to loud fails.
fn put_or_fail(k: &[u8], v: Option<&[u8]>) -> Result<(), Fail> {
    match v {
        Some(v) => fluent_guest::put(k, v).map_err(|e| Fail::new(5, format!("index write failed ({e})"))),
        None => fluent_guest::delete(k).map_err(|e| Fail::new(5, format!("index delete failed ({e})"))),
    }
}

/// Delete every key under `prefix` (teardown). One transaction — demo scale.
fn delete_prefix(prefix: &[u8]) -> Result<(), Fail> {
    let scan = fluent_guest::scan_prefix(prefix).map_err(|_| Fail::new(4, "teardown scan failed"))?;
    for (k, _) in scan {
        put_or_fail(&k, None)?;
    }
    Ok(())
}
