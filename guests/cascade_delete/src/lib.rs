//! `cascade_delete` — referential cleanup from the change feed: the
//! `ON DELETE CASCADE` replacement. When a parent record is deleted, every
//! key under its subtree goes with it, atomically with the event's
//! consumption. Demo: `cargo run -p fluent31 --example cascade_delete`.
//!
//! Keyspace (parent and descendants share a prefix):
//!   parent:      doc/<id>            the record
//!   descendants: doc/<id>/<...>      attachments, comments, anything
//!
//! Register over the whole family: `mktrig cascade cascade_delete doc/ doc0`.
//!
//! What this demonstrates about changes-mode triggers:
//! - **Op kinds matter**: only Delete events of PARENT keys (no '/' past
//!   the id) act; puts and descendant traffic are filtered out in code —
//!   no reads wasted asking "was this a delete?" as keys mode would need.
//! - **No stacking, by construction**: the sweep deletes descendants that
//!   are themselves inside the watched range, yet those deletes generate
//!   no further events (trigger writes never do) — cascades cannot loop
//!   or amplify. One event, one sweep, done.
//! - **Convergent replay**: re-delivery after a crash re-scans an already
//!   empty subtree and deletes nothing — idempotent by shape.

use fluent_guest::{Change, Fail};

const DOC: &[u8] = b"doc/";

#[fluent_guest::on_apply]
fn cascade_delete(changes: Vec<Change>) -> Result<(), Fail> {
    for change in changes {
        let Change::Delete { key, .. } = &change else {
            continue; // only deletes cascade
        };
        let Some(id) = key.strip_prefix(DOC) else {
            continue;
        };
        if id.is_empty() || id.contains(&b'/') {
            continue; // a descendant was deleted, not a parent: no cascade
        }
        let subtree = [DOC, id, b"/"].concat();
        let scan = fluent_guest::scan_prefix(&subtree)
            .map_err(|_| Fail::new(4, "cascade scan failed"))?;
        for (k, _) in scan {
            fluent_guest::delete(&k).map_err(|_| Fail::new(5, "cascade delete failed"))?;
        }
    }
    Ok(())
}
