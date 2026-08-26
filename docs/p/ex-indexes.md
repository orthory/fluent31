<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-indexes -->

# Secondary indexes

> The category: finding records by something other than their key — a trigger plus a key convention.

An index in fluent31 is not a special structure — it is ordinary keys under their own prefix, maintained by a trigger so it stays current no matter who writes:

```
orders/00000042                   the record            (what the app writes)
idx/customer/acme/00000042  ""    the index entry       (what the trigger writes)
idx/order/00000042          acme  the back-pointer      (how updates find the stale entry)
```

The lookup is a prefix scan of `idx/customer/acme/`. No writer cooperates: plain puts, batches, transactions and other executors all keep the index current, because the trigger fires on every commit into the range.

## The keys-mode shape

`guests/customer_index` is the reference. Keys mode fits indexing exactly because an index is a function of *current* state — the event only says "this key was touched", and the module reconciles:

<!-- guests/customer_index/src/lib.rs (the reconcile skeleton) -->
```
let old = fluent_guest::get(&back_key);          // what the index says now
let cur = /* read the record; its customer, or None if deleted */;
if old == cur { continue; }                      // replay or no-op touch
if let Some(o) = &old {
    fluent_guest::delete(&stale_entry(o))?;      // unindex via the back-pointer
}
match &cur {
    Some(c) => {
        fluent_guest::put(&entry(c), b"")?;      // index current state
        fluent_guest::put(&back_key, c)?;
    }
    None => fluent_guest::delete(&back_key)?,   // gone: drop the back-pointer too
}
```

The two category rules this encodes:

- **Reconcile, don't apply.** Read current state and make the index match it. Written this way the module converges under replay, coalescing and reordering — all of which keys mode will do to you.
- **Keep a back-pointer, and delete it with the record.** The event carries no old value, so updates and deletes need the module's own record of what it indexed last time. Dropping the back-pointer on delete is not optional: if it lingered, a delete followed by a re-create would compare equal (`old == cur`) and the record would never be re-indexed.

## Indexes created at runtime

`guests/dynamic_index` pushes the category further: index *definitions* are themselves ordinary keys. Writing `idxspec/<name> = {"field": "city"}` creates a fully backfilled index over that field; updating the spec swaps generations; deleting it tears the index down. One module backs two changes-mode triggers — one on the data range, one on the spec range — and the backfill runs inside the same transaction that consumes the spec event, so the index appears atomically, already complete.

It is also the answer to a sharp edge: **trigger registration does not backfill.** Keys already in the range fire no events. Either scan on demand as `dynamic_index` does, or re-put the range with a [one-shot executor](ex-oneshot.md).

## Run it

```
mktrig customerIndex customer_index orders/ orders0      # shell
cargo run -p fluent31 --example dynamic_index            # spec write → backfilled index, asserted
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Secondary indexes` in *Recipes*
