<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-views-feeds -->

# Views, feeds, cascades

> The category: derived data that must be exact and ordered — live aggregate tables, audit logs and event streams, referential cleanup. All changes mode.

Where an index is a function of current state (reconcile — [keys mode](ex-indexes.md)), everything on this page needs the actual sequence of committed operations: op kinds, order, values, one event per op. That is changes mode, and the engine's delivery guarantees — durable capture, exactly-once effects, no stacking — are what make these patterns correct rather than approximately correct.

## Live aggregates

`guests/live_stats` keeps per-customer totals that are never recomputed. Every committed change adjusts the group's totals by exactly its delta:

<!-- guests/live_stats/src/lib.rs (the fold skeleton) -->
```
let new = /* what this record is NOW: the event's inline value */;
let old = fluent_guest::get(&fold_key);   // what it contributed BEFORE
if old == new { continue; }
if let Some((customer, cents)) = &old { adjust(customer, -1, -cents)?; }
if let Some((customer, cents)) = &new { adjust(customer, 1, cents)?; }
// then record the new contribution under fold_key
```

Why this is exact and not merely close: the fold commits atomically with the events' consumption, so effects are exactly-once — totals cannot drift under retries, crashes or concurrency. Updates move a record between groups; deletes subtract it; the `fold/` back-pointer is what makes both subtractable. The demo (`cargo run -p fluent31 --example live_stats`) proves it: after a concurrent write storm, the folded stats equal a full recount.

## Changefeeds and audit logs

`guests/order_feed` materializes an ordered, durable changefeed — the CDC / audit-log / event-sourcing shape. One JSON entry per committed op, written under `feed/<seqno, zero-padded>`. Deriving the feed key from the seqno is the category's core trick: a replay after a crash or conflict *overwrites* the same entries instead of duplicating them.

Because it also declares a `feed` in its descriptor, installing it gives a typed live subscription. The idiom that falls out is the whole point:

```
history:  { scan(prefix: {text: "feed/"}) { ... } }        # replayable, replicable
live:     subscription { orderFeed { event { seqno op id record } } }
```

A disconnected consumer misses nothing durable — history is a scan, live is the tail of the same range. This is also how you build [per-record history](consistency.md): the same shape, keyed `history/<id>/<seqno>`.

## Cascades

`guests/cascade_delete` is referential cleanup: when a parent `doc/<id>` is deleted, scan and delete its `doc/<id>/…` subtree. Two contract details carry the pattern:

- **Op kinds matter.** Only `Delete` events of parent keys act; puts and descendant traffic are filtered out in code. Keys mode would have to read every touched key just to ask "was this a delete?".
- **No stacking, by construction.** The sweep deletes keys inside its own watched range, yet trigger writes never generate events — cascades cannot loop or amplify. One event, one sweep, done.

## Run them

```
cargo run -p fluent31 --example live_stats       # folded stats == full recount, asserted
cargo run -p fluent31 --example cascade_delete
mktrig orderFeed order_feed orders/ orders0      # mode auto-detected from on_apply
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Views, feeds, cascades` in *Recipes*
