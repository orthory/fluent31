<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Concepts · Human version: https://orthory.github.io/fluent31/#consistency -->

# The consistency contract

> MVCC is how the engine gives you consistent reads and optimistic transactions. It is not an application-level version store, and the rules below follow from that.

## The rules

- **Snapshots are operation-scoped.** Take one, read, drop it. Its cost is store-wide: the GC watermark is the seqno of the oldest live snapshot, and nothing at or above it is reclaimed, for any key, not just the ones the snapshot reads.
- **Seqnos are addresses, not ids.** `db.seqno()` names the current state and stays resolvable only until GC passes it. Don't store seqnos in application data. A journal rebuild renumbers them wholesale, and a fork or restore mints a new store identity.
- **Pins and forks are coarse, named cuts.** Use them for a handful of deliberate points: before a migration, a staging clone, a rollback anchor. A pin is a durable store-wide GC hold; a fork is a whole database directory. Neither is priced per document, let alone per write.
- **There is no retention policy.** Old versions survive until the GC watermark passes them. There is no "keep N versions of this key".

## If you need history, make it data

Bind a changes-mode [trigger](triggers.md) to the range. Every committed change (kind, key, seqno, and the value up to `trigger_inline_value`; larger values arrive key-only and you read them back) is delivered durably, in order, with exactly-once effect. Write it under keys you own:

```
doc/42                     current value       (what the app writes)
history/doc/42/<seqno>     one entry per write (what the trigger writes)
```

History is then scannable, replicable and live-tailable as a subscription, and you prune it yourself with a scan and a delete batch, on whatever schedule suits the data. Modules have no clock, so if entries need timestamps the writer puts them in the value. `guests/order_feed` is the reference shape.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `The consistency contract` in *Concepts*
