<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Concepts · Human version: https://orthory.github.io/fluent31/#snapshots -->

# Snapshots and seqnos

> Every write gets a sequence number; every read happens at one. That single idea is the engine's whole notion of time.

## Seqnos

Every write — a `put`, a `WriteBatch`, a transaction commit or an executor's writes — gets a contiguous range of sequence numbers (seqnos) and becomes visible all at once. `db.seqno()` is the latest committed seqno, the address of "now". A `Snapshot` reads at one seqno; a GraphQL query operation pins one for all its fields; a module invocation runs at one.

Readers never see a partial batch and never block writers: a reader at seqno *s* sees every version at or below *s* and nothing above it, however much has been committed since.

## The watermark

A snapshot is a hold. The GC watermark is the seqno of the oldest live snapshot, and compaction keeps every version above it — for every key in the store, not only the keys that snapshot reads. Pins and subscriptions register the same way.

That is the one cost worth internalising: holding a snapshot open across a long job stalls version GC and value-log reclamation store-wide. Take one, read, drop it. The rules that follow from this are on [The consistency contract](consistency.md).

## The engine's own keyspace

Keys beginning with byte `0x00` belong to the engine: installed module bytes, trigger definitions and trigger queues live there. Reads and writes of those keys are rejected and scans clamp to the user keyspace — but because they are ordinary versioned keys underneath, your modules and triggers are written durably with the data, recovered with it, and copied into every fork of it.

## Time travel

Any seqno still above the watermark is readable: `db.snapshot_at(s)` reopens that state, `db.query_at(module, input, &snap)` runs a module against it, and `db.fork_at(name, s)` cuts a whole database there. Because module bytes are versioned too, `query_at` travels code and data together. Below the watermark those addresses are gone — which is what a [pin](concept-forks.md) exists to prevent.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Snapshots and seqnos` in *Concepts*
