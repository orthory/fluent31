<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Start here · Human version: https://orthory.github.io/fluent31/#introduction -->

# Introduction

> fluent31 is an embedded key-value database engine in Rust whose query surface is WebAssembly. You install code into the database, and the engine runs it as reads, as transactions, and as triggers — against an LSM store built to move as few bytes as possible.

## Your code runs inside the database

This is the feature everything else is arranged around. A module is a WASM binary stored in the database like any other value, invoked by name, executing next to the data against a kernel-style syscall ABI: `get`, `get_for_update`, `put`, `delete`, batched scans, fuel and memory limits. Its exports decide what it can be — and one binary can be several at once:

- **A query** runs read-only at one pinned snapshot. An aggregate over a million keys returns its five numbers instead of a million values, and the whole answer comes from one consistent state.
- **An executor** runs in a fresh optimistic transaction: exit 0 commits, anything else aborts, and a commit conflict re-runs the attempt against a fresh snapshot. Invariants that need read-then-write — uniqueness, non-negative balances, dense id allocation — hold under concurrency without a lock anywhere.
- **A trigger consumer** is invoked by the engine after every committed write into a key range you bind it to. Indexes, materialized views, live aggregates, changefeeds and cascades maintain themselves, whoever did the writing, with events captured durably alongside the write and effects applied exactly once.
- **A descriptor** turns the module into API. Export `describe` and installing it adds a typed, documented GraphQL field to the running server's schema.

Because module bytes are stored as ordinary versioned keys, your code is durable with the data, recovered with it, copied into every fork of it, and time-travelled alongside it — `query_at` runs the code as it was at a past sequence number against the data as it was then. And because the same bytes can run without being installed, a migration is a one-shot executor: one atomic transaction, no deployment, no trace but its writes.

Modules are sandboxed: fuel-metered, memory-capped, no WASI, no clock, no randomness, no imports but the host's own. What that buys is bounded, deterministic execution — the engine stays predictable no matter what the module does. [Extending with WASM](wasm-overview.md) is the section on all of it: the roles, the two API surfaces, and what to build with them.

## Built to move as few bytes as possible

The storage engine is an LSM tree with WiscKey-style key-value separation: values at or above `value_threshold` live in an append-only log and the tree holds pointers. Compaction therefore relocates pointers rather than payloads, so write amplification is governed by key volume, not value volume, and the index stays small enough to keep resident — every fragment's bloom filter and index in memory, for the whole dataset.

- **Group commit.** Under the default `SyncMode::Always`, concurrent writers share one value-log fsync and one WAL fsync per cycle. An ack still means fsynced; the cost is amortized across everyone committing at that moment, and `stats` reports how many fsyncs were saved.
- **io_uring on Linux**, probed at open with an automatic fallback to portable IO elsewhere.
- **Batched value resolution.** A scan over large values resolves value-log pointers in windows — one batched read per file — so a range read costs one IO round per group rather than one per entry.
- **Lazy leveling.** Tiered merges on the upper levels, one leveled run at the bottom: fewer rewrites for write-heavy ranges, a compact bottom for reads.
- **No round trips.** The fastest query is the one that never crosses a network boundary. Modules put the loop next to the data; triggers move the work off the read path entirely, so a report can be a single `get` of a number a trigger already folded.

## A typed API you did not write

Run the server and the store gets a GraphQL plane: `get`, `scan`, `put`, `writeBatch`, module invocation, trigger and fork administration, engine stats — and GraphiQL to explore it. The part worth noticing is that the schema is not fixed. Every installed module that describes itself contributes its own root field with real argument and output types, and the schema is rebuilt and hot-swapped on install and uninstall, so shipping a module ships an API.

```
mutation { placeOrder(customer: "you", amountCents: "4200") { id customerTotalCents } }
query    { topCustomers(limit: 3) { customer orders totalCents avgCents } }
subscription { orderFeed { seqno event { id record } query { snapshotSeqno } } }
```

Subscriptions stream committed changes, raw or typed, and every item carries the whole query root pinned at the exact state in which that change became visible — so a consumer can read consistent context for an event without racing the writer. Forks are addressable too: each one is a full instance at `/graphql/<instanceId>` with its own modules, triggers and schema.

## The pieces

- **Engine** — LSM storage with key-value separation, MVCC snapshots, optimistic transactions, io_uring on Linux.
- **Modules** — WASM installed in the database, run as queries, executors or trigger consumers.
- **Triggers** — a module bound to a key range, invoked after every committed write into it. Events are durable with the write and effects are exactly-once.
- **Forks** — `fork("name")` publishes a complete, consistent, hard-linked copy of the database at a cost proportional to the file count, not the data. Open it for a writable copy-on-write clone; pins make a point fork-able later.
- **Journal** — opt-in and off the commit path. An independent mutation log from which a fresh database is rebuilt when the store directory is lost.
- **Server** — one process, one store, two planes: GraphQL for typed and admin operations with live subscriptions, and a replication join point for full replicas and key-range edge caches.

## The surfaces

| Surface | What it is |
|---|---|
| `fluent31` crate | The engine. Embed it in a Rust process. |
| `fluent-guest` crate | The SDK for writing WASM modules. |
| `fluent-cli` | An interactive shell. Also the journal rebuild tool. |
| `fluent-server` | One process serving one store on two planes: GraphQL (typed and admin operations, subscriptions) and replication (the join point for replicas). |
| `fluent-graphql`, `fluent-replication` | Each plane as a standalone binary, with the same defaults. |

## What it is not

- **Not a query language.** There is no parser, no columns, no joins — key layout and module code take their place. [Translation guide](sql-mapping.md) covers the relational vocabulary if you want the comparison.
- **Not point-in-time recovery.** Forks are named cuts, not continuous log archiving.
- **Not a public-facing sandbox.** The WASM limits protect reliability and integrity. Authentication and authorization are a layer you put in front.
- **Single-node today.** A store is one directory, locked by one process. Replicas are read-only followers.

## The documents

This site is the usage documentation and its source. The same pages are generated as one markdown file each, under `docs/p/`, indexed by [llms.txt](https://orthory.github.io/fluent31/llms.txt) — that is the copy to point an agent at, because a fetcher cannot address a page here. The specs below go beneath what this site describes; when a detail matters they win, and when the docs and the code disagree, the code wins.

| Document | Covers |
|---|---|
| [llms.txt](https://orthory.github.io/fluent31/llms.txt) | Every page here as one fetchable markdown file, in reading order. The index an agent should start from. |
| [SKILL.md](https://github.com/orthory/fluent31/blob/master/SKILL.md) | The primer for agents: the model in twelve lines, exact signatures, the traps, and the assumptions carried in from other databases that are wrong here. |
| [WASM.md](https://github.com/orthory/fluent31/blob/master/WASM.md) | The module authoring manual and ABI spec. |
| [DESIGN.md](https://github.com/orthory/fluent31/blob/master/DESIGN.md) | The architecture as implemented, section by section. |
| [REPLICATION.md](https://github.com/orthory/fluent31/blob/master/REPLICATION.md) | The replica protocol spec. |

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Introduction` in *Start here*
