<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-uses -->

# What to build with it

> The shapes a module takes, grouped by the role that carries them — and an honest account of what a module cannot do.

## Reads that belong next to the data

Anything whose inputs are much larger than its answer. The range never leaves the process, and the whole computation happens at one snapshot.

| Shape | What the module earns |
|---|---|
| aggregates over a range — count, sum, min, max, averages | a million keys in, a handful of bytes out |
| ranked top-N with a floor or a filter | ranking needs the whole range but returns a page of it |
| projections — three fields of a large record | values are opaque to the engine, so only a module can narrow them |
| lookups that combine two ranges | both sides are read at one snapshot, so the result cannot tear |
| graph and adjacency walks | each hop is a `get` at the same snapshot; a client pays a round trip per hop |
| index-backed search — tags, terms, a prefix of a secondary key | scan the index range and resolve the hits in one invocation |
| existence, integrity and drift checks over a range | a full scan in, a verdict out |
| rendered summaries and server-computed page cursors | the computation is the point; shipping its inputs is the waste |

## Writes that have to hold an invariant

Anything where reading and writing must be one indivisible step, or where several keys have to move together.

| Shape | What makes it need an executor |
|---|---|
| claiming a name — usernames, slugs, seat reservations | read-then-write; `get_for_update` makes exactly one concurrent claimant win |
| transfers and ledger entries | two balances must move together or not at all, and neither may go negative |
| dense id allocation | a counter read under `get_for_update`; retries keep the ids gap-free |
| conditional writes — only if absent, only if unchanged | the condition is evaluated inside the transaction that commits it |
| state machine transitions | the legal-transition check and the write are one atomic step |
| validation gateways | the executor becomes the write path for a range, so the constraint holds for every caller that uses it |
| denormalization on write — a record plus its stats plus its index entry | coordinated multi-key writes, committed together or not at all |
| idempotent submits | the marker and the effect share one commit, which is what makes a retry provably safe |
| outbox writes for downstream delivery | the record and its outbox entry cannot come apart |
| bulk edits over a range | one transaction: invisible until it commits, serialized by conflict detection |

## Derived data that maintains itself

Bound to a key range, a module stops being something callers invoke and becomes something the engine invokes for them. The mode picks itself from what the derived state is a function of.

| Shape | Mode | Why that mode |
|---|---|---|
| secondary indexes | `on_touch` | the index is a function of current state, so coalescing is free correctness |
| reverse lookups and mirrors under another key layout | `on_touch` | the same reconcile shape, different output keys |
| invariant checkers and repair sweeps | `on_touch` | re-reading current state is exactly what a checker wants |
| changefeeds, audit trails, event logs | `on_apply` | every op matters, in order, with its value; coalescing would lose entries |
| live per-group aggregates | `on_apply` | each change contributes its delta exactly once, so totals cannot drift |
| per-record history | `on_apply` | one entry per write, keyed by sequence number |
| cascading deletes and reference cleanup | `on_apply` | the op kind is the condition, and trigger writes never re-fire |
| indexes defined at runtime by writing a spec key | `on_apply` | one module, two ranges: specs backfill and tear down, data folds |
| fan-out projections — one write, several read-shaped copies | `on_apply` | the projection trails the write without the writer knowing it exists |
| expiry sweeps | `on_apply` | workable, but the deadline has to be a value the writer stored — see the limits below |

## Work that runs once and is never installed

The same executor contract, invoked on bytes that are stored nowhere: format migrations, backfills after a shape change, data repair, bulk re-encoding, and one-off reports. Nothing is listed, cached or replicated, and the committed writes are the only trace — so the script in your repository is the audit trail. Triggers still fire on those writes, which is what keeps indexes and feeds correct straight through a migration. [Migrations & one-shots](ex-oneshot.md) works the shape end to end.

## What a module cannot do

- **No clock.** There is no time source at all. Anything time-shaped — timestamps on records, deadlines, rate limits, scheduled expiry — takes the instant from the caller and stores it as data.
- **No randomness.** Ids, tokens and nonces are either derived from data the module can read, or passed in.
- **No network, files or environment.** There is no WASI: the only imports are the host's own database calls. A module cannot call out, and nothing can call in except the engine.
- **No state between invocations.** Linear memory is fresh every time, and an executor's memory is fresh on every retry. The database is the only memory a module has.
- **No vetoing a write.** Triggers run after the commit, so a trigger can compensate but cannot reject. Validation that must refuse belongs in an executor that owns the write path.
- **No reaching outside its store.** A module sees the store it runs in, and inside it only the user keyspace — keys starting with `0x00` are the engine's own.
- **No unbounded work.** Fuel, memory, input, output, log volume and open scan handles are all capped per invocation, and a transaction's write set is capped too.

Within that, it is ordinary Rust. Any crate that compiles for `wasm32-unknown-unknown` without operating-system access works, which covers serialization, parsing, compression and arithmetic; the reference modules use `serde_json`. The limits exist to protect the engine's reliability and integrity — authentication and authorization are a layer you put in front.

## The modules in the repository

Ten working modules under `guests/`, one per shape, each with a demo script that exercises it.

| Module | Role | Shows |
|---|---|---|
| `agg` | query | prefix count/sum/min/max over u64 LE values; raw bytes in and out |
| `top_customers` | query, typed | typed list output, `scan_prefix` aggregation at a snapshot, limit clamping |
| `transfer` | execute | a balance transfer with `get_for_update`, conflict retries, an exit code per failure |
| `claim` | execute | a uniqueness invariant: exactly one winner under concurrency, idempotent re-claim, attributable failures |
| `place_order` | execute, typed | id allocation, a record and a stats fold in one transaction; input validation, corruption checks that fail loudly |
| `customer_index` | `on_touch` | a secondary index reconciled against current state, with the back-pointer pattern for updates and deletes |
| `order_feed` | `on_apply` + feed | an ordered changefeed materialized as keys and subscribable live, with an `elided` flag for oversized values |
| `live_stats` | `on_apply` | an always-fresh per-group aggregate folded exactly once per change; the demo checks it against a full recount |
| `dynamic_index` | `on_apply` | index specs stored as keys, scan-backfill on spec write, teardown on delete; one module, two triggers |
| `cascade_delete` | `on_apply` | a parent delete sweeping its subtree; the no-stacking rule doing the loop prevention |

[Choosing the shape](choosing.md) works the same ground from the other direction — starting at the kind of database work rather than the role — and the recipes from [Queries in the database](ex-queries.md) onward carry the code.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `What to build with it` in *Extending with WASM*
