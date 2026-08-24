---
name: fluent31
description: Use, embed, serve, or extend fluent31, an embedded Rust key-value engine (LSM, MVCC, key-value separation) that runs WebAssembly modules instead of SQL, with write-range triggers, hard-linked database forks, an opt-in rebuild journal, and a server that exposes GraphQL, a binary wire protocol, and read replicas. Load this when working in or against this repository, writing a WASM guest for it, choosing between its surfaces, or answering how any of its features behave.
---

# fluent31

An embedded key-value engine in Rust. Ordered byte keys, atomic batches,
snapshot reads, optimistic transactions. Instead of SQL you install WASM
modules into the database and run them as read-only queries, as
transactional executors, or as triggers bound to key ranges. On top of
that: forks, pins, a journal, a server with three planes, and read
replicas.

## Read this first

| Need | Read |
|---|---|
| Use any feature: the API, CLI, server, config, examples | [GUIDE.md](GUIDE.md), the complete usage document |
| Write a WASM module: ABI, SDK, typed GraphQL, triggers, one-shot | [WASM.md](WASM.md) |
| Why it behaves as it does; on-disk format; invariants | [DESIGN.md](DESIGN.md) |
| The binary protocol | [WIRE.md](WIRE.md) |
| The replica protocol | [REPLICATION.md](REPLICATION.md) |
| Coding rules for changes to this repo | the repo's `CLAUDE.md`, when present |

When the docs and the code disagree, the code wins. The public API is in
`crates/fluent31/src/{db.rs,config.rs,txn.rs,journal.rs,fork.rs,trigger.rs}`,
the GraphQL schema in `crates/fluent-graphql/src/{builtins.rs,subscriptions.rs}`,
the host ABI in `crates/fluent31/src/wasm/abi.rs`, the SDK in
`crates/fluent-guest/src/lib.rs`, and the server config in
`crates/fluent-server/src/config.rs`.

## The model in twelve lines

1. Keys and values are bytes. Keys sort bytewise. A prefix scan is `[p, p+1)`, so `user/` scans to `user0`.
2. Keys starting with `0x00` belong to the engine. Reads and writes there are rejected; scans skip it.
3. Every write is atomic and gets a seqno. `db.seqno()` is the address of "now".
4. Reads are snapshot-consistent. A snapshot is for one operation: take it, read, drop it. The oldest live snapshot pins GC for the whole store.
5. Seqnos are addresses, not ids. They are valid until GC passes them, renumbered by a journal rebuild, and belong to a new identity after a fork or restore. Never store them in application data.
6. Transactions are optimistic. Call `get_for_update` on every key you base a write on. `commit` returning `Error::Conflict` means run the whole thing again.
7. A module's exports are its roles: `query` (read-only), `execute` (a transaction; exit 0 commits; re-run on conflict, so it must be a pure function of input and state), `on_touch` and `on_apply` (trigger consumers), `describe` (a typed GraphQL field).
8. Triggers come in two modes. Keys mode says "reconcile this key" and coalesces. Changes mode delivers every op in order with values. Events are durable with the write, effects are exactly-once, a trigger's writes never fire triggers, and failures hold the queue (`pending`, `lastError`).
9. If you need history, materialize it with a changes-mode trigger under your own keys. MVCC is not a version store.
10. Forks are hard-linked complete database directories; opening one gives a writable copy-on-write clone. Pins make a seqno fork-able later. Both are coarse, named, and few.
11. `SyncMode::Always` means acked implies fsynced (group-committed). `Periodic` and `Never` trade a loss window, never corruption. The journal is opt-in, off the commit path, and rebuilds user data only.
12. One process per store directory (flock). Server mode is one process with GraphQL, wire and replication. Replication needs a `store_name`. Forks are instances at `/graphql/<instanceId>`, and the id is an address, not a credential.

## Commands

```sh
cargo test --workspace                                                    # verify
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release   # modules
cargo run -p fluent-cli -- ./data                                         # shell (`help`)
cargo run -p fluent-server -- ./data --store-name prod                    # :8317 graphql, :8427 wire, :8428 replication
cargo run -p fluent31 --example {dynamic_index,live_stats,cascade_delete,claim}   # self-asserting walkthroughs
scripts/demo-orders.sh                                                    # typed-module demo against a running server
cargo run -p fluent-cli -- journal-rebuild <journal-dir> <dest-dir>      # rebuild from a journal
```

## Doing things

- Embed: `Db::open(dir, Options { .. })`, then `put`, `get`, `delete`, `write`, `iter`, `snapshot`, `begin`. Share it as `Arc<Db>`. GUIDE §5.
- Query or mutate over the network: GraphQL at `/graphql`. Bytes travel as `{text}`, `{base64}` or `{hex}`; 64-bit numbers are strings (`U64`); errors are in `extensions.code`. GUIDE §12.
- Hot path: the wire protocol, pipelined, with out-of-order completion; `fluent_wire::WireClient`. GUIDE §13.
- Write a module: a `guests/<name>` cdylib with `#[fluent_guest::query|execute|on_touch|on_apply]`, `Fail::new(code, msg)`, distinct codes per failure class, checked arithmetic, `get_for_update` for read-modify-write. Install with `installModule`, which also accepts WAT. WASM.md §4 and §7.
- Typed GraphQL field: `fluent_describe!(json)`. Prefix your type names. Confirm `typed: true`. WASM.md §5.
- Index, view, feed, cascade: a trigger over the range. Copy the shape of `guests/customer_index` (keys mode) or `guests/order_feed`, `live_stats`, `dynamic_index`, `cascade_delete` (changes mode). GUIDE §7.
- Migration: a one-shot executor (`execonce`, `wasmExecuteOnce`, `db.execute_wasm`), idempotent by inspection, rehearsed on a fork. WASM.md §10.
- Backup, staging, rollback: `fork(name)`; `pin` then `fork_at`; `restore_to`. GUIDE §8.
- Disaster recovery: attach a journal (a `[journal]` section in the server TOML, or `--journal DIR` on the standalone `fluent-graphql`); rebuild with `fluent-cli journal-rebuild`. GUIDE §9.
- Replica: a named master, then `fluent-replication edge --master .. --dir .. [--lo/--hi]`. GUIDE §14.
- Tune: every `Options` field with its default is in GUIDE §5.1. The server TOML `[engine]` section mirrors it, except that `sync` and `store-name` are top-level keys.

## Traps

- Holding a `Snapshot`, a `Subscription` or a pin stalls GC for the whole store.
- `fork_at` or `snapshot_at` on a seqno below the GC watermark returns `InvalidArgument`. Pin first.
- An executor can run several times per call. No side channels, no "did I run" flags outside the data.
- `OUTPUT_SCHEMA_VIOLATION` on a typed executor carries `committed: true`. The transaction committed; don't blind-retry.
- A wire disconnect leaves in-flight requests with unknown outcome. Make `EXEC` modules idempotent.
- A journal rebuild restores user data only. Reinstall modules, recreate triggers.
- With `wasm_enabled = false`, or a build without the `wasm` feature, writes made while the layer is off never fire triggers, not even later.
- Keys-mode events carry no old value. Keep a back-pointer so you can unindex.
- Docker blocks io_uring by default: `--security-opt seccomp=unconfined`, or `io-backend = "std"`.
- Two processes cannot open one store directory. Server mode shares the handle.

## Changing this repo

Follow the repo's `CLAUDE.md`: docs ride the feature into the owning spec
(never a per-feature doc), ordered writes have one executor, control flow
is explicit, predicates are named, and tests wait on events. Verify with
`cargo test --workspace` plus a probe of the changed path.
