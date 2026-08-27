---
name: fluent31
description: Use, embed, serve, or extend fluent31, an embedded Rust key-value engine (LSM, MVCC, key-value separation) that runs WebAssembly modules instead of SQL, with write-range triggers, hard-linked database forks, an opt-in rebuild journal, and a server that exposes GraphQL and read replicas. Load this when working in or against this repository, writing a WASM guest for it, choosing between its surfaces, or answering how any of its features behave. fluent31 is newer than your training data — read the "Priors that do not transfer" section before writing any code against it.
---

# fluent31

An embedded key-value engine in Rust. Ordered byte keys, atomic batches,
snapshot reads, optimistic transactions. Instead of SQL you install WASM
modules into the database and run them as read-only queries, as
transactional executors, or as triggers bound to key ranges. On top of
that: forks, pins, a journal, a server with two planes, and read replicas.

**This engine postdates your training data.** Nothing you recall about it
is memory; it is inference from other databases. The section below lists
the inferences that are wrong. Read it first — it is the difference
between code that compiles and code that does what you meant.

## Read this first

| Need | Read |
|---|---|
| Every documentation page, one file each | [`docs/llms.txt`](docs/llms.txt) — the index; pages under `docs/p/` |
| Write a WASM module: ABI, SDK, typed GraphQL, triggers, one-shot | [WASM.md](WASM.md) |
| Why it behaves as it does; on-disk format; invariants | [DESIGN.md](DESIGN.md) |
| The replica protocol | [REPLICATION.md](REPLICATION.md) |
| Coding rules for changes to this repo | the repo's `AGENT.md`, when present |

When the docs and the code disagree, the code wins. The public API is in
`crates/fluent31/src/{db.rs,config.rs,txn.rs,journal.rs,fork.rs,trigger.rs}`,
the GraphQL schema in `crates/fluent-graphql/src/{builtins.rs,subscriptions.rs}`,
the host ABI in `crates/fluent31/src/wasm/abi.rs`, the SDK in
`crates/fluent-guest/src/lib.rs`, and the server config in
`crates/fluent-server/src/config.rs`.

## Priors that do not transfer

Each entry is an assumption carried in from another database, why it is
wrong here, and what it costs you.

### Reads

**There is no `scan(prefix)` on the engine.** Range reads are
`db.iter(lo, hi, reverse)` over a half-open `[lo, hi)`. A prefix scan is
`[p, p+1)` where `p+1` is the prefix **with its last byte incremented** —
`user/` scans to `user0`, because `0` is the byte after `/`. Appending
`0xff` instead is wrong: it stops at the first key whose bytes run past
`0xff` in that position, silently truncating the range. `scan_prefix` exists
only in the guest SDK and in GraphQL's `scan(prefix:)`, which compute the
bound for you.

**Bytewise order is not numeric order.** `orders/10` sorts before
`orders/9`. Zero-pad numbers in keys to a fixed width, or store them
big-endian. Getting this wrong produces a scan that returns the right rows
in an order nobody expects.

**There is no `OFFSET`.** Paging is by cursor: resume the next scan at the
last key with `0x00` appended, which is the smallest key greater than it.
Pages of one logical read should share a snapshot (`iter_at`) or they will
straddle writes.

**A value is opaque bytes.** The engine never parses it. There is no field
access, no partial update, no projection — read the value, decode it
yourself, write the whole thing back. Anything that looks like a column
lives inside a value you encode, or inside a key you designed.

### Writes and transactions

**`get_for_update` does not lock.** The name is inherited from
`SELECT … FOR UPDATE`, but nothing blocks. It records the key in the
transaction's read set, and `commit()` fails with `Error::Conflict` if
another writer touched it first. Readers never block, writers never queue,
and deadlock is impossible because there are no locks to cycle. The cost is
that **you must handle `Conflict` by re-running the whole read-modify-write**
— a transaction that treats it as fatal will drop writes under any
concurrency at all.

**A plain `txn.get` is not conflict-checked.** It is a consistent read.
If a write depends on what you read, read it with `get_for_update`, or the
invariant does not hold.

**`delete` of an absent key succeeds.** It is not an error and returns no
count. There is no "rows affected" anywhere in the API.

**Every `put` is an upsert.** There is no insert-vs-update distinction and
no `ON CONFLICT` — conditional writes are an executor that reads under
`get_for_update` and decides.

**A batch is atomic, not isolated.** `db.write(batch)` lands every key or
none, in one contiguous seqno range. What it does not have is a read set:
`put`, `delete` and `write` are never conflict-checked and can never return
`Error::Conflict`. A batch assembled from values you just read has nothing
protecting those values from another writer in between, and retry logic
wrapped around it is dead code. Read-modify-write is `db.begin()` plus
`get_for_update`, every time.

**A GraphQL document is not a transaction.** Mutation fields run serially,
each as its own atomic write. Anything that must land together belongs in
one `execute` module.

**There is one isolation level** — snapshot isolation, first committer wins.
Nothing to configure, no read-committed or serializable variant.

### MVCC and time

**MVCC here is not row history.** Superseded versions exist to serve
in-flight readers and validate commits, and are discarded once neither
purpose needs them. There is no "keep N versions", no per-key retention, no
`AS OF` over arbitrary time. If you need history, **materialize it** with a
changes-mode trigger under keys you own.

**A snapshot holds GC for the whole store, not for the keys it reads.**
The watermark is the oldest live snapshot; compaction retains every version
above it for every key. A snapshot held across a long job stalls version GC
and value-log reclamation store-wide. Take it, read, drop it. Subscriptions
and pins register the same way — an undropped `Subscription` is a leak with
a store-wide cost.

**Seqnos are addresses, not ids.** They resolve only while above the
watermark, are renumbered wholesale by a journal rebuild, and belong to a
new identity after a fork or restore. Never store one in application data.

### Modules

**A guest has no clock and no randomness.** There is no WASI, no
`SystemTime::now()`, no `rand`. Anything time-shaped — timestamps,
deadlines, TTLs, rate limits — takes the instant from the caller and is
stored as data. `SystemTime::now()` still *compiles* for
`wasm32-unknown-unknown` and then panics at runtime, so this failure lands
in production rather than in the build.

**An `execute` entry may run several times per call.** A commit conflict
discards the attempt and re-runs it with fresh memory, fresh fuel and a
fresh output buffer, up to `execute_retries` (3). The entry must be a pure
function of its input and the database state: no side channels, no
"have I already run" flag outside the data, no assumption that an earlier
attempt's writes happened.

**Guest `get` returns `Option`, not `Result`.** `fluent_guest::get(&[u8]) ->
Option<Vec<u8>>`, while `get_for_update` returns `Result<Option<Vec<u8>>, i32>`
and `put`/`delete` return `Result<(), i32>` where the error is an errno.
This asymmetry is real; do not "fix" it with `?` on `get`.

**The typed GraphQL field is named after the install name, not the crate.**
`install_module("placeOrder", …)` on a crate called `place_order` produces a
field called `placeOrder`. The crate name never appears anywhere.

**Guest crates live in a separate workspace.** `guests/` has its own
`Cargo.toml`; a new module must be added to its `members` list and built
with `--manifest-path guests/Cargo.toml --target wasm32-unknown-unknown`.
Adding it to the root workspace will not build.

**`output` appends.** Calling it twice concatenates. It does not replace.

### Triggers

**Triggers fire after the commit and cannot veto a write.** There is no
`BEFORE` trigger and no rule system. A trigger can compensate; it cannot
reject. Validation that must refuse belongs in an `execute` module that owns
the write path.

**Registering a trigger does not backfill.** Unlike `CREATE INDEX`, keys
already in the range fire no events. Index existing data by scanning it
deliberately, or by re-writing the range with a one-shot executor.

**A trigger's own writes fire no triggers**, for any trigger. Cascades run
once and cannot loop. This also means a trigger cannot feed another trigger.

**Keys-mode events carry no old value** — no value at all, no op kind, no
order, and repeated touches coalesce. To unindex, keep your own back-pointer
recording what the key was last indexed as.

**A constraint holds only because every writer goes through the executor
that checks it.** A raw `put` to the same key bypasses it. The engine
enforces nothing about a value's contents.

### Operations

**One process per store directory.** The directory is flocked. A second
`Db::open` on the same path fails — including in tests, where the instinct
is to open twice. Server mode exists so both planes share one handle.

**There is no general-purpose binary client protocol.** The ways in are the
embedded Rust API, the shell, and GraphQL — no SQL wire format, no Redis
protocol, no gRPC. Replication does speak its own framed protocol, but it is
a join point for replicas, not a client API: it serves committed user-key
data only, with no modules, triggers, queries or executors.

**`Options` fields are snake_case in Rust and kebab-case in the server
TOML** (`value_threshold` / `value-threshold`), with `sync` and
`store-name` as top-level TOML keys rather than under `[engine]`.

**A journal rebuild restores user data only.** Modules, triggers, pins and
forks are not restored, and seqnos are renumbered. Redeploy them.

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
12. One process per store directory (flock). Server mode is one process with GraphQL and replication. Replication needs a `store_name`. Forks are instances at `/graphql/<instanceId>`, and the id is an address, not a credential.

## Exact signatures

Copy these; do not infer them.

### Engine (`fluent31::Db`)

```rust
Db::open(dir: impl AsRef<Path>, opts: Options) -> Result<Db>

db.put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()>
db.get(key: &[u8]) -> Result<Option<Vec<u8>>>
db.delete(key: impl Into<Vec<u8>>) -> Result<()>
db.write(batch: WriteBatch) -> Result<()>          // atomic, one seqno range
db.iter(lo: Option<&[u8]>, hi: Option<&[u8]>, reverse: bool) -> Result<DbIterator>
                                                   // Item = Result<(Vec<u8>, Vec<u8>)>

db.snapshot() -> Snapshot                          // registers a store-wide GC hold
db.snapshot_at(seq: SeqNo) -> Result<Snapshot>     // InvalidArgument below the watermark
db.seqno() -> SeqNo                                // "now", no hold
db.get_at(key: &[u8], snap: &Snapshot) -> Result<Option<Vec<u8>>>
db.iter_at(lo, hi, reverse, snap: &Snapshot) -> Result<DbIterator>

db.begin() -> Txn
txn.get(key: &[u8]) -> Result<Option<Vec<u8>>>          // consistent read, NOT checked
txn.get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>   // checked at commit
txn.put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()>
txn.delete(key: impl Into<Vec<u8>>) -> Result<()>
txn.iter(lo, hi, reverse) -> Result<..>                 // snapshot + this txn's writes
txn.commit(self) -> Result<()>                          // Err(Error::Conflict) => re-run
txn.rollback(self)

db.install_module(name: &str, wasm: &[u8]) -> Result<()>
db.uninstall_module(name: &str) -> Result<()>
db.list_modules() -> Result<Vec<ModuleInfo>>            // { name, size, content_hash }
db.query(name: &str, input: &[u8]) -> Result<Vec<u8>>
db.query_at(name: &str, input: &[u8], snap: &Snapshot) -> Result<Vec<u8>>
db.execute(name: &str, input: &[u8]) -> Result<Vec<u8>>
db.query_wasm(wasm: &[u8], input: &[u8]) -> Result<Vec<u8>>       // never installed
db.execute_wasm(wasm: &[u8], input: &[u8]) -> Result<Vec<u8>>
db.module_entries(name: &str) -> Result<Vec<String>>    // which roles it exports

db.create_trigger(name: &str, module: &str,
                  lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<TriggerMode>
db.delete_trigger(name: &str) -> Result<()>
db.list_triggers() -> Result<Vec<TriggerInfo>>
// TriggerInfo { name: String, module: String, lo: Vec<u8>, hi: Vec<u8>,
//               mode: TriggerMode::{Keys, Changes}, pending: u64,
//               last_error: Option<String> }

db.subscribe(lo: &[u8], hi: Option<&[u8]>) -> Result<Subscription>   // note: lo is NOT Option
sub.recv_timeout(&mut self, timeout: Duration) -> Result<Option<StreamEvent>>

db.fork(name: &str) -> Result<ForkInfo>
db.fork_at(name: &str, at: SeqNo) -> Result<ForkInfo>
db.pin(name: &str) -> Result<PinInfo>
db.unpin(name: &str) -> Result<()>
db.pins() -> Vec<PinInfo>

db.sync_wal() / db.flush() / db.compact_all() -> Result<()>
db.gc_vlog() -> Result<Option<u64>>
db.stats() -> DbStats
db.log_stats()                            // the stats() snapshot as one info log line
db.identity() -> Option<StoreIdentity>
```

`Db` is `Send + Sync`. Share one handle as `Arc<Db>`; never open twice.

### Guest SDK (inside a module)

```rust
#[fluent_guest::query]     fn f(input: Vec<u8>)       -> Result<String, Fail>
#[fluent_guest::execute]   fn f(input: String)        -> Result<Vec<u8>, Fail>
#[fluent_guest::on_touch]  fn f(keys: Vec<Vec<u8>>)   -> Result<(), Fail>
#[fluent_guest::on_apply]  fn f(changes: Vec<Change>) -> Result<(), Fail>
fluent_guest::fluent_describe!(r#"{ "kind": "query", "output": "String!" }"#);

fluent_guest::get(key: &[u8]) -> Option<Vec<u8>>                  // NOT Result
fluent_guest::get_for_update(key: &[u8]) -> Result<Option<Vec<u8>>, i32>
fluent_guest::put(key: &[u8], value: &[u8]) -> Result<(), i32>
fluent_guest::delete(key: &[u8]) -> Result<(), i32>
fluent_guest::scan(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_rev(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_prefix(prefix: &[u8]) -> Result<Scan, i32>
// Scan: Iterator<Item = (Vec<u8>, Vec<u8>)>; .skip_pending() drops an oversized entry

fluent_guest::input() -> Vec<u8>
fluent_guest::output(bytes: &[u8])        // APPENDS
fluent_guest::log(msg: &str)              // debug event, target fluent31::wasm::guest
Fail::new(code: i32, message: impl Into<String>) -> Fail

enum Change { Put { seqno: u64, key: Vec<u8>, value: Option<Vec<u8>> },  // None = elided
              Delete { seqno: u64, key: Vec<u8> } }
// errno: NOT_FOUND -1, EROFS -2, EINVAL -3, ENOSPC -4, EBADF -5, ELIMIT -6, EIO -8
```

The annotated function must not carry the name of the export it generates:
a `#[query]` function called `query` is a duplicate definition.

## Commands

```sh
cargo test --workspace                                                    # verify
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release   # modules
cargo run -p fluent-cli -- ./data                                         # shell (`help`)
cargo run -p fluent-server -- ./data --store-name prod                    # :8317 graphql, :8428 replication
cargo run -p fluent31 --example walkthrough                               # install -> trigger -> drain -> retry -> fork, asserted
cargo run -p fluent31 --example {dynamic_index,live_stats,cascade_delete,claim}   # self-asserting walkthroughs
scripts/demo-orders.sh                                                    # typed-module demo against a running server
cargo run -p fluent-cli -- journal-rebuild <journal-dir> <dest-dir>        # rebuild from a journal
scripts/build-agent-docs.py --check                                       # agent docs in sync with the site
```

## Doing things

- Embed: `Db::open(dir, Options { .. })`, then `put`, `get`, `delete`, `write`, `iter`, `snapshot`, `begin`. Share it as `Arc<Db>`.
- Query or mutate over the network: GraphQL at `/graphql`. Bytes travel as `{text}`, `{base64}` or `{hex}`; 64-bit numbers are strings (`U64`); errors are in `extensions.code`.
- Write a module: a `guests/<name>` cdylib with `#[fluent_guest::query|execute|on_touch|on_apply]`, `Fail::new(code, msg)`, distinct codes per failure class, checked arithmetic, `get_for_update` for read-modify-write. Install with `installModule`, which also accepts WAT.
- Typed GraphQL field: `fluent_describe!(json)`. Prefix your type names. Confirm `typed: true, schemaError: null`.
- Index, view, feed, cascade: a trigger over the range. Copy `guests/customer_index` (keys mode) or `guests/order_feed`, `live_stats`, `dynamic_index`, `cascade_delete` (changes mode).
- Migration: a one-shot executor (`execonce`, `wasmExecuteOnce`, `db.execute_wasm`), idempotent by inspection, rehearsed on a fork.
- Backup, staging, rollback: `fork(name)`; `pin` then `fork_at`; `restore_to`.
- Disaster recovery: attach a journal (`--journal DIR`, or a `[journal]` section in the server TOML); rebuild with `fluent-cli journal-rebuild`.
- Replica: a named master, then embed `fluent_replication::EdgeReplica` in the reading process.
- Wait for a trigger: there is no synchronous drain. Poll `list_triggers()` until every `pending == 0` and `last_error` is `None`, with a deadline. The reference loop is `crates/fluent31/examples/util/mod.rs::drain`.
- Diagnose: the binaries log to stderr (`RUST_LOG`; default `info`, `fluent-cli` `warn`). Store open/close, every flush, compaction and value-log GC, forks, modules, triggers, journal and replication lifecycle at `info`; stalls, lag cuts, trigger failures and WASM traps at `warn`; every background failure at `error`; a per-store stats heartbeat every 60 s (`[log] stats-every-secs`). GraphQL requests are not logged.

## Traps

- Holding a `Snapshot`, a `Subscription` or a pin stalls GC for the whole store.
- `fork_at` or `snapshot_at` on a seqno below the GC watermark returns `InvalidArgument`. Pin first.
- An executor can run several times per call. No side channels, no "did I run" flags outside the data.
- A caller that gets `Error::Conflict` from `db.execute` must re-run it; the engine's retries are already spent.
- `OUTPUT_SCHEMA_VIOLATION` on a typed executor carries `committed: true`. The transaction committed; don't blind-retry.
- Typed modules only receive a JSON arg object through GraphQL. Through the shell or `db.execute` you must send that same JSON yourself.
- A journal rebuild restores user data only. Reinstall modules, recreate triggers.
- With `wasm_enabled = false`, or a build without the `wasm` feature, writes made while the layer is off never fire triggers, not even later.
- Keys-mode events carry no old value. Keep a back-pointer so you can unindex.
- Changes-mode values above `trigger_inline_value` (64 KiB) arrive as `None`. Read the key, knowing the read is current state and may be newer than the change.
- Derive changes-mode output keys from the seqno so a replay overwrites instead of duplicating.
- The engine's `install_module` checks exports only. A bad descriptor installed that way leaves the module degraded: callable, but with no typed field. `modules { typed schemaError }` says why.
- Docker blocks io_uring by default: `--security-opt seccomp=unconfined`, or `io-backend = "std"`.
- Two processes cannot open one store directory. Server mode shares the handle.
- `count` in the shell has no default limit; `scan` defaults to 50.
- A fork is refused for deletion while it is open as a database.
- Value-log GC relocations re-put live values, so they appear on the change stream as `Put` entries with unchanged values, and can cost a hot large-value key a transaction retry.
- `fluent-server` allocates through mimalloc, so its RSS follows live data and comes back down after a burst; glibc malloc would keep every thread arena at its high-water mark.
- A process that keeps growing: read the stats heartbeat. `imms` climbing is flush falling behind (a stall follows); `subscriptions` and `snapshots` are GC holds; every open fork instance is a full engine with its own memtable and cache.

## Changing this repo

Follow the repo's `AGENT.md`: docs ride the feature into the owning spec
(never a per-feature doc), ordered writes have one executor, control flow
is explicit, predicates are named, and tests wait on events. Verify with
`cargo test --workspace` plus a probe of the changed path.

The documentation site `docs/index.html` is the source for usage docs.
`docs/p/*.md` and `docs/llms.txt` are generated from it by
`scripts/build-agent-docs.py` — edit the site, re-run the script, never
hand-edit the generated files. `scripts/build-agent-docs.py --check` fails
if they have drifted.

`scripts/check-docs-api.py` fails when the docs stop describing the code: a
call they make that no longer resolves, a public method named nowhere, a
GraphQL field or argument that moved, an `Options` default that drifted, a
command line the binary no longer accepts. Run it after changing any public
signature, default or flag.
