# fluent31

An embedded key-value database engine in Rust:

- **LSM storage, tuned for writes *and* lookups** — lazy leveling (tiered
  merges everywhere, leveled bottom), bloom-guarded fragmented runs,
  WiscKey-style **key-value separation** so the index stays small and
  memory-friendly while big values live in an append-only value log.
- **MVCC everywhere** — consistent snapshots, optimistic transactions with
  first-committer-wins conflicts and `get_for_update` write-skew defense.
- **io_uring** on Linux (batched reads for scans/value resolution), portable
  positioned IO elsewhere. Develops and tests fine on macOS.
- **No SQL — WASM.** Install WebAssembly modules *into* the database and run
  them as read-only **queries** or transactional **executors** against a
  kernel-style syscall ABI (`get`/`put`/`delete`, batched scans, input/output
  streams, fuel + memory limits).
- **Write-range triggers** — bind a module to a key range and the engine
  invokes it asynchronously whenever a committed write touches the range:
  schema-free custom indexes, materialized views, and changefeeds with no
  writer cooperation. Two modes, picked by the module's exports: **keys**
  (coalesced touched keys — reconcile against current state) and
  **changes** (`on_apply` receives the ordered list of committed changes,
  values included). Events are durable (they commit atomically with the
  triggering write) and consumed exactly-once.
- **Database forks** — not PITR (no log archiving, no
  restore-to-arbitrary-time; a fork is a named cut, from recent or from a
  specific point): `fork("name")` pins an MVCC snapshot at the current
  head and hard-links the immutable files into `archive/name/`, so
  creation copies almost nothing and leaves live readers/writers
  undisturbed. `pin("name")` durably marks the current seqno as fork-able
  later; `fork_at("name", seqno)` then cuts at exactly that point (the
  tables are rewritten to the cut, the value log stays hard-linked). Each
  fork is itself a complete database directory — open it read-write and
  it's a live, copy-on-write clone of the parent.
- **Opt-in rebuild journal** — an off-by-default catastrophe-recovery net
  (`journal::Journal::attach`): a separate, async append-only record of every
  user-key mutation, independent of the store's own files, from which a fresh
  database is rebuilt from zero (`journal::rebuild`) for the day a disk block
  goes bad or the directory is lost. It never sits on the commit path — the DB
  stays the fast source of truth and the journal trails it.
- **Server mode** — `fluent-server` serves one store over every network
  plane in one process: GraphQL (typed/admin plane, where installed WASM
  modules surface as their own query/mutation fields), the binary wire
  pipe (data plane), and a replication join point where full replicas and
  key-range edge caches attach.

See [DESIGN.md](DESIGN.md) for the full architecture.

## Quick start

```rust
use fluent31::{Db, Options};

let db = Db::open("./data", Options::default())?;
db.put("user/1", "ada")?;
assert_eq!(db.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));

// snapshots
let snap = db.snapshot();
db.put("user/1", "grace")?;
assert_eq!(db.get_at(b"user/1", &snap)?.as_deref(), Some(&b"ada"[..]));

// transactions (optimistic, snapshot isolation)
let mut txn = db.begin();
let bal = txn.get_for_update(b"acct")?;
txn.put("acct", "90")?;
txn.commit()?; // Err(Error::Conflict) if someone else wrote acct

// ordered scans, both directions
for kv in db.iter(Some(b"user/"), Some(b"user0"), false)? {
    let (k, v) = kv?;
}

// fork — an MVCC cut, hard-linked; open it for a writable CoW clone
let fork = db.fork("before-migration")?;
let clone = Db::open(&fork.path, Options::default())?;

// or address the cut explicitly: pin now, fork that exact point later
let pin = db.pin("pre-import")?; // durable; holds GC until unpin
// ... more writes ...
let fork = db.fork_at("rollback-point", pin.seqno)?;

// db.seqno() addresses "now" without the durable hold: capture once,
// cut any number of deterministic forks of that same version
let s = db.seqno();
let a = db.fork_at("replica-a", s)?;
let b = db.fork_at("replica-b", s)?; // same cut as replica-a
```

## WASM instead of SQL

Write a guest with the SDK, build it for `wasm32-unknown-unknown`, install
it, run it:

```rust
// guests/agg/src/lib.rs — "SELECT count,sum,min,max WHERE prefix"
use fluent_guest::Fail;

#[fluent_guest::main]                       // exports the `run` entry point
fn agg(prefix: Vec<u8>) -> Result<Vec<u8>, Fail> {
    let scan = fluent_guest::scan_prefix(&prefix).map_err(|_| Fail::new(3, "scan failed"))?;
    let (mut count, mut sum) = (0u64, 0u64);
    for (_k, v) in scan {
        count += 1;
        sum += u64::from_le_bytes(v[..8].try_into().unwrap());
    }
    Ok([count.to_le_bytes(), sum.to_le_bytes()].concat())
}
```

`Ok` output becomes the invocation's result; `Err(Fail { code, message })`
becomes a non-zero exit with the message in the output buffer. (The raw
`fluent_main!(fn() -> i32)` layer still exists for exit-code-speaking
modules.)

```rust
db.install_module("agg", &std::fs::read("agg.wasm")?)?;
let out = db.query("agg", b"metric/")?;          // read-only, snapshot-bound
let out = db.execute("transfer", &input)?;        // transactional, auto-retried
```

The same modules surface over the server: a module that describes its
interface becomes its own typed GraphQL query or mutation field the
moment it's installed (see [Server mode](#server-mode)).

Executors run inside a transaction: guest exit `0` commits, anything else
aborts; commit conflicts re-run the module against a fresh snapshot
automatically. Guests are sandboxed hard: fuel-metered, memory-capped,
output/log/scan/write-set-capped, no WASI, reserved keyspace invisible.

An executor can also be bound to a key range as a **trigger** — the engine
then invokes it after every committed write into the range, with the
touched keys as input:

```rust
db.create_trigger("customerIndex", "customer_index",
                  Some(b"orders/"), Some(b"orders0"))?;
db.put("orders/00000042", r#"{"customer":"acme","amountCents":500}"#)?;
// moments later, with no writer cooperation:
//   idx/customer/acme/00000042  (maintained by guests/customer_index)
```

A keys-mode trigger event means "this key was touched — reconcile it": the
module reads current state and converges, so replays and coalesced
re-touches are harmless. A module exporting `on_apply` gets **changes
mode** instead: the ordered list of committed changes (op kind, key,
value, commit seqno), one event per op — the post-apply filter that feeds
changefeeds and event-driven index generation (see `guests/order_feed`).
See [WASM.md](WASM.md) §8 for both authoring contracts.

Build the bundled guests:

```sh
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release
```

Or watch the whole story run — self-asserting end-to-end walkthroughs that
build the guests, open a store, and drive them (each is also the reference
implementation of a classic SQL feature, rebuilt schema-free):

```sh
cargo run -p fluent31 --example dynamic_index   # CREATE INDEX at runtime: spec keys
                                                # backfill, maintain, and tear down indexes
cargo run -p fluent31 --example live_stats      # GROUP BY that's always fresh: exactly-once
                                                # folding, proven drift-free under a storm
cargo run -p fluent31 --example cascade_delete  # ON DELETE CASCADE: parent delete sweeps
                                                # its subtree; no-stacking stops loops
cargo run -p fluent31 --example claim           # UNIQUE constraint: 8 concurrent claimers,
                                                # exactly one winner via OCC
```

## The shell

```
$ cargo run -p fluent-cli -- ./data
fluent31 shell — ./data — opened in (54.78 ms) — `help` for commands
io backend: std
fluent31> put hello world
OK  (3.02 ms)
fluent31> get hello
"world"  (28.7 µs)
fluent31> scan - - --limit 10
   1) "hello" => "world"  (237.6 µs)
fluent31> fork snap1
fork snap1 @ seq 2 -> ./data/archive/snap1  (61.20 ms)
fluent31> stats
backend        std
visible seqno  2
...
```

Every command prints its wall-clock latency. `begin/tput/commit` drive
transactions, `install/query/exec` drive WASM, `mktrig/deltrig/triggers`
manage write-range triggers, `gc` runs value-log GC.

## Server mode

The engine embeds, but it also serves. `fluent-server` is the formal
server mode: one process, one `Db`, all three network planes —

```sh
cargo run -p fluent-server -- ./data --store-name prod
# graphql      http://127.0.0.1:8317/graphql   typed/admin plane, GraphiQL at /
# wire         tcp 127.0.0.1:8427              binary data-plane pipe (WIRE.md)
# replication  tcp 127.0.0.1:8428              join point (REPLICATION.md)
```

The store directory is flocked — the planes cannot be split across
processes, so server mode is how they share one database handle. From the
replication join point, two kinds of node attach (via `fluent-replication
edge`): a **full replica** (unbounded scope) or an **edge cache** holding
only a key-range slice. Replication anchors provenance on the
deterministic store identity, so the join point opens only on a named
store: pass `--store-name` once — the name persists — and it opens on
every later start; without a name, graphql + wire still serve and the
join point stays closed. `--graphql/--wire/--replication` rebind the
ports, `--sync` picks the durability mode.

When you want exactly one surface, each plane also runs standalone
(`fluent-graphql`, `fluent-wire`, `fluent-replication master`) with the
same defaults — those binaries are documented in the sections below.

## The GraphQL plane

```sh
cargo run -p fluent-server -- ./data             # or standalone, one plane only:
cargo run -p fluent-graphql -- ./data            # http://127.0.0.1:8317/graphql, GraphiQL at /
cargo run -p fluent-graphql -- ./data --sync periodic:50   # memory-speed acks, <=50ms loss window
cargo run -p fluent-graphql -- --print-schema    # dump the SDL
```

One schema covers the direct operations **and every installed WASM
module**: a module that declares its interface becomes its own typed root
field on `Query` or `Mutation` the moment it's installed (next section) —
the schema is dynamic, not fixed. Every field of a single GraphQL query
operation executes at one pinned MVCC snapshot, so multi-field reads are
mutually consistent.

The server routes by instance: the primary database answers at
`/graphql`, and every fork answers at `/graphql/<instanceId>` with the
same full surface (its own schema, modules, even its own forks). The
`fork` mutation returns the new branch's `instanceId`; instances open
lazily on first request and idle ones close automatically. The id is an
address, not a credential — put real access control in front if you need
isolation.

```graphql
query {
  snapshotSeqno
  seqno            # current visible seqno — the `at:` address of "now"
  get(key: {text: "user/1"}) { text }
  scan(prefix: {text: "user/"}, limit: 100) {
    pairs { key { text } value { base64 } }
    hasMore
    nextAfter { base64 }        # pass back as `after` to paginate
  }
  topCustomers(limit: 3) { customer totalCents }   # an installed module's own typed field
  wasm(module: "agg", input: {text: "user/"}) { hex }   # generic fallback: raw bytes in/out
}

mutation {
  put(key: {text: "user/3"}, value: {text: "carol"})
  writeBatch(ops: [{put: {key: {text: "a"}, value: {text: "1"}}},
                   {delete: {text: "b"}}])                # atomic
  placeOrder(customer: "acme", amountCents: "4200") { id }  # typed executor module
  wasmExecute(module: "transfer", input: {base64: "..."}) { base64 }  # generic fallback
  installModule(name: "agg", wasm: {base64: "..."}) { name size }
  createTrigger(name: "idx", module: "customer_index",
                lo: {text: "orders/"}, hi: {text: "orders0"})
  fork(name: "snap1") { instanceId }   # branch this instance at its head
  pin(name: "p1") { seqno }            # durably mark this point fork-able
  fork(name: "rollback", at: "42") { instanceId }  # branch at a pinned seqno
}
```

Keys and values are raw bytes: inputs take exactly one of `text` / `base64` /
`hex`, outputs expose all three plus `len`. 64-bit engine quantities (seqnos,
timestamps, byte totals) use the string-encoded `U64` scalar — they don't fit
GraphQL's 32-bit `Int` or JS double precision. Engine failures map to
`extensions.code` (`CONFLICT`, `INVALID_ARGUMENT`, `GUEST_FAILED` with the
guest's exit code and output, ...).

### Every module can be its own query or mutation

Installing a WASM module doesn't just make it callable through the generic
`wasm`/`wasmExecute` byte pipes — a module that exports `describe`
(emitting a JSON schema descriptor — see
`crates/fluent-graphql/src/descriptor.rs`) becomes its **own typed root
field**, dynamically: `kind: "query"` modules land on `Query`, `kind:
"execute"` on `Mutation`, named after the module, with declared arguments
and a declared output type. The GraphQL schema is rebuilt and hot-swapped
on every `installModule`/`uninstallModule`, and at server startup for
already-installed modules — install `placeOrder`, and `mutation {
placeOrder(...) }` exists; uninstall it, and it's gone. Described modules
must use a valid GraphQL field name and may not shadow built-in fields or
redeclare reserved/claimed type names — enforced at install time. Modules
without `describe` stay reachable through the generic `wasm`/`wasmExecute`
fields. `mutation { reloadSchema }` re-describes everything — the resync
path after installing modules through the CLI (or after a failed
post-install rebuild).

The full authoring manual and ABI spec live in [`WASM.md`](WASM.md).
In a Rust guest this is one macro next to the entry-point function:

```rust
fluent_guest::fluent_describe!(r#"{
  "kind": "execute",
  "args": [{"name": "customer", "type": "String!"},
           {"name": "amountCents", "type": "U64!"}],
  "types": [{"name": "PlacedOrder", "fields": [
    {"name": "id", "type": "U64!"},
    {"name": "customerTotalCents", "type": "U64!"}]}],
  "output": "PlacedOrder!"
}"#);
```

Typed args arrive at the guest as one JSON object; the guest's output is
parsed as JSON and validated against the declared type before it reaches the
client.

### Demo: the order pair

`guests/place_order` (writer: id allocation + order record + customer stats,
one transaction, OCC-retried) and `guests/top_customers` (reader: rank
customers by lifetime spend at the operation's snapshot) show the full
typed-module workflow. With a server running:

```sh
scripts/demo-orders.sh          # builds, installs both modules, seeds orders, ranks
```

Then in GraphiQL:

```graphql
mutation { placeOrder(customer: "you", amountCents: "4200") { id customerTotalCents } }
query    { topCustomers(limit: 3) { customer orders totalCents avgCents } }
```

## The wire pipe

For the data-plane heat lane — raw bytes, request/response correlation by
id, out-of-order completion on one connection (a slow `EXEC` never blocks
the `GET`s pipelined behind it). `fluent-server` opens it alongside
GraphQL; standalone:

```sh
cargo run -p fluent-wire -- ./data --sync periodic:50    # tcp 127.0.0.1:8427
```

Spec in [`WIRE.md`](WIRE.md); reference client `fluent_wire::WireClient`.
GraphQL stays the general/typed/admin plane.

## Replicas and edge caches

Read replicas that attach to a running server's replication join point
(`fluent-server`'s `:8428`, or the standalone master below) and hold a
key-range slice of it — `[lo, hi)` unbounded for a full replica, narrow
for an edge cache: the overlapping index fragments copied locally, values
fetched lazily and cached, committed writes streamed in. Provenance is
anchored on a deterministic store identity — a restored/forked master
re-mints its instance id and every edge invalidates wholesale instead of
serving a divergent history.

```sh
cargo run -p fluent-server -- ./data --store-name prod                    # join point on :8428
# or serve only the replication plane:
cargo run -p fluent-replication -- master ./data --store-name prod       # tcp 127.0.0.1:8428

cargo run -p fluent-replication -- edge --master 127.0.0.1:8428 \
    --dir /tmp/edge-cache --lo user/ --hi user0 --serve 127.0.0.1:8427   # wire-v1 reads
```

Spec in [`REPLICATION.md`](REPLICATION.md); identity model in
[`DESIGN.md`](DESIGN.md) §14.

## Testing

```sh
cargo test --workspace           # randomized model, group commit, wasm, graphql, server-mode &
                                 # replication e2e, plus durability: hard-crash recovery,
                                 # corruption fuzz, journal rebuild
cargo test -p fluent31 --features fault-injection   # fsync-failure / ENOSPC / read-fault paths
cargo test --test backup_and_soak -- --ignored      # opt-in endurance soak
```

The durability suites are the confidence floor for system-of-record use: a
SIGKILLed child process proves acked writes survive a hard crash
(`crash_recovery`), a fault-injecting IO backend proves a failed fsync is never
a false ack (`fault_injection`), a mutation sweep proves no on-disk byte can
panic the reader (`corruption_fuzz`), and a nuke-the-directory replay proves the
journal rebuilds exact state (`journal_rebuild`).

On Linux the suite exercises the io_uring backend automatically. Under
Docker, io_uring syscalls are blocked by the default seccomp profile:

```sh
docker run --security-opt seccomp=unconfined -v $PWD:/src -w /src rust:1 \
  sh -c "rustup target add wasm32-unknown-unknown && cargo test --workspace"
```

`cargo check -p fluent31 --no-default-features` builds the engine without
the WASM layer (no wasmtime).

## Crate layout

```
crates/fluent31           the engine (lib), incl. store identity + edge store
crates/fluent-guest       guest-side SDK for WASM modules
crates/fluent-cli         interactive shell
crates/fluent-server      server mode: all three planes below in one process
crates/fluent-graphql     GraphQL server (axum + async-graphql)
crates/fluent-wire        binary wire-protocol server + reference client
crates/fluent-replication edge replication channel: master server + replica driver
guests/               example WASM guests (separate workspace): agg, transfer,
                      place_order + top_customers (typed GraphQL demo pair)
```
