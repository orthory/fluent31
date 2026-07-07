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
- **Write-range triggers** — bind an executor module to a key range and the
  engine invokes it asynchronously whenever a committed write touches the
  range: schema-free custom indexes and materialized views with no writer
  cooperation. Events are durable (they commit atomically with the
  triggering write), coalesced per key, and consumed exactly-once.
- **PITR checkpoints** — `checkpoint("name")` hard-links the immutable files
  into `archive/name/`, which is itself a complete database directory:
  restore = open, and opening read-write forks copy-on-write.

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

// point-in-time checkpoint; restore by just opening the archive dir
let cp = db.checkpoint("before-migration")?;
let frozen = Db::open(&cp.path, Options::default())?;
```

## WASM instead of SQL

Write a guest with the SDK, build it for `wasm32-unknown-unknown`, install
it, run it:

```rust
// guests/agg/src/lib.rs — "SELECT count,sum,min,max WHERE prefix"
fn agg_main() -> i32 {
    let prefix = fluent_guest::input();
    let (mut count, mut sum) = (0u64, 0u64);
    for (_k, v) in fluent_guest::scan_prefix(&prefix).unwrap() {
        count += 1;
        sum += u64::from_le_bytes(v[..8].try_into().unwrap());
    }
    fluent_guest::output(&count.to_le_bytes());
    fluent_guest::output(&sum.to_le_bytes());
    0
}
fluent_guest::fluent_main!(agg_main);
```

```rust
db.install_module("agg", &std::fs::read("agg.wasm")?)?;
let out = db.query("agg", b"metric/")?;          // read-only, snapshot-bound
let out = db.execute("transfer", &input)?;        // transactional, auto-retried
```

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

A trigger event means "this key was touched — reconcile it": the module
reads current state and converges, so replays and coalesced re-touches are
harmless. See [WASM.md](WASM.md) §8 for the authoring contract.

Build the bundled examples:

```sh
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release
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
fluent31> checkpoint snap1
checkpoint snap1 @ seq 2 -> ./data/archive/snap1  (61.20 ms)
fluent31> stats
backend        std
visible seqno  2
...
```

Every command prints its wall-clock latency. `begin/tput/commit` drive
transactions, `install/query/exec` drive WASM, `mktrig/deltrig/triggers`
manage write-range triggers, `gc` runs value-log GC.

## The GraphQL server

```sh
cargo run -p fluent-graphql -- ./data            # http://127.0.0.1:8317/graphql, GraphiQL at /
cargo run -p fluent-graphql -- ./data --sync periodic:50   # memory-speed acks, <=50ms loss window
cargo run -p fluent-graphql -- --print-schema    # dump the SDL
```

One schema covers both the direct operations and the registered WASM
programs. Every field of a single GraphQL query operation executes at one
pinned MVCC snapshot, so multi-field reads are mutually consistent:

```graphql
query {
  snapshotSeqno
  get(key: {text: "user/1"}) { text }
  scan(prefix: {text: "user/"}, limit: 100) {
    pairs { key { text } value { base64 } }
    hasMore
    nextAfter { base64 }        # pass back as `after` to paginate
  }
  wasm(module: "agg", input: {text: "user/"}) { hex }   # read-only WASM query
}

mutation {
  put(key: {text: "user/3"}, value: {text: "carol"})
  writeBatch(ops: [{put: {key: {text: "a"}, value: {text: "1"}}},
                   {delete: {text: "b"}}])                # atomic
  wasmExecute(module: "transfer", input: {base64: "..."}) { base64 }  # transactional
  installModule(name: "agg", wasm: {base64: "..."}) { name size }
  createTrigger(name: "idx", module: "customer_index",
                lo: {text: "orders/"}, hi: {text: "orders0"})
  checkpoint(name: "snap1") { lastSeqno }
}
```

Keys and values are raw bytes: inputs take exactly one of `text` / `base64` /
`hex`, outputs expose all three plus `len`. 64-bit engine quantities (seqnos,
timestamps, byte totals) use the string-encoded `U64` scalar — they don't fit
GraphQL's 32-bit `Int` or JS double precision. Engine failures map to
`extensions.code` (`CONFLICT`, `INVALID_ARGUMENT`, `GUEST_FAILED` with the
guest's exit code and output, ...).

### Typed WASM root fields

A module that exports `describe` (emitting a JSON schema descriptor — see
`crates/fluent-graphql/src/descriptor.rs`) becomes its **own typed root
field**: `kind: "query"` modules land on `Query`, `kind: "execute"` on
`Mutation`. The GraphQL schema is rebuilt and hot-swapped on every
`installModule`/`uninstallModule`, and at server startup for already-installed
modules. Described modules must use a valid GraphQL field name and may not
shadow built-in fields or redeclare reserved/claimed type names — enforced at
install time. Modules without `describe` stay reachable through the generic
`wasm`/`wasmExecute` fields. `mutation { reloadSchema }` re-describes
everything — the resync path after installing modules through the CLI (or
after a failed post-install rebuild).

The full authoring manual and ABI spec live in [`WASM.md`](WASM.md).
In a Rust guest this is one macro next to `fluent_main!`:

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

## The wire protocol

For the data-plane heat lane — raw bytes, request/response correlation by
id, out-of-order completion on one connection (a slow `EXEC` never blocks
the `GET`s pipelined behind it):

```sh
cargo run -p fluent-wire -- ./data --sync periodic:50    # tcp 127.0.0.1:8427
```

Spec in [`WIRE.md`](WIRE.md); reference client `fluent_wire::WireClient`.
GraphQL stays the general/typed/admin plane.

## Testing

```sh
cargo test --workspace           # 130 tests incl. randomized model, group commit, wasm & graphql e2e
```

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
crates/fluent31       the engine (lib)
crates/fluent-guest   guest-side SDK for WASM modules
crates/fluent-cli     interactive shell
crates/fluent-graphql GraphQL server (axum + async-graphql)
crates/fluent-wire    binary wire-protocol server + reference client
guests/               example WASM guests (separate workspace): agg, transfer,
                      place_order + top_customers (typed GraphQL demo pair)
```
