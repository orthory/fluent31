# fluent31 guide

How to use fluent31: embed it, drive it from the shell, serve it, write
modules for it, run it in production.

This is the usage document. The specs behind it are [DESIGN.md](DESIGN.md)
(the architecture as implemented), [WASM.md](WASM.md) (the module ABI and
authoring manual) and [REPLICATION.md](REPLICATION.md) (the replica
protocol). When a detail
matters, they win. [SKILL.md](SKILL.md) is the short entry point for
agents.

---

**Contents**

1. [What fluent31 is](#1-what-fluent31-is)
2. [Install and build](#2-install-and-build)
3. [Quick start](#3-quick-start)
4. [Concepts](#4-concepts)
5. [Embedded API (Rust)](#5-embedded-api-rust)
6. [WASM modules](#6-wasm-modules)
7. [Triggers: indexes, views, feeds](#7-triggers-indexes-views-feeds)
8. [Forks, pins, clones](#8-forks-pins-clones)
9. [Durability, recovery, journal](#9-durability-recovery-journal)
10. [The shell](#10-the-shell)
11. [Server mode](#11-server-mode)
12. [GraphQL plane](#12-graphql-plane)
13. [Replicas and edge caches](#13-replicas-and-edge-caches)
14. [Operations](#14-operations)
15. [Testing](#15-testing)
16. [Advanced: how it works](#16-advanced-how-it-works)
17. [Glossary](#17-glossary)

---

## 1. What fluent31 is

fluent31 is an embedded key-value database engine written in Rust: an
LSM tree with key-value separation (large values live in an append-only
value log, so the index stays small enough to keep in memory), MVCC
snapshots, optimistic transactions, io_uring on Linux and portable IO
elsewhere.

Where a SQL database gives you a query language, fluent31 gives you
WebAssembly. You install modules into the database and run them as
read-only queries or as transactional executors. You can also bind a
module to a key range as a trigger, and the engine will invoke it after
every commit into that range to maintain indexes, aggregates and
changefeeds. There is no schema. Key layout and module code take its
place.

Around the engine:

| Surface | What it is |
|---|---|
| `fluent31` crate | The engine. Embed it in a Rust process. |
| `fluent-guest` crate | The SDK for writing WASM modules. |
| `fluent-cli` | An interactive shell. Also the journal rebuild tool. |
| `fluent-server` | One process serving one store on two planes: GraphQL (typed and admin operations, subscriptions) and replication (the join point for replicas). |
| `fluent-graphql`, `fluent-replication` | Each plane as a standalone binary, with the same defaults. |

What it is not:

- Not SQL. There is no query language, no columns, no joins. You get key
  layout and modules.
- Not a version store. MVCC is the engine's own consistency machinery.
  The contract is in [§4.3](#43-the-consistency-contract).
- Not point-in-time recovery. Forks are named cuts, not continuous log
  archiving.
- Not a public-facing sandbox. The WASM limits protect reliability and
  integrity. Authentication and authorization are a layer you put in
  front.
- Single-node today. A store is one directory, locked by one process.
  Replicas are read-only followers.

## 2. Install and build

You need stable Rust (2021 edition). For modules, add the wasm target:

```sh
rustup target add wasm32-unknown-unknown
```

Build and test the workspace:

```sh
cargo build --workspace --release
cargo test --workspace
```

The example modules live in a separate workspace under `guests/` and only
build for wasm32:

```sh
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release
# artifacts: guests/target/wasm32-unknown-unknown/release/<name>.wasm
```

If your `cargo` is not rustup's, point it at rustup's rustc so the wasm32
standard library is found: `RUSTC="$(rustup which rustc)" cargo build ...`.

Cargo features on `fluent31`:

| Feature | Default | Effect |
|---|---|---|
| `wasm` | on | The WASM layer (wasmtime). `--no-default-features` builds the pure storage engine; module and trigger APIs do not exist. |
| `fault-injection` | off | A test seam that exposes the IO traits and `Db::open_with_io`. Never enable it in production. |

Platforms: Linux, where io_uring is probed automatically with a fallback
to portable IO, and macOS with portable IO. Docker's default seccomp
profile blocks io_uring, so run with `--security-opt seccomp=unconfined`
or set `io_backend = Std`.

## 3. Quick start

### 3.1 Embedded

```rust
use fluent31::{Db, Options, WriteBatch};

let db = Db::open("./data", Options::default())?;

db.put("user/1", "ada")?;
assert_eq!(db.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));

// atomic batch
let mut b = WriteBatch::new();
b.put("user/2", "grace");
b.delete("user/0");
db.write(b)?;

// ordered scan over [lo, hi)
for kv in db.iter(Some(b"user/"), Some(b"user0"), false)? {
    let (k, v) = kv?;
}

// transaction: read-modify-write with conflict detection
let mut txn = db.begin();
let n = txn.get_for_update(b"counter")?.map(parse).unwrap_or(0);
txn.put("counter", (n + 1).to_string())?;
txn.commit()?;                       // Err(Error::Conflict) if counter changed meanwhile
```

`Db` is `Send + Sync`, so share one handle across threads with `Arc<Db>`.
Dropping it stops and joins the background threads (with a final WAL sync
under `Periodic`). The memtable is not written out to tables on drop; the
next open replays it from the WAL.

### 3.2 Shell

```
$ cargo run -p fluent-cli -- ./data
fluent31> put hello world
OK  (3.02 ms)
fluent31> get hello
"world"  (28.7 µs)
fluent31> scan - - --limit 10
   1) "hello" => "world"  (237.6 µs)
fluent31> help
```

### 3.3 Server

```sh
cargo run -p fluent-server -- ./data --store-name prod
# graphql      http://127.0.0.1:8317/graphql   (GraphiQL at /)
# replication  tcp 127.0.0.1:8428
```

```sh
curl -s http://127.0.0.1:8317/graphql -H 'content-type: application/json' \
  -d '{"query":"mutation { put(key:{text:\"user/1\"}, value:{text:\"ada\"}) }"}'
curl -s http://127.0.0.1:8317/graphql -H 'content-type: application/json' \
  -d '{"query":"{ get(key:{text:\"user/1\"}) { text } }"}'
```

### 3.4 A module

```rust
// guests/hello/src/lib.rs, built as a cdylib for wasm32-unknown-unknown
use fluent_guest::Fail;

#[fluent_guest::query]                         // exports `query`: read-only
fn count(prefix: Vec<u8>) -> Result<String, Fail> {
    let n = fluent_guest::scan_prefix(&prefix).map_err(|_| Fail::new(2, "scan"))?.count();
    Ok(n.to_string())
}
```

```rust
db.install_module("count", &std::fs::read("count.wasm")?)?;
let out = db.query("count", b"user/")?;        // b"2"
```

## 4. Concepts

### 4.1 Keys, seqnos, snapshots

A user key is non-empty, does not start with byte `0x00`, and is at most
`max_key_size` (16 KiB). Values go up to `max_value_size` (256 MiB).

The `0x00` prefix is the engine's reserved keyspace, where installed
modules, trigger definitions and trigger queues live. Reads and writes of
such keys are rejected (`InvalidArgument` from the API, `EINVAL` inside a
module), and scans clamp to the user keyspace.

There is no prefix argument in the Rust API or the shell. A prefix scan
is `[prefix, prefix+1)`, where `prefix+1` is the prefix with its last
byte incremented: `user/` scans to `user0`. GraphQL's `scan(prefix:)`
and the SDK's `scan_prefix` compute this for you.

Every write, whether a `put`, a `WriteBatch`, a transaction commit or an
executor's writes, gets a contiguous range of sequence numbers (seqnos)
and becomes visible all at once. `db.seqno()` is the latest committed
seqno, the address of "now". A `Snapshot` reads at one seqno; a GraphQL
query operation pins one for all its fields; a module invocation runs at
one.

The examples use `<entity>/<id>` for records, zero-padded numeric ids so
they sort (`orders/00000042`), and derived data under its own prefix
(`idx/customer/acme/00000042`).

### 4.2 Durability

`Options::sync` decides when writes reach stable storage.

| `SyncMode` | An ack means | Crash loss |
|---|---|---|
| `Always` (default) | fsynced. Concurrent writers share one fsync (group commit). | none of what was acked |
| `Periodic { every }` | in memory; a background timer fsyncs on that interval. `db.sync_wal()` is the on-demand barrier. | up to one interval |
| `Never` | in memory; the OS flushes when it likes. | the recent tail |

No mode can corrupt the store. Recovery truncates a torn tail and
resumes. The server flags spell these `always`, `periodic:<ms>` and
`never`.

### 4.3 The consistency contract

MVCC is how the engine gives you consistent reads and optimistic
transactions. It is not an application-level version store, and the
rules below follow from that.

- Snapshots are operation-scoped. Take one, read, drop it. Its cost is
  store-wide: the GC watermark is the seqno of the oldest live snapshot,
  and nothing at or above it is reclaimed, for any key, not just the ones
  the snapshot reads.
- Seqnos are addresses, not ids. `db.seqno()` names the current state and
  stays resolvable only until GC passes it. Don't store seqnos in
  application data. A journal rebuild renumbers them wholesale, and a
  fork or restore mints a new store identity.
- Pins and forks are coarse, named cuts. Use them for a handful of
  deliberate points: before a migration, a staging clone, a rollback
  anchor. A pin is a durable store-wide GC hold; a fork is a whole
  database directory. Neither is priced per document, let alone per
  write.
- There is no retention policy. Old versions survive until the GC
  watermark passes them. There is no "keep N versions of this key".

If you need history, make it data. Bind a changes-mode trigger to the
range. Every committed change (kind, key, seqno, and the value up to
`trigger_inline_value`; larger values arrive key-only and you read them
back) is delivered durably, in order, with exactly-once effect. Write it
under keys you own:

```
doc/42                     current value       (what the app writes)
history/doc/42/<seqno>     one entry per write (what the trigger writes)
```

History is then scannable, replicable and live-tailable as a
subscription, and you prune it yourself with a scan and a delete batch,
on whatever schedule suits the data. Modules have no clock, so if entries
need timestamps the writer puts them in the value. `guests/order_feed` is
the reference shape and [§7](#7-triggers-indexes-views-feeds) has the
contract.

### 4.4 Transactions

Optimistic, snapshot isolation, first committer wins. `commit()` checks
every key read with `get_for_update` and every key in the write set
against the transaction's snapshot; a committed delete conflicts too. The
loser gets `Error::Conflict` with nothing written, and the fix is to run
the whole read-modify-write again. A plain `get` inside a transaction is
a consistent read but not a conflict check, so use `get_for_update` for
every key you base a write on.

### 4.5 Modules and roles

A module is a WASM binary stored in the database. Its exports are its
roles:

| Export | Role | Runs as |
|---|---|---|
| `query` | read-only query | one pinned snapshot; writes return `EROFS` |
| `execute` | executor | a fresh transaction; exit 0 commits, non-zero aborts; conflicts re-run it |
| `on_touch` | keys-mode trigger consumer | an executor, invoked by the engine with touched keys |
| `on_apply` | changes-mode trigger consumer | an executor, invoked by the engine with the ordered change list |
| `describe` | optional, the typed GraphQL surface | read-only at install and schema build |

Modules are sandboxed. They are fuel-metered and memory-capped, have no
WASI, no clock and no randomness, and can import only the `fluent` host
functions. An executor may run several times per call because of
conflict retries, so it has to be a pure function of its input and the
database state.

### 4.6 Triggers

A trigger binds an installed module to a key range. After every committed
write that touches the range, the engine invokes the module
asynchronously. The module's exports pick one of two modes. Keys mode
(`on_touch`) delivers coalesced "this key was touched, reconcile it"
events. Changes mode (`on_apply`) delivers every committed op, in order,
with values. Events commit atomically with the write that caused them,
their consumption commits atomically with the module's own writes, and a
trigger's writes never fire triggers.
[§4.10](#410-sql-feature-to-fluent31-primitive) maps SQL features onto
them.

### 4.7 Forks and pins

`fork(name)` publishes a complete, consistent copy of the database at its
current head (the cut) under `archive/<name>/`. It is built from hard
links plus one bounded copy of the growing value-log file, so the cost is
proportional to the number of files, not to the amount of data. Open it
and you have a live copy-on-write clone. `pin(name)` durably marks the
current seqno so that `fork_at(name, seqno)` can cut there later.

### 4.8 Journal

The journal is off by default. It is an independent append-only record of
every user-key mutation, written asynchronously off the commit path. From
it a fresh database can be rebuilt when the store directory itself is
lost or damaged beyond what the engine can self-recover.

### 4.9 Store identity and instances

A store can carry an operator-chosen name. From the name the engine mints
a deterministic 128-bit instance id, and forks and restores mint new
ones. Replication verifies the id on every connection, so a replaced
master invalidates every replica at once. Under the server, every fork
is an instance addressed at `/graphql/<instanceId>`. The id is an
address, not a credential.

### 4.10 SQL feature to fluent31 primitive

| You want | Use | Reference |
|---|---|---|
| `SELECT ... WHERE key LIKE 'p%'` | `iter`/`scan` with a prefix range | §5.3 |
| an aggregate over a range | a `query` module | `guests/agg`, `guests/top_customers` |
| a stored procedure or multi-key transaction | an `execute` module | `guests/place_order`, `guests/transfer` |
| a `UNIQUE` constraint | `execute` plus `get_for_update` | `guests/claim` |
| `CREATE INDEX` | a keys-mode trigger | `guests/customer_index` |
| an index created at runtime from a spec | a changes-mode trigger | `guests/dynamic_index` |
| a `GROUP BY` that is always current | a changes-mode trigger folding deltas | `guests/live_stats` |
| `ON DELETE CASCADE` | a changes-mode trigger | `guests/cascade_delete` |
| CDC, an audit log, an event stream | a changes-mode trigger plus a `feed` subscription | `guests/order_feed` |
| per-row history | a changes-mode trigger writing `history/...` keys | §4.3 |
| a migration script | a one-shot executor | §6.8 |
| a staging copy or rollback point | a fork or a pin | §8 |
| a read replica or edge cache | replication | §14 |

## 5. Embedded API (Rust)

`fluent31` exports `Db`, `Options`, `SyncMode`, `IoBackend`,
`Compression`, `WriteBatch`, `Snapshot`, `Txn`, `DbIterator`, `DbStats`,
`Error`, `Result`, `ForkInfo`, `PinInfo`, `ModuleInfo`, `TriggerInfo`,
`TriggerMode`, `Journal`, `JournalConfig`, `JournalStats`,
`RebuildReport`, `Subscription`, `StreamEvent`, `StreamEntry`,
`StoreIdentity`, `InstanceId`, `SeqNo`, `ValueKind`, `restore_to`,
`list_forks_at`, the replication-surface types `SliceManifest`,
`SliceRun` and `SliceTable`, and the `journal`, `identity` and `edge`
modules.

### 5.1 Opening

```rust
let db = Db::open(path, Options { sync: SyncMode::Periodic { every: Duration::from_millis(50) }, ..Options::default() })?;
```

`Db::open` creates the directory when `create_if_missing` is set (the
default), takes an exclusive flock so that a second open of the same
directory fails, recovers from the WAL, and then starts the flush,
compaction, commit and trigger threads. Recovery time is proportional to
the unflushed WAL.

Every `Options` field, with its default:

| Field | Default | Meaning |
|---|---|---|
| `create_if_missing` | `true` | Create the directory. `false` fails on a missing store. |
| `sync` | `Always` | The durability mode (§4.2). |
| `io_backend` | `Auto` | `Auto` probes io_uring and falls back; `Uring` forces it and open fails where unsupported; `Std` forces pread/pwrite. |
| `wasm_enabled` | `true` | `false` makes the WASM layer inert at runtime: module and trigger calls return `Error::Wasm`, the trigger runner does not start, and writes made while disabled never fire triggers. Listing still works. |
| `store_name` | `None` | An operator-chosen name, unique across your fleet, that fixes the store identity (§4.9). Required for replication. Set it once: an unnamed store adopts it, an omitted name on reopen keeps the persisted one, and a different name is `InvalidArgument`. A fork's name is fixed at fork time. |
| `memtable_size` | 8 MiB | Freeze and flush the memtable past this. |
| `max_immutable_memtables` | 2 | Frozen memtables waiting for flush before writers stall. |
| `block_size` | 8 KiB | Target data block size in tables. |
| `compression` | `None` | `Lz4` compresses the data and index blocks of newly written tables. Reads never depend on it; a store is readable under either setting. |
| `bloom_bits_per_key` | 10 | Bloom filter budget. |
| `block_cache_size` | 64 MiB | The shared block cache (table blocks and value-log records up to 64 KiB). |
| `l0_compaction_trigger` | 4 | L0 runs that trigger a merge into L1. |
| `tier_width` | 4 | Runs per level that trigger a merge to the next. |
| `max_levels` | 7 | The level count; the last level is one leveled run. |
| `l0_stall_trigger` | 12 | L0 runs at which writers stall until compaction catches up. |
| `target_file_size` | 64 MiB | Compaction splits runs into fragments of about this size. |
| `value_threshold` | 4096 | Values at or above this go to the value log; smaller ones stay inline. `0` separates everything and `usize::MAX` disables separation. |
| `vlog_file_size` | 128 MiB | Seal and rotate the value-log head at this size. |
| `vlog_gc_ratio` | 0.5 | A sealed value-log file becomes a GC victim once this fraction of it is known dead. |
| `max_key_size` | 16 KiB | Hard cap. |
| `max_value_size` | 256 MiB | Hard cap. |
| `max_txn_write_bytes` | 256 MiB | Cap on one transaction's buffered writes, executors included. |
| `sub_queue_bytes` | 8 MiB | Buffered bytes per change-stream subscriber. Past it the subscriber is cut off (`Lagged`); writers are never stalled. |
| `wasm_fuel` | 1e9 | Fuel per invocation. Exhaustion traps. |
| `wasm_memory_limit` | 64 MiB | Linear memory cap per invocation. |
| `execute_retries` | 3 | Attempts per executor call, the first included (minimum 1). A commit conflict re-runs until they are spent, then the call returns `Conflict`. |
| `max_wasm_input` | 64 MiB | Input cap, rejected before execution. |
| `max_wasm_output` | 32 MiB | Output cap (`ENOSPC` to the guest). |
| `max_wasm_log` | 1 MiB | Guest log cap. |
| `max_wasm_scans` | 64 | Open scan handles per invocation. |
| `wasm_module_cache` | 32 | Compiled modules kept in memory, keyed by content hash. |
| `trigger_batch` | 512 | Events per trigger invocation. |
| `trigger_inline_value` | 64 KiB | Changes-mode events carry the written value up to this size. Above it the value is elided and the event carries the key only. |

### 5.2 Point operations and batches

```rust
db.put(key, value)?;           // key/value: impl Into<Vec<u8>>
db.delete(key)?;               // succeeds whether or not the key exists
let v: Option<Vec<u8>> = db.get(b"key")?;

let mut b = WriteBatch::new();
b.put("a", "1"); b.delete("b");
b.len(); b.is_empty(); b.byte_size();
db.write(b)?;                  // atomic, one contiguous seqno range
```

### 5.3 Scans

```rust
// [lo, hi); None = open end; reverse = descending
let it: DbIterator = db.iter(Some(b"user/"), Some(b"user0"), false)?;
for kv in it {
    let (key, value): (Vec<u8>, Vec<u8>) = kv?;   // Item = Result<(Vec<u8>, Vec<u8>)>
}
```

The iterator resolves value-log pointers in batches (a prefetch window of
32 entries or 256 KiB, one batched read per value-log file), so a scan
over large values costs one IO round per group rather than one per entry.
An error ends iteration. Bounds are byte-exact and there is no prefix
argument at this layer, so compute `hi` as in §4.1. To resume after a key
`k` when paging, start the next scan at `k ++ 0x00`, the smallest key
greater than `k`. Pages that belong to one logical read should share a
snapshot (`iter_at`).

### 5.4 Snapshots

```rust
let snap: Snapshot = db.snapshot();          // registers a GC hold
snap.seqno();
db.get_at(b"k", &snap)?;
db.iter_at(lo, hi, reverse, &snap)?;
db.query_at("module", input, &snap)?;        // module bytes AND data at the snapshot
drop(snap);                                  // releases the hold

let s: SeqNo = db.seqno();                   // "now", without a hold
let snap = db.snapshot_at(s)?;               // Err(InvalidArgument) once GC passed s
```

Hold a snapshot for one operation. A snapshot held across a long job
stalls value-log reclamation and version GC for the whole store.

### 5.5 Transactions

```rust
let mut txn: Txn = db.begin();
txn.snapshot_seqno();
let cur = txn.get(b"k")?;                    // consistent read, no conflict check
let cur = txn.get_for_update(b"k")?;         // read + conflict check at commit
txn.put("k", "v")?; txn.delete("j")?;        // buffered until commit
txn.write_set_len();
for kv in txn.iter(lo, hi, reverse)? { }     // snapshot merged with this txn's writes
                                             // (overlay captured when iter() is called)
match txn.commit() {
    Ok(()) => {}
    Err(fluent31::Error::Conflict) => { /* nothing written; retry the whole thing */ }
    Err(e) => return Err(e),
}
// txn.rollback() or drop: discard
```

The retry loop:

```rust
loop {
    let mut txn = db.begin();
    let n = txn.get_for_update(b"seq")?.map(decode).unwrap_or(0);
    txn.put("seq", encode(n + 1))?;
    match txn.commit() {
        Ok(()) => break,
        Err(fluent31::Error::Conflict) => continue,
        Err(e) => return Err(e),
    }
}
```

Under `SyncMode::Always`, concurrent commits share fsyncs with plain
writers through group commit. Validation and application happen as one
atomic step against every other writer, including plain `db.put`.

### 5.6 Durability and maintenance

```rust
db.sync_wal()?;      // barrier: everything acked before this is durable on return
db.flush()?;         // freeze the memtable and wait until it is in tables
db.compact_all()?;   // compact until no trigger fires
db.gc_vlog()?;       // one value-log GC pass; Ok(Some(file_id)) if a file was retired
let s: DbStats = db.stats();
```

`DbStats` has `backend` (`"io_uring"` or `"std"`), `visible_seqno`,
`memtable_bytes`, `immutable_memtables`, `levels` as a `Vec<(runs, files,
bytes)>`, `vlog_files`, `vlog_retired` (retired files waiting on the
deletion gates), `discard_bytes` (value-log bytes known to be dead),
`cache_hits`, `cache_misses`, `commit_groups`, `commit_batches` (the
difference from `commit_groups` is how many fsyncs group commit saved)
and `wal_syncs`.

Compaction and value-log GC run on their own on background threads. The
manual calls exist for tests, benchmarks and "reclaim now".

### 5.7 Modules

```rust
db.install_module("name", &wasm_bytes)?;     // validates: exports memory + a role entry
db.uninstall_module("name")?;
let mods: Vec<ModuleInfo> = db.list_modules()?;   // name, size, content_hash

let out: Vec<u8> = db.query("name", input)?;             // requires the `query` export
let out = db.query_at("name", input, &snap)?;            // time travel: code and data at snap
let out = db.execute("name", input)?;                    // requires `execute`; OCC-retried

// one-shot: run bytes that are never installed (§6.8)
db.query_wasm(&wasm, input)?;  db.query_wasm_at(&wasm, input, &snap)?;
db.execute_wasm(&wasm, input)?;

db.module_entries("name")?;      // the exported role entries, e.g. ["execute", "describe"]
db.wasm_entries(&wasm)?;
db.describe_module("name")?;     // Option<Vec<u8>>: the descriptor JSON, if `describe` exists
db.describe_wasm(&wasm)?;
```

Module names use `[A-Za-z0-9._-]` and are at most 64 characters.
Installing over an existing name replaces it; invocations already in
flight finish on the old bytes. Module bytes live in the reserved
keyspace, so they are versioned, recovered and forked along with the
data. A guest that exits non-zero is `Error::GuestFailed { code, output
}`. A trap, a compile error or fuel exhaustion is `Error::Wasm`.

### 5.8 Triggers

```rust
let mode: TriggerMode = db.create_trigger("customerIndex", "customer_index",
                                          Some(b"orders/"), Some(b"orders0"))?;
// TriggerMode::Keys (module exports on_touch) or TriggerMode::Changes (on_apply)
db.delete_trigger("customerIndex")?;         // discards pending events
for t in db.list_triggers()? {
    // TriggerInfo { name, module, lo, hi (empty = open), mode, pending, last_error }
}
```

`None` bounds mean an open end. The module must already be installed.
Names follow the module-name rules and must be unique. `lo >= hi`, or a
bound longer than `max_key_size`, is rejected. One module may back many
triggers.

### 5.9 Forks and pins

```rust
let f: ForkInfo = db.fork("before-migration")?;
// ForkInfo { name, instance_id, created_unix_ms, last_seqno, path }
let clone = Db::open(&f.path, Options::default())?;      // live CoW clone

let p: PinInfo = db.pin("pre-import")?;                  // durable store-wide GC hold
// PinInfo { name, seqno, created_unix_ms }
let f = db.fork_at("rollback", p.seqno)?;                // cut exactly there
db.unpin("pre-import")?;
db.pins();                                               // Vec<PinInfo>, oldest first

let s = db.seqno();                                      // capture "now"
let a = db.fork_at("replica-a", s)?; let b = db.fork_at("replica-b", s)?;  // identical cuts

db.list_forks()?;  db.delete_fork("name")?;              // refused while the fork is open
fluent31::list_forks_at(Path::new("./data"))?;           // lock-free, works on a live store
fluent31::restore_to(&archive_path, &dest, Some("copy-name"))?;  // hard-link (or copy) into a fresh dir
```

Fork and pin names use `[A-Za-z0-9._-]`, at most 64 characters, with no
leading dot. `fork_at` accepts the head or any seqno at or above the GC
watermark, which means a recent one or one held by a pin or a live
snapshot. Anything older, or above the head, is `Error::InvalidArgument`.
`restore_to` refuses an existing `dest` and an archive that has already
been opened read-write (fork that live copy instead). `db.path()` returns
the directory the store was opened at. [§8](#8-forks-pins-clones) has
the details.

### 5.10 Journal

```rust
use fluent31::{Journal, JournalConfig, journal};

let db = Arc::new(Db::open(dir, opts)?);
let j = Journal::attach(db.clone(), "./journal")?;       // base snapshot now, deltas trail
let j = Journal::attach_with_config(db.clone(), dir, JournalConfig {
    rotate_bytes: 128 << 20,                             // rotate the log file at this size
    compact_when_deltas_exceed: Some(1.0),               // fresh base once deltas reach 1x the base; None = manual only
    compact_min_bytes: 64 << 20,                         // never auto-compact below this
})?;
j.stats();               // JournalStats { deltas_written, base_records_written, last_seqno,
                         //                rebaselines, compactions, files_pruned, last_error }
j.request_checkpoint();  // compact now
drop(j);                 // joins the drainer, final flush (the journal holds its own Arc<Db>)

let report: RebuildReport = journal::rebuild("./journal", "./rebuilt", Options::default())?;
// RebuildReport { source_instance, base_keys, deltas_applied, last_seqno }
```

The journal keeps the `Arc<Db>` for its lifetime. It records the user
keyspace only, so after a rebuild you reinstall modules and recreate
triggers. `rebuild` refuses a journal with a missing middle segment
(`Error::JournalGap`) or with mixed lineages. [§9.3](#93-the-journal) has
the rest.

### 5.11 Change stream

```rust
let mut sub: Subscription = db.subscribe(b"orders/", Some(b"orders0"))?;
sub.start_seqno();                                   // everything strictly above flows
loop {
    match sub.recv_timeout(Duration::from_secs(1))? {
        None => continue,                            // timeout
        Some(StreamEvent::Batch(entries)) => for e in entries {
            // StreamEntry { key, seqno, commit_seqno, kind: ValueKind::{Put,Delete}, value: Option<Vec<u8>> }
        },
        Some(StreamEvent::Lagged) => break,          // queue cap exceeded; re-subscribe
    }
}
```

Delivery is post-commit, seqno-ascending and gap-free past `start_seqno`,
and values arrive resolved. `seqno` is the op's own. `commit_seqno` is
the last seqno of the atomic commit the op belonged to, which is the one
state in which the op became visible; `snapshot_at(commit_seqno)` reads
it, whereas a per-op seqno inside a batch is not a readable state.
Value-log GC relocations re-put a live value through the write path, so
they show up as `Put` entries carrying the unchanged value, which is
harmless for any consumer. Drop subscriptions you stop consuming, since
an undropped one holds a GC pin. The journal, GraphQL subscriptions and
replication are all built on this stream.

The rest of the replication surface, `slice_manifest(lo, hi)`,
`read_table_chunk` and `read_vlog_chunk`, serves raw fragment and
value-log bytes to replicas ([§13](#13-replicas-and-edge-caches)).
Application code has no use for it.

### 5.12 Identity

```rust
if let Some(id) = db.identity() {
    id.name; id.instance_id; id.instance_hex(); id.parent;  // parent: Option<(InstanceId, cut_seqno)>
}
```

### 5.13 Errors

| `Error` | Meaning | What to do |
|---|---|---|
| `Io(e)` | an OS-level failure | inspect it; a hard IO failure in the write path degrades the store |
| `Corruption(msg)` | on-disk data failed validation | stop; restore from a fork or the journal |
| `InvalidArgument(msg)` | a reserved key, a bad name, an unknown module, a seqno below the watermark, and so on | fix the call |
| `Conflict` | the transaction lost first-committer-wins | retry the whole read-modify-write |
| `Closed` | the database was shut down | nothing |
| `Background(msg)` | a background thread or the write path failed; writes and maintenance refuse until reopened, reads keep serving | reopen |
| `Wasm(msg)` | a compile error, trap, fuel or memory exhaustion, or `wasm_enabled = false` | fix the module or raise the limit |
| `GuestFailed { code, output }` | the guest exited non-zero; `output` holds its message | an application-level failure |
| `ProvenanceMismatch(msg)` | replica data does not descend from the expected instance | re-attach from scratch (the `edge` driver does this itself) |
| `Gone(msg)` | a replicated file left the master's live version | re-pull the slice |
| `JournalGap(msg)` | a middle journal segment is missing | restore the segment and rebuild again |

## 6. WASM modules

[WASM.md](WASM.md) is the complete authoring manual and ABI spec; what
follows is the working summary.

### 6.1 Crate setup

```toml
# guests/<name>/Cargo.toml; add the crate to guests/Cargo.toml `members`
[package]
name = "my_module"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
fluent-guest = { path = "../../crates/fluent-guest" }
serde_json = "1"      # optional; works on wasm32-unknown-unknown
```

Build as in §2. There is no WASI, so don't use `std::time`, `std::env`,
`rand` or anything else that needs an operating system. Take entropy and
time as input.

### 6.2 Entry points

Put one attribute per role on a function of the shape `fn(T: FromInput)
-> Result<O: IntoOutput, Fail>`. The macro exports the entry point. `Ok`
becomes exit 0 with the encoded output, and `Err(Fail { code, message })`
becomes a non-zero exit with the message in the output buffer (a code of
0 is coerced to 1).

```rust
use fluent_guest::{Change, Fail};

#[fluent_guest::query]     fn view(input: Vec<u8>)        -> Result<String, Fail> { .. }
#[fluent_guest::execute]   fn write(input: String)        -> Result<Vec<u8>, Fail> { .. }
#[fluent_guest::on_touch]  fn index(keys: Vec<Vec<u8>>)   -> Result<(), Fail>     { .. }
#[fluent_guest::on_apply]  fn feed(changes: Vec<Change>)  -> Result<(), Fail>     { .. }
fluent_guest::fluent_describe!(r#"{ ... }"#);   // optional typed surface (§6.6)
```

`FromInput` is implemented for `Vec<u8>` (raw bytes), `String` (UTF-8,
with invalid input failing with code 3), `Vec<Vec<u8>>` (the keys-mode
input) and `Vec<Change>` (the changes-mode input). `IntoOutput` covers
`Vec<u8>`, `String` and `()`. The annotated function must not carry the
name of the export it generates: a `#[query]` function named `query` is a
duplicate definition. A module may export any combination of roles.
`Fail` also converts from `String` and `&str` with code 1, so `?` works
on string errors.

The raw layer is still there for modules that want to speak exit codes
directly: `fluent_query!(f)`, `fluent_execute!(f)`,
`fluent_on_touch!(f)` and `fluent_on_apply!(f)` export an `fn() -> i32`.
Pair them with `fluent_guest::trigger_keys()` or
`fluent_guest::changes()` to read trigger input, or with
`parse_trigger_keys(&[u8])` and `parse_changes(&[u8])` on bytes you
already hold.

### 6.3 SDK functions

```rust
fluent_guest::input() -> Vec<u8>                      // the input blob
fluent_guest::output(&[u8])                           // append to the output
fluent_guest::log(&str)                               // debug log (a `debug` event under target fluent31::wasm::guest)
fluent_guest::get(&[u8]) -> Option<Vec<u8>>
fluent_guest::get_for_update(&[u8]) -> Result<Option<Vec<u8>>, i32>   // Err = errno (EROFS in a query)
fluent_guest::put(&[u8], &[u8]) -> Result<(), i32>
fluent_guest::delete(&[u8]) -> Result<(), i32>
fluent_guest::scan(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Scan, i32>
fluent_guest::scan_rev(lo, hi) -> Result<Scan, i32>
fluent_guest::scan_prefix(&[u8]) -> Result<Scan, i32>
// Scan: Iterator<Item = (Vec<u8>, Vec<u8>)>; .skip_pending() skips an entry too big to buffer
fluent_guest::errno::{NOT_FOUND, EROFS, EINVAL, ENOSPC, EBADF, ELIMIT, EIO}   // -1 -2 -3 -4 -5 -6 -8 (-7 unused)

enum Change { Put { seqno: u64, key: Vec<u8>, value: Option<Vec<u8>> },   // value None = elided
              Delete { seqno: u64, key: Vec<u8> } }
change.seqno(); change.key();   // seqno: the op's own, unique and increasing across the feed
```

Reads see the invocation's snapshot, plus the transaction's own buffered
writes in an executor. Big values can be read in chunks through the raw
ABI, where `get` returns the full length and copies from an offset. The
SDK's `get` fetches whole values.

### 6.4 Executor semantics

- Each attempt is a fresh transaction. Exit 0 commits. Any other exit
  aborts and surfaces as `GuestFailed { code, output }`.
- On a commit conflict the whole attempt is discarded and re-run against
  a fresh snapshot, with fresh memory, fuel and output, until
  `execute_retries` attempts (3, first run included) are spent. Then the
  call returns `Conflict`. Your code may run several times per call, so
  no side channels and no "did I already run" state anywhere but the
  data.
- Use `get_for_update` on every key you base a write on.
- Use distinct non-zero exit codes per failure class and put the message
  in the output. Present-but-malformed state is corruption: fail loudly,
  never default.
- Use checked arithmetic. An executor that overflows corrupts durable
  state.
- `EIO` from any host call means the engine failed. The invocation fails
  on the host side even if you exit 0.

### 6.5 Limits and errnos

| Limit (`Options`) | Default | On breach |
|---|---|---|
| `wasm_fuel` | 1e9 | trap, `Error::Wasm` |
| `wasm_memory_limit` | 64 MiB | `memory.grow` fails |
| `max_wasm_input` | 64 MiB | `InvalidArgument` before execution |
| `max_wasm_output` | 32 MiB | `output_write` returns `ENOSPC` |
| `max_wasm_log` | 1 MiB | `log` returns `ENOSPC` |
| `max_wasm_scans` | 64 | `scan_open` returns `ELIMIT` |
| `max_txn_write_bytes` | 256 MiB | `put` returns `ENOSPC` |

The errnos are `NOT_FOUND -1`, `EROFS -2` (a write in a query), `EINVAL
-3` (a reserved, empty or oversized key, an oversized value, bad flags),
`ENOSPC -4`, `EBADF -5` (a bad scan handle), `ELIMIT -6` and `EIO -8`.
Memory-safety violations trap. On reserved keys, `get`, `get_for_update`,
`put` and `delete` return `EINVAL`, and scans clamp to the user keyspace.

### 6.6 Typed GraphQL surface (`describe`)

A module that exports `describe` becomes its own typed root field as soon
as it is installed on a server. `kind: "query"` lands on `Query`, `kind:
"execute"` on `Mutation`, and a `feed` declaration on `Subscription`. The
schema is rebuilt and hot-swapped on every install and uninstall, at
server start, and on `mutation { reloadSchema }`.

```rust
fluent_guest::fluent_describe!(r#"{
  "kind": "execute",
  "description": "docs for the root field",
  "args": [{"name": "customer", "type": "String!"},
           {"name": "amountCents", "type": "U64!"},
           {"name": "note", "type": "String"}],
  "types": [{"name": "PlacedOrder", "fields": [
    {"name": "id", "type": "U64!"},
    {"name": "customerTotalCents", "type": "U64!"}]}],
  "output": "PlacedOrder!",
  "feed": {"prefix": "feed/", "event": "OrderFeedEntry!"}   // optional; needs on_apply
}"#);
```

- Scalars are `String`, `Int` (32-bit), `Float`, `Boolean`, `U64` (a
  string on the wire, with numbers accepted on input) and `Json`
  (opaque). At most one list level. `args` reference scalars only;
  `output` and `types` may reference declared types.
- Shape: at least one of `kind` and `feed`. `output` is required with
  `kind` and rejected without it. `args` require `kind`. `feed.prefix` is
  non-empty. Every declaration must be backed by its export (`"query"` by
  `query`, `"execute"` by `execute`, `feed` by `on_apply`) or the install
  is rejected.
- Input: with `args`, the entry receives one JSON object holding every
  declared arg, with omitted optional args as `null` and `U64` as a
  number. Without `args`, the field takes an optional `input: BytesInput`
  and the entry receives raw bytes.
- Output is parsed as JSON and validated against `output`. Undeclared
  keys are dropped. Missing declared fields become `null`, which is an
  error if the field is `!`. A violation is `OUTPUT_SCHEMA_VIOLATION`, and
  for executors it carries `committed: true` because the transaction has
  already committed. Don't blind-retry those.
- Naming: the module name must be a valid GraphQL name
  (`[_A-Za-z][_0-9A-Za-z]*`, no leading `__`) and must not shadow a
  built-in root field (`changes`, `get`, `scan`, `wasm`, `wasmOnce`,
  `modules`, `stats`, `forks`, `pins`, `triggers`, `snapshotSeqno`,
  `seqno`, `put`, `delete`, `writeBatch`, `wasmExecute`,
  `wasmExecuteOnce`, `installModule`, `uninstallModule`, `fork`,
  `deleteFork`, `pin`, `unpin`, `createTrigger`, `deleteTrigger`,
  `flush`, `compactAll`, `gcVlog`, `reloadSchema`, `syncWal`). Type
  names must not be reserved (`Query`,
  `Mutation`, `Subscription`, `Bytes`, `BytesInput`, `U64`, `Json`,
  `Pair`, `ScanPage`, `ChangeEvent`, `ChangeKind`, `Module`, `Fork`,
  `Pin`, `Trigger`, `GcResult`, `LevelStats`, `Stats`, `WriteOp`, `PutOp`,
  `String`, `Int`, `Float`, `Boolean`, `ID`) and must not collide with a
  type another installed module declares, so prefix yours (`PlacedOrder`,
  not `Order`). A `feed` module also claims `<Module>Event`. The limits
  are 32 types, 64 fields per type, 16 args, and a 64 KiB descriptor.
- Where this is enforced: the server's `installModule` runs `describe`
  and rejects a module whose descriptor breaks these rules. The engine's
  `install_module` (the Rust API and the shell) checks only the exports,
  so a module installed that way with a bad descriptor ends up degraded.
  It is callable through the generic byte fields, has no typed field, and
  `modules { typed schemaError }` says why.
- Other surfaces: the GraphQL layer builds the JSON arg object. Through
  the shell or `db.execute`, a typed module receives whatever bytes you
  pass, so send the same JSON object yourself.

Install and confirm:

```graphql
mutation Install($w: BytesInput!) { installModule(name: "placeOrder", wasm: $w) { typed schemaError } }
# variables: {"w": {"base64": "<base64 of place_order.wasm>"}}   ->  typed: true, schemaError: null
```

### 6.7 Invoking

| Surface | Query | Execute |
|---|---|---|
| Engine | `db.query(name, input)` | `db.execute(name, input)` |
| Shell | `query NAME [INPUT]` | `exec NAME [INPUT]` |
| GraphQL (generic) | `wasm(module:, input:) { text base64 hex len }` | `wasmExecute(module:, input:) { .. }` |
| GraphQL (typed) | `<module>(args) { fields }` on `Query` | `<module>(args) { fields }` on `Mutation` |

`installModule` also accepts WAT text (`wasm: {text: "(module ...)"}`).

### 6.8 One-shot: migrations without install

You can run module bytes that are never stored:

| Surface | Query | Execute |
|---|---|---|
| Engine | `db.query_wasm(wasm, input)`, `query_wasm_at(.., &snap)` | `db.execute_wasm(wasm, input)` |
| Shell | `queryonce FILE.wasm [INPUT]` | `execonce FILE.wasm [INPUT]` |
| GraphQL | `wasmOnce(wasm:, input:)` | `wasmExecuteOnce(wasm:, input:)` |

Same ABI, SDK, limits and retry loop, except that the code is pinned for
all attempts. Nothing is listed, cached or replicated; an executor's
committed writes are the only trace. Triggers still fire on those writes.
`describe` is ignored, so there is no typed field, and a trigger can only
bind to an installed module.

The migration shape is to walk a prefix, skip already-migrated records
and rewrite the rest, all in one transaction: atomic, invisible until
commit, serialized by OCC. It has to fit `max_txn_write_bytes` and
`wasm_fuel`. Past that, shard by cursor (start key in, next start key
out) and drive the loop from the caller. Rehearse on a fork first, and
keep the script in your repo, because that is the audit trail. The full
recipe is in [WASM.md §10](WASM.md).

### 6.9 Reference modules (`guests/`)

| Module | Role | Shows |
|---|---|---|
| `agg` | query (untyped) | prefix count/sum/min/max over u64 LE values; raw bytes in and out |
| `transfer` | execute (untyped) | a balance transfer with `get_for_update`, OCC retries, an exit code per failure |
| `place_order` | execute (typed) | id allocation, a record and a stats fold in one transaction; input validation |
| `top_customers` | query (typed) | typed list output, snapshot aggregation, limit clamping |
| `claim` | execute | a UNIQUE constraint: exactly one winner under concurrency, idempotent re-claim |
| `customer_index` | keys-mode trigger | a secondary index that reconciles against current state, with a back-pointer for updates and deletes |
| `order_feed` | changes-mode trigger plus feed | an ordered changefeed materialized as keys, subscribable live, with an `elided` flag for oversized values |
| `dynamic_index` | changes-mode trigger | index specs stored as keys, backfill on spec write, teardown on delete; one module, two triggers |
| `live_stats` | changes-mode trigger | an always-fresh `GROUP BY` folded exactly once per change; the demo checks it against a full recount |
| `cascade_delete` | changes-mode trigger | a parent delete sweeps its subtree; no-stacking stops loops |

Self-asserting walkthroughs, which build the guests first:

```sh
cargo run -p fluent31 --example dynamic_index
cargo run -p fluent31 --example live_stats
cargo run -p fluent31 --example cascade_delete
cargo run -p fluent31 --example claim
scripts/demo-orders.sh      # against a running server: installs the typed pair, seeds orders, ranks customers
```

### 6.10 Authoring checklist

1. Pick the role or roles and export the matching entries.
2. Define the keyspace. Validate any input that becomes a key segment: no
   `/`, non-empty, bounded length.
3. `get_for_update` on every read-modify-write key.
4. Distinct exit codes with the message in the output. Malformed state
   fails loudly.
5. Checked arithmetic.
6. A static descriptor with prefixed type names.
7. Build with `--release`, install, and confirm `typed: true, schemaError:
   null`.
8. Test the happy path, each failure exit, concurrency for executors, and
   a restart (the typed field must reappear).

## 7. Triggers: indexes, views, feeds

### 7.1 Registering

```rust
db.create_trigger("name", "module", Some(b"orders/"), Some(b"orders0"))?;   // Rust
```
```
mktrig NAME MODULE [LO|-] [HI|-]            # shell; - = open end
```
```graphql
mutation { createTrigger(name: "idx", module: "customer_index",
                         lo: {text: "orders/"}, hi: {text: "orders0"}) }
query { triggers { name module lo { text } hi { text } mode pending lastError } }
mutation { deleteTrigger(name: "idx") }     # discards pending events
```

The mode is detected from the module's exports at registration and fixed
for the trigger's life. `on_apply` present means changes mode; otherwise
`on_touch` means keys mode; neither is rejected. Replacing the module's
bytes later does not change the mode. A changes-mode trigger whose module
lost `on_apply` fails its drains loudly (`lastError`) and holds its
events.

Registration does not backfill. Keys already in the range fire no events.
To index existing data, have the module scan on demand (as
`guests/dynamic_index` does when a spec key is written) or re-put the
range with a one-shot executor. Each trigger has its own queue, and one
runner thread drains them independently. There is no ordering between
triggers, and overlapping ranges each get their own copy of an event.

### 7.2 Keys mode (`on_touch`)

The input is the touched keys, up to `trigger_batch` per invocation. No
values, no op kind, no order. Re-touches of one key coalesce into one
pending event while a backlog exists.

The contract: an event means "reconcile this key". Read the key at your
snapshot. If it is present, upsert your derived state; if it is absent,
remove it. Written this way the module converges under replay, coalescing
and reordering. Updates and deletes need your own back-pointer (say
`idx/order/<id>` pointing at the customer) to find the stale entry,
because the event carries no old value. `guests/customer_index` is the
reference.

### 7.3 Changes mode (`on_apply`)

The input is the ordered list of committed changes, one per op, up to
`trigger_batch` per invocation. Each carries `seqno` (the op's own seqno,
assigned at commit, unique and strictly increasing across the feed), the
kind (put, delete, or put with the value elided), the key, and the value
inline up to `trigger_inline_value` (64 KiB). Above that, `value` is
`None`, and you read the key knowing the read is current state, possibly
newer than the change.

The contract: one event per op, in commit order, never coalesced. A key
written three times yields three changes. Filter in code; the range is
only the coarse cut. Derive output keys from the seqno
(`feed/<seqno zero-padded>`) so that replays overwrite instead of
duplicating. Old values are still your job. A hot key grows the backlog
where keys mode would coalesce it. The references are
`guests/order_feed`, `guests/live_stats`, `guests/dynamic_index` and
`guests/cascade_delete`.

### 7.4 Delivery guarantees (both modes)

- Durable capture. Events commit in the same atomic batch as the write
  that caused them. A write that survives a crash fires its trigger after
  recovery; one that doesn't, doesn't.
- At-least-once invocation, exactly-once effects. Consumed events are
  deleted inside the module's own transaction. A crash or a conflict
  re-runs the whole attempt, and your writes and the events' consumption
  are inseparable.
- No stacking. Writes made by a trigger invocation never generate events,
  for any trigger. No chains, no loops.
- Asynchronous. Derived state trails the base data by the backlog.
  Nothing waits for a trigger. Watch `pending` to see how far behind it
  is.
- Failure holds, never drops. A failing module (a guest error, a missing
  module, conflict exhaustion) leaves the batch queued. The runner backs
  off per trigger, starting at 100 ms and doubling up to a 6.4 s
  ceiling.
  `list_triggers` and `triggers { pending lastError }` show the depth and
  the reason. Fix the module by reinstalling it and the backlog drains.
- Batch bounds. A drain hands the module at most `trigger_batch` events
  and never more than `max_wasm_input` bytes. Inlined values are clamped
  so that every event fits. A single keys-mode event that cannot fit (a
  key near `max_key_size` under a tiny `max_wasm_input`) fails the drain
  with `InvalidArgument`, visible in `lastError`.
- Trigger definitions and queues live in the reserved keyspace, so they
  are versioned, recovered and forked with everything else. A store
  rebuilt from the journal has neither, so recreate the triggers.

Every writer fires triggers: plain puts, batches, transactions,
executors, one-shot executors, and every network surface. Trigger
invocations themselves, value-log GC relocations, and writes made while
`wasm_enabled = false` do not.

### 7.5 Waiting for a drain

There is no synchronous "run the triggers now". Poll `list_triggers()`
until `pending == 0` for the triggers you care about and `last_error` is
`None`. `crates/fluent31/examples/util/mod.rs::drain` is the reference.

## 8. Forks, pins, clones

### 8.1 What a fork is

A fork is a named, consistent branch of the whole database, published as
a complete database directory under `<dir>/archive/<name>/`. Tables and
sealed value-log files are immutable, so the fork hard-links them.
Creation cost is proportional to the number of files, plus one bounded
copy of the still-growing value-log head (at most `vlog_file_size`).
Shared bytes exist once on disk. `du` on the archive re-counts shared
inodes, so the apparent size is not the added size. Real divergence
accrues only as parent and child compact away from the shared base.

A fork exists completely or not at all. It is built in a temporary
directory, fsynced and published by a single rename, and a crashed build
is swept at the next open.

### 8.2 Cutting

| Call | Cut | Cost |
|---|---|---|
| `fork(name)` | the current flushed head | a memtable flush plus hard links |
| `fork_at(name, seqno)` | that exact seqno | the same, plus the table files are rewritten to the cut (values stay hard-linked) |

`fork_at` needs a point that is still materializable: the head, a seqno
captured moments ago with `db.seqno()`, or one held by `pin(name)`. A pin
is a durable, store-wide GC hold recorded in the manifest. It survives
restarts and costs retention until `unpin`. Seqnos below the watermark
are refused.

Live readers and writers keep running during a fork. What the store pays
is one memtable flush, a brief hold of the manifest lock (structural
installs pause, traffic does not), and, because the cut is a registered
snapshot for the build's duration, GC held at the cut and value-log
deletions deferred until the build finishes.

### 8.3 Using a fork

- Open equals activate. `Db::open(fork.path, ..)` gives you a live,
  writable, copy-on-write clone. New writes land in its own files and its
  compactions unlink only its own links. The parent is untouched.
- `restore_to(archive, dest, new_name)` hard-links the archive into a
  fresh directory, or copies it when `dest` is on another filesystem, so
  the archived cut stays pristine. `new_name` is required for forks of a
  named store, since each copy mints its own identity, and optional for
  unnamed ones.
- `delete_fork(name)` refuses while the fork is open as a database.
- `list_forks_at(dir)` reads `archive/*/fork.meta` without taking a lock,
  so it works on a store another process has open.
- Under the server, every fork is an instance at `/graphql/<instanceId>`
  with the same full surface, including its own forks
  ([§12.7](#127-instances)).

### 8.4 Expectations

Forks branch; they do not follow. A fork contains exactly the history up
to its cut and nothing the parent commits afterwards. They are priced for
a handful of deliberate cuts (a pre-migration anchor, a staging clone, a
rollback point), not for per-document versioning
([§4.3](#43-the-consistency-contract)). Pins hold GC for the whole store.

### 8.5 Rolling back the primary

There is no in-place restore. A rollback swaps directories.

1. Before the risky change, `fork("pre-migration")`, or `pin` now and
   `fork_at` later. Rehearse the change on the fork's own instance.
2. To roll back, stop the process. Then either open the fork directly as
   the new primary (`Db::open("<dir>/archive/pre-migration")`; its first
   read-write open fixes its identity under the fork's name), or keep the
   archive pristine with `restore_to(archive, "<new-dir>",
   Some("prod-2"))` and start on `<new-dir>`. Pass no `store_name` on
   later opens, since the name is persisted.
3. The rolled-back primary has a new instance id. Every replica notices on
   its next connection and re-attaches from scratch; nothing else needs
   telling. Any stored seqnos are meaningless across the swap
   ([§4.3](#43-the-consistency-contract)).
4. Delete the abandoned directory when you are sure.

The shell and GraphQL expose `fork` and `pin` but not restore. Step 2 is
a filesystem operation or the Rust call.

## 9. Durability, recovery, journal

### 9.1 What survives a crash

Under `SyncMode::Always`, every acked write survives. A value-log payload
is synced before the WAL record that points at it, so a durable pointer
never precedes its data. Under `Periodic`, everything up to the last
timer tick or `sync_wal` survives. Under `Never`, whatever the OS had
flushed.

In every mode the store reopens consistent. The WAL's torn tail is
truncated, tables are self-describing and synced before the manifest
references them, and the manifest flips atomically. Corruption in a
sealed file is a hard `Corruption` error, never silent.

The test suite proves this with a SIGKILLed child process
(`crash_recovery`), a fault-injecting IO backend (`fault_injection`,
which shows a failed fsync is never a false ack) and a byte-mutation
sweep (`corruption_fuzz`, which shows no on-disk byte can panic the
reader).

### 9.2 Degraded state

A hard IO failure in the write path or a background thread failure sets a
store-wide error. After that, writes, `flush`, `sync_wal`, `pin` and
subscriptions return `Error::Background`, while `get`, `iter` and
snapshots keep serving what is there. Reopen the store; recovery brings
it back to the last durable state.

### 9.3 The journal

The store's own WAL and manifest are its durability. The journal is for
the day that is not enough: a bad disk block, a truncated file, a lost
directory. It is off unless you attach it, and it never sits on the
commit path.

At attach it writes a base snapshot of the user keyspace, then trails the
change stream on a background thread, appending each mutation to
`journal-*.log` segments that rotate at `rotate_bytes`. Once the delta
bytes written since the last base exceed `compact_when_deltas_exceed`
times that base's size, and also exceed `compact_min_bytes`, it writes a
fresh base and prunes the superseded segments, so disk stays near the
live set plus one window of recent deltas. If the consumer ever lags past `sub_queue_bytes`, it heals by
writing a new base. The log header records the source instance id, and a
different store's journal in the same directory is refused.

How to attach it:

| Surface | How |
|---|---|
| Rust | `Journal::attach(db, dir)` or `attach_with_config` ([§5.10](#510-journal)) |
| `fluent-graphql` | `--journal DIR [--journal-rotate-bytes N] [--journal-compact-when-deltas-exceed R\|off] [--journal-compact-min-bytes N]` |
| `fluent-server` | a `[journal]` section in the TOML config, with `dir` required |

How to rebuild from it:

```sh
fluent-cli journal-rebuild <journal-dir> <dest-dir>
# prints: source instance, base keys, deltas applied, last seqno
```

or `fluent31::journal::rebuild(journal_dir, dest, opts)`, where `opts`
are the rebuilt store's `Options` (give it a `store_name` for a fresh
root identity). `dest` must be a fresh directory. The rebuilt store holds
all user data as of the journal's last durable record, as a new lineage:
seqnos are renumbered, the instance id is fresh, and modules, triggers,
pins and forks are not restored, so redeploy them. A missing middle
segment is refused (`JournalGap`), never rebuilt around.

The tail is approximate in both directions. The journal's last few
unsynced records can be lost. And under `Periodic` or `Never`, the
journal, which is fed from the in-memory commit stream, can hold writes
the crashed store lost, so a rebuild is slightly ahead of what the store
would have recovered. Both are acceptable. You reach for the journal only
when the store itself is gone, and the rebuild replaces it.

### 9.4 Backups

- For a consistent snapshot on the same filesystem, `fork(name)`. It is
  cheap, instant, consistent and hard-linked.
- To copy elsewhere, copy `archive/<name>/`, which is a plain directory
  tree (the hard links copy as full files), or `restore_to` into a mount
  point.
- For continuous off-box protection, ship the journal directory's
  segments. A reassembled journal is verified for contiguity at rebuild.

Never delete `wal-*.log`, `MANIFEST-*`, `CURRENT` or `LOCK` by hand.

## 10. The shell

```
fluent-cli <db-dir> [--std|--uring] [--nosync] [--sync-every <ms>]
fluent-cli journal-rebuild <journal-dir> <dest-dir>
```

`--std` and `--uring` force the IO backend. `--nosync` is
`SyncMode::Never` and `--sync-every` is `Periodic`. Every command prints
its wall-clock latency. Byte arguments are plain UTF-8 or `hex:DEADBEEF`.
Output shows printable bytes quoted and everything else as `hex:`.

| Group | Commands |
|---|---|
| kv | `get K`, `put K V`, `del K`, `scan [LO\|-] [HI\|-] [--rev] [--limit N]` (default limit 50), `count [LO] [HI]` |
| txn | `begin`, `tget K`, `tlock K` (get_for_update), `tput K V`, `tdel K`, `commit`, `abort`. The prompt shows `(txn)` while one is open. |
| snapshots | `snap` (prints an id), `snaps`, `sget ID K`, `snapdrop ID` |
| wasm | `install NAME FILE.wasm`, `modules`, `uninstall NAME`, `query NAME [INPUT]`, `exec NAME [INPUT]`, `queryonce FILE.wasm [INPUT]`, `execonce FILE.wasm [INPUT]` |
| triggers | `mktrig NAME MODULE [LO\|-] [HI\|-]`, `deltrig NAME`, `triggers` |
| forks | `fork NAME [AT]`, `forks`, `delfork NAME` |
| pins | `pin NAME`, `pins`, `unpin NAME`, `seqno` |
| admin | `flush`, `compact`, `gc`, `stats`, `help`, `exit` |

`count` takes the same `-`, `--rev` and `--limit` syntax as `scan`, and
`quit` is the same as `exit`. Values longer than 160 bytes print
truncated with their length. A guest failure prints `guest exited with
code N, output ...`. A transaction conflict prints a `CONFLICT (first
committer wins)` line; the transaction is rolled back.

## 11. Server mode

```
fluent-server <db-dir> [--config FILE] [--store-name NAME]
              [--graphql ADDR:PORT] [--replication ADDR:PORT]
              [--sync always|never|periodic:<ms>] [--max-body-bytes N]
```

One process, one `Db`, two planes:

| Plane | Default | Purpose |
|---|---|---|
| graphql | `127.0.0.1:8317` | typed and admin operations, GraphiQL at `/`, subscriptions over graphql-ws, fork instances at `/graphql/<instanceId>` |
| replication | `127.0.0.1:8428` | the join point for replicas and edge caches ([§13](#13-replicas-and-edge-caches)); opens only on a named store |

The store directory is flocked, so the planes cannot be split across
processes. Server mode is how they share one handle. `--store-name` is
persisted in the store, so pass it once. Without a name, graphql serves
and the join point stays closed; the startup banner says so.

On the first SIGINT or SIGTERM the server stops accepting and drains
in-flight GraphQL requests, then the process exits and open replication
connections drop (the WAL keeps the store consistent). A second signal
exits immediately. The banner prints each bound address and, for a named
store, its name and instance id. If the engine degrades
(`Error::Background`), GraphQL answers `BACKGROUND` and replication
answers `ERR`; restart the process.

Every plane defaults to loopback and speaks plain TCP or HTTP with no
authentication. To expose one, bind it explicitly and put TLS and access
control in front: a reverse proxy for GraphQL, a network boundary for
replication.

### 11.1 Config file

`--config server.toml`. The top-level keys, `[listen]` and
`[graphql].max-body-bytes` mirror the flags, and an explicit flag wins.
The rest is file-only. Unknown keys are an error. Every key, with its
default:

```toml
dir = "./data"
store-name = "prod"
sync = "always"               # always | never | periodic:<ms>

[listen]
graphql = "127.0.0.1:8317"
replication = "127.0.0.1:8428"

[graphql]
max-body-bytes = 33554432     # 32 MiB request body cap
fork-max-open = 8             # open fork instances beyond the primary (LRU past this)
fork-idle-ttl-secs = 300      # idle instances close after this

[replication]
max-frame-bytes = 1048576
ping-every-ms = 2000

[journal]                     # present = attached; absent = off
dir = "./journal"             # required once the section exists
rotate-bytes = 134217728
compact-when-deltas-exceed = 1.0
compact-min-bytes = 67108864

[engine]                      # every fluent31::Options tunable (§5.1), kebab-case
create-if-missing = true
wasm-enabled = true
io-backend = "auto"           # auto | uring | std
compression = "none"          # none | lz4
memtable-size = 8388608
max-immutable-memtables = 2
block-size = 8192
bloom-bits-per-key = 10
block-cache-size = 67108864
l0-compaction-trigger = 4
tier-width = 4
max-levels = 7
l0-stall-trigger = 12
target-file-size = 67108864
value-threshold = 4096
vlog-file-size = 134217728
vlog-gc-ratio = 0.5
max-key-size = 16384
max-value-size = 268435456
max-txn-write-bytes = 268435456
sub-queue-bytes = 8388608
wasm-fuel = 1000000000
wasm-memory-limit = 67108864
execute-retries = 3
max-wasm-input = 67108864
max-wasm-output = 33554432
max-wasm-log = 1048576
max-wasm-scans = 64
wasm-module-cache = 32
trigger-batch = 512
trigger-inline-value = 65536
```

### 11.2 Standalone planes

Each plane also runs on its own, with the same defaults:

```sh
fluent-graphql <db-dir> [--listen ADDR:PORT] [--sync ..] [--max-body-bytes N] [--journal DIR ...]
fluent-graphql --print-schema                 # the base SDL (built-ins only)
fluent-replication <db-dir> [--store-name NAME] [--listen ADDR:PORT]   # the name is needed once, then persisted
```

Only one of them can hold the store at a time. `fluent-graphql` refuses
`--journal-*` tuning flags without `--journal DIR`.

### 11.3 Embedding the server

```rust
use fluent_server::{Server, ServerConfig};

let db = Arc::new(Db::open(&dir, opts.clone())?);
let server = Server::start(db.clone(), &dir, opts, ServerConfig::default()).await?;
server.graphql_addr; server.replication_addr;   // replication_addr: None when unnamed
server.db();
server.shutdown().await;
```

`ServerConfig` holds `graphql_addr`, `replication_addr`,
`max_body_bytes`, `registry: RegistryConfig { max_open, idle_ttl }` and
`replication: ReplServerConfig { max_frame, ping_every }`. Nothing is
served unless
every bind succeeds; failures come back as `StartError::{Engine, Bind}`.
The TOML loader is public too (`FileConfig::load`, `overlay`,
`server_config`, `engine_options`, `parse_sync`).

For the GraphQL plane alone: `SchemaManager::new(db)`, then
`InstanceRegistry::new(mgr, &dir, Options { store_name: None, ..opts },
RegistryConfig::default())`, then `fluent_graphql::router(registry,
max_body)`, which is an `axum::Router`. Forks carry their own identity,
so passing the primary's name to the registry makes every fork open
fail. Call `registry.evict_idle()` periodically; the binaries tick every
60 seconds. `SchemaManager::{execute, execute_stream, schema}` run
operations in-process, and `base_sdl()` is the built-in schema text.

For replication: `ReplServer::new(db, cfg)?` fails with
`InvalidArgument` on an unnamed store; then `.serve(listener)`, and
`.identity()` is the served instance.

## 12. GraphQL plane

The endpoints are `POST /graphql` for the primary and `POST
/graphql/<instanceId>` for a fork. A `GET` on either serves GraphiQL. A
`GET` with a WebSocket upgrade and the graphql-ws subprotocol serves
subscriptions.

### 12.1 Encoding

Keys and values are raw bytes. Inputs take exactly one of `{text}`,
`{base64}` or `{hex}` (`BytesInput`, a `@oneOf` input). Outputs expose
`text` (null if not UTF-8), `base64`, `hex` and `len` (`Bytes`).

`U64` is a string-encoded 64-bit unsigned scalar used for seqnos,
timestamps and byte totals. Inputs also accept numbers. `Json` is opaque
passthrough, used by typed modules only.

### 12.2 Query

| Field | Notes |
|---|---|
| `get(key: BytesInput!): Bytes` | null when absent |
| `scan(lo, hi, prefix, after, reverse, limit): ScanPage` | `[lo, hi)` or `prefix`; `limit` defaults to 100 and tops out at 10000; `ScanPage { pairs { key value } hasMore nextAfter }`; pass `nextAfter` back as `after` |
| `wasm(module: String!, input: BytesInput): Bytes` | a generic query module call |
| `wasmOnce(wasm: BytesInput!, input: BytesInput): Bytes` | a one-shot query, binary or WAT |
| `modules: [Module!]` | `{ name size typed schemaError }`, current state |
| `stats: Stats` | the `DbStats` fields in camelCase (`visibleSeqno`, `levels { runs tables bytes }`, `commitGroups`, and so on) |
| `forks: [Fork!]` | `{ name instanceId createdUnixMs lastSeqno path }` |
| `pins: [Pin!]` | `{ name seqno createdUnixMs }`, oldest first |
| `triggers: [Trigger!]` | `{ name module lo hi mode pending lastError }` |
| `snapshotSeqno: U64` | the seqno this operation reads at |
| `seqno: U64!` | the current visible seqno, not snapshot-bound; pass it to `fork(at:)` to cut "now" deterministically |
| `<module>(...)` | every installed typed `kind: "query"` module |

Every read field of one query operation runs at one pinned snapshot.
`stats`, `modules`, `forks`, `pins`, `triggers` and `seqno` report
current state.

### 12.3 Mutation

| Field | Notes |
|---|---|
| `put(key, value): Boolean`, `delete(key): Boolean` | |
| `writeBatch(ops: [WriteOp!]!): Int` | `WriteOp` is `@oneOf { put: {key value} \| delete: BytesInput }`; atomic; returns the number of ops applied |
| `wasmExecute(module, input): Bytes` | a generic executor call |
| `wasmExecuteOnce(wasm, input): Bytes` | a one-shot executor |
| `installModule(name, wasm): Module` | binary (`base64`) or WAT (`text`); hot-swaps the schema |
| `uninstallModule(name): Boolean` | |
| `createTrigger(name, module, lo, hi): Boolean`, `deleteTrigger(name): Boolean` | |
| `reloadSchema: Boolean` | re-describes everything; the resync path after out-of-band installs |
| `fork(name, at: U64): Fork` | omit `at` for the head; returns the new `instanceId` |
| `deleteFork(name): Boolean` | refused while in use |
| `pin(name): Pin`, `unpin(name): Boolean` | |
| `syncWal: Boolean` | a durability barrier, the companion to `--sync periodic` |
| `flush: Boolean`, `compactAll: Boolean`, `gcVlog: GcResult { retired }` | |
| `<module>(...)` | every installed typed `kind: "execute"` module |

Mutation fields run serially in document order, each as an independent
atomic write, and executor fields each run their own transaction. A
document is never one transaction. `fork(at:)` takes the `U64` as a
string or a number. `wasmExecuteOnce`, like `installModule` and
`wasmOnce`, accepts WAT text. `deleteFork` first closes the server's own
idle instance of that fork.

### 12.4 Subscription

```graphql
subscription {                      # raw plane: no module needed
  changes(lo: {text: "orders/"}, hi: {text: "orders0"}) {
    kind seqno commitSeqno key { text } value { text }
    query { snapshotSeqno get(key: {text: "orders/count"}) { text } }
  }
}
subscription {                      # typed plane: a module with a `feed` descriptor
  orderFeed { kind seqno commitSeqno key { text } event { seqno op id record elided } query { .. } }
}
```

- `kind` is `ATTACHED`, `PUT` or `DELETE`. The stream opens with one
  `ATTACHED` marker with no key, value or event. Its `seqno` is the
  attach boundary: everything at or below it is readable through the
  marker's `query`, and everything above arrives on the stream. Gap-free,
  with no overlap.
- Every item carries `query: Query!`, the full Query root pinned at the
  item's `commitSeqno`, which is the exact state in which the op became
  visible. The ops of one atomic commit share a `commitSeqno`.
- Typed feeds deliver puts only, so feed GC deletes are invisible. `event`
  is the written value validated against the declared event type.
- A consumer that falls behind `sub_queue_bytes` is cut off with a
  "lagged" error. Re-subscribe and re-scan from the new boundary. Items
  hold snapshots, so consume promptly. A server restart ends every
  subscription; nothing about them is persisted.
- The raw plane also shows value-log GC relocations as a `PUT` of an
  unchanged value ([§5.11](#511-change-stream)). Typed feeds don't,
  because feed keys are written by the module.
- The idiom: history is a `scan` of the feed range, the latest value is a
  `get`, and live is a subscription. A disconnected client misses nothing
  durable as long as the module materializes its feed.

### 12.5 Errors

Engine failures map to `errors[].extensions.code`: `IO`, `CORRUPTION`,
`INVALID_ARGUMENT`, `CONFLICT` (retries exhausted), `CLOSED`,
`BACKGROUND`, `WASM`, `GUEST_FAILED` (with `guestExitCode`,
`guestOutputBase64`, and `guestOutputText` when the output is UTF-8),
`PROVENANCE_MISMATCH`, `GONE`, `JOURNAL_GAP` and
`OUTPUT_SCHEMA_VIOLATION` (a typed output mismatch, carrying `committed:
true` for executors). Documents are capped at depth 32 and complexity
5000.

Root fields are always outer-nullable, so a failure yields `field: null`
plus an `errors` entry rather than a spec-invalid response.

### 12.6 Typed modules

Install a described module and its field exists. Uninstall it and the
field is gone. One request in flight across an `installModule` that
replaces the same name can run the new bytes with old-shaped args, so
replace under quiesced writes if that matters. See
[§6.6](#66-typed-graphql-surface-describe).

### 12.7 Instances

`fork(name:) { instanceId }` returns the address of the new branch, and
`/graphql/<instanceId>` serves it with the same full surface: its own
modules, triggers, schema and forks. Instances open lazily on the first
request and close when idle (`fork-idle-ttl-secs`, checked every 60
seconds) or when evicted past `fork-max-open`. Forks nest up to 8 deep
under one primary. An unknown id is a 404 with `{"error": "unknown
instance ..."}`, and a fork that fails to open is a 500. The id is
routing, not authorization.

### 12.8 Demo

```sh
cargo run -p fluent-server -- ./data
scripts/demo-orders.sh [endpoint]     # builds the guests, installs placeOrder + topCustomers, seeds, ranks
```
```graphql
mutation { placeOrder(customer: "you", amountCents: "4200") { id customerTotalCents } }
query    { topCustomers(limit: 3) { customer orders totalCents avgCents } }
```

## 13. Replicas and edge caches

A replica attaches to a running master's replication join point and
holds the slice of the master's tree that overlaps its key scope `[lo,
hi)`. The scope is unbounded for a full replica and narrow for an edge
cache. The overlapping index fragments are copied locally, values are
fetched lazily and cached, and committed in-scope writes stream in. The
replica is a library component: the process that needs the scoped reads
embeds an `EdgeReplica` and reads through its store (`get` and `scan`,
clamped to the scope). [REPLICATION.md](REPLICATION.md) is the spec.

```sh
# master: fluent-server on a named store (join point :8428), or the plane alone
fluent-replication ./data --store-name prod [--listen 127.0.0.1:8428]   # the name persists after the first open
```

How it behaves:

- Named master only. An unnamed store cannot open a join point:
  `fluent-server` leaves the port closed and `ReplServer::new` returns
  `InvalidArgument`.
- Provenance. Every connection compares the master's instance id. With
  the same id, every cached byte stays valid across disconnects and lag.
  With a different id (the master was restored, forked or replaced) the
  edge wipes and re-attaches from scratch. Stale history is never served.
- Gap-free attach. The edge subscribes first and then pulls the slice, so
  the union covers everything. Overlap is harmless because entries carry
  seqnos.
- Ephemeral. The edge directory is a cache, wiped on attach, and the
  master keeps no per-edge state beyond the subscription. A stale file
  reference answers `GONE` and the edge re-pulls. Only committed user-key
  data is readable: no modules, no triggers, no queries or executors.
- Lag. A slow edge is cut off (`LAGGED`) rather than stalling the master.
  It re-syncs and keeps its caches.
- Scope. An out-of-scope `get` is refused (`InvalidArgument`), scans
  clamp to the scope, and the reserved keyspace is never copied or
  streamed. A `lo` below the user keyspace is clamped; an empty scope is
  rejected.

The limits are deliberate: one contiguous scope per replica, read-only,
embedded (a replica serves no network protocol of its own), a
memory-only stream overlay (a restart re-attaches), and no WASM at the
edge.

The library surface is `fluent_replication::{ReplServer,
ReplServerConfig, ReplClient, EdgeReplica, EdgeReplicaConfig,
MasterInfo}` and, on the engine side, `fluent31::edge::{EdgeStore,
EdgeConfig, EdgeStats, ValueFetcher}`.

```rust
let mut cfg = EdgeReplicaConfig::new("127.0.0.1:8428", "/tmp/edge", b"user/".to_vec(), Some(b"user0".to_vec()));
// fields: master_addr, dir, scope_lo, scope_hi, refresh_every (300 s; None = only on re-sync),
//         value_cache_bytes (256 MiB), block_cache_size (32 MiB)
cfg.refresh_every = Some(Duration::from_secs(60));
let replica = EdgeReplica::start(cfg)?;                // returns once a complete scoped view is available
replica.store().get(b"user/1")?;  replica.store().stats();   // EdgeStats
replica.master();                                      // StoreIdentity

// lower level: ReplClient::connect(addr) -> (client, MasterInfo { name, instance_id, visible_seqno });
// client.snapshot(lo, hi), fetch_table_chunk(..), fetch_value(..); ReplClient implements edge::ValueFetcher
```

## 14. Operations

### 14.1 Directory layout

```
<dir>/
  LOCK               exclusive flock for the process lifetime
  CURRENT            names the live MANIFEST
  MANIFEST-<gen>     full metadata snapshot
  wal-<id>.log       write-ahead logs, one per memtable generation
  sst-<id>.tbl       immutable table fragments
  vlog-<id>.vlog     value-log files; one active head, the rest sealed
  archive/<name>/    forks, each a complete database directory
```

One process per directory. Everything is CRC32C-checked. Don't hand-edit
anything, and don't delete WALs or manifests.

### 14.2 Sizing

| Knob | Raise it when | Lower it when |
|---|---|---|
| `memtable_size` | write bursts stall on flush | memory is tight |
| `block_cache_size` | the workload is read-heavy and the working set fits | memory is tight |
| `value_threshold` | values are small and scans should stay inline | values are large and the index should stay small |
| `compression = Lz4` | you are disk-bound with compressible values | you are CPU-bound |
| `vlog_gc_ratio` | you want less GC churn | you want space back sooner |
| `trigger_inline_value` | changes-mode consumers need payloads without a read | values are large and write amplification matters |
| `sub_queue_bytes` | subscribers are bursty | memory is tight |

Writers stall rather than fail when frozen memtables exceed
`max_immutable_memtables` or L0 exceeds `l0_stall_trigger`. Sustained
stalls mean compaction cannot keep up.

### 14.3 Monitoring

Two channels: pull state through `stats`, or read the log.

- `stats` (engine, shell, GraphQL) reports the seqno, the memtable and
  level shape, value-log live, retired and discardable bytes, the cache
  hit rate, group commit amortization, and the live subscription and
  snapshot counts (a subscription buffers up to `sub_queue_bytes`; a
  snapshot holds GC). An edge replica reports through `EdgeStats`
  instead: flushed and frontier seqno, fragments, overlay and
  value-cache bytes.
- `triggers` reports `pending` (the backlog depth) and `lastError` per
  trigger.
- `Journal::stats()`: `last_seqno` against `db.seqno()` is the journal
  lag; `last_error` is the last failure.

#### Logging

The engine emits [`tracing`](https://docs.rs/tracing) events. The
binaries write them to stderr and read `RUST_LOG` for the level
(default `info`; `fluent-cli` defaults to `warn` so the shell stays
quiet). An embedding process installs its own subscriber; without one
the events cost nothing. Every engine line names the store it is about
(`db{dir=… store=… instance=…}`), so a server holding forks stays
legible.

| Level | What |
|---|---|
| `error` | a background failure degraded the store (every one is logged, not only the first); the journal stopped; a network plane died |
| `warn` | a write stall began (and why), a subscriber cut for lag, a trigger run failing (with its backoff), a torn WAL tail at recovery, a WASM trap, a replica re-syncing, a file the store could not delete |
| `info` | open (recovery summary) and close; every flush, compaction and value-log GC; forks created and deleted; modules, triggers and pins added and removed; journal base, rotate and compact; replication streams starting and ending; fork instances the server opens and closes; the stats heartbeat |
| `debug` | each WASM invocation (fuel, memory, duration), trigger drains, subscriptions opening and closing, GC liveness sampling, execute retries |
| `trace` | per batch: journal deltas, streamed batches |

GraphQL requests are not logged.

The **stats heartbeat** is the `stats` snapshot as one `info` line per
open store (the primary and every fork the server holds open) plus the
fork registry's occupancy, every 60 s by default — `[log]
stats-every-secs` in the server config, `--stats-every-secs` on
`fluent-graphql`, `0` turns it off; an embedder gets the same line from
`Db::log_stats()`. When memory grows, the heartbeat says which it was:
`imms` climbing (flush not keeping up — a stall follows), subscriptions,
snapshots pinning history, or fork instances.

Guest `log` output is a `debug` event under its own target, enabled
alone with `RUST_LOG=fluent31::wasm::guest=debug`.

### 14.4 Limits

| | |
|---|---|
| key | non-empty, no leading `0x00`, at most 16 KiB |
| value | at most 256 MiB |
| transaction write set | at most 256 MiB |
| names (module, trigger, fork, pin, store) | `[A-Za-z0-9._-]`, at most 64; fork, pin and store names also no leading dot |
| described module name | a valid GraphQL name that is not a built-in root field |
| descriptor | at most 64 KiB, 32 types, 64 fields per type, 16 args, one list level |
| GraphQL body | 32 MiB by default; document depth 32, complexity 5000 |
| GraphQL `scan` page | at most 10000 |
| fork nesting under the server | 8 |
| engine calls in flight per plane | GraphQL 128 reads and 32 writes; replication 64 |
| seqno | 56-bit |

Known limits (v1, deliberate): no block compression by default (LZ4 is
opt-in); value-log discard statistics lag, since dead pointers are only
discovered when compaction reaches them; GC relocations bump seqnos, so
a hot large-value key can cost a transaction a retry; a fixed level
count; and bottom-level merges rewrite the whole bottom level.

Compatibility: every `Options` field except `store_name` may change
between opens of the same store, and `compression` affects only newly
written tables. On-disk formats are versioned. An unnamed store stays on
manifest format 1, a named one writes format 2, and pins bump it to 3;
older binaries read only the formats they know. The replication
protocol advertises its version in `HELLO`.

### 14.5 Platform notes

`IoBackend::Auto` probes io_uring at open and falls back to portable IO;
`stats.backend` tells you which one is active. Docker's default seccomp
profile blocks io_uring, so use `--security-opt seccomp=unconfined` or
`io-backend = "std"`. macOS uses portable IO throughout.

## 15. Testing

```sh
cargo test --workspace                              # engine model tests, group commit, wasm, graphql,
                                                    # server e2e, replication e2e, durability suites
cargo test -p fluent31 --features fault-injection   # fsync failure / ENOSPC / read-fault paths
cargo test --test backup_and_soak -- --ignored      # endurance soak
cargo check -p fluent31 --no-default-features       # the engine without the WASM layer
cargo run --release -p fluent31 --example bench     # throughput probe
cargo run --release -p fluent31 --example gc_bench -- [threads] [always|never] [ops-per-thread] [txn]
```

Suites worth knowing by name: `engine` (a randomized model test against a
`BTreeMap` with interleaved flush, compaction, GC and reopen),
`crash_recovery` (a SIGKILLed child), `fault_injection`,
`corruption_fuzz`, `journal_rebuild`, `durability_modes`, `group_commit`,
`fork_stress` (forks under concurrent writers, flush, compaction and GC),
`trigger_changes`, `trigger_robustness`, `wasm` and `wasm_sandbox`.
`fluent-graphql/tests/graphql.rs` has WAT fixtures for modules,
including `describe`; `fluent-server/tests/server.rs` and
`fluent-replication/tests/replication.rs` are the end-to-end suites.

To test your own modules, look at the GraphQL suite's WAT fixtures for
minimal modules. For executors, spawn N concurrent calls and assert no
lost updates. Restart the server and assert the typed field reappears.

Under Docker: `docker run --security-opt seccomp=unconfined -v
$PWD:/src -w /src rust:1 sh -c "rustup target add wasm32-unknown-unknown
&& cargo test --workspace"`.

## 16. Advanced: how it works

Enough architecture to predict behavior. Each item names its section in
[DESIGN.md](DESIGN.md).

### 16.1 Write path (§2, §13)

A batch is placed first: values at or above `value_threshold` are
appended to the value-log head and the tree entry becomes a pointer.
Then it is logged to the WAL, inserted into the memtable, and published
by advancing the visible seqno, so readers never see a partial batch.
Under `SyncMode::Always` a dedicated commit thread drains everything
queued each cycle and applies it in size-bounded chunks, each with one
value-log fsync and one WAL fsync, so the steady-state group size
approaches the number of concurrent writers. Transactions validate and
apply inside the same critical section, revalidating against earlier
batches in the same group.

### 16.2 Storage shape (§3, §4, §8)

Lazy leveling: tiered merges on the upper levels, where a full level
merges into one run at the front of the next, and one leveled run at the
bottom. Runs are split into key-bounded fragments of about
`target_file_size`, each with its own bloom filter and index, small
enough to pin in memory for the whole dataset. Key-value separation keeps
the tree small because compaction moves pointers rather than payloads.
The value log is reclaimed by its own GC: it relocates a file's live
records through the normal write path, retires the file, and deletes it
only once no snapshot can still reach the old versions and the
relocations sit in fsynced tables. `vlog_retired` in `stats` counts files
between those two steps.

### 16.3 MVCC and the watermark (§6)

The GC watermark is the oldest registered snapshot, and pins and
subscriptions register too. Compaction keeps every version above the
watermark plus the newest at or below it, and nothing else. That is the
whole reason behind §4.3: holding any version holds every version of
every key, a seqno is addressable only while it is above the watermark,
and per-key retention does not exist.

Commit validation reads the newest committed version, tombstones
included, of each `get_for_update` key and each written key, inside the
same critical section every other writer uses. The transaction's own
snapshot bounds the watermark, so that evidence cannot be compacted away
mid-transaction.

### 16.4 Recovery (§5)

The manifest is a full metadata snapshot, rewritten per change and
flipped by an atomic `CURRENT` rename. Tables are fsynced before any
manifest references them. Recovery replays every WAL at or above the
manifest's floor, validates value-log pointers against the scanned file
prefix, truncates the newest WAL's torn tail, flushes the replayed
memtable synchronously so that a crash during recovery only re-replays,
and opens a fresh value-log head, because the engine never appends to a
file that predates a crash. Orphaned files and crashed fork builds are
swept.

### 16.5 WASM layer (§9)

wasmtime with fuel metering, memory limits, NaN canonicalization,
deterministic SIMD and no WASI. Queries run against a registered snapshot
pinned for the whole invocation. Executors run in a `Txn`, and a conflict
discards the store and re-runs. Compiled modules are cached by content
hash; one-shot bytes are compiled uncached. Module bytes live at
`\x00wasm\x00<name>` as ordinary versioned keys, which is what lets
`query_at` time-travel code together with data.

### 16.6 Triggers (§9, "Write-range triggers")

Capture runs inside the commit critical section. Each committed batch's
keys are matched against the trigger registry and the event records are
appended to the same batch: one WAL record, one seqno range. Keys mode
queues at `\x00trgq\x00<trigger>\x00<key>`, where the key is the queue
entry, which is why re-touches coalesce. Changes mode queues at
`\x00trgq\x00<trigger>\x00<seqno>` with the change as the value, which is
why events stay ordered and never coalesce. A runner thread drains each
backlog in chunks as a system transaction pre-seeded with deletes of the
consumed entries. System transactions skip capture; that is the
no-stacking rule. The consumed queue keys sit in the transaction's write
set, so a re-touch that lands after the drain's snapshot conflicts the
commit and the drain re-runs against fresh state. Ordinary OCC closes
the race.

### 16.7 Forks (§10)

A head fork flushes, pins a cut under a brief manifest lock, hard-links
every table and sealed value-log file, copies the head value-log file up
to its synced length, writes a fresh manifest and `fork.meta`, fsyncs,
and renames into place. A point cut rewrites the tables to the cut with
one merge that keeps the newest version at or below the cut per key,
while the values stay linked. Pins are manifest records that re-register
a snapshot at every open before the background threads start.

### 16.8 Change stream, subscriptions, journal (§14, WASM.md §9)

`Db::subscribe` taps the apply path right after the seqno is published,
so delivery is ordered and gap-free from installation. Entries carry
unresolved pointers, and the consumer resolves them off the write path
under an advancing snapshot pin, so value-log GC cannot delete a file
that is still in flight. A subscriber past `sub_queue_bytes` is dropped.
GraphQL subscriptions, the journal and replication are all consumers of
this one stream.

### 16.9 Identity and replication (§14, REPLICATION.md)

The instance id is `H(name)` for a root store and `H(parent ‖ cut ‖
name)` for a fork. It is deterministic, so a crash between minting and
persisting re-mints the same id. File ids and offsets are unique only
within one lifetime, and the instance id is the outer qualifier every
replica checks. The edge copies the overlapping fragments (with bounds
and size cross-checked and blocks CRC-verified), applies the stream into
an overlay memtable, resolves values inline first, then from a local
cache, then by fetching from the master, and reads through the same
merge and MVCC iterator stack as the engine.

### 16.10 Threads and locks (§11)

Per store there are the user writers, one flush thread, one compaction
thread that also runs value-log GC, the commit thread and the trigger
runner. An attached journal adds its own drainer thread, and the GraphQL
plane adds one forwarder thread per active subscription. Background
failures degrade the store (`Error::Background`) instead of hanging
waiters. The lock order is strict: `write`, then `manifest`, then
`state`, then `snapshots`.

## 17. Glossary

- **changes mode**: the trigger mode that delivers every committed op, in order, to `on_apply`.
- **commit seqno**: the last seqno of an atomic commit; the state in which its ops became visible.
- **cut**: the seqno a fork captures.
- **edge cache**: a replica scoped to a key range.
- **elided**: a changes-mode event whose value exceeded `trigger_inline_value` and arrives key-only.
- **executor**: a module invoked through `execute`, inside a transaction.
- **feed**: a descriptor declaration that makes a changes-mode module's output range a typed subscription.
- **fork**: a named, hard-linked, complete copy of the database at a cut.
- **instance**: a database directory, primary or fork, as addressed by a server; identified by its instance id.
- **join point**: the replication listener that replicas attach to.
- **keys mode**: the trigger mode that delivers coalesced touched keys to `on_touch`.
- **lineage**: a store and the forks and restores descending from it, linked by instance ids.
- **module**: a WASM binary installed in the database.
- **one-shot**: invoking module bytes without installing them.
- **pin**: a durable, named, store-wide GC hold at a seqno.
- **querier**: a module invoked through `query`, read-only at a snapshot.
- **reserved keyspace**: keys starting with `0x00`. Engine state, invisible to users.
- **seqno**: the sequence number of an op, and also the address of a state.
- **store name, identity**: an operator name that maps to a deterministic instance id; required for replication.
- **trigger**: a binding of a module to a key range, invoked after commits into the range.
- **value log, vlog**: append-only files holding values at or above `value_threshold`. The tree holds pointers.
- **WAT**: the WebAssembly text format, accepted wherever module bytes are.
- **watermark**: the oldest registered snapshot; the GC boundary.
