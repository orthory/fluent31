# fluent31

An embedded key-value database engine in Rust that runs WebAssembly
instead of SQL.

- Engine: LSM storage with WiscKey-style key-value separation (a small
  index, big values in an append-only log), MVCC snapshots, optimistic
  transactions, io_uring on Linux.
- Modules: install WASM into the database and run it as read-only
  queries or transactional executors against a kernel-style syscall ABI
  (`get`, `put`, `delete`, batched scans, fuel and memory limits). A
  module that describes itself becomes its own typed GraphQL field.
- Triggers: bind a module to a key range and the engine invokes it after
  every committed write into the range. Schema-free indexes,
  materialized views, `GROUP BY` tables, cascades, changefeeds. Events
  are durable with the write and effects are exactly-once.
- Forks: `fork("name")` publishes a complete, consistent, hard-linked
  copy of the database at a cost proportional to the file count, not the
  data. Open it for a writable copy-on-write clone. Pins make a point
  fork-able later.
- Journal: opt-in and off the commit path. An independent mutation log
  from which a fresh database is rebuilt when the store directory is
  lost.
- Server: one process, one store, two planes. GraphQL for typed and
  admin operations with live subscriptions, and a replication join point
  for full replicas and key-range edge caches. Structured logs
  (`tracing`) of every flush, compaction, fork, journal and replication
  event, and a per-store stats heartbeat.

## Quick start

```rust
use fluent31::{Db, Options};

let db = Db::open("./data", Options::default())?;
db.put("user/1", "ada")?;
assert_eq!(db.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));

for kv in db.iter(Some(b"user/"), Some(b"user0"), false)? {
    let (k, v) = kv?;
}

let mut txn = db.begin();
let bal = txn.get_for_update(b"acct")?;
txn.put("acct", "90")?;
txn.commit()?;                       // Err(Error::Conflict) if someone else wrote acct
```

```sh
cargo run -p fluent-cli -- ./data                        # interactive shell
cargo run -p fluent-server -- ./data --store-name prod   # GraphQL :8317, replication :8428
cargo run -p fluent31 --example live_stats               # a trigger-maintained GROUP BY, checked against a recount
```

A module, end to end:

```rust
#[fluent_guest::query]                                   // read-only at one snapshot
fn count(prefix: Vec<u8>) -> Result<String, fluent_guest::Fail> {
    Ok(fluent_guest::scan_prefix(&prefix).map_err(|_| "scan")?.count().to_string())
}
```

```rust
db.install_module("count", &std::fs::read("count.wasm")?)?;
db.query("count", b"user/")?;                            // b"1"
```

## Documentation

Hosted at [orthory.github.io/fluent31](https://orthory.github.io/fluent31/).

| | |
|---|---|
| [GUIDE.md](GUIDE.md) | Start here. Everything about using fluent31: concepts, the embedded API, modules, triggers, forks, durability, the shell, server mode, GraphQL, replication, operations, and an "advanced" section on how it works. |
| [SKILL.md](SKILL.md) | The short entry point for agents: the model in twelve lines, commands, traps, and where to read more. |
| [WASM.md](WASM.md) | The module authoring manual and ABI spec. |
| [DESIGN.md](DESIGN.md) | The architecture as implemented, section by section. |
| [REPLICATION.md](REPLICATION.md) | The replica protocol spec. |

## Testing

```sh
cargo test --workspace                              # model tests, group commit, wasm, graphql, server and
                                                    # replication e2e, hard-crash recovery, corruption fuzz,
                                                    # journal rebuild
cargo test -p fluent31 --features fault-injection   # fsync-failure / ENOSPC / read-fault paths
```

Under Docker, io_uring is blocked by the default seccomp profile. Add
`--security-opt seccomp=unconfined`.

## Layout

```
crates/fluent31           the engine (lib)
crates/fluent-guest       guest-side SDK for WASM modules (+ fluent-guest-macros)
crates/fluent-cli         interactive shell, journal rebuild
crates/fluent-server      server mode: both planes in one process
crates/fluent-graphql     GraphQL plane (axum + async-graphql)
crates/fluent-replication replication: master server + embeddable edge replica
guests/                   example modules (a separate wasm32 workspace)
scripts/demo-orders.sh    typed-module demo against a running server
```
