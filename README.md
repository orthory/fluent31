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
txn.commit()?;                       // Err(Error::Conflict) if acct moved: re-run the whole block
```

```sh
cargo run -p fluent-cli -- ./data                        # interactive shell
cargo run -p fluent-server -- ./data --store-name prod   # GraphQL :8317, replication :8428
cargo run -p fluent31 --example walkthrough              # the whole path in one program, asserting every step
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

| | |
|---|---|
| [orthory.github.io/fluent31](https://orthory.github.io/fluent31/) | Start here. Everything about using fluent31: a tutorial, the concepts, extending it with WASM, the full reference, worked recipes, and a translation guide if you are coming from SQL. |
| [SKILL.md](SKILL.md) | The primer for agents: the model in twelve lines, exact signatures, the traps, and the assumptions carried in from other databases that are wrong here. |
| [docs/llms.txt](docs/llms.txt) | The same documentation as one markdown file per page, for agents and scripts. Generated from the site. |
| [WASM.md](WASM.md) | The module authoring manual and ABI spec. |
| [DESIGN.md](DESIGN.md) | The architecture as implemented, section by section. |
| [REPLICATION.md](REPLICATION.md) | The replica protocol spec. |

### Working on it with an agent

fluent31 is newer than the training data of every current model, so an agent
asked to use it will not recall an API — it will infer one from RocksDB or
SQL, confidently and wrongly. Give it the real one first.

Point it at the index, which links every documentation page as its own
fetchable file:

```
https://orthory.github.io/fluent31/llms.txt
```

Or install the primer where your agent looks for skills, so it loads without
being asked. For Claude Code, in the repo that uses fluent31:

```sh
mkdir -p .claude/skills/fluent31
curl -fsSL -o .claude/skills/fluent31/SKILL.md \
  https://raw.githubusercontent.com/orthory/fluent31/master/SKILL.md
```

`SKILL.md` opens with the assumptions that do not transfer from other
databases — `get_for_update` takes no lock, triggers cannot veto a write,
registering a trigger does not backfill, a guest has no clock. Those are the
mistakes an agent makes first.

## Testing

```sh
cargo test --workspace                              # model tests, group commit, wasm, graphql, server and
                                                    # replication e2e, hard-crash recovery, corruption fuzz,
                                                    # journal rebuild
cargo test -p fluent31 --features fault-injection   # fsync-failure / ENOSPC / read-fault paths
scripts/build-agent-docs.py --check                 # docs/p/ and docs/llms.txt match docs/index.html
scripts/check-docs-api.py                           # the docs still describe the code
```

`docs/p/*.md` and `docs/llms.txt` are generated from `docs/index.html`. Edit
the site, then re-run `scripts/build-agent-docs.py`; `--check` fails if they
have drifted.

`check-docs-api.py` compares the usage docs against the crates: every call
they make resolves, every public method is named somewhere, GraphQL fields
and arguments exist, `Options` defaults still match `config.rs`, and the
printed command lines are the ones the binaries accept. Run it after
changing a public signature, a default, or a flag.

Under Docker, io_uring is blocked by the default seccomp profile. Add
`--security-opt seccomp=unconfined`.

## Layout

```
crates/fluent31           the engine (lib)
crates/fluent-guest       guest-side SDK for WASM modules (+ fluent-guest-macros)
crates/fluent-cli         interactive shell, journal rebuild
crates/fluent-server      the server binary: both planes in one process
crates/fluent-graphql     GraphQL plane (axum + async-graphql), a library
crates/fluent-replication replication plane: master side + embeddable edge replica, a library
guests/                   example modules (a separate wasm32 workspace)
scripts/demo-orders.sh    typed-module demo against a running server
```
