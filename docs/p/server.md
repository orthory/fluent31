<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#server -->

# Server mode

> One process, one `Db`, two planes.

```
fluent-server <db-dir> [--config FILE] [--store-name NAME]
              [--graphql ADDR:PORT] [--replication ADDR:PORT]
              [--sync always|never|periodic:<ms>] [--max-body-bytes N]
              [--journal DIR]
fluent-server --print-schema                   # the base SDL (built-ins only)
```

| Plane | Default | Purpose |
|---|---|---|
| graphql | `127.0.0.1:8317` | typed and admin operations, GraphiQL at `/`, subscriptions over graphql-ws, fork instances at `/graphql/<instanceId>` |
| replication | `127.0.0.1:8428` | the join point for replicas and edge caches; opens only on a named store |

The store directory is flocked, so the planes cannot be split across processes. Server mode is how they share one handle. `--store-name` is persisted in the store, so pass it once. Without a name, graphql serves and the join point stays closed; the log says so.

On the first SIGINT or SIGTERM the server stops accepting and drains in-flight GraphQL requests, then the process exits and open replication connections drop (the WAL keeps the store consistent). A second signal exits immediately. The log (stderr; `RUST_LOG` sets the level — [Operations](operations.md)) reports each bound address as it comes up and, for a named store, its name and instance id; every flush, compaction, fork and journal event follows at `info`, with a stats heartbeat every 60 s. If the engine degrades (`Error::Background`), GraphQL answers `BACKGROUND` and replication answers `ERR`; restart the process.

> **Exposure.** Every plane defaults to loopback and speaks plain TCP or HTTP with no authentication. To expose one, bind it explicitly and put TLS and access control in front: a reverse proxy for GraphQL, a network boundary for replication.

## Config file

`--config server.toml`. The top-level keys, `[listen]` and `[graphql].max-body-bytes` mirror the flags, and an explicit flag wins. The rest is file-only. Unknown keys are an error. Every key, with its default:

<!-- server.toml -->
```
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

[log]
stats-every-secs = 60         # stats heartbeat per open store; 0 = off

[engine]                      # every fluent31::Options tunable, kebab-case
create-if-missing = true
wasm-enabled = true
io-backend = "auto"           # auto | uring | std
compression = "none"          # none | lz4
memtable-size = 8388608
# … every Options field from the Embedded API page, kebab-case
```

## Embedding the server

```
use fluent_server::{Server, ServerConfig};

let db = Arc::new(Db::open(&dir, opts.clone())?);
let server = Server::start(db.clone(), &dir, opts, ServerConfig::default()).await?;
server.graphql_addr; server.replication_addr;   // replication_addr: None when unnamed
server.db();
server.shutdown().await;
```

`ServerConfig` holds `graphql_addr`, `replication_addr`, `max_body_bytes`, `registry: RegistryConfig { max_open, idle_ttl }`, `replication: ReplServerConfig { max_frame, ping_every }` and `stats_every` (the heartbeat period; 60 s, zero = off). Nothing is served unless every bind succeeds; failures come back as `StartError::{Engine, Bind}`. The TOML loader is public too (`FileConfig::load`, `overlay`, `server_config`, `engine_options`, `parse_sync`).

For the GraphQL plane alone: `SchemaManager::new(db)`, then `InstanceRegistry::new(..)`, then `fluent_graphql::router(registry, max_body)`, which is an `axum::Router`. Call `registry.evict_idle()` periodically; the server ticks every 60 seconds. The stats heartbeat is `fluent_graphql::stats_heartbeat(registry, every)`, a future to spawn. For replication: `ReplServer::new(db, cfg)?` fails with `InvalidArgument` on an unnamed store; then `.serve(listener)`.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Server mode` in *Reference*
