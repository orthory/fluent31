<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#replication -->

# Replication

> Read-only replicas that attach to a running master's join point and hold the slice of the tree overlapping their key scope.

The scope is unbounded for a full replica and narrow for an edge cache. The overlapping index fragments are copied locally, values are fetched lazily and cached, and committed in-scope writes stream in. The replica is a library component — the process that needs the scoped reads embeds an `EdgeReplica` and reads through its store (`get` and `scan`, clamped to the scope) — or the server binary serves it: the edge role ([Server mode](server.md)) is the same driver behind the read-only edge GraphQL surface. [REPLICATION.md](https://github.com/orthory/fluent31/blob/master/REPLICATION.md) is the spec.

```
# master: fluent-server on a named store opens the join point (:8428)
fluent-server ./data --store-name prod [--replication 127.0.0.1:8428]
```

## How it behaves

- **Named master only.** An unnamed store cannot open a join point: `fluent-server` leaves the port closed and `ReplServer::new` returns `InvalidArgument`.
- **Provenance.** Every connection compares the master's instance id. With the same id, every cached byte stays valid across disconnects and lag. With a different id (the master was restored, forked or replaced) the edge wipes and re-attaches from scratch. Stale history is never served.
- **Gap-free attach.** The edge subscribes first and then pulls the slice, so the union covers everything. Overlap is harmless because entries carry seqnos.
- **Ephemeral.** The edge directory is a cache, wiped on attach, and the master keeps no per-edge state beyond the subscription. A stale file reference answers `GONE` and the edge re-pulls. Only committed user-key data is readable: no modules, no triggers, no queries or executors.
- **Lag.** A slow edge is cut off (`LAGGED`) rather than stalling the master. It re-syncs and keeps its caches.
- **Scope.** An out-of-scope `get` is refused (`InvalidArgument`), scans clamp to the scope, and the reserved keyspace is never copied or streamed.
- **Frontier.** The store's frontier is the master position its scoped view is complete through: the slice's flush watermark, then each applied batch's commit seqno. Waiting for a write to become visible is an event, not a poll: `wait_frontier(seqno)` on the store blocks until the frontier covers that seqno, and `wait_attached(&instance_id)` on the replica blocks until it is attached to that master instance and hands out that store, which is the handle to hold after the master was replaced. Neither wait is bounded.

The limits are deliberate: one contiguous scope per replica, read-only, a library driver that serves no network protocol of its own (the server binary's edge role is the onward surface), a memory-only stream overlay (a restart re-attaches), and no WASM at the edge.

## Embedding a replica

```
let mut cfg = EdgeReplicaConfig::new("127.0.0.1:8428", "/tmp/edge",
                                     b"user/".to_vec(), Some(b"user0".to_vec()));
// fields: master_addr, dir, scope_lo, scope_hi, refresh_every (300 s; None = only on re-sync),
//         value_cache_bytes (256 MiB), block_cache_size (32 MiB)
cfg.refresh_every = Some(Duration::from_secs(60));
let replica = EdgeReplica::start(cfg)?;      // returns once a complete scoped view is available
replica.store().get(b"user/1")?;
replica.store().stats();                     // EdgeStats
replica.store().wait_frontier(seqno);        // blocks until the view covers a master seqno
replica.wait_attached(&instance_id);        // blocks until attached to that master instance; returns its store
replica.master();                            // StoreIdentity
```

The library surface is `fluent_replication::{ReplServer, ReplServerConfig, ReplClient, EdgeReplica, EdgeReplicaConfig, MasterInfo}` and, on the engine side, `fluent31::edge::{EdgeStore, EdgeConfig, EdgeStats, ValueFetcher}`. Lower level: `ReplClient::connect(addr)` gives `(client, MasterInfo { name, instance_id, visible_seqno })`, with `snapshot`, `fetch_table_chunk` and `fetch_value`.

A replica logs its attach, every slice pull and every re-sync at `info`; a lag cut, a broken stream and a changed master identity are `warn`. The master logs each stream it serves and why it ended.

## Store identity

A store can carry an operator-chosen name. From the name the engine mints a deterministic 128-bit instance id, and forks and restores mint new ones. Replication verifies the id on every connection, so a replaced master invalidates every replica at once. Under the server, every fork is an instance addressed at `/graphql/<instanceId>`. The id is an address, not a credential.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Replication` in *Reference*
