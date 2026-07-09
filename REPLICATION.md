# fluent replication v1 — edge replica protocol specification

The replication channel for fluent31: a small **ephemeral edge replica**
holds the slice of one master's tree that overlaps its key scope
`[lo, hi)`, serves reads locally, and reaches back for what it doesn't
have. Server: `fluent-replication master` (default `127.0.0.1:8428`).
Edge: `fluent-replication edge` (drives a replica and serves standard
wire-v1 reads over it). Library types: `fluent_replication::{ReplServer,
ReplClient, EdgeReplica}`, engine-side `fluent31::edge::EdgeStore`.

This is a **separate channel** from `fluent-wire` — its own port and
opcode space — but the frame layout is identical, so the framing code is
shared. All integers little-endian; `blob` = `[u32 len][bytes]`;
`opt X` = `[u8 present]` then the field when present=1.

## 1. Model

- **Scoped index copy.** Runs are sequences of key-bounded fragment
  files, so a replica needs only the fragments overlapping its scope. It
  copies them verbatim (self-verifying: CRC'd blocks, advertised
  size/bounds cross-checked at install) and opens them exactly like a
  normal store — index + bloom pinned in memory.
- **Lazy values.** WiscKey pointers into the master's value log resolve
  through a local record cache first, then a `FETCH_VALUE` reach-back.
  Every record re-verifies CRC + embedded key before it is served or
  cached — locally and remotely.
- **Streamed syncs.** A subscription delivers committed in-scope writes
  (values resolved master-side) in seqno order into an in-memory overlay.
- **Ephemeral.** The edge never burdens the master's file lifecycle: no
  slice pins, no leases. A stale reference answers `GONE` and the replica
  re-pulls. The only master-side state is the subscription itself, whose
  advancing snapshot pin is bounded by the lag cutoff.

**Gap-free attach**: subscribe FIRST (the reply carries `start_seqno`),
then pull the slice — `SNAPSHOT` flushes the master, so its
`flushed_seqno >= start_seqno` and (slice ∪ stream) covers everything.
Overlap is harmless: entries carry seqnos, and a slice install prunes
overlay entries at or below its watermark.

## 2. Provenance

`(vlog file id, offset)` and fragment ids are unique only within one
**store lifetime**. The master's manifest carries a deterministic
128-bit `instance_id` (see `identity.rs`: `H(name)` for a root store,
`H(parent ‖ cut ‖ name)` for a checkpoint fork/restore; uniqueness is an
operator contract — fleet-unique store names). Every connection HELLOs
and the client compares instance ids by equality:

- same id ⇒ every locally cached byte is still valid (re-syncs after
  disconnects/lag keep all caches);
- different id (master restored, forked, replaced) ⇒
  `ProvenanceMismatch`: the replica wipes and re-attaches from scratch.

A master with no identity (`Options::store_name` unset) refuses to serve
replication (`NO_IDENTITY`).

## 3. Frame

Identical to wire v1:

```
[u32 frame_len]   length of everything AFTER this field (id + op + payload)
[u64 request_id]  client-allocated, echoed verbatim; 0 = server-initiated
[u8  opcode|status]
[payload…]
```

Requests are answered in order on this channel (no out-of-order
completion — the fetch path is sequential by design; open more
connections for parallel fetches).

## 4. Opcodes

| op | name | request payload | OK response payload |
|---|---|---|---|
| 0x00 | HELLO | (empty) | `[u32 version][blob store_name][16B instance_id][u64 visible_seqno]` |
| 0x01 | SNAPSHOT | `[opt lo][opt hi]` | slice manifest (§5) — flushes first |
| 0x02 | FETCH_TABLE | `[u64 table_id][u64 off][u32 len]` | raw fragment bytes (clamped at file end) |
| 0x03 | FETCH_VALUE | `[u64 vlog_id][u64 off][u32 len]` | raw record bytes `[crc][klen][vlen][key][value]` |
| 0x04 | SUBSCRIBE | `[opt lo][opt hi]` | `[u64 start_seqno]`, then the connection is push-only (§6) |

## 5. Slice manifest encoding

```
[u64 flushed_seqno]
[u32 nlevels] {
  [u32 nruns] {
    [u64 run_id]
    [u32 ntables] { [u64 table_id][u64 size][blob min_ukey][blob max_ukey] }
  }
}
```

Levels hold runs newest-first, fragments key-ordered — the same shape as
the live version, restricted to fragments overlapping `[lo, hi)`.
Fragment copying is **file-granular**: a fragment straddling the scope
boundary is copied whole, so out-of-scope residents can reach the edge's
disk; the edge never *serves* them (get/scan clamp to the scope), and the
reserved `\x00` keyspace is never served nor streamed.

## 6. Subscription (push mode)

After `SUBSCRIBE` succeeds the client sends nothing further; the server
pushes frames with `request_id = 0`:

| op | name | payload |
|---|---|---|
| 0x10 | STREAM | `[u32 count]` then per entry `[u8 kind (1=put, 0=del)][u64 seqno][blob key]` + `[blob value]` for puts |
| 0x11 | PING | (empty) — sent on idle so the edge can detect a dead master |
| 0x12 | LAGGED | (empty) — the subscriber fell behind and was cut off; connection closes |

Delivery is seqno-ascending and gap-free above `start_seqno`. Values
arrive resolved (the server's subscription holds an advancing snapshot
pin, so pointer resolution never races vlog GC). Backpressure is end to
end: a slow edge stalls its socket, the server's bounded forward channel
fills, the engine-side queue overflows past `Options::sub_queue_bytes`,
and the subscriber is dropped with `LAGGED` — writers never stall. After
`LAGGED` the edge re-syncs: new subscription, fresh slice pull; local
caches stay valid (same instance id).

## 7. Status codes

| st | meaning |
|---|---|
| 0x00 | OK |
| 0x01 | ERR — engine/server failure; payload: UTF-8 message |
| 0x02 | GONE — the file left the live version (compaction/GC); re-pull the slice |
| 0x03 | NO_IDENTITY — master store is unnamed; replication refused |
| 0x04 | BAD_FRAME — unknown opcode / malformed payload |

## 8. Edge behavior (reference driver)

`EdgeReplica::start` attaches an `EdgeStore` (directory wiped — an edge
is a cache, not a store of record; an `EDGE` stamp file records master
name, instance id, and scope for inspection), subscribes, pulls the
slice (retrying `GONE` races), and then follows the stream. Slice
refreshes (periodic, and after every re-sync) prune the overlay.
Reads: overlay → fragments; values: inline → local value cache →
`FETCH_VALUE`. The local value cache is an append-only file of verbatim
records, reset wholesale when it exceeds its cap. The replica implements
`fluent_wire::WireBackend`, so the standard wire server fronts it:
GET/SCAN/STATS work (scans clamp to the scope, out-of-scope GETs answer
INVALID), writes and WASM ops answer INVALID.

## 9. Known v1 limits (deliberate)

- One contiguous scope per replica; attach again for more ranges.
- The edge is read-only and never proxies out-of-scope requests.
- The overlay is memory-only: an edge restart re-attaches from scratch.
- Streamed values land inline in the overlay (large hot values inflate
  it until the next slice refresh prunes).
- No WASM at the edge.
- Heavy master compaction inside the scope can make slice pulls retry;
  the engine returns `GONE` per race and the driver retries with fresh
  snapshots.
