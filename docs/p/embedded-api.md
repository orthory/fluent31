<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#embedded-api -->

# Embedded API

> The `fluent31` crate, embedded in a Rust process: open, read, write, scan, snapshot, transact, maintain.

## Opening

```
let db = Db::open(path, Options {
    sync: SyncMode::Periodic { every: Duration::from_millis(50) },
    ..Options::default()
})?;
```

`Db::open` creates the directory when `create_if_missing` is set (the default), takes an exclusive flock so that a second open of the same directory fails, recovers from the WAL, and then starts the flush, compaction, commit and trigger threads. Recovery time is proportional to the unflushed WAL.

## Options

Every field, with its default:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `create_if_missing` | `bool` | `true` | Create the directory. `false` fails on a missing store. |
| `sync` | `SyncMode` | `Always` | The [durability mode](durability.md). |
| `io_backend` | `IoBackend` | `Auto` | `Auto` probes io_uring and falls back; `Uring` forces it and open fails where unsupported; `Std` forces pread/pwrite. |
| `wasm_enabled` | `bool` | `true` | `false` makes the WASM layer inert at runtime: module and trigger calls return `Error::Wasm`, the trigger runner does not start, and writes made while disabled never fire triggers. Listing still works. |
| `store_name` | `Option<String>` | `None` | An operator-chosen name, unique across your fleet, that fixes the store identity. Required for replication. Set it once: an unnamed store adopts it, an omitted name on reopen keeps the persisted one, and a different name is `InvalidArgument`. A fork's name is fixed at fork time. |
| `memtable_size` | `usize` | 8 MiB | Freeze and flush the memtable past this. |
| `max_immutable_memtables` | `usize` | 2 | Frozen memtables waiting for flush before writers stall. |
| `block_size` | `usize` | 8 KiB | Target data block size in tables. |
| `compression` | `Compression` | `None` | `Lz4` compresses the data and index blocks of newly written tables. Reads never depend on it; a store is readable under either setting. |
| `bloom_bits_per_key` | `usize` | 10 | Bloom filter budget. |
| `block_cache_size` | `usize` | 64 MiB | The shared block cache (table blocks and value-log records up to 64 KiB). |
| `l0_compaction_trigger` | `usize` | 4 | L0 runs that trigger a merge into L1. |
| `tier_width` | `usize` | 4 | Runs per level that trigger a merge to the next. |
| `max_levels` | `usize` | 7 | The level count; the last level is one leveled run. |
| `l0_stall_trigger` | `usize` | 12 | L0 runs at which writers stall until compaction catches up. |
| `target_file_size` | `u64` | 64 MiB | Compaction splits runs into fragments of about this size. |
| `value_threshold` | `usize` | 4096 | Values at or above this go to the value log; smaller ones stay inline. `0` separates everything and `usize::MAX` disables separation. |
| `vlog_file_size` | `u64` | 128 MiB | Seal and rotate the value-log head at this size. |
| `vlog_gc_ratio` | `f64` | 0.5 | A sealed value-log file becomes a GC victim once this fraction of it is known dead. |
| `max_key_size` | `usize` | 16 KiB | Hard cap. |
| `max_value_size` | `usize` | 256 MiB | Hard cap. |
| `max_txn_write_bytes` | `usize` | 256 MiB | Cap on one transaction's buffered writes, executors included. |
| `sub_queue_bytes` | `usize` | 8 MiB | Buffered bytes per change-stream subscriber. Past it the subscriber is cut off (`Lagged`); writers are never stalled. |
| `wasm_fuel` | `u64` | 1e9 | Fuel per invocation. Exhaustion traps. |
| `wasm_memory_limit` | `usize` | 64 MiB | Linear memory cap per invocation. |
| `execute_retries` | `usize` | 3 | Attempts per executor call, the first included (minimum 1). A commit conflict re-runs until they are spent, then the call returns `Conflict`. |
| `max_wasm_input` | `usize` | 64 MiB | Input cap, rejected before execution. |
| `max_wasm_output` | `usize` | 32 MiB | Output cap (`ENOSPC` to the guest). |
| `max_wasm_log` | `usize` | 1 MiB | Guest log cap. |
| `max_wasm_scans` | `usize` | 64 | Open scan handles per invocation. |
| `wasm_module_cache` | `usize` | 32 | Compiled modules kept in memory, keyed by content hash. |
| `trigger_batch` | `usize` | 512 | Events per trigger invocation. |
| `trigger_inline_value` | `usize` | 64 KiB | Changes-mode events carry the written value up to this size. Above it the value is elided and the event carries the key only. |

## Keys and values

A user key is non-empty, does not start with byte `0x00`, and is at most `max_key_size` (16 KiB); values go up to `max_value_size` (256 MiB). Keys starting with `0x00` are the engine's own ([modules, trigger definitions and queues](snapshots.md#the-engines-own-keyspace)): reading or writing one is `InvalidArgument` from the API and `EINVAL` inside a module, and scans clamp to the user keyspace.

Key layout is your schema. The examples use `<entity>/<id>` for records, zero-padded numeric ids so they sort (`orders/00000042`), and derived data under its own prefix (`idx/customer/acme/00000042`).

## Point operations and batches

```
db.put(key, value)?;           // key/value: impl Into<Vec<u8>>
db.delete(key)?;               // succeeds whether or not the key exists
let v: Option<Vec<u8>> = db.get(b"key")?;

let mut b = WriteBatch::new();
b.put("a", "1"); b.delete("b");
b.len(); b.is_empty(); b.byte_size();
db.write(b)?;                  // atomic, one contiguous seqno range
```

## Scans

```
// [lo, hi); None = open end; reverse = descending
let it: DbIterator = db.iter(Some(b"user/"), Some(b"user0"), false)?;
for kv in it {
    let (key, value): (Vec<u8>, Vec<u8>) = kv?;   // Item = Result<(Vec<u8>, Vec<u8>)>
}
```

The iterator resolves value-log pointers in batches (a prefetch window of 32 entries or 256 KiB, one batched read per value-log file), so a scan over large values costs one IO round per group rather than one per entry. An error ends iteration. Bounds are byte-exact and there is no prefix argument at this layer: a prefix scan is `[prefix, prefix+1)`, where `prefix+1` is the prefix with its last byte incremented, so `user/` scans to `user0`. GraphQL's `scan(prefix:)` and the SDK's `scan_prefix` compute that for you. To resume after a key `k` when paging, start the next scan at `k ++ 0x00`, the smallest key greater than `k`. Pages that belong to one logical read should share a snapshot (`iter_at`).

## Snapshots

```
let snap: Snapshot = db.snapshot();          // registers a GC hold
snap.seqno();
db.get_at(b"k", &snap)?;
db.iter_at(lo, hi, reverse, &snap)?;
db.query_at("module", input, &snap)?;        // module bytes AND data at the snapshot
drop(snap);                                  // releases the hold

let s: SeqNo = db.seqno();                   // "now", without a hold
let snap = db.snapshot_at(s)?;               // Err(InvalidArgument) once GC passed s
```

> **Hold snapshots briefly.** A snapshot held across a long job stalls value-log reclamation and version GC for the whole store.

## Transactions

```
let mut txn: Txn = db.begin();
txn.snapshot_seqno();
let cur = txn.get(b"k")?;                    // consistent read, no conflict check
let cur = txn.get_for_update(b"k")?;         // read + conflict check at commit
txn.put("k", "v")?; txn.delete("j")?;        // buffered until commit
txn.write_set_len();
for kv in txn.iter(lo, hi, reverse)? { }     // snapshot merged with this txn's writes
txn.commit()?;                               // or txn.rollback(), or drop to discard
```

Semantics and the retry loop are on [Transactions](transactions.md).

## Durability and maintenance

```
db.sync_wal()?;      // barrier: everything acked before this is durable on return
db.flush()?;         // freeze the memtable and wait until it is in tables
db.compact_all()?;   // compact until no trigger fires
db.gc_vlog()?;       // one value-log GC pass; Ok(Some(file_id)) if a file was retired
let s: DbStats = db.stats();
```

`DbStats` has `backend` (`"io_uring"` or `"std"`), `visible_seqno`, `memtable_bytes`, `immutable_memtables`, `levels` as a `Vec<(runs, files, bytes)>`, `vlog_files`, `vlog_retired` (retired files waiting on the deletion gates), `discard_bytes` (value-log bytes known to be dead), `cache_hits`, `cache_misses`, `commit_groups`, `commit_batches` (the difference from `commit_groups` is how many fsyncs group commit saved) and `wal_syncs`.

Compaction and value-log GC run on their own on background threads. The manual calls exist for tests, benchmarks and "reclaim now".

## Modules and triggers

```
db.install_module("name", &wasm_bytes)?;   // validates: exports memory + a role entry
db.uninstall_module("name")?;
db.list_modules()?;                        // Vec<ModuleInfo>: name, size, content_hash

let out: Vec<u8> = db.query("name", input)?;     // requires the `query` export
let out: Vec<u8> = db.execute("name", input)?;   // requires `execute`; OCC-retried
db.query_wasm(&wasm, input)?;              // one-shot: bytes never installed
db.execute_wasm(&wasm, input)?;

db.create_trigger("name", "module", Some(b"orders/"), Some(b"orders0"))?;
db.delete_trigger("name")?;
db.list_triggers()?;                       // Vec<TriggerInfo> { name: String, module: String,
                                           //   lo: Vec<u8>, hi: Vec<u8> (empty = open), mode: TriggerMode::{Keys, Changes},
                                           //   pending: u64, last_error: Option<String> }
```

The full contracts are on [Roles and lifecycles](wasm-roles.md) and [Triggers](triggers.md).

## Change stream

```
let mut sub: Subscription = db.subscribe(b"orders/", Some(b"orders0"))?;
sub.start_seqno();                                   // everything strictly above flows
loop {
    match sub.recv_timeout(Duration::from_secs(1))? {
        None => continue,                            // timeout
        Some(StreamEvent::Batch(entries)) => for e in entries {
            // StreamEntry { key, seqno, commit_seqno, kind: Put|Delete, value: Option<Vec<u8>> }
        },
        Some(StreamEvent::Lagged) => break,          // queue cap exceeded; re-subscribe
    }
}
```

Delivery is post-commit, seqno-ascending and gap-free past `start_seqno`, and values arrive resolved. `seqno` is the op's own. `commit_seqno` is the last seqno of the atomic commit the op belonged to, which is the one state in which the op became visible; `snapshot_at(commit_seqno)` reads it. Value-log GC relocations re-put a live value through the write path, so they show up as `Put` entries carrying the unchanged value, which is harmless for any consumer. Drop subscriptions you stop consuming, since an undropped one holds a GC pin. The journal, GraphQL subscriptions and replication are all built on this stream.

## Identity

```
if let Some(id) = db.identity() {
    id.name; id.instance_id; id.instance_hex();
    id.parent;   // Option<(InstanceId, cut_seqno)>
}
```

## Errors

| Error | Meaning | What to do |
|---|---|---|
| `Io(e)` | an OS-level failure | inspect it; a hard IO failure in the write path degrades the store |
| `Corruption(msg)` | on-disk data failed validation | stop; restore from a fork or the journal |
| `InvalidArgument(msg)` | a reserved key, a bad name, an unknown module, a seqno below the watermark, and so on | fix the call |
| `Conflict` | the transaction lost first-committer-wins | retry the whole read-modify-write |
| `Closed` | the database was shut down | nothing |
| `Background(msg)` | a background thread or the write path failed; writes and maintenance refuse until reopened, reads keep serving | reopen |
| `Wasm(msg)` | a compile error, trap, fuel or memory exhaustion, or `wasm_enabled = false` | fix the module or raise the limit |
| `GuestFailed { code: i32, output: Vec<u8> }` | the guest exited non-zero; `output` holds its message | an application-level failure |
| `ProvenanceMismatch(msg)` | replica data does not descend from the expected instance | re-attach from scratch (the `edge` driver does this itself) |
| `Gone(msg)` | a replicated file left the master's live version | re-pull the slice |
| `JournalGap(msg)` | a middle journal segment is missing | restore the segment and rebuild again |

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Embedded API` in *Reference*
