<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#operations -->

# Operations

> The directory on disk, the knobs that matter, what to watch, and the hard limits.

## Directory layout

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

One process per directory. Everything is CRC32C-checked. Don't hand-edit anything, and don't delete WALs or manifests.

## Sizing

| Knob | Raise it when | Lower it when |
|---|---|---|
| `memtable_size` | write bursts stall on flush | memory is tight |
| `block_cache_size` | the workload is read-heavy and the working set fits | memory is tight |
| `value_threshold` | values are small and scans should stay inline | values are large and the index should stay small |
| `compression = Lz4` | you are disk-bound with compressible values | you are CPU-bound |
| `vlog_gc_ratio` | you want less GC churn | you want space back sooner |
| `trigger_inline_value` | changes-mode consumers need payloads without a read | values are large and write amplification matters |
| `sub_queue_bytes` | subscribers are bursty | memory is tight |

Writers stall rather than fail when frozen memtables exceed `max_immutable_memtables` or L0 exceeds `l0_stall_trigger`. Sustained stalls mean compaction cannot keep up.

## Monitoring

- `stats` (engine, shell, GraphQL) reports the seqno, the memtable and level shape, value-log live, retired and discardable bytes, the cache hit rate, and group commit amortization. An edge replica reports through `EdgeStats` instead.
- `triggers` reports `pending` (the backlog depth) and `lastError` per trigger.
- `Journal::stats()`: `last_seqno` against `db.seqno()` is the journal lag; `last_error` is the last failure.
- `FLUENT31_WASM_LOG=1` prints guest `log` calls to stderr.

## Limits

|  |  |
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

Known limits (v1, deliberate): no block compression by default (LZ4 is opt-in); value-log discard statistics lag, since dead pointers are only discovered when compaction reaches them; GC relocations bump seqnos, so a hot large-value key can cost a transaction a retry; a fixed level count; and bottom-level merges rewrite the whole bottom level.

## Compatibility

Every `Options` field except `store_name` may change between opens of the same store, and `compression` affects only newly written tables. On-disk formats are versioned. An unnamed store stays on manifest format 1, a named one writes format 2, and pins bump it to 3; older binaries read only the formats they know. The replication protocol advertises its version in `HELLO`.

## Platform notes

`IoBackend::Auto` probes io_uring at open and falls back to portable IO; `stats.backend` tells you which one is active. Docker's default seccomp profile blocks io_uring, so use `--security-opt seccomp=unconfined` or `io-backend = "std"`. macOS uses portable IO throughout.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Operations` in *Reference*
