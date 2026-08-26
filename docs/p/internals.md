<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#internals -->

# Architecture

> The engine as implemented: the write path, the storage layout, the concurrency-control machinery, recovery, and the subsystems built on top of them. The behaviour documented elsewhere on this site follows from the mechanisms described here.

## Write path

A batch is placed before it is logged. Values at or above `value_threshold` are appended to the value-log head and the corresponding tree entry becomes a pointer; smaller values remain inline. The batch is then written to the write-ahead log, inserted into the memtable, and published by advancing the visible sequence number, so that no reader observes a partial batch.

Under `SyncMode::Always` a dedicated commit thread drains everything queued in each cycle and applies it in size-bounded chunks. Each chunk costs one value-log fsync and one write-ahead-log fsync, which the participating writers share; the steady-state group size therefore approaches the number of concurrent writers. Transactions are validated and applied inside the same critical section as plain writes, and are revalidated against batches applied earlier in the same group.

## Storage layout

The tree is laid out for lazy leveling: upper levels are merged tierwise, where a full level merges into a single run at the front of the level below, and the bottom level holds one leveled run. Runs are divided into key-bounded fragments of approximately `target_file_size`, each carrying its own bloom filter and index, sized so that the index and filter of an entire dataset can remain resident in memory.

Key-value separation keeps the tree small: compaction relocates pointers rather than payloads, so the cost of a merge is governed by key volume rather than value volume. The value log is reclaimed by a separate collector, which rewrites a file's live records through the ordinary write path, retires the file, and unlinks it only once no registered snapshot can still reach the superseded versions and the relocations are present in fsynced tables. The `vlog_retired` statistic counts the files held between those two conditions.

## Multi-version concurrency control

The garbage-collection watermark is the sequence number of the oldest registered snapshot; pins and stream subscriptions register in the same way. Compaction retains every version above the watermark, together with the newest version at or below it, and discards the remainder. Two consequences follow directly: a snapshot held for any key holds every version of every key, and a sequence number remains addressable only while it is above the watermark. Per-key retention policies therefore cannot exist.

Commit validation reads the newest committed version of each key read under `get_for_update` and each key in the write set, tombstones included, within the same critical section used by every other writer. The transaction's own snapshot bounds the watermark for its duration, so the evidence validation depends upon cannot be compacted away mid-transaction.

The versions the engine retains exist to serve in-flight readers and to validate commits, and they are discarded as soon as neither purpose requires them. Superseded versions are therefore not a record's history: they are unaddressable once the watermark passes them, they are renumbered wholesale by a journal rebuild, and a fork or restore mints a new identity for them. A history that must survive those events is written as data — by a changes-mode trigger, under keys chosen for the purpose — and is then subject to the same retention rules as any other data.

## Recovery

The manifest is a complete metadata snapshot, rewritten on each structural change and made current by an atomic rename of `CURRENT`. Table files are fsynced before any manifest references them, so a referenced file is always readable.

Recovery replays every write-ahead log at or above the manifest's floor, validates value-log pointers against the scanned prefix of each file, and truncates a torn tail on the newest log. The replayed memtable is flushed synchronously, so that a crash during recovery results only in a repeated replay. A fresh value-log head is opened rather than appended to, because the engine never resumes writing to a file that predates a crash. Orphaned files and partially built forks are swept at the same time.

## Module execution

Modules are compiled and run by wasmtime with fuel metering, a memory limit, NaN canonicalization, deterministic SIMD and no WASI imports. A query executes against a snapshot registered for the duration of the invocation. An executor runs inside a transaction; a commit conflict discards the instance and re-runs the invocation against a fresh snapshot with fresh memory, fuel and output.

Compiled modules are cached by content hash, while one-shot bytes are compiled without being cached, so that a stream of one-shot invocations cannot evict installed modules. Module bytes are stored at `\x00wasm\x00<name>` as ordinary versioned keys, which is what allows `query_at` to travel code and data together.

## Trigger capture and drain

Capture occurs inside the commit critical section. The keys of each committed batch are matched against the trigger registry and the resulting event records are appended to that same batch, so an event shares one write-ahead-log record and one sequence-number range with the write that caused it.

Keys-mode queues are addressed at `\x00trgq\x00<trigger>\x00<key>`, where the touched key is itself the queue entry, which is why repeated touches coalesce. Changes-mode queues are addressed at `\x00trgq\x00<trigger>\x00<seqno>` with the change as the value, which is why events remain ordered and are never coalesced.

A runner thread drains each backlog in chunks, as a system transaction pre-seeded with deletions of the consumed entries. System transactions are exempt from capture, which is the mechanism behind the no-stacking rule. Because the consumed queue keys are in the drain transaction's write set, a touch landing after the drain's snapshot conflicts the commit and the drain re-runs against fresh state; ordinary optimistic concurrency control closes the race.

## Fork construction

A fork at the head flushes the memtable, registers the cut under a brief hold of the manifest lock, hard-links every table and sealed value-log file, copies the value-log head up to its synced length, writes a fresh manifest and fork metadata, fsyncs, and publishes the result by a single rename. A fork at an earlier sequence number additionally rewrites the tables to that cut with one merge, retaining the newest version at or below the cut for each key, while values remain hard-linked. Pins are manifest records that re-register a snapshot at every open, before the background threads start.

## Change stream

A subscription taps the apply path immediately after the visible sequence number is advanced, so delivery is ordered and gap-free from the point of installation. Entries carry unresolved value-log pointers, which the consumer resolves off the write path under an advancing snapshot pin, ensuring that value-log collection cannot unlink a file still in flight. A subscriber exceeding `sub_queue_bytes` is dropped rather than allowed to stall writers. GraphQL subscriptions, the journal and replication are all consumers of this single stream.

## Instance identity and replication

The instance identifier is derived as a hash of the store name for a root store, and of the parent identifier, cut and fork name for a fork. Derivation is deterministic, so a crash between minting and persisting produces the same identifier again. File identifiers and offsets are unique only within one store lifetime; the instance identifier is the outer qualifier that every replica verifies on connection.

An edge replica copies the index fragments overlapping its scope, cross-checking bounds and sizes and verifying block checksums, applies the change stream into an overlay memtable, and resolves values inline first, then from its local cache, and finally by fetching from the master. Reads traverse the same merge and MVCC iterator stack as the engine's own.

## Threads and lock order

Each store runs the user's writer threads, one flush thread, one compaction thread that also performs value-log collection, the commit thread and the trigger runner. An attached journal adds a drainer thread, and the GraphQL plane adds one forwarder thread per active subscription. A background failure degrades the store rather than leaving waiters blocked. The lock order is strict: write, then manifest, then state, then snapshots.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Architecture` in *Reference*
