# fluent31 — design

An embedded LSM key-value engine in Rust with MVCC snapshots and optimistic
transactions, WiscKey-style key-value separation, io_uring-backed IO on
Linux, copy-on-write database forks, and an in-database WASM execution
layer that replaces SQL.

This document describes the system **as implemented**. It went through two
adversarial design-review rounds (60 findings) before implementation; the
fixes are baked in below and called out where the reasoning is subtle.

Contents: §1 on-disk layout · §2 write path · §3 tables · §4 LSM shape ·
§5 manifest/recovery · §6 MVCC & transactions · §7 IO backends · §8 value
log · §9 WASM layer · §10 forks · §11 concurrency & locks ·
§12 testing · §13 tooling & limits · §14 identity & edge replication.

---

## 1. On-disk layout

```
<dir>/
  LOCK                 # flock'd exclusively for the process lifetime
  CURRENT              # "MANIFEST-<gen>\n" (tmp + fsync + rename + dir fsync)
  MANIFEST-<gen>       # full metadata snapshot (§5)
  wal-<id>.log         # write-ahead logs, one per memtable generation
  sst-<id>.tbl         # immutable table fragments (§3)
  vlog-<id>.vlog       # value-log files (§8); one active head, rest sealed
  archive/<name>/      # forks — each one a complete DB directory (§10)
```

All integers little-endian; all checksums CRC32C. One `next_file_id`
counter names every wal/sst/vlog file and run id; ids are never reused
(`create_new` semantics catch collisions).

**Internal keys.** `user_key ++ trailer(8B)`, `trailer = (seqno << 8) |
kind` big-endian; ordering `(user_key asc, seqno desc, kind desc)` via an
explicit comparator. `kind`: 1 = Put, 0 = Delete; trailer kind `0xff` is a
seek sentinel that sorts before every real entry at the same seqno, so
`lower_bound(seek(k, s))` lands exactly on the newest version of `k` with
`seqno <= s` — the MVCC read primitive everywhere (memtable, block, table).
Seqnos are 56-bit.

**Reserved keyspace.** User keys must be non-empty and must not start with
`0x00`. `\x00wasm\x00<name>` stores installed WASM modules — through the
same MVCC/WAL machinery, so modules are durable, versioned, and recovered
for free. Public API and WASM ABI both reject/clamp the reserved prefix.

## 2. Write path

`WriteBatch` is the atomic unit. Under the write mutex:

1. **Value placement**: values `>= value_threshold` (default 4 KiB) are
   appended to the vlog head as `[crc32c][klen][vlen][key][value]` records;
   the LSM entry becomes a pointer `{file, offset, record_len}`. Smaller
   values stay inline. (`0` separates everything; `usize::MAX` disables.)
2. **Durability ordering**: if any value went to the vlog and
   `SyncMode::Always`, `vlog.sync_head()` runs **before** the WAL append —
   a durable pointer never precedes its payload.
3. WAL record `[len u32][crc32c u32][payload]`, payload =
   `[base_seqno][count]` + per-op `[kind][key][repr]` (reprs, not raw
   values — recovery replays pointers verbatim). fsync per SyncMode.
4. Memtable inserts (crossbeam skiplist keyed by internal key), then
   `visible_seqno.store(base + n - 1, Release)` — readers load the visible
   seqno before filtering, so partially applied batches are invisible.
5. Rotations: memtable at `memtable_size` (freeze → new WAL → flush queue);
   vlog head at `vlog_file_size` (seal+fsync → fresh head published in a new
   Version; **no manifest write** — recovery adopts young vlog files, §5).

WAL rotation **seals** the old file with fdatasync and the new WAL's
directory entry is fsynced before any write to it is acknowledged. Writers
stall (100ms poll on the progress signal) when frozen memtables pile past
`max_immutable_memtables` or L0 exceeds `l0_stall_trigger`.

## 3. Tables (sorted-run fragments)

`[data block]* [filter block] [index block] [stats block] [footer 48B]`,
every block carrying `[compression=0 u8][crc32c u32]`.

- Data block (~8 KiB): `[iklen][rlen][ikey][repr]` entries + a `u32` offset
  array for in-block binary search.
- Filter: one bloom per fragment over user keys (10 bits/key, consecutive
  duplicates deduped). Fragments are bounded (§4), so blooms/indexes stay
  small and are pinned per open table.
- Stats: entry/tombstone counts, min/max seqno, first/last internal key.
- `TableBuilder::finish` **fdatasyncs before returning** — a table is
  durable before any manifest can reference it.

Reads go through a 16-shard LRU block cache keyed `(file_id, offset)`;
vlog records ≤ 64 KiB are admitted into the same cache.

## 4. LSM shape: lazy leveling with fragmented runs

A **run** is a sequence of key-ordered, non-overlapping fragment files
(split at user-key boundaries, ~`target_file_size` each). A **Version** is
the immutable levels array (runs newest-first per level) plus the live
vlog-file set; it is published under the state lock and pinned by `Arc`.

- **Flush**: oldest frozen memtable → new run at the front of L0
  (all versions verbatim — flush never GCs).
- **Tier merge** (levels `i < last`): when level i reaches its trigger
  (`l0_compaction_trigger` / `tier_width`), ALL of its runs (pinned at pick
  time) merge into one run inserted at the **front** of level i+1.
  Installation removes exactly the pinned inputs, so flushes prepending to
  L0 mid-merge are safe. Full-tier merges preserve the newest-first
  recency invariant.
- **Bottom level**: whenever it holds ≥ 2 runs, everything merges into one
  (leveling at the bottom).
- **Point lookup**: memtable → frozen → runs newest-first; each run binary
  searches its single candidate fragment after a bloom + range check; the
  first version with `seqno <= snap` wins; a run whose versions are all
  newer than the snapshot does **not** terminate the search.

**Merge-time GC** (watermark `W` = min registered snapshot, else
`visible+1`, both read under the snapshot-list lock): per user key keep all
versions `> W` plus the newest `<= W`; drop the rest (each dropped pointer
credits `discard[vlog_file] += record_len`). A tombstone that is the kept
version is dropped entirely iff **no strictly-older run can contain the
key** — checked by probing the bloom filters of the destination level's
current runs and every deeper run (design-review fix: the *destination
level's* older siblings matter, not just deeper levels; bloom-negative is
proof of absence, false positives just keep the tombstone).

## 5. Manifest & recovery

Full-snapshot manifest per metadata change:
`{next_file_id, last_flushed_seqno, wal_floor, levels[runs[table_ids]],
vlog_live, vlog_head, vlog_retired[(id, seqno)], discard[(id, bytes)]}`
with magic/format/CRC framing. Commit ordering, always: table files
fdatasynced → dir fsync → `MANIFEST-<gen+1>` written + fsynced → dir fsync
→ CURRENT flipped (tmp + fsync + rename + dir fsync) → obsolete files
dropped. sst/vlog unlinks happen **only in handle `Drop`** after
`mark_obsolete` — pinned Versions (readers, forks) keep paths alive
for hard-linking; WALs/manifests are deleted by path (never linked).

**`wal_floor`**: WALs with id ≥ floor are live; recovery replays every
`wal-*.log` with id ≥ floor **present in the directory**, in id order —
recovery never deletes a WAL above the floor, which closes the
rotation-vs-GC data-loss window found in design review.

**Recovery** (`Db::open`):
1. flock LOCK; load CURRENT → manifest; delete orphaned `MANIFEST-*`.
2. Open all referenced tables (self-describing; manifest stores only ids).
3. Vlog: open the manifest's live set, then **adopt young files**
   (id ≥ manifest's head id — vlog rotation doesn't flip the manifest);
   scan each young file's valid record prefix.
4. Replay WALs ≥ floor. Pointer reprs into **young** vlog files are
   validated against the scanned prefix (offset+len+**embedded key**
   match); a mismatch is torn-tail loss → replay stops globally. CRC
   damage in a sealed (non-newest) WAL is a hard corruption error — sealed
   WALs were fdatasynced at rotation, prefix semantics stay honest. A
   zero-filled tail region classifies as torn (an all-zero header would
   otherwise pass the CRC check). The newest WAL's torn tail is
   **truncated on the spot** — otherwise a crash later in recovery would
   leave a file that the next open misreads as damaged-sealed, bricking
   the store.
5. The replayed memtable is **flushed synchronously** — it is tagged with
   the newest replayed WAL id, so the flush's own manifest write records
   the recovery SST and advances the floor past every replayed WAL
   *atomically* (a crash in between can only re-replay, never duplicate).
   A **fresh vlog head** is then created — the engine never appends to a
   file that predates a crash (kills the offset-reuse aliasing bug class;
   the key check in every vlog read is the second layer).
6. Startup GC: delete sst/vlog/wal files no durable state references, and
   sweep crashed fork builds (`archive/.tmp-*`).

## 6. MVCC, snapshots, transactions

- **ReadView** = `{mem, imms, version}` Arcs cloned under the state read
  lock, then `visible_seqno` loaded **after** cloning — a pinned older
  structure still contains anything a later GC dropped, so unregistered
  reads are safe (design-review fix for the load-then-clone race).
- **Snapshots** register their seqno in a refcounted list; the seqno load
  happens inside the same critical section the watermark reader uses, so a
  snapshot can never materialize below an already-computed watermark.
- **Iterators**: K-way linear-scan merge over memtables + all runs in
  internal-key order. Forward visibility: first version ≤ snap per key,
  tombstones skip the key. Reverse: versions arrive oldest-first per key,
  so a candidate is overwritten while `seqno <= snap` and emitted at the
  key boundary. Bounds `[lo, hi)` both directions; `DbIterator` prefetches
  a window (32 entries / 256 KiB), groups pending vlog pointers by file and
  resolves each group with one batched read (§7).
- **Transactions** (OCC, snapshot isolation): buffered write set +
  `get_for_update` conflict set; reads overlay the write set; `iter()`
  captures the overlay at creation. **Commit validates and applies inside
  one write-mutex critical section** — atomic against every writer
  including plain `db.put` (design-review fix). Validation reads each
  key's newest committed version **including tombstones** (a committed
  delete conflicts); GC can't remove the evidence because the txn's own
  registered snapshot bounds the watermark. First committer wins;
  `Error::Conflict` aborts cleanly.

## 7. IO backends

Data plane behind `Io`/`DbFile` traits: `read_at`/`read_exact_at`,
batched `read_many(&mut [ReadReq])`, sequential `append`, `sync_data`.

- **std** (everywhere): pread/pwrite via `FileExt`.
- **io_uring** (Linux, `IoBackend::Auto` probes and falls back): single
  reads, appends and fsyncs stay plain syscalls — a shared mutexed ring
  would serialize foreground reads behind background batches (design
  review killed the naive shared-ring plan). `read_many` grabs a ring from
  a 4-ring pool, submits the whole batch in one `io_uring_enter`, reaps
  all completions, and finishes rare short reads synchronously. Ring users:
  scan/vlog batch resolution and compaction-adjacent readahead. One ring
  serves one batch at a time — no cross-thread completion dispatch.

Control plane (rename, hard_link, dir fsync, prefix copy) is plain std::fs.

## 8. Value log

Goal: the tree holds keys + pointers, so blooms/indexes for the whole
dataset stay memory-resident, and compaction moves pointers, not payloads.

- Records embed their key; **every dereference verifies CRC + key match**,
  so a dangling/aliased pointer is a loud `Corruption`, never another
  key's bytes.
- Flush syncs the head before its manifest flip: no durable SST pointer
  without a durable payload, in every sync mode.
- **GC** (`gc_vlog`, auto after compaction passes + manual): victim = the
  sealed live file with the highest discard ratio ≥ `vlog_gc_ratio`.
  Its records are scanned, sorted by key, and relocated in chunks of 256:
  each chunk takes the write mutex once, re-checks pointer equality
  against the *current* newest version (under the mutex — atomic vs all
  writers, no OCC conflicts with user txns needed for correctness of GC
  itself), and re-puts still-live values through the normal write path.
  Before the retirement manifest flip, the vlog head and WAL are synced —
  a durable "victim disowned" record must never precede the durability of
  the relocations it depends on. Retirement seqno `S` = visible seqno
  after the last chunk; every shadow of a victim-pointing version
  therefore has seqno ≤ S.
- **Retirement ≠ removal from resolution** (code-review fix): the victim
  stays in `Version::vlogs` — snapshots at/below `S` still resolve old
  versions that point into it. It leaves the resolution map only when the
  **deletion gates** pass: (a) the snapshot watermark strictly above `S` —
  no snapshot can resolve a version that dereferences the victim — **and**
  (b) `last_flushed_seqno >= S` — the relocations are in fsynced tables,
  so no crash can resurrect pointers into a deleted file (the BadgerDB
  bug class). Only then is the handle dropped and `mark_obsolete`d;
  physical unlink happens on the last Arc drop, so long scans and
  fork creation holding old ReadViews pins the file regardless. At reopen,
  gated victims are re-adopted as resolvable-but-retired (never back into
  `vlog_live`).
- Discard stats are persisted in the same manifest flip as the compaction
  that produced them. Known limitation: stats lag under lazy leveling
  (old pointers surface only when merges reach them); `stats` exposes
  pending-retired and discardable bytes.

## 9. WASM execution layer ("fluentabi v1")

wasmtime, cranelift, fuel metering (default 1e9/invocation), memory cap
via StoreLimits (64 MiB), NaN canonicalization, deterministic relaxed-SIMD,
no WASI — the only imports are the `fluent` module. Compiled modules are
LRU-cached by content hash (`wasm_module_cache` entries).

- `install_module` compiles/validates first (must export `run() -> i32`
  and `memory`), then stores bytes at `\x00wasm\x00<name>`.
- **Queries** run read-only against a registered snapshot (pinned for the
  whole invocation, so GC can't outrun a slow guest). `query_at`
  time-travels: module bytes AND data resolve at the given snapshot.
- **Executors** run inside a fresh `Txn`; guest exit 0 → commit, non-zero
  → abort with `GuestFailed{code, output}`. On commit conflict the entire
  attempt is discarded and re-run — fresh Store, fresh Txn/snapshot (which
  re-resolves the module), fresh fuel, fresh output — up to
  `execute_retries` times.

ABI (all lengths/pointers u32; memory-safety violations trap; semantic
misuse returns errnos `NOT_FOUND -1, EROFS -2, EINVAL -3, ENOSPC -4,
EBADF -5, ELIMIT -6, EIO -8`):

```
input_len / input_read(dst, cap, off)         output_write(ptr, len)
log(level, ptr, len)
get(k, klen, off, buf, cap) -> i64            # returns FULL length; copies
get_for_update(...same...)                    #   min(cap, len-off) from off —
put(k, klen, v, vlen)   delete(k, klen)       #   values bigger than guest
scan_open(lo, llen, hi, hlen, flags) -> h     #   memory read in chunks
scan_next(h, buf, cap) -> bytes               # packs [klen][vlen][k][v]* —
scan_entry_hint(h) -> exact next entry size   #   a batch per boundary
scan_skip(h)   scan_close(h)                  #   crossing ("data-wise")
```

Host-side resource caps (all `Options`): input ≤ `max_wasm_input`, output
≤ `max_wasm_output`, log ≤ `max_wasm_log`, open scans ≤ `max_wasm_scans`,
executor write set ≤ `max_txn_write_bytes`. Scan handles live in the
per-invocation store context — every exit path (traps included) drops
them. The reserved keyspace is invisible through the ABI: writes EINVAL,
scans clamped to the user keyspace.

Guest SDK: `fluent-guest` (safe wrappers, growable scan batches, chunked
big-value reads, `fluent_main!`). Example guests in `guests/`:
**agg** (prefix count/sum/min/max — the `SELECT agg WHERE prefix`
replacement), **transfer** (balance transfer via `get_for_update` —
the stored procedure replacement, exercised concurrently in tests), and
**customer_index** (a trigger-maintained secondary index — the
`CREATE INDEX` replacement).

### Write-range triggers (`trigger.rs`)

The async fan-out from committed writes to executor modules — the
schema-free replacement for SQL indexes/materialized views. A trigger is
`(name, module, [lo, hi))`, persisted at `\x00trg\x00<name>` and mirrored
in an in-memory registry (an `Arc<Vec<_>>` swapped whole on create/delete;
the write path pays one read-lock clone per batch, nothing when empty).

**Capture.** `write_batch` and `Txn::commit` match their *logical* write
keys against the registry and append event records to the same batch —
same WAL record, same seqno range, same crash atomicity: "the write
happened" and "the trigger owes a run" are inseparable. Engine-internal
writes bypass capture entirely (vlog-GC relocations rewrite placement, not
state; system-txn commits — see below — enforce no-stacking).

**Queue.** The event key is `\x00trgq\x00<name>\x00<touched user key>` with
an empty value: the touched key IS the queue key, so a hot key coalesces
to one pending event no matter how far the runner lags, and queue depth is
bounded by distinct touched keys. This encodes the delivery semantic: an
event means "reconcile this key against current state", not "replay this
op" — modules read the key and converge, making replays and reordering
harmless by construction.

**Runner.** A dedicated thread (parked on a signal notified by capturing
commits) discovers backlogged triggers by skip-seeking the queue prefix,
then drains each in chunks of `trigger_batch`: it invokes the module as a
normal executor whose transaction is pre-seeded — marked *system* and
carrying deletes of the consumed queue entries. The module's writes and
the queue consumption therefore commit atomically: invocation is
at-least-once (a crash mid-run re-fires), effects are exactly-once. The
consumed entries sit in the transaction's write set, so OCC closes the
drain race for free: a re-touch after the drain snapshot rewrites the
queue key, conflicts the commit, and the attempt re-runs against a fresh
snapshot. System-txn commits skip capture, so a trigger's own writes never
enqueue events — no cascades, no loops, by decree.

**Failure.** A failing module (guest error, missing module, conflict
exhaustion) never loses events: the batch stays queued, the runner backs
off exponentially (100ms → 6.4s) per trigger, and `list_triggers` exposes
`pending` + `last_error`. Queue entries whose trigger no longer exists
(create/delete race, crash mid-delete) are garbage-collected by the
runner. Recovery needs no machinery: events are ordinary durable keys.

## 10. Forks (explicit, manual)

Not PITR: there is no continuous log archiving and no
restore-to-arbitrary-time — a fork captures an explicitly named cut. What
a fork is: an MVCC-pinned snapshot materialized as hard links, so
creation copies almost nothing (one bounded head copy) and leaves live
readers and writers essentially undisturbed — the cost is a memtable
flush, a brief manifest-lock hold, and vlog GC deletions deferred for the
duration of the build.

`db.fork(name)`:
1. Flush everything (freeze + drain the frozen queue).
2. Under the manifest lock (freezes structure): clone manifest data, take
   a ReadView, `cut = last_flushed_seqno`. Register a snapshot at `cut` —
   this blocks every *future* vlog retirement (their `S` ≥ any current
   visible ≥ cut ⇒ gate `watermark > S` can't pass), while victims retired
   with `S ≤ cut` are provably unreferenced by the archive. Victims
   retired-but-undeleted stay linkable because the pinned Version holds
   their Arc handles.
3. Build `archive/.tmp-<name>/`: hard-link every table fragment and sealed
   vlog file (immutable; paths alive via the pinned Version), **copy** the
   head vlog up to its synced length (fresh `sync_head` first — covers
   every pointer at the cut; hard-linking a growing file would share the
   inode with the parent's future appends), write an archive manifest
   (levels from the pinned view, `wal_floor = next_file_id` ⇒ no WALs,
   retired list emptied) and `fork.meta` — which records a freshly minted
   `instance_id`, the fork's stable routing handle (servers address the
   fork by it, e.g. GraphQL at `/graphql/<instanceId>`; recreating a
   same-named fork mints a new id so stale handles can't alias).
4. fsync every written file + the tmp dir, rename to `archive/<name>`,
   fsync `archive/` — a fork either exists completely or not at all. A
   build that fails partway removes its own `.tmp` dir (the name stays
   immediately reusable); `.tmp` dirs orphaned by a crash are swept at
   the next `Db::open`.

**Open = activate.** Every fork is a complete database; opening it
read-write gives a live copy-on-write clone (its compactions unlink only
its own hard links). `delete_fork` refuses if the fork is flock'd open as
a database. `clone_to` re-links a fork to a fresh directory — work on the
copy, keep the archived fork pristine.

## 11. Concurrency & locks

Threads: writers (user), one **flush** thread, one **compaction** thread
(also runs vlog GC + retirement gates). Background errors poison the DB
(`Error::Background`) rather than hanging waiters.

Lock order (strict): `write_mu → manifest → state → snapshots`, with
`gc_mu`/`compaction_mu` outermost within their flows. Never hold the state
guard while taking the manifest lock (a stats() violation deadlocked
exactly as predicted and is fixed). `compaction_mu` serializes the
maintenance thread and user `compact_all` — two concurrent pickers would
merge the same inputs.

## 12. Testing

72 tests. Unit: encodings, bloom FPR, blocks, WAL torn-tail/corruption,
manifest roundtrip, cache, memtable/iterator semantics. Integration: CRUD,
batch atomicity, snapshot isolation across flush+compaction, conflict
matrix (txn-txn, txn-vs-plain-put, tombstone conflicts, write-skew defense),
fwd/rev/bounded iterators across mixed memtable/table state, WAL replay,
torn-tail truncation, vlog roundtrip + GC + reopen, fork
create/open/delete + GC interleaving, double-open lock, and a
**randomized model test**: 4 000 seeded ops mirrored against a `BTreeMap`
with interleaved flushes, compactions, GC, snapshots, bounded scans both
directions, and full reopens — exact-equality asserted throughout. WASM:
WAT-based ABI conformance (EROFS, EINVAL on reserved keys, fuel trap,
OOB trap, ENOSPC), module versioning/time travel, persistence across
reopen, and the real Rust guests end-to-end (agg over 600 keys; 4 threads
× 50 concurrent transfers preserving Σbalances under OCC retries).
On Linux the same suite runs with io_uring engaged (`IoBackend::Auto`)
plus an explicit uring-selection test; under Docker the default seccomp
profile blocks io_uring — run with `--security-opt seccomp=unconfined`.

## 13. Tooling, features, limits

- `fluent-cli`: interactive shell (get/put/del/scan/count, txns,
  snapshots, install/query/exec, forks, flush/compact/gc/stats) —
  every command prints its latency.
- Cargo features: `wasm` (default) gates wasmtime; `--no-default-features`
  builds the pure storage engine (used to cross-check the uring backend).
- Group commit (committer-thread pipeline): `SyncMode::Always` batch
  writers enqueue on a commit queue and park; a dedicated
  `fluent31-commit` thread drains EVERYTHING queued each cycle, applies it
  in cap-bounded chunks (1 MiB, or first+128 KiB when the front batch is
  small) under one `write_mu` section per chunk with ONE vlog fsync + ONE
  WAL fsync, delivers results, and immediately drains again. While an
  fsync is in flight every active writer has time to enqueue, so
  steady-state group size approaches the number of in-flight writers —
  throughput scales with client concurrency instead of convoying on
  fsyncs (this out-groups LevelDB's leader/follower design, which loses
  batches to leader-election gaps). Each batch keeps its own WAL record,
  contiguous seqno range, and all-or-nothing atomicity;
  `DbStats.commit_{groups,batches}` and `wal_syncs` expose the
  amortization. `SyncMode::Never` and `SyncMode::Periodic` writers take a
  direct path (no inline fsyncs to amortize; Periodic rides a background
  timer on the commit thread, bounding crash loss to its interval, with
  `Db::sync_wal` / GraphQL `syncWal` as the explicit barrier), as does GC relocation, which validates
  liveness under its own `write_mu` section. OCC transaction commits DO
  group under `SyncMode::Always`: the committer performs validation and
  application in one `write_mu` critical section per chunk, checking each
  transaction's read/lock set against the store view AND the writes of
  earlier batches in the same fsync group (in-group revalidation) — so
  concurrent transactions and `wasmExecute` executors share fsyncs like
  plain writers. Relaxed modes keep the direct commit path. Hard IO failures in the write
  path degrade the store (`bg_error`) instead of leaving WAL/vlog state
  ambiguous; a committer panic fails the in-flight group (unwind guard)
  and parked writers poll `bg_error`, so client threads can never hang.
- Known v1 limits (documented, deliberate): no block compression
  (format-versioned for later); discard-stat lag under lazy leveling (no sampling fallback yet);
  GC relocations bump seqnos, so a hot large-value key can cost a user txn
  a retry; fixed `max_levels` (no dynamic depth); bottom merges rewrite the
  whole bottom level (fragments bound file sizes, not total merge work).

## 14. Store identity & edge replication

**Identity** (`identity.rs`, manifest format 2). A store carries an
operator-chosen name and a deterministic 128-bit `instance_id`:
`H(name)` at creation, `H(parent_id ‖ cut_seqno ‖ name)` when a
fork mints (first read-write open of an archive consumes a
`pending_fork` marker; `restore_to` requires a fresh name per copy so
two restores of one archive can never mint the same id). Determinism
makes minting crash-safe with no commit ordering — a crash before the
persisting manifest write re-mints the identical id — and makes the
lineage chain verifiable from metadata. Uniqueness is an operator
contract (fleet-unique names), like hostnames. Normal restarts keep the
id: file ids are monotonic and recovery never appends to a pre-crash
vlog file, so `(file, offset)` never aliases within one lifetime.
Unnamed stores stay on manifest format 1, readable by older binaries.

**Why identity matters**: replication copies bytes across machines and
names them by `(file id, offset)` — coordinates unique only within one
store lifetime. The instance id is the outer qualifier; every replica
connection verifies it by equality, and a re-minted master (restore,
fork, replacement) invalidates every remote cache wholesale instead of
silently serving a divergent history.

**Master surface** (db.rs replication section). `Db::subscribe(lo, hi)`
taps both apply paths under `write_mu` right after `visible_seqno`
publication: delivery is seqno-ordered and gap-free past the
subscription's start (installation also holds `write_mu`). Entries
carry unresolved reprs; the consumer resolves values off the write path
against a `ReadView`, protected by an **advancing registered-snapshot
pin** — a vlog GC victim retired at seqno S is deletable only once the
watermark passes S, and the pin sits at-or-below the oldest unconsumed
entry, so streamed pointers always resolve. A subscriber that exceeds
`sub_queue_bytes` is cut off (Lagged) rather than stalling writers.
`Db::slice_manifest(lo, hi)` flushes, then reports per run only the
fragments overlapping the range (id, size, key bounds from pinned
stats); `read_table_chunk`/`read_vlog_chunk` serve raw bytes from the
live version and answer `Error::Gone` once compaction/GC dropped the
file — slice readers pin nothing on the master, by design.

**Edge store** (`edge.rs`) + **channel** (`fluent-replication`,
REPLICATION.md). The edge copies overlapping fragments locally (bounds +
size cross-checked at install, blocks CRC-verified as always), applies
the stream into an in-memory overlay memtable, and resolves values
inline → local record cache → `ValueFetcher` reach-back; every record
re-verifies CRC + embedded key before serving or caching. Reads reuse
the engine's merge/MVCC iterator stack at `MAX_SEQNO` over
overlay + scoped runs. Slice refreshes prune the overlay to the new
flush watermark. The replica serves standard wire-v1 reads through
`fluent_wire::WireBackend`; writes answer INVALID. Re-sync after
lag/disconnect keeps all local caches (same instance id); a provenance
mismatch wipes and re-attaches behind an atomic store swap.
