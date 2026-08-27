<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#durability -->

# Durability & recovery

> What an ack means, what survives a crash, and the journal for the day the store directory itself is lost.

## Sync modes

`Options::sync` decides when writes reach stable storage. The server flags spell these `always`, `periodic:<ms>` and `never`.

| SyncMode | An ack means | Crash loss |
|---|---|---|
| `Always` (default) | fsynced. Concurrent writers share one fsync (group commit). | none of what was acked |
| `Periodic { every }` | in memory; a background timer fsyncs on that interval. `db.sync_wal()` is the on-demand barrier. | up to one interval |
| `Never` | in memory; the OS flushes when it likes. | the recent tail |

## What survives a crash

Under `SyncMode::Always`, every acked write survives. A value-log payload is synced before the WAL record that points at it, so a durable pointer never precedes its data. Under `Periodic`, everything up to the last timer tick or `sync_wal` survives. Under `Never`, whatever the OS had flushed.

In every mode the store reopens consistent. The WAL's torn tail is truncated, tables are self-describing and synced before the manifest references them, and the manifest flips atomically. Corruption in a sealed file is a hard `Corruption` error, never silent.

The test suite proves this with a SIGKILLed child process (`crash_recovery`), a fault-injecting IO backend (`fault_injection`, which shows a failed fsync is never a false ack) and a byte-mutation sweep (`corruption_fuzz`, which shows no on-disk byte can panic the reader).

## Degraded state

A hard IO failure in the write path or a background thread failure sets a store-wide error. After that, writes, `flush`, `sync_wal`, `pin` and subscriptions return `Error::Background`, while `get`, `iter` and snapshots keep serving what is there. Reopen the store; recovery brings it back to the last durable state.

## The journal

The store's own WAL and manifest are its durability. The journal is for the day that is not enough: a bad disk block, a truncated file, a lost directory. It is off unless you attach it, and it never sits on the commit path.

At attach it writes a base snapshot of the user keyspace, then trails the change stream on a background thread, appending each mutation to `journal-*.log` segments that rotate at `rotate_bytes`. Once the delta bytes written since the last base exceed `compact_when_deltas_exceed` times that base's size, and also exceed `compact_min_bytes`, it writes a fresh base and prunes the superseded segments, so disk stays near the live set plus one window of recent deltas. If the consumer ever lags past `sub_queue_bytes`, it heals by writing a new base. The log header records the source instance id, and a different store's journal in the same directory is refused.

```
use fluent31::{Journal, JournalConfig, journal};

let db = Arc::new(Db::open(dir, opts)?);
let j = Journal::attach(db.clone(), "./journal")?;       // base snapshot now, deltas trail
let j = Journal::attach_with_config(db.clone(), dir, JournalConfig {
    rotate_bytes: 128 << 20,
    compact_when_deltas_exceed: Some(1.0),               // None = manual only
    compact_min_bytes: 64 << 20,
})?;
j.stats();               // deltas_written, base_records_written, last_seqno,
                         // rebaselines, compactions, files_pruned, last_error
j.request_checkpoint();  // compact now
drop(j);                 // joins the drainer, final flush
```

How to attach it on each surface:

| Surface | How |
|---|---|
| Rust | `Journal::attach(db, dir)` or `attach_with_config` |
| `fluent-server` | `--journal DIR`, or a `[journal]` section in the TOML config with `dir` required; `rotate-bytes`, `compact-when-deltas-exceed` and `compact-min-bytes` tune it there |

### Observing the journal

An embedder that mirrors the journal somewhere else — another volume, an object store — attaches it with an observer instead of polling the directory. The observer hears every fact of the log's life, in the order it became true, and every fact is already durable on disk when it is reported: bytes of a file below a reported length are fsynced and will never change (the log is append-only; the one truncation it performs, a torn tail after a crash, happens before `attached` reports the file). So a mirror copies exactly the reported bytes, deletes exactly the reported files, and never re-reads or lists the source.

```
use fluent31::{Journal, JournalConfig, JournalObserver, journal};

struct Ship;
impl JournalObserver for Ship {
    fn attached(&self, dir: &Path, files: &[(u64, u64)]) {}       // (id, durable length) of every file present
    fn appended(&self, file: u64, durable_len: u64) {}              // file is fsynced through durable_len
    fn rotated(&self, sealed: u64, sealed_len: u64, next: u64) {}   // sealed is final; next is active
    fn pruned(&self, anchor: u64) {}                                // every id below anchor is deleted
    fn stopped(&self, error: Option<&str>) {}                       // None on a clean detach
}
let j = Journal::attach_observed(db.clone(), dir, JournalConfig::default(), Arc::new(Ship))?;
journal::log_file_name(14);          // "journal-000014.log" — the naming contract
journal::log_file_id("journal-000014.log"); // Some(14)
```

`attached` arrives on the attaching thread before `attach_observed` returns; everything after it on the journal's own thread. A compaction reports the anchor file's base as durable (`appended`) before it reports the superseded files gone (`pruned`), so a mirror that applies facts in order never holds only superseded files. Observers return promptly and do their I/O elsewhere — the drainer waits for them.

## Rebuilding from the journal

```
fluent-cli journal-rebuild <journal-dir> <dest-dir>
# prints: source instance, base keys, deltas applied, last seqno
```

Or `fluent31::journal::rebuild(journal_dir, dest, opts)`, where `opts` are the rebuilt store's `Options` (give it a `store_name` for a fresh root identity). `dest` must be absent or an empty directory; a directory that already holds anything is refused (`InvalidArgument`), never merged into. The rebuilt store holds all user data as of the journal's last durable record, as a new lineage: seqnos are renumbered, the instance id is fresh, and modules, triggers, pins and forks are not restored, so redeploy them. A missing middle segment is refused (`JournalGap`), never rebuilt around.

The tail is approximate in both directions. The journal's last few unsynced records can be lost. And under `Periodic` or `Never`, the journal, which is fed from the in-memory commit stream, can hold writes the crashed store lost, so a rebuild is slightly ahead of what the store would have recovered. Both are acceptable. You reach for the journal only when the store itself is gone, and the rebuild replaces it.

## Backups

- For a consistent snapshot on the same filesystem, `fork(name)`. It is cheap, instant, consistent and hard-linked.
- To copy elsewhere, copy `archive/<name>/`, which is a plain directory tree (the hard links copy as full files), or `restore_to` into a mount point.
- For continuous off-box protection, mirror the journal through a `JournalObserver` (above): it reports every durable byte and every deletion, so the mirror needs no polling. A reassembled journal is verified for contiguity at rebuild.

> **Never** delete `wal-*.log`, `MANIFEST-*`, `CURRENT` or `LOCK` by hand.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Durability & recovery` in *Reference*
