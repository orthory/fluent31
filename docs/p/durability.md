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
| `fluent-graphql` | `--journal DIR` plus optional `--journal-rotate-bytes`, `--journal-compact-when-deltas-exceed`, `--journal-compact-min-bytes` |
| `fluent-server` | a `[journal]` section in the TOML config, with `dir` required |

## Rebuilding from the journal

```
fluent-cli journal-rebuild <journal-dir> <dest-dir>
# prints: source instance, base keys, deltas applied, last seqno
```

Or `fluent31::journal::rebuild(journal_dir, dest, opts)`, where `opts` are the rebuilt store's `Options` (give it a `store_name` for a fresh root identity). `dest` must be a fresh directory. The rebuilt store holds all user data as of the journal's last durable record, as a new lineage: seqnos are renumbered, the instance id is fresh, and modules, triggers, pins and forks are not restored, so redeploy them. A missing middle segment is refused (`JournalGap`), never rebuilt around.

The tail is approximate in both directions. The journal's last few unsynced records can be lost. And under `Periodic` or `Never`, the journal, which is fed from the in-memory commit stream, can hold writes the crashed store lost, so a rebuild is slightly ahead of what the store would have recovered. Both are acceptable. You reach for the journal only when the store itself is gone, and the rebuild replaces it.

## Backups

- For a consistent snapshot on the same filesystem, `fork(name)`. It is cheap, instant, consistent and hard-linked.
- To copy elsewhere, copy `archive/<name>/`, which is a plain directory tree (the hard links copy as full files), or `restore_to` into a mount point.
- For continuous off-box protection, ship the journal directory's segments. A reassembled journal is verified for contiguity at rebuild.

> **Never** delete `wal-*.log`, `MANIFEST-*`, `CURRENT` or `LOCK` by hand.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Durability & recovery` in *Reference*
