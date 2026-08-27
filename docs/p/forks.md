<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#forks -->

# Forks, pins, clones

> A fork is a named, consistent branch of the whole database, published as a complete database directory.

## What a fork is

Forks land under `<dir>/archive/<name>/`. Tables and sealed value-log files are immutable, so the fork hard-links them. Creation cost is proportional to the number of files, plus one bounded copy of the still-growing value-log head (at most `vlog_file_size`). Shared bytes exist once on disk. `du` on the archive re-counts shared inodes, so the apparent size is not the added size. Real divergence accrues only as parent and child compact away from the shared base.

A fork exists completely or not at all. It is built in a temporary directory, fsynced and published by a single rename, and a crashed build is swept at the next open.

## Cutting

| Call | Cut | Cost |
|---|---|---|
| `fork(name)` | the current flushed head | a memtable flush plus hard links |
| `fork_at(name, seqno)` | that exact seqno | the same, plus the table files are rewritten to the cut (values stay hard-linked) |

`fork_at` needs a point that is still materializable: the head, a seqno captured moments ago with `db.seqno()`, or one held by `pin(name)`. A pin is a durable, store-wide GC hold recorded in the manifest. It survives restarts and costs retention until `unpin`. Seqnos below the watermark are refused.

Live readers and writers keep running during a fork. What the store pays is one memtable flush, a brief hold of the manifest lock (structural installs pause, traffic does not), and, because the cut is a registered snapshot for the build's duration, GC held at the cut and value-log deletions deferred until the build finishes.

## The API

```
let f: ForkInfo = db.fork("before-migration")?;
// ForkInfo { name, instance_id, created_unix_ms, last_seqno, path }
let clone = Db::open(&f.path, Options::default())?;      // live CoW clone

let p: PinInfo = db.pin("pre-import")?;                  // durable store-wide GC hold
let f = db.fork_at("rollback", p.seqno)?;                // cut exactly there
db.unpin("pre-import")?;
db.pins();                                               // Vec<PinInfo>, oldest first

let s = db.seqno();                                      // capture "now"
let a = db.fork_at("replica-a", s)?;
let b = db.fork_at("replica-b", s)?;                     // identical cuts

db.list_forks()?;  db.delete_fork("name")?;              // refused while the fork is open
fluent31::list_forks_at(Path::new("./data"))?;           // lock-free, works on a live store
fluent31::restore_to(&archive_path, &dest, Some("copy-name"))?;
```

Fork and pin names use `[A-Za-z0-9._-]`, at most 64 characters, with no leading dot. `restore_to` refuses an existing `dest` and an archive that has already been opened read-write (fork that live copy instead).

## Using a fork

- **Open equals activate.** `Db::open(fork.path, ..)` gives you a live, writable, copy-on-write clone. New writes land in its own files and its compactions unlink only its own links. The parent is untouched.
- `restore_to(archive, dest, new_name)` hard-links the archive into a fresh directory, or copies it when `dest` is on another filesystem, so the archived cut stays pristine. `new_name` is required for forks of a named store, since each copy mints its own identity.
- `delete_fork(name)` refuses while the fork is open as a database.
- `list_forks_at(dir)` reads `archive/*/fork.meta` without taking a lock, so it works on a store another process has open.
- Under the server, every fork is an instance at `/graphql/<instanceId>` with the same full surface, including its own forks.

> **Expectations.** Forks branch; they do not follow. A fork contains exactly the history up to its cut and nothing the parent commits afterwards. They are priced for a handful of deliberate cuts — a pre-migration anchor, a staging clone, a rollback point — not for per-document versioning. Pins hold GC for the whole store.

## Rolling back the primary

There is no in-place restore. A rollback swaps directories.

1. Before the risky change, `fork("pre-migration")`, or `pin` now and `fork_at` later. Rehearse the change on the fork's own instance.
2. To roll back, stop the process. Then either open the fork directly as the new primary (its first read-write open fixes its identity under the fork's name), or keep the archive pristine with `restore_to(archive, "<new-dir>", Some("prod-2"))` and start on `<new-dir>`. Pass no `store_name` on later opens, since the name is persisted.
3. The rolled-back primary has a new instance id. Every replica notices on its next connection and re-attaches from scratch; nothing else needs telling. Any stored seqnos are meaningless across the swap.
4. Delete the abandoned directory when you are sure.

The shell and GraphQL expose `fork` and `pin` but not restore. Step 2 is a filesystem operation or the Rust call.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Forks, pins, clones` in *Reference*
