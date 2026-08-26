<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#ex-forks -->

# Forks in practice

> The category: everything you'd want a copy of production for — rehearsal, staging, rollback anchors, backups — priced at hard-link cost.

`fork("name")` publishes a complete, consistent database directory under `archive/<name>/`, built from hard links: cost proportional to the file count, not the data. Opening it gives a live, writable, copy-on-write clone with its own identity — modules, triggers and data included. The playbooks below are the category; [Forks, pins, clones](forks.md) has the mechanics.

## The pre-change anchor

Before anything risky — a migration, a bulk import, a new trigger over live data:

```
db.fork("pre-migration")?;               // anchor now
// or: pin now, decide later whether to materialize the cut
let p = db.pin("pre-import")?;
let f = db.fork_at("rollback", p.seqno)?;
```

If the change goes wrong, rollback is a directory swap: stop the process, open the fork (or `restore_to` a pristine copy) as the new primary. Replicas notice the new identity and re-attach on their own. If the change goes right, `delete_fork` and move on. A pin is the lighter anchor — a durable GC hold you can still `fork_at` later — but it costs store-wide retention until `unpin`.

## The rehearsal

Under the server, every fork is a full instance at `/graphql/<instanceId>` — same schema, same modules, its own data. That makes "try it on prod without trying it on prod" one mutation:

```
mutation { fork(name: "rehearsal") { instanceId } }
# run the migration one-shot against /graphql/<instanceId>, inspect the result,
# then run the same bytes against the primary — or delete the fork and rethink
```

This is the standing advice from [Migrations & one-shots](ex-oneshot.md): rehearse every migration on a fork first, with the exact bytes you will run for real.

## The staging clone

Open a fork and you have a second environment with real data that cannot touch the first: new writes land in the clone's own files, its compactions unlink only its own links, the parent is untouched. Forks branch, they do not follow — the clone sees nothing the parent commits after the cut, which is exactly what a staging environment wants.

## The backup

A fork *is* a consistent snapshot backup on the same filesystem, instantly. For an off-box copy, copy `archive/<name>/` — a plain directory tree — or `restore_to` onto a mount point; the hard links copy out as full files. For continuous off-box protection, that is the [journal's](durability.md) job, not a fork cadence.

## Identical cuts

Seeding two environments that must start byte-identical: capture one seqno, cut twice.

```
let s = db.seqno();
let a = db.fork_at("replica-a", s)?;
let b = db.fork_at("replica-b", s)?;      // same cut, same contents
```

## What forks are not for

Per-document versioning and point-in-time recovery. Forks and pins are coarse, named, and few — a handful of deliberate cuts, each holding GC for the whole store while it matters. If you need history per record, [materialize it with a changes-mode trigger](ex-views-feeds.md); that is the [consistency contract](consistency.md).

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Forks in practice` in *Recipes*
