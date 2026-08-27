<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Concepts · Human version: https://orthory.github.io/fluent31/#concept-forks -->

# Branching the database

> `fork("name")` publishes a complete, consistent copy of the whole database — at a cost proportional to the number of files, not the amount of data.

Tables and sealed value-log files are immutable, so a fork hard-links them and copies only the growing head. Shared bytes exist once on disk, and divergence accrues only as parent and child compact away from the shared base. What you get is not an export or a dump: it is a database directory. Open it and you have a live, writable, copy-on-write clone — its own modules, its own triggers, its own data, its own identity — while the parent keeps serving, untouched.

## What that makes cheap

- **Rehearsal.** Run the risky change against a fork first. Under the server every fork is a full instance at `/graphql/<instanceId>`, so "try it on production without trying it on production" is one mutation.
- **Rollback anchors.** Cut before the change; if it goes wrong, the rollback is a directory swap rather than a restore.
- **Second environments.** A staging clone with real data that cannot touch the original.
- **Backups.** A consistent snapshot on the same filesystem, instantly; copy the directory elsewhere at your leisure.

## Pins: a cut you can take later

`pin(name)` durably marks the current seqno as still-materializable, so `fork_at(name, seqno)` can cut exactly there afterwards. A pin is cheap to take and costs retention while it is held — it holds the GC watermark for the whole store, like any snapshot.

## Branches, not timelines

A fork contains exactly the history up to its cut and nothing the parent commits afterwards; forks branch, they do not follow. Each copy mints its own instance identity, which is how a replica notices that its master was replaced and re-attaches from scratch. And because a fork is a whole database, it is priced for a handful of deliberate cuts — not for per-record versioning, which is [something you make out of data](consistency.md).

> **Next** [Forks, pins, clones](forks.md) is the reference — cutting, restoring, rolling back the primary. [Forks in practice](ex-forks.md) is the playbook set.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Branching the database` in *Concepts*
