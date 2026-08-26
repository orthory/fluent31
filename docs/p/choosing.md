<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Recipes · Human version: https://orthory.github.io/fluent31/#choosing -->

# Choosing the shape

> Six shapes cover the work. Start from the category of database work you have, not from the tool.

| Your work | Shape | Why |
|---|---|---|
| read a key or a range you can name | plain `get` / `iter` / `scan` | already optimal; no module needed |
| a computation over many keys whose result is small | a [query module](ex-queries.md) | the data stays in the database; only the answer travels |
| a write whose correctness depends on what was read | an [executor](ex-executors.md) (or an embedded `Txn`) | OCC makes the invariant hold under concurrency |
| derived data that must stay current no matter who writes | a [trigger](ex-indexes.md) | the engine invokes it after every commit into the range |
| a change you make once — backfill, migration, repair | a [one-shot executor](ex-oneshot.md) | nothing installed; the committed writes are the only trace |
| a safety point, a second environment, a backup | a [fork or pin](ex-forks.md) | a complete consistent copy at hard-link cost |
| reads far from the store | a [replica or edge cache](replication.md) | read-only follower scoped to a key range |

## Module or app code?

If your process is the only writer and the logic fits there, the embedded API is enough — `db.begin()` gives you the same OCC transaction an executor gets. Write a module when one of three things is true:

- **The computation must run near the data.** An aggregate over a million keys should not ship a million values to the client for five numbers.
- **The invariant must hold for every writer.** An installed executor is the same logic on every surface — Rust, shell, GraphQL — so no caller can skip the constraint.
- **The logic must react to writes you don't control.** Only a trigger sees every commit into a range, whoever made it.

## Installed or one-shot?

Install what is part of the system: called repeatedly, backing a trigger, or exposed as a typed GraphQL field. Installed bytes are versioned in the store, recovered, forked and time-travelable, so the store can answer "what code ran here". One-shot what is an event: migrations, backfills, repairs. A one-shot leaves no record in the database — the script in your repo and its git history are the audit trail. Install the big audited migrations; one-shot the rest.

## Keys mode or changes mode?

The two trigger modes are two different contracts, and the choice is about what your derived state is a function of.

- **Keys mode reconciles.** An event means "this key was touched — reconcile it". Re-touches coalesce. Right when derived state is a function of *current* state, as an index is: read the key, upsert or remove the entry, converge.
- **Changes mode folds.** Every committed op arrives once, in order, with its value. Right when you need op kinds, ordering, or per-op deltas — feeds, exact aggregates, cascades — where coalescing would destroy information.

Rule of thumb: if a replay of the same event must be harmless by *re-reading*, use keys mode; if it must be harmless by *overwriting the same derived keys*, use changes mode.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Choosing the shape` in *Recipes*
