<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Concepts · Human version: https://orthory.github.io/fluent31/#concept-triggers -->

# Reactive derived state

> Bind a module to a key range and the engine invokes it after every committed write into that range. Derived data maintains itself, whoever did the writing.

Indexes, materialized views, running aggregates, changefeeds, referential cleanup: all of them are the same shape — data derived from other data, which goes stale the moment someone writes without updating it. A trigger removes that "without". Plain puts, batches, transactions, executors and every network surface all fire it; no writer has to know the derived state exists.

What the trigger writes is ordinary keys under a prefix you choose, so derived data is scannable, replicable and subscribable exactly like everything else. There is no separate index structure to learn.

## Two contracts

The mode is chosen by which export the module carries, and it decides what your derived state is a function of.

- **Keys mode reconciles.** An event means "this key was touched — reconcile it", and re-touches of one key coalesce while a backlog exists. Right when the derived state is a function of *current* state, as an index is: read the key, upsert or remove the entry, converge.
- **Changes mode folds.** Every committed op arrives once, in commit order, carrying its kind and its value. Right when you need op kinds, ordering, or per-op deltas — feeds, exact aggregates, cascades — where coalescing would destroy information.

## Why the derived state can be trusted

Derived data is only worth maintaining if it cannot silently diverge from what it is derived from, and that is a property of where the work happens rather than of how carefully the module is written. An event is captured inside the commit that caused it, so a write that survives a crash has an event waiting for it afterwards and a write that does not leaves nothing behind. Consuming that event happens inside the module's own transaction, together with whatever the module writes, so the two cannot come apart: an aggregate folded this way is exact rather than approximately right, however many times the attempt is retried.

Two consequences follow. Writes made by a trigger generate no events of their own, so no chain of triggers can loop or amplify — a cascade runs once. And the price of all of it is that the work is asynchronous: derived state trails the base data by whatever is queued, and nothing waits for it. A failure holds the queue rather than dropping it, which makes a broken module a visible backlog rather than a silent hole.

> **Next** A trigger is a module, so [Extending with WASM](wasm-overview.md) comes next — what the consumer roles receive and how to write one. [Triggers](triggers.md) is the reference for the binding itself: registration, both modes in detail, the delivery guarantees and the drain loop.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Reactive derived state` in *Concepts*
