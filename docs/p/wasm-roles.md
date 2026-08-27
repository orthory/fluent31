<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-roles -->

# Roles and lifecycles

> A module's exports decide what it is. Each role has its own input, its own execution context, and its own rules about what happens when it fails.

## Required exports

```
"memory"     the guest's linear memory                  // required
"query"      () -> i32   read-only entry
"execute"    () -> i32   transactional entry
"on_touch"   () -> i32   keys-mode trigger consumer
"on_apply"   () -> i32   changes-mode trigger consumer
"describe"   () -> i32   typed GraphQL descriptor    // optional
```

Install is rejected unless the module exports `memory` and at least one role entry. Entries take no parameters and return an `i32` exit code; everything they receive and everything they return travels through the host calls. One binary may carry any combination — a module that maintains an index and also answers questions about it is one artifact, not two.

## The five roles

| Export | Input it receives | Context it runs in | Failure |
|---|---|---|---|
| `query` | the caller's bytes | one snapshot pinned for the whole invocation; writes return `EROFS` | non-zero exit → `GuestFailed`; nothing to roll back |
| `execute` | the caller's bytes | a fresh transaction per attempt; reads see its snapshot plus the transaction's own buffered writes | exit 0 commits; anything else aborts the transaction → `GuestFailed` |
| `on_touch` | the touched keys, coalesced, unordered, no values, up to `trigger_batch` | an executor the engine invokes; its writes and the events' consumption commit together | the batch stays queued and the runner backs off; visible as `lastError` |
| `on_apply` | the ordered change list — one entry per committed op, with kind, key, seqno and the value inline up to `trigger_inline_value` | the same | the same |
| `describe` | empty | read-only, run by the GraphQL server at install and at every schema build | a descriptor that does not hold up rejects the install |

## The query lifecycle

One snapshot is registered before the entry runs and released after it returns, so every read inside the invocation — however many scans and lookups it makes — sees one state. That is what lets a computed report be internally consistent without the caller coordinating anything. A query never writes: `put`, `delete` and `get_for_update` all return `EROFS`.

`query_at` pins a snapshot you choose instead of the current one. Because module bytes are themselves versioned keys, that travels the code as well as the data: a query run at an old sequence number is the module as it existed then.

## The executor lifecycle

This is the role with real rules, because the engine may run the entry more than once per call.

- Each attempt begins a fresh transaction and gets fresh linear memory, fresh fuel and a fresh output buffer. Nothing survives a previous attempt except what is in the database.
- Exit 0 commits the transaction. Any other exit aborts it and surfaces as `Error::GuestFailed { code, output }`, with nothing written.
- A commit conflict discards the attempt and re-runs it against a fresh snapshot, up to `execute_retries` attempts (3 by default, the first included). When they are spent the call returns `Conflict` and re-running becomes the caller's job.
- So the entry has to be a pure function of its input and the database state: no side channels, no "have I already run" flag anywhere but the data, and no assumption that an earlier attempt's writes happened.
- Call `get_for_update` on every key a write depends on. That is what puts the key in the conflict set, and it is what makes the invariant hold under concurrency.
- Use checked arithmetic, and treat present-but-malformed state as corruption: fail loudly with a distinct code rather than defaulting. An executor that silently defaults or overflows corrupts durable state.
- `EIO` from any host call means the engine itself failed. The invocation fails host-side even if the guest swallows the errno and exits 0.

## The trigger lifecycles

Both trigger roles run as executors, with the engine supplying the input and owning the transaction. What differs is what arrives and what it means.

**`on_touch` is asked to reconcile.** The input is a set of keys that were written, with repeated touches of one key coalesced into one entry while a backlog exists — no values, no op kinds, no order. The contract is to read each key's current state and make the derived state match: present means upsert, absent means remove. Written that way the module converges however the events were batched or replayed. Because the event carries no previous value, anything that needs to undo earlier work keeps its own back-pointer.

**`on_apply` is told what happened.** The input is every committed op in commit order, each with its kind, key, sequence number and — up to `trigger_inline_value` — its value. Nothing is coalesced: a key written three times produces three entries. Values above the inline limit arrive elided, and the module reads the key instead, knowing that read is current state and may be newer than the change it is holding. Derive output keys from the sequence number and a replay overwrites instead of duplicating.

A trigger's own writes generate no events, for any trigger, so consumers cannot chain or loop. Registration is by key range and is covered in [Triggers](triggers.md).

## Exit codes

Zero means success. The convention for everything else is one distinct non-zero code per failure class, with a human-readable message written to the output buffer. Callers see both — over GraphQL as `guestExitCode` and `guestOutputText` — so a client can tell "insufficient funds" from "malformed input" from "corrupt record" without parsing prose. The modules in `guests/` use 2 through 7.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Roles and lifecycles` in *Extending with WASM*
