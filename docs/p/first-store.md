<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#first-store -->

# Your first store

> One command, no code: a running database you can write to and read back.

```
$ cargo run -p fluent-cli -- ./data
fluent31> put hello world
OK  (3.02 ms)
fluent31> get hello
"world"  (28.7 µs)
```

The directory did not have to exist. Opening it created the store, took an exclusive lock on the directory, recovered anything a previous run had left, and started the background threads that flush, compact and commit. Every command prints its own wall-clock latency.

## Write a few records and read the range

```
fluent31> put user/ada engineer
fluent31> put user/grace admiral
fluent31> put user/katherine mathematician
fluent31> scan user/ user0
   1) "user/ada" => "engineer"
   2) "user/grace" => "admiral"
   3) "user/katherine" => "mathematician"
```

Two things happened there that are worth naming, because everything else builds on them.

**Keys sort bytewise**, which is why the three came back in that order and why a range read is the primitive rather than a special operation. **A scan takes `[lo, hi)`** — inclusive low, exclusive high — so scanning a prefix means scanning to that prefix with its last byte incremented: `user/` ends at `user0`, because `0` is the byte after `/`. A `-` stands for an open end, so `scan - -` walks the whole store.

## Enough of the shell to be useful

```
fluent31> count user/ user0            # how many, without printing them
fluent31> scan user/ user0 --rev --limit 2
fluent31> del hello                    # the scratch key from a moment ago
fluent31> begin                        # a transaction; the prompt shows (txn)
fluent31> tlock counter                # read + conflict-check at commit
fluent31> tput counter 1
fluent31> commit
fluent31> stats                        # levels, cache, group-commit amortization
fluent31> help
```

Byte arguments are plain UTF-8 or `hex:DEADBEEF`, and output prints printable bytes quoted and everything else as `hex:`.

## What is on disk

`./data` now holds `LOCK`, `CURRENT`, a manifest, and a write-ahead log; table and value-log files appear as data is flushed out of memory. The lock is the part to remember: **one process at a time owns a store directory.** That is why the next step starts by leaving this one.

```
fluent31> exit
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Your first store` in *Tutorial*
