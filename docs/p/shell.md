<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#shell -->

# The shell

> An interactive prompt over one store, and the journal rebuild tool.

```
fluent-cli <db-dir> [--std|--uring] [--nosync] [--sync-every <ms>]
fluent-cli journal-rebuild <journal-dir> <dest-dir>
```

`--std` and `--uring` force the IO backend. `--nosync` is `SyncMode::Never` and `--sync-every` is `Periodic`. Every command prints its wall-clock latency. Byte arguments are plain UTF-8 or `hex:DEADBEEF`. Output shows printable bytes quoted and everything else as `hex:`.

## Commands

| Group | Commands |
|---|---|
| kv | `get K`, `put K V`, `del K`, `scan [LO\|-] [HI\|-] [--rev] [--limit N]` (default limit 50), `count [LO] [HI]` |
| txn | `begin`, `tget K`, `tlock K` (get_for_update), `tput K V`, `tdel K`, `commit`, `abort`. The prompt shows `(txn)` while one is open. |
| snapshots | `snap` (prints an id), `snaps`, `sget ID K`, `snapdrop ID` |
| wasm | `install NAME FILE.wasm`, `modules`, `uninstall NAME`, `query NAME [INPUT]`, `exec NAME [INPUT]`, `queryonce FILE.wasm [INPUT]`, `execonce FILE.wasm [INPUT]` |
| triggers | `mktrig NAME MODULE [LO\|-] [HI\|-]`, `deltrig NAME`, `triggers` |
| forks | `fork NAME [AT]`, `forks`, `delfork NAME` |
| pins | `pin NAME`, `pins`, `unpin NAME`, `seqno` |
| admin | `flush`, `compact`, `gc`, `stats`, `help`, `exit` |

`count` shares `scan`'s parser, so it takes the same `-` bounds, `--rev` and `--limit`; unlike `scan` it has no default limit, so it counts the whole range. `quit` is the same as `exit`. Values longer than 160 bytes print truncated with their length. A guest failure prints `guest exited with code N, output …`. A transaction conflict prints a `CONFLICT (first committer wins)` line; the transaction is rolled back.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `The shell` in *Reference*
