<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#first-graphql -->

# Serve it over GraphQL

> The same store on a network surface, with a schema, an explorer and live subscriptions — no code yet.

Make sure the shell from the previous step has exited: it holds the store's lock, and the server needs it.

```
$ cargo run -p fluent-server -- ./data --store-name prod
INFO db{dir=./data store=prod instance=6065…}: fluent31::db: store opened backend="io_uring" seqno=0 …
INFO fluent_server: serving graphql: /graphql (GraphiQL at /, …) listen=127.0.0.1:8317
INFO fluent_server: serving replication: … listen=127.0.0.1:8428 store=prod instance=6065…
```

The log is stderr; `RUST_LOG` sets the level ([Operations](operations.md)).

`--store-name` is persisted in the store on first use, so it is passed once and then omitted. It fixes the store's identity, and it is what opens the replication join point; without a name the GraphQL plane still serves and that port stays closed.

Open [http://127.0.0.1:8317/](http://127.0.0.1:8317/) for GraphiQL, which has the whole schema and its documentation built in. Everything below works there or over `curl`.

## Reading

```
curl -s http://127.0.0.1:8317/graphql -H 'content-type: application/json' \
  -d '{"query":"{ get(key:{text:\"user/grace\"}) { text } }"}'
# {"data":{"get":{"text":"admiral"}}}
```

Keys and values are raw bytes, so an input takes exactly one of `{text}`, `{base64}` or `{hex}`, and an output offers `text` (null when the bytes are not UTF-8), `base64`, `hex` and `len`. Sequence numbers and byte counts travel as strings, because they are 64-bit.

## Writing and scanning

```
mutation { put(key: {text: "user/alan"}, value: {text: "logician"}) }

query {
  scan(prefix: {text: "user/"}, limit: 10) {
    pairs { key { text } value { text } }
    hasMore
    nextAfter { text }
  }
}
```

`scan` takes either a `prefix` or an explicit `lo`/`hi` pair, and pages by cursor: pass `nextAfter` back as `after` to continue. There is no offset. Every read field in one query operation runs at a single pinned snapshot, so a multi-field query cannot see a write land halfway through.

## Watching it change

```
subscription {
  changes(lo: {text: "user/"}, hi: {text: "user0"}) {
    kind key { text } value { text }
  }
}
```

The stream opens with one `ATTACHED` marker — the boundary — and then delivers every committed change into the range, in order. Run a `put` from another window and watch it arrive.

## Administration is in the same schema

`stats`, `modules`, `triggers`, `forks` and `pins` are query fields; `fork`, `pin`, `installModule`, `createTrigger`, `flush`, `compactAll`, `gcVlog` and `syncWal` are mutations. There is no separate admin channel to learn.

> **Before exposing it** Both planes bind to loopback and speak plain HTTP and TCP with no authentication. Put TLS and access control in front — a reverse proxy for GraphQL, a network boundary for replication.

Every field used so far is built in. The next step adds one that is not.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Serve it over GraphQL` in *Tutorial*
