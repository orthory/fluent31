<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#embed -->

# Embed it in Rust

> The shell and the server are wrappers around a library. This is the library.

```
use fluent31::{Db, Options, WriteBatch};

let db = Db::open("./data", Options::default())?;

db.put("user/1", "ada")?;
assert_eq!(db.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));
db.delete(b"user/0")?;                       // fine whether or not it existed

let mut b = WriteBatch::new();                 // atomic: one contiguous seqno range
b.put("user/2", "grace");
b.delete("user/3");
db.write(b)?;

for kv in db.iter(Some(b"user/"), Some(b"user0"), false)? {
    let (key, value) = kv?;
}
```

`Db` is `Send + Sync`, so one handle is shared across threads as `Arc<Db>` rather than opened twice — a second open of the same directory fails on the lock. Dropping it stops and joins the background threads.

## Transactions

```
loop {
    let mut txn = db.begin();
    let n = txn.get_for_update(b"counter")?.map(decode).unwrap_or(0);
    txn.put("counter", encode(n + 1))?;
    match txn.commit() {
        Ok(()) => break,
        Err(fluent31::Error::Conflict) => continue,   // nothing written; run it all again
        Err(e) => return Err(e),
    }
}
```

Transactions are optimistic: no locks are taken, readers never block, and `get_for_update` is what puts a key in the conflict set. A commit that loses the race returns `Conflict` having written nothing, and the fix is always to re-run the whole read-modify-write — which is also why an executor module must be a pure function of its input and the data.

## The rest of the surface

```
db.install_module("count", &wasm)?;      db.query("count", b"user/")?;
db.create_trigger("idx", "customer_index", Some(b"orders/"), Some(b"orders0"))?;
db.snapshot();                              // a consistent read point
db.fork("pre-migration")?;                   // a whole-database branch
db.subscribe(b"orders/", Some(b"orders0"))?;  // the change stream
db.stats();
```

Everything done through the shell and the server in the previous steps is available here, because that is what both of them call.

## Next

Each step so far has shown one surface on its own. The last one puts them in a single program — one that installs, binds, contends and forks, and checks its own answers.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Embed it in Rust` in *Tutorial*
