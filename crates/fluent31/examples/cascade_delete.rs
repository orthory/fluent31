//! Referential cleanup from the change feed — the `ON DELETE CASCADE`
//! replacement. The `guests/cascade_delete` changes-mode trigger watches a
//! family of keys; deleting a parent record sweeps its whole subtree,
//! atomically with the event's consumption, while puts and descendant
//! traffic pass through untouched. Trigger writes never re-fire triggers,
//! so cascades cannot loop or amplify.
//!
//! ```sh
//! cargo run -p fluent31 --example cascade_delete
//! ```

#[path = "util/mod.rs"]
mod util;

use fluent31::{Db, Options, SyncMode};
use util::{drain, guest_wasm, put, show};

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Db::open(
        dir.path(),
        Options {
            sync: SyncMode::Never,
            ..Options::default()
        },
    )
    .expect("open");
    db.install_module("cascade_delete", &guest_wasm("cascade_delete")).expect("install");
    let mode = db
        .create_trigger("cascade", "cascade_delete", Some(b"doc/"), Some(b"doc0"))
        .expect("trigger");
    println!("== cascade trigger over [doc/, doc0) mode={}\n", mode.as_str());

    println!("== two documents, each with attachments and comments");
    put(&db, "doc/a", r#"{"title":"specs"}"#);
    put(&db, "doc/a/att/1", "blueprint.pdf");
    put(&db, "doc/a/att/2", "notes.txt");
    put(&db, "doc/a/comment/1", "ship it");
    put(&db, "doc/b", r#"{"title":"roadmap"}"#);
    put(&db, "doc/b/att/1", "q3.xlsx");
    drain(&db);
    show(&db, "doc/");

    println!("== updating doc/b cascades nothing");
    put(&db, "doc/b", r#"{"title":"roadmap v2"}"#);
    drain(&db);
    show(&db, "doc/b");

    println!("== deleting a descendant directly cascades nothing either");
    db.delete(b"doc/b/att/1".to_vec()).expect("delete");
    drain(&db);
    show(&db, "doc/b");

    println!("== deleting the PARENT doc/a sweeps its whole subtree");
    db.delete(b"doc/a".to_vec()).expect("delete");
    drain(&db);
    show(&db, "doc/");
    println!("done: doc/b survives, doc/a and every descendant are gone.");
}
