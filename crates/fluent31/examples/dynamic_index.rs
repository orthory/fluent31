//! Event-driven DYNAMIC index generation, end to end: index definitions are
//! ordinary keys, so writing `idxspec/<name>` creates a fully backfilled
//! secondary index at runtime, updating a record keeps every index live,
//! and deleting the spec tears the index down — all maintained
//! asynchronously by the `guests/dynamic_index` changes-mode trigger
//! module, with no writer cooperation and no reinstall.
//!
//! Run it (builds the guest workspace for wasm32 first):
//!
//! ```sh
//! cargo run -p fluent31 --example dynamic_index
//! ```

#[path = "util/mod.rs"]
mod util;

use fluent31::{Db, Options, SyncMode};
use util::{drain, guest_wasm, keys, put, show};

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

    println!("== install the dynamic_index module and bind its two triggers");
    db.install_module("dynamic_index", &guest_wasm("dynamic_index")).expect("install");
    let data = db
        .create_trigger("dynIdxData", "dynamic_index", Some(b"rec/"), Some(b"rec0"))
        .expect("data trigger");
    let spec = db
        .create_trigger("dynIdxSpec", "dynamic_index", Some(b"idxspec/"), Some(b"idxspec0"))
        .expect("spec trigger");
    println!("   dynIdxData over [rec/, rec0)         mode={}", data.as_str());
    println!("   dynIdxSpec over [idxspec/, idxspec0) mode={}\n", spec.as_str());

    println!("== seed records (no index exists yet)");
    put(&db, "rec/001", r#"{"customer":"acme","status":"open","amountCents":500}"#);
    put(&db, "rec/002", r#"{"customer":"bob","status":"open","amountCents":120}"#);
    put(&db, "rec/003", r#"{"customer":"acme","status":"paid","amountCents":990}"#);
    drain(&db);
    show(&db, "idx/");

    println!("== write a spec key -> the byCustomer index appears, already backfilled");
    put(&db, "idxspec/byCustomer", r#"{"field":"customer"}"#);
    drain(&db);
    show(&db, "idx/byCustomer/");

    println!("== live maintenance: add rec/004, move rec/001 to a new customer, delete rec/002");
    put(&db, "rec/004", r#"{"customer":"bob","status":"open","amountCents":75}"#);
    put(&db, "rec/001", r#"{"customer":"zorg","status":"open","amountCents":500}"#);
    db.delete(b"rec/002".to_vec()).expect("delete");
    drain(&db);
    show(&db, "idx/byCustomer/");

    println!("== a second spec indexes another field of the same records");
    put(&db, "idxspec/byStatus", r#"{"field":"status"}"#);
    drain(&db);
    show(&db, "idx/byStatus/");

    println!("== \"which acme records?\" is now a prefix scan");
    for k in keys(&db, "idx/byCustomer/acme/") {
        println!("   rec id {}", k.rsplit('/').next().unwrap());
    }
    println!();

    println!("== deleting the spec tears the index down");
    db.delete(b"idxspec/byCustomer".to_vec()).expect("delete spec");
    drain(&db);
    show(&db, "idx/");
    println!("done: byStatus remains, byCustomer is gone.");
}
