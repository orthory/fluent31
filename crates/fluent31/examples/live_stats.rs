//! Always-fresh per-group aggregates with EXACT arithmetic — the
//! `SELECT count(*), sum(cents) GROUP BY customer` that never runs a
//! query. The `guests/live_stats` changes-mode trigger folds every
//! committed order change into running totals (updates move records
//! between groups, deletes subtract), and because trigger effects are
//! exactly-once, the totals cannot drift: this demo hammers the range from
//! four threads and then PROVES the folded stats equal a from-scratch
//! recount.
//!
//! ```sh
//! cargo run -p fluent31 --example live_stats
//! ```

#[path = "util/mod.rs"]
mod util;

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use fluent31::{Db, Options, SyncMode};
use util::{drain, guest_wasm, pairs, put};

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .expect("open"),
    );
    db.install_module("live_stats", &guest_wasm("live_stats")).expect("install");
    let mode = db
        .create_trigger("liveStats", "live_stats", Some(b"ord/"), Some(b"ord0"))
        .expect("trigger");
    println!("== liveStats trigger over [ord/, ord0) mode={}\n", mode.as_str());

    println!("== three orders arrive");
    put(&db, "ord/001", r#"{"customer":"acme","cents":500}"#);
    put(&db, "ord/002", r#"{"customer":"bob","cents":120}"#);
    put(&db, "ord/003", r#"{"customer":"acme","cents":990}"#);
    drain(&db);
    print_stats(&db);

    println!("== ord/001 moves to bob and doubles; ord/003 is refunded (deleted)");
    put(&db, "ord/001", r#"{"customer":"bob","cents":1000}"#);
    db.delete(b"ord/003".to_vec()).expect("delete");
    drain(&db);
    print_stats(&db);

    println!("== storm: 4 threads x 100 writes (upserts, moves, deletes) on 25 order ids");
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4u64)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for j in 0..100u64 {
                    let id = (t * 100 + j * 13) % 25;
                    let key = format!("ord/{id:03}");
                    // deterministic pseudo-random mix of ops and groups
                    match (t + j) % 5 {
                        4 => {
                            let _ = db.delete(key.into_bytes());
                        }
                        m => {
                            let customer = ["acme", "bob", "zorg", "dyn"][m as usize % 4];
                            let cents = (j + 1) * 7 + t;
                            let rec =
                                format!(r#"{{"customer":"{customer}","cents":{cents}}}"#);
                            db.put(key.into_bytes(), rec.into_bytes()).expect("put");
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("join");
    }
    drain(&db);
    print_stats(&db);

    println!("== proof: recount every order from scratch and compare");
    let mut expected: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for (_, v) in pairs(&db, "ord/") {
        let rec: serde_json::Value = serde_json::from_str(&v).expect("record json");
        let e = expected
            .entry(rec["customer"].as_str().unwrap().to_string())
            .or_default();
        e.0 += 1;
        e.1 += rec["cents"].as_i64().unwrap();
    }
    let mut folded: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for (k, v) in pairs(&db, "stat/") {
        let s: serde_json::Value = serde_json::from_str(&v).expect("stat json");
        folded.insert(
            k["stat/".len()..].to_string(),
            (s["orders"].as_i64().unwrap(), s["cents"].as_i64().unwrap()),
        );
    }
    assert_eq!(folded, expected, "folded stats drifted from ground truth");
    println!("   folded stats == full recount for {} groups ✓", folded.len());
    println!("done: exactly-once folding, no drift.");
}

fn print_stats(db: &Db) {
    for (k, v) in pairs(db, "stat/") {
        println!("   {k} = {v}");
    }
    println!();
}
