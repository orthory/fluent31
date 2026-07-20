//! The `journal-rebuild` one-shot mode over the real binary: a store's
//! mutation journal rebuilds into an identical fresh store, and every
//! RebuildReport field reaches stdout.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluent31::journal::Journal;
use fluent31::{Db, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        ..Options::default()
    }
}

/// Full live contents of a db as a map (user keyspace).
fn dump(db: &Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.iter(None, None, false)
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn rebuild_mode_round_trips_and_reports() {
    let db_dir = tempfile::tempdir().unwrap();
    let jrn_dir = tempfile::tempdir().unwrap();

    let expected = {
        let db = Arc::new(Db::open(db_dir.path(), opts()).unwrap());
        let journal = Journal::attach(db.clone(), jrn_dir.path()).unwrap();
        for i in 0..200u32 {
            db.put(
                format!("key/{i:04}").into_bytes(),
                format!("val-{i}").into_bytes(),
            )
            .unwrap();
        }
        db.delete(b"key/0007".to_vec()).unwrap();
        let expected = dump(&db);
        let target = db.stats().visible_seqno;
        let deadline = Instant::now() + Duration::from_secs(10);
        while journal.stats().last_seqno < target {
            assert!(Instant::now() < deadline, "journal never caught up");
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(journal); // clean stop + final flush
        expected
    };

    let dest = tempfile::tempdir().unwrap();
    let dest_dir = dest.path().join("rebuilt");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-cli"))
        .arg("journal-rebuild")
        .arg(jrn_dir.path())
        .arg(&dest_dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    for field in ["source instance", "base keys", "deltas applied", "last seqno"] {
        assert!(stdout.contains(field), "report field {field:?} missing:\n{stdout}");
    }

    let rebuilt = Db::open(&dest_dir, opts()).unwrap();
    assert_eq!(dump(&rebuilt), expected, "rebuilt state diverged from original");
}

#[test]
fn rebuild_mode_wrong_arity_is_a_usage_error() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-cli"))
        .arg("journal-rebuild")
        .arg("only-one-arg")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage:"));
}

#[test]
fn rebuild_mode_failure_exits_nonzero() {
    let empty = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-cli"))
        .arg("journal-rebuild")
        .arg(empty.path())
        .arg(dest.path().join("rebuilt"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("journal-rebuild failed"));
}
