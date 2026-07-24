//! `--journal DIR` wiring over the real binary: the flag attaches the
//! opt-in mutation journal (fluent31::journal) — the attach-time base
//! snapshot captures state that predates the flag, live GraphQL writes
//! stream in as deltas, and a SIGTERM shutdown drains cleanly — proven by
//! rebuilding fresh stores from the journal directory alone. The
//! `--journal-*` tuning flags plumb a JournalConfig through to the writer —
//! proven by forcing a rotation with a tiny `--journal-rotate-bytes` — and
//! are refused without `--journal DIR`.

use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use fluent31::{journal, Db, Options, SyncMode};

fn opts() -> Options {
    Options {
        sync: SyncMode::Never,
        ..Options::default()
    }
}

/// Rebuild the journal into a fresh directory and look `key` up there.
/// `None` covers both "rebuild failed" (journal mid-write) and "key not
/// journaled yet", so callers just poll until the value appears.
fn rebuilt_value(jrn: &Path, key: &[u8]) -> Option<Vec<u8>> {
    let dest = tempfile::tempdir().unwrap();
    journal::rebuild(jrn, dest.path(), opts()).ok()?;
    let db = Db::open(dest.path(), opts()).unwrap();
    db.get(key).unwrap()
}

/// `journal-*.log` files currently in the journal directory.
fn log_file_count(jrn: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(jrn) else { return 0 };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("journal-") && name.ends_with(".log")
        })
        .count()
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Minimal blocking HTTP/1.1 POST — enough to hit the GraphQL plane
/// without pulling an HTTP client into the dev-dependencies.
fn graphql_post(addr: &str, body: &str) -> String {
    let mut sock = std::net::TcpStream::connect(addr).unwrap();
    let req = format!(
        "POST /graphql HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

#[cfg(unix)]
fn kill_term(pid: u32) {
    let ok = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .unwrap()
        .success();
    assert!(ok, "kill -TERM {pid} failed");
}

#[cfg(unix)]
#[test]
fn journal_flag_attaches_streams_and_survives_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let jrn = tempfile::tempdir().unwrap();

    // state that predates the journal: only the attach-time base can carry it
    {
        let db = Db::open(dir.path(), opts()).unwrap();
        db.put(b"pre".to_vec(), b"base".to_vec()).unwrap();
    }

    // ephemeral port picked up front: the binary announces the literal
    // --listen string, so :0 could not be discovered from its stdout
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-graphql"))
        .arg(dir.path())
        .arg("--listen")
        .arg(&addr)
        .arg("--sync")
        .arg("never")
        .arg("--journal")
        .arg(jrn.path())
        .spawn()
        .unwrap();

    // the attach-time base snapshot covers the pre-existing key
    wait_for("base snapshot to cover pre-attach state", || {
        rebuilt_value(jrn.path(), b"pre") == Some(b"base".to_vec())
    });

    // a live write flows through the delta stream (fsynced per batch)
    wait_for("graphql plane to accept", || {
        std::net::TcpStream::connect(&addr).is_ok()
    });
    let resp = graphql_post(
        &addr,
        r#"{"query":"mutation { put(key: {text: \"live\"}, value: {text: \"delta\"}) }"}"#,
    );
    assert!(resp.contains(r#""put":true"#), "{resp}");
    wait_for("delta to reach the journal", || {
        rebuilt_value(jrn.path(), b"live") == Some(b"delta".to_vec())
    });

    // graceful shutdown: the journal drains and flushes before the Db
    // drops — a deadlock or panic there would show as a dirty exit
    kill_term(child.id());
    let status = child.wait().unwrap();
    assert!(status.success(), "clean shutdown must exit 0, got {status}");
    assert_eq!(rebuilt_value(jrn.path(), b"pre"), Some(b"base".to_vec()));
    assert_eq!(rebuilt_value(jrn.path(), b"live"), Some(b"delta".to_vec()));
}

#[cfg(unix)]
#[test]
fn journal_rotate_bytes_flag_reaches_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let jrn = tempfile::tempdir().unwrap();

    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-graphql"))
        .arg(dir.path())
        .arg("--listen")
        .arg(&addr)
        .arg("--sync")
        .arg("never")
        .arg("--journal")
        .arg(jrn.path())
        .arg("--journal-rotate-bytes")
        .arg("512")
        .spawn()
        .unwrap();

    wait_for("graphql plane to accept", || {
        std::net::TcpStream::connect(&addr).is_ok()
    });
    // one delta bigger than rotate-bytes: appending it pushes the active
    // file past 512 and rotates — a second journal file can only appear if
    // the flag reached the LogWriter (the default is 128 MiB)
    let val = "x".repeat(4 << 10);
    let resp = graphql_post(
        &addr,
        &format!(r#"{{"query":"mutation {{ put(key: {{text: \"big\"}}, value: {{text: \"{val}\"}}) }}"}}"#),
    );
    assert!(resp.contains(r#""put":true"#), "{resp}");
    wait_for("rotation to a second journal file", || {
        log_file_count(jrn.path()) >= 2
    });

    // a rebuild spanning the rotation boundary still reconstructs the value
    wait_for("delta to reach the journal", || {
        rebuilt_value(jrn.path(), b"big") == Some(val.as_bytes().to_vec())
    });
    kill_term(child.id());
    let status = child.wait().unwrap();
    assert!(status.success(), "clean shutdown must exit 0, got {status}");
}

#[test]
fn journal_tuning_flags_without_journal_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fluent-graphql"))
        .arg(dir.path())
        .arg("--journal-rotate-bytes")
        .arg("512")
        .output()
        .unwrap();
    assert!(!out.status.success(), "tuning without --journal must be refused");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--journal-* tuning flags need --journal DIR"), "{stderr}");
}
