//! Fork-creation error paths. A failed build must remove its
//! `archive/.tmp-<name>` dir so the name stays immediately reusable, must
//! leave the parent undisturbed, and must never delete a concurrent
//! builder's in-progress dir. Failures are induced naturally — damaging
//! vlog files under the engine — so the cleanup path is exercised exactly
//! where real IO errors would strike.

use std::path::{Path, PathBuf};
use std::sync::Barrier;

use fluent31::{Db, Options, SyncMode};

fn small_opts() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_size: 4 << 10,
        block_size: 512,
        l0_compaction_trigger: 2,
        tier_width: 2,
        max_levels: 4,
        target_file_size: 4 << 10,
        value_threshold: 64,
        vlog_file_size: 8 << 10,
        // background auto-GC would retire the very files these tests damage;
        // an unreachable ratio pins them in place deterministically
        vlog_gc_ratio: f64::INFINITY,
        ..Options::default()
    }
}

fn vlog_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "vlog"))
        .collect();
    v.sort(); // zero-padded ids: lexicographic == numeric
    v
}

fn tmp_build_dir(dir: &Path, name: &str) -> PathBuf {
    dir.join("archive").join(format!(".tmp-{name}"))
}

/// Damage the vlog head (truncate under the engine) so create() fails at
/// the bounded head copy — after the tables are already linked, i.e. with a
/// partially populated build dir to clean up.
#[test]
fn failed_create_cleans_tmp_and_name_stays_usable() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..5u32 {
        db.put(format!("k{i}").into_bytes(), vec![b'v'; 200]).unwrap();
    }
    db.flush().unwrap();

    let vlogs = vlog_files(dir.path());
    assert_eq!(vlogs.len(), 1, "expected a single (head) vlog file");
    let head = &vlogs[0];
    let saved = std::fs::read(head).unwrap();
    assert!(!saved.is_empty());
    std::fs::write(head, b"").unwrap();

    db.fork("snap").unwrap_err();
    assert!(
        !tmp_build_dir(dir.path(), "snap").exists(),
        "failed build left its .tmp dir behind"
    );

    // the name is not poisoned: the retry reaches the same underlying
    // failure instead of "already being created"
    let retry = format!("{}", db.fork("snap").unwrap_err());
    assert!(!retry.contains("already being created"), "poisoned: {retry}");
    assert!(!tmp_build_dir(dir.path(), "snap").exists());
    assert!(db.list_forks().unwrap().is_empty());

    // parent undisturbed; once the file is repaired the same name succeeds
    std::fs::write(head, &saved).unwrap();
    assert_eq!(db.get(b"k0").unwrap().unwrap(), vec![b'v'; 200]);
    let info = db.fork("snap").unwrap();
    let fdb = Db::open(&info.path, small_opts()).unwrap();
    for i in 0..5u32 {
        assert_eq!(
            fdb.get(format!("k{i}").as_bytes()).unwrap().unwrap(),
            vec![b'v'; 200]
        );
    }
}

/// Delete a sealed vlog file so create() fails while hard-linking — both
/// the link and its copy fallback hit ENOENT.
#[test]
fn missing_sealed_vlog_fails_create_and_cleans_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    // overwrite rounds until the vlog rotates a few times
    let mut round = 0u32;
    while vlog_files(dir.path()).len() < 3 {
        for i in 0..20u32 {
            let val = format!("r{round}-{}", "x".repeat(300));
            db.put(format!("k{i:03}").into_bytes(), val.into_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        round += 1;
        assert!(round < 200, "vlog never rotated");
    }
    let last = round - 1;

    let sealed = vlog_files(dir.path()).into_iter().next().unwrap();
    let saved = std::fs::read(&sealed).unwrap();
    std::fs::remove_file(&sealed).unwrap();

    db.fork("snap").unwrap_err();
    assert!(!tmp_build_dir(dir.path(), "snap").exists());
    let retry = format!("{}", db.fork("snap").unwrap_err());
    assert!(!retry.contains("already being created"), "poisoned: {retry}");
    assert!(!tmp_build_dir(dir.path(), "snap").exists());

    // repair and fork for real
    std::fs::write(&sealed, &saved).unwrap();
    let info = db.fork("snap").unwrap();
    let fdb = Db::open(&info.path, small_opts()).unwrap();
    for i in 0..20u32 {
        let expect = format!("r{last}-{}", "x".repeat(300));
        assert_eq!(
            fdb.get(format!("k{i:03}").as_bytes()).unwrap().unwrap(),
            expect.into_bytes()
        );
    }
}

/// A `.tmp-<name>` dir we did not create (a concurrent build, or a crashed
/// one from a previous process) must survive our failed attempt untouched,
/// and be swept at the next `Db::open`.
#[test]
fn foreign_tmp_dir_is_preserved_then_swept_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();

    let foreign = tmp_build_dir(dir.path(), "x");
    std::fs::create_dir_all(&foreign).unwrap();
    std::fs::write(foreign.join("sentinel"), b"other-builder").unwrap();

    let err = format!("{}", db.fork("x").unwrap_err());
    assert!(err.contains("already being created"), "{err}");
    assert!(
        foreign.join("sentinel").exists(),
        "loser deleted another builder's in-progress dir"
    );

    drop(db);
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert!(!foreign.exists(), "stale .tmp dir not swept at open");
    let info = db.fork("x").unwrap();
    let fdb = Db::open(&info.path, small_opts()).unwrap();
    assert_eq!(fdb.get(b"a").unwrap().unwrap(), b"1");
}

/// Racing creators of the same name: exactly one wins per round, losers
/// error out, and nobody leaves (or deletes the winner's) build dir. The
/// loser who grabs the tmp dir after the winner's rename builds fully and
/// fails at the final rename — its guard must clean up.
#[test]
fn concurrent_same_name_forks_exactly_one_wins() {
    const THREADS: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..50u32 {
        db.put(format!("k{i:03}").into_bytes(), vec![b'v'; 100]).unwrap();
    }

    for round in 0..5 {
        let barrier = Barrier::new(THREADS);
        let wins: usize = std::thread::scope(|s| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let db = &db;
                    let barrier = &barrier;
                    s.spawn(move || {
                        barrier.wait();
                        db.fork("same").is_ok()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).filter(|ok| *ok).count()
        });
        assert_eq!(wins, 1, "round {round}: expected exactly one winner");
        assert!(
            !tmp_build_dir(dir.path(), "same").exists(),
            "round {round}: .tmp residue after the race"
        );
        let listed = db.list_forks().unwrap();
        assert_eq!(listed.len(), 1, "round {round}");
        let fdb = Db::open(&listed[0].path, small_opts()).unwrap();
        assert_eq!(fdb.get(b"k000").unwrap().unwrap(), vec![b'v'; 100]);
        drop(fdb);
        db.delete_fork("same").unwrap();
    }
}
