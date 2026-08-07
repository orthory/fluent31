use fluent31::{Compression, Db, Error, Options, SyncMode};

fn options() -> Options {
    Options {
        sync: SyncMode::Always,
        io_backend: fluent31::IoBackend::Std,
        block_size: 256,
        compression: Compression::Lz4,
        target_file_size: 2 << 10,
        value_threshold: 48,
        vlog_file_size: 2 << 10,
        ..Options::default()
    }
}

fn entries(count: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("key/{i:06}").into_bytes();
            let len = if i % 2 == 0 { 32 } else { 96 };
            let value = vec![(i % 251) as u8; len];
            (key, value)
        })
        .collect()
}

#[test]
fn creates_final_level_base_with_configured_storage_features() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = entries(400);

    let db = Db::create_from_sorted(&path, options(), expected.clone()).unwrap();
    assert_eq!(db.seqno(), expected.len() as u64);
    assert_eq!(
        db.iter(None, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        expected
    );

    let stats = db.stats();
    assert_eq!(stats.memtable_bytes, 0);
    assert_eq!(stats.immutable_memtables, 0);
    assert!(stats.levels[..stats.levels.len() - 1]
        .iter()
        .all(|(runs, fragments, _)| *runs == 0 && *fragments == 0));
    let (runs, fragments, bytes) = stats.levels.last().copied().unwrap();
    assert_eq!(runs, 1);
    assert!(fragments > 1);
    assert!(bytes > 0);
    assert!(stats.vlog_files > 1);

    let before_compact = stats.levels.clone();
    db.compact_all().unwrap();
    assert_eq!(db.stats().levels, before_compact);
    drop(db);

    let db = Db::open(&path, options()).unwrap();
    assert_eq!(
        db.iter(None, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        expected
    );
    db.put(b"key/000001".to_vec(), b"updated".to_vec()).unwrap();
    db.delete(b"key/000002".to_vec()).unwrap();
    db.put(b"key/999999".to_vec(), b"new".to_vec()).unwrap();
    db.flush().unwrap();
    db.compact_all().unwrap();
    drop(db);

    let db = Db::open(&path, options()).unwrap();
    assert_eq!(db.get(b"key/000001").unwrap().unwrap(), b"updated");
    assert!(db.get(b"key/000002").unwrap().is_none());
    assert_eq!(db.get(b"key/999999").unwrap().unwrap(), b"new");
}

#[test]
fn rejects_non_increasing_keys_without_publishing_a_partial_base() {
    let mut after_completed_fragments = entries(200);
    after_completed_fragments.push((b"key/000100".to_vec(), b"duplicate".to_vec()));
    for input in [
        vec![
            (b"b".to_vec(), b"1".to_vec()),
            (b"a".to_vec(), b"2".to_vec()),
        ],
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"a".to_vec(), b"2".to_vec()),
        ],
        after_completed_fragments,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let err = Db::create_from_sorted(&path, options(), input)
            .err()
            .expect("unsorted input must fail");
        assert!(
            matches!(err, Error::InvalidArgument(message) if message.contains("strictly increasing"))
        );

        let db = Db::open(&path, options()).unwrap();
        assert_eq!(db.seqno(), 0);
        assert_eq!(db.iter(None, None, false).unwrap().count(), 0);
    }
}

#[test]
fn fallible_input_error_does_not_publish_completed_fragments() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let input =
        entries(200)
            .into_iter()
            .map(Ok)
            .chain(std::iter::once(Err(Error::InvalidArgument(
                "source failed".into(),
            ))));

    let err = Db::create_from_sorted_fallible(&path, options(), input)
        .err()
        .expect("the source error must be returned");
    assert!(matches!(err, Error::InvalidArgument(message) if message == "source failed"));

    let db = Db::open(&path, options()).unwrap();
    assert_eq!(db.seqno(), 0);
    assert_eq!(db.iter(None, None, false).unwrap().count(), 0);
}

#[test]
fn validates_keys_and_values_like_the_normal_write_path() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("reserved");
    let err = Db::create_from_sorted(&path, options(), vec![(vec![0, b'k'], b"value".to_vec())])
        .err()
        .expect("reserved keys must fail");
    assert!(matches!(err, Error::InvalidArgument(_)));

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("oversized");
    let mut opts = options();
    opts.max_value_size = 3;
    let err = Db::create_from_sorted(&path, opts, vec![(b"key".to_vec(), b"value".to_vec())])
        .err()
        .expect("oversized values must fail");
    assert!(matches!(err, Error::InvalidArgument(message) if message.contains("max_value_size")));
}

#[test]
fn chooses_a_bottom_level_large_enough_for_the_base() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut opts = options();
    opts.max_levels = 1;
    opts.memtable_size = 128;
    opts.l0_compaction_trigger = 1;
    opts.tier_width = 2;

    let db = Db::create_from_sorted(&path, opts, entries(400)).unwrap();
    let stats = db.stats();
    assert!(stats.levels.len() > 1);
    assert!(stats.levels[..stats.levels.len() - 1]
        .iter()
        .all(|(runs, fragments, _)| *runs == 0 && *fragments == 0));
    assert_eq!(stats.levels.last().unwrap().0, 1);
}

#[test]
fn refuses_an_existing_store_or_nonempty_directory() {
    let existing = tempfile::tempdir().unwrap();
    drop(Db::open(existing.path(), options()).unwrap());
    let err = Db::create_from_sorted(
        existing.path(),
        options(),
        std::iter::empty::<(Vec<u8>, Vec<u8>)>(),
    )
    .err()
    .expect("an existing store must be refused");
    assert!(
        matches!(err, Error::InvalidArgument(message) if message.contains("not an empty destination"))
    );

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("unrelated"), b"keep").unwrap();
    assert!(
        Db::create_from_sorted(&path, options(), std::iter::empty::<(Vec<u8>, Vec<u8>)>(),)
            .is_err()
    );
    assert_eq!(std::fs::read(path.join("unrelated")).unwrap(), b"keep");
}
