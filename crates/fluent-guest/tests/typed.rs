//! Host-side tests of the typed layer: the change-list decoding against the
//! engine's wire framing, Fail/exit-code mapping rules, and a compile-level
//! proof that the attribute macros expand on an ordinary typed function.

use fluent_guest::{parse_changes, Change, Fail, FromInput, IntoOutput};

/// Mirror of the engine's `pack_changes` framing (trigger.rs): LE, u32
/// lengths, `[u32 count]` then `[u64 seqno][u8 kind][u32 klen][key]` plus
/// `[u32 vlen][value]` for kind 0.
fn encode(changes: &[(u64, u8, &[u8], &[u8])]) -> Vec<u8> {
    let mut out = (changes.len() as u32).to_le_bytes().to_vec();
    for (seqno, kind, key, value) in changes {
        out.extend_from_slice(&seqno.to_le_bytes());
        out.push(*kind);
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        if *kind == 0 {
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value);
        }
    }
    out
}

#[test]
fn change_list_roundtrip() {
    let input = encode(&[
        (100, 0, b"orders/1", b"v1"),
        (101, 2, b"orders/2", b""),
        (103, 1, b"orders/1", b""),
    ]);
    let got = parse_changes(&input).unwrap();
    assert_eq!(
        got,
        vec![
            Change::Put {
                seqno: 100,
                key: b"orders/1".to_vec(),
                value: Some(b"v1".to_vec()),
            },
            Change::Put {
                seqno: 101,
                key: b"orders/2".to_vec(),
                value: None,
            },
            Change::Delete {
                seqno: 103,
                key: b"orders/1".to_vec(),
            },
        ]
    );
    assert_eq!(got[0].seqno(), 100);
    assert_eq!(got[2].key(), b"orders/1");

    assert_eq!(parse_changes(&encode(&[])).unwrap(), vec![]);
}

#[test]
fn malformed_change_lists_are_none_not_garbage() {
    let good = encode(&[(7, 0, b"k", b"v")]);
    assert!(parse_changes(&good[..good.len() - 1]).is_none(), "truncated");
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(parse_changes(&trailing).is_none(), "trailing bytes");
    let mut bad_kind = good;
    bad_kind[12] = 9; // kind byte of the first change
    assert!(parse_changes(&bad_kind).is_none(), "unknown kind");
    assert!(parse_changes(b"").is_none(), "no count header");
    // an over-claiming count must not allocate or panic
    let huge = u32::MAX.to_le_bytes();
    assert!(parse_changes(&huge).is_none());
}

#[test]
fn from_input_and_into_output_conversions() {
    assert_eq!(Vec::<u8>::from_input(b"raw".to_vec()).unwrap(), b"raw");
    assert_eq!(String::from_input(b"text".to_vec()).unwrap(), "text");
    let err = String::from_input(vec![0xff, 0xfe]).unwrap_err();
    assert_eq!(err.code, 3);
    let err = Vec::<Change>::from_input(b"nonsense".to_vec()).unwrap_err();
    assert_eq!(err.code, 3);

    assert_eq!(b"bytes".to_vec().into_output(), b"bytes");
    assert_eq!("text".to_string().into_output(), b"text");
    assert_eq!(().into_output(), Vec::<u8>::new());
}

#[test]
fn fail_conversions_carry_code_and_message() {
    let f: Fail = "boom".into();
    assert_eq!((f.code, f.message.as_str()), (1, "boom"));
    let f: Fail = format!("id {}", 7).into();
    assert_eq!((f.code, f.message.as_str()), (1, "id 7"));
    let f = Fail::new(42, "specific");
    assert_eq!((f.code, f.message.as_str()), (42, "specific"));
}

// The attribute macros must expand on typed functions on any target (the
// generated exports only ever RUN inside the database). Calling them here
// would hit the host ABI stubs, so this is a compile-level assertion only.
#[fluent_guest::main]
fn typed_main(input: Vec<u8>) -> Result<Vec<u8>, Fail> {
    Ok(input)
}

#[fluent_guest::on_apply]
fn typed_on_apply(changes: Vec<Change>) -> Result<(), Fail> {
    let _ = changes;
    Ok(())
}

#[test]
fn attribute_macros_export_the_entry_symbols() {
    // the wrappers exist with the right signatures
    let _run: extern "C" fn() -> i32 = run;
    let _on_apply: extern "C" fn() -> i32 = on_apply;
}
