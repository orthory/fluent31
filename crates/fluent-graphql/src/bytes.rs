//! Byte-string codecs: engine keys and values are raw bytes, GraphQL
//! speaks text. `BytesInput` (oneof text/base64/hex) is decoded by
//! [`decode_bytes_input`]; outputs are `Bytes` objects whose fields decode
//! lazily in the dynamic schema (see `schema.rs`).

use async_graphql::dynamic::ObjectAccessor;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;

pub fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>, async_graphql::Error> {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(2) {
        return Err(async_graphql::Error::new("invalid hex: odd length"));
    }
    b.chunks_exact(2)
        .map(|p| Some((nibble(p[0])? << 4) | nibble(p[1])?))
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(|| async_graphql::Error::new("invalid hex: non-hex digit"))
}

pub fn encode_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((byte & 0xF) as u32, 16).unwrap());
    }
    s
}

pub fn encode_b64(b: &[u8]) -> String {
    B64.encode(b)
}

pub fn decode_b64(s: &str) -> Result<Vec<u8>, async_graphql::Error> {
    B64.decode(s.as_bytes())
        .map_err(|e| async_graphql::Error::new(format!("invalid base64: {e}")))
}

/// Decode a `BytesInput` oneof object (exactly one of text/base64/hex; the
/// oneof constraint itself is enforced by the schema).
pub fn decode_bytes_input(obj: &ObjectAccessor<'_>) -> Result<Vec<u8>, async_graphql::Error> {
    if let Some(v) = obj.get("text") {
        return Ok(v.string()?.as_bytes().to_vec());
    }
    if let Some(v) = obj.get("base64") {
        return decode_b64(v.string()?);
    }
    if let Some(v) = obj.get("hex") {
        return decode_hex(v.string()?);
    }
    Err(async_graphql::Error::new(
        "BytesInput requires one of text/base64/hex",
    ))
}
