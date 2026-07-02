//! Byte-string handling: engine keys and values are raw bytes, GraphQL
//! speaks text — so input is a oneof over encodings and output is an object
//! whose fields decode lazily.

use async_graphql::{Object, OneofObject};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;

/// Raw bytes supplied by the client in exactly one encoding.
#[derive(OneofObject, Clone, Debug)]
pub enum BytesInput {
    /// UTF-8 text, stored as its bytes.
    Text(String),
    /// RFC 4648 standard base64.
    Base64(String),
    /// Hexadecimal, case-insensitive, no `0x` prefix.
    Hex(String),
}

impl BytesInput {
    pub fn into_bytes(self) -> async_graphql::Result<Vec<u8>> {
        match self {
            BytesInput::Text(s) => Ok(s.into_bytes()),
            BytesInput::Base64(s) => B64
                .decode(s.as_bytes())
                .map_err(|e| async_graphql::Error::new(format!("invalid base64: {e}"))),
            BytesInput::Hex(s) => decode_hex(&s),
        }
    }
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn decode_hex(s: &str) -> async_graphql::Result<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() % 2 != 0 {
        return Err(async_graphql::Error::new("invalid hex: odd length"));
    }
    b.chunks_exact(2)
        .map(|p| Some((nibble(p[0])? << 4) | nibble(p[1])?))
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(|| async_graphql::Error::new("invalid hex: non-hex digit"))
}

fn encode_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((byte & 0xF) as u32, 16).unwrap());
    }
    s
}

/// Raw bytes returned by the engine; request whichever representations you
/// need.
pub struct Bytes(pub Vec<u8>);

#[Object]
impl Bytes {
    /// The bytes decoded as UTF-8; null when they are not valid UTF-8.
    async fn text(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// RFC 4648 standard base64.
    async fn base64(&self) -> String {
        B64.encode(&self.0)
    }

    /// Lowercase hexadecimal.
    async fn hex(&self) -> String {
        encode_hex(&self.0)
    }

    /// Byte length.
    async fn len(&self) -> i32 {
        // values are engine-capped (max_value_size) far below 2^31
        i32::try_from(self.0.len()).unwrap_or(i32::MAX)
    }
}
