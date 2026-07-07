//! Engine → GraphQL error mapping. Every engine failure carries a
//! machine-readable `extensions.code`; guest failures additionally carry the
//! guest's exit code and output so callers can distinguish "the module
//! rejected this input" from "the engine broke".

use async_graphql::ErrorExtensions;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;

pub fn engine_err(e: fluent31::Error) -> async_graphql::Error {
    use fluent31::Error as E;
    let code = match &e {
        E::Io(_) => "IO",
        E::Corruption(_) => "CORRUPTION",
        E::InvalidArgument(_) => "INVALID_ARGUMENT",
        E::Conflict => "CONFLICT",
        E::Closed => "CLOSED",
        E::Background(_) => "BACKGROUND",
        E::Wasm(_) => "WASM",
        E::GuestFailed { .. } => "GUEST_FAILED",
        E::ProvenanceMismatch(_) => "PROVENANCE_MISMATCH",
        E::Gone(_) => "GONE",
    };
    let err = async_graphql::Error::new(e.to_string()).extend_with(|_, x| x.set("code", code));
    if let E::GuestFailed { code, output } = e {
        return err.extend_with(|_, x| {
            x.set("guestExitCode", code);
            x.set("guestOutputBase64", B64.encode(&output));
            if let Ok(s) = std::str::from_utf8(&output) {
                x.set("guestOutputText", s);
            }
        });
    }
    err
}
