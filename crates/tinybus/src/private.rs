//! Support code for the code `#[tinybus::interface]` generates.
//!
//! Not a public API. It is `pub` because generated code lives in the *user's*
//! crate and has to be able to name it, and it is `#[doc(hidden)]` because
//! nothing here is a promise. Anything the macro needs goes through this module
//! rather than being emitted inline, so a fix to argument decoding is a change
//! to one function rather than a recompile-the-world change to the expansion.

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use async_trait::async_trait;
pub use serde_json::Value;

use crate::error::{Error, Result};
use crate::name::{InterfaceName, MemberName};

/// Deserialize a positional argument array into a method's parameter tuple.
///
/// Names the member and quotes serde's complaint, but never the arguments —
/// bodies routinely carry credentials, and an error string ends up in logs.
pub fn decode_args<T: DeserializeOwned>(member: &MemberName, args: Value) -> Result<T> {
    serde_json::from_value(args).map_err(|e| Error::bad_arguments(member.clone(), e))
}

/// Serialize a method's return value into a reply body.
pub fn encode_reply<T: Serialize>(value: &T) -> Result<Value> {
    Ok(serde_json::to_value(value)?)
}

/// Parse an interface name written as a literal in `#[interface(name = "…")]`.
///
/// Panics on a malformed name. The alternative — surfacing it as a `Result` on
/// every dispatch — would push a mistake that is fully determined at compile
/// time into every call site's error handling. It panics at the service's first
/// registration, in its own process, with the offending literal quoted.
pub fn parse_interface(name: &str) -> InterfaceName {
    InterfaceName::new(name).unwrap_or_else(|e| panic!("#[interface(name = \"{name}\")]: {e}"))
}

/// Parse a member name derived from a method name. Panics for the same reason
/// as [`parse_interface`].
pub fn parse_member(name: &str) -> MemberName {
    MemberName::new(name).unwrap_or_else(|e| panic!("interface member `{name}`: {e}"))
}

/// The error a generated `call` returns for a member it does not have.
pub fn unknown_method(interface: &str, member: &str) -> Error {
    Error::UnknownMethod {
        interface: parse_interface(interface),
        member: MemberName::new(member).unwrap_or_else(|_| parse_member("Unknown")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_helpers_round_trip_values_and_redact_bad_arguments() {
        let member = MemberName::new("Add").unwrap();
        let decoded: (u32, u32) = decode_args(&member, serde_json::json!([2, 3])).unwrap();
        assert_eq!(decoded, (2, 3));
        assert_eq!(encode_reply(&5u32).unwrap(), serde_json::json!(5));

        let sensitive = "sensitive-argument-value";
        let error = decode_args::<(u32,)>(&member, serde_json::json!([sensitive])).unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains("Add"));
        assert!(rendered.contains("bad arguments"));
        assert!(!rendered.contains(sensitive));
    }

    #[test]
    fn literal_parsers_reject_invalid_generated_names() {
        assert_eq!(
            parse_interface("ai.tinyhumans.Example").as_str(),
            "ai.tinyhumans.Example"
        );
        assert_eq!(parse_member("Call").as_str(), "Call");
        assert!(std::panic::catch_unwind(|| parse_interface("not-valid")).is_err());
        assert!(std::panic::catch_unwind(|| parse_member("not.valid")).is_err());
    }

    #[test]
    fn unknown_method_uses_a_safe_fallback_member() {
        let error = unknown_method("ai.tinyhumans.Example", "not.valid");
        let rendered = error.to_string();
        assert!(rendered.contains("ai.tinyhumans.Example"));
        assert!(rendered.contains("Unknown"));
    }
}
