//! The four validated newtypes every address on the bus is made of.
//!
//! Addresses are parsed once, at construction, and are infallible thereafter.
//! The router therefore never has to ask whether a destination is well-formed —
//! a [`BusName`] that exists is a [`BusName`] that is valid. This is the same
//! reason D-Bus validates names at the bus rather than at the endpoint: a
//! malformed address is a routing decision you cannot make, and discovering
//! that halfway through a dispatch table is how messages get delivered to the
//! wrong peer.
//!
//! The grammars are deliberately narrower than D-Bus's. Interface and bus names
//! are ASCII-only and disallow leading digits per element, so a name is always
//! a legal identifier in Rust, in a filename, and in a log line that someone
//! will eventually grep.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

fn invalid(kind: &'static str, input: &str, reason: impl fmt::Display) -> Error {
    Error::InvalidName {
        kind,
        input: input.to_string(),
        reason: reason.to_string(),
    }
}

/// Elements of a dotted name: `[A-Za-z_][A-Za-z0-9_]*`, with `-` also allowed
/// after the first character so hyphenated product names survive.
fn validate_dotted(kind: &'static str, input: &str) -> Result<()> {
    if input.is_empty() {
        return Err(invalid(kind, input, "is empty"));
    }
    if input.len() > MAX_NAME_LEN {
        return Err(invalid(
            kind,
            input,
            format!("is longer than {MAX_NAME_LEN} bytes"),
        ));
    }
    let elements: Vec<&str> = input.split('.').collect();
    if elements.len() < 2 {
        return Err(invalid(
            kind,
            input,
            "needs at least two dot-separated elements",
        ));
    }
    for element in elements {
        if element.is_empty() {
            return Err(invalid(kind, input, "has an empty element"));
        }
        let mut chars = element.chars();
        let first = chars.next().expect("element is non-empty");
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(invalid(
                kind,
                input,
                format!("element `{element}` must start with a letter or underscore"),
            ));
        }
        for c in chars {
            if !(c.is_ascii_alphanumeric() || c == '_' || c == '-') {
                return Err(invalid(
                    kind,
                    input,
                    format!("element `{element}` contains `{c}`"),
                ));
            }
        }
    }
    Ok(())
}

/// The cap on any single address component.
///
/// Not a style rule: names arrive from the wire, and an unbounded name is an
/// unbounded allocation in the router's hash map keyed by attacker input.
pub const MAX_NAME_LEN: usize = 255;

/// A peer's address on the bus.
///
/// Two flavours share one type, because every routing decision treats them
/// identically:
///
/// - **Unique** — `:1.7`, assigned by the broker at connect time, never reused
///   for the lifetime of the broker, and released when the peer disconnects.
/// - **Well-known** — `ai.tinyhumans.openhuman.Voice`, requested by a service
///   and owned by at most one peer at a time. This is what the kernel targets;
///   it does not care which process, or which restart of it, answers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BusName(String);

impl BusName {
    /// Parse and validate a bus name.
    pub fn new(input: impl Into<String>) -> Result<Self> {
        let input = input.into();
        if let Some(rest) = input.strip_prefix(':') {
            // Unique names: `:` followed by dot-separated digit runs. The
            // broker mints these, but they also arrive from the wire as
            // `sender` fields, so they get the same treatment as anything else.
            if rest.is_empty() {
                return Err(invalid("bus name", &input, "is just `:`"));
            }
            for element in rest.split('.') {
                if element.is_empty() || !element.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid(
                        "bus name",
                        &input,
                        "a unique name is `:` followed by dot-separated digits",
                    ));
                }
            }
            return Ok(Self(input));
        }
        validate_dotted("bus name", &input)?;
        Ok(Self(input))
    }

    /// Mint the unique name for peer `id`. Broker-internal; the numbering
    /// (`:1.<id>`) mirrors D-Bus so existing tooling reads it without surprise.
    pub(crate) fn unique(id: u64) -> Self {
        Self(format!(":1.{id}"))
    }

    /// Whether this is a broker-assigned unique name rather than a requested
    /// well-known one.
    pub fn is_unique(&self) -> bool {
        self.0.starts_with(':')
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The object an interface is exported at, e.g.
/// `/ai/tinyhumans/openhuman/Voice`.
///
/// A service exports one object per addressable *thing*, not per interface: a
/// mail integration with three accounts exports three paths, each carrying the
/// same `Mailbox` interface. That is what makes the kernel's proxy code
/// account-agnostic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPath(String);

impl ObjectPath {
    /// Parse and validate an object path.
    pub fn new(input: impl Into<String>) -> Result<Self> {
        let input = input.into();
        if input.len() > MAX_NAME_LEN {
            return Err(invalid(
                "object path",
                &input,
                format!("is longer than {MAX_NAME_LEN} bytes"),
            ));
        }
        if input == "/" {
            return Ok(Self(input));
        }
        if !input.starts_with('/') {
            return Err(invalid("object path", &input, "must start with `/`"));
        }
        if input.ends_with('/') {
            return Err(invalid("object path", &input, "must not end with `/`"));
        }
        for element in input[1..].split('/') {
            if element.is_empty() {
                return Err(invalid("object path", &input, "has an empty element"));
            }
            if let Some(c) = element
                .chars()
                .find(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
            {
                return Err(invalid(
                    "object path",
                    &input,
                    format!("element `{element}` contains `{c}`"),
                ));
            }
        }
        Ok(Self(input))
    }

    /// The root path, `/`.
    pub fn root() -> Self {
        Self("/".to_string())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `self` is `other` or lives beneath it.
    ///
    /// Used by [`crate::router::MatchRule`] for subtree subscriptions: a
    /// kernel that wants every mailbox's signals watches
    /// `/ai/tinyhumans/openhuman/Mail` and gets all accounts under it.
    pub fn starts_with(&self, other: &ObjectPath) -> bool {
        if other.0 == "/" {
            return true;
        }
        self.0 == other.0
            || (self.0.starts_with(&other.0) && self.0.as_bytes()[other.0.len()] == b'/')
    }
}

/// The name of an interface, e.g. `ai.tinyhumans.openhuman.Voice`.
///
/// An interface is a *contract*, versioned by renaming: a breaking change ships
/// as `Voice2`, never as a redefinition, so a kernel and a service built months
/// apart either speak or fail loudly at the first call.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InterfaceName(String);

impl InterfaceName {
    /// Parse and validate an interface name.
    pub fn new(input: impl Into<String>) -> Result<Self> {
        let input = input.into();
        validate_dotted("interface name", &input)?;
        Ok(Self(input))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A method or signal name, e.g. `Transcribe`.
///
/// PascalCase by convention and by what the `#[interface]` macro generates from
/// a `snake_case` Rust fn; the grammar itself only bars characters that would
/// make a name ambiguous in a match rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MemberName(String);

impl MemberName {
    /// Parse and validate a member name.
    pub fn new(input: impl Into<String>) -> Result<Self> {
        let input = input.into();
        if input.is_empty() {
            return Err(invalid("member name", &input, "is empty"));
        }
        if input.len() > MAX_NAME_LEN {
            return Err(invalid(
                "member name",
                &input,
                format!("is longer than {MAX_NAME_LEN} bytes"),
            ));
        }
        let mut chars = input.chars();
        let first = chars.next().expect("member name is non-empty");
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(invalid(
                "member name",
                &input,
                "must start with a letter or underscore",
            ));
        }
        if let Some(c) = chars.find(|c| !(c.is_ascii_alphanumeric() || *c == '_')) {
            return Err(invalid("member name", &input, format!("contains `{c}`")));
        }
        Ok(Self(input))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// The four types differ only in their grammar, so everything downstream of
// validation — Display, FromStr, TryFrom, Into<String> — is generated.
macro_rules! name_boilerplate {
    ($($ty:ident),* $(,)?) => {$(
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $ty {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $ty {
            type Error = Error;
            fn try_from(s: String) -> Result<Self> {
                Self::new(s)
            }
        }

        impl TryFrom<&str> for $ty {
            type Error = Error;
            fn try_from(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }

        impl From<$ty> for String {
            fn from(v: $ty) -> String {
                v.0
            }
        }

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    )*};
}

name_boilerplate!(BusName, ObjectPath, InterfaceName, MemberName);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_and_unique_names_both_parse() {
        assert!(
            !BusName::new("ai.tinyhumans.openhuman.Voice")
                .unwrap()
                .is_unique()
        );
        assert!(BusName::new(":1.42").unwrap().is_unique());
    }

    #[test]
    fn a_single_element_is_not_a_bus_name() {
        // The two-element minimum is what stops an integration from squatting
        // `Voice` and colliding with every other vendor on the bus.
        let err = BusName::new("Voice").unwrap_err();
        assert!(
            err.to_string().contains("two dot-separated elements"),
            "{err}"
        );
    }

    #[test]
    fn a_unique_name_must_be_digits() {
        assert!(BusName::new(":1.beta").is_err());
        assert!(BusName::new(":").is_err());
    }

    #[test]
    fn dotted_names_reject_empty_and_leading_digit_elements() {
        assert!(InterfaceName::new("ai..Voice").is_err());
        assert!(InterfaceName::new("ai.9lives").is_err());
        assert!(InterfaceName::new("ai.tiny-humans.Voice").is_ok());
    }

    #[test]
    fn over_long_names_are_refused_rather_than_stored() {
        let long = format!("ai.{}", "x".repeat(MAX_NAME_LEN));
        assert!(InterfaceName::new(long).is_err());
    }

    #[test]
    fn object_paths_follow_the_slash_grammar() {
        assert!(ObjectPath::new("/").is_ok());
        assert!(ObjectPath::new("/ai/tinyhumans/openhuman/Voice").is_ok());
        assert!(ObjectPath::new("ai/tinyhumans").is_err());
        assert!(ObjectPath::new("/ai/").is_err());
        assert!(ObjectPath::new("/ai//voice").is_err());
        assert!(ObjectPath::new("/ai/voice-1").is_err());
    }

    #[test]
    fn subtree_matching_does_not_match_a_sibling_with_a_shared_prefix() {
        let mail = ObjectPath::new("/ai/Mail").unwrap();
        let account = ObjectPath::new("/ai/Mail/work").unwrap();
        let sibling = ObjectPath::new("/ai/Mailbox").unwrap();
        assert!(account.starts_with(&mail));
        assert!(mail.starts_with(&mail));
        assert!(!sibling.starts_with(&mail));
        assert!(sibling.starts_with(&ObjectPath::root()));
    }

    #[test]
    fn names_round_trip_through_json() {
        let name = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"ai.tinyhumans.openhuman.Voice\"");
        assert_eq!(serde_json::from_str::<BusName>(&json).unwrap(), name);
    }

    #[test]
    fn deserializing_a_malformed_name_fails_rather_than_producing_one() {
        // The router's invariant — every stored name is valid — depends on
        // this: the wire is the only place an unvalidated name can enter.
        assert!(serde_json::from_str::<BusName>("\"nope\"").is_err());
    }

    #[test]
    fn member_names_reject_dots_so_match_rules_stay_unambiguous() {
        assert!(MemberName::new("Transcribe").is_ok());
        assert!(MemberName::new("Voice.Transcribe").is_err());
    }
}
