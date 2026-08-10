//! The crate error type and its `Result` alias.
//!
//! Every fallible function in tinybus returns [`Result<T>`]. Two things make
//! this error type unusual, and both are deliberate:
//!
//! 1. It round-trips the wire. A method call that fails in a service process
//!    has to arrive back at the caller as an error, not as a successful reply
//!    containing a sad-looking value — so [`Error::MethodFailed`] carries a
//!    stable `name` (`ai.tinyhumans.tinybus.Error.UnknownMethod`) that a caller
//!    can match on across a process boundary, plus prose for humans.
//! 2. It never carries the payload that caused it. Bodies routinely hold
//!    OAuth tokens, mail contents and wallet material; a `Debug`-printed
//!    message in a log is an exfiltration path. Errors name the *member* and
//!    the *type*, never the value.

use std::path::PathBuf;

use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};

/// Errors produced anywhere in tinybus.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A name, path, interface or member did not satisfy its grammar.
    ///
    /// Validation happens at the newtype boundary, so this is raised at
    /// construction time rather than in the router — a malformed destination
    /// can never reach the routing table.
    #[error("invalid {kind} {input:?}: {reason}")]
    InvalidName {
        /// What was being parsed: `bus name`, `object path`, …
        kind: &'static str,
        /// The offending input, quoted.
        input: String,
        /// Which rule it broke.
        reason: String,
    },

    /// The message could not be framed, parsed, or exceeded the size cap.
    #[error("protocol: {0}")]
    Protocol(String),

    /// The transport is gone: the socket closed, the peer exited, the
    /// in-memory channel was dropped.
    #[error("transport: {0}")]
    Transport(String),

    /// The connection's dispatch loop has stopped, so no further calls can be
    /// made on it. Terminal — reconnect rather than retry.
    #[error("connection closed")]
    ConnectionClosed,

    /// The outbound queue is full: this process is producing faster than the
    /// broker is draining.
    ///
    /// Only ever returned by the non-blocking senders
    /// ([`crate::Connection::try_send`] and friends). It is a *dropped
    /// notification*, not a lost call — a caller that cannot tolerate the drop
    /// should use the awaiting send and take the backpressure instead.
    #[error("outbound queue is full; message dropped")]
    Backpressure,

    /// No peer owns the destination name.
    ///
    /// The common cause is an integration that has not been started yet, which
    /// is why the message names the destination rather than saying "not found".
    #[error("no peer owns the name `{0}`")]
    NameHasNoOwner(BusName),

    /// `RequestName` lost: another peer already owns it and did not allow
    /// replacement.
    #[error("`{name}` is already owned by {owner}")]
    NameTaken {
        /// The contested well-known name.
        name: BusName,
        /// The unique name of the peer that holds it.
        owner: BusName,
    },

    /// The destination peer exports no object at that path.
    ///
    /// Carries only the path: the caller already knows which destination it
    /// addressed, and the *service* — which is where this is raised — knows
    /// its unique name but not which of its names the caller used.
    #[error("no object at `{path}`")]
    UnknownObject {
        /// The path that was asked for.
        path: ObjectPath,
    },

    /// The object exists but does not implement that interface.
    #[error("{path}: no interface `{interface}`")]
    UnknownInterface {
        /// The object that was found.
        path: ObjectPath,
        /// The interface that was not on it.
        interface: InterfaceName,
    },

    /// The interface exists but has no such member.
    #[error("{interface}: no member `{member}`")]
    UnknownMethod {
        /// The interface that was dispatched to.
        interface: InterfaceName,
        /// The member that was not on it.
        member: MemberName,
    },

    /// An event's domain is not usable as an object-path element.
    ///
    /// A catalog bug in the host, not a bus failure: the event cannot be
    /// addressed, so it cannot be published. Named rather than silently
    /// dropped, because an event that vanishes with no error is the hardest
    /// possible thing to debug from the subscriber's end.
    #[error("event domain {domain:?} is not a usable path element: {reason}")]
    InvalidDomain {
        /// The offending domain string.
        domain: String,
        /// Why it cannot be a path element.
        reason: String,
    },

    /// A method's arguments did not deserialize into the signature it declares.
    ///
    /// Carries the member and the serde message — never the arguments.
    #[error("{member}: bad arguments: {reason}")]
    BadArguments {
        /// The member that was called.
        member: MemberName,
        /// What serde objected to.
        reason: String,
    },

    /// A peer speaks a version of an interface this peer cannot work with.
    ///
    /// Raised by [`crate::Connection::require`] at the point of checking, not
    /// at the point of failing — which is the difference between a startup
    /// error naming two versions and a deserialize error hours later naming
    /// neither.
    #[error("{peer} is not compatible on {interface}: {detail}")]
    IncompatibleVersion {
        /// The peer that was checked.
        peer: String,
        /// The interface in question.
        interface: String,
        /// Which side rejected which version.
        detail: String,
    },

    /// A method ran and failed. This is the variant that crosses the wire.
    #[error("{name}: {message}")]
    MethodFailed {
        /// A stable, dotted error name a caller can match on.
        name: String,
        /// Prose for a human. Never the arguments, never a credential.
        message: String,
    },

    /// A call exceeded its deadline. The service may still be running it;
    /// tinybus does not cancel remote work, it stops waiting.
    #[error("call to `{member}` timed out after {timeout_ms}ms")]
    Timeout {
        /// The member that was called.
        member: MemberName,
        /// The deadline that elapsed.
        timeout_ms: u64,
    },

    /// A path that had to exist did not, or could not be used.
    #[error("{path}: {message}")]
    Path {
        /// The offending path.
        path: PathBuf,
        /// What went wrong with it.
        message: String,
    },

    /// A feature required for this code path was not compiled in.
    #[error("{0} requires the `{1}` feature; rebuild with --features {1}")]
    FeatureDisabled(&'static str, &'static str),

    /// A dynamic module failed a fixed admission rule.
    ///
    /// `file` is a basename only and `reason` is selected by the host. Neither
    /// field may contain a path or attacker-controlled descriptor bytes.
    #[error("module `{file}` refused: {reason}")]
    ModuleRefused {
        /// Sanitized artifact basename.
        file: String,
        /// Fixed admission failure phrase.
        reason: String,
    },

    /// A known module cannot serve calls in its terminal/current state.
    #[error("module `{module}` is unavailable ({state}): {detail}")]
    ModuleUnavailable {
        /// Stable module identity.
        module: String,
        /// Closed lifecycle state name.
        state: String,
        /// Safe state detail, never a body or environment value.
        detail: String,
    },

    /// Filesystem or socket I/O failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization failed.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// The dotted error name for the `UnknownMethod` family.
    pub const UNKNOWN_METHOD: &'static str = "ai.tinyhumans.tinybus.Error.UnknownMethod";
    /// The dotted error name a failing method body gets by default.
    pub const FAILED: &'static str = "ai.tinyhumans.tinybus.Error.Failed";

    /// Build an [`Error::Protocol`] from anything displayable.
    pub fn protocol(message: impl std::fmt::Display) -> Self {
        Self::Protocol(message.to_string())
    }

    /// Build an [`Error::Transport`] from anything displayable.
    pub fn transport(message: impl std::fmt::Display) -> Self {
        Self::Transport(message.to_string())
    }

    /// Build an [`Error::Path`] for `path`.
    pub fn path(path: impl Into<PathBuf>, message: impl std::fmt::Display) -> Self {
        Self::Path {
            path: path.into(),
            message: message.to_string(),
        }
    }

    /// Build a redacted module refusal from an artifact path.
    pub fn module_refused(path: &std::path::Path, reason: impl Into<String>) -> Self {
        let file = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(sanitize_untrusted)
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "module".to_string());
        let reason = reason.into();
        let reason = reason
            .split_whitespace()
            .map(sanitize_untrusted)
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        Self::ModuleRefused { file, reason }
    }

    /// Build an [`Error::BadArguments`] from a serde failure, with the
    /// offending *values* stripped out.
    ///
    /// serde's messages quote what it choked on: "invalid type: integer `42`,
    /// expected a string". That is exactly what a developer wants and
    /// exactly what must not be in this error: argument bodies carry OAuth
    /// tokens, recovery phrases and mail contents, and this string travels back
    /// across the bus and into the caller's logs. [`redact_values`] keeps the
    /// shape of the complaint and drops the payload.
    pub fn bad_arguments(member: MemberName, reason: impl std::fmt::Display) -> Self {
        Self::BadArguments {
            member,
            reason: redact_values(&reason.to_string()),
        }
    }

    /// Build an [`Error::InvalidDomain`] for `domain`.
    pub fn invalid_domain(domain: impl Into<String>, reason: impl std::fmt::Display) -> Self {
        Self::InvalidDomain {
            domain: domain.into(),
            reason: reason.to_string(),
        }
    }

    /// Build the generic remote failure, the one a service returns when its own
    /// error type has no better mapping.
    pub fn failed(message: impl std::fmt::Display) -> Self {
        Self::MethodFailed {
            name: Self::FAILED.to_string(),
            message: message.to_string(),
        }
    }

    /// The prose half of this error, as it should travel on the wire.
    ///
    /// For everything except [`Error::MethodFailed`] that is just `Display`.
    /// For `MethodFailed` it is the message *without* the name, because the
    /// name travels in its own header field — including it here is how an
    /// error picks up a duplicated prefix each time it crosses the bus.
    pub fn wire_message(&self) -> String {
        match self {
            Self::MethodFailed { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }

    /// The stable dotted name this error travels under.
    ///
    /// Callers match on this, not on the prose. Every variant maps to one, so
    /// an error crossing the wire and being reconstructed on the far side keeps
    /// its identity even though it loses its structure.
    pub fn wire_name(&self) -> &str {
        match self {
            Self::InvalidName { .. } => "ai.tinyhumans.tinybus.Error.InvalidName",
            Self::Protocol(_) => "ai.tinyhumans.tinybus.Error.Protocol",
            Self::Transport(_) | Self::Io(_) => "ai.tinyhumans.tinybus.Error.Transport",
            Self::ConnectionClosed => "ai.tinyhumans.tinybus.Error.ConnectionClosed",
            Self::Backpressure => "ai.tinyhumans.tinybus.Error.Backpressure",
            Self::NameHasNoOwner(_) => "ai.tinyhumans.tinybus.Error.NameHasNoOwner",
            Self::NameTaken { .. } => "ai.tinyhumans.tinybus.Error.NameTaken",
            Self::UnknownObject { .. } => "ai.tinyhumans.tinybus.Error.UnknownObject",
            Self::UnknownInterface { .. } => "ai.tinyhumans.tinybus.Error.UnknownInterface",
            Self::UnknownMethod { .. } => Self::UNKNOWN_METHOD,
            Self::BadArguments { .. } => "ai.tinyhumans.tinybus.Error.BadArguments",
            Self::InvalidDomain { .. } => "ai.tinyhumans.tinybus.Error.InvalidDomain",
            Self::Timeout { .. } => "ai.tinyhumans.tinybus.Error.Timeout",
            Self::IncompatibleVersion { .. } => "ai.tinyhumans.tinybus.Error.IncompatibleVersion",
            Self::ModuleRefused { .. } => "ai.tinyhumans.tinybus.Error.ModuleRefused",
            Self::ModuleUnavailable { .. } => "ai.tinyhumans.tinybus.Error.ModuleUnavailable",
            Self::Path { .. } => "ai.tinyhumans.tinybus.Error.Path",
            Self::FeatureDisabled(_, _) => "ai.tinyhumans.tinybus.Error.FeatureDisabled",
            Self::Json(_) => "ai.tinyhumans.tinybus.Error.Json",
            Self::MethodFailed { name, .. } => name,
        }
    }
}

/// Replace every backtick-quoted span with `…`.
///
/// serde puts the values it rejected in backticks, and so do most of the
/// libraries a service will wrap. Redacting the span rather than dropping the
/// whole message keeps the diagnostic — "invalid type: integer, expected a
/// string" still tells you what went wrong — while making the error safe to log
/// and safe to send to a peer that must not see the argument.
pub fn redact_values(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut inside = false;
    for c in message.chars() {
        match (c, inside) {
            ('`', false) => {
                out.push_str("`…");
                inside = true;
            }
            ('`', true) => {
                out.push('`');
                inside = false;
            }
            (_, false) => out.push(c),
            (_, true) => {}
        }
    }
    out
}

/// Keep only log-safe descriptor characters and cap their length.
pub(crate) fn sanitize_untrusted(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '+' | '-'))
        .take(32)
        .collect()
}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_disabled_names_the_flag_to_rebuild_with() {
        let err = Error::FeatureDisabled("serving over a socket", "uds");
        assert_eq!(
            err.to_string(),
            "serving over a socket requires the `uds` feature; rebuild with --features uds"
        );
    }

    #[test]
    fn a_method_failure_travels_under_its_own_name() {
        let err = Error::MethodFailed {
            name: "ai.tinyhumans.openhuman.Voice.Error.NoDevice".into(),
            message: "no capture device".into(),
        };
        assert_eq!(
            err.wire_name(),
            "ai.tinyhumans.openhuman.Voice.Error.NoDevice"
        );
    }

    #[test]
    fn bad_arguments_keeps_the_diagnosis_and_drops_the_value() {
        // serde's own phrasing for a type mismatch, which quotes the value.
        let err = Error::bad_arguments(
            MemberName::new("Sign").unwrap(),
            "invalid type: string \"seed phrase here\", expected u64 at `0xdeadbeef`",
        );
        let text = err.to_string();
        assert!(text.contains("expected u64"), "{text}");
        assert!(!text.contains("0xdeadbeef"), "{text}");
    }

    #[test]
    fn redaction_survives_an_unclosed_quote() {
        // A truncated message must not leak the tail just because its closing
        // backtick never arrived.
        assert_eq!(redact_values("bad token `abc"), "bad token `…");
        assert_eq!(redact_values("no quotes here"), "no quotes here");
    }

    #[test]
    fn a_generic_failure_falls_back_to_the_failed_name() {
        assert_eq!(Error::failed("boom").wire_name(), Error::FAILED);
    }

    #[test]
    fn missing_owner_names_the_integration_that_is_not_running() {
        let err =
            Error::NameHasNoOwner(BusName::try_from("ai.tinyhumans.openhuman.Voice").unwrap());
        assert_eq!(
            err.to_string(),
            "no peer owns the name `ai.tinyhumans.openhuman.Voice`"
        );
    }

    #[test]
    fn module_refusal_sanitizes_an_untrusted_reason() {
        let error = Error::module_refused(
            std::path::Path::new("module.so"),
            "loader exposed /secret/path and spaces",
        );
        let Error::ModuleRefused { reason, .. } = error else {
            panic!("expected module refusal");
        };
        assert_eq!(reason, "loader exposed secretpath and spaces");
    }

    #[test]
    fn module_refusal_uses_a_safe_filename_fallback() {
        let error = Error::module_refused(std::path::Path::new("/"), "refused");
        let Error::ModuleRefused { file, .. } = error else {
            panic!("expected module refusal");
        };
        assert_eq!(file, "module");
    }

    #[test]
    fn every_structured_error_has_a_stable_wire_name() {
        let bus = BusName::new("ai.tinyhumans.Example").unwrap();
        let path = ObjectPath::new("/ai/tinyhumans/Example").unwrap();
        let interface = InterfaceName::new("ai.tinyhumans.Example").unwrap();
        let member = MemberName::new("Call").unwrap();
        let errors = [
            Error::InvalidName {
                kind: "name",
                input: "bad".into(),
                reason: "bad".into(),
            },
            Error::protocol("bad"),
            Error::transport("bad"),
            Error::ConnectionClosed,
            Error::Backpressure,
            Error::NameHasNoOwner(bus.clone()),
            Error::NameTaken {
                name: bus.clone(),
                owner: bus,
            },
            Error::UnknownObject { path: path.clone() },
            Error::UnknownInterface {
                path,
                interface: interface.clone(),
            },
            Error::UnknownMethod {
                interface,
                member: member.clone(),
            },
            Error::bad_arguments(member.clone(), "bad"),
            Error::invalid_domain("bad", "bad"),
            Error::Timeout {
                member,
                timeout_ms: 1,
            },
            Error::IncompatibleVersion {
                peer: "peer".into(),
                interface: "interface".into(),
                detail: "bad".into(),
            },
            Error::module_refused(std::path::Path::new("module.so"), "bad"),
            Error::ModuleUnavailable("module".into()),
            Error::path("path", "bad"),
            Error::FeatureDisabled("thing", "uds"),
            Error::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
        ];
        for error in errors {
            assert!(error.wire_name().starts_with("ai.tinyhumans."));
        }
    }
}
