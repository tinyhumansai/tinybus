//! The wire format: one [`Message`] type, five kinds, JSON bodies.
//!
//! # Why JSON and not a binary encoding
//!
//! D-Bus has its own marshalling; we deliberately do not. The integrations on
//! this bus are things like "transcribe a file" and "sign a transaction" — the
//! call rate is human-scale, and the body is dwarfed by the work it triggers.
//! What we get in exchange is that `tinybus monitor` is readable, a service can
//! be written in any language in an afternoon, and `serde` derives on the
//! kernel side are the entire client binding. The one place this would be the
//! wrong trade is bulk binary payloads (audio, PDFs), which is why those do not
//! travel in a body at all: [`crate::stream`] carries them beside the call as
//! chunks, and the body carries only a handle. A path is still cheaper when
//! both peers can see the same filesystem.
//!
//! # Why the header is flat
//!
//! Routing reads `destination` and nothing else. Keeping it a flat struct of
//! `Option`s rather than an enum per kind means the broker can route a message
//! without reading the body. Directed signals add a destination-bound hint for
//! a live local callback; older peers reject that new message kind safely.

pub mod codec;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};

/// What a message is for.
///
/// Non-exhaustive on purpose: a broker that meets a kind it does not know still
/// routes it, because routing only needs the destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MessageKind {
    /// A request for a method to run, expecting exactly one reply.
    MethodCall,
    /// The successful reply to a [`MessageKind::MethodCall`].
    MethodReturn,
    /// The failed reply to a [`MessageKind::MethodCall`].
    Error,
    /// A broadcast. Has no reply and no destination; delivery is decided by
    /// each peer's [`crate::router::MatchRule`]s.
    Signal,
    /// A one-recipient signal. The broker authorizes the exact callback binding
    /// and routes only to its current module peer.
    DirectedSignal,
}

/// Closed lifecycle metadata accompanying a module availability error.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleLifecycle {
    /// The module has not finished its current initialization attempt.
    Initializing,
    /// A future lifecycle state this version cannot classify safely.
    Unknown,
}

impl<'de> Deserialize<'de> for ModuleLifecycle {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "initializing" => Self::Initializing,
            _ => Self::Unknown,
        })
    }
}

/// Everything the router and the dispatcher need, and nothing else.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Header {
    /// What this message is for.
    pub kind: MessageKind,
    /// Per-connection, monotonically increasing. Unique only within a sender.
    pub serial: u64,
    /// For replies: the `serial` of the call being answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_serial: Option<u64>,
    /// Stamped by the broker, never trusted from the peer.
    ///
    /// Caller authorization uses `CallContext::authenticated_sender`, which
    /// also verifies delivery provenance. Direct transports can supply arbitrary
    /// headers, so this wire field alone is not authenticated authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<BusName>,
    /// Conditional call routing: deliver only while this unique peer owns the
    /// destination. Optional on the wire; guarded clients require the broker's
    /// `ResolveCallTarget` method before relying on this restriction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_responder: Option<BusName>,
    /// Who the message is for. Broadcast signals omit this; directed signals
    /// carry the unique peer selected by their live callback binding. A
    /// [`MessageKind::Signal`] may carry a unique name to be delivered to that
    /// one peer only (still subject to its match rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<BusName>,
    /// The object being addressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<ObjectPath>,
    /// The interface the member belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<InterfaceName>,
    /// The method or signal name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberName>,
    /// For [`MessageKind::Error`]: the stable dotted error name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_name: Option<String>,
    /// Closed lifecycle metadata for module availability errors. Older peers
    /// ignore this optional field and continue to classify by `error_name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_lifecycle: Option<ModuleLifecycle>,
    /// This body is a secret: deliver it to the attested destination, an exact
    /// active SDK-created local host callback capability, or nobody.
    ///
    /// Set by the sender and *not* overwritten on ingress, unlike `sender`. The
    /// asymmetry is deliberate and safe in this direction: the flag only ever
    /// causes the broker to apply more restrictions, so a peer that forges it
    /// can restrict its own traffic and nothing else. A flag the broker
    /// controlled would instead need a rule for who may ask for confidentiality,
    /// and there is no such rule worth having — everyone may.
    ///
    /// An older broker that does not know this field routes the message
    /// normally, which is why a sender must not assume the guarantee holds
    /// without checking `GetAttestation` first. See [`crate::attest`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub confidential: bool,
    /// Host-control payload that must be redacted from observation surfaces.
    /// Unlike `confidential`, this does not claim an attested remote recipient;
    /// it is accepted only for the broker's module configuration methods.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sensitive: bool,
    /// The caller accepts a successful result through a reply stream.
    ///
    /// Optional for wire compatibility: an older peer ignores this field and
    /// returns an ordinary method result, which the caller still accepts.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stream_reply: bool,
}

/// A framed message: header plus a JSON body.
///
/// The body is positional — a JSON array of arguments for a call, a single
/// value for a return. Positional rather than named because it makes the
/// generated dispatch in `#[interface]` a plain tuple deserialize, and because
/// a named body invites callers to depend on parameter *names*, which are an
/// implementation detail of the service's Rust signature.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Routing and dispatch metadata.
    pub header: Header,
    /// The payload. `Value::Null` when there is none, and omitted from the
    /// wire in that case — a bus carrying mostly argument-less signals should
    /// not spend four bytes per message saying so.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub body: Value,
}

impl Message {
    /// Build a method call. `serial` is assigned by the connection at send
    /// time, so this starts at zero.
    pub fn method_call(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        Self {
            header: Header {
                kind: MessageKind::MethodCall,
                serial: 0,
                reply_serial: None,
                sender: None,
                expected_responder: None,
                destination: Some(destination),
                path: Some(path),
                interface: Some(interface),
                member: Some(member),
                error_name: None,
                module_lifecycle: None,
                confidential: false,
                sensitive: false,
                stream_reply: false,
            },
            body,
        }
    }

    /// Build a method call whose body the broker must not let anyone but the
    /// attested destination see.
    ///
    /// Use it for the payloads whose disclosure is the failure — a private key,
    /// a recovery phrase, a bearer token. The broker will refuse to deliver it
    /// unless it has itself verified the destination's artifact against the
    /// operator's trust store, so a send that would have gone to an
    /// impersonator fails instead of succeeding quietly. See [`crate::attest`]
    /// for exactly what "verified" covers.
    pub fn confidential_call(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        let mut message = Self::method_call(destination, path, interface, member, body);
        message.header.confidential = true;
        message
    }

    /// Build a private host-control call for module configuration.
    pub(crate) fn sensitive_control_call(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        let mut message = Self::method_call(destination, path, interface, member, body);
        message.header.sensitive = true;
        message
    }

    /// Build a method call that asks the service to stream its successful
    /// serialized result while retaining ordinary-reply compatibility.
    pub(crate) fn streaming_call(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        let mut message = Self::method_call(destination, path, interface, member, body);
        message.header.stream_reply = true;
        message
    }

    /// Build the successful reply to `call`.
    pub fn method_return(call: &Header, body: Value) -> Self {
        Self {
            header: Header {
                kind: MessageKind::MethodReturn,
                serial: 0,
                reply_serial: Some(call.serial),
                sender: None,
                expected_responder: None,
                // Replies are addressed back at the *unique* name the broker
                // stamped, not at whatever well-known name the caller happens
                // to own — the caller may have released it mid-call.
                destination: call.sender.clone(),
                path: None,
                interface: None,
                member: None,
                error_name: None,
                module_lifecycle: None,
                // A reply inherits the call's confidentiality. A secret asked
                // for confidentially is usually answered with another one — a
                // key derivation returns a key — and a reply that quietly lost
                // the flag would be the leak the call avoided, one hop later.
                confidential: call.confidential,
                sensitive: false,
                stream_reply: false,
            },
            body,
        }
    }

    /// Build the failed reply to `call`.
    ///
    /// Takes the [`Error`] rather than a string so the dotted name and the
    /// prose cannot drift apart.
    pub fn error_reply(call: &Header, error: &Error) -> Self {
        Self {
            header: Header {
                kind: MessageKind::Error,
                serial: 0,
                reply_serial: Some(call.serial),
                sender: None,
                expected_responder: None,
                destination: call.sender.clone(),
                path: None,
                interface: None,
                member: None,
                error_name: Some(error.wire_name().to_string()),
                module_lifecycle: match error {
                    Error::ModuleUnavailable { state, .. }
                        if state.eq_ignore_ascii_case("initializing") =>
                    {
                        Some(ModuleLifecycle::Initializing)
                    }
                    _ => None,
                },
                // Not inherited. Errors never carry the value that caused them,
                // so an error reply has no secret to protect — and marking it
                // confidential would make it undeliverable exactly when the
                // recipient failed attestation, swallowing the diagnosis.
                confidential: false,
                sensitive: false,
                stream_reply: false,
            },
            body: Value::String(error.wire_message()),
        }
    }

    /// Build a broadcast signal. The router fans it out to every peer whose
    /// match rules accept it. Use [`Message::signal_to`] to address one
    /// unique-name peer.
    pub fn signal(
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        Self {
            header: Header {
                kind: MessageKind::Signal,
                serial: 0,
                reply_serial: None,
                sender: None,
                expected_responder: None,
                destination: None,
                path: Some(path),
                interface: Some(interface),
                member: Some(member),
                error_name: None,
                module_lifecycle: None,
                // Structurally impossible to set: a signal is a broadcast, and
                // `validate` refuses the combination on ingress.
                confidential: false,
                sensitive: false,
                stream_reply: false,
            },
            body,
        }
    }

    /// Build a signal for one peer. The router delivers it only to the peer
    /// that owns the unique name `destination`, and only if that peer has a
    /// match rule accepting it; every other subscriber sees nothing.
    ///
    /// Only a broker that implements addressed signals honours the
    /// destination; a broker that predates it ignores the field and broadcasts
    /// by match rules, so use this only against a broker of the same release.
    pub fn signal_to(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        let mut message = Self::signal(path, interface, member, body);
        message.header.destination = Some(destination);
        message
    }

    /// Build an exact-recipient signal for SDK-owned callback routing.
    ///
    /// The broker accepts this kind only for a currently live host callback
    /// binding that declared the exact source, destination, path, interface,
    /// and member. It is not a general unicast escape hatch.
    pub(crate) fn directed_signal(
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: Value,
    ) -> Self {
        Self {
            header: Header {
                kind: MessageKind::DirectedSignal,
                serial: 0,
                reply_serial: None,
                sender: None,
                expected_responder: None,
                destination: Some(destination),
                path: Some(path),
                interface: Some(interface),
                member: Some(member),
                error_name: None,
                module_lifecycle: None,
                confidential: false,
                sensitive: false,
                stream_reply: false,
            },
            body,
        }
    }

    /// Reconstruct the [`Error`] an [`MessageKind::Error`] message carries.
    ///
    /// Most error structure does not survive the wire — only its name and
    /// message do. The optional closed module lifecycle metadata reconstructs
    /// the typed initializing state while retaining the stable error name.
    pub fn into_error(self) -> Error {
        let name = self
            .header
            .error_name
            .unwrap_or_else(|| Error::FAILED.to_string());
        if self.header.kind == MessageKind::Error
            && name == Error::MODULE_UNAVAILABLE
            && self.header.module_lifecycle == Some(ModuleLifecycle::Initializing)
        {
            return Error::ModuleUnavailable {
                module: "module".to_owned(),
                state: "initializing".to_owned(),
                detail: "module initialization is still in progress".to_owned(),
            };
        }
        let message = match self.body {
            Value::String(s) => s,
            other => other.to_string(),
        };
        Error::MethodFailed { name, message }
    }

    /// The member this message addresses, for error messages that want to name
    /// it. Returns a placeholder rather than failing — this is only ever used
    /// to build a message a human reads.
    pub(crate) fn member_or_unknown(&self) -> MemberName {
        self.header
            .member
            .clone()
            .unwrap_or_else(|| MemberName::new("Unknown").expect("literal is a valid member"))
    }

    /// Check the invariants the router relies on before a message is accepted
    /// from a peer.
    ///
    /// Called on ingress, once, so that every later stage can index the header
    /// without re-checking. A call with no destination would otherwise sit in
    /// the router as an unroutable message with a caller blocked on it forever.
    pub fn validate(&self) -> Result<()> {
        if self.header.stream_reply && self.header.kind != MessageKind::MethodCall {
            return Err(Error::protocol(
                "only a method call can request a streamed reply",
            ));
        }
        if self
            .header
            .expected_responder
            .as_ref()
            .is_some_and(|expected| {
                self.header.kind != MessageKind::MethodCall || !expected.is_unique()
            })
        {
            return Err(Error::protocol(
                "expected responder must be unique and only accompany a call",
            ));
        }
        // Checked before the per-kind rules, and checked on ingress rather than
        // at delivery: a confidential signal has no destination, so there is no
        // one recipient to attest and fan-out is the only thing it could mean.
        // Refusing it here means no later stage has to ask whether a broadcast
        // might be a secret.
        if self.header.confidential || self.header.sensitive {
            if matches!(
                self.header.kind,
                MessageKind::Signal | MessageKind::DirectedSignal
            ) {
                return Err(Error::protocol("signals cannot carry a private body"));
            }
            if self.header.destination.is_none() {
                return Err(Error::protocol("a private message needs a destination"));
            }
        }
        match self.header.kind {
            MessageKind::MethodCall => {
                if self.header.destination.is_none() {
                    return Err(Error::protocol("method call has no destination"));
                }
                if self.header.path.is_none()
                    || self.header.interface.is_none()
                    || self.header.member.is_none()
                {
                    return Err(Error::protocol(
                        "method call needs a path, an interface and a member",
                    ));
                }
            }
            MessageKind::MethodReturn | MessageKind::Error => {
                if self.header.reply_serial.is_none() {
                    return Err(Error::protocol("reply has no reply_serial"));
                }
            }
            MessageKind::Signal => {
                if self.header.interface.is_none() || self.header.member.is_none() {
                    return Err(Error::protocol("signal needs an interface and a member"));
                }
                if self
                    .header
                    .destination
                    .as_ref()
                    .is_some_and(|name| !name.is_unique())
                {
                    return Err(Error::protocol(
                        "a signal can only be addressed to a unique name",
                    ));
                }
            }
            MessageKind::DirectedSignal => {
                if self
                    .header
                    .destination
                    .as_ref()
                    .is_none_or(|name| !name.is_unique())
                {
                    return Err(Error::protocol(
                        "directed signal needs a unique destination",
                    ));
                }
                if self.header.path.is_none()
                    || self.header.interface.is_none()
                    || self.header.member.is_none()
                {
                    return Err(Error::protocol(
                        "directed signal needs a path, an interface and a member",
                    ));
                }
                if self.header.reply_serial.is_some()
                    || self.header.error_name.is_some()
                    || self.header.expected_responder.is_some()
                    || self.header.stream_reply
                {
                    return Err(Error::protocol(
                        "directed signal has invalid routing fields",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
