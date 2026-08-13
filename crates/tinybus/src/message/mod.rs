//! The wire format: one [`Message`] type, four kinds, JSON bodies.
//!
//! # Why JSON and not a binary encoding
//!
//! D-Bus has its own marshalling; we deliberately do not. The integrations on
//! this bus are things like "transcribe a file" and "sign a transaction" — the
//! call rate is human-scale, and the body is dwarfed by the work it triggers.
//! What we get in exchange is that `tinybus monitor` is readable, a service can
//! be written in any language in an afternoon, and `serde` derives on the
//! kernel side are the entire client binding. The one place this would be the
//! wrong trade is bulk binary payloads (audio, PDFs), and that is exactly what
//! `ROADMAP.md`'s file-descriptor passing milestone is for; until then large
//! payloads travel as paths, not as base64.
//!
//! # Why the header is flat
//!
//! Routing reads `destination` and nothing else. Keeping it a flat struct of
//! `Option`s rather than an enum per kind means the broker can route a message
//! without knowing what kind it is, which is what lets an unknown future kind
//! pass through an old broker instead of being dropped.

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
    /// A service authorising a call reads this field; if peers could set it,
    /// every authorisation decision on the bus would be forgeable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<BusName>,
    /// Who the message is for. `None` on signals, which are broadcast.
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
    /// This body is a secret: deliver it to the attested destination or to
    /// nobody.
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub confidential: bool,
}

/// `skip_serializing_if` needs a path, and `bool::not` takes `self` by value.
fn is_false(value: &bool) -> bool {
    !*value
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
                destination: Some(destination),
                path: Some(path),
                interface: Some(interface),
                member: Some(member),
                error_name: None,
                confidential: false,
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

    /// Build the successful reply to `call`.
    pub fn method_return(call: &Header, body: Value) -> Self {
        Self {
            header: Header {
                kind: MessageKind::MethodReturn,
                serial: 0,
                reply_serial: Some(call.serial),
                sender: None,
                // Replies are addressed back at the *unique* name the broker
                // stamped, not at whatever well-known name the caller happens
                // to own — the caller may have released it mid-call.
                destination: call.sender.clone(),
                path: None,
                interface: None,
                member: None,
                error_name: None,
                // A reply inherits the call's confidentiality. A secret asked
                // for confidentially is usually answered with another one — a
                // key derivation returns a key — and a reply that quietly lost
                // the flag would be the leak the call avoided, one hop later.
                confidential: call.confidential,
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
                destination: call.sender.clone(),
                path: None,
                interface: None,
                member: None,
                error_name: Some(error.wire_name().to_string()),
                // Not inherited. Errors never carry the value that caused them,
                // so an error reply has no secret to protect — and marking it
                // confidential would make it undeliverable exactly when the
                // recipient failed attestation, swallowing the diagnosis.
                confidential: false,
            },
            body: Value::String(error.wire_message()),
        }
    }

    /// Build a signal. Signals carry no destination: the router fans them out
    /// to every peer whose match rules accept them.
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
                destination: None,
                path: Some(path),
                interface: Some(interface),
                member: Some(member),
                error_name: None,
                // Structurally impossible to set: a signal is a broadcast, and
                // `validate` refuses the combination on ingress.
                confidential: false,
            },
            body,
        }
    }

    /// Reconstruct the [`Error`] an [`MessageKind::Error`] message carries.
    ///
    /// The variant's structure does not survive the wire — only its name and
    /// message do — so this always returns [`Error::MethodFailed`]. Callers
    /// match on [`Error::wire_name`], which does survive.
    pub fn into_error(self) -> Error {
        let name = self
            .header
            .error_name
            .unwrap_or_else(|| Error::FAILED.to_string());
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
        // Checked before the per-kind rules, and checked on ingress rather than
        // at delivery: a confidential signal has no destination, so there is no
        // one recipient to attest and fan-out is the only thing it could mean.
        // Refusing it here means no later stage has to ask whether a broadcast
        // might be a secret.
        if self.header.confidential {
            if self.header.kind == MessageKind::Signal {
                return Err(Error::protocol(
                    "a signal cannot be confidential: it is a broadcast",
                ));
            }
            if self.header.destination.is_none() {
                return Err(Error::protocol(
                    "a confidential message needs a destination",
                ));
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
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> Message {
        Message::method_call(
            BusName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            ObjectPath::new("/ai/tinyhumans/openhuman/Voice").unwrap(),
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            MemberName::new("Transcribe").unwrap(),
            serde_json::json!(["/tmp/clip.wav"]),
        )
    }

    #[test]
    fn a_reply_goes_back_to_the_senders_unique_name() {
        let mut c = call();
        c.header.serial = 9;
        c.header.sender = Some(BusName::new(":1.3").unwrap());
        let reply = Message::method_return(&c.header, serde_json::json!("hello"));
        assert_eq!(reply.header.reply_serial, Some(9));
        assert_eq!(
            reply.header.destination,
            Some(BusName::new(":1.3").unwrap())
        );
    }

    #[test]
    fn an_error_reply_keeps_the_dotted_name_matchable() {
        let c = call();
        let err = Error::UnknownMethod {
            interface: InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            member: MemberName::new("Nope").unwrap(),
        };
        let reply = Message::error_reply(&c.header, &err);
        assert_eq!(
            reply.header.error_name.as_deref(),
            Some(Error::UNKNOWN_METHOD)
        );
        assert_eq!(reply.into_error().wire_name(), Error::UNKNOWN_METHOD);
    }

    #[test]
    fn a_call_without_a_destination_is_refused_on_ingress() {
        let mut c = call();
        c.header.destination = None;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("no destination"), "{err}");
    }

    #[test]
    fn a_signal_needs_no_destination() {
        let sig = Message::signal(
            ObjectPath::new("/ai/tinyhumans/openhuman/Mail").unwrap(),
            InterfaceName::new("ai.tinyhumans.openhuman.Mail").unwrap(),
            MemberName::new("Received").unwrap(),
            serde_json::json!([{ "id": "abc" }]),
        );
        sig.validate().unwrap();
        assert!(sig.header.destination.is_none());
    }

    #[test]
    fn the_header_omits_absent_fields_rather_than_writing_nulls() {
        // Signals outnumber every other message on a busy bus; not writing
        // seven `null`s per signal is most of the framing cost.
        let sig = Message::signal(
            ObjectPath::root(),
            InterfaceName::new("ai.tinyhumans.Test").unwrap(),
            MemberName::new("Tick").unwrap(),
            Value::Null,
        );
        let json = serde_json::to_string(&sig).unwrap();
        assert!(!json.contains("null"), "{json}");
        assert!(!json.contains("destination"), "{json}");
    }

    #[test]
    fn messages_round_trip_through_json() {
        let c = call();
        let bytes = serde_json::to_vec(&c).unwrap();
        assert_eq!(serde_json::from_slice::<Message>(&bytes).unwrap(), c);
    }
}
