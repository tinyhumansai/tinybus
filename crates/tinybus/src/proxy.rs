//! The client side: a typed handle to one interface on one remote object.
//!
//! This is the entire surface the OpenHuman kernel is meant to depend on. A
//! kernel that used to link `whisper-rs`, `cpal` and a model runtime to
//! transcribe audio now does this:
//!
//! ```no_run
//! # async fn example(connection: tinybus::Connection) -> tinybus::Result<()> {
//! let voice = connection.proxy(
//!     "ai.tinyhumans.openhuman.Voice",
//!     "/ai/tinyhumans/openhuman/Voice",
//!     "ai.tinyhumans.openhuman.Voice",
//! )?;
//! let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await?;
//! # let _ = transcript;
//! # Ok(())
//! # }
//! ```
//!
//! …and links `serde`. The type parameters do the work the removed dependency
//! used to: `R` is checked against what actually came back, so a service that
//! changes its return shape fails at the caller with a deserialize error naming
//! the mismatch, rather than silently producing a default.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::connection::{Connection, DEFAULT_TIMEOUT};
use crate::error::Result;
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::router::MatchRule;

/// A handle to `destination` + `path` + `interface`, with a per-proxy timeout.
#[derive(Clone)]
pub struct Proxy {
    connection: Connection,
    destination: BusName,
    path: ObjectPath,
    interface: InterfaceName,
    timeout: Duration,
}

impl Proxy {
    /// Build a proxy. Prefer [`Connection::proxy`], which parses the names for
    /// you.
    pub fn new(
        connection: Connection,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
    ) -> Result<Self> {
        Ok(Self {
            connection,
            destination,
            path,
            interface,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Override the deadline for every call made through this proxy.
    ///
    /// Per-proxy rather than per-call because the natural unit is the
    /// *integration*: a wallet signature and a PDF render have different
    /// reasonable waits, and every call to one of them shares its wait.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The name this proxy addresses.
    pub fn destination(&self) -> &BusName {
        &self.destination
    }

    /// The object this proxy addresses.
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }

    /// The contract this proxy speaks.
    pub fn interface(&self) -> &InterfaceName {
        &self.interface
    }

    /// Call `member` with positional `args` and deserialize the reply.
    ///
    /// Pass a tuple for several arguments (`("a", 1)`), a bare value for one,
    /// and `()` for none.
    pub async fn call<R: DeserializeOwned>(&self, member: &str, args: impl Serialize) -> Result<R> {
        let member = MemberName::new(member)?;
        self.connection
            .call_with_timeout(
                self.destination.clone(),
                self.path.clone(),
                self.interface.clone(),
                member,
                args,
                self.timeout,
            )
            .await
    }

    /// Call `member` and receive a potentially bulk result through a bounded
    /// reply stream.
    ///
    /// Ordinary replies from older peers remain compatible. Use this for
    /// variable-size results whose encoded JSON may exceed one frame.
    pub async fn call_streaming<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R> {
        self.connection
            .call_streaming_with_timeout(
                self.destination.clone(),
                self.path.clone(),
                self.interface.clone(),
                MemberName::new(member)?,
                args,
                self.timeout,
            )
            .await
    }

    /// Call `member` with a body the bus must not show to anyone else.
    ///
    /// The call fails with [`crate::Error::NotAttested`] unless the broker has
    /// itself verified the destination's artifact against the operator's trust
    /// store, or this exact call matches an active SDK-created local host callback
    /// capability for this verified loaded module sender. That refusal is the feature: handing a private key to whoever
    /// happened to claim the name first is the outcome this exists to prevent,
    /// and it is better to fail a deploy than to succeed at that.
    ///
    /// For artifact-backed modules, use [`Proxy::attestation`] first to
    /// distinguish "not installed" from "not trusted" before assembling a secret.
    /// For scoped host callbacks, use the SDK-created binding and its scope
    /// checks: callback authority comes from that local ModuleHost capability,
    /// which deliberately has no artifact attestation.
    pub async fn call_confidential<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R> {
        let message = crate::message::Message::confidential_call(
            self.destination.clone(),
            self.path.clone(),
            self.interface.clone(),
            MemberName::new(member)?,
            crate::connection::to_body(&args)?,
        );
        let reply = self.connection.call_raw(message, self.timeout).await?;
        Ok(serde_json::from_value(reply)?)
    }

    /// Call a broker module-control method with a body redacted from every
    /// observation surface.
    ///
    /// The broker accepts this marker only for module load and
    /// reinitialization. It is distinct from [`Proxy::call_confidential`],
    /// whose destination must be an already attested module.
    pub async fn call_sensitive<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R> {
        let message = crate::message::Message::sensitive_control_call(
            self.destination.clone(),
            self.path.clone(),
            self.interface.clone(),
            MemberName::new(member)?,
            crate::connection::to_body(&args)?,
        );
        let reply = self.connection.call_raw(message, self.timeout).await?;
        Ok(serde_json::from_value(reply)?)
    }

    /// What the broker has verified about this proxy's destination, if
    /// anything.
    ///
    /// `None` means no secret may be sent here — either nothing owns the name
    /// or the operator never allowlisted an artifact for it.
    pub async fn attestation(&self) -> Result<Option<crate::attest::Attestation>> {
        self.connection.attestation(self.destination.clone()).await
    }

    /// Whether a peer currently owns this proxy's destination.
    ///
    /// Worth checking before a first call in a startup path: the difference
    /// between "the integration is not installed" and "the call failed" is
    /// something a user can act on, and only this distinguishes them.
    pub async fn is_available(&self) -> Result<bool> {
        Ok(self
            .connection
            .name_owner(self.destination.as_str())
            .await?
            .is_some())
    }

    /// Subscribe to this object's signals on this interface.
    ///
    /// Narrower than [`Connection::add_match`] on purpose: a proxy knows its
    /// own address, and a subscription built from it cannot accidentally match
    /// another account's traffic.
    pub async fn receive_signal(
        &self,
        member: &str,
    ) -> Result<tokio::sync::broadcast::Receiver<crate::message::Message>> {
        let rule = MatchRule::new()
            .signals()
            .interface(self.interface.clone())
            .member(MemberName::new(member)?)
            .path_namespace(self.path.clone());
        self.connection.add_match(rule).await
    }

    /// Subscribe only to this object's recipient-directed signals.
    ///
    /// Directed signals are delivered to the unique recipient selected by a
    /// live SDK callback binding; they are never fanned out to ordinary signal
    /// subscribers.
    pub async fn receive_directed_signal(
        &self,
        member: &str,
    ) -> Result<tokio::sync::broadcast::Receiver<crate::message::Message>> {
        let rule = MatchRule::new()
            .directed_signals()
            .interface(self.interface.clone())
            .member(MemberName::new(member)?)
            .path_namespace(self.path.clone());
        self.connection.add_match(rule).await
    }

    /// The connection underneath, for the rare call that needs the raw surface.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written so the connection — which holds the transport, which may
        // name a socket path — never lands in a log line.
        f.debug_struct("Proxy")
            .field("destination", &self.destination)
            .field("path", &self.path)
            .field("interface", &self.interface)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[cfg(test)]
#[path = "proxy_tests.rs"]
mod tests;
