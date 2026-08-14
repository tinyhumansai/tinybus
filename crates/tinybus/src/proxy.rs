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

    /// Call `member` with a body the bus must not show to anyone else.
    ///
    /// The call fails with [`crate::Error::NotAttested`] unless the broker has
    /// itself verified the destination's artifact against the operator's trust
    /// store. That refusal is the feature: handing a private key to whoever
    /// happened to claim the name first is the outcome this exists to prevent,
    /// and it is better to fail a deploy than to succeed at that.
    ///
    /// Use [`Proxy::attestation`] first if the caller wants to distinguish
    /// "not installed" from "not trusted" before it assembles the secret.
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

    /// The connection underneath, for the rare call that needs the raw surface.
    pub fn connection(&self) -> &Connection {
        &self.connection
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
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::broker::Broker;
    use crate::message::Message;
    use crate::service::Interface;
    use crate::transport::memory::MemoryBus;

    const DESTINATION: &str = "ai.tinyhumans.openhuman.Echo";
    const PATH: &str = "/ai/tinyhumans/openhuman/Echo";
    const INTERFACE: &str = "ai.tinyhumans.openhuman.Echo";

    struct Echo;

    #[async_trait]
    impl Interface for Echo {
        fn name(&self) -> InterfaceName {
            InterfaceName::new(INTERFACE).unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Echo").unwrap()]
        }

        async fn call(&self, _member: &MemberName, args: Value) -> Result<Value> {
            Ok(args)
        }
    }

    async fn peers() -> (Connection, Connection) {
        let bus = MemoryBus::new();
        Broker::new().spawn(bus.clone());
        let service = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        (service, client)
    }

    fn proxy(connection: Connection) -> Proxy {
        Proxy::new(
            connection,
            BusName::new(DESTINATION).unwrap(),
            ObjectPath::new(PATH).unwrap(),
            InterfaceName::new(INTERFACE).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_proxy_calls_its_service_and_exposes_its_address() {
        let (service, client) = peers().await;
        service.request_name(DESTINATION).await.unwrap();
        service
            .serve_at(ObjectPath::new(PATH).unwrap(), Echo)
            .await
            .unwrap();

        let proxy = proxy(client).with_timeout(Duration::from_millis(250));
        assert_eq!(proxy.destination().as_str(), DESTINATION);
        assert_eq!(proxy.path().as_str(), PATH);
        assert_eq!(proxy.interface().as_str(), INTERFACE);
        assert!(proxy.is_available().await.unwrap());
        assert_eq!(
            proxy.call::<Value>("Echo", ("hello",)).await.unwrap(),
            serde_json::json!(["hello"])
        );
        assert!(format!("{proxy:?}").contains(DESTINATION));
        assert!(proxy.connection().unique_name().is_some());
    }

    #[tokio::test]
    async fn a_proxy_reports_a_missing_destination_and_filters_its_signals() {
        let (service, client) = peers().await;
        let proxy = proxy(client);
        assert!(!proxy.is_available().await.unwrap());

        service
            .emit(
                ObjectPath::new(PATH).unwrap(),
                InterfaceName::new(INTERFACE).unwrap(),
                MemberName::new("Changed").unwrap(),
                serde_json::json!([]),
            )
            .await
            .unwrap();
        let mut signals = proxy.receive_signal("Changed").await.unwrap();

        service
            .emit(
                ObjectPath::new("/ai/tinyhumans/openhuman/Other").unwrap(),
                InterfaceName::new(INTERFACE).unwrap(),
                MemberName::new("Changed").unwrap(),
                serde_json::json!(["outside"]),
            )
            .await
            .unwrap();
        service
            .emit(
                ObjectPath::new(PATH).unwrap(),
                InterfaceName::new(INTERFACE).unwrap(),
                MemberName::new("Changed").unwrap(),
                serde_json::json!(["inside"]),
            )
            .await
            .unwrap();

        let message: Message = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .expect("matching signal arrives")
            .expect("connection stays live");
        assert_eq!(message.body, serde_json::json!(["inside"]));
    }
}
