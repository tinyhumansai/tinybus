//! The broker: accepts peers, owns the routing table, answers the bus's own
//! interface.
//!
//! # Shape
//!
//! One accept loop, and per peer: a reader task and a writer task with a
//! bounded queue between them. The queue is what makes a slow peer *its own*
//! problem — a service that stops reading fills its queue, and senders to it
//! block or fail, but no other peer's traffic is delayed. A single shared
//! outbound path would let one wedged integration stall the bus, which is
//! precisely the failure mode we are moving integrations out of the kernel to
//! avoid.
//!
//! # What the broker does not do
//!
//! It does not start services, restart them, or know what any of them are for.
//! Activation — "call the wallet, and if nothing owns that name, launch it" —
//! is a real feature and a deliberate omission at this milestone; see
//! `ROADMAP.md`. The broker also does not inspect bodies. It reads the header,
//! routes, and forwards; a body is opaque bytes to it, which is what lets a
//! service pass credentials over the bus without them being logged, cached, or
//! parsed by a process that has no business seeing them.

use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::message::{Message, MessageKind};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::ports::{Listener, Transport};
use crate::router::{MatchRule, NameChange, Router};
use crate::version::PeerManifest;

/// How many messages may queue for one peer before senders wait.
pub const PEER_QUEUE_CAPACITY: usize = 256;

/// The bus itself.
///
/// Cheap to clone; every clone shares one routing table.
#[derive(Clone)]
pub struct Broker {
    router: Arc<Mutex<Router>>,
    id: String,
}

impl Broker {
    /// Build a broker with an empty routing table.
    pub fn new() -> Self {
        Self {
            router: Arc::new(Mutex::new(Router::default())),
            // The id changes per broker *process*, so a peer that reconnects
            // can tell "the bus restarted" (every name is gone, re-register)
            // from "my socket blipped" (state is intact).
            id: format!("tinybus-{}-{}", crate::VERSION, std::process::id()),
        }
    }

    /// This broker's id, as reported by the bus's `GetId`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Serve until the listener stops accepting.
    ///
    /// A per-peer failure never ends this loop; only the listener itself
    /// failing does. One local process must not be able to take the bus down by
    /// misbehaving.
    pub async fn serve(&self, listener: impl Listener) -> Result<()> {
        tracing::info!(listener = %listener.describe(), id = %self.id, "tinybus is listening");
        loop {
            match listener.accept().await {
                Ok(Some(transport)) => {
                    self.attach(transport.into());
                }
                Ok(None) => {
                    tracing::info!("listener closed; broker stopping");
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(error = %e, "accept failed; broker stopping");
                    return Err(e);
                }
            }
        }
    }

    /// Spawn [`Broker::serve`] on the current runtime and return its handle.
    ///
    /// The handle is what an embedding process keeps to shut the bus down; the
    /// test suite uses it to run a whole bus inside one `#[tokio::test]`.
    pub fn spawn(&self, listener: impl Listener) -> tokio::task::JoinHandle<Result<()>> {
        let broker = self.clone();
        tokio::spawn(async move { broker.serve(listener).await })
    }

    /// Register one peer and start its reader and writer tasks.
    pub fn attach(&self, transport: Arc<dyn Transport>) -> BusName {
        let (outbox, inbox) = mpsc::channel(PEER_QUEUE_CAPACITY);
        let (id, unique) = self
            .router
            .lock()
            .expect("router lock is never held across a panic point")
            .attach(outbox);

        tracing::debug!(peer = %unique, transport = %transport.describe(), "peer attached");
        tokio::spawn(writer_task(transport.clone(), inbox));
        tokio::spawn(reader_task(self.clone(), transport, id, unique.clone()));
        unique
    }

    /// Route one inbound message from peer `id`.
    async fn route(&self, from: u64, from_name: &BusName, mut message: Message) -> Result<()> {
        message.validate()?;
        // Overwrite rather than trust: a peer that could set `sender` could
        // impersonate the kernel to every service on the bus.
        message.header.sender = Some(from_name.clone());

        match message.header.kind {
            MessageKind::Signal => {
                let targets = self
                    .router
                    .lock()
                    .expect("router lock")
                    .subscribers(&message, from);
                for target in targets {
                    // A full or closed queue is that subscriber's problem;
                    // signals are best-effort and must not block the sender's
                    // reader task behind a peer that stopped reading.
                    let _ = target.try_send(message.clone());
                }
                Ok(())
            }
            _ => {
                let destination = message
                    .header
                    .destination
                    .clone()
                    .ok_or_else(|| Error::protocol("message has no destination"))?;

                if destination.as_str() == crate::BUS_NAME {
                    return self.handle_bus_call(from, from_name, message).await;
                }

                let target = self
                    .router
                    .lock()
                    .expect("router lock")
                    .resolve(&destination)?;
                target
                    .send(message)
                    .await
                    .map_err(|_| Error::NameHasNoOwner(destination))
            }
        }
    }

    /// Answer a call addressed at the bus's own service.
    async fn handle_bus_call(
        &self,
        from: u64,
        from_name: &BusName,
        message: Message,
    ) -> Result<()> {
        let header = message.header.clone();
        let member = header
            .member
            .clone()
            .ok_or_else(|| Error::protocol("bus call has no member"))?;

        let (result, changes) = self.bus_method(from, from_name, &member, message.body);

        let reply = match result {
            Ok(value) => Message::method_return(&header, value),
            Err(e) => Message::error_reply(&header, &e),
        };

        // Reply before announcing, so a service that requested a name is
        // registered by the time anyone reacts to hearing that it is.
        let outbox = self
            .router
            .lock()
            .expect("router lock")
            .resolve(from_name)?;
        let _ = outbox.send(reply).await;

        for change in changes {
            self.announce_name_change(change).await;
        }
        Ok(())
    }

    /// The bus's own interface. Synchronous: it only touches the routing table,
    /// and holding the lock across an await is exactly what this design avoids.
    fn bus_method(
        &self,
        from: u64,
        from_name: &BusName,
        member: &MemberName,
        body: Value,
    ) -> (Result<Value>, Vec<NameChange>) {
        let mut changes = Vec::new();
        let mut router = self.router.lock().expect("router lock");

        let result = (|| -> Result<Value> {
            match member.as_str() {
                "Hello" => Ok(serde_json::to_value(from_name)?),
                "GetId" => Ok(Value::String(self.id.clone())),
                "Ping" => Ok(Value::Null),
                "RequestName" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    changes.push(router.request_name(from, name)?);
                    Ok(Value::Bool(true))
                }
                "ReleaseName" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    changes.push(router.release_name(from, &name)?);
                    Ok(Value::Bool(true))
                }
                "ListNames" => Ok(serde_json::to_value(router.list_names())?),
                // Version negotiation. The broker stores and serves manifests
                // but never *enforces* them: refusing to route between two
                // peers the broker thinks are incompatible would make the bus
                // the arbiter of every contract on it, and would break the
                // moment a peer's declaration was merely stale. The peers
                // decide; the broker only makes the facts available.
                "Announce" => {
                    let (manifest,): (PeerManifest,) = parse_args(member, body)?;
                    router.set_manifest(from, manifest);
                    Ok(Value::Bool(true))
                }
                "GetManifest" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    Ok(serde_json::to_value(router.manifest_of(&name))?)
                }
                "ListPeers" => Ok(serde_json::to_value(router.peer_records())?),
                "GetNameOwner" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    Ok(serde_json::to_value(router.owner_of(&name))?)
                }
                "AddMatch" => {
                    let (rule,): (String,) = parse_args(member, body)?;
                    router.add_match(from, MatchRule::parse(&rule)?);
                    Ok(Value::Null)
                }
                "RemoveMatch" => {
                    let (rule,): (String,) = parse_args(member, body)?;
                    router.remove_match(from, &MatchRule::parse(&rule)?);
                    Ok(Value::Null)
                }
                other => Err(Error::UnknownMethod {
                    interface: InterfaceName::new(crate::BUS_INTERFACE)
                        .expect("the bus interface constant is valid"),
                    member: MemberName::new(other).unwrap_or_else(|_| member.clone()),
                }),
            }
        })();

        (result, changes)
    }

    /// Broadcast `NameOwnerChanged`.
    ///
    /// This is how the kernel learns an integration died — without it, the only
    /// signal would be a call timing out thirty seconds later, by which point a
    /// user has been staring at a spinner. Subscribers still have to have asked
    /// for it; the broker does not push it at peers that did not.
    async fn announce_name_change(&self, change: NameChange) {
        let signal = Message {
            header: crate::message::Header {
                kind: MessageKind::Signal,
                serial: 0,
                reply_serial: None,
                sender: Some(
                    BusName::new(crate::BUS_NAME).expect("the bus name constant is valid"),
                ),
                destination: None,
                path: Some(
                    ObjectPath::new(crate::BUS_PATH).expect("the bus path constant is valid"),
                ),
                interface: Some(
                    InterfaceName::new(crate::BUS_INTERFACE)
                        .expect("the bus interface constant is valid"),
                ),
                member: Some(
                    MemberName::new("NameOwnerChanged").expect("literal is a valid member"),
                ),
                error_name: None,
            },
            body: serde_json::json!([change.name, change.old_owner, change.new_owner]),
        };

        let targets = self
            .router
            .lock()
            .expect("router lock")
            .broadcast_targets(&signal);
        for target in targets {
            let _ = target.try_send(signal.clone());
        }
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

/// Deserialize a positional argument array, naming the member on failure.
fn parse_args<T: serde::de::DeserializeOwned>(member: &MemberName, body: Value) -> Result<T> {
    serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))
}

/// Drain one peer's queue onto its transport.
async fn writer_task(transport: Arc<dyn Transport>, mut inbox: mpsc::Receiver<Message>) {
    while let Some(message) = inbox.recv().await {
        if let Err(e) = transport.send(message).await {
            tracing::debug!(error = %e, "peer write failed; dropping the peer");
            break;
        }
    }
}

/// Read one peer's messages until it disconnects, then release its names.
async fn reader_task(broker: Broker, transport: Arc<dyn Transport>, id: u64, name: BusName) {
    loop {
        match transport.recv().await {
            Ok(Some(message)) => {
                let header = message.header.clone();
                if let Err(e) = broker.route(id, &name, message).await {
                    tracing::debug!(peer = %name, error = %e, "routing failed");
                    // A call that cannot be routed must produce an error reply,
                    // or the caller waits out its full timeout for a message
                    // that was never going anywhere.
                    if header.kind == MessageKind::MethodCall {
                        let mut reply = Message::error_reply(&header, &e);
                        reply.header.sender =
                            Some(BusName::new(crate::BUS_NAME).expect("bus name is valid"));
                        let _ = transport.send(reply).await;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(peer = %name, error = %e, "peer read failed");
                break;
            }
        }
    }

    tracing::debug!(peer = %name, "peer detached");
    let changes = broker.router.lock().expect("router lock").detach(id);
    for change in changes {
        broker.announce_name_change(change).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::service::Interface;
    use crate::transport::memory::MemoryBus;
    use async_trait::async_trait;
    use std::time::Duration;

    struct Voice;

    #[async_trait]
    impl Interface for Voice {
        fn name(&self) -> InterfaceName {
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Transcribe").unwrap()]
        }

        async fn call(&self, _member: &MemberName, args: Value) -> Result<Value> {
            let (path,): (String,) = serde_json::from_value(args)?;
            Ok(Value::String(format!("transcript of {path}")))
        }
    }

    const VOICE_NAME: &str = "ai.tinyhumans.openhuman.Voice";
    const VOICE_PATH: &str = "/ai/tinyhumans/openhuman/Voice";

    /// A broker, a registered service, and a client — all in one process.
    async fn bus() -> (MemoryBus, Connection, Connection) {
        let bus = MemoryBus::new();
        Broker::new().spawn(bus.clone());

        let service = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        service
            .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
            .await
            .unwrap();
        service.request_name(VOICE_NAME).await.unwrap();

        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        (bus, service, client)
    }

    #[tokio::test]
    async fn a_client_calls_a_service_by_its_well_known_name() {
        let (_bus, _service, client) = bus().await;
        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
        let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await.unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }

    #[tokio::test]
    async fn hello_assigns_distinct_unique_names() {
        let (_bus, service, client) = bus().await;
        let a = service.unique_name().unwrap();
        let b = client.unique_name().unwrap();
        assert!(a.is_unique() && b.is_unique());
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn calling_an_integration_that_is_not_running_fails_fast_and_names_it() {
        let (_bus, _service, client) = bus().await;
        let absent = client
            .proxy(
                "ai.tinyhumans.openhuman.Wallet",
                "/ai/tinyhumans/openhuman/Wallet",
                "ai.tinyhumans.openhuman.Wallet",
            )
            .unwrap()
            .with_timeout(Duration::from_secs(5));

        assert!(!absent.is_available().await.unwrap());
        let err = absent.call::<Value>("Sign", ("0xdead",)).await.unwrap_err();
        // Fast, and specific: not a timeout, and it names the missing service.
        assert!(err.to_string().contains("Wallet"), "{err}");
        assert!(!matches!(err, Error::Timeout { .. }), "{err}");
    }

    #[tokio::test]
    async fn a_second_claimant_for_a_name_is_refused() {
        let (bus, _service, _client) = bus().await;
        let impostor = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let err = impostor.request_name(VOICE_NAME).await.unwrap_err();
        assert!(err.to_string().contains("already owned"), "{err}");
    }

    #[tokio::test]
    async fn the_bus_reserves_its_own_name() {
        let (bus, _service, _client) = bus().await;
        let peer = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        assert!(peer.request_name(crate::BUS_NAME).await.is_err());
    }

    #[tokio::test]
    async fn list_names_shows_the_well_known_name_and_the_unique_ones() {
        let (_bus, _service, client) = bus().await;
        let names = client.list_names().await.unwrap();
        assert!(names.iter().any(|n| n.as_str() == VOICE_NAME));
        assert!(names.iter().filter(|n| n.is_unique()).count() >= 2);
    }

    #[tokio::test]
    async fn a_signal_reaches_a_subscriber_and_skips_a_non_subscriber() {
        let (bus, service, client) = bus().await;
        let mut subscribed = client
            .add_match(
                MatchRule::new()
                    .signals()
                    .interface(InterfaceName::new(VOICE_NAME).unwrap()),
            )
            .await
            .unwrap();
        let bystander = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let mut ignored = bystander.signals();

        service
            .emit(
                ObjectPath::new(VOICE_PATH).unwrap(),
                InterfaceName::new(VOICE_NAME).unwrap(),
                MemberName::new("TranscriptReady").unwrap(),
                ("clip-1",),
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), subscribed.recv())
            .await
            .expect("the subscriber is woken")
            .unwrap();
        assert_eq!(received.header.member.unwrap().as_str(), "TranscriptReady");
        // The bystander added no match, so the broker never woke it.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ignored.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_sender_is_stamped_by_the_broker_and_cannot_be_forged() {
        let (_bus, service, client) = bus().await;
        let mut signals = client.add_match(MatchRule::new().signals()).await.unwrap();

        // Claim to be the bus itself.
        let mut forged = Message::signal(
            ObjectPath::new(VOICE_PATH).unwrap(),
            InterfaceName::new(VOICE_NAME).unwrap(),
            MemberName::new("TranscriptReady").unwrap(),
            Value::Null,
        );
        forged.header.sender = Some(BusName::new(crate::BUS_NAME).unwrap());
        service.send(forged).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .expect("delivered")
            .unwrap();
        assert_eq!(received.header.sender, service.unique_name());
    }

    #[tokio::test]
    async fn a_service_dying_releases_its_name_and_announces_it() {
        let (bus, service, client) = bus().await;
        let mut signals = client
            .add_match(
                MatchRule::new()
                    .signals()
                    .member(MemberName::new("NameOwnerChanged").unwrap()),
            )
            .await
            .unwrap();

        drop(service);

        let announcement = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .expect("the kernel is told")
            .unwrap();
        let (name, _old, new): (BusName, Option<BusName>, Option<BusName>) =
            serde_json::from_value(announcement.body).unwrap();
        assert_eq!(name.as_str(), VOICE_NAME);
        assert!(new.is_none(), "the name has no owner now");

        // And the name is genuinely free again — a restarted service can claim it.
        let restarted = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        restarted.request_name(VOICE_NAME).await.unwrap();
    }

    #[tokio::test]
    async fn an_unknown_bus_method_is_an_error_reply_rather_than_a_hang() {
        let (_bus, _service, client) = bus().await;
        let bus_proxy = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        let err = bus_proxy.call::<Value>("Enumerate", ()).await.unwrap_err();
        assert_eq!(err.wire_name(), Error::UNKNOWN_METHOD);
    }

    #[tokio::test]
    async fn a_peer_announces_and_another_reads_the_manifest_back() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        let manifest = PeerManifest::new("voice-service")
            .version(Version::new(0, 4, 2))
            .provides(InterfaceVersion::provided(
                InterfaceName::new(VOICE_NAME).unwrap(),
                Version::new(2, 3, 0),
            ));
        service.announce(&manifest).await.unwrap();

        // Readable by well-known name, which is what a caller actually holds.
        let seen = client
            .manifest_of(VOICE_NAME)
            .await
            .unwrap()
            .expect("announced");
        assert_eq!(seen, manifest);

        let peers = client.peers().await.unwrap();
        assert_eq!(peers.len(), 1, "only the peer that announced");
        assert_eq!(peers[0].names, vec![BusName::new(VOICE_NAME).unwrap()]);
    }

    #[tokio::test]
    async fn require_passes_on_a_compatible_peer_and_names_both_versions_otherwise() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        let interface = InterfaceName::new(VOICE_NAME).unwrap();
        service
            .announce(
                &PeerManifest::new("voice-service").provides(InterfaceVersion::provided(
                    interface.clone(),
                    Version::new(2, 3, 0),
                )),
            )
            .await
            .unwrap();

        // A caller written against 2.1 is served by a 2.3 provider.
        let ok = PeerManifest::new("openhuman").consumes(InterfaceVersion::consumed(
            interface.clone(),
            Version::new(2, 1, 0),
        ));
        client.require(VOICE_NAME, VOICE_NAME, &ok).await.unwrap();

        // A caller that needs 3.x is not, and the error says so with numbers.
        let stale = PeerManifest::new("openhuman")
            .consumes(InterfaceVersion::consumed(interface, Version::new(3, 0, 0)));
        let err = client
            .require(VOICE_NAME, VOICE_NAME, &stale)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IncompatibleVersion { .. }), "{err}");
        assert!(err.to_string().contains("2.3.0"), "{err}");
        assert!(err.to_string().contains("3.0.0"), "{err}");
    }

    #[tokio::test]
    async fn a_peer_that_never_announced_is_still_callable() {
        // Manifests roll out service by service; a peer without one must not
        // be locked off the bus by peers that have adopted them.
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, _service, client) = bus().await;
        let local = PeerManifest::new("openhuman").consumes(InterfaceVersion::consumed(
            InterfaceName::new(VOICE_NAME).unwrap(),
            Version::new(9, 0, 0),
        ));
        client
            .require(VOICE_NAME, VOICE_NAME, &local)
            .await
            .unwrap();

        let transcript: String = client
            .proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME)
            .unwrap()
            .call("Transcribe", ("/tmp/clip.wav",))
            .await
            .unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }

    #[tokio::test]
    async fn a_dead_peers_manifest_goes_with_it() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        service
            .announce(
                &PeerManifest::new("voice-service").provides(InterfaceVersion::provided(
                    InterfaceName::new(VOICE_NAME).unwrap(),
                    Version::new(2, 3, 0),
                )),
            )
            .await
            .unwrap();
        assert!(client.manifest_of(VOICE_NAME).await.unwrap().is_some());

        drop(service);
        // Wait for the detach to land, then the name — and its manifest — are gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while client.manifest_of(VOICE_NAME).await.unwrap().is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "manifest outlived its peer"
            );
            tokio::task::yield_now().await;
        }
        assert!(client.peers().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_bus_answers_ping_and_reports_a_stable_id() {
        let (_bus, _service, client) = bus().await;
        let bus_proxy = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap();
        bus_proxy.call::<Value>("Ping", ()).await.unwrap();
        let first: String = bus_proxy.call("GetId", ()).await.unwrap();
        let second: String = bus_proxy.call("GetId", ()).await.unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with("tinybus-"), "{first}");
    }

    #[tokio::test]
    async fn one_wedged_peer_does_not_stall_another_peers_traffic() {
        // The load-bearing property of the whole design: a service that stops
        // reading is isolated behind its own bounded queue.
        let (bus, _service, client) = bus().await;

        // A peer that subscribes to everything and then never reads again —
        // an integration blocked inside a third-party library, in other words.
        // Driven at the transport level because a `Connection` would read the
        // replies, which is the thing this peer is refusing to do.
        let sulker = bus.connect().await.unwrap();
        let mut subscribe = Message::method_call(
            BusName::new(crate::BUS_NAME).unwrap(),
            ObjectPath::new(crate::BUS_PATH).unwrap(),
            InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
            MemberName::new("AddMatch").unwrap(),
            serde_json::json!(["type=signal"]),
        );
        subscribe.header.serial = 1;
        sulker.send(subscribe).await.unwrap();

        for _ in 0..(PEER_QUEUE_CAPACITY * 2) {
            client
                .emit(
                    ObjectPath::new(VOICE_PATH).unwrap(),
                    InterfaceName::new(VOICE_NAME).unwrap(),
                    MemberName::new("Noise").unwrap(),
                    (),
                )
                .await
                .unwrap();
        }

        let voice = client
            .proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME)
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await.unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }
}
