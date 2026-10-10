use super::*;
use crate::connection::Connection;
use crate::ports::Listener;
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

struct ClosedListener;

#[async_trait]
impl Listener for ClosedListener {
    async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
        Ok(None)
    }

    fn describe(&self) -> String {
        "closed-test-listener".into()
    }
}

struct FailingListener;

#[async_trait]
impl Listener for FailingListener {
    async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
        Err(Error::transport("accept failed"))
    }
}

#[tokio::test]
async fn serving_stops_cleanly_on_listener_shutdown_and_reports_listener_errors() {
    let broker = Broker::default();
    assert!(broker.id().starts_with("tinybus-"));
    broker.serve(ClosedListener).await.unwrap();
    assert!(broker.serve(FailingListener).await.is_err());
}

#[tokio::test]
async fn malformed_bus_arguments_are_replied_to_without_hanging() {
    let (_bus, _service, client) = bus().await;
    let invalid = Message::method_call(
        BusName::new(crate::BUS_NAME).unwrap(),
        ObjectPath::new(crate::BUS_PATH).unwrap(),
        InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
        MemberName::new("AddMatch").unwrap(),
        serde_json::json!([42]),
    );
    let error = client
        .call_raw(invalid, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.BadArguments"
    );
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

/// A bus with a service that the host has attested, as a module load would.
///
/// `attest_module` is the same call the module host makes after hashing an
/// artifact against `modules.toml`; driving it directly keeps the test on
/// the in-memory transport instead of requiring a built `cdylib` on disk.
#[cfg(feature = "modules")]
async fn attested_bus() -> (MemoryBus, Broker, Connection, Connection) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus.clone());

    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service
        .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
        .await
        .unwrap();
    service.request_name(VOICE_NAME).await.unwrap();
    broker.attest_module(
        &service.unique_name().unwrap(),
        crate::attest::Attestation {
            name: BusName::new(VOICE_NAME).unwrap(),
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        },
    );

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (bus, broker, service, client)
}

#[tokio::test]
async fn a_confidential_call_to_an_unattested_recipient_is_refused() {
    // The default bus has no trust store, so nothing is attested — and the
    // service is reachable by an ordinary call, which is what makes the
    // refusal meaningful rather than incidental.
    let (_bus, _service, client) = bus().await;
    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
    assert!(
        voice
            .call::<String>("Transcribe", ("/tmp/a.wav",))
            .await
            .is_ok()
    );

    let error = voice
        .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
    assert_eq!(voice.attestation().await.unwrap(), None);
}

#[tokio::test]
async fn a_confidential_call_to_an_unattested_unique_peer_is_refused() {
    // An ordinary connection has no admitted artifact, even at its unique name.
    let (_bus, service, client) = bus().await;
    let unique = service.unique_name().unwrap();
    let proxy = client
        .proxy(unique.as_str(), VOICE_PATH, VOICE_NAME)
        .unwrap();
    let error = proxy
        .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
}

#[tokio::test]
async fn a_confidential_call_to_the_bus_itself_is_refused_before_its_body_is_parsed() {
    // The bus's own service is never a loaded, hash-verified module, so it
    // can never be an attested recipient. Before the fix this dispatch
    // reached `handle_bus_call` -> `bus_method` -> `parse_args`, which
    // deserializes the body — breaking "the broker never parses a body"
    // for exactly the messages that must never be parsed. An ordinary
    // (non-confidential) bus call must keep working.
    let (_bus, _service, client) = bus().await;
    let bus_proxy = client
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
        .unwrap();
    let error = bus_proxy
        .call_confidential::<String>("GetId", ())
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
    let id: String = bus_proxy.call("GetId", ()).await.unwrap();
    assert!(id.starts_with("tinybus-"), "{id}");

    #[cfg(feature = "modules")]
    {
        // Module configuration is deliberately the one confidential
        // broker-control payload. Reaching the module-host error proves
        // dispatch accepted it instead of failing the attestation gate.
        let error = bus_proxy
            .call_sensitive::<serde_json::Value>(
                "ReinitializeModule",
                ("missing", serde_json::json!({ "secret": "redacted" })),
            )
            .await
            .unwrap_err();
        assert_ne!(error.wire_name(), Error::NOT_ATTESTED);
        assert!(!error.to_string().contains("redacted"));
    }
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn a_confidential_call_reaches_a_module_the_host_verified() {
    let (_bus, _broker, _service, client) = attested_bus().await;
    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();

    let attestation = voice.attestation().await.unwrap().expect("attested");
    assert_eq!(attestation.name.as_str(), VOICE_NAME);

    let transcript: String = voice
        .call_confidential("Transcribe", ("/tmp/secret.wav",))
        .await
        .unwrap();
    assert_eq!(transcript, "transcript of /tmp/secret.wav");
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn a_name_handed_on_to_another_peer_does_not_hand_on_its_attestation() {
    // The property under test is whether trust is attached to the *name* or
    // to the *peer*. If it were the name, any process that grabbed it after
    // the real module released it would inherit the right to be handed
    // secrets without a single byte having been hashed.
    let (bus, _broker, service, client) = attested_bus().await;
    let fixed = client
        .proxy(
            service.unique_name().unwrap().as_str(),
            VOICE_PATH,
            VOICE_NAME,
        )
        .unwrap();
    let admitted = fixed.attestation().await.unwrap();
    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
    assert!(voice.attestation().await.unwrap().is_some());

    service.release_name(VOICE_NAME).await.unwrap();
    let impostor = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    impostor
        .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
        .await
        .unwrap();
    impostor.request_name(VOICE_NAME).await.unwrap();

    assert_eq!(fixed.attestation().await.unwrap(), admitted);
    assert!(
        fixed
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .is_ok()
    );
    let replacement = client
        .proxy(
            impostor.unique_name().unwrap().as_str(),
            VOICE_PATH,
            VOICE_NAME,
        )
        .unwrap();
    assert_eq!(replacement.attestation().await.unwrap(), None);
    assert_eq!(
        replacement
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err()
            .wire_name(),
        Error::NOT_ATTESTED
    );
    service.request_name("org.example.Extra").await.unwrap();
    let extra = client
        .proxy("org.example.Extra", VOICE_PATH, VOICE_NAME)
        .unwrap();
    assert_eq!(extra.attestation().await.unwrap(), None);
    assert_eq!(
        extra
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err()
            .wire_name(),
        Error::NOT_ATTESTED
    );

    // The impostor owns the name and answers ordinary calls...
    assert!(
        voice
            .call::<String>("Transcribe", ("/tmp/a.wav",))
            .await
            .is_ok()
    );
    // ...and is refused the secret, because nothing verified its artifact.
    assert_eq!(voice.attestation().await.unwrap(), None);
    let error = voice
        .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::NOT_ATTESTED);

    let mut changes = client
        .add_match(
            MatchRule::new()
                .signals()
                .member(MemberName::new("NameOwnerChanged").unwrap()),
        )
        .await
        .unwrap();
    service.close().await.unwrap();
    let detached = tokio::time::timeout(Duration::from_secs(5), changes.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detached.body[0], "org.example.Extra");
    assert_eq!(fixed.attestation().await.unwrap(), None);
    assert_eq!(
        fixed
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err()
            .wire_name(),
        "ai.tinyhumans.tinybus.Error.NameHasNoOwner"
    );
}

#[tokio::test]
async fn a_confidential_body_is_never_fanned_out_to_a_monitor() {
    let (_bus, _service, client) = bus().await;
    let watcher = Connection::connect(_bus.connect().await.unwrap())
        .await
        .unwrap();
    // The broadest possible subscription: if anything could see a secret,
    // this would.
    let mut seen = watcher.add_match(MatchRule::new()).await.unwrap();

    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
    let _ = voice
        .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
        .await;

    // Nothing arrives at all, rather than something arriving redacted.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), seen.recv())
            .await
            .is_err()
    );
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn a_confidential_call_carrying_a_stream_handle_is_refused_before_it_is_sent() {
    // The footgun this closes: a stream's bytes travel as their own
    // unflagged `Write` calls, so a handle inside a confidential body would
    // attest the recipient of the *handle* while the payload it stands for
    // went out unattested — and the caller would have every reason to
    // believe otherwise. Refused in the sender's own process, because the
    // broker would have to read a confidential body to see it.
    let (_bus, _broker, _service, client) = attested_bus().await;
    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();

    // The recipient really is attested, so the refusal below is about the
    // stream handle and nothing else.
    assert!(voice.attestation().await.unwrap().is_some());

    let handle = crate::stream::StreamRef {
        id: "s1".to_string(),
        content_type: None,
        len: Some(4096),
    };
    let error = voice
        .call_confidential::<Value>("Transcribe", (handle,))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("stream handle"), "{error}");

    // The same handle in a *non*-confidential call is not intercepted: this
    // guards a confidentiality claim, it does not ban streams. The call
    // still fails, because this fixture's `Transcribe` takes a string — but
    // it fails at the service, having been sent, rather than being refused
    // here. Asserting on which error distinguishes the two.
    let sent = voice
        .call::<Value>(
            "Transcribe",
            (crate::stream::StreamRef {
                id: "s1".to_string(),
                content_type: None,
                len: Some(4096),
            },),
        )
        .await
        .unwrap_err();
    // Only that the guard did not fire — which error the fixture's own
    // signature mismatch produces downstream is not this test's business.
    assert!(!sent.to_string().contains("stream handle"), "{sent}");
}

#[tokio::test]
async fn get_attestation_answers_for_a_name_nobody_owns() {
    let (_bus, _service, client) = bus().await;
    let missing = BusName::new("ai.tinyhumans.openhuman.Absent").unwrap();
    assert_eq!(client.attestation(missing).await.unwrap(), None);
}

#[tokio::test]
async fn the_stream_interface_gets_no_exemption_from_attestation() {
    // Bulk payloads travel as `Stream.Write` calls rather than in a body,
    // which makes the stream interface the one place a second delivery path
    // could have grown. It did not: a chunk is an ordinary method call and
    // `route` reaches it through the same check as everything else. Pinned
    // as a test because the cost of the stream path ever being special-cased
    // is every secret on the bus, and nothing else would notice.
    let (_bus, service, client) = bus().await;
    let stream = client
        .proxy(
            VOICE_NAME,
            crate::stream::STREAM_PATH,
            crate::stream::STREAM_INTERFACE,
        )
        .unwrap();

    // The peer is reachable on the stream interface by an ordinary call —
    // it answers `UnknownStream`, not `NotAttested` — so the refusal below
    // is the attestation check firing and not the name failing to resolve.
    let ordinary = stream
        .call::<Value>("Abort", ("no-such-stream",))
        .await
        .unwrap_err();
    assert_eq!(
        ordinary.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownStream"
    );

    let refused = stream
        .call_confidential::<Value>("Abort", ("no-such-stream",))
        .await
        .unwrap_err();
    assert_eq!(refused.wire_name(), Error::NOT_ATTESTED);
    drop(service);
}

#[tokio::test]
async fn a_sensitive_call_is_refused_for_every_member_but_module_configuration() {
    let (_bus, _service, client) = bus().await;
    let bus_proxy = client
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
        .unwrap();
    let error = bus_proxy
        .call_sensitive::<Value>("GetId", ())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("reserved for module configuration"),
        "{error}"
    );
}

#[tokio::test]
async fn a_sensitive_call_may_only_address_the_bus_module_host() {
    let (_bus, _service, client) = bus().await;
    let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
    let error = voice
        .call_sensitive::<Value>("Transcribe", ("/tmp/clip.wav",))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must address the bus module host"),
        "{error}"
    );
}

#[tokio::test]
async fn remove_match_validates_its_rule_and_accepts_one_that_was_added() {
    let (_bus, _service, client) = bus().await;
    let bus_proxy = client
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
        .unwrap();
    let rule = "type=signal".to_string();
    bus_proxy
        .call::<Value>("AddMatch", (rule.clone(),))
        .await
        .unwrap();
    bus_proxy
        .call::<Value>("RemoveMatch", (rule,))
        .await
        .unwrap();
    let error = bus_proxy
        .call::<Value>("RemoveMatch", (42,))
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.BadArguments"
    );
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn module_control_calls_validate_their_arguments_and_answer_for_unknown_modules() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _host = crate::module::host::ModuleHost::new(broker.clone());
    broker.spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bus_proxy = client
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
        .unwrap();
    let bad_arguments = "ai.tinyhumans.tinybus.Error.BadArguments";

    let listed: Value = bus_proxy.call("ListModules", ()).await.unwrap();
    assert_eq!(listed, serde_json::json!([]));
    let absent: Value = bus_proxy.call("GetModule", ("nope",)).await.unwrap();
    assert_eq!(absent, Value::Null);
    let no_manifest: Value = bus_proxy
        .call("GetModuleManifest", ("nope",))
        .await
        .unwrap();
    assert_eq!(no_manifest, Value::Null);

    for (member, body) in [
        ("GetModule", serde_json::json!([42])),
        ("GetModuleManifest", serde_json::json!([42])),
        ("StopModule", serde_json::json!([42])),
        ("ReinitializeModule", serde_json::json!([42])),
        ("LoadModule", serde_json::json!({})),
        ("LoadModule", serde_json::json!([])),
        ("LoadModule", serde_json::json!(["a", {}, "extra"])),
        ("RescanModules", serde_json::json!({})),
        ("RescanModules", serde_json::json!([[], false, "extra"])),
    ] {
        let message = Message::method_call(
            BusName::new(crate::BUS_NAME).unwrap(),
            ObjectPath::new(crate::BUS_PATH).unwrap(),
            InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
            MemberName::new(member).unwrap(),
            body,
        );
        let error = client
            .call_raw(message, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(error.wire_name(), bad_arguments, "{member}: {error}");
    }
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn a_confidential_call_reaches_the_fixed_unique_admitted_peer() {
    let (_bus, _broker, service, client) = attested_bus().await;
    let proxy = client
        .proxy(
            service.unique_name().unwrap().as_str(),
            VOICE_PATH,
            VOICE_NAME,
        )
        .unwrap();
    let transcript: String = proxy
        .call_confidential("Transcribe", ("/tmp/secret.wav",))
        .await
        .unwrap();
    assert_eq!(transcript, "transcript of /tmp/secret.wav");
    assert_eq!(
        proxy.attestation().await.unwrap(),
        client
            .attestation(BusName::new(VOICE_NAME).unwrap())
            .await
            .unwrap()
    );
}
