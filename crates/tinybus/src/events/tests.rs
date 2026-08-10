//! Behavioural coverage for the typed pub/sub surface.
//!
//! These assert the properties OpenHuman's ~340 call sites are relying on, in
//! the same terms the old in-process bus guaranteed them: a publish reaches
//! subscribers, a domain filter excludes what it says it excludes, a panicking
//! handler does not take its neighbours down, and a dropped handle stops
//! delivery.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::broker::Broker;
use crate::connection::Connection;
use crate::events::{Event, EventBus, EventBusConfig, EventHandler};
use crate::message::Message;
use crate::name::{InterfaceName, MemberName, ObjectPath};
use crate::transport::memory::MemoryBus;

/// A miniature stand-in for OpenHuman's `DomainEvent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum TestEvent {
    AgentTurnCompleted { run: String },
    CronJobTriggered { job: String },
    SystemStartup,
}

impl Event for TestEvent {
    fn domain(&self) -> &str {
        match self {
            Self::AgentTurnCompleted { .. } => "agent",
            Self::CronJobTriggered { .. } => "cron",
            Self::SystemStartup => "system",
        }
    }
}

fn config() -> EventBusConfig {
    EventBusConfig::new(
        "/ai/tinyhumans/openhuman/events",
        "ai.tinyhumans.openhuman.Events",
    )
    .unwrap()
}

/// A capturing handler with an optional domain filter.
struct Capture {
    name: &'static str,
    domains: Option<Vec<&'static str>>,
    seen: Arc<Mutex<Vec<TestEvent>>>,
}

#[async_trait]
impl EventHandler<TestEvent> for Capture {
    fn name(&self) -> &str {
        self.name
    }

    fn domains(&self) -> Option<&[&str]> {
        self.domains.as_deref()
    }

    async fn handle(&self, event: &TestEvent) {
        self.seen.lock().await.push(event.clone());
    }
}

/// One broker, and one connected event bus per caller.
async fn bus() -> (MemoryBus, EventBus<TestEvent>) {
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let connection = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let bus = EventBus::attach(connection, config()).await.unwrap();
    (transport, bus)
}

/// Wait for `seen` to reach `n` entries, or fail the test. Not a sleep: it
/// polls a deadline so a passing run costs microseconds.
async fn wait_for(seen: &Arc<Mutex<Vec<TestEvent>>>, n: usize) -> Vec<TestEvent> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let guard = seen.lock().await;
            if guard.len() >= n {
                return guard.clone();
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "timed out waiting for {n} events; saw {:?}",
                seen.lock().await
            );
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_published_event_reaches_a_subscriber() {
    let (_t, bus) = bus().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _handle = bus.subscribe(Arc::new(Capture {
        name: "test::all",
        domains: None,
        seen: seen.clone(),
    }));

    bus.publish(TestEvent::AgentTurnCompleted {
        run: "run-1".into(),
    });

    let events = wait_for(&seen, 1).await;
    assert_eq!(
        events[0],
        TestEvent::AgentTurnCompleted {
            run: "run-1".into()
        }
    );
}

#[tokio::test]
async fn a_domain_filter_excludes_other_domains() {
    let (_t, bus) = bus().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _handle = bus.subscribe(Arc::new(Capture {
        name: "test::cron_only",
        domains: Some(vec!["cron"]),
        seen: seen.clone(),
    }));

    bus.publish(TestEvent::AgentTurnCompleted { run: "a".into() });
    bus.publish(TestEvent::CronJobTriggered {
        job: "nightly".into(),
    });
    bus.publish(TestEvent::AgentTurnCompleted { run: "b".into() });

    let events = wait_for(&seen, 1).await;
    assert_eq!(events.len(), 1, "only the cron event: {events:?}");
    assert_eq!(
        events[0],
        TestEvent::CronJobTriggered {
            job: "nightly".into()
        }
    );
}

#[tokio::test]
async fn every_subscriber_sees_every_matching_event() {
    let (_t, bus) = bus().await;
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));
    let _a = bus.subscribe(Arc::new(Capture {
        name: "test::a",
        domains: None,
        seen: first.clone(),
    }));
    let _b = bus.subscribe(Arc::new(Capture {
        name: "test::b",
        domains: None,
        seen: second.clone(),
    }));

    bus.publish(TestEvent::SystemStartup);

    assert_eq!(wait_for(&first, 1).await.len(), 1);
    assert_eq!(wait_for(&second, 1).await.len(), 1);
}

#[tokio::test]
async fn a_panicking_handler_does_not_stop_its_neighbour_or_itself() {
    let (_t, bus) = bus().await;

    struct Exploder {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventHandler<TestEvent> for Exploder {
        fn name(&self) -> &str {
            "test::exploder"
        }
        async fn handle(&self, _event: &TestEvent) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("as requested");
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let _boom = bus.subscribe(Arc::new(Exploder {
        calls: calls.clone(),
    }));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _ok = bus.subscribe(Arc::new(Capture {
        name: "test::survivor",
        domains: None,
        seen: seen.clone(),
    }));

    bus.publish(TestEvent::SystemStartup);
    bus.publish(TestEvent::CronJobTriggered { job: "j".into() });

    // The neighbour saw both...
    assert_eq!(wait_for(&seen, 2).await.len(), 2);
    // ...and the panicking handler was called twice, meaning its own loop
    // survived the first panic rather than silently unsubscribing.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::SeqCst) < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "exploder stopped after panicking"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn dropping_the_handle_stops_delivery() {
    let (_t, bus) = bus().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handle = bus.subscribe(Arc::new(Capture {
        name: "test::transient",
        domains: None,
        seen: seen.clone(),
    }));

    bus.publish(TestEvent::SystemStartup);
    wait_for(&seen, 1).await;

    drop(handle);
    // Give the abort a chance to land before publishing again.
    tokio::task::yield_now().await;
    bus.publish(TestEvent::CronJobTriggered {
        job: "after".into(),
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(seen.lock().await.len(), 1, "no delivery after drop");
}

#[tokio::test]
async fn an_event_crosses_a_process_boundary_between_two_peers() {
    // The property the in-process bus could not offer at all: a publisher and a
    // subscriber that share nothing but the broker.
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());

    let publisher = EventBus::<TestEvent>::without_match(
        Connection::connect(transport.connect().await.unwrap())
            .await
            .unwrap(),
        config(),
    );
    let subscriber = EventBus::<TestEvent>::attach(
        Connection::connect(transport.connect().await.unwrap())
            .await
            .unwrap(),
        config(),
    )
    .await
    .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let _handle = subscriber.subscribe(Arc::new(Capture {
        name: "test::remote",
        domains: None,
        seen: seen.clone(),
    }));

    publisher.publish(TestEvent::AgentTurnCompleted {
        run: "remote".into(),
    });

    let events = wait_for(&seen, 1).await;
    assert_eq!(
        events[0],
        TestEvent::AgentTurnCompleted {
            run: "remote".into()
        }
    );
}

#[tokio::test]
async fn a_receiver_yields_decoded_events() {
    let (_t, bus) = bus().await;
    let mut receiver = bus.receiver();

    bus.publish(TestEvent::CronJobTriggered {
        job: "nightly".into(),
    });

    let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("an event arrives")
        .expect("the connection is live");
    assert_eq!(
        event,
        TestEvent::CronJobTriggered {
            job: "nightly".into()
        }
    );
}

#[tokio::test]
async fn a_peer_that_publishes_and_subscribes_sees_the_event_exactly_once() {
    // The invariant the local loopback depends on: the publisher delivers to
    // its own subscribers directly, and the broker deliberately does not echo
    // a signal back to its sender. Break either half and this is 0 or 2.
    let (_t, bus) = bus().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _handle = bus.subscribe(Arc::new(Capture {
        name: "test::self",
        domains: None,
        seen: seen.clone(),
    }));

    bus.publish(TestEvent::SystemStartup);
    wait_for(&seen, 1).await;

    // Give the broker ample opportunity to echo it back before asserting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(seen.lock().await.len(), 1, "delivered exactly once");
}

#[tokio::test]
async fn a_local_subscriber_still_receives_once_the_outbox_has_overflowed() {
    // Local delivery runs before the wire send and does not depend on the
    // broker, so a bus that has stopped draining degrades to in-process-only
    // rather than to silence. Driven with no broker at all: the far end of this
    // transport is never read, so the outbox fills and stays full.
    let (near, _far) = crate::transport::memory::MemoryTransport::pair();
    let connection = Connection::attach(Arc::new(near));
    let bus = EventBus::<TestEvent>::without_match(connection, config());

    let seen = Arc::new(Mutex::new(Vec::new()));
    let _handle = bus.subscribe(Arc::new(Capture {
        name: "test::local",
        domains: None,
        seen: seen.clone(),
    }));

    // Overflow the outbox. Publishing is infallible-by-design, so this cannot
    // fail; what it can do is stop reaching the wire.
    for i in 0..(crate::connection::OUTBOX_CAPACITY * 2) {
        bus.publish(TestEvent::AgentTurnCompleted {
            run: format!("run-{i}"),
        });
    }

    // The wire is definitively refusing traffic by now...
    let wire = bus.try_publish(TestEvent::SystemStartup).unwrap_err();
    assert!(
        matches!(wire, crate::Error::Backpressure),
        "expected backpressure, got {wire}"
    );
    // ...and the local subscriber is still being fed. It will have lagged —
    // a 256-slot broadcast cannot hold 2048 events — so this asserts that
    // delivery continued, not that nothing was dropped.
    assert!(!wait_for(&seen, 1).await.is_empty());
}

#[tokio::test]
async fn try_recv_drains_without_waiting_and_reports_why_it_is_empty() {
    use crate::events::TryRecvError;

    let (_t, bus) = bus().await;
    let mut receiver = bus.receiver();

    // Nothing published yet: empty, not blocked.
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

    bus.publish(TestEvent::CronJobTriggered { job: "a".into() });
    bus.publish(TestEvent::CronJobTriggered { job: "b".into() });

    assert_eq!(
        receiver.try_recv().unwrap(),
        TestEvent::CronJobTriggered { job: "a".into() }
    );
    assert_eq!(
        receiver.try_recv().unwrap(),
        TestEvent::CronJobTriggered { job: "b".into() }
    );
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[tokio::test]
async fn try_recv_skips_another_catalogs_signals_rather_than_reporting_them() {
    use crate::events::TryRecvError;
    use crate::name::{InterfaceName, MemberName, ObjectPath};

    let (_t, bus) = bus().await;
    let mut receiver = bus.receiver();

    // A signal on the same connection but a different interface: not ours.
    bus.connection()
        .deliver_local(crate::message::Message::signal(
            ObjectPath::new("/somewhere/else").unwrap(),
            InterfaceName::new("ai.tinyhumans.other.Events").unwrap(),
            MemberName::new("Published").unwrap(),
            serde_json::json!([{ "nope": true }]),
        ));

    // `Empty` means nothing for *this* catalog, having skipped the rest.
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[tokio::test]
async fn a_closure_subscriber_receives_events() {
    let (_t, bus) = bus().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _handle = bus.on("test::closure", move |event| {
        let sink = sink.clone();
        async move {
            sink.lock().await.push(event);
        }
    });

    bus.publish(TestEvent::SystemStartup);
    assert_eq!(wait_for(&seen, 1).await[0], TestEvent::SystemStartup);
}

#[tokio::test]
async fn publishing_with_no_subscribers_is_not_an_error() {
    // The old bus dropped events with no receivers silently, and 254 call sites
    // depend on that being a non-event.
    let (_t, bus) = bus().await;
    bus.try_publish(TestEvent::SystemStartup).unwrap();
}

#[tokio::test]
async fn a_domain_becomes_a_path_element() {
    let config = config();
    let root = config.root.clone();
    let bus = EventBus::<TestEvent>::without_match(
        // A connection is not needed to compute a path, but the type is, so
        // this uses one over a transport that is never driven.
        Connection::attach(Arc::new(
            crate::transport::memory::MemoryTransport::pair().0,
        )),
        config,
    );
    assert_eq!(
        bus.path_for("cron").unwrap().as_str(),
        format!("{}/cron", root.as_str())
    );
    // A domain that cannot be a path element is reported, not swallowed.
    let err = bus.path_for("not a domain").unwrap_err();
    assert!(err.to_string().contains("not a domain"), "{err}");
}

#[tokio::test]
async fn root_catalogs_and_awaited_publishing_keep_the_same_wire_shape() {
    let (_transport, ordinary) = bus().await;
    ordinary
        .publish_awaited(TestEvent::SystemStartup)
        .await
        .unwrap();

    let root_config = EventBusConfig::new("/", "ai.tinyhumans.openhuman.Events").unwrap();
    let root_bus = EventBus::<TestEvent>::without_match(
        Connection::attach(Arc::new(crate::transport::memory::MemoryTransport::pair().0)),
        root_config.clone(),
    );
    assert_eq!(root_bus.path_for("cron").unwrap().as_str(), "/cron");
    assert_eq!(root_bus.config().root, root_config.root);
    assert_eq!(root_bus.connection().describe(), "memory");
}

#[test]
fn invalid_catalog_configuration_fails_before_a_subscription_is_created() {
    assert!(EventBusConfig::new("not/a/path", "ai.tinyhumans.Events").is_err());
    assert!(EventBusConfig::new("/events", "not-an-interface").is_err());
}

#[test]
fn decoding_rejects_other_catalogs_and_malformed_event_bodies() {
    let config = config();
    let path = ObjectPath::new("/ai/tinyhumans/openhuman/events/cron").unwrap();
    let interface = InterfaceName::new("ai.tinyhumans.openhuman.Events").unwrap();
    let published = MemberName::new("Published").unwrap();

    let wrong_interface = Message::signal(
        path.clone(),
        InterfaceName::new("ai.tinyhumans.other.Events").unwrap(),
        published.clone(),
        serde_json::json!([TestEvent::SystemStartup]),
    );
    assert!(crate::events::decode::<TestEvent>(&config, &wrong_interface).is_none());

    let wrong_member = Message::signal(
        path.clone(),
        interface.clone(),
        MemberName::new("Other").unwrap(),
        serde_json::json!([TestEvent::SystemStartup]),
    );
    assert!(crate::events::decode::<TestEvent>(&config, &wrong_member).is_none());

    let wrong_path = Message::signal(
        ObjectPath::new("/elsewhere").unwrap(),
        interface.clone(),
        published.clone(),
        serde_json::json!([TestEvent::SystemStartup]),
    );
    assert!(crate::events::decode::<TestEvent>(&config, &wrong_path).is_none());

    let malformed = Message::signal(path, interface, published, serde_json::json!([{"bad": true}]));
    assert!(crate::events::decode::<TestEvent>(&config, &malformed).is_none());
}
