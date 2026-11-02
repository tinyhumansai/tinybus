//! Streamed module calls preserve the same checks and reporting as typed calls.

use super::*;
use crate::broker::Broker;
use crate::module::failure::{FailureReport, FailureSink};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Error, Interface, InterfaceName, MemberName, ObjectPath, StreamRef};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "stream-client-test",
    bus_name: "ai.tinyhumans.test.StreamClient",
    object_path: "/ai/tinyhumans/test/StreamClient",
    version: "0.2.1",
    release_url: "https://example.invalid/tinybus",
    assets: &[],
};

#[cfg(feature = "modules")]
fn bound_stream_record() -> &'static ReleaseRecord {
    let host_key = Box::leak(
        crate::module::platform::host_candidates()
            .into_iter()
            .next()
            .expect("the test host has a published module key")
            .into_boxed_str(),
    );
    let assets = Box::leak(
        vec![crate::module::ReleaseAsset {
            host_key,
            archive: Box::leak("bound-stream-test.tar.gz".to_string().into_boxed_str()),
            sha256: Box::leak("a".repeat(64).into_boxed_str()),
            library_sha256: Some(Box::leak("b".repeat(64).into_boxed_str())),
        }]
        .into_boxed_slice(),
    );
    Box::leak(Box::new(ReleaseRecord {
        id: "bound-stream-test",
        bus_name: "ai.tinyhumans.test.BoundStreamClient",
        object_path: "/ai/tinyhumans/test/BoundStreamClient",
        version: "0.3.1",
        release_url: "https://example.invalid/tinybus",
        assets,
    }))
}

#[cfg(feature = "modules")]
struct HandoverStreamArguments {
    stream: StreamRef,
    started: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    barrier: Arc<std::sync::Barrier>,
}

#[cfg(feature = "modules")]
impl serde::Serialize for HandoverStreamArguments {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        self.barrier.wait();
        serde::Serialize::serialize(&self.stream, serializer)
    }
}

#[derive(Default)]
struct Reports(Mutex<Vec<Reason>>);

impl FailureSink for Reports {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0.lock().unwrap().push(report.reason);
    }
}

struct StreamAccess {
    record: &'static ReleaseRecord,
    connection: Connection,
    reporter: FailureReporter,
    enabled: bool,
    ready: bool,
}

impl ModuleAccess for StreamAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == self.record.id).then_some(self.record)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.ready {
                Ok(())
            } else {
                Err("not ready".to_string())
            }
        })
    }

    fn prepare_attested<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async move {
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
}

struct ObservedTransport {
    inner: Box<dyn crate::ports::Transport>,
    opens: Arc<AtomicUsize>,
    aborts: tokio::sync::mpsc::UnboundedSender<()>,
    refuse_first_write: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl crate::ports::Transport for ObservedTransport {
    async fn send(&self, message: crate::Message) -> crate::Result<()> {
        let interface = message.header.interface.as_ref().map(InterfaceName::as_str);
        let member = message.header.member.as_ref().map(MemberName::as_str);
        if interface == Some(crate::stream::STREAM_INTERFACE) {
            match member {
                Some("Open") => {
                    self.opens.fetch_add(1, Ordering::SeqCst);
                }
                Some("Abort") => {
                    self.aborts.send(()).unwrap();
                }
                _ => {}
            }
            if member == Some("Write")
                && self
                    .refuse_first_write
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::protocol("private write refusal detail"));
            }
        }
        self.inner.send(message).await
    }

    async fn recv(&self) -> crate::Result<Option<crate::Message>> {
        self.inner.recv().await
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }
}

#[derive(Clone)]
enum Behavior {
    Consume(tokio::sync::mpsc::UnboundedSender<Vec<u8>>),
    #[cfg(feature = "modules")]
    Bound {
        label: &'static str,
        received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    },
    Invalid,
    Unavailable,
    Stall {
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        resume: Arc<tokio::sync::Notify>,
    },
}

struct StreamService {
    interface_name: &'static str,
    connection: Connection,
    behavior: Behavior,
    calls: Arc<AtomicUsize>,
    events: Option<Arc<Mutex<Vec<String>>>>,
}

#[async_trait]
impl Interface for StreamService {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(self.interface_name).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        [
            "Upload",
            "Generate",
            "Read",
            "Release",
            "InvalidInput",
            "Unavailable",
            "Stall",
        ]
        .map(|name| MemberName::new(name).unwrap())
        .to_vec()
    }

    async fn call(&self, member: &MemberName, args: Value) -> crate::Result<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(events) = &self.events {
            events.lock().unwrap().push(member.as_str().to_string());
        }
        match (&self.behavior, member.as_str()) {
            (Behavior::Consume(received), "Upload") => {
                let (stream,): (StreamRef,) = serde_json::from_value(args)?;
                let bytes = self.connection.read_stream(&stream).await?;
                received.send(bytes.clone()).unwrap();
                Ok(serde_json::json!(bytes.len()))
            }
            #[cfg(feature = "modules")]
            (Behavior::Bound { received, .. }, "Generate") => {
                let (stream,): (StreamRef,) = serde_json::from_value(args)?;
                let bytes = self.connection.read_stream(&stream).await?;
                received.send(bytes).unwrap();
                Ok(serde_json::json!("/child"))
            }
            #[cfg(feature = "modules")]
            (Behavior::Bound { label, .. }, "Read" | "Release") => Ok(serde_json::json!(label)),
            (Behavior::Invalid, "InvalidInput") => Err(Error::MethodFailed {
                name: "ai.tinyhumans.tinydocs.Error.InvalidInput".to_string(),
                message: "secret document detail".to_string(),
            }),
            (Behavior::Unavailable, "Unavailable") => Err(Error::MethodFailed {
                name: Error::MODULE_UNAVAILABLE.to_string(),
                message: "secret unavailable detail".to_string(),
            }),
            (Behavior::Stall { entered, resume }, "Stall") => {
                entered.send(()).unwrap();
                resume.notified().await;
                Ok(Value::Null)
            }
            _ => Err(Error::failed("unexpected test method")),
        }
    }
}

struct Fixture {
    client: ModuleClient,
    connection: Connection,
    opens: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    refuse_first_write: Arc<std::sync::atomic::AtomicBool>,
    aborts: tokio::sync::mpsc::UnboundedReceiver<()>,
    reports: Arc<Reports>,
}

async fn fixture(behavior: Behavior) -> Fixture {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let refuse_first_write = Arc::new(std::sync::atomic::AtomicBool::new(false));
    service
        .serve_at(
            ObjectPath::new(RECORD.object_path).unwrap(),
            StreamService {
                interface_name: RECORD.bus_name,
                connection: service.clone(),
                behavior: behavior.clone(),
                calls: calls.clone(),
                events: None,
            },
        )
        .await
        .unwrap();
    service
        .serve_at(
            ObjectPath::new("/child").unwrap(),
            StreamService {
                interface_name: RECORD.bus_name,
                connection: service.clone(),
                behavior,
                calls: calls.clone(),
                events: None,
            },
        )
        .await
        .unwrap();

    let (aborts_tx, aborts) = tokio::sync::mpsc::unbounded_channel();
    let observed = ObservedTransport {
        inner: bus.connect().await.unwrap(),
        opens: opens.clone(),
        aborts: aborts_tx,
        refuse_first_write: refuse_first_write.clone(),
    };
    let connection = Connection::connect(Box::new(observed)).await.unwrap();
    let reports = Arc::new(Reports::default());
    let access = StreamAccess {
        record: &RECORD,
        connection: connection.clone(),
        reporter: FailureReporter::new(reports.clone()),
        enabled: true,
        ready: true,
    };
    let proxy = connection
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    Fixture {
        client: ModuleClient::new(access).with_fixture(proxy),
        connection,
        opens,
        calls,
        refuse_first_write,
        aborts,
        reports,
    }
}

#[tokio::test]
async fn module_calls_stream_empty_and_past_window_payloads() {
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    let fixture = fixture(Behavior::Consume(received_tx)).await;
    let bytes = vec![0x5a; crate::stream::MAX_CHUNK_LEN * 9 + 7];

    let length: usize = fixture
        .client
        .call_with_stream_detailed(
            RECORD.id,
            "Upload",
            |stream| serde_json::json!([stream]),
            &bytes,
        )
        .await
        .unwrap();
    assert_eq!(length, bytes.len());
    assert_eq!(received_rx.recv().await.unwrap(), bytes);

    let empty_length: usize = fixture
        .client
        .call_with_stream_detailed(
            RECORD.id,
            "Upload",
            |stream| serde_json::json!([stream]),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(empty_length, 0);
    assert!(received_rx.recv().await.unwrap().is_empty());
    assert_eq!(fixture.opens.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "modules")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_bound_stream_call_and_returned_object_stay_on_the_attested_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let service_a = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_b = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = bound_stream_record();
    let events_a = Arc::new(Mutex::new(Vec::new()));
    let events_b = Arc::new(Mutex::new(Vec::new()));
    let (received_a_tx, mut received_a_rx) = tokio::sync::mpsc::unbounded_channel();
    let (received_b_tx, mut received_b_rx) = tokio::sync::mpsc::unbounded_channel();

    for (service, label, events, received) in [
        (&service_a, "peer-a", events_a.clone(), received_a_tx),
        (&service_b, "peer-b", events_b.clone(), received_b_tx),
    ] {
        service
            .serve_at(
                ObjectPath::new(record.object_path).unwrap(),
                StreamService {
                    interface_name: record.bus_name,
                    connection: service.clone(),
                    behavior: Behavior::Bound {
                        label,
                        received: received.clone(),
                    },
                    calls: Arc::new(AtomicUsize::new(0)),
                    events: Some(events.clone()),
                },
            )
            .await
            .unwrap();
        service
            .serve_at(
                ObjectPath::new("/child").unwrap(),
                StreamService {
                    interface_name: record.bus_name,
                    connection: service.clone(),
                    behavior: Behavior::Bound { label, received },
                    calls: Arc::new(AtomicUsize::new(0)),
                    events: Some(events),
                },
            )
            .await
            .unwrap();
    }
    service_a.request_name(record.bus_name).await.unwrap();
    broker.attest_module(
        &service_a.unique_name().unwrap(),
        crate::attest::Attestation {
            name: crate::name::BusName::new(record.bus_name).unwrap(),
            sha256: record.assets[0].sha256.to_string(),
            library_sha256: record.assets[0].library_sha256.map(str::to_string),
        },
    );
    let client = ModuleClient::new(StreamAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(Arc::new(Reports::default())),
        enabled: true,
        ready: true,
    });
    let bound = client.bind_attested(record.id).await.unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started_tx)));
    let call_client = bound.clone();
    let call_barrier = barrier.clone();
    let call_started = started.clone();
    let payload = b"bound stream payload".to_vec();
    let call_payload = payload.clone();
    let generating = tokio::spawn(async move {
        call_client
            .call_with_stream_detailed::<String, _, _>(
                "Generate",
                move |stream| HandoverStreamArguments {
                    stream: stream.clone(),
                    started: call_started,
                    barrier: call_barrier,
                },
                &call_payload,
            )
            .await
    });
    started_rx.await.unwrap();
    service_a.release_name(record.bus_name).await.unwrap();
    service_b.request_name(record.bus_name).await.unwrap();
    barrier.wait();

    let child_path = generating.await.unwrap().unwrap();
    assert_eq!(child_path, "/child");
    assert_eq!(received_a_rx.recv().await.unwrap(), payload);
    assert!(received_b_rx.try_recv().is_err());

    let child = bound.object_client(&child_path).unwrap();
    assert_eq!(child.call::<String>("Read", ()).await.unwrap(), "peer-a");
    assert_eq!(child.call::<String>("Release", ()).await.unwrap(), "peer-a");

    let sanitized_bytes = b"sanitized bound stream";
    assert_eq!(
        bound
            .call_with_stream::<String, _, _>(
                "Generate",
                |stream| serde_json::json!([stream]),
                sanitized_bytes,
            )
            .await
            .unwrap(),
        "/child"
    );
    assert_eq!(received_a_rx.recv().await.unwrap(), sanitized_bytes);
    assert_eq!(
        events_a.lock().unwrap().as_slice(),
        &["Generate", "Read", "Release", "Generate"]
    );
    assert!(events_b.lock().unwrap().is_empty());

    service_a.close().await.unwrap();
    assert!(
        bound
            .call_with_stream::<String, _, _>(
                "Generate",
                |stream| serde_json::json!([stream]),
                b"after disconnect",
            )
            .await
            .is_err()
    );
    assert!(events_b.lock().unwrap().is_empty());
    assert!(received_b_rx.try_recv().is_err());
    broker_task.abort();
}

#[tokio::test]
async fn invalid_or_unavailable_module_stream_calls_do_not_run_the_builder_or_open_a_stream() {
    let (received_tx, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let fixture = fixture(Behavior::Consume(received_tx)).await;
    let builder_calls = Arc::new(AtomicUsize::new(0));
    assert!(
        fixture
            .client
            .call_with_stream_detailed::<Value, _, _>(
                "unknown",
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    let proxy = fixture.client.fixture_proxy().unwrap();
    let disabled = ModuleClient::new(StreamAccess {
        record: &RECORD,
        connection: fixture.connection.clone(),
        reporter: FailureReporter::new(Arc::new(Reports::default())),
        enabled: false,
        ready: true,
    })
    .with_fixture(proxy.clone());
    assert!(
        disabled
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    let unready = ModuleClient::new(StreamAccess {
        record: &RECORD,
        connection: fixture.connection.clone(),
        reporter: FailureReporter::new(Arc::new(Reports::default())),
        enabled: true,
        ready: false,
    });
    assert!(
        unready
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    assert!(
        fixture
            .client
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "invalid/member",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );
    assert_eq!(builder_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn detailed_stream_failures_preserve_wire_input_errors_and_sanitize_renderings() {
    let invalid_fixture = fixture(Behavior::Invalid).await;
    let failure = invalid_fixture
        .client
        .call_with_stream_detailed::<Value, _, _>(
            RECORD.id,
            "InvalidInput",
            |stream| serde_json::json!([stream]),
            b"untrusted input",
        )
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(
        failure.wire_name,
        "ai.tinyhumans.tinydocs.Error.InvalidInput"
    );
    assert_eq!(failure.message, "secret document detail");
    for rendered in [format!("{failure:?}"), failure.to_string()] {
        assert!(!rendered.contains("InvalidInput"));
        assert!(!rendered.contains("secret document detail"));
    }

    let object = invalid_fixture
        .client
        .object_client(RECORD.id, "/child")
        .await
        .unwrap();
    let object_failure = object
        .call_detailed::<Value>("InvalidInput", ())
        .await
        .unwrap_err();
    assert_eq!(object_failure.wire_name, failure.wire_name);
    assert_eq!(object_failure.message, failure.message);
    let sanitized_stream = invalid_fixture
        .client
        .call_with_stream::<Value, _, _>(
            RECORD.id,
            "InvalidInput",
            |stream| serde_json::json!([stream]),
            b"untrusted input",
        )
        .await
        .unwrap_err();
    assert_eq!(sanitized_stream, ModuleCallError::ModuleFault);
    assert!(!format!("{sanitized_stream:?}").contains("secret document detail"));
    assert!(
        !sanitized_stream
            .to_string()
            .contains("secret document detail")
    );
    let unavailable_fixture = fixture(Behavior::Unavailable).await;
    let unavailable_object = unavailable_fixture
        .client
        .object_client(RECORD.id, "/child")
        .await
        .unwrap();
    let unavailable = unavailable_object
        .call_detailed::<Value>("Unavailable", ())
        .await
        .unwrap_err();
    assert_eq!(unavailable.kind, ModuleCallError::Unavailable);
    assert_eq!(unavailable.wire_name, Error::MODULE_UNAVAILABLE);
    assert_eq!(unavailable.message, "secret unavailable detail");

    let sanitized = object.call::<Value>("InvalidInput", ()).await.unwrap_err();
    assert_eq!(sanitized, ModuleCallError::ModuleFault);
    assert!(!format!("{sanitized:?}").contains("secret document detail"));
    assert!(!sanitized.to_string().contains("secret document detail"));
    let reports = invalid_fixture.reports.0.lock().unwrap();
    assert_eq!(
        reports.as_slice(),
        &[
            Reason::ModuleFault,
            Reason::ModuleFault,
            Reason::ModuleFault,
            Reason::ModuleFault,
        ]
    );
}

struct FailsSerialization;

impl serde::Serialize for FailsSerialization {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("secret serialization detail"))
    }
}

#[tokio::test]
async fn argument_serialization_failure_aborts_before_delivering_the_business_method() {
    let (received_tx, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut fixture = fixture(Behavior::Consume(received_tx)).await;
    let builder_calls = Arc::new(AtomicUsize::new(0));
    let called = builder_calls.clone();
    let failure = fixture
        .client
        .call_with_stream_detailed::<Value, _, FailsSerialization>(
            RECORD.id,
            "Upload",
            move |_stream| {
                called.fetch_add(1, Ordering::SeqCst);
                FailsSerialization
            },
            b"payload",
        )
        .await
        .unwrap_err();

    assert_eq!(failure.kind, ModuleCallError::IncompatibleContract);
    assert!(!format!("{failure:?}").contains("secret serialization detail"));
    assert!(!failure.to_string().contains("secret serialization detail"));
    assert_eq!(builder_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.opens.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(10), fixture.aborts.recv())
        .await
        .expect("failed argument serialization dropped the opened writer")
        .expect("the abort observer remained connected");
}

#[tokio::test]
async fn module_client_timeout_bounds_a_stalled_method_reply() {
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut fixture = fixture(Behavior::Stall {
        entered: entered_tx,
        resume: resume.clone(),
    })
    .await;
    let client = fixture
        .client
        .clone()
        .with_timeout(Duration::from_millis(100));
    let bytes = vec![0x22; crate::stream::MAX_CHUNK_LEN * 10];
    let call = tokio::spawn(async move {
        client
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Stall",
                |stream| serde_json::json!([stream]),
                &bytes,
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("stalled method was dispatched")
        .expect("the method-entry observer remained connected");
    let failure = tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .expect("ModuleClient timeout bounded the stalled reply")
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::TransportFailed);
    tokio::time::timeout(Duration::from_secs(10), fixture.aborts.recv())
        .await
        .expect("timeout dropped the unfinished stream writer")
        .expect("the abort observer remained connected");
    resume.notify_one();
}

#[tokio::test]
async fn a_refused_stream_write_aborts_the_opened_upload() {
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut fixture = fixture(Behavior::Stall {
        entered: entered_tx,
        resume: resume.clone(),
    })
    .await;
    fixture
        .refuse_first_write
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = fixture.client.clone();
    let task = tokio::spawn(async move {
        client
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Stall",
                |stream| serde_json::json!([stream]),
                b"write will be refused",
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("stalled method was dispatched")
        .expect("the method-entry observer remained connected");
    let failure = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the refused chunk failed without waiting for the stalled method")
        .unwrap()
        .unwrap_err();
    assert_eq!(
        failure.kind,
        ModuleCallError::TransportFailed,
        "wire failure: {} ({})",
        failure.wire_name,
        failure.message
    );
    assert!(
        !fixture
            .refuse_first_write
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    tokio::time::timeout(Duration::from_secs(10), fixture.aborts.recv())
        .await
        .expect("write failure dropped the unfinished stream writer")
        .expect("the abort observer remained connected");
    resume.notify_one();
}

#[tokio::test]
async fn method_failure_and_cancellation_abort_a_streamed_upload() {
    let failed_fixture = fixture(Behavior::Invalid).await;
    let bytes = vec![0x33; crate::stream::MAX_CHUNK_LEN * 12];
    let failure = failed_fixture
        .client
        .call_with_stream_detailed::<Value, _, _>(
            RECORD.id,
            "InvalidInput",
            |stream| serde_json::json!([stream]),
            &bytes,
        )
        .await
        .unwrap_err();
    assert_eq!(
        failure.wire_name,
        "ai.tinyhumans.tinydocs.Error.InvalidInput"
    );
    let mut failed_fixture = failed_fixture;
    tokio::time::timeout(Duration::from_secs(10), failed_fixture.aborts.recv())
        .await
        .expect("method failure dropped the unfinished stream writer")
        .expect("the abort observer remained connected");

    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut stalled = fixture(Behavior::Stall {
        entered: entered_tx,
        resume: resume.clone(),
    })
    .await;
    let bytes = vec![0x44; crate::stream::MAX_CHUNK_LEN * 12];
    let task = {
        let client = stalled.client.clone();
        tokio::spawn(async move {
            client
                .call_with_stream_detailed::<Value, _, _>(
                    RECORD.id,
                    "Stall",
                    |stream| serde_json::json!([stream]),
                    &bytes,
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("stalled method was dispatched")
        .expect("the method-entry observer remained connected");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), stalled.aborts.recv())
        .await
        .expect("cancellation dropped the unfinished stream writer")
        .expect("the abort observer remained connected");
    resume.notify_one();
}
