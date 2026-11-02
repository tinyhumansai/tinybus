use super::*;
use crate::message::{Message, MessageKind};
use crate::ports::{Listener, Transport};
use crate::transport::memory::MemoryTransport;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

#[derive(Clone)]
struct HeldMetadataBus {
    accepted: mpsc::Sender<Box<dyn Transport>>,
    pending: Arc<AsyncMutex<mpsc::Receiver<Box<dyn Transport>>>>,
}

impl HeldMetadataBus {
    fn new() -> Self {
        let (accepted, pending) = mpsc::channel(8);
        Self {
            accepted,
            pending: Arc::new(AsyncMutex::new(pending)),
        }
    }

    async fn connect(
        &self,
        held_member: Option<&'static str>,
    ) -> (crate::Connection, Arc<Notify>, Arc<StdMutex<Vec<String>>>) {
        let (client, broker) = MemoryTransport::pair();
        let held = Arc::new(Notify::new());
        let observed = Arc::new(StdMutex::new(Vec::new()));
        self.accepted
            .send(Box::new(HoldMetadataRequest {
                inner: broker,
                held_member,
                held: held.clone(),
                observed: observed.clone(),
            }))
            .await
            .unwrap();
        (
            crate::Connection::connect(Box::new(client)).await.unwrap(),
            held,
            observed,
        )
    }
}

#[async_trait::async_trait]
impl Listener for HeldMetadataBus {
    async fn accept(&self) -> crate::Result<Option<Box<dyn Transport>>> {
        Ok(self.pending.lock().await.recv().await)
    }

    fn describe(&self) -> String {
        "test-held-metadata".to_string()
    }
}

struct HoldMetadataRequest {
    inner: MemoryTransport,
    held_member: Option<&'static str>,
    held: Arc<Notify>,
    observed: Arc<StdMutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Transport for HoldMetadataRequest {
    async fn send(&self, message: Message) -> crate::Result<()> {
        self.inner.send_brokered(message).await
    }

    async fn recv(&self) -> crate::Result<Option<Message>> {
        loop {
            let Some((message, _)) = self.inner.recv_with_provenance().await? else {
                return Ok(None);
            };
            if let Some(member) = message.header.member.as_ref() {
                self.observed
                    .lock()
                    .unwrap()
                    .push(member.as_str().to_string());
            }
            let is_held_query = self.held_member.is_some_and(|member| {
                message.header.kind == MessageKind::MethodCall
                    && message
                        .header
                        .member
                        .as_ref()
                        .is_some_and(|actual| actual.as_str() == member)
            });
            if is_held_query {
                self.held.notify_one();
                continue;
            }
            return Ok(Some(message));
        }
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }

    fn describe(&self) -> String {
        self.inner.describe()
    }
}

async fn client_waiting_on_metadata(
    member: &'static str,
) -> (
    ModuleClient,
    crate::Connection,
    &'static ReleaseRecord,
    Arc<Notify>,
    Arc<Mutex<Vec<serde_json::Value>>>,
    Arc<Recording>,
    Arc<StdMutex<Vec<String>>>,
    tokio::task::JoinHandle<crate::Result<()>>,
) {
    let bus = HeldMetadataBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (service, _, _) = bus.connect(None).await;
    let (caller, held, observed) = bus.connect(Some(member)).await;
    let record = strict_record();
    let calls = Arc::new(Mutex::new(Vec::new()));
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedReceiver {
                label: "attested",
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();
    if member == "GetAttestation" {
        assert_eq!(
            caller.name_owner(record.bus_name).await.unwrap(),
            service.unique_name()
        );
    }

    let sink = Arc::new(Recording::default());
    let access = PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let client = ModuleClient::new(access).with_timeout(Duration::from_millis(5));
    (
        client,
        service,
        record,
        held,
        calls,
        sink,
        observed,
        broker_task,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StalledPreparationStage {
    Readiness,
    Proxy,
}

struct StalledPreparationAccess {
    record: &'static ReleaseRecord,
    connection: crate::Connection,
    reporter: FailureReporter,
    stage: StalledPreparationStage,
    stall: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<Notify>,
    readiness_calls: Arc<std::sync::atomic::AtomicUsize>,
    proxy_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl ModuleAccess for StalledPreparationAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == self.record.id).then_some(self.record)
    }

    fn enabled(&self) -> bool {
        true
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        self.readiness_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.stage == StalledPreparationStage::Readiness
            && self.stall.load(std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            Box::pin(std::future::pending())
        } else {
            Box::pin(async { Ok(()) })
        }
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<crate::Proxy, ProxyFailure>> {
        self.proxy_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.stage == StalledPreparationStage::Proxy
            && self.stall.load(std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            return Box::pin(std::future::pending());
        }
        Box::pin(async move {
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
}

async fn stalled_binding_preparation_is_bounded(stage: StalledPreparationStage) {
    let bus = HeldMetadataBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (service, _, _) = bus.connect(None).await;
    let (caller, _, observed) = bus.connect(None).await;
    let record = strict_record();
    let calls = Arc::new(Mutex::new(Vec::new()));
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedReceiver {
                label: "ready-after-timeout",
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();

    let sink = Arc::new(Recording::default());
    let stall = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let entered = Arc::new(Notify::new());
    let readiness_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proxy_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = ModuleClient::new(StalledPreparationAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        stage,
        stall: stall.clone(),
        entered: entered.clone(),
        readiness_calls: readiness_calls.clone(),
        proxy_calls: proxy_calls.clone(),
    })
    .with_timeout(Duration::from_millis(5));

    let entered_wait = entered.notified();
    let client_for_bind = client.clone();
    let mut binding = tokio::spawn(async move { client_for_bind.bind(record.id).await });
    tokio::time::timeout(Duration::from_millis(100), entered_wait)
        .await
        .expect("the configured preparation stage should begin");
    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut binding).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            binding.abort();
            panic!("explicit client timeout must bound module preparation");
        }
    };

    assert!(matches!(
        result,
        Err(failure) if failure.kind == ModuleCallError::TransportFailed
    ));
    assert_eq!(readiness_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        proxy_calls.load(std::sync::atomic::Ordering::SeqCst),
        usize::from(stage == StalledPreparationStage::Proxy)
    );
    assert!(
        observed
            .lock()
            .unwrap()
            .iter()
            .all(|member| member != "GetNameOwner"),
        "a timed-out preparation must not query the owner"
    );
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::TransportFailed)]
    );

    // Preparation is an invocation deadline, not a terminal loader failure.
    stall.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(client.bind(record.id).await.is_ok());
    let observed = observed.lock().unwrap();
    assert_eq!(
        observed
            .iter()
            .filter(|member| member.as_str() == "GetNameOwner")
            .count(),
        1
    );
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::TransportFailed)]
    );
    broker_task.abort();
}

#[tokio::test(start_paused = true)]
async fn ordinary_binding_timeout_bounds_stalled_readiness_without_poisoning_it() {
    stalled_binding_preparation_is_bounded(StalledPreparationStage::Readiness).await;
}

#[tokio::test(start_paused = true)]
async fn ordinary_binding_timeout_bounds_stalled_proxy_preparation_without_poisoning_it() {
    stalled_binding_preparation_is_bounded(StalledPreparationStage::Proxy).await;
}

async fn stalled_metadata_call_is_bounded(member: &'static str) {
    let (client, _service, record, held, calls, sink, observed, broker_task) =
        client_waiting_on_metadata(member).await;
    let serializations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serializations_for_call = serializations.clone();
    let held_request = held.notified();
    let mut call = tokio::spawn(async move {
        client
            .call_attested::<String>(
                record.id,
                "Consume",
                SerializeCounter(serializations_for_call),
            )
            .await
    });
    let held_result = tokio::time::timeout(Duration::from_millis(100), async {
        tokio::select! {
            _ = held_request => true,
            joined = &mut call => panic!("client returned before the selected metadata query was held: {:?}; observed={:?}", joined, observed.lock().unwrap()),
        }
    })
    .await
    .expect("the selected metadata request should reach the holding transport");
    assert!(held_result);
    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut call).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            call.abort();
            panic!("the configured ModuleClient timeout must bound metadata requests");
        }
    };

    assert_eq!(result, Err(ModuleCallError::TransportFailed));
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::TransportFailed)]
    );
    broker_task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_stalled_name_owner_lookup_uses_the_client_timeout() {
    stalled_metadata_call_is_bounded("GetNameOwner").await;
}

#[tokio::test(start_paused = true)]
async fn ordinary_binding_uses_the_fixture_proxy_deadline_for_owner_lookup() {
    let bus = HeldMetadataBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (service, _, _) = bus.connect(None).await;
    let (caller, held, observed) = bus.connect(Some("GetNameOwner")).await;
    let record = strict_record();
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedReceiver {
                label: "linked",
                calls: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();

    let sink = Arc::new(Recording::default());
    let proxy = caller
        .proxy(record.bus_name, record.object_path, record.bus_name)
        .unwrap()
        .with_timeout(Duration::from_millis(5));
    let client = ModuleClient::new(PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    })
    .with_fixture(proxy);
    let mut binding = tokio::spawn(async move { client.bind(record.id).await });
    let held_request = held.notified();
    tokio::time::timeout(Duration::from_millis(100), async {
        tokio::select! {
            _ = held_request => {},
            _joined = &mut binding => panic!("bind returned before owner lookup was held; observed={:?}", observed.lock().unwrap()),
        }
    })
    .await
    .expect("owner lookup should reach the holding transport");

    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut binding).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            binding.abort();
            panic!("bind must use the fixture proxy deadline for owner lookup");
        }
    };
    assert!(matches!(result, Err(failure) if failure.kind == ModuleCallError::TransportFailed));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::TransportFailed)]
    );
    broker_task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_stalled_attestation_lookup_uses_the_client_timeout() {
    stalled_metadata_call_is_bounded("GetAttestation").await;
}
