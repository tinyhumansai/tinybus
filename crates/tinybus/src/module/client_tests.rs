use super::*;
use crate::message::{Message, MessageKind};
use crate::module::failure::{FailureReport, FailureReporter, FailureSink};
use crate::ports::{Listener, Transport};
use crate::transport::memory::{MemoryBus, MemoryTransport};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

#[derive(Default)]
struct Recording(Mutex<Vec<(String, Reason)>>);

impl FailureSink for Recording {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((report.module.to_string(), report.reason));
    }
}

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "client-test-module",
    bus_name: "ai.tinyhumans.test.ClientModule",
    object_path: "/ai/tinyhumans/test/ClientModule",
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};

/// A host without the loader: it knows its registry and refuses every load.
struct Loaderless {
    enabled: bool,
    reporter: FailureReporter,
}

impl ModuleAccess for Loaderless {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        let _ = id;
        Box::pin(async {
            self.reporter()
                .report(RECORD.id, RECORD.version, Reason::LoaderDisabled);
            Err("this build has no module loader".to_string())
        })
    }

    fn proxy<'a>(
        &'a self,
        _bus_name: &'a str,
        _object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async { Err(ProxyFailure::Transport) })
    }
}

struct ReadyAccess {
    connection: crate::Connection,
    reporter: FailureReporter,
    proxy_failure: Option<ProxyFailure>,
    proxy_timeout: Option<Duration>,
}

impl ModuleAccess for ReadyAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        true
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async move {
            if let Some(failure) = self.proxy_failure {
                return Err(failure);
            }
            let proxy = self
                .connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)?;
            Ok(self
                .proxy_timeout
                .map_or(proxy.clone(), |timeout| proxy.with_timeout(timeout)))
        })
    }
}

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

    async fn connect(&self, held_member: Option<&'static str>) -> (crate::Connection, Arc<Notify>) {
        let (client, broker) = MemoryTransport::pair();
        let held = Arc::new(Notify::new());
        self.accepted
            .send(Box::new(HoldMetadataRequest {
                inner: broker,
                held_member,
                held: held.clone(),
            }))
            .await
            .unwrap();
        (
            crate::Connection::connect(Box::new(client)).await.unwrap(),
            held,
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

fn resolver(enabled: bool) -> (Arc<Recording>, Loaderless) {
    let sink = Arc::new(Recording::default());
    let access = Loaderless {
        enabled,
        reporter: FailureReporter::new(sink.clone()),
    };
    (sink, access)
}

#[tokio::test]
async fn lifecycle_defaults_keep_loaderless_access_source_compatible() {
    let (_, access) = resolver(true);
    assert!(access.prepare_attested(RECORD.id).await.is_err());
    assert_eq!(
        access
            .reinitialize(RECORD.id, serde_json::Value::Null)
            .await,
        Err(ReconfigureFailure::Unsupported)
    );
    access.mark_faulted(RECORD.id);
}

fn method_failed(name: &str) -> Error {
    Error::MethodFailed {
        name: name.to_string(),
        message: String::new(),
    }
}

#[test]
fn bus_errors_classify_to_closed_outcomes() {
    let cases = [
        (
            Error::ModuleUnavailable {
                module: "m".to_string(),
                state: String::new(),
                detail: String::new(),
            },
            ModuleCallError::Unavailable,
            Reason::ModuleUnavailable,
        ),
        (
            method_failed("ai.tinyhumans.tinybus.Error.ModuleUnavailable"),
            ModuleCallError::Unavailable,
            Reason::ModuleUnavailable,
        ),
        (
            method_failed(Error::UNKNOWN_METHOD),
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        (
            Error::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        (
            method_failed(Error::NOT_ATTESTED),
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
        (
            method_failed("ai.tinyhumans.tinybus.Error.Protocol"),
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
        (
            method_failed("com.example.Protocol"),
            ModuleCallError::ModuleFault,
            Reason::ModuleFault,
        ),
        (
            method_failed("com.example.Whatever"),
            ModuleCallError::ModuleFault,
            Reason::ModuleFault,
        ),
        (
            Error::Transport("down".to_string()),
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
    ];
    for (error, outcome, reason) in cases {
        assert_eq!(classify(&error), (outcome, reason), "{error:?}");
    }
}

#[test]
fn wire_kind_is_the_closed_classification_not_the_wire_error_name() {
    let error = method_failed("com.example.PrivateFailure");
    assert_eq!(wire_kind(&error), ModuleCallError::ModuleFault);
}

#[test]
fn unavailable_proxy_failures_are_deduplicated_as_terminal_module_state() {
    let sink = Arc::new(Recording::default());
    let reporter = FailureReporter::new(sink.clone());
    let error = Error::ModuleUnavailable {
        module: RECORD.id.to_owned(),
        state: "faulted".to_owned(),
        detail: "module is faulted".to_owned(),
    };
    assert_eq!(
        report_proxy_failure(&reporter, &RECORD, &error),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleUnavailable)]
    );
}

#[test]
fn call_errors_render_the_already_reported_prefix() {
    for error in [
        ModuleCallError::Unavailable,
        ModuleCallError::IncompatibleContract,
        ModuleCallError::TransportFailed,
        ModuleCallError::ModuleFault,
    ] {
        assert!(
            error
                .to_string()
                .starts_with("MODULE_CALL_REPORTED: module ")
        );
    }
}

#[tokio::test]
async fn an_unknown_module_is_unavailable_and_reported_anonymously() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let result: Result<String, _> = client.call("not-registered", "Member", ()).await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[("unregistered".to_string(), Reason::UnknownModule)]
    );
}

#[tokio::test]
async fn a_disabled_registry_refuses_without_starting_anything() {
    let (sink, resolver) = resolver(false);
    let client = ModuleClient::new(resolver);
    let result: Result<String, _> = client.call(RECORD.id, "Member", ()).await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::Disabled)]
    );
}

#[test]
fn malformed_json_is_a_module_fault_reported_by_identity_only() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let decoded: Result<serde_json::Value, _> = client.decode_json(RECORD.id, "not json {");
    assert_eq!(decoded, Err(ModuleCallError::ModuleFault));
    let decoded: Result<serde_json::Value, _> = client.decode_json(RECORD.id, "{\"a\":1}");
    assert!(decoded.is_ok());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleFault)]
    );
}

#[tokio::test]
async fn a_fixture_proxy_is_classified_and_reported_when_the_call_fails() {
    use crate::broker::Broker;
    use crate::transport::memory::MemoryBus;

    let (sink, resolver) = resolver(true);
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let connection = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(resolver)
        .with_fixture(proxy)
        .with_timeout(Duration::from_millis(500));
    assert!(client.fixture_proxy().is_some());
    let result: Result<String, _> = client.call(RECORD.id, "Member", ()).await;
    assert!(result.is_err());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleFault)]
    );
}

const REMOTE_DETAIL: &str = "fixture-only credential-like value: not-a-real-credential-token-7391";

struct DetailedFailure;

#[async_trait::async_trait]
impl crate::Interface for DetailedFailure {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Fail").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::MethodFailed {
            name: "com.example.PrivateFailure".to_string(),
            message: REMOTE_DETAIL.to_string(),
        })
    }
}

#[tokio::test]
async fn detailed_call_preserves_remote_failure_without_rendering_it() {
    let sink = Arc::new(Recording::default());
    let transport = crate::transport::memory::MemoryBus::new();
    let _broker = crate::broker::Broker::new().spawn(transport.clone());
    let service = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    service
        .serve_at(
            crate::ObjectPath::new(RECORD.object_path).unwrap(),
            DetailedFailure,
        )
        .await
        .unwrap();
    let caller = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(ReadyAccess {
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        proxy_failure: None,
        proxy_timeout: None,
    })
    .with_fixture(proxy);

    let failure = client
        .call_detailed::<String>(RECORD.id, "Fail", ())
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(failure.wire_name, "com.example.PrivateFailure");
    assert_eq!(failure.message, REMOTE_DETAIL);
    for rendered in [format!("{failure:?}"), failure.to_string()] {
        assert!(!rendered.contains("com.example.PrivateFailure"));
        assert!(!rendered.contains(REMOTE_DETAIL));
    }

    let sanitized = client
        .call::<String>(RECORD.id, "Fail", ())
        .await
        .unwrap_err();
    assert_eq!(sanitized, ModuleCallError::ModuleFault);
    assert!(!format!("{sanitized:?}").contains(REMOTE_DETAIL));
    assert!(!sanitized.to_string().contains(REMOTE_DETAIL));
    let reports = sink.0.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert!(!format!("{reports:?}").contains(REMOTE_DETAIL));
}

struct JsonFixture;
#[async_trait::async_trait]
impl crate::Interface for JsonFixture {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Json").unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!("{\"answer\":42}"))
    }
}

#[tokio::test]
async fn fixture_json_and_object_calls_preserve_deadlines_and_confidential_refusals() {
    let (sink, resolver) = resolver(true);
    let bus = crate::transport::memory::MemoryBus::new();
    let _broker = crate::broker::Broker::new();
    let _broker_task = _broker.clone().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    for path in [RECORD.object_path, "/child"] {
        service
            .serve_at(crate::ObjectPath::new(path).unwrap(), JsonFixture)
            .await
            .unwrap();
    }
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(resolver)
        .with_fixture(proxy)
        .with_timeout(Duration::from_secs(1));
    let reply: serde_json::Value = client.call_json(RECORD.id, "Json", ()).await.unwrap();
    assert_eq!(reply, serde_json::json!({"answer":42}));
    let object = client.object_client(RECORD.id, "/child").await.unwrap();
    let reply: String = object.call("Json", ()).await.unwrap();
    assert_eq!(reply, "{\"answer\":42}");

    let direct_sink = Arc::new(Recording::default());
    let direct = ModuleClient::new(ReadyAccess {
        connection: caller.clone(),
        reporter: FailureReporter::new(direct_sink.clone()),
        proxy_failure: None,
        proxy_timeout: None,
    });
    let direct_object = direct.object_client(RECORD.id, "/child").await.unwrap();
    let reply: String = direct_object.call("Json", ()).await.unwrap();
    assert_eq!(reply, "{\"answer\":42}");

    for (failure, expected, reason) in [
        (
            ProxyFailure::Transport,
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
        (
            ProxyFailure::Contract,
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
    ] {
        let reporter = Arc::new(Recording::default());
        let failing = ModuleClient::new(ReadyAccess {
            connection: caller.clone(),
            reporter: FailureReporter::new(reporter.clone()),
            proxy_failure: Some(failure),
            proxy_timeout: None,
        });
        let result: Result<String, _> = failing.call(RECORD.id, "Json", ()).await;
        assert_eq!(result, Err(expected));
        assert_eq!(
            reporter.0.lock().unwrap().as_slice(),
            &[(RECORD.id.to_string(), reason)]
        );
    }
    assert!(
        direct
            .object_client(RECORD.id, "invalid path")
            .await
            .is_err()
    );
    assert_eq!(
        direct_sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::IncompatibleContract)]
    );
    let reply: Result<String, _> = object.call_confidential("Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    let reply: Result<String, _> = client.call_confidential(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    let reply: Result<serde_json::Value, _> =
        client.call_confidential_json(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    assert!(
        sink.0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, reason)| *reason == Reason::TransportFailed)
    );

    #[cfg(feature = "modules")]
    {
        _broker.attest_module(
            &service.unique_name().unwrap(),
            crate::attest::Attestation {
                name: crate::BusName::new(RECORD.bus_name).unwrap(),
                sha256: "a".repeat(64),
                library_sha256: None,
            },
        );
        let reply: serde_json::Value = client
            .call_confidential_json(RECORD.id, "Json", ())
            .await
            .unwrap();
        assert_eq!(reply, serde_json::json!({ "answer": 42 }));
    }
}

#[tokio::test]
async fn unavailable_loads_and_explicit_reports_use_only_registered_identity() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let reply: Result<String, _> = client.call(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::Unavailable));
    assert!(client.object_client(RECORD.id, "/child").await.is_err());
    client.report_incompatible_contract(RECORD.id);
    client.report_unavailable(RECORD.id);
    client.report_malformed_reply("unknown");
    client.report_incompatible_contract("unknown");
    client.report_unavailable("unknown");
    assert!(
        sink.0
            .lock()
            .unwrap()
            .iter()
            .all(|(module, _)| module == RECORD.id)
    );
}

#[tokio::test]
async fn unloaded_access_reports_loader_disabled_only_for_known_records() {
    let sink = Arc::new(Recording::default());
    let reporter = Arc::new(FailureReporter::new(sink.clone()));
    let access = UnloadedAccess::new(vec![&RECORD], true, reporter.clone());

    assert!(access.enabled());
    assert!(std::ptr::eq(access.find(RECORD.id).unwrap(), &RECORD));
    assert!(access.find("arbitrary-unregistered-id").is_none());
    assert_eq!(
        access.ensure_loaded("arbitrary-unregistered-id").await,
        Err("module is unavailable: this build has no module loader".into())
    );
    assert!(sink.0.lock().unwrap().is_empty());

    let error = access.ensure_loaded(RECORD.id).await.unwrap_err();
    assert!(error.contains("no module loader"));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::LoaderDisabled)]
    );
}

#[tokio::test]
async fn unloaded_access_preserves_the_disabled_reason_before_loader_disabled() {
    let sink = Arc::new(Recording::default());
    let reporter = Arc::new(FailureReporter::new(sink.clone()));
    let client = ModuleClient::new(UnloadedAccess::new(vec![&RECORD], false, reporter));

    let result: Result<String, _> = client.call(RECORD.id, "Call", ()).await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::Disabled)]
    );
}

#[path = "client_attested_tests.rs"]
mod attested_tests;
struct OwnerFixture(&'static str);
#[async_trait::async_trait]
impl crate::Interface for OwnerFixture {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        ["Who", "Block"]
            .map(|name| crate::MemberName::new(name).unwrap())
            .to_vec()
    }
    async fn call(
        &self,
        member: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        if member.as_str() == "Block" {
            std::future::pending::<()>().await;
        }
        Ok(serde_json::json!(self.0))
    }
}

struct StalledObject(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);

#[async_trait::async_trait]
impl crate::Interface for StalledObject {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Block").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        let _ = self.0.lock().unwrap().take().unwrap().send(());
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn pin_owner_obeys_the_object_proxy_deadline_for_name_owner_lookup() {
    let bus = HeldMetadataBus::new();
    let broker_task = crate::broker::Broker::new().spawn(bus.clone());
    let (connection, held) = bus.connect(Some("GetNameOwner")).await;
    let proxy = connection
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(ReadyAccess {
        connection,
        reporter: FailureReporter::new(Arc::new(Recording::default())),
        proxy_failure: None,
        proxy_timeout: None,
    })
    .with_fixture(proxy)
    .with_timeout(Duration::from_millis(5));
    let object = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap();
    let mut pin = tokio::spawn(async move { object.pin_owner().await });

    tokio::time::timeout(Duration::from_millis(100), held.notified())
        .await
        .expect("pin_owner should issue GetNameOwner");
    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut pin).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            pin.abort();
            panic!("pin_owner should use the object's configured metadata deadline");
        }
    };

    assert!(matches!(result, Err(ModuleCallError::TransportFailed)));
    broker_task.abort();
}

#[tokio::test(start_paused = true)]
async fn derived_object_keeps_a_custom_module_access_proxy_deadline() {
    let bus = MemoryBus::new();
    let _broker = crate::broker::Broker::new().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    let (started, started_rx) = tokio::sync::oneshot::channel();
    service
        .serve_at(
            crate::ObjectPath::new("/child").unwrap(),
            StalledObject(Mutex::new(Some(started))),
        )
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let access = ReadyAccess {
        connection: caller,
        reporter: FailureReporter::new(Arc::new(Recording::default())),
        proxy_failure: None,
        proxy_timeout: Some(Duration::from_millis(5)),
    };
    let root = ModuleClient::new(access)
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap();
    let child = root.object_at("/child").unwrap();
    let mut call = tokio::spawn(async move { child.call::<String>("Block", ()).await });

    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("the child method should reach its service")
        .unwrap();
    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut call).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            call.abort();
            panic!("a derived child should keep ModuleAccess's proxy deadline");
        }
    };

    assert_eq!(result, Err(ModuleCallError::TransportFailed));
}

#[tokio::test]
async fn pinned_objects_and_derived_children_keep_the_original_peer() {
    let (_, access) = resolver(true);
    let bus = crate::transport::memory::MemoryBus::new();
    crate::broker::Broker::new().spawn(bus.clone());
    let original = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let replacement = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    for (service, label) in [(&original, "original"), (&replacement, "replacement")] {
        for path in [RECORD.object_path, "/child"] {
            service
                .serve_at(crate::ObjectPath::new(path).unwrap(), OwnerFixture(label))
                .await
                .unwrap();
        }
    }
    original.request_name(RECORD.bus_name).await.unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = ModuleClient::new(access).with_fixture(
        caller
            .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
            .unwrap(),
    );
    let root = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap()
        .pin_owner()
        .await
        .unwrap();
    original.release_name(RECORD.bus_name).await.unwrap();
    replacement.request_name(RECORD.bus_name).await.unwrap();
    assert_eq!(root.call::<String>("Who", ()).await.unwrap(), "original");
    let child = root.object_at("/child").unwrap();
    assert_eq!(child.call::<String>("Who", ()).await.unwrap(), "original");
    assert_eq!(
        child.call_confidential::<String>("Who", ()).await,
        Err(ModuleCallError::TransportFailed)
    );
    assert!(root.object_at("invalid path").is_err());
}

#[tokio::test(start_paused = true)]
async fn derived_objects_keep_the_configured_deadline_and_report_failures() {
    let (sink, access) = resolver(true);
    let bus = crate::transport::memory::MemoryBus::new();
    crate::broker::Broker::new().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    service
        .serve_at(
            crate::ObjectPath::new("/child").unwrap(),
            OwnerFixture("original"),
        )
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = ModuleClient::new(access)
        .with_fixture(
            caller
                .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
                .unwrap(),
        )
        .with_timeout(Duration::from_millis(50));
    let root = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap()
        .pin_owner()
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    assert_eq!(
        root.object_at("/child")
            .unwrap()
            .call::<String>("Block", ())
            .await,
        Err(ModuleCallError::TransportFailed)
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(
        sink.0.lock().unwrap().last().unwrap().1,
        Reason::TransportFailed
    );
    let missing = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap();
    service.release_name(RECORD.bus_name).await.unwrap();
    assert!(matches!(
        missing.pin_owner().await,
        Err(ModuleCallError::Unavailable)
    ));
}

#[tokio::test]
async fn pin_owner_reports_an_alias_that_has_lost_its_owner_as_unavailable() {
    let sink = Arc::new(Recording::default());
    let bus = crate::transport::memory::MemoryBus::new();
    crate::broker::Broker::new().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    service
        .serve_at(
            crate::ObjectPath::new(RECORD.object_path).unwrap(),
            OwnerFixture("owner"),
        )
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let access = ReadyAccess {
        connection: caller.clone(),
        reporter: FailureReporter::new(sink.clone()),
        proxy_failure: None,
        proxy_timeout: None,
    };
    let client = ModuleClient::new(access).with_fixture(
        caller
            .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
            .unwrap(),
    );
    let object = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap();

    service.release_name(RECORD.bus_name).await.unwrap();
    assert!(matches!(
        object.pin_owner().await,
        Err(ModuleCallError::Unavailable)
    ));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleUnavailable)]
    );
}

#[tokio::test]
async fn derived_object_rejecting_an_invalid_path_reports_incompatible_contract() {
    let sink = Arc::new(Recording::default());
    let bus = crate::transport::memory::MemoryBus::new();
    crate::broker::Broker::new().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let access = ReadyAccess {
        connection: caller.clone(),
        reporter: FailureReporter::new(sink.clone()),
        proxy_failure: None,
        proxy_timeout: None,
    };
    let client = ModuleClient::new(access).with_fixture(
        caller
            .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
            .unwrap(),
    );
    let object = client
        .object_client(RECORD.id, RECORD.object_path)
        .await
        .unwrap();

    assert!(matches!(
        object.object_at("invalid path"),
        Err(ModuleCallError::IncompatibleContract)
    ));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::IncompatibleContract)]
    );
}
