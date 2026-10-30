use super::*;
use crate::broker::Broker;
use crate::module::abi::{TB_BAD_ARGUMENT, TB_OK, TbAbiDescriptor, TbHostVtable, TbModuleVtable};
use crate::module::failure::{FailureReport, FailureReporter, FailureSink, Reason};
use crate::module::host::ModuleHost;
use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy};
use crate::module::{AccessFuture, ModuleState, ReleaseRecord};
use crate::name::{BusName, ObjectPath};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Message, Proxy};
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

const ID: &str = "reconfigure-test-module";
const BUS: &str = "ai.tinyhumans.test.Reconfigure";
const UNATTESTED_BUS: &str = "ai.tinyhumans.test.Unattested";
const PATH: &str = "/ai/tinyhumans/test/Reconfigure";
static RECORD: ReleaseRecord = ReleaseRecord {
    id: ID,
    bus_name: BUS,
    object_path: PATH,
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};
static FAILED_RESOLVER_RECORD: ReleaseRecord = ReleaseRecord {
    id: "reconfigure-report-resolution-failure",
    bus_name: "ai.tinyhumans.test.ReconfigureReportResolutionFailure",
    object_path: "/ai/tinyhumans/test/ReconfigureReportResolutionFailure",
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};

#[derive(Default)]
struct RecordingSink(StdMutex<Vec<(String, String, Reason)>>);

impl FailureSink for RecordingSink {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0.lock().unwrap().push((
            report.module.to_owned(),
            report.version.to_owned(),
            report.reason,
        ));
    }
}
static FIXTURE_LOCK: Mutex<()> = Mutex::const_new(());
static INITIALIZER_STARTED: OnceLock<StdMutex<Option<std::sync::mpsc::SyncSender<()>>>> =
    OnceLock::new();
static INITIALIZER_RELEASE: OnceLock<StdMutex<Option<std::sync::mpsc::Receiver<()>>>> =
    OnceLock::new();
static BLOCKED_MEMBER: OnceLock<StdMutex<Option<BlockedMember>>> = OnceLock::new();
static MEMBER_CONFIG_CALLS: AtomicUsize = AtomicUsize::new(0);

struct BlockedMember {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
    finished: std::sync::mpsc::SyncSender<()>,
}

struct FixtureState {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
    config: Arc<StdMutex<serde_json::Value>>,
}

unsafe extern "C" fn fixture_deliver(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || ptr.is_null() {
        return crate::module::abi::TB_BAD_ARGUMENT;
    }
    let state = unsafe { &*ctx.cast::<FixtureState>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    match state.tx.try_send(bytes) {
        Ok(()) => TB_OK,
        Err(std::sync::mpsc::TrySendError::Full(_)) => crate::module::abi::TB_BACKPRESSURE,
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => crate::module::abi::TB_CLOSED,
    }
}

unsafe extern "C" fn fixture_reinitialize(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || (ptr.is_null() && len != 0) {
        return TB_BAD_ARGUMENT;
    }
    let config = if len == 0 {
        serde_json::Value::Null
    } else {
        match serde_json::from_slice(unsafe { std::slice::from_raw_parts(ptr, len) }) {
            Ok(config) => config,
            Err(_) => return TB_BAD_ARGUMENT,
        }
    };
    if config.get("reject").and_then(serde_json::Value::as_bool) == Some(true) {
        return TB_BAD_ARGUMENT;
    }
    let state = unsafe { &*ctx.cast::<FixtureState>() };
    *state.config.lock().expect("fixture config lock") = config;
    TB_OK
}

unsafe extern "C" fn fixture_shutdown(_: *mut c_void, _: u64) -> i32 {
    TB_OK
}

unsafe extern "C" fn fixture_init(host: *const TbHostVtable, out: *mut TbModuleVtable) -> i32 {
    if host.is_null() || out.is_null() {
        return TB_BAD_ARGUMENT;
    }
    let host = unsafe { *host };
    let initial_config = if host.config.len == 0 {
        serde_json::Value::Null
    } else {
        if host.config.ptr.is_null() {
            return TB_BAD_ARGUMENT;
        }
        match serde_json::from_slice(unsafe {
            std::slice::from_raw_parts(host.config.ptr, host.config.len)
        }) {
            Ok(config) => config,
            Err(_) => return TB_BAD_ARGUMENT,
        }
    };
    let host_ctx = host.host_ctx as usize;
    let send = host.send;
    let ready = host.ready;
    let config = Arc::new(StdMutex::new(initial_config));
    let service_config = config.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(16);
    std::thread::spawn(move || {
        unsafe { ready(host_ctx as *mut c_void) };
        while let Ok(bytes) = rx.recv() {
            let service_config = service_config.clone();
            std::thread::spawn(move || {
                let Ok(message) = serde_json::from_slice::<Message>(&bytes) else {
                    return;
                };
                if message.header.kind != crate::message::MessageKind::MethodCall {
                    return;
                }
                let member = message.header.member.as_ref().map(|name| name.as_str());
                let blocked = if member == Some("Configure") {
                    let config = message
                        .body
                        .as_array()
                        .and_then(|values| values.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    MEMBER_CONFIG_CALLS.fetch_add(1, Ordering::SeqCst);
                    if config
                        .get("hold_member")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    {
                        BLOCKED_MEMBER
                            .get_or_init(|| StdMutex::new(None))
                            .lock()
                            .expect("blocked member fixture lock")
                            .take()
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(blocked) = &blocked {
                    let _ = blocked.entered.send(());
                    let _ = blocked.release.recv();
                }
                let body = match member {
                    Some("ReadConfig") => service_config.lock().unwrap().clone(),
                    Some("Configure") => {
                        let config = message
                            .body
                            .as_array()
                            .and_then(|values| values.first())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        *service_config.lock().unwrap() = config.clone();
                        config
                    }
                    _ => serde_json::Value::Null,
                };
                let reply = Message::method_return(&message.header, body);
                let reply = serde_json::to_vec(&reply).expect("fixture reply serializes");
                let _ = unsafe { send(host_ctx as *mut c_void, reply.as_ptr(), reply.len()) };
                if let Some(blocked) = blocked {
                    let _ = blocked.finished.send(());
                }
            });
        }
    });
    let state = Box::into_raw(Box::new(FixtureState { tx, config }));
    unsafe {
        *out = TbModuleVtable {
            size: std::mem::size_of::<TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: state.cast(),
            deliver: fixture_deliver,
            shutdown: fixture_shutdown,
            reinitialize: Some(fixture_reinitialize),
        };
    }
    TB_OK
}

unsafe extern "C" fn blocked_fixture_init(
    host: *const TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    let started = INITIALIZER_STARTED
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .take();
    let release = INITIALIZER_RELEASE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .take();
    let Some((started, release)) = started.zip(release) else {
        return TB_BAD_ARGUMENT;
    };
    let _ = started.send(());
    if release.recv().is_err() {
        return TB_BAD_ARGUMENT;
    }
    unsafe { fixture_init(host, out) }
}

struct Quiet;
impl FailureSink for Quiet {
    fn capture(&self, _: &crate::module::failure::FailureReport<'_>) {}
}

struct FixtureAccess {
    _host: Arc<ModuleHost>,
    connection: Connection,
    broker_task: tokio::task::JoinHandle<crate::Result<()>>,
    reporter: FailureReporter,
    registered: bool,
    enabled: bool,
    unattested_service: Option<Connection>,
    use_unattested_proxy: bool,
    /// The retry scenario holds a concurrent first call inside initialization.
    skip_ready_wait: bool,
    member_proxy_timeout: StdMutex<Option<Duration>>,
    forced_reinitialize_failure: Option<ReconfigureFailure>,
    proxy_failure: Option<ProxyFailure>,
    initializing_left: StdMutex<usize>,
    reinitialize_calls: Arc<AtomicUsize>,
    fault_calls: Arc<AtomicUsize>,
    initializing_notifier: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
    initializing_gate: StdMutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    ensure_loaded_calls: AtomicUsize,
    fail_ensure_loaded_at: Option<usize>,
}

impl Drop for FixtureAccess {
    fn drop(&mut self) {
        self.broker_task.abort();
    }
}

impl ModuleAccess for FixtureAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (self.registered && id == ID).then_some(&RECORD)
    }
    fn enabled(&self) -> bool {
        self.enabled
    }
    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }
    fn ensure_loaded<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let call = self.ensure_loaded_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_ensure_loaded_at == Some(call) {
                self.reporter
                    .report(ID, RECORD.version, Reason::ResolutionFailed);
                return Err("module resolution failed".to_owned());
            }
            if self.skip_ready_wait {
                return Ok(());
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    if self._host.list().iter().any(|module| {
                        matches!(&module.state, ModuleState::Ready | ModuleState::Serving)
                    }) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| "fixture module did not become ready".to_owned())?;
            Ok(())
        })
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
            let destination = if self.use_unattested_proxy {
                UNATTESTED_BUS
            } else {
                bus_name
            };
            let proxy = self
                .connection
                .proxy(destination, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)?;
            Ok(self
                .member_proxy_timeout
                .lock()
                .expect("member proxy timeout lock")
                .take()
                .map_or(proxy.clone(), |timeout| proxy.with_timeout(timeout)))
        })
    }
    fn reinitialize<'a>(
        &'a self,
        id: &'a str,
        config: serde_json::Value,
    ) -> AccessFuture<'a, Result<(), ReconfigureFailure>> {
        Box::pin(async move {
            self.reinitialize_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(failure) = self.forced_reinitialize_failure {
                return Err(failure);
            }
            let return_initializing = {
                let mut left = self
                    .initializing_left
                    .lock()
                    .expect("initializing attempt lock");
                if *left > 0 {
                    *left -= 1;
                    true
                } else {
                    false
                }
            };
            if return_initializing {
                if let Some(notifier) = self
                    .initializing_notifier
                    .lock()
                    .expect("initializing notifier lock")
                    .take()
                {
                    let _ = notifier.send(());
                }
                return Err(ReconfigureFailure::Initializing);
            }
            let outcome = self
                .connection
                .reinitialize_module(id, config)
                .await
                .map(|_| ())
                .map_err(|error| crate::module::resolve::access::reconfigure_failure(&error));
            if outcome == Err(ReconfigureFailure::Initializing) {
                if let Some(notifier) = self
                    .initializing_notifier
                    .lock()
                    .expect("initializing notifier lock")
                    .take()
                {
                    let _ = notifier.send(());
                }
                let gate = self
                    .initializing_gate
                    .lock()
                    .expect("initializing gate lock")
                    .take();
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
            }
            outcome
        })
    }

    fn mark_faulted(&self, _: &str) {
        self.fault_calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn fixture_manifest() -> ModuleManifest {
    ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: ID.to_owned(),
            version: crate::version::Version::new(0, 0, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: BusName::new(BUS).unwrap(),
        object_path: ObjectPath::new(PATH).unwrap(),
        provides: vec![],
        requires: vec![],
        environment: vec![],
        capabilities: vec![],
        lazy_init: false,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    }
}

async fn fixture() -> FixtureAccess {
    fixture_with_sink(Arc::new(Quiet)).await
}

async fn fixture_with_sink(sink: Arc<dyn FailureSink>) -> FixtureAccess {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    host.attach_linked_with_config(
        crate::module::LinkedModule {
            descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
            manifest: fixture_manifest(),
            init: fixture_init,
            init_with_setup: None,
        },
        serde_json::Value::Null,
    )
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    FixtureAccess {
        _host: host,
        connection,
        broker_task,
        reporter: FailureReporter::new(sink),
        registered: true,
        enabled: true,
        unattested_service: None,
        use_unattested_proxy: false,
        skip_ready_wait: false,
        member_proxy_timeout: StdMutex::new(None),
        forced_reinitialize_failure: None,
        proxy_failure: None,
        initializing_left: StdMutex::new(0),
        reinitialize_calls: Arc::new(AtomicUsize::new(0)),
        fault_calls: Arc::new(AtomicUsize::new(0)),
        initializing_notifier: StdMutex::new(None),
        initializing_gate: StdMutex::new(None),
        ensure_loaded_calls: AtomicUsize::new(0),
        fail_ensure_loaded_at: None,
    }
}

async fn blocked_member_fixture(
    timeout: Option<Duration>,
) -> (
    Arc<ReconfigurableModule>,
    Connection,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
) {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(1);
    *BLOCKED_MEMBER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("blocked member fixture lock") = Some(BlockedMember {
        entered: entered_tx,
        release: release_rx,
        finished: finished_tx,
    });
    MEMBER_CONFIG_CALLS.store(0, Ordering::SeqCst);
    let access = fixture().await;
    ModuleAccess::ensure_loaded(&access, ID)
        .await
        .expect("linked fixture reaches serving before the controlled call");
    *access
        .member_proxy_timeout
        .lock()
        .expect("member proxy timeout lock") = timeout;
    let connection = access.connection.clone();
    let module = Arc::new(
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure")),
    );
    (module, connection, entered_rx, release_tx, finished_rx)
}

async fn lazy_fixture(
    block_initialization: bool,
) -> (
    Arc<ModuleHost>,
    Connection,
    tokio::task::JoinHandle<crate::Result<()>>,
) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    let mut manifest = fixture_manifest();
    manifest.lazy_init = true;
    host.attach_linked_with_config(
        crate::module::LinkedModule {
            descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
            manifest,
            init: if block_initialization {
                blocked_fixture_init
            } else {
                fixture_init
            },
            init_with_setup: None,
        },
        serde_json::Value::Null,
    )
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (host, connection, broker_task)
}

#[tokio::test]
async fn reinitialize_rejects_initializing_but_updates_deferred_lazy_config() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    *INITIALIZER_STARTED
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(started_tx);
    *INITIALIZER_RELEASE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(release_rx);
    let (host, connection, broker_task) = lazy_fixture(true).await;
    let proxy = connection.proxy(BUS, PATH, BUS).unwrap();
    let in_flight =
        tokio::spawn(async move { proxy.call::<serde_json::Value>("ReadConfig", ()).await });
    tokio::task::spawn_blocking(move || started_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let initializing = connection
        .reinitialize_module(ID, serde_json::json!({ "route": "new" }))
        .await
        .unwrap_err();
    assert!(matches!(
        initializing,
        crate::Error::ModuleUnavailable { ref state, .. }
            if state == "initializing"
    ));
    release_tx.send(()).unwrap();
    in_flight.await.unwrap().unwrap();
    broker_task.abort();
    drop(host);

    let (host, connection, broker_task) = lazy_fixture(false).await;
    let resolved = connection
        .reinitialize_module(ID, serde_json::json!({ "route": "new" }))
        .await
        .unwrap();
    assert_eq!(resolved.state, ModuleState::Resolved);
    let proxy = connection.proxy(BUS, PATH, BUS).unwrap();
    assert_eq!(
        proxy
            .call::<serde_json::Value>("ReadConfig", ())
            .await
            .unwrap(),
        serde_json::json!({ "route": "new" })
    );
    broker_task.abort();
    drop(host);
}

#[tokio::test]
async fn reconfiguration_retries_a_wire_initializing_result_after_the_linked_module_is_ready() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    *INITIALIZER_STARTED
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(started_tx);
    *INITIALIZER_RELEASE
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = Some(release_rx);
    let (host, connection, broker_task) = lazy_fixture(true).await;
    let initial_proxy = connection.proxy(BUS, PATH, BUS).unwrap();
    let in_flight = tokio::spawn(async move {
        initial_proxy
            .call::<serde_json::Value>("ReadConfig", ())
            .await
    });
    tokio::task::spawn_blocking(move || started_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let (notifying_tx, notifying_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let access = FixtureAccess {
        _host: host,
        connection,
        broker_task,
        reporter: FailureReporter::new(Arc::new(Quiet)),
        registered: true,
        enabled: true,
        unattested_service: None,
        use_unattested_proxy: false,
        skip_ready_wait: true,
        member_proxy_timeout: StdMutex::new(None),
        forced_reinitialize_failure: None,
        proxy_failure: None,
        initializing_left: StdMutex::new(0),
        reinitialize_calls: calls.clone(),
        fault_calls: Arc::new(AtomicUsize::new(0)),
        initializing_notifier: StdMutex::new(Some(notifying_tx)),
        initializing_gate: StdMutex::new(Some(gate_rx)),
        ensure_loaded_calls: AtomicUsize::new(0),
        fail_ensure_loaded_at: None,
    };
    let module =
        Arc::new(ReconfigurableModule::new(access, ID).retry_while_initializing(2, Duration::ZERO));
    let config = serde_json::json!({ "route": "after-init" });
    let retrying_module = module.clone();
    let retrying_config = config.clone();
    let retrying = tokio::spawn(async move { retrying_module.proxy(&retrying_config).await });
    notifying_rx.await.unwrap();
    release_tx.send(()).unwrap();
    in_flight.await.unwrap().unwrap();
    gate_tx.send(()).unwrap();

    let proxy = retrying.await.unwrap().unwrap();
    assert_eq!(read_config(&proxy).await, config);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

struct UnattestedService(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl crate::Interface for UnattestedService {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(BUS).unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Configure").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::Value::Null)
    }
}

async fn fixture_with_unattested_destination() -> (FixtureAccess, Arc<AtomicUsize>) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    host.attach_linked_with_config(
        crate::module::LinkedModule {
            descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
            manifest: fixture_manifest(),
            init: fixture_init,
            init_with_setup: None,
        },
        serde_json::Value::Null,
    )
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let unattested_service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    unattested_service
        .request_name(UNATTESTED_BUS)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    unattested_service
        .serve_at(
            ObjectPath::new(PATH).unwrap(),
            UnattestedService(calls.clone()),
        )
        .await
        .unwrap();
    (
        FixtureAccess {
            _host: host,
            connection,
            broker_task,
            reporter: FailureReporter::new(Arc::new(Quiet)),
            registered: true,
            enabled: true,
            unattested_service: Some(unattested_service),
            use_unattested_proxy: true,
            skip_ready_wait: false,
            member_proxy_timeout: StdMutex::new(None),
            forced_reinitialize_failure: None,
            proxy_failure: None,
            initializing_left: StdMutex::new(0),
            reinitialize_calls: Arc::new(AtomicUsize::new(0)),
            fault_calls: Arc::new(AtomicUsize::new(0)),
            initializing_notifier: StdMutex::new(None),
            initializing_gate: StdMutex::new(None),
            ensure_loaded_calls: AtomicUsize::new(0),
            fail_ensure_loaded_at: None,
        },
        calls,
    )
}

async fn read_config(proxy: &Proxy) -> serde_json::Value {
    proxy.call("ReadConfig", ()).await.unwrap()
}

#[test]
fn fingerprints_retain_only_a_digest() {
    let config = serde_json::json!({ "token": "sensitive-value" });
    let fingerprint = ConfigFingerprint::of(&config).unwrap();
    assert_eq!(fingerprint.0.len(), 32);
    assert!(fingerprint == ConfigFingerprint::of(&config).unwrap());
    assert!(fingerprint != ConfigFingerprint::of(&serde_json::json!({})).unwrap());
}

#[tokio::test]
async fn changed_configuration_reinitializes_and_equal_configuration_does_not() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    let first = serde_json::json!({ "route": "one" });
    let proxy = module.proxy(&first).await.unwrap();
    assert_eq!(read_config(&proxy).await, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let same = module
        .proxy(&serde_json::json!({ "route": "one" }))
        .await
        .unwrap();
    assert_eq!(
        read_config(&same).await,
        serde_json::json!({ "route": "one" })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let changed = serde_json::json!({ "route": "two" });
    let proxy = module.proxy(&changed).await.unwrap();
    assert_eq!(read_config(&proxy).await, changed);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recorded_load_configuration_is_reused_until_invalidated() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    module.record_loaded_with(&serde_json::Value::Null).unwrap();
    let proxy = module.proxy(&serde_json::Value::Null).await.unwrap();
    assert_eq!(read_config(&proxy).await, serde_json::Value::Null);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    module.invalidate();
    module.proxy(&serde_json::Value::Null).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_change_forgets_the_old_fingerprint_before_a_revert() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    let original = serde_json::json!({ "route": "original" });
    module.proxy(&original).await.unwrap();
    let rejected = serde_json::json!({ "reject": true });
    assert_eq!(
        module.proxy(&rejected).await.unwrap_err(),
        ModuleCallError::ModuleFault
    );
    let reverted = module.proxy(&original).await.unwrap();
    assert_eq!(read_config(&reverted).await, original);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn reconfiguration_failure_reports_only_registry_identity_and_closed_reason() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let sink = Arc::new(RecordingSink::default());
    let mut access = fixture_with_sink(sink.clone()).await;
    access.forced_reinitialize_failure = Some(ReconfigureFailure::Failed);
    let module = ReconfigurableModule::new(access, ID);
    let config = serde_json::json!({ "credential": "private-test-value" });

    assert_eq!(
        module.proxy(&config).await.unwrap_err(),
        ModuleCallError::ModuleFault
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(ID.to_owned(), "0.0.0".to_owned(), Reason::ModuleFault)]
    );
}

#[tokio::test]
async fn missing_and_disabled_reconfiguration_report_closed_registry_reasons() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let missing_sink = Arc::new(RecordingSink::default());
    let mut missing = fixture_with_sink(missing_sink.clone()).await;
    missing.registered = false;
    let missing = ReconfigurableModule::new(missing, ID);
    assert_eq!(
        missing.proxy(&serde_json::Value::Null).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        missing_sink.0.lock().unwrap().as_slice(),
        &[(
            "unregistered".to_owned(),
            "unknown".to_owned(),
            Reason::UnknownModule
        )]
    );

    let disabled_sink = Arc::new(RecordingSink::default());
    let mut disabled = fixture_with_sink(disabled_sink.clone()).await;
    disabled.enabled = false;
    let disabled = ReconfigurableModule::new(disabled, ID);
    assert_eq!(
        disabled.proxy(&serde_json::Value::Null).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        disabled_sink.0.lock().unwrap().as_slice(),
        &[(ID.to_owned(), "0.0.0".to_owned(), Reason::Disabled)]
    );
}

#[tokio::test]
async fn proxy_failure_after_configuration_reports_transport_failure() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let sink = Arc::new(RecordingSink::default());
    let mut access = fixture_with_sink(sink.clone()).await;
    access.proxy_failure = Some(ProxyFailure::Transport);
    let module = ReconfigurableModule::new(access, ID);

    assert_eq!(
        module
            .proxy(&serde_json::json!({ "route": "transport-failure" }))
            .await
            .unwrap_err(),
        ModuleCallError::TransportFailed
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(ID.to_owned(), "0.0.0".to_owned(), Reason::TransportFailed)]
    );
}

#[tokio::test]
async fn resolver_load_failure_is_reported_once_with_its_resolution_reason() {
    let sink = Arc::new(RecordingSink::default());
    let reporter = Arc::new(FailureReporter::new(sink.clone()));
    let resolver = crate::module::Resolver::new(crate::module::ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: std::collections::HashMap::new(),
        linked: Arc::new(crate::module::LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&FAILED_RESOLVER_RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(|| {
            Box::pin(async { Err(crate::Error::transport("runtime not configured")) })
        }),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter,
    });
    let module = ReconfigurableModule::new(resolver, FAILED_RESOLVER_RECORD.id);

    assert_eq!(
        module.proxy(&serde_json::Value::Null).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(
            FAILED_RESOLVER_RECORD.id.to_owned(),
            FAILED_RESOLVER_RECORD.version.to_owned(),
            Reason::ResolutionFailed,
        )]
    );
}

#[tokio::test]
async fn resolver_reconfigures_a_lazy_linked_module_before_its_first_call() {
    let runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
        crate::module::ModuleRuntime::start(&crate::module::RuntimeNames::default(), None).unwrap(),
    ));
    let mut manifest = fixture_manifest();
    manifest.lazy_init = true;
    runtime
        .host()
        .attach_linked_with_config(
            crate::module::LinkedModule {
                descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
                manifest,
                init: fixture_init,
                init_with_setup: None,
            },
            serde_json::Value::Null,
        )
        .unwrap();
    let resolver = crate::module::Resolver::new(crate::module::ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: std::collections::HashMap::new(),
        linked: Arc::new(crate::module::LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(Arc::new(Quiet))),
    });
    resolver.ensure_loaded(ID).await.unwrap();
    assert_eq!(runtime.host().list()[0].state, ModuleState::Resolved);
    let module = ReconfigurableModule::new(resolver, ID);
    let config = serde_json::json!({ "credential": "private-before-first-call" });

    let proxy = module
        .proxy(&config)
        .await
        .expect("lazy configuration should be applied before the first business call");
    assert_eq!(read_config(&proxy).await, config);
}

#[tokio::test]
async fn member_second_load_failure_preserves_resolution_report_reason() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let sink = Arc::new(RecordingSink::default());
    let mut access = fixture_with_sink(sink.clone()).await;
    access.fail_ensure_loaded_at = Some(2);
    let module =
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure"));

    assert_eq!(
        module
            .proxy(&serde_json::json!({ "route": "second-load" }))
            .await
            .unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(
            ID.to_owned(),
            RECORD.version.to_owned(),
            Reason::ResolutionFailed
        )]
    );
}

#[tokio::test]
async fn member_strategy_applies_config_through_the_linked_fixture_bus() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module =
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure"));
    let config = serde_json::json!({ "route": "member" });
    let proxy = module.proxy(&config).await.unwrap();
    assert_eq!(read_config(&proxy).await, config);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn canceled_member_reconfigure_fences_late_completion_and_cannot_be_reopened() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let (module, observer, entered, release, finished) = blocked_member_fixture(None).await;
    let first_config = serde_json::json!({ "route": "first", "hold_member": true });
    let first_module = module.clone();
    let first = tokio::spawn(async move { first_module.proxy(&first_config).await });
    tokio::task::spawn_blocking(move || entered.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second_config = serde_json::json!({ "route": "second" });
    assert_eq!(
        module.proxy(&second_config).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);

    module.invalidate();
    module.record_loaded_with(&second_config).unwrap();
    assert_eq!(
        module.proxy(&second_config).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);

    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || finished.recv())
        .await
        .unwrap()
        .unwrap();
    let raw_proxy = observer.proxy(BUS, PATH, BUS).unwrap();
    assert_eq!(
        raw_proxy
            .call::<serde_json::Value>("ReadConfig", ())
            .await
            .unwrap(),
        serde_json::json!({ "route": "first", "hold_member": true })
    );
    assert_eq!(
        module.proxy(&second_config).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn timed_out_member_reconfigure_fences_late_completion() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let (module, observer, entered, release, finished) =
        blocked_member_fixture(Some(Duration::from_millis(20))).await;
    let first_config = serde_json::json!({ "route": "timed-out", "hold_member": true });
    let first_module = module.clone();
    let first = tokio::spawn(async move { first_module.proxy(&first_config).await });
    tokio::task::spawn_blocking(move || entered.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(
        first.await.unwrap().unwrap_err(),
        ModuleCallError::ModuleFault
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);

    let second_config = serde_json::json!({ "route": "after-timeout" });
    assert_eq!(
        module.proxy(&second_config).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);

    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || finished.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::resume();
    let raw_proxy = observer.proxy(BUS, PATH, BUS).unwrap();
    assert_eq!(
        raw_proxy
            .call::<serde_json::Value>("ReadConfig", ())
            .await
            .unwrap(),
        serde_json::json!({ "route": "timed-out", "hold_member": true })
    );
    assert_eq!(
        module.proxy(&second_config).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(MEMBER_CONFIG_CALLS.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn member_strategy_refuses_an_unattested_recipient_before_delivery() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let (access, calls) = fixture_with_unattested_destination().await;
    assert!(access.unattested_service.is_some());
    let module =
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure"));
    let result = module
        .proxy(&serde_json::json!({ "token": "private" }))
        .await;
    assert_eq!(result.unwrap_err(), ModuleCallError::ModuleFault);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_exclusive_guard_keeps_the_config_stable_until_the_bus_call_finishes() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let module = Arc::new(ReconfigurableModule::new(access, ID));
    let first = serde_json::json!({ "route": "first" });
    let second = serde_json::json!({ "route": "second" });
    let (proxy_ready_tx, proxy_ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let first_module = module.clone();
    let first_config = first.clone();
    let first_task = tokio::spawn(async move {
        let mut guard = first_module.exclusive().await;
        let proxy = guard.proxy(&first_config).await.unwrap();
        proxy_ready_tx.send(()).unwrap();
        let _ = release_rx.await;
        read_config(&proxy).await
    });
    proxy_ready_rx.await.unwrap();
    let second_module = module.clone();
    let second_config = second.clone();
    let second_task =
        tokio::spawn(async move { second_module.proxy(&second_config).await.unwrap() });
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();
    assert_eq!(first_task.await.unwrap(), first);
    let second_proxy = second_task.await.unwrap();
    assert_eq!(read_config(&second_proxy).await, second);
}

#[path = "reconfigure_policy_tests.rs"]
mod policy;
