//! Ordinary module bindings retain their resolved peer without native proof.

use super::*;
use crate::broker::Broker;
use crate::module::failure::{FailureReport, FailureSink};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Interface, InterfaceName, MemberName, ObjectPath, StreamRef};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "ordinary-pinned-client-test",
    bus_name: "ai.tinyhumans.test.OrdinaryPinnedClient",
    object_path: "/ai/tinyhumans/test/OrdinaryPinnedClient",
    version: "0.3.3",
    release_url: "https://example.invalid/tinybus",
    // Ordinary fixtures and linked host factories intentionally have no
    // release archive/library pair to attest.
    assets: &[],
};

const REMOTE_DETAIL: &str = "private ordinary-call detail: fixture-token-7319";

struct DiscardReports;

impl FailureSink for DiscardReports {
    fn capture(&self, _: &FailureReport<'_>) {}
}

fn reporter() -> FailureReporter {
    FailureReporter::new(Arc::new(DiscardReports))
}

#[derive(Default)]
struct RecordedReports(Mutex<Vec<Reason>>);

impl FailureSink for RecordedReports {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0.lock().unwrap().push(report.reason);
    }
}

struct Access {
    connection: Connection,
    reporter: FailureReporter,
    enabled: bool,
}

impl ModuleAccess for Access {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        self.enabled
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
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
}

#[derive(Clone)]
struct Peer {
    label: &'static str,
    connection: Connection,
    received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Interface for Peer {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        ["Who", "Upload", "Read", "Release", "Fail", "Hang"]
            .map(|member| MemberName::new(member).unwrap())
            .to_vec()
    }

    async fn call(&self, member: &MemberName, args: Value) -> crate::Result<Value> {
        self.events.lock().unwrap().push(member.as_str().to_owned());
        match member.as_str() {
            "Who" | "Read" | "Release" => Ok(serde_json::json!(self.label)),
            "Upload" => {
                let (stream,): (StreamRef,) = serde_json::from_value(args)?;
                let bytes = self.connection.read_stream(&stream).await?;
                self.received.send(bytes.clone()).unwrap();
                Ok(serde_json::json!(self.label))
            }
            "Fail" => Err(crate::Error::MethodFailed {
                name: "ai.tinyhumans.docs.Error.Rejected".to_owned(),
                message: REMOTE_DETAIL.to_owned(),
            }),
            "Hang" => std::future::pending().await,
            _ => Err(crate::Error::failed("unexpected test method")),
        }
    }
}

#[derive(Clone)]
struct HandoverArguments {
    stream: StreamRef,
    started: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    barrier: Arc<std::sync::Barrier>,
}

impl serde::Serialize for HandoverArguments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        // Delay method argument construction after the stream has opened, then
        // reassign the public alias before the upload method is sent.
        self.barrier.wait();
        serde::Serialize::serialize(&self.stream, serializer)
    }
}

async fn peer(
    bus: &MemoryBus,
    label: &'static str,
    received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    events: Arc<Mutex<Vec<String>>>,
) -> Connection {
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    for path in [RECORD.object_path, "/child"] {
        connection
            .serve_at(
                ObjectPath::new(path).unwrap(),
                Peer {
                    label,
                    connection: connection.clone(),
                    received: received.clone(),
                    events: events.clone(),
                },
            )
            .await
            .unwrap();
    }
    connection
}

#[tokio::test]
async fn an_ordinary_fixture_with_no_release_assets_can_bind() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(&bus, "fixture", received, Arc::new(Mutex::new(Vec::new()))).await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy);

    let pinned = client.bind(RECORD.id).await.unwrap();
    let response: String = pinned.call("Who", ()).await.unwrap();
    assert_eq!(response, "fixture");
}

#[cfg(feature = "modules")]
struct LinkedOrdinaryContext {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_deliver(
    context: *mut std::ffi::c_void,
    bytes: *const u8,
    len: usize,
) -> i32 {
    let context = unsafe { &*(context.cast::<LinkedOrdinaryContext>()) };
    let bytes = unsafe { std::slice::from_raw_parts(bytes, len) }.to_vec();
    context
        .sender
        .send(bytes)
        .map_or(-1, |_| crate::module::abi::TB_OK)
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_shutdown(context: *mut std::ffi::c_void, _: u64) -> i32 {
    drop(unsafe { Box::from_raw(context.cast::<LinkedOrdinaryContext>()) });
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_reinitialize(
    _: *mut std::ffi::c_void,
    _: *const u8,
    _: usize,
) -> i32 {
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut crate::module::abi::TbModuleVtable,
) -> i32 {
    let host = unsafe { *host };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let context = Box::into_raw(Box::new(LinkedOrdinaryContext { sender }));
    unsafe {
        *out = crate::module::abi::TbModuleVtable {
            size: std::mem::size_of::<crate::module::abi::TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: context.cast(),
            deliver: linked_ordinary_deliver,
            shutdown: linked_ordinary_shutdown,
            reinitialize: Some(linked_ordinary_reinitialize),
        };
    }
    let host_context = host.host_ctx as usize;
    let send = host.send;
    std::thread::spawn(move || {
        while let Some(bytes) = receiver.blocking_recv() {
            let Ok(message) = serde_json::from_slice::<crate::Message>(&bytes) else {
                continue;
            };
            if message.header.kind != crate::message::MessageKind::MethodCall {
                continue;
            }
            let value = message
                .body
                .as_array()
                .and_then(|values| values.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let reply = crate::Message::method_return(&message.header, value);
            let bytes = serde_json::to_vec(&reply).unwrap();
            let _ = unsafe {
                send(
                    host_context as *mut std::ffi::c_void,
                    bytes.as_ptr(),
                    bytes.len(),
                )
            };
        }
    });
    unsafe { (host.ready)(host.host_ctx) };
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
fn linked_ordinary_factory() -> crate::Result<crate::module::LinkedModule> {
    use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy};
    use crate::{BusName, ObjectPath, Version};

    Ok(crate::module::LinkedModule {
        descriptor: crate::module::abi::TbAbiDescriptor::current(RECORD.id, RECORD.version),
        manifest: crate::module::manifest::ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: RECORD.id.to_owned(),
                version: Version::new(0, 3, 3),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(RECORD.bus_name).unwrap(),
            object_path: ObjectPath::new(RECORD.object_path).unwrap(),
            provides: Vec::new(),
            requires: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        },
        init: linked_ordinary_init,
        init_with_setup: None,
    })
}

#[cfg(feature = "modules")]
static RESOLVER_LINKED_STARTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "modules")]
static RESOLVER_LINKED_DELIVERIES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "modules")]
static RESOLVER_LINKED_FACTORY_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "modules")]
static RESOLVER_LINKED_GATE: std::sync::OnceLock<Mutex<Option<std::sync::mpsc::Receiver<()>>>> =
    std::sync::OnceLock::new();

#[cfg(feature = "modules")]
static RESOLVER_LINKED_STARTED: std::sync::OnceLock<
    Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
> = std::sync::OnceLock::new();

#[cfg(feature = "modules")]
static RESOLVER_LINKED_RECORD: ReleaseRecord = ReleaseRecord {
    id: "ordinary-binding-linked-readiness",
    bus_name: "ai.tinyhumans.test.OrdinaryBindingLinkedReadiness",
    object_path: "/ai/tinyhumans/test/OrdinaryBindingLinkedReadiness",
    version: "0.3.3",
    release_url: "https://example.invalid/tinybus",
    assets: &[],
};

#[cfg(feature = "modules")]
unsafe extern "C" fn resolver_linked_deliver(
    _: *mut std::ffi::c_void,
    _: *const u8,
    _: usize,
) -> i32 {
    RESOLVER_LINKED_DELIVERIES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn resolver_linked_shutdown(context: *mut std::ffi::c_void, _: u64) -> i32 {
    drop(unsafe { Box::from_raw(context.cast::<u8>()) });
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn resolver_linked_reinitialize(
    _: *mut std::ffi::c_void,
    _: *const u8,
    _: usize,
) -> i32 {
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn resolver_linked_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut crate::module::abi::TbModuleVtable,
) -> i32 {
    RESOLVER_LINKED_STARTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let host = unsafe { *host };
    let context = Box::into_raw(Box::new(0_u8));
    unsafe {
        *out = crate::module::abi::TbModuleVtable {
            size: std::mem::size_of::<crate::module::abi::TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: context.cast(),
            deliver: resolver_linked_deliver,
            shutdown: resolver_linked_shutdown,
            reinitialize: Some(resolver_linked_reinitialize),
        };
    }
    if let Some(started) = RESOLVER_LINKED_STARTED
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .take()
    {
        let _ = started.send(());
    }
    if let Some(gate) = RESOLVER_LINKED_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .take()
    {
        let _ = gate.recv();
    }
    unsafe { (host.ready)(host.host_ctx) };
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
fn resolver_linked_factory() -> crate::Result<crate::module::LinkedModule> {
    use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy};
    use crate::{BusName, ObjectPath, Version};

    RESOLVER_LINKED_FACTORY_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let record = &RESOLVER_LINKED_RECORD;
    Ok(crate::module::LinkedModule {
        descriptor: crate::module::abi::TbAbiDescriptor::current(record.id, record.version),
        manifest: crate::module::manifest::ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: record.id.to_owned(),
                version: Version::new(0, 3, 3),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(record.bus_name).unwrap(),
            object_path: ObjectPath::new(record.object_path).unwrap(),
            provides: Vec::new(),
            requires: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        },
        init: resolver_linked_init,
        init_with_setup: None,
    })
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn ordinary_binding_timeout_keeps_resolver_owned_initialization_for_retry() {
    use crate::module::runtime::{ModuleRuntime, RuntimeNames};
    use crate::module::{Availability, LinkedRegistry, ModuleSource, Resolver, ResolverInputs};
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Warm the executable digest through a real linked admission on a distinct
    // record. The resolver case below is about preserving cold ModuleRuntime
    // startup across caller teardown; it must not also budget cold executable
    // hashing, which can be slow on hosted coverage machines.
    let digest_warm_runtime =
        ModuleRuntime::start(&RuntimeNames::default(), None).expect("digest warmup runtime starts");
    digest_warm_runtime
        .host()
        .attach_linked_with_config(
            linked_ordinary_factory().expect("digest warmup linked factory succeeds"),
            serde_json::Value::Null,
        )
        .expect("a real linked admission warms the executable digest");

    RESOLVER_LINKED_STARTS.store(0, Ordering::SeqCst);
    RESOLVER_LINKED_DELIVERIES.store(0, Ordering::SeqCst);
    RESOLVER_LINKED_FACTORY_CALLS.store(0, Ordering::SeqCst);
    let (release, gate) = std::sync::mpsc::channel();
    *RESOLVER_LINKED_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some(gate);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    *RESOLVER_LINKED_STARTED
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap() = Some(started);

    static RUNTIME: OnceLock<crate::module::ModuleRuntime> = OnceLock::new();
    let runtime_provider_started = Arc::new(tokio::sync::Notify::new());
    let runtime_provider_gate = Arc::new(tokio::sync::Notify::new());
    let first_runtime_request = Arc::new(AtomicBool::new(true));
    let runtime_provider_calls = Arc::new(AtomicUsize::new(0));
    let started_runtime_provider = runtime_provider_started.clone();
    let release_runtime_provider = runtime_provider_gate.clone();
    let first_runtime_request_for_provider = first_runtime_request.clone();
    let runtime_provider_calls_for_provider = runtime_provider_calls.clone();
    let linked = Arc::new(LinkedRegistry::new());
    linked.register(RESOLVER_LINKED_RECORD.id, resolver_linked_factory);
    let reports = Arc::new(RecordedReports::default());
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::from([(RESOLVER_LINKED_RECORD.id.to_owned(), ModuleSource::Linked)]),
        linked,
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RESOLVER_LINKED_RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(move || {
            runtime_provider_calls_for_provider.fetch_add(1, Ordering::SeqCst);
            let first = first_runtime_request_for_provider.swap(false, Ordering::SeqCst);
            let started = started_runtime_provider.clone();
            let gate = release_runtime_provider.clone();
            Box::pin(async move {
                if first {
                    started.notify_one();
                    gate.notified().await;
                }
                Ok(RUNTIME.get_or_init(|| {
                    ModuleRuntime::start(&RuntimeNames::default(), None)
                        .expect("test runtime starts after provider gate opens")
                }))
            })
        }),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(reports.clone())),
    });

    let proxy_calls = Arc::new(AtomicUsize::new(0));
    struct CountedResolverAccess {
        resolver: Resolver,
        proxy_calls: Arc<AtomicUsize>,
    }
    impl ModuleAccess for CountedResolverAccess {
        fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
            self.resolver.find(id)
        }

        fn enabled(&self) -> bool {
            self.resolver.enabled()
        }

        fn reporter(&self) -> &FailureReporter {
            self.resolver.reporter()
        }

        fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
            Box::pin(self.resolver.ensure_loaded(id))
        }

        fn ensure_loaded_within<'a>(
            &'a self,
            id: &'a str,
            within: Option<std::time::Duration>,
        ) -> AccessFuture<'a, Result<(), crate::module::ReadinessFailure>> {
            Box::pin(ModuleAccess::ensure_loaded_within(
                &self.resolver,
                id,
                within,
            ))
        }

        fn proxy<'a>(
            &'a self,
            bus_name: &'a str,
            object_path: &'a str,
        ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
            let calls = Arc::clone(&self.proxy_calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                ModuleAccess::proxy(&self.resolver, bus_name, object_path).await
            })
        }
    }

    let client = ModuleClient::new(CountedResolverAccess {
        resolver: resolver.clone(),
        proxy_calls: proxy_calls.clone(),
    })
    .with_timeout(std::time::Duration::from_millis(50));
    let runtime_provider_started_for_runtime = runtime_provider_started.clone();
    let client_for_abandoned_runtime = client.clone();
    let (runtime_dropped_tx, runtime_dropped_rx) = tokio::sync::oneshot::channel();
    let abandoned_runtime = std::thread::spawn(move || {
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("temporary caller runtime starts");
        let bind = caller_runtime.spawn(async move {
            client_for_abandoned_runtime
                .bind(RESOLVER_LINKED_RECORD.id)
                .await
        });
        caller_runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                runtime_provider_started_for_runtime.notified(),
            )
            .await
            .expect("the first runtime-provider request should enter its gate");
        });
        drop(caller_runtime);
        drop(bind);
        let _ = runtime_dropped_tx.send(());
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), runtime_dropped_rx)
        .await
        .expect("the caller runtime should shut down while its provider is gated")
        .unwrap();
    abandoned_runtime.join().unwrap();
    assert_eq!(RESOLVER_LINKED_STARTS.load(Ordering::SeqCst), 0);
    assert_eq!(runtime_provider_calls.load(Ordering::SeqCst), 1);
    assert_eq!(RESOLVER_LINKED_DELIVERIES.load(Ordering::SeqCst), 0);
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);
    assert!(reports.0.lock().unwrap().is_empty());

    let linked_initializer_started = started_rx;
    runtime_provider_gate.notify_one();
    let initializer_started = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        linked_initializer_started,
    )
    .await;
    if !matches!(initializer_started, Ok(Ok(()))) {
        let runtime_worker_alive = if let Some(runtime) = RUNTIME.get() {
            let (probe_tx, probe_rx) = tokio::sync::oneshot::channel();
            runtime.spawn(async move {
                let _ = probe_tx.send(());
            });
            matches!(
                tokio::time::timeout(std::time::Duration::from_secs(1), probe_rx).await,
                Ok(Ok(()))
            )
        } else {
            false
        };
        panic!(
            "the process-owned resolver should start linked initialization; availability={:?}, runtime_provider_calls={}, linked_factory_calls={}, runtime_worker_alive={}",
            resolver.state_of(RESOLVER_LINKED_RECORD.id),
            runtime_provider_calls.load(Ordering::SeqCst),
            RESOLVER_LINKED_FACTORY_CALLS.load(Ordering::SeqCst),
            runtime_worker_alive,
        )
    }
    assert_eq!(RESOLVER_LINKED_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(RESOLVER_LINKED_FACTORY_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(runtime_provider_calls.load(Ordering::SeqCst), 3);
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);

    let client_for_second_bind = client.clone();
    let mut second_bind =
        tokio::spawn(async move { client_for_second_bind.bind(RESOLVER_LINKED_RECORD.id).await });
    let second_result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut second_bind)
        .await
        .expect("the second caller should use a bounded wait on the existing resolution")
        .unwrap();
    assert!(matches!(
        second_result,
        Err(failure) if failure.kind == ModuleCallError::TransportFailed
    ));
    assert_eq!(RESOLVER_LINKED_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(runtime_provider_calls.load(Ordering::SeqCst), 3);
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        reports.0.lock().unwrap().as_slice(),
        &[Reason::TransportFailed]
    );

    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if resolver.state_of(RESOLVER_LINKED_RECORD.id) == Availability::Ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the existing resolver attempt should finish after readiness opens");
    let pinned = client.bind(RESOLVER_LINKED_RECORD.id).await.unwrap();
    let (runtime_task_tx, runtime_task_rx) = tokio::sync::oneshot::channel();
    let runtime_task = RUNTIME
        .get()
        .expect("the gated provider should have created the module runtime")
        .spawn(async move {
            let _ = runtime_task_tx.send(7);
        });
    runtime_task.await.unwrap();
    assert_eq!(runtime_task_rx.await.unwrap(), 7);
    assert_eq!(RESOLVER_LINKED_STARTS.load(Ordering::SeqCst), 1);
    // The bind above revisits the cached `Ready` slot, which re-checks the host
    // for a module that faulted after load: one more runtime lookup.
    assert_eq!(runtime_provider_calls.load(Ordering::SeqCst), 5);
    assert_eq!(RESOLVER_LINKED_DELIVERIES.load(Ordering::SeqCst), 0);
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reports.0.lock().unwrap().as_slice(),
        &[Reason::TransportFailed]
    );
    drop(pinned);
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn ordinary_binding_accepts_a_resolver_owned_linked_factory_without_library_proof() {
    use crate::module::runtime::{ModuleRuntime, RuntimeNames};
    use crate::module::{LinkedRegistry, ModuleSource, Resolver, ResolverInputs};
    use std::collections::HashMap;
    use std::sync::OnceLock;

    static RUNTIME: OnceLock<ModuleRuntime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        ModuleRuntime::start(&RuntimeNames::default(), None).expect("test runtime starts")
    });
    let linked = Arc::new(LinkedRegistry::new());
    linked.register(RECORD.id, linked_ordinary_factory);
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::from([(RECORD.id.to_owned(), ModuleSource::Linked)]),
        linked,
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(Arc::new(DiscardReports))),
    });

    assert!(RECORD.assets.is_empty());
    let diagnostic_resolver = resolver.clone();
    let client = ModuleClient::new(resolver);
    let pinned = client.bind(RECORD.id).await.unwrap_or_else(|failure| {
        panic!(
            "ordinary linked factory binding failed ({failure:?}); resolver state: {:?}",
            diagnostic_resolver.state_of(RECORD.id)
        )
    });
    let response: String = pinned.call("Echo", "linked-host-factory").await.unwrap();
    assert_eq!(response, "linked-host-factory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn ordinary_binding_keeps_root_object_and_stream_with_the_original_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (received_a_tx, mut received_a_rx) = tokio::sync::mpsc::unbounded_channel();
    let (received_b_tx, mut received_b_rx) = tokio::sync::mpsc::unbounded_channel();
    let events_a = Arc::new(Mutex::new(Vec::new()));
    let events_b = Arc::new(Mutex::new(Vec::new()));
    let service_a = peer(&bus, "peer-a", received_a_tx, events_a.clone()).await;
    let service_b = peer(&bus, "peer-b", received_b_tx, events_b.clone()).await;
    service_a.request_name(RECORD.bus_name).await.unwrap();

    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy);
    let pinned = client.bind(RECORD.id).await.unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started_tx)));
    let call_client = pinned.clone();
    let payload = b"ordinary pinned payload".to_vec();
    let call_payload = payload.clone();
    let call_started = started.clone();
    let call_barrier = barrier.clone();
    let upload = tokio::spawn(async move {
        call_client
            .call_with_stream::<String, _, _>(
                "Upload",
                move |stream| HandoverArguments {
                    stream: stream.clone(),
                    started: call_started,
                    barrier: call_barrier,
                },
                &call_payload,
            )
            .await
    });

    started_rx.await.unwrap();
    service_a.release_name(RECORD.bus_name).await.unwrap();
    service_b.request_name(RECORD.bus_name).await.unwrap();
    barrier.wait();
    assert_eq!(upload.await.unwrap().unwrap(), "peer-a");
    assert_eq!(received_a_rx.recv().await.unwrap(), payload);
    assert!(received_b_rx.try_recv().is_err());

    // This opens a new stream after B already owns the alias, proving the
    // stream endpoint itself is selected from the bound unique destination.
    let second_payload = b"opened after handover".to_vec();
    let response: String = pinned
        .call_with_stream("Upload", |stream| (stream.clone(),), &second_payload)
        .await
        .unwrap();
    assert_eq!(response, "peer-a");
    assert_eq!(received_a_rx.recv().await.unwrap(), second_payload);
    assert!(received_b_rx.try_recv().is_err());

    let root: String = pinned.call("Who", ()).await.unwrap();
    assert_eq!(root, "peer-a");
    let object = pinned.object_client("/child").unwrap();
    let response: String = object.call("Read", ()).await.unwrap();
    assert_eq!(response, "peer-a");
    let response: String = object.call("Release", ()).await.unwrap();
    assert_eq!(response, "peer-a");
    assert_eq!(
        events_a.lock().unwrap().as_slice(),
        &["Upload", "Upload", "Who", "Read", "Release"]
    );
    assert!(events_b.lock().unwrap().is_empty());

    service_a.close().await.unwrap();
    let root_after_close: Result<String, _> = pinned.call("Who", ()).await;
    let object_after_close: Result<String, _> = object.call("Read", ()).await;
    let stream_after_close: Result<String, _> = pinned
        .call_with_stream("Upload", |stream| (stream.clone(),), b"after close")
        .await;
    assert!(root_after_close.is_err());
    assert!(object_after_close.is_err());
    assert!(stream_after_close.is_err());
    assert!(events_b.lock().unwrap().is_empty());
    assert!(received_b_rx.try_recv().is_err());
    broker_task.abort();
}

#[tokio::test]
async fn ordinary_binding_refuses_disabled_unknown_and_unowned_modules() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let disabled_reports = Arc::new(RecordedReports::default());
    let disabled = ModuleClient::new(Access {
        connection: caller.clone(),
        reporter: FailureReporter::new(disabled_reports.clone()),
        enabled: false,
    });
    let disabled_result = disabled.bind(RECORD.id).await;
    assert!(
        matches!(disabled_result, Err(failure) if failure.kind == ModuleCallError::Unavailable)
    );
    assert_eq!(
        disabled_reports.0.lock().unwrap().as_slice(),
        &[Reason::Disabled]
    );

    let unknown_reports = Arc::new(RecordedReports::default());
    let unknown = ModuleClient::new(Access {
        connection: caller.clone(),
        reporter: FailureReporter::new(unknown_reports.clone()),
        enabled: true,
    });
    let unknown_result = unknown.bind("not-registered").await;
    assert!(matches!(unknown_result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    assert_eq!(unknown_reports.0.lock().unwrap().len(), 1);

    // The broker has no peer advertising the registered well-known name.
    let unowned_reports = Arc::new(RecordedReports::default());
    let unowned_proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let unowned = ModuleClient::new(Access {
        connection: caller,
        reporter: FailureReporter::new(unowned_reports.clone()),
        enabled: true,
    })
    .with_fixture(unowned_proxy);
    let unowned_result = unowned.bind(RECORD.id).await;
    assert!(matches!(unowned_result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    assert_eq!(
        unowned_reports.0.lock().unwrap().as_slice(),
        &[Reason::ModuleUnavailable]
    );
}

#[tokio::test]
async fn ordinary_bound_detailed_failures_keep_fields_and_sanitize_renderings() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(
        &bus,
        "failure-peer",
        received,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let pinned = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy)
    .bind(RECORD.id)
    .await
    .unwrap();

    let failure = pinned
        .call_detailed::<String>("Fail", ())
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(failure.wire_name, "ai.tinyhumans.docs.Error.Rejected");
    assert_eq!(failure.message, REMOTE_DETAIL);
    assert!(!format!("{failure:?}").contains(REMOTE_DETAIL));
    assert!(!failure.to_string().contains(REMOTE_DETAIL));
}

#[tokio::test(start_paused = true)]
async fn ordinary_bound_calls_keep_the_configured_deadline_and_report_timeout() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(
        &bus,
        "slow-peer",
        received,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let reports = Arc::new(RecordedReports::default());
    let pinned = ModuleClient::new(Access {
        connection: caller,
        reporter: FailureReporter::new(reports.clone()),
        enabled: true,
    })
    .with_fixture(proxy.with_timeout(std::time::Duration::from_secs(2)))
    .with_timeout(std::time::Duration::from_millis(20))
    .bind(RECORD.id)
    .await
    .unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(21),
        pinned.call::<String>("Hang", ()),
    )
    .await;
    assert!(matches!(result, Ok(Err(ModuleCallError::TransportFailed))));
    assert_eq!(
        reports.0.lock().unwrap().as_slice(),
        &[Reason::TransportFailed]
    );
}

#[tokio::test]
async fn ordinary_binding_preserves_fixture_proxy_deadlines_for_root_and_objects() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(
        &bus,
        "deadline-peer",
        received,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap()
        .with_timeout(std::time::Duration::from_millis(20));
    let pinned = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy)
    .bind(RECORD.id)
    .await
    .unwrap();

    let root_result = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        pinned.call::<String>("Hang", ()),
    )
    .await
    .expect("root calls inherit the fixture proxy deadline");
    assert!(matches!(root_result, Err(ModuleCallError::TransportFailed)));

    let object = pinned.object_client("/child").unwrap();
    let object_result = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        object.call::<String>("Hang", ()),
    )
    .await
    .expect("held-object calls inherit the fixture proxy deadline");
    assert!(matches!(
        object_result,
        Err(ModuleCallError::TransportFailed)
    ));
}

#[test]
fn unsupported_module_availability_keeps_the_unavailable_classification() {
    let error = crate::Error::ModuleUnavailable {
        module: RECORD.id.to_owned(),
        state: "unsupported".to_owned(),
        detail: "the host does not support this module source".to_owned(),
    };

    assert_eq!(
        classify(&error),
        (ModuleCallError::Unavailable, Reason::ModuleUnavailable)
    );
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn ordinary_binding_refuses_an_unsupported_resolver_without_proxy_or_business_calls() {
    use crate::module::runtime::{ModuleRuntime, RuntimeNames};
    use crate::module::{Availability, LinkedRegistry, Resolver, ResolverInputs};
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static RECORD: ReleaseRecord = ReleaseRecord {
        id: "ordinary-binding-unsupported",
        bus_name: "ai.tinyhumans.test.OrdinaryBindingUnsupported",
        object_path: "/ai/tinyhumans/test/OrdinaryBindingUnsupported",
        version: "0.3.3",
        release_url: "https://example.invalid/tinybus",
        assets: &[],
    };
    static RUNTIME: OnceLock<ModuleRuntime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        ModuleRuntime::start(&RuntimeNames::default(), None).expect("test runtime starts")
    });
    let reports = Arc::new(RecordedReports::default());
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::new(),
        linked: Arc::new(LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(reports.clone())),
    });

    assert_eq!(resolver.status_of(&RECORD).0, Availability::Unsupported);
    let proxy_calls = Arc::new(AtomicUsize::new(0));
    struct CountedAccess {
        resolver: Resolver,
        proxy_calls: Arc<AtomicUsize>,
    }
    impl ModuleAccess for CountedAccess {
        fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
            self.resolver.find(id)
        }

        fn enabled(&self) -> bool {
            self.resolver.enabled()
        }

        fn reporter(&self) -> &FailureReporter {
            self.resolver.reporter()
        }

        fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
            Box::pin(self.resolver.ensure_loaded(id))
        }

        fn proxy<'a>(
            &'a self,
            bus_name: &'a str,
            object_path: &'a str,
        ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
            let calls = Arc::clone(&self.proxy_calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                self.resolver.proxy(bus_name, object_path).await
            })
        }
    }
    let client = ModuleClient::new(CountedAccess {
        resolver: resolver.clone(),
        proxy_calls: proxy_calls.clone(),
    });

    for _ in 0..2 {
        let result = client.bind(RECORD.id).await;
        assert!(matches!(result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    }
    assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        reports.0.lock().unwrap().as_slice(),
        &[Reason::ResolutionFailed]
    );
}
