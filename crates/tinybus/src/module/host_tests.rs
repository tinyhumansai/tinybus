//! Host module tests, including raw-vtable fixtures and real-loader cases.

use super::*;
use crate::Connection;
use crate::module::abi::TbAbiDescriptor;
use crate::module::runtime::{ModuleRuntime, RuntimeNames};
use crate::transport::memory::{MemoryBus, MemoryTransport};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex as StdMutex, OnceLock};

#[path = "host_callback_lifecycle_tests.rs"]
mod callback_lifecycle_tests;

static INIT_RAN: AtomicBool = AtomicBool::new(false);
static LAZY_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static FAILED_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static FAKE_MODULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static REINIT_CONFIG: OnceLock<StdMutex<Vec<u8>>> = OnceLock::new();
static BLOCKING_REINIT_RECEIVER: OnceLock<StdMutex<Option<std::sync::mpsc::Receiver<()>>>> =
    OnceLock::new();
static BLOCKING_REINIT_STARTED: AtomicBool = AtomicBool::new(false);

#[test]
fn module_host_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<ModuleHost>();
}

#[cfg(unix)]
fn make_private_tree(root: &Path, path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    for component in path
        .ancestors()
        .take_while(|component| component.starts_with(root))
    {
        std::fs::set_permissions(component, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

struct FakeModule {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
}

unsafe extern "C" fn fake_deliver(ctx: *mut std::ffi::c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || ptr.is_null() {
        return crate::module::abi::TB_BAD_ARGUMENT;
    }
    let module = unsafe { &*ctx.cast::<FakeModule>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    match module.tx.try_send(bytes) {
        Ok(()) => TB_OK,
        Err(std::sync::mpsc::TrySendError::Full(_)) => crate::module::abi::TB_BACKPRESSURE,
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => crate::module::abi::TB_CLOSED,
    }
}

unsafe extern "C" fn fake_shutdown(_: *mut std::ffi::c_void, _: u64) -> i32 {
    TB_OK
}

unsafe extern "C" fn fake_reinitialize(
    _: *mut std::ffi::c_void,
    ptr: *const u8,
    len: usize,
) -> i32 {
    if ptr.is_null() && len != 0 {
        return crate::module::abi::TB_BAD_ARGUMENT;
    }
    let bytes = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
    };
    *REINIT_CONFIG
        .get_or_init(|| StdMutex::new(Vec::new()))
        .lock()
        .expect("reinit config lock") = bytes;
    TB_OK
}

unsafe extern "C" fn blocking_reinitialize(
    _: *mut std::ffi::c_void,
    _: *const u8,
    _: usize,
) -> i32 {
    BLOCKING_REINIT_STARTED.store(true, Ordering::Release);
    if let Some(receiver) = BLOCKING_REINIT_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("blocking reinitialize receiver lock")
        .take()
    {
        let _ = receiver.recv();
    }
    TB_OK
}

unsafe extern "C" fn lazy_echo_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    LAZY_INIT_COUNT.fetch_add(1, Ordering::AcqRel);
    let host = unsafe { *host };
    let host_ctx = host.host_ctx as usize;
    let send = host.send;
    let ready = host.ready;
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(8);
    std::thread::spawn(move || {
        unsafe { ready(host_ctx as *mut std::ffi::c_void) };
        while let Ok(bytes) = rx.recv() {
            let Ok(message) = serde_json::from_slice::<crate::Message>(&bytes) else {
                continue;
            };
            if message.header.kind == crate::message::MessageKind::MethodCall {
                if message.header.member.as_ref().map(|member| member.as_str()) == Some("Hang") {
                    continue;
                }
                let body = if message.header.member.as_ref().map(|member| member.as_str())
                    == Some("Sender")
                {
                    serde_json::to_value(&message.header.sender).expect("fake sender serializes")
                } else {
                    message
                        .body
                        .as_array()
                        .and_then(|values| values.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                };
                let reply = crate::Message::method_return(&message.header, body);
                let bytes = serde_json::to_vec(&reply).expect("fake reply serializes");
                let _ = unsafe {
                    send(
                        host_ctx as *mut std::ffi::c_void,
                        bytes.as_ptr(),
                        bytes.len(),
                    )
                };
            }
        }
    });
    let module = Box::into_raw(Box::new(FakeModule { tx }));
    unsafe {
        *out = TbModuleVtable {
            size: size_of::<TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: module.cast(),
            deliver: fake_deliver,
            shutdown: fake_shutdown,
            reinitialize: Some(fake_reinitialize),
        };
    }
    TB_OK
}

unsafe extern "C" fn blocking_reinitialize_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    let code = unsafe { lazy_echo_init(host, out) };
    if code == TB_OK {
        unsafe { (*out).reinitialize = Some(blocking_reinitialize) };
    }
    code
}

unsafe extern "C" fn failing_init(
    _: *const crate::module::abi::TbHostVtable,
    _: *mut TbModuleVtable,
) -> i32 {
    FAILED_INIT_COUNT.fetch_add(1, Ordering::AcqRel);
    crate::module::abi::TB_BAD_ARGUMENT
}

unsafe extern "C" fn invalid_vtable_init(
    _: *const crate::module::abi::TbHostVtable,
    _: *mut TbModuleVtable,
) -> i32 {
    TB_OK
}

unsafe extern "C" fn init_that_must_not_run(
    _: *const crate::module::abi::TbHostVtable,
    _: *mut TbModuleVtable,
) -> i32 {
    INIT_RAN.store(true, Ordering::Release);
    TB_OK
}

pub(super) fn manifest() -> ModuleManifest {
    named_manifest("clock", "Clock")
}

#[tokio::test]
async fn pre_ready_route_policy_is_bounded_unique_and_fixed_before_admission() {
    let host = ModuleHost::new(Broker::new());
    let route = PreReadyConfidentialRoute {
        path: ObjectPath::new("/ai/tinyhumans/storage").unwrap(),
        interface: crate::InterfaceName::new("ai.tinyhumans.Storage").unwrap(),
        member: crate::MemberName::new("GetSchema").unwrap(),
    };

    host.set_pre_ready_confidential_routes(vec![route.clone()])
        .expect("one exact route is valid");
    assert!(
        host.set_pre_ready_confidential_routes(vec![route.clone(), route.clone()])
            .is_err()
    );
    assert!(
        host.set_pre_ready_confidential_routes(
            (0..=MAX_PRE_READY_CONFIDENTIAL_ROUTES)
                .map(|index| PreReadyConfidentialRoute {
                    path: ObjectPath::new(format!("/ai/tinyhumans/storage/item{index}")).unwrap(),
                    interface: crate::InterfaceName::new("ai.tinyhumans.Storage").unwrap(),
                    member: crate::MemberName::new("GetSchema").unwrap(),
                })
                .collect(),
        )
        .is_err()
    );

    host.attach_linked_with_config(
        LinkedModule {
            descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
            manifest: manifest(),
            init: lazy_echo_init,
            init_with_setup: None,
        },
        serde_json::json!({}),
    )
    .expect("fixture module is admitted");
    assert!(host.set_pre_ready_confidential_routes(Vec::new()).is_err());
}

fn named_manifest(module_name: &str, surface_name: &str) -> ModuleManifest {
    ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: module_name.to_string(),
            version: Version::new(0, 1, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: BusName::new(format!("ai.tinyhumans.module.{surface_name}")).unwrap(),
        object_path: ObjectPath::new(format!("/ai/tinyhumans/module/{surface_name}")).unwrap(),
        provides: vec![],
        requires: vec![],
        environment: vec![],
        capabilities: vec![],
        lazy_init: false,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    }
}

#[tokio::test]
async fn linked_module_is_admitted_without_a_library_file_and_attested_as_host_code() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut module_manifest = manifest();
    module_manifest.lazy_init = true;
    let info = host
        .attach_linked_with_config(
            LinkedModule {
                descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
                manifest: module_manifest,
                init: lazy_echo_init,
                init_with_setup: None,
            },
            serde_json::json!({}),
        )
        .unwrap();
    assert_eq!(info.state, ModuleState::Resolved);

    // Identity comes from the module's broker-assigned unique peer. A
    // well-known alias is only a routing name and must never resolve as a
    // module sender.
    let unique = host.inner.loaded.lock().unwrap()[0].unique_name.clone();
    let lookup = host.sender_lookup();
    let module_id = host
        .module_for_sender(&unique)
        .expect("an admitted module resolves from its unique peer");
    assert_eq!(module_id.as_str(), "clock");
    assert!(host.sender_for_module("clock").is_none());
    assert!(lookup.sender_for_module("clock").is_none());
    assert_eq!(host.list()[0].state, ModuleState::Resolved);
    assert_eq!(
        lookup
            .module_for_sender(&unique)
            .as_ref()
            .map(ModuleId::as_str),
        Some("clock")
    );
    assert!(host.module_for_sender(&info.manifest.bus_name).is_none());
    assert!(
        host.sender_for_module("ai.tinyhumans.module.Clock")
            .is_none()
    );
    assert!(lookup.module_for_sender(&info.manifest.bus_name).is_none());

    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attestation = connection
        .attestation(info.manifest.bus_name.clone())
        .await
        .unwrap()
        .expect("linked host code is attested");
    let executable = std::env::current_exe().unwrap();
    assert_eq!(
        attestation.sha256,
        crate::module::sha256_file(executable).unwrap()
    );

    let alias: crate::BusName = "ai.tinyhumans.test.ModuleAliasHandover".parse().unwrap();
    let first_owner = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    first_owner.request_name(alias.as_str()).await.unwrap();
    assert!(host.module_for_sender(&alias).is_none());
    drop(first_owner);
    let replacement_owner = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    replacement_owner
        .request_name(alias.as_str())
        .await
        .unwrap();
    assert!(host.module_for_sender(&alias).is_none());
    assert_eq!(
        host.module_for_sender(&unique)
            .expect("alias handover does not change module identity")
            .as_str(),
        "clock"
    );

    host.inner
        .stop(&info.name, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(host.module_for_sender(&unique).is_none());
    assert!(host.sender_for_module("clock").is_none());
    assert!(lookup.module_for_sender(&unique).is_none());
    broker_task.abort();
}

#[test]
fn an_owned_sender_lookup_does_not_keep_its_host_alive() {
    let host = ModuleHost::new(Broker::new());
    let observed_lifetime = std::sync::Arc::downgrade(&host.inner);
    let lookup = host.sender_lookup();
    let peer: crate::BusName = ":1.4242".parse().unwrap();

    assert_eq!(observed_lifetime.strong_count(), 1);
    assert!(lookup.module_for_sender(&peer).is_none());
    assert!(lookup.sender_for_module("clock").is_none());
    drop(host);

    assert_eq!(observed_lifetime.strong_count(), 0);
    assert!(lookup.module_for_sender(&peer).is_none());
    assert!(lookup.sender_for_module("clock").is_none());
}

#[test]
fn linked_module_with_an_unmet_required_interface_is_refused_before_init() {
    let host = ModuleHost::new(Broker::new());
    let mut module_manifest = manifest();
    module_manifest
        .requires
        .push(crate::module::manifest::Dependency {
            interface: crate::version::InterfaceVersion::consumed(
                "ai.tinyhumans.module.Missing".parse().unwrap(),
                Version::new(1, 0, 0),
            ),
            optional: false,
            reason: String::new(),
        });
    let error = host
        .attach_linked_with_config(
            LinkedModule {
                descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
                manifest: module_manifest,
                init: init_that_must_not_run,
                init_with_setup: None,
            },
            serde_json::json!({}),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("required interface has no provider")
    );
    assert!(host.list().is_empty());
}

#[tokio::test]
async fn a_lazy_module_initializes_on_the_first_call_and_two_racing_callers_initialize_it_once() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    LAZY_INIT_COUNT.store(0, Ordering::Release);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    let info = unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    assert_eq!(info.state, ModuleState::Resolved);
    assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 0);

    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let first = proxy.call::<String>("Echo", ("first",));
    let second = proxy.call::<String>("Echo", ("second",));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap(), "first");
    assert_eq!(second.unwrap(), "second");
    assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 1);
    assert_eq!(host.list()[0].state, ModuleState::Ready);
    broker_task.abort();
}

#[tokio::test]
async fn a_ready_module_accepts_replacement_configuration_without_reloading() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let host = ModuleHost::new(Broker::new());
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            manifest(),
            lazy_echo_init,
        )
    }
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while host.list()[0].state != ModuleState::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let info = host
        .reinitialize("clock", serde_json::json!({ "token": "replacement" }))
        .await
        .unwrap();
    assert_eq!(info.state, ModuleState::Ready);
    let captured = REINIT_CONFIG.get().unwrap().lock().unwrap().clone();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured).unwrap(),
        serde_json::json!({ "token": "replacement" })
    );
}

#[tokio::test]
async fn cancelling_reinitialization_releases_the_host_lifecycle_reservation() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let host = ModuleHost::new(Broker::new());
    let (release, receiver) = std::sync::mpsc::sync_channel(1);
    *BLOCKING_REINIT_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("blocking reinitialize receiver lock") = Some(receiver);
    BLOCKING_REINIT_STARTED.store(false, Ordering::Release);
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            manifest(),
            blocking_reinitialize_init,
        )
    }
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while host.list()[0].state != ModuleState::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let inner = host.inner.clone();
    let reinitializing = tokio::spawn(async move {
        inner
            .reinitialize("clock", serde_json::json!({ "token": "first" }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !BLOCKING_REINIT_STARTED.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    reinitializing.abort();
    assert!(reinitializing.await.unwrap_err().is_cancelled());

    release.send(()).unwrap();
    let info = tokio::time::timeout(
        Duration::from_secs(1),
        host.reinitialize("clock", serde_json::json!({ "token": "second" })),
    )
    .await
    .expect("the cancelled operation releases the host reservation")
    .unwrap();
    assert_eq!(info.state, ModuleState::Ready);
}

#[tokio::test]
async fn shutdown_does_not_report_a_module_stopped_while_reinitialization_is_running() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let host = ModuleHost::new(Broker::new());
    let (release, receiver) = std::sync::mpsc::sync_channel(1);
    *BLOCKING_REINIT_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("blocking reinitialize receiver lock") = Some(receiver);
    BLOCKING_REINIT_STARTED.store(false, Ordering::Release);
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            manifest(),
            blocking_reinitialize_init,
        )
    }
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while host.list()[0].state != ModuleState::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let inner = host.inner.clone();
    let reinitializing = tokio::spawn(async move {
        inner
            .reinitialize("clock", serde_json::json!({ "token": "first" }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !BLOCKING_REINIT_STARTED.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    host.shutdown(Duration::from_millis(10)).await;
    assert_eq!(host.list()[0].state, ModuleState::Ready);
    release.send(()).unwrap();
    reinitializing.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelling_a_waiting_stop_clears_its_pending_transition() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let host = ModuleHost::new(Broker::new());
    let (release, receiver) = std::sync::mpsc::sync_channel(1);
    *BLOCKING_REINIT_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("blocking reinitialize receiver lock") = Some(receiver);
    BLOCKING_REINIT_STARTED.store(false, Ordering::Release);
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            manifest(),
            blocking_reinitialize_init,
        )
    }
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while host.list()[0].state != ModuleState::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let inner = host.inner.clone();
    let reinitializing = tokio::spawn(async move {
        inner
            .reinitialize("clock", serde_json::json!({ "token": "first" }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !BLOCKING_REINIT_STARTED.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    reinitializing.abort();
    assert!(reinitializing.await.unwrap_err().is_cancelled());

    let inner = host.inner.clone();
    let stopping = tokio::spawn(async move { inner.stop("clock", Duration::from_secs(1)).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let stop_is_pending = {
                let loaded = host.inner.loaded.lock().expect("module list lock");
                loaded[0].lifecycle_busy && loaded[0].transition_from.is_some()
            };
            if stop_is_pending {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    let loaded = host.inner.loaded.lock().expect("module list lock");
    assert!(!loaded[0].lifecycle_busy);
    assert!(loaded[0].transition_from.is_none());
    drop(loaded);

    release.send(()).unwrap();
}

#[tokio::test]
async fn a_lazy_manifest_registers_an_unmapped_library_and_the_first_call_loads_it() {
    let temp_root = std::fs::canonicalize(std::env::temp_dir()).unwrap();
    let directory = tempfile::tempdir_in(temp_root).unwrap();
    #[cfg(unix)]
    make_private_tree(directory.path(), directory.path());
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    let artifact = directory.path().join(format!("clock.{extension}"));
    // These are deliberately not a dynamic library. Registration succeeding
    // proves discovery did not ask the platform loader to map the artifact.
    std::fs::write(&artifact, b"not loaded until the first call").unwrap();
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    std::fs::write(
        lazy_manifest_path(&artifact),
        serde_json::to_vec(&lazy_manifest).unwrap(),
    )
    .unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    #[cfg(not(windows))]
    let info = {
        let loaded = host.load_dir(directory.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        loaded.into_iter().next().unwrap().unwrap()
    };
    #[cfg(windows)]
    let info = {
        // Windows module directories admit only their owner, LocalSystem, and
        // Administrators. TempDir inherits a CI-runner ACE that is deliberately
        // rejected before discovery, so exercise the same sidecar seam directly;
        // the dedicated loader job covers a CI-provisioned private directory.
        let discovered = read_lazy_manifest(&artifact).unwrap().unwrap();
        // No pin: this stands in for a directory scan, which vouches for an
        // artifact with the `modules.toml` beside it or not at all.
        host.register_lazy(&artifact, discovered, serde_json::json!({}), None, None)
            .unwrap()
    };
    assert_eq!(info.state, ModuleState::Resolved);

    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let error = proxy.call::<()>("Now", ()).await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    assert!(matches!(host.list()[0].state, ModuleState::Failed { .. }));
    broker_task.abort();
}

/// The installer-bundle marker path end to end: no archive, a marker equal to
/// the pin, and an extraction the host admits. A lazy sidecar stands in for a
/// real library, as above, so admission runs without mapping anything.
#[cfg(not(windows))]
#[tokio::test]
async fn a_bundle_marker_uses_the_post_sign_allowlist_when_the_release_library_pin_differs() {
    use crate::module::{ReleaseAsset, ReleasePlan, artifact_dir, load_first_admitted};

    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    // Code signing can rewrite the bundled library after the release archive
    // was hashed. The sealed local allowlist carries the final library digest.
    let release_library_sha256 = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    let asset = ReleaseAsset {
        host_key: "macos-15-arm64",
        archive: "clock-macos-15-arm64.tar.gz",
        sha256: "abababababababababababababababababababababababababababababababab",
        library_sha256: Some(release_library_sha256),
    };
    let dir = artifact_dir(bundled.path(), "clock", "0.1.0", asset.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    #[cfg(unix)]
    make_private_tree(bundled.path(), &dir);
    let artifact = dir.join(format!(
        "clock.{}",
        crate::module::cache::library_extension()
    ));
    std::fs::write(&artifact, b"not loaded until the first call").unwrap();
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    std::fs::write(
        lazy_manifest_path(&artifact),
        serde_json::to_vec(&lazy_manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(
        crate::module::cache::digest_marker_path(&dir, asset.archive),
        format!("{}\n", asset.sha256),
    )
    .unwrap();
    std::fs::write(
        dir.join("modules.toml"),
        format!(
            "{:?} = {:?}\n",
            artifact.file_name().unwrap().to_str().unwrap(),
            crate::module::sha256_file(&artifact).unwrap()
        ),
    )
    .unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let assets = [asset];
    let plan = ReleasePlan {
        id: "clock",
        version: "0.1.0",
        release_url: "https://github.com/tinyhumansai/clock/releases/tag/v0.1.0",
        assets: &assets,
        install_root: user_cache.path(),
        bundled_root: Some(bundled.path()),
        allow_download: false,
    };
    let info = load_first_admitted(&host, &plan, &serde_json::json!({})).unwrap();
    assert_eq!(info.state, ModuleState::Resolved);
    assert!(!dir.join(asset.archive).exists());

    // Only an allowlist hashed against the mapped file attests; the marker's
    // claim about an archive nobody hashed does not.
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attestation = connection
        .attestation(info.manifest.bus_name.clone())
        .await
        .unwrap()
        .expect("the allowlisted extraction is attested");
    let signed_library_sha256 = crate::module::sha256_file(&artifact).unwrap();
    assert_eq!(attestation.sha256, signed_library_sha256);
    assert_eq!(
        attestation.library_sha256.as_deref(),
        Some(signed_library_sha256.as_str())
    );
    assert_ne!(attestation.sha256, asset.sha256);
    assert_ne!(attestation.library_sha256.as_deref(), asset.library_sha256);
    broker_task.abort();
}

#[cfg(not(windows))]
#[tokio::test]
async fn a_bundle_marker_without_an_allowlist_is_refused_rather_than_loaded_unhashed() {
    use crate::module::{ReleaseAsset, ReleasePlan, artifact_dir, load_first_admitted};

    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let asset = ReleaseAsset {
        host_key: "macos-15-arm64",
        archive: "clock-macos-15-arm64.tar.gz",
        sha256: "abababababababababababababababababababababababababababababababab",
        library_sha256: None,
    };
    let dir = artifact_dir(bundled.path(), "clock", "0.1.0", asset.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    #[cfg(unix)]
    make_private_tree(bundled.path(), &dir);
    let artifact = dir.join(format!(
        "clock.{}",
        crate::module::cache::library_extension()
    ));
    std::fs::write(&artifact, b"not loaded until the first call").unwrap();
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    std::fs::write(
        lazy_manifest_path(&artifact),
        serde_json::to_vec(&lazy_manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(
        crate::module::cache::digest_marker_path(&dir, asset.archive),
        format!("{}\n", asset.sha256),
    )
    .unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let assets = [asset];
    let plan = ReleasePlan {
        id: "clock",
        version: "0.1.0",
        release_url: "https://github.com/tinyhumansai/clock/releases/tag/v0.1.0",
        assets: &assets,
        install_root: user_cache.path(),
        bundled_root: Some(bundled.path()),
        allow_download: false,
    };
    let error = load_first_admitted(&host, &plan, &serde_json::json!({})).unwrap_err();
    assert!(error.contains("installer bundle"), "{error}");
    assert!(host.list().is_empty(), "nothing was registered");
    broker_task.abort();
}

#[test]
fn an_invalid_lazy_sidecar_refuses_the_artifact_instead_of_loading_it_eagerly() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    });
    std::fs::write(&artifact, b"not a dynamic library").unwrap();
    std::fs::write(lazy_manifest_path(&artifact), b"not valid JSON").unwrap();

    let error = read_lazy_manifest(&artifact).unwrap_err();
    assert!(error.to_string().contains("not valid JSON"), "{error}");
}

#[test]
fn a_sidecar_without_lazy_init_refuses_the_artifact_instead_of_loading_it_eagerly() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    });
    std::fs::write(&artifact, b"not a dynamic library").unwrap();
    std::fs::write(
        lazy_manifest_path(&artifact),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();

    let error = read_lazy_manifest(&artifact).unwrap_err();
    assert!(error.to_string().contains("must set lazy_init"), "{error}");
}

#[tokio::test]
async fn a_module_whose_init_fails_is_terminal_and_is_never_initialized_again() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    FAILED_INIT_COUNT.store(0, Ordering::Release);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            failing_init,
        )
    }
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let first = proxy.call::<()>("Call", ()).await.unwrap_err();
    assert_eq!(
        first.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(host.list()[0].state, ModuleState::Failed { .. }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second = proxy.call::<()>("Call", ()).await.unwrap_err();
    assert_eq!(
        second.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    assert_eq!(FAILED_INIT_COUNT.load(Ordering::Acquire), 1);
    broker_task.abort();
}

#[tokio::test]
async fn a_call_to_a_rejected_module_names_the_state_rather_than_timing_out() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.magic = 0;
    unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) }
        .unwrap_err();

    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let error = proxy.call::<()>("Call", ()).await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    assert!(error.to_string().contains("rejected"), "{error}");
    broker_task.abort();
}

#[tokio::test]
async fn a_match_rule_on_a_lazy_modules_signal_does_not_initialize_it() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    LAZY_INIT_COUNT.store(0, Ordering::Release);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let _signals = connection
        .add_match(
            crate::router::MatchRule::parse("type=signal,sender=ai.tinyhumans.module.Clock")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 0);
    broker_task.abort();
}

#[tokio::test]
async fn stopping_one_module_leaves_the_other_serving() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    LAZY_INIT_COUNT.store(0, Ordering::Release);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    for (module_name, surface_name) in [("one", "One"), ("two", "Two")] {
        let mut module_manifest = named_manifest(module_name, surface_name);
        module_manifest.lazy_init = true;
        unsafe {
            host.attach_raw(
                format!("{module_name}.so"),
                TbAbiDescriptor::current(module_name, "0.1.0"),
                module_manifest,
                lazy_echo_init,
            )
        }
        .unwrap();
    }
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = |surface_name: &str| {
        connection
            .proxy(
                format!("ai.tinyhumans.module.{surface_name}"),
                format!("/ai/tinyhumans/module/{surface_name}"),
                format!("ai.tinyhumans.module.{surface_name}"),
            )
            .unwrap()
    };
    assert_eq!(
        proxy("One").call::<String>("Echo", ("one",)).await.unwrap(),
        "one"
    );
    assert_eq!(
        proxy("Two").call::<String>("Echo", ("two",)).await.unwrap(),
        "two"
    );
    connection
        .stop_module("one", Duration::from_secs(1))
        .await
        .unwrap();
    let enable_error = connection.enable_module("one", true).await.unwrap_err();
    assert_eq!(
        enable_error.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    assert_eq!(host.list()[0].state, ModuleState::Stopped);
    assert_eq!(
        proxy("Two")
            .call::<String>("Echo", ("still serving",))
            .await
            .unwrap(),
        "still serving"
    );
    let stopped = proxy("One")
        .call::<()>("Echo", ("stopped",))
        .await
        .unwrap_err();
    assert_eq!(
        stopped.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    broker_task.abort();
}

#[tokio::test]
async fn a_call_to_a_disabled_module_that_still_owns_its_name_is_refused_promptly() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    LAZY_INIT_COUNT.store(0, Ordering::Release);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let disabled = connection.enable_module("clock", false).await.unwrap();
    assert_eq!(disabled.state, ModuleState::Disabled);
    // The disabled module's peer is still attached and still owns the
    // well-known name, so the router alone would deliver the call into a
    // queue nobody drains. The broker must answer from the host's state.
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        connection
            .proxy(
                "ai.tinyhumans.module.Clock",
                "/ai/tinyhumans/module/Clock",
                "ai.tinyhumans.module.Clock",
            )
            .unwrap()
            .call::<String>("Echo", ("disabled",)),
    )
    .await
    .expect("a disabled module is refused, not left to time out")
    .unwrap_err();
    assert_eq!(
        refused.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
    );
    assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 0);
    broker_task.abort();
}

#[tokio::test]
async fn a_module_that_never_replies_times_out_the_caller_and_leaves_the_bus_usable() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    // Only the deliberately unanswered call needs a short deadline. The
    // recovery call crosses an OS thread and must tolerate CI scheduling.
    let error = proxy
        .clone()
        .with_timeout(Duration::from_millis(20))
        .call::<()>("Hang", ())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Timeout { .. }), "{error}");
    assert_eq!(
        proxy.call::<String>("Echo", ("usable",)).await.unwrap(),
        "usable"
    );
    assert_eq!(host.list()[0].state, ModuleState::Serving);
    broker_task.abort();
}

#[tokio::test]
async fn the_sender_on_a_module_frame_is_stamped_by_the_broker_like_any_other_peers() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let sender: Option<BusName> = proxy.call("Sender", ()).await.unwrap();
    assert_eq!(sender, connection.unique_name());
    broker_task.abort();
}

#[tokio::test]
async fn authenticated_peer_is_available_only_for_a_live_ready_module() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);

    assert!(host.authenticated_peer("clock").is_none());
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    assert!(host.authenticated_peer("clock").is_none());
    assert!(
        host.authenticated_peer("ai.tinyhumans.module.Clock")
            .is_none()
    );

    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(
            "ai.tinyhumans.module.Clock",
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    let _: Option<BusName> = proxy.call("Sender", ()).await.unwrap();
    let authenticated = host
        .authenticated_peer("clock")
        .expect("ready module has an authenticated peer");
    assert!(authenticated.is_unique());
    {
        let mut loaded = host.inner.loaded.lock().unwrap();
        loaded[0].info.enabled = false;
    }
    assert!(host.authenticated_peer("clock").is_none());
    {
        let mut loaded = host.inner.loaded.lock().unwrap();
        loaded[0].info.enabled = true;
        loaded[0].info.state = ModuleState::Faulted {
            reason: "fixture".into(),
        };
    }
    assert!(host.authenticated_peer("clock").is_none());
    host.inner.loaded.lock().unwrap()[0].info.state = ModuleState::Ready;
    // The fake's Sender method reports its caller (this test connection),
    // not the module's broker-assigned identity. The compiled-module proof
    // verifies that an outbound module call carries this identity.

    host.inner
        .stop("clock", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(host.authenticated_peer("clock").is_none());
    broker_task.abort();
}

#[tokio::test]
async fn one_wedged_module_does_not_stall_another_modules_traffic() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    for (module_name, surface_name) in [("one", "One"), ("two", "Two")] {
        let mut module_manifest = named_manifest(module_name, surface_name);
        module_manifest.lazy_init = true;
        unsafe {
            host.attach_raw(
                format!("{module_name}.so"),
                TbAbiDescriptor::current(module_name, "0.1.0"),
                module_manifest,
                lazy_echo_init,
            )
        }
        .unwrap();
    }
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = |surface_name: &str| {
        connection
            .proxy(
                format!("ai.tinyhumans.module.{surface_name}"),
                format!("/ai/tinyhumans/module/{surface_name}"),
                format!("ai.tinyhumans.module.{surface_name}"),
            )
            .unwrap()
    };
    let wedged_proxy = proxy("One").with_timeout(Duration::from_millis(30));
    let healthy_proxy = proxy("Two");
    let wedged = wedged_proxy.call::<()>("Hang", ());
    let healthy = healthy_proxy.call::<String>("Echo", ("healthy",));
    let (wedged, healthy) = tokio::join!(wedged, healthy);
    assert!(matches!(wedged.unwrap_err(), Error::Timeout { .. }));
    assert_eq!(healthy.unwrap(), "healthy");
    broker_task.abort();
}

#[tokio::test]
async fn module_control_tracks_disable_stop_detach_and_unavailable_states() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus);
    let host = ModuleHost::new(broker);
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let control = host.inner.clone();
    let (disabled, transition) = control.enable("clock", false).unwrap();
    assert_eq!(disabled.state, ModuleState::Disabled);
    assert!(transition.is_some());
    let unavailable = control
        .unavailable_for(&manifest().bus_name)
        .expect("disabled module is unavailable");
    assert!(matches!(unavailable, Error::ModuleUnavailable { state, .. } if state == "disabled"));
    let (enabled, transition) = control.enable("clock", true).unwrap();
    assert_eq!(enabled.state, ModuleState::Resolved);
    assert!(transition.is_some());
    let unique_name = control.loaded.lock().unwrap()[0].unique_name.clone();
    let stopped = control.stop("clock", Duration::from_secs(5)).await.unwrap();
    assert_eq!(stopped.state, ModuleState::Stopped);
    let transition = control.peer_detached(&unique_name);
    assert!(transition.is_none() || transition.unwrap().2 == ModuleState::Stopped);
    assert!(matches!(
        control.enable("clock", true),
        Err(Error::ModuleUnavailable { state, .. }) if state == "stopped"
    ));
    assert!(matches!(
        control.stop("missing", Duration::ZERO).await,
        Err(Error::MethodFailed { .. })
    ));
    assert!(matches!(
        control.enable("missing", true),
        Err(Error::MethodFailed { .. })
    ));
    broker_task.abort();
}

#[tokio::test]
async fn admission_rejects_duplicate_names_bad_initializers_collisions_and_missing_dependencies() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let host = ModuleHost::new(Broker::new());
    let descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    unsafe { host.attach_raw("clock.so", descriptor, manifest(), lazy_echo_init) }.unwrap();
    assert!(
        unsafe {
            host.attach_raw(
                "again.so",
                TbAbiDescriptor::current("clock", "0.1.0"),
                manifest(),
                lazy_echo_init,
            )
        }
        .unwrap_err()
        .to_string()
        .contains("already loaded")
    );

    let failed = unsafe {
        host.attach_raw(
            "failed.so",
            TbAbiDescriptor::current("failed", "0.1.0"),
            named_manifest("failed", "Failed"),
            failing_init,
        )
    }
    .unwrap_err();
    assert!(failed.to_string().contains("initialization failed"));
    let invalid = unsafe {
        host.attach_raw(
            "invalid.so",
            TbAbiDescriptor::current("invalid", "0.1.0"),
            named_manifest("invalid", "Invalid"),
            invalid_vtable_init,
        )
    }
    .unwrap_err();
    assert!(invalid.to_string().contains("invalid vtable"));

    let mut colliding_manifest = manifest();
    colliding_manifest.module.name = "other".to_string();
    let collision = unsafe {
        host.attach_raw(
            "other.so",
            TbAbiDescriptor::current("other", "0.1.0"),
            colliding_manifest,
            lazy_echo_init,
        )
    }
    .unwrap_err();
    assert!(collision.to_string().contains("already owned"));

    let mut dependency = named_manifest("dependent", "Dependent");
    dependency
        .requires
        .push(crate::module::manifest::Dependency {
            interface: crate::version::InterfaceVersion::consumed(
                "ai.tinyhumans.module.Missing".parse().unwrap(),
                Version::new(1, 0, 0),
            ),
            optional: false,
            reason: String::new(),
        });
    assert!(
        host.ensure_dependencies(&dependency, Path::new("dependent.so"))
            .unwrap_err()
            .to_string()
            .contains("no provider")
    );
}

#[test]
fn a_module_compiled_with_panic_abort_is_refused_because_a_panic_would_kill_the_host() {
    let broker = Broker::new();
    let host = ModuleHost::new(broker);
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.flags &= !1;
    let error = host
        .validate(Path::new("clock.so"), &descriptor, &manifest())
        .unwrap_err();
    assert!(error.to_string().contains("panic abort"), "{error}");
}

#[test]
fn a_descriptor_with_a_bad_magic_is_refused_without_reading_past_the_prefix() {
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.magic = 0;
    let error = host
        .validate(Path::new("clock.so"), &descriptor, &manifest())
        .unwrap_err();
    assert!(error.to_string().contains("magic"), "{error}");
}

#[test]
fn a_module_built_for_an_older_abi_revision_is_refused_before_its_init_runs() {
    INIT_RAN.store(false, Ordering::Release);
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.abi_revision = 0;
    let error =
        unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) }
            .unwrap_err();
    assert!(error.to_string().contains("revision"), "{error}");
    assert!(!INIT_RAN.load(Ordering::Acquire));
}

#[test]
fn a_descriptor_smaller_than_the_host_expects_is_refused_and_a_larger_one_is_accepted() {
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.descriptor_size = size_of::<TbAbiDescriptor>() as u32 - 1;
    assert!(
        host.validate(Path::new("clock.so"), &descriptor, &manifest())
            .is_err()
    );
    descriptor.descriptor_size = size_of::<TbAbiDescriptor>() as u32 + 64;
    assert!(
        host.validate(Path::new("clock.so"), &descriptor, &manifest())
            .is_ok()
    );
}

#[test]
fn a_module_built_for_a_different_target_triple_is_refused() {
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.target_triple = [0; 64];
    descriptor.target_triple[..13].copy_from_slice(b"other-unknown");
    let error = host
        .validate(Path::new("clock.so"), &descriptor, &manifest())
        .unwrap_err();
    assert!(error.to_string().contains("target triple"), "{error}");
}

#[test]
fn a_module_needing_a_feature_the_host_lacks_is_refused_and_names_the_feature() {
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.tinybus_feature_bits |= 1 << 63;
    let error = host
        .validate(Path::new("clock.so"), &descriptor, &manifest())
        .unwrap_err();
    assert!(
        error.to_string().contains("unavailable tinybus feature"),
        "{error}"
    );
}

#[test]
fn a_module_built_against_an_incompatible_tinybus_is_refused_and_names_both_versions() {
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.tinybus_major = 99;
    let error = host
        .validate(Path::new("clock.so"), &descriptor, &manifest())
        .unwrap_err();
    let text = error.to_string();
    let module_version = format!(
        "99.{}",
        crate::VERSION
            .split_once('.')
            .expect("crate versions have a minor component")
            .1
    );
    assert!(text.contains(crate::VERSION), "{text}");
    assert!(text.contains(&module_version), "{text}");
}

#[test]
fn a_module_that_never_had_its_init_called_is_the_refusal_path() {
    INIT_RAN.store(false, Ordering::Release);
    let host = ModuleHost::new(Broker::new());
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.pointer_width = if usize::BITS == 64 { 32 } else { 64 };
    let _ = unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) };
    assert!(!INIT_RAN.load(Ordering::Acquire));
}

#[test]
fn a_module_built_by_a_different_rustc_is_refused_in_strict_mode_and_only_warned_about_otherwise() {
    let broker = Broker::new();
    let permissive = ModuleHost::new(broker.clone());
    let strict = ModuleHost::new(broker).strict(true);
    let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
    descriptor.rustc_version = [0; 48];
    descriptor.rustc_version[..5].copy_from_slice(b"0.0.0");
    assert!(
        permissive
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .is_ok()
    );
    assert!(
        strict
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err()
            .to_string()
            .contains("strict mode")
    );
}

#[test]
fn a_refusal_names_the_file_but_never_the_path_or_the_descriptor_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    let path = directory.path().join(format!("broken.{extension}"));
    std::fs::write(&path, b"not a dynamic library").unwrap();
    let host = ModuleHost::new(Broker::new());
    let error = host.load_file(&path).unwrap_err();
    assert!(
        !error
            .to_string()
            .contains(&directory.path().display().to_string())
    );
    let listed = host.list();
    assert_eq!(listed.len(), 1);
    assert!(matches!(
        listed[0].state,
        ModuleState::Rejected { ref reason } if !reason.is_empty()
    ));
}

#[cfg(unix)]
#[test]
fn a_world_writable_module_directory_is_refused_before_any_dlopen() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    let metadata = std::fs::metadata(directory.path()).unwrap();
    assert_eq!(
        unix_directory_refusal(
            metadata.uid(),
            metadata.gid(),
            metadata.permissions().mode(),
            metadata.uid(),
            None
        ),
        Some("module directory is writable by another user")
    );
}

#[cfg(windows)]
#[test]
fn a_creator_owner_cache_directory_is_accepted() {
    let directory = tempfile::tempdir().unwrap();
    let output = std::process::Command::new("icacls")
        .arg(directory.path())
        .args(["/grant", "*S-1-3-0:(OI)(CI)(IO)(F)"])
        .output()
        .expect("icacls is installed on Windows");
    assert!(output.status.success(), "icacls failed: {output:?}");
    let acl = std::process::Command::new("icacls")
        .arg(directory.path())
        .output()
        .unwrap();
    assert!(
        !windows_path_grants_untrusted_write(directory.path()).unwrap(),
        "CREATOR OWNER does not grant another account write access; ACL: {}",
        String::from_utf8_lossy(&acl.stdout)
    );
}

#[cfg(windows)]
#[test]
fn a_directory_writable_by_everyone_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let output = std::process::Command::new("icacls")
        .arg(directory.path())
        .args(["/grant", "*S-1-1-0:(F)"])
        .output()
        .expect("icacls is installed on Windows");
    assert!(output.status.success(), "icacls failed: {output:?}");
    assert!(windows_path_grants_untrusted_write(directory.path()).unwrap());
}

#[cfg(windows)]
fn icacls_grant(path: &Path, grant: &str) {
    let output = std::process::Command::new("icacls")
        .arg(path)
        .args(["/grant", grant])
        .output()
        .expect("icacls is installed on Windows");
    assert!(output.status.success(), "icacls failed: {output:?}");
}

/// Every folder under `C:\Program Files` inherits Full Control for
/// TrustedInstaller, so a per-machine (MSI) install carries this ACE.
#[cfg(windows)]
#[test]
fn a_directory_trusted_installer_can_write_is_accepted() {
    let directory = tempfile::tempdir().unwrap();
    icacls_grant(
        directory.path(),
        "*S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464:(OI)(CI)(F)",
    );
    assert!(!windows_path_grants_untrusted_write(directory.path()).unwrap());
}

/// An inherit-only grant gives Everyone nothing on the directory itself; the
/// file it seeds is what carries the write, and that file is refused.
#[cfg(windows)]
#[test]
fn an_inherit_only_grant_is_judged_on_the_files_it_seeds() {
    let directory = tempfile::tempdir().unwrap();
    icacls_grant(directory.path(), "*S-1-1-0:(OI)(CI)(IO)(M)");
    assert!(!windows_path_grants_untrusted_write(directory.path()).unwrap());

    let file = directory.path().join("module.dll");
    std::fs::write(&file, b"seeded").unwrap();
    assert!(windows_path_grants_untrusted_write(&file).unwrap());
}

#[cfg(windows)]
#[test]
fn a_cache_directory_writable_by_the_current_user_is_accepted() {
    let directory = tempfile::tempdir().unwrap();
    assert!(!windows_path_grants_untrusted_write(directory.path()).unwrap());
}

#[cfg(windows)]
#[test]
fn a_module_file_writable_by_everyone_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("module.dll");
    std::fs::write(&path, b"not a module").unwrap();
    let output = std::process::Command::new("icacls")
        .arg(&path)
        .args(["/grant", "*S-1-1-0:(F)"])
        .output()
        .expect("icacls is installed on Windows");
    assert!(output.status.success(), "icacls failed: {output:?}");
    assert!(
        check_file(&path)
            .unwrap_err()
            .to_string()
            .contains("artifact is writable by another user")
    );
}

#[cfg(unix)]
#[test]
fn a_sticky_world_writable_module_directory_is_accepted() {
    assert_eq!(unix_directory_refusal(0, 0, 0o1777, 1_000, None), None);
    assert_eq!(
        unix_directory_refusal(1_000, 1_000, 0o1777, 1_000, None),
        None
    );
}

#[cfg(unix)]
#[test]
fn a_module_directory_owned_by_another_user_is_refused() {
    assert_eq!(
        unix_directory_refusal(1_001, 1_001, 0o755, 1_000, None),
        Some("module directory is owned by another user")
    );
    assert_eq!(unix_directory_refusal(0, 0, 0o755, 1_000, None), None);
}

#[cfg(target_os = "macos")]
#[test]
fn a_root_owned_directory_writable_by_wheel_is_accepted_on_macos() {
    // `wheel` holds only root on macOS.
    assert_eq!(unix_directory_refusal(0, 0, 0o775, 501, None), None);
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn group_write_by_gid_zero_is_refused_off_macos() {
    // gid 0 is not guaranteed to be root-only outside macOS.
    assert_eq!(
        unix_directory_refusal(0, 0, 0o775, 1_000, None),
        Some("module directory is writable by another user")
    );
}

#[cfg(target_os = "macos")]
#[test]
fn the_macos_applications_directory_is_accepted() {
    // /Applications ships as `root:admin 0775`.
    assert_eq!(unix_directory_refusal(0, 80, 0o40775, 501, None), None);
}

#[cfg(unix)]
#[test]
fn group_write_by_an_ordinary_group_is_still_refused() {
    let refused = Some("module directory is writable by another user");
    // A root-owned directory writable by a non-root group.
    assert_eq!(unix_directory_refusal(0, 20, 0o775, 501, None), refused);
    // The user's own directory writable by a group: the gid alone cannot
    // show the group is private to them.
    assert_eq!(
        unix_directory_refusal(1_000, 1_000, 0o775, 1_000, None),
        refused
    );
    #[cfg(not(target_os = "macos"))]
    assert_eq!(unix_directory_refusal(0, 80, 0o775, 1_000, None), refused);
}

/// Ubuntu's user-private-group scheme (umask 002) leaves `$HOME` and
/// `~/.cache` as `user:user 0775`. Group write there grants nobody but the
/// user, so the per-user release cache beneath them must stay admissible.
#[cfg(unix)]
#[test]
fn group_write_by_the_users_private_group_is_admitted() {
    // The user's own directory, group-writable by their private group.
    assert_eq!(
        unix_directory_refusal(1_000, 1_000, 0o775, 1_000, Some(1_000)),
        None
    );
    assert_eq!(
        unix_directory_refusal(1_000, 1_000, 0o40770, 1_000, Some(1_000)),
        None
    );
    // A root-owned directory whose group is the user's private group.
    assert_eq!(
        unix_directory_refusal(0, 1_000, 0o775, 1_000, Some(1_000)),
        None
    );
}

#[cfg(unix)]
#[test]
fn group_write_by_a_shared_group_is_refused_even_with_a_private_group() {
    let refused = Some("module directory is writable by another user");
    // `users` (gid 100) is shared, whoever owns the directory.
    assert_eq!(
        unix_directory_refusal(1_000, 100, 0o775, 1_000, Some(1_000)),
        refused
    );
    assert_eq!(
        unix_directory_refusal(0, 100, 0o775, 1_000, Some(1_000)),
        refused
    );
    // Another user's private group is not this user's.
    assert_eq!(
        unix_directory_refusal(1_000, 1_001, 0o775, 1_000, Some(1_000)),
        refused
    );
    // A private group never excuses world write.
    assert_eq!(
        unix_directory_refusal(1_000, 1_000, 0o777, 1_000, Some(1_000)),
        refused
    );
    // Nor ownership by another account.
    assert_eq!(
        unix_directory_refusal(1_001, 1_000, 0o775, 1_000, Some(1_000)),
        Some("module directory is owned by another user")
    );
}

#[cfg(unix)]
#[test]
fn the_user_private_group_rule_reads_local_accounts_and_proves_exclusivity() {
    let passwd = "root:x:0:0::/root:/bin/sh\nalice:x:1000:1000::/home/alice:/bin/sh\n";
    let group = "root:x:0:\nalice:x:1000:\n";
    assert_eq!(private_group_in(passwd, group, 1_000), Some(1_000));
    // The user listed in their own group is still private.
    assert_eq!(
        private_group_in(passwd, "alice:x:1000:alice\n", 1_000),
        Some(1_000)
    );
    // Another member can write through the group.
    assert_eq!(
        private_group_in(passwd, "alice:x:1000:alice,bob\n", 1_000),
        None
    );
    // Another account with this primary gid, absent from the member list.
    let shared = format!("{passwd}bob:x:1001:1000::/home/bob:/bin/sh\n");
    assert_eq!(private_group_in(&shared, group, 1_000), None);
    // A primary group not named after the user is shared (`users`).
    let users = "alice:x:1000:100::/home/alice:/bin/sh\n";
    assert_eq!(private_group_in(users, "users:x:100:\n", 1_000), None);
    // Two accounts under one name are ambiguous.
    let twin = format!("{passwd}alice:x:1001:1001::/home/twin:/bin/sh\n");
    assert_eq!(private_group_in(&twin, group, 1_000), None);
    // A malformed record could be the account that shares the gid.
    assert_eq!(
        private_group_in(&format!("{passwd}broken-line\n"), group, 1_000),
        None
    );
    // A malformed group record could share the gid.
    assert_eq!(
        private_group_in(passwd, &format!("{group}other:x:1000\n"), 1_000),
        None
    );
    // Unknown account, missing group, or duplicate group entries prove nothing.
    assert_eq!(private_group_in(passwd, group, 4_242), None);
    assert_eq!(private_group_in(passwd, "root:x:0:\n", 1_000), None);
    assert_eq!(
        private_group_in(passwd, "alice:x:1000:\nother:x:1000:\n", 1_000),
        None
    );
}

#[cfg(unix)]
#[test]
fn a_gid_inside_a_subordinate_range_is_delegated() {
    let subgid = "alice:100000:65536\n# c\n";
    assert!(gid_is_delegated(subgid, 100_000));
    assert!(gid_is_delegated(subgid, 165_535));
    assert!(!gid_is_delegated(subgid, 165_536));
    assert!(!gid_is_delegated(subgid, 1_000));
    assert!(!gid_is_delegated("", 1_000));
    // An unreadable record is assumed to cover the gid.
    assert!(gid_is_delegated("garbage\n", 1_000));
}

#[cfg(unix)]
#[test]
fn accounts_count_as_local_only_without_a_directory_service() {
    assert!(accounts_are_local(
        "passwd: files systemd\ngroup: files systemd\nhosts: dns\n"
    ));
    assert!(accounts_are_local("# c\npasswd: files\ngroup: files # x\n"));
    // `compat` can import NIS entries through `+` lines.
    assert!(!accounts_are_local("passwd: compat\ngroup: compat\n"));
    assert!(!accounts_are_local("passwd: files sss\ngroup: files\n"));
    assert!(!accounts_are_local("passwd: files ldap\ngroup: files\n"));
    // Supplementary memberships may come from `initgroups`.
    assert!(!accounts_are_local(
        "passwd: files\ngroup: files\ninitgroups: files sss\n"
    ));
    assert!(accounts_are_local(
        "passwd: files\ngroup: files\ninitgroups: files\n"
    ));
    assert!(!accounts_are_local(
        "passwd: files\ngroup: files\nsubid: sss\n"
    ));
    // Both databases must be stated.
    assert!(!accounts_are_local("passwd: files\n"));
    assert!(!accounts_are_local(""));
}

#[cfg(unix)]
#[test]
fn a_directory_counts_as_empty_only_when_missing_or_without_entries() {
    let directory = tempfile::tempdir().unwrap();
    assert!(directory_has_no_entries(directory.path()));
    assert!(directory_has_no_entries(&directory.path().join("missing")));
    std::fs::write(directory.path().join("alice.user"), "{}").unwrap();
    assert!(!directory_has_no_entries(directory.path()));
    // Not a directory: unreadable, so not proven empty.
    assert!(!directory_has_no_entries(
        &directory.path().join("alice.user")
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn a_plain_directory_has_no_extended_acl() {
    let directory = tempfile::tempdir().unwrap();
    // A parent with a default ACL hands it to the new directory; only a
    // directory without one is a fair subject.
    let parent_default_acl = directory.path().parent().is_some_and(|parent| {
        let parent = std::ffi::CString::new(parent.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: valid NUL-terminated strings; null buffer asks for the length.
        unsafe {
            libc::getxattr(
                parent.as_ptr(),
                c"system.posix_acl_default".as_ptr(),
                std::ptr::null_mut(),
                0,
            ) >= 0
        }
    });
    if !parent_default_acl {
        assert!(
            !has_extended_acl(directory.path()),
            "a plain directory must not be reported as ACL-bearing"
        );
    }
    assert!(has_extended_acl(&directory.path().join("missing")));
}

/// A refusal names the ancestor that failed: walking to `/` means the culprit
/// is rarely the module directory itself, and the bare phrase left users
/// guessing which of a dozen directories to fix.
#[cfg(unix)]
#[test]
fn a_directory_refusal_names_the_ancestor_that_failed() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let shared = root.join("shared-parent");
    let module_dir = shared.join("ubuntu-22.04-x86_64");
    std::fs::create_dir_all(&module_dir).unwrap();
    std::fs::set_permissions(&module_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).unwrap();

    let refusal = check_directory(&module_dir).unwrap_err();
    let Error::ModuleRefused { file, reason } = &refusal else {
        panic!("unexpected error {refusal}");
    };
    assert_eq!(file, "ubuntu-22.04-x86_64");
    assert!(
        reason.starts_with("module directory is writable by another user"),
        "{reason}"
    );
    // The failing component is not a fixed system name, so it is not echoed.
    assert!(reason.contains("an ancestor directory"), "{reason}");
    assert!(!reason.contains("shared-parent"), "{reason}");
    assert!(is_placement_refusal(&refusal), "{refusal}");
}

#[cfg(unix)]
#[test]
fn a_refused_ancestor_is_named_without_leaking_a_path() {
    let module = Path::new("/opt/app/bundled-modules/x86_64");
    let label = |ancestor: &str| ancestor_label(Path::new(ancestor), module);
    assert_eq!(label("/"), "the filesystem root");
    assert_eq!(
        label("/opt/app/bundled-modules/x86_64"),
        "the directory itself"
    );
    assert_eq!(label("/opt"), "opt");
    assert_eq!(label("/opt/app"), "an ancestor directory");
    // Names are trusted only directly beneath the root.
    assert_eq!(label("/srv/secret/.cache"), "an ancestor directory");
    assert_eq!(label("/tmp/private"), "an ancestor directory");
    assert_eq!(label("/home/someone/.cache"), "an ancestor directory");
    assert_eq!(label("/home/.alice"), "an ancestor directory");
    assert_eq!(label("/home/someone-else"), "an ancestor directory");
    assert_eq!(label("/export/home/bob"), "an ancestor directory");
}

#[cfg(unix)]
#[test]
fn world_write_without_sticky_is_refused_even_for_root_groups() {
    assert_eq!(
        unix_directory_refusal(0, 0, 0o777, 1_000, None),
        Some("module directory is writable by another user")
    );
}

#[test]
fn a_file_that_is_not_a_regular_file_is_skipped() {
    let directory = tempfile::tempdir().unwrap();
    let error = check_file(directory.path()).unwrap_err();
    assert!(error.to_string().contains("not a regular file"));
}

#[test]
fn module_host_helpers_preserve_safe_names_states_and_allowlist_decisions() {
    let states = [
        ModuleState::Discovered,
        ModuleState::Rejected {
            reason: "no".into(),
        },
        ModuleState::Unresolved {
            reason: "no".into(),
        },
        ModuleState::Resolved,
        ModuleState::Initializing,
        ModuleState::Ready,
        ModuleState::Serving,
        ModuleState::Faulted {
            reason: "no".into(),
        },
        ModuleState::Failed {
            reason: "no".into(),
        },
        ModuleState::Stopped,
        ModuleState::Disabled,
    ];
    assert_eq!(
        states.iter().map(state_name).collect::<Vec<_>>(),
        [
            "discovered",
            "rejected",
            "unresolved",
            "resolved",
            "initializing",
            "ready",
            "serving",
            "faulted",
            "failed",
            "stopped",
            "disabled",
        ]
    );
    assert_eq!(state_detail(&states[1]), Some("no"));
    assert_eq!(state_detail(&states[2]), Some("no"));
    assert_eq!(state_detail(&states[0]), None);
    assert_eq!(
        sanitized_field(b"clock\0ignored"),
        Some("clock".to_string())
    );
    assert_eq!(sanitized_field(b"bad\nname"), None);
    assert_eq!(sanitized_field(&[0xff]), None);
    assert_eq!(safe_file_name(Path::new("/private/clock.so")), "clock.so");
    assert_eq!(safe_file_name(Path::new("/private/\n")), "module");
    assert_eq!(safe_file_name(Path::new("/")), "module");
    assert!(has_library_extension(Path::new(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    })));
    assert!(!has_library_extension(Path::new("clock.txt")));

    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let directory = root.path().join("private");
    // Protect the artifact parent before files inherit the checkout's DACL.
    super::super::windows_acl::create_private_dir_all(&directory).unwrap();
    let module = directory.join(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    });
    std::fs::write(&module, b"module bytes").unwrap();
    let digest = crate::module::hash::file_hex(std::fs::File::open(&module).unwrap()).unwrap();
    std::fs::write(
        directory.join("modules.toml"),
        format!(
            "clock.{} = \"{digest}\"\n",
            if cfg!(windows) {
                "dll"
            } else if cfg!(target_os = "macos") {
                "dylib"
            } else {
                "so"
            }
        ),
    )
    .unwrap();
    assert!(check_file(&module).is_ok());
    let text = directory.join("clock.txt");
    std::fs::write(&text, b"module bytes").unwrap();
    assert!(
        check_file(&text)
            .unwrap_err()
            .to_string()
            .contains("extension is not loadable")
    );
    std::fs::write(directory.join("modules.toml"), "other = \"00\"\n").unwrap();
    assert!(
        check_file(&module)
            .unwrap_err()
            .to_string()
            .contains("absent")
    );
    std::fs::write(directory.join("modules.toml"), "clock = \"not-a-hash\"\n").unwrap();
    assert!(
        check_file(&module)
            .unwrap_err()
            .to_string()
            .contains("invalid hash")
    );
    let refused = Error::module_refused(&module, "nope");
    let info = rejection_info(&refused);
    assert!(matches!(info.state, ModuleState::Rejected { .. }));
    assert_eq!(rejection_info(&Error::ConnectionClosed).name, "module");
}

#[test]
fn an_allowlist_with_a_mismatched_hash_refuses_the_file() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let directory = root.path().join("private");
    // Protect the artifact parent before files inherit the checkout's DACL.
    super::super::windows_acl::create_private_dir_all(&directory).unwrap();
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    let path = directory.join(format!("clock.{extension}"));
    std::fs::write(&path, b"not a module").unwrap();
    std::fs::write(
        directory.join("modules.toml"),
        format!("clock.{extension} = \"{}\"\n", "0".repeat(64)),
    )
    .unwrap();
    let error = check_file(&path).unwrap_err();
    assert!(error.to_string().contains("hash does not match"), "{error}");
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_real_cdylib_loads_and_serves_a_call() {
    let path = std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE");
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    let modules = ModuleHost::new(broker);
    modules
        .load_file_with_config(path, serde_json::json!({ "prefix": "configured:" }))
        .unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client
                .list_names()
                .await
                .unwrap()
                .iter()
                .any(|name| name.as_str() == "ai.tinyhumans.openhuman.Clock")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let clock = client
        .proxy(
            "ai.tinyhumans.openhuman.Clock",
            "/ai/tinyhumans/openhuman/Clock",
            "ai.tinyhumans.openhuman.Clock",
        )
        .unwrap();
    let value: String = clock.call("Now", ()).await.unwrap();
    assert!(value.starts_with("configured:"), "{value}");
    let control = client
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
        .unwrap();
    let listed: Vec<ModuleInfo> = control.call("ListModules", ()).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "tinybus");
    let mut state_changes = client
        .add_match(
            crate::router::MatchRule::parse(
                "type=signal,interface=ai.tinyhumans.tinybus.Bus,member=ModuleStateChanged",
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut name_changes = client
        .add_match(
            crate::router::MatchRule::parse(
                "type=signal,interface=ai.tinyhumans.tinybus.Bus,member=NameOwnerChanged",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let panic_error = clock.call::<()>("Panic", ()).await.unwrap_err();
    let panic_text = panic_error.to_string();
    assert!(panic_text.contains("ModulePanicked"), "{panic_text}");
    assert!(panic_text.contains("module_clock.rs"), "{panic_text}");
    assert!(!panic_text.contains("secret-token"), "{panic_text}");
    let name_change = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = name_changes.recv().await.unwrap();
            if message.body.get(0).and_then(serde_json::Value::as_str)
                == Some("ai.tinyhumans.openhuman.Clock")
                && message.body.get(2).is_some_and(serde_json::Value::is_null)
            {
                break message;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(name_change.body[0], "ai.tinyhumans.openhuman.Clock");
    assert!(name_change.body[2].is_null());

    let state_change = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = state_changes.recv().await.unwrap();
            if message.header.member.as_ref().map(|member| member.as_str())
                == Some("ModuleStateChanged")
                && message.body.get(2).and_then(serde_json::Value::as_str) == Some("faulted")
            {
                break message;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(state_change.body[0], "tinybus");
    assert_eq!(state_change.body[2], "faulted");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !client
                .list_names()
                .await
                .unwrap()
                .iter()
                .any(|name| name.as_str() == "ai.tinyhumans.openhuman.Clock")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn scanning_loading_rescanning_and_shutting_down_a_module_directory_are_consistent() {
    let source =
        PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE"));
    let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let file_name = source.file_name().expect("module filename");
    let module = directory.path().join(file_name);
    std::fs::copy(source, &module).unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus);
    let host = ModuleHost::new(broker);
    host.set_config("tinybus", serde_json::json!({ "prefix": "directory:" }));
    let scanned = host.scan_dir(directory.path()).unwrap();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].state, ModuleState::Resolved);
    let loaded = host.load_dir(directory.path()).unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded[0].as_ref().unwrap().enabled);
    assert!(host.load_dir(directory.path()).unwrap().is_empty());
    let (dry_run, dry_transitions) = host
        .inner
        .clone()
        .rescan(vec![directory.path().to_path_buf()], true)
        .unwrap();
    assert_eq!(dry_run.len(), 1);
    assert!(dry_transitions.is_empty());
    host.shutdown(Duration::from_millis(10)).await;
    assert!(matches!(host.list()[0].state, ModuleState::Stopped));
    task.abort();
}

#[cfg(not(windows))]
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn duplicate_module_declarations_are_reported_without_attaching_either_copy() {
    let source =
        PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE"));
    let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    for name in ["one", "two"] {
        let extension = source.extension().expect("module extension");
        std::fs::copy(
            &source,
            directory.path().join(name).with_extension(extension),
        )
        .unwrap();
    }
    let host = ModuleHost::new(Broker::new());
    let scanned = host.scan_dir(directory.path()).unwrap();
    assert_eq!(scanned.len(), 2);
    assert!(
        scanned
            .iter()
            .all(|info| matches!(info.state, ModuleState::Unresolved { .. }))
    );
    let loaded = host.load_dir(directory.path()).unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded.iter().all(Result::is_err));
    assert_eq!(host.list().len(), 2);
}

#[test]
#[ignore = "requires TINYBUS_TEST_MODULE and TINYBUS_TEST_MODULE_TWO"]
fn two_modules_exporting_the_same_symbol_name_each_resolve_to_their_own() {
    let first =
        PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE"));
    let second = PathBuf::from(
        std::env::var_os("TINYBUS_TEST_MODULE_TWO").expect("TINYBUS_TEST_MODULE_TWO"),
    );
    let first = loader::load(&first, false).unwrap();
    let second = loader::load(&second, false).unwrap();
    assert_eq!(first.manifest.module.name, "tinybus");
    assert_eq!(second.manifest.module.name, "module-clock-two");
    assert_ne!(first.descriptor.module_name, second.descriptor.module_name);
}

#[test]
#[ignore = "requires TINYBUS_TEST_WRONG_TARGET"]
fn a_cdylib_built_for_a_different_target_is_refused_at_the_gate() {
    let path = PathBuf::from(
        std::env::var_os("TINYBUS_TEST_WRONG_TARGET").expect("TINYBUS_TEST_WRONG_TARGET"),
    );
    let host = ModuleHost::new(Broker::new());
    let error = host.load_file(path).unwrap_err();
    assert!(error.to_string().contains("target triple"), "{error}");
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE and TINYBUS_TEST_WRONG_TARGET"]
async fn one_refused_module_does_not_stop_the_others_in_the_directory_from_loading() {
    let valid =
        PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE"));
    let invalid = PathBuf::from(
        std::env::var_os("TINYBUS_TEST_WRONG_TARGET").expect("TINYBUS_TEST_WRONG_TARGET"),
    );
    let prepared_directory = std::env::var_os("TINYBUS_TEST_MODULE_DIRECTORY").map(PathBuf::from);
    let temporary_directory = prepared_directory.is_none().then(|| {
        tempfile::tempdir_in(valid.parent().expect("module artifact has a parent")).unwrap()
    });
    let directory = prepared_directory.as_deref().unwrap_or_else(|| {
        let directory = temporary_directory.as_ref().unwrap().path();
        std::fs::copy(&valid, directory.join(valid.file_name().unwrap())).unwrap();
        std::fs::copy(&invalid, directory.join(invalid.file_name().unwrap())).unwrap();
        directory
    });

    let host = ModuleHost::new(Broker::new());
    let outcomes = host.load_dir(directory).unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_err()).count(),
        1
    );
}

/// Copy `artifact` into a fresh, private directory with its hash allowlist.
///
/// Each native fixture needs a separate mapping: the SDK installs a global
/// tracing subscriber inside the library, so initializing the same DLL path
/// twice shares state even across independent hosts. Windows staging uses the
/// release-cache ACL repair so the real admission gate remains in force.
pub(super) fn staged_module(artifact: &Path, hash: &str) -> (tempfile::TempDir, PathBuf) {
    // Staged beside the artifact rather than in `/tmp`: the loader refuses a
    // directory another user could write to, and `/tmp` is exactly that. The
    // artifact's own directory is user-owned (CI builds into
    // `target/debug/examples`), so it already satisfies the check that `/tmp`
    // fails — which is the admission check doing its job, not an obstacle to
    // route around.
    let root = artifact.parent().expect("artifact has a parent directory");
    let dir = tempfile::tempdir_in(root).unwrap();
    #[cfg(windows)]
    crate::module::cache::secure_release_cache(dir.path(), dir.path());
    let file_name = artifact.file_name().unwrap();
    let staged = dir.path().join(file_name);
    std::fs::copy(artifact, &staged).unwrap();
    std::fs::write(
        dir.path().join("modules.toml"),
        format!("{:?} = {hash:?}\n", file_name.to_str().unwrap()),
    )
    .unwrap();
    (dir, staged)
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_module_loaded_from_an_allowlisted_artifact_becomes_an_attested_recipient() {
    // The one seam the in-memory fixtures cannot reach: a real artifact, hashed
    // off the disk by the host, becoming eligible to receive a secret.
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, staged) = staged_module(&artifact, &hash);

    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let info = host.load_file(&staged).unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attestation = client
        .attestation(info.manifest.bus_name.clone())
        .await
        .unwrap()
        .expect("an allowlisted module is attested");
    assert_eq!(attestation.sha256, hash);
    assert_eq!(attestation.name, info.manifest.bus_name);
    let unique = client
        .name_owner(info.manifest.bus_name)
        .await
        .unwrap()
        .unwrap();
    let proxy = client
        .proxy(
            unique.as_str(),
            "/ai/tinyhumans/openhuman/Clock",
            "ai.tinyhumans.openhuman.Clock",
        )
        .unwrap();
    assert_eq!(proxy.attestation().await.unwrap(), Some(attestation));
    let now: String = proxy.call_confidential("Now", ()).await.unwrap();
    assert!(!now.is_empty());
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_module_whose_artifact_does_not_match_the_allowlist_never_loads_at_all() {
    // The refusal happens before `dlopen`, so the question of attestation never
    // arises: unverified code is not admitted, let alone handed a secret.
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let (_dir, staged) = staged_module(&artifact, &"a".repeat(64));

    let host = ModuleHost::new(Broker::new());
    let error = host.load_file(&staged).unwrap_err();
    assert!(error.to_string().contains("allowlist"), "{error}");
}

/// Copy `artifact` into a fresh directory carrying no allowlist at all.
///
/// This is the shape of a release download: the host extracted the archive
/// into a private directory it just created, so there is no `modules.toml`
/// beside the library and nothing on disk to re-read a digest from.
#[cfg(unix)]
fn staged_module_without_allowlist(artifact: &Path) -> (tempfile::TempDir, PathBuf) {
    let root = artifact.parent().expect("artifact has a parent directory");
    let dir = tempfile::tempdir_in(root).unwrap();
    let staged = dir.path().join(artifact.file_name().unwrap());
    std::fs::copy(artifact, &staged).unwrap();
    (dir, staged)
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_module_from_a_pinned_release_becomes_an_attested_recipient_without_an_allowlist_file() {
    // The seam a host that loads from a release actually travels. `acquire`
    // has already checked the archive against the release manifest and against
    // the caller's compiled-in digest; what is proven here is that the fact
    // survives into an `Attestation` instead of being dropped on the floor
    // because no `modules.toml` happened to sit beside the extracted library.
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let (_dir, staged) = staged_module_without_allowlist(&artifact);
    let pinned = "b".repeat(64);

    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let info = host
        .load_file_pinned(&staged, serde_json::json!({}), Some(pinned.clone()), None)
        .unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attestation = client
        .attestation(info.manifest.bus_name.clone())
        .await
        .unwrap()
        .expect("a module loaded against a pinned digest is attested");
    assert_eq!(attestation.name, info.manifest.bus_name);

    // Recorded verbatim, and deliberately *not* the hash of the library file.
    // The pin names the release archive the library was extracted from, which
    // is the only artifact any operator asserted anything about. Re-hashing
    // the extracted file here would replace a checked fact with a number this
    // code computed and then trusted itself for, so the two must differ.
    assert_eq!(attestation.sha256, pinned);
    let library_hash =
        crate::module::hash::file_hex(std::fs::File::open(&staged).unwrap()).unwrap();
    assert_ne!(attestation.sha256, library_hash);
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_module_with_neither_a_pin_nor_an_allowlist_is_loaded_but_never_attested() {
    // The regression this change exists to fix, asserted from the other side:
    // before it, every release-loaded module looked exactly like this, so a
    // confidential call to one was refused no matter how carefully the host
    // had pinned the digest. Loading must still succeed — an unattested module
    // is ineligible for secrets, not inadmissible.
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let (_dir, staged) = staged_module_without_allowlist(&artifact);

    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let info = host.load_file(&staged).unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(
        client
            .attestation(info.manifest.bus_name.clone())
            .await
            .unwrap()
            .is_none(),
        "a module nobody vouched for must not be eligible to receive a secret"
    );
}

struct NativeContextCallback;
#[async_trait::async_trait]
impl crate::Interface for NativeContextCallback {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::failed("missing contextual dispatch"))
    }
    async fn call_with_context(
        &self,
        _: &crate::MemberName,
        args: serde_json::Value,
        context: &crate::CallContext,
    ) -> crate::Result<serde_json::Value> {
        assert_eq!(args, serde_json::json!([]));
        Ok(serde_json::json!(
            context
                .authenticated_sender()
                .expect("native callback is broker routed")
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_MODULE to point at the built cdylib"]
async fn a_real_cdylib_authenticates_incoming_calls_and_its_host_callback() {
    authenticates_native_context("TINYBUS_TEST_CONTEXT_MODULE").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONFIGURED_CONTEXT_MODULE to point at the built cdylib"]
async fn a_configured_context_cdylib_preserves_binding_through_reinitialize() {
    let artifact =
        PathBuf::from(std::env::var_os("TINYBUS_TEST_CONFIGURED_CONTEXT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);

    let broker = Broker::new_restricted_module_control();
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    let selected_controller = broker
        .attach_module_controller(Arc::new(broker_transport))
        .unwrap();
    let controller = Connection::connect(Box::new(controller_transport))
        .await
        .unwrap();
    assert_eq!(
        controller.unique_name().as_ref(),
        Some(&selected_controller)
    );

    let modules = ModuleHost::new(broker.clone());
    let info = modules.load_file(&artifact).unwrap();
    modules
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
        .expect("configured setup receives the selected controller context");
    let proxy = controller
        .proxy(
            info.manifest.bus_name.as_str(),
            "/context",
            "org.example.Context",
        )
        .unwrap();
    assert!(proxy.attestation().await.unwrap().is_some());
    assert_eq!(
        proxy.call::<String>("Configuration", ()).await.unwrap(),
        "initial"
    );
    let initial: serde_json::Value = proxy.call_confidential("TrustedContext", ()).await.unwrap();
    assert_eq!(initial["authorized"], true);
    assert_eq!(initial["previousAuthorized"], false);

    modules
        .reinitialize(
            info.manifest.module.name.as_str(),
            serde_json::json!({"marker": "reconfigured"}),
        )
        .await
        .expect("reconfigure receives a fresh context with the same binding");
    assert_eq!(
        proxy.call::<String>("Configuration", ()).await.unwrap(),
        "reconfigured"
    );
    let after_reinitialize: serde_json::Value =
        proxy.call_confidential("TrustedContext", ()).await.unwrap();
    assert_eq!(after_reinitialize["authorized"], true);

    controller.close().await.unwrap();
    let (other_server, other_transport) = MemoryTransport::pair();
    broker.attach(Arc::new(other_server));
    let other = Connection::connect(Box::new(other_transport))
        .await
        .unwrap();
    let other_proxy = other
        .proxy(
            info.manifest.bus_name.as_str(),
            "/context",
            "org.example.Context",
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let authorization: serde_json::Value = other_proxy
                .call_confidential("TrustedContext", ())
                .await
                .expect("the attested module remains reachable by another peer");
            if authorization["previousAuthorized"] == false {
                assert_eq!(authorization["authorized"], false);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the previously selected controller's binding is revoked on disconnect");

    // Exercise the same context-bearing artifact through TinyBus's public
    // process-lifetime runtime, not only through a hand-built ModuleHost.
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let (_runtime_dir, runtime_artifact) = staged_module(&artifact, &hash);
    let runtime_info = runtime.host().load_file(&runtime_artifact).unwrap();
    runtime
        .host()
        .ensure_ready(runtime_info.manifest.bus_name.as_str())
        .await
        .expect("public ModuleRuntime provides the selected trusted controller");
    let runtime_proxy = runtime
        .connection()
        .proxy(
            runtime_info.manifest.bus_name.as_str(),
            "/context",
            "org.example.Context",
        )
        .unwrap();
    assert!(runtime_proxy.attestation().await.unwrap().is_some());
    let initial: serde_json::Value = runtime_proxy
        .call_confidential("TrustedContext", ())
        .await
        .unwrap();
    assert_eq!(initial["authorized"], true);
    assert_eq!(initial["previousAuthorized"], false);
    runtime
        .host()
        .reinitialize(
            runtime_info.manifest.module.name.as_str(),
            serde_json::json!({"marker": "runtime-reconfigured"}),
        )
        .await
        .expect("public runtime reinitialization retains the selected host binding");
    let after_reinitialize: serde_json::Value = runtime_proxy
        .call_confidential("TrustedContext", ())
        .await
        .unwrap();
    assert_eq!(after_reinitialize["authorized"], true);
    assert_eq!(after_reinitialize["previousAuthorized"], false);

    // An ordinary linked module still starts through the same public runtime.
    let mut ordinary_manifest = manifest();
    ordinary_manifest.lazy_init = true;
    let ordinary_info = runtime
        .host()
        .attach_linked_with_config(
            crate::module::LinkedModule {
                descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
                manifest: ordinary_manifest,
                init: lazy_echo_init,
                init_with_setup: None,
            },
            serde_json::json!({}),
        )
        .unwrap();
    runtime
        .host()
        .ensure_ready(ordinary_info.manifest.bus_name.as_str())
        .await
        .expect("ordinary setup remains compatible with a selected runtime host");
    let ordinary_proxy = runtime
        .connection()
        .proxy(
            ordinary_info.manifest.bus_name.as_str(),
            ordinary_info.manifest.object_path.as_str(),
            ordinary_info.manifest.bus_name.as_str(),
        )
        .unwrap();
    let echoed: String = ordinary_proxy
        .call("Echo", ("ordinary-runtime".to_owned(),))
        .await
        .unwrap();
    assert_eq!(echoed, "ordinary-runtime");
}

async fn contextual_factory_cdylib_instance(
    artifact: &Path,
    restricted: bool,
) -> (ModuleHost, Broker, Connection, crate::Proxy, String) {
    let broker = Broker::new_restricted_module_control();
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    let selected_controller = broker
        .attach_module_controller(Arc::new(broker_transport))
        .unwrap();
    let controller = Connection::connect(Box::new(controller_transport))
        .await
        .unwrap();
    assert_eq!(
        controller.unique_name().as_ref(),
        Some(&selected_controller)
    );

    let modules = ModuleHost::new(broker.clone());
    let info = modules
        .load_file_with_config(artifact, serde_json::json!({"restricted":restricted}))
        .unwrap();
    modules
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
        .expect("compiled context factory must receive the selected controller binding");
    let proxy = controller
        .proxy(
            info.manifest.bus_name.as_str(),
            "/context_factory",
            "org.example.ContextFactory",
        )
        .unwrap();
    (
        modules,
        broker,
        controller,
        proxy,
        info.manifest.module.name,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_FACTORY_MODULE to point at the built cdylib"]
async fn a_context_factory_cdylib_keeps_instance_state_and_refreshes_trusted_context() {
    let artifact = PathBuf::from(
        std::env::var_os("TINYBUS_TEST_CONTEXT_FACTORY_MODULE")
            .expect("TINYBUS_TEST_CONTEXT_FACTORY_MODULE"),
    );
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_staging, staged) = staged_module(&artifact, &hash);

    let (
        (first, broker, controller, first_proxy, module_name),
        (_second, _, _, second_proxy, second_module_name),
    ) = tokio::join!(
        contextual_factory_cdylib_instance(&staged, true),
        contextual_factory_cdylib_instance(&staged, false),
    );
    assert_eq!(module_name, second_module_name);
    assert!(first_proxy.attestation().await.unwrap().is_some());
    assert!(
        first_proxy
            .call::<serde_json::Value>("Inspect", ())
            .await
            .is_err()
    );
    let first_initial: serde_json::Value =
        first_proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(first_initial["restricted"], true);
    assert_eq!(first_initial["generation"], 1);
    assert!(first_initial["factory_id"].as_u64().is_some());

    let second_initial: serde_json::Value =
        second_proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(second_initial["restricted"], false);
    assert_eq!(second_initial["generation"], 1);

    let (outsider_server, outsider_client) = MemoryTransport::pair();
    broker.attach(Arc::new(outsider_server));
    let outsider = Connection::connect(Box::new(outsider_client))
        .await
        .unwrap();
    let outsider_proxy = outsider
        .proxy(
            first_proxy.destination().as_str(),
            "/context_factory",
            "org.example.ContextFactory",
        )
        .unwrap();
    let denied = outsider_proxy
        .call_confidential::<serde_json::Value>("Inspect", serde_json::json!("malformed"))
        .await
        .unwrap_err();
    assert!(
        denied
            .to_string()
            .contains("factory context authority unavailable")
    );
    let selected_malformed = first_proxy
        .call_confidential::<serde_json::Value>("Inspect", serde_json::json!("malformed"))
        .await
        .unwrap_err();
    assert!(
        selected_malformed
            .to_string()
            .contains("expected no arguments")
    );

    first
        .reinitialize(&module_name, serde_json::json!({"restricted":true}))
        .await
        .expect("same-mode reinitialization should use a fresh trusted context");
    let after_reinitialize: serde_json::Value =
        first_proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(after_reinitialize["restricted"], true);
    assert_eq!(after_reinitialize["generation"], 2);
    assert_eq!(
        after_reinitialize["factory_id"],
        first_initial["factory_id"]
    );

    assert!(
        first
            .reinitialize(&module_name, serde_json::json!({"restricted":false}))
            .await
            .is_err()
    );
    let after_refusal: serde_json::Value =
        first_proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(after_refusal, after_reinitialize);
    let second_still_isolated: serde_json::Value =
        second_proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(second_still_isolated["restricted"], false);
    assert_eq!(second_still_isolated["generation"], 1);
    controller.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_EAGER_MODULE to point at the built cdylib"]
async fn an_eager_cdylib_authenticates_after_its_worker_runs_before_admission() {
    authenticates_native_context("TINYBUS_TEST_CONTEXT_EAGER_MODULE").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MIGRATION_MODULE to point at the built cdylib"]
async fn a_compiled_migration_module_negotiates_before_ordinary_lease_registration() {
    let artifact = PathBuf::from(
        std::env::var_os("TINYBUS_TEST_MIGRATION_MODULE").expect("TINYBUS_TEST_MIGRATION_MODULE"),
    );
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);

    let broker = Broker::new_restricted_module_control();
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    let selected_controller = broker
        .attach_module_controller(Arc::new(broker_transport))
        .unwrap();
    let controller = Connection::connect(Box::new(controller_transport))
        .await
        .unwrap();
    assert_eq!(
        controller.unique_name().as_ref(),
        Some(&selected_controller)
    );
    let modules = ModuleHost::new(broker.clone());
    let info = modules.load_file(artifact).unwrap();

    modules
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
        .expect("compiled migration setup must receive the selected controller binding");
    let proxy = controller
        .proxy(
            info.manifest.bus_name.as_str(),
            "/migration",
            "org.example.TrustedMigration",
        )
        .unwrap();
    assert!(proxy.attestation().await.unwrap().is_some());

    let (other_server, other_client) = MemoryTransport::pair();
    broker.attach(Arc::new(other_server));
    let other = Connection::connect(Box::new(other_client)).await.unwrap();
    let other_proxy = other
        .proxy(
            info.manifest.bus_name.as_str(),
            "/migration",
            "org.example.TrustedMigration",
        )
        .unwrap();
    assert!(other_proxy.attestation().await.unwrap().is_some());
    assert!(proxy.call::<serde_json::Value>("Schema", ()).await.is_err());
    assert!(
        other_proxy
            .call_confidential::<serde_json::Value>("Schema", ())
            .await
            .is_err(),
        "recipient attestation alone must not authorize a different sender"
    );
    assert!(
        other_proxy
            .call_confidential::<serde_json::Value>("Transform", ("attacker-id",))
            .await
            .is_err(),
        "a denied caller cannot alter the migration transcript state"
    );
    assert!(
        other_proxy
            .call_confidential::<serde_json::Value>("RegisterOrdinaryLease", ())
            .await
            .is_err(),
        "a denied caller cannot register the ordinary lease"
    );
    let forged_identity = serde_json::json!({
        "authenticated_sender": controller.unique_name(),
        "is_confidential": true
    });
    assert!(
        other_proxy
            .call_confidential::<serde_json::Value>("Schema", (forged_identity,))
            .await
            .is_err(),
        "JSON claims cannot replace the broker-stamped sender"
    );
    let schema: serde_json::Value = proxy.call_confidential("Schema", ()).await.unwrap();
    assert_eq!(schema["required"], serde_json::json!(["id"]));
    let transformed: serde_json::Value = proxy
        .call_confidential("Transform", ("legacy-id",))
        .await
        .unwrap();
    assert_eq!(transformed["id"], "record-legacy-id");
    assert_eq!(transformed["ordinary_lease_registered"], false);
    let lease_registered: bool = proxy
        .call_confidential("RegisterOrdinaryLease", ())
        .await
        .unwrap();
    assert!(lease_registered);
}

async fn authenticates_native_context(variable: &str) {
    let artifact = PathBuf::from(std::env::var_os(variable).expect(variable));
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    let modules = ModuleHost::new(broker);
    let info = modules.load_file(artifact).unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    client
        .serve_at("/callback".parse().unwrap(), NativeContextCallback)
        .await
        .unwrap();
    let unique = client
        .name_owner("org.example.Context")
        .await
        .unwrap()
        .unwrap();
    let proxy = client
        .proxy(unique.as_str(), "/context", "org.example.Context")
        .unwrap();
    assert_eq!(
        proxy.attestation().await.unwrap(),
        Some(crate::attest::Attestation {
            name: info.manifest.bus_name.clone(),
            sha256: hash.clone(),
            library_sha256: Some(hash),
        })
    );
    let sender: crate::BusName = proxy.call_confidential("Inspect", ()).await.unwrap();
    assert_eq!(Some(sender.clone()), client.unique_name());
    let module = client
        .name_owner("org.example.Context")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        modules.authenticated_peer(&info.name),
        Some(module.clone()),
        "the peer returned by the host API must match the compiled module's broker-stamped sender"
    );
    assert!(
        modules
            .authenticated_peer(info.manifest.bus_name.as_str())
            .is_none()
    );
    let callback: (crate::BusName, crate::BusName) =
        proxy.call_confidential("Callback", ()).await.unwrap();
    assert_eq!(callback, (sender, module));
    modules
        .inner
        .stop(&info.name, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(modules.authenticated_peer(&info.name).is_none());
    task.abort();
}

#[tokio::test]
async fn fixed_unique_module_calls_are_refused_in_terminal_states_before_detach() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let mut lazy_manifest = manifest();
    lazy_manifest.lazy_init = true;
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            lazy_manifest,
            lazy_echo_init,
        )
    }
    .unwrap();
    let unique = host.inner.loaded.lock().unwrap()[0].unique_name.clone();
    broker.attest_module(
        &unique,
        crate::attest::Attestation {
            name: manifest().bus_name,
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client
        .proxy(
            unique.as_str(),
            "/ai/tinyhumans/module/Clock",
            "ai.tinyhumans.module.Clock",
        )
        .unwrap();
    for (state, expected) in [
        (ModuleState::Stopped, "stopped"),
        (ModuleState::Disabled, "disabled"),
        (
            ModuleState::Faulted {
                reason: "fixture".into(),
            },
            "faulted",
        ),
        (
            ModuleState::Failed {
                reason: "fixture".into(),
            },
            "failed",
        ),
    ] {
        // Keep the real transport attached while setting its terminal state:
        // this probes the pre-detach window without racing the reader task.
        host.inner.loaded.lock().unwrap()[0].info.state = state;
        assert!(host.authenticated_peer("clock").is_none());
        assert!(
            host.inner
                .unavailable_for(&client.unique_name().unwrap())
                .is_none()
        );
        assert!(
            matches!(host.inner.unavailable_for(&unique), Some(Error::ModuleUnavailable { state, .. }) if state == expected)
        );
        for confidential in [false, true] {
            let result = if confidential {
                proxy
                    .call_confidential::<String>("Echo", ("terminal",))
                    .await
            } else {
                proxy.call::<String>("Echo", ("terminal",)).await
            };
            assert_eq!(
                result.unwrap_err().wire_name(),
                "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
            );
        }
    }
    broker_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_MODULE to point at the built cdylib"]
async fn a_verified_cdylib_can_deliver_a_confidential_host_callback() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTEXT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    let modules = ModuleHost::new(broker);
    let binding = modules
        .prepare_host_callbacks(vec![crate::module::HostCallbackEndpoint {
            path: "/callback".parse().unwrap(),
            interface: "org.example.Callback".parse().unwrap(),
            members: vec!["Observe".parse().unwrap()],
        }])
        .await
        .unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    binding
        .serve_at(
            "/callback".parse().unwrap(),
            CountingNativeCallback(calls.clone()),
        )
        .await
        .unwrap();
    modules
        .load_file_with_host_callbacks(artifact, serde_json::json!({}), &binding)
        .await
        .unwrap();
    let module = binding.guard().module_peer().unwrap();
    let result = binding
        .proxy(module.as_str(), "/context", "org.example.Context")
        .unwrap()
        .call_confidential::<(crate::BusName, crate::BusName)>("ConfidentialCallback", ())
        .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "confidential callback required"
    );
    assert_eq!(
        result.unwrap(),
        (binding.unique_name().clone(), module.clone())
    );
    let ordinary = binding
        .proxy(module.as_str(), "/context", "org.example.Context")
        .unwrap()
        .call::<serde_json::Value>("Callback", ())
        .await;
    assert!(
        ordinary.is_err(),
        "scoped confidential endpoints refuse ordinary calls even from their module"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let attacker = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut forged = crate::message::Message::method_call(
        binding.unique_name().clone(),
        "/callback".parse().unwrap(),
        "org.example.Callback".parse().unwrap(),
        "Observe".parse().unwrap(),
        serde_json::json!([]),
    );
    forged.header.sender = Some(module.clone());
    forged.header.confidential = true;
    assert!(
        attacker
            .call_raw(forged, Duration::from_secs(2))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    binding.revoke();
    let revoked = binding
        .proxy(module.as_str(), "/context", "org.example.Context")
        .unwrap()
        .call_confidential::<serde_json::Value>("ConfidentialCallback", ())
        .await;
    assert!(revoked.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_MODULE to point at the built cdylib"]
async fn a_bound_cdylib_receives_only_its_declared_host_directed_signal() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTEXT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    let modules = ModuleHost::new(broker);
    let binding = modules
        .prepare_host_callbacks_with_signals(
            vec![crate::module::HostCallbackEndpoint {
                path: "/callback".parse().unwrap(),
                interface: "org.example.Callback".parse().unwrap(),
                members: vec!["Observe".parse().unwrap()],
            }],
            vec![crate::module::HostCallbackSignal {
                path: "/callback".parse().unwrap(),
                interface: "org.example.Callback".parse().unwrap(),
                members: vec!["WatchChanged".parse().unwrap()],
            }],
        )
        .await
        .unwrap();
    binding
        .serve_at("/callback".parse().unwrap(), NativeContextCallback)
        .await
        .unwrap();
    modules
        .load_file_with_host_callbacks(artifact, serde_json::json!({}), &binding)
        .await
        .unwrap();

    let outsider = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut outsider_signals = outsider
        .proxy("org.example.Callback", "/callback", "org.example.Callback")
        .unwrap()
        .receive_directed_signal("WatchChanged")
        .await
        .unwrap();
    let notice = serde_json::json!(["opaque-watch", {"kind":"invalidated","cursor":"opaque-next"}]);
    binding
        .emit_signal(
            &"/callback".parse().unwrap(),
            &"org.example.Callback".parse().unwrap(),
            &"WatchChanged".parse().unwrap(),
            &notice,
        )
        .await
        .unwrap();

    let module = binding.guard().module_peer().unwrap();
    let received: serde_json::Value = binding
        .proxy(module.as_str(), "/context", "org.example.Context")
        .unwrap()
        .call("WaitForWatchChanged", ())
        .await
        .unwrap();
    assert_eq!(received, notice);
    assert!(outsider_signals.try_recv().is_err());
    task.abort();
}

struct CountingNativeCallback(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl crate::Interface for CountingNativeCallback {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::failed("missing contextual dispatch"))
    }
    async fn call_with_context(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
        context: &crate::CallContext,
    ) -> crate::Result<serde_json::Value> {
        let sender = context
            .authenticated_sender()
            .ok_or_else(|| Error::failed("unverified callback"))?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!(sender))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_HOST_CALLBACK_MODULE to point at the built cdylib"]
async fn eager_native_setup_callbacks_are_bound_before_ready_and_failures_rollback() {
    for failed in [false, true] {
        let artifact =
            PathBuf::from(std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").unwrap());
        let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
        let (_dir, artifact) = staged_module(&artifact, &hash);
        let broker = Broker::new();
        let modules = ModuleHost::new(broker.clone());
        let binding = modules
            .prepare_host_callbacks(vec![crate::module::HostCallbackEndpoint {
                path: "/callback".parse().unwrap(),
                interface: "org.example.Callback".parse().unwrap(),
                members: vec!["Observe".parse().unwrap()],
            }])
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = binding.guard();
        let sender_lookup = modules.sender_lookup();
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                BoundNativeCallback {
                    calls: calls.clone(),
                    guard: guard.clone(),
                    sender_lookup,
                },
            )
            .await
            .unwrap();
        let config = serde_json::json!({
            "host": binding.unique_name(), "fail": failed,
            "credential": "callback-secret-must-not-be-retained",
            "nested": [{"zero": -0.0, "literal": "\"-0.0\""}],
        });
        let result = modules
            .load_file_with_host_callbacks(artifact, config.clone(), &binding)
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!failed));
        if failed {
            assert!(result.is_err());
            assert!(modules.list().is_empty());
            assert!(!guard.authorizes(&crate::CallContext::brokered(
                guard.module_peer().as_ref(),
                true
            )));
        } else {
            let info = result.unwrap();
            assert!(matches!(info.state, ModuleState::Ready));
            assert_eq!(
                Some(binding.unique_name().clone()),
                broker
                    .router
                    .lock()
                    .unwrap()
                    .owner_of(binding.unique_name())
            );
            assert!(
                modules
                    .reinitialize(
                        &info.name,
                        serde_json::json!({"host": ":1.999", "fail": false})
                    )
                    .await
                    .is_err()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let retains_plaintext = {
                let loaded = modules.inner.loaded.lock().unwrap();
                let retained = &loaded
                    .iter()
                    .find(|m| m.info.name == info.name)
                    .unwrap()
                    .callback_config;
                // Check the live retained representation, never freed memory after Drop.
                (retained as &dyn std::any::Any)
                    .downcast_ref::<Option<serde_json::Value>>()
                    .is_some_and(Option::is_some)
            };
            assert!(
                !retains_plaintext,
                "initialized callback module retains secret-bearing JSON"
            );
            let mut equivalent = config.clone();
            equivalent["nested"][0]["zero"] = serde_json::json!(0.0);
            assert_eq!(equivalent, config);
            modules.reinitialize(&info.name, equivalent).await.unwrap();
            let mut different = config.clone();
            different["nested"][0]["zero"] = serde_json::json!(0);
            assert_ne!(different, config);
            assert!(modules.reinitialize(&info.name, different).await.is_err());
            modules.reinitialize(&info.name, config).await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            drop(modules);
            assert!(!guard.authorizes(&crate::CallContext::brokered(
                guard.module_peer().as_ref(),
                true
            )));
        }
        binding.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_HOST_CALLBACK_MODULE to point at the built cdylib"]
async fn an_eager_setup_callback_resolves_its_admitted_module_before_ready() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let modules = ModuleHost::new(broker);
    let callback = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
    callback
        .serve_at(
            "/callback".parse().unwrap(),
            EagerSetupSenderLookup {
                lookup: modules.sender_lookup(),
                observed: std::sync::Mutex::new(Some(observed_tx)),
            },
        )
        .await
        .unwrap();

    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    *modules
        .inner
        .after_loaded_publish
        .lock()
        .expect("loaded publication barrier lock") = Some(LoadedPublishBarrier {
        reached: reached_tx,
        release: release_rx,
    });
    let config = serde_json::json!({
        "host": callback.unique_name(),
        "fail": false,
        "confidential": false
    });
    let loading_modules = ModuleHost {
        inner: modules.inner.clone(),
    };
    let mut loading = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { loading_modules.load_file_with_config(artifact, config) })
    });

    let reached = tokio::task::spawn_blocking(move || {
        reached_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("attach did not reach the pre-publication barrier");
    });
    tokio::select! {
        result = &mut loading => panic!("load completed before reaching the publication barrier: {result:?}"),
        result = reached => result.expect("publication barrier observer panicked"),
    }
    let observation = tokio::select! {
        result = &mut loading => panic!("load completed before its eager callback was observed: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(30), observed_rx) => result,
    };
    release_tx
        .send(())
        .expect("release the attachment after observing callback identity");
    let observed = observation
        .expect("eager setup did not call the authenticated host callback")
        .expect("host callback dropped its observation");
    loading.await.unwrap().expect("eager module loads");

    assert_eq!(
        observed.as_ref().map(crate::module::ModuleId::as_str),
        Some("tinybus"),
        "the authenticated callback must resolve the exact admitted module before READY"
    );
    broker_task.abort();
}

struct PanickingStagedStartSubscriber {
    host: Arc<ModuleHostInner>,
    peer: Arc<StdMutex<Option<crate::BusName>>>,
}

impl tracing::Subscriber for PanickingStagedStartSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::DEBUG
            && event.metadata().target().ends_with("broker")
        {
            let peer = self
                .host
                .loaded
                .lock()
                .unwrap()
                .last()
                .expect("staged module row is published before broker start")
                .unique_name
                .clone();
            *self.peer.lock().unwrap() = Some(peer);
            panic!("test subscriber panics while staged dispatch starts");
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[test]
fn a_staged_start_unwind_removes_its_exact_published_peer() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "module::host::tests::staged_start_unwind_child_process",
            "--test-threads=1",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed;"),
        "isolated staged-start unwind check failed or was not selected: {}{}",
        stdout,
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn staged_start_unwind_child_process() {
    if !std::env::args()
        .any(|argument| argument == "module::host::tests::staged_start_unwind_child_process")
    {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(staged_start_unwind_case());
}

async fn staged_start_unwind_case() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let peer = Arc::new(StdMutex::new(None));
    let mut manifest = named_manifest("staged-start-panic", "StagedStartPanic");
    manifest.lazy_init = true;

    let result = tracing::subscriber::with_default(
        PanickingStagedStartSubscriber {
            host: host.inner.clone(),
            peer: peer.clone(),
        },
        || {
            host.attach_linked_with_config(
                LinkedModule {
                    descriptor: TbAbiDescriptor::current("staged-start-panic", "0.1.0"),
                    manifest,
                    init: lazy_echo_init,
                    init_with_setup: None,
                },
                serde_json::json!({}),
            )
        },
    );
    assert!(
        result.is_err(),
        "a failed dispatch start must refuse attachment"
    );
    let unique = peer
        .lock()
        .unwrap()
        .clone()
        .expect("peer observed at panic");
    assert!(
        host.inner.loaded.lock().unwrap().is_empty(),
        "failed start removes the published row"
    );
    assert!(
        host.module_for_sender(&unique).is_none(),
        "failed start revokes lookup authority for the exact peer"
    );
    assert_eq!(
        observer
            .name_owner("ai.tinyhumans.module.StagedStartPanic")
            .await
            .unwrap(),
        None,
        "failed start detaches its reserved bus name"
    );
    broker_task.abort();
}

struct EagerSetupSenderLookup {
    lookup: crate::module::ModuleSenderLookup,
    observed:
        std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Option<crate::module::ModuleId>>>>,
}

#[async_trait::async_trait]
impl crate::Interface for EagerSetupSenderLookup {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::failed("callback requires authenticated dispatch"))
    }

    async fn call_with_context(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
        context: &crate::CallContext,
    ) -> crate::Result<serde_json::Value> {
        let sender = context
            .authenticated_sender()
            .ok_or_else(|| Error::failed("callback sender is unauthenticated"))?;
        let module = self.lookup.module_for_sender(sender);
        if let Some(observed) = self.observed.lock().unwrap().take() {
            let _ = observed.send(module);
        }
        Ok(serde_json::json!(sender))
    }
}

struct BoundNativeCallback {
    calls: Arc<AtomicUsize>,
    guard: crate::module::HostCallbackGuard,
    sender_lookup: crate::module::ModuleSenderLookup,
}
#[async_trait::async_trait]
impl crate::Interface for BoundNativeCallback {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::failed("unverified"))
    }
    async fn call_with_context(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
        context: &crate::CallContext,
    ) -> crate::Result<serde_json::Value> {
        assert_eq!(
            context.authenticated_sender(),
            self.guard.module_peer().as_ref()
        );
        assert!(
            self.guard.authorizes(context),
            "peer must be pinned before eager callback"
        );
        assert!(
            self.sender_lookup
                .module_for_sender(
                    context
                        .authenticated_sender()
                        .expect("authenticated sender")
                )
                .is_some(),
            "the admitted module identity must be published before its eager setup callback"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!(context.authenticated_sender().unwrap()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTEXT_MODULE to point at the built cdylib"]
async fn a_native_callback_timeout_and_revoke_do_not_claim_worker_completion() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTEXT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = staged_module(&artifact, &hash);
    let modules = ModuleHost::new(Broker::new());
    let binding = modules
        .prepare_host_callbacks(vec![crate::module::HostCallbackEndpoint {
            path: "/callback".parse().unwrap(),
            interface: "org.example.Callback".parse().unwrap(),
            members: vec!["Observe".parse().unwrap()],
        }])
        .await
        .unwrap();
    let guard = binding.guard();
    let (entered, mut receiver) = tokio::sync::mpsc::channel(1);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    binding
        .serve_at(
            "/callback".parse().unwrap(),
            NativeBlockingCallback {
                guard: guard.clone(),
                entered,
                release: release.clone(),
                published: published.clone(),
            },
        )
        .await
        .unwrap();
    modules
        .load_file_with_host_callbacks(artifact, serde_json::json!({}), &binding)
        .await
        .unwrap();
    let module = guard.module_peer().unwrap();
    let proxy = binding
        .proxy(module.as_str(), "/context", "org.example.Context")
        .unwrap()
        .with_timeout(Duration::from_millis(100));
    let request = tokio::spawn(async move {
        proxy
            .call_confidential::<serde_json::Value>("ConfidentialCallback", ())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(request.await.unwrap(), Err(Error::Timeout { .. })));
    assert_eq!(
        guard.running(),
        1,
        "caller timeout does not complete the host worker"
    );
    binding.revoke();
    assert_eq!(
        guard.running(),
        1,
        "revoke retains running admission charge"
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), guard.drained())
        .await
        .unwrap();
    assert_eq!(guard.running(), 0);
    assert_eq!(published.load(Ordering::SeqCst), 0);
    modules.shutdown(Duration::from_secs(2)).await;
}

struct NativeBlockingCallback {
    guard: crate::module::HostCallbackGuard,
    entered: tokio::sync::mpsc::Sender<()>,
    release: Arc<tokio::sync::Semaphore>,
    published: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl crate::Interface for NativeBlockingCallback {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::failed("unverified"))
    }
    async fn call_with_context(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
        context: &crate::CallContext,
    ) -> crate::Result<serde_json::Value> {
        assert!(self.guard.authorizes(context));
        self.entered.send(()).await.unwrap();
        let _permit = self.release.acquire().await.unwrap();
        if !self.guard.authorizes(context) {
            return Err(Error::failed("revoked before publication"));
        }
        self.published.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!(context.authenticated_sender().unwrap()))
    }
}

static BLOCKING_STOP_RECEIVER: OnceLock<StdMutex<Option<std::sync::mpsc::Receiver<()>>>> =
    OnceLock::new();
static BLOCKING_STOP_STARTED: AtomicBool = AtomicBool::new(false);
static BLOCKING_STOP_CALLS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn blocking_shutdown(_: *mut std::ffi::c_void, _: u64) -> i32 {
    BLOCKING_STOP_CALLS.fetch_add(1, Ordering::AcqRel);
    BLOCKING_STOP_STARTED.store(true, Ordering::Release);
    if let Some(receiver) = BLOCKING_STOP_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("H00")
        .take()
    {
        let _ = receiver.recv();
    }
    TB_OK
}
unsafe extern "C" fn blocking_shutdown_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    let code = unsafe { lazy_echo_init(host, out) };
    if code == TB_OK {
        unsafe {
            (*out).shutdown = blocking_shutdown;
        }
    }
    code
}
struct StopRelease(Option<std::sync::mpsc::SyncSender<()>>);
impl Drop for StopRelease {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn ordinary_stop_never_fabricates_faulted_before_or_after_callback_completion() {
    let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.expect("H18"))
        .await
        .expect("H19");
    let mut state_changes = observer
        .add_match(
            crate::router::MatchRule::parse(
                "type=signal,interface=ai.tinyhumans.tinybus.Bus,member=ModuleStateChanged",
            )
            .expect("H20"),
        )
        .await
        .expect("H21");
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let release = StopRelease(Some(sender));
    *BLOCKING_STOP_RECEIVER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("H01") = Some(receiver);
    BLOCKING_STOP_STARTED.store(false, Ordering::Release);
    BLOCKING_STOP_CALLS.store(0, Ordering::Release);
    unsafe {
        host.attach_raw(
            "clock.so",
            TbAbiDescriptor::current("clock", "0.1.0"),
            manifest(),
            blocking_shutdown_init,
        )
    }
    .expect("H02");
    tokio::time::timeout(Duration::from_secs(2), async {
        while host.list()[0].state != ModuleState::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("H03");
    let inner = host.inner.clone();
    let stopping =
        tokio::spawn(async move { inner.stop("clock", Duration::from_millis(100)).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !BLOCKING_STOP_STARTED.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("H04");
    assert!(stopping.await.expect("H05").is_err(), "H06");
    assert_eq!(host.list()[0].state, ModuleState::Ready, "H07");
    let retry = host.inner.stop("clock", Duration::from_millis(10)).await;
    assert!(matches!(retry, Err(Error::MethodFailed { .. })), "H08");
    assert_eq!(BLOCKING_STOP_CALLS.load(Ordering::Acquire), 1, "H09");
    // Hold the host observer's lock while the blocking callback returns. The
    // real broker cannot commit peer detach until this assertion is finished.
    // No await occurs under this lock, and the opaque callback needs no host lock.
    {
        let loaded = host.inner.loaded.lock().expect("H10");
        drop(release);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while loaded[0].transport.is_ready() {
            assert!(std::time::Instant::now() < deadline, "H11");
            std::thread::yield_now();
        }
        assert!(!loaded[0].transport.is_faulted(), "H12");
        // A disposed transport refuses a retry immediately, before any new
        // physical task or marker change, even while detach is held back.
        let mut retry = std::pin::pin!(loaded[0].transport.clone().stop(Duration::from_secs(1)));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            matches!(
                std::future::Future::poll(retry.as_mut(), &mut context),
                std::task::Poll::Ready(Err(StopError::NotStarted(Error::ConnectionClosed)))
            ),
            "H15"
        );
        assert!(loaded[0].transport.stop_task_spawned(), "H16");
        assert_eq!(BLOCKING_STOP_CALLS.load(Ordering::Acquire), 1, "H17");
        assert!(
            !matches!(
                loaded[0].snapshot().state,
                ModuleState::Faulted { .. } | ModuleState::Failed { .. }
            ),
            "H13"
        );
    }
    // This is a current-thread runtime. Without an await or yield after dropping
    // the loaded lock, the broker cannot process its ready detach task before
    // this real Host retry completes synchronously against the closed transport.
    {
        let mut retry = std::pin::pin!(host.inner.stop("clock", Duration::from_secs(1)));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            matches!(
                std::future::Future::poll(retry.as_mut(), &mut context),
                std::task::Poll::Ready(Err(Error::ConnectionClosed))
            ),
            "H22"
        );
    }
    let origin = host.inner.loaded.lock().expect("H23")[0]
        .transition_from
        .clone();
    assert!(matches!(origin, Some(ModuleState::Ready)), "H24");
    tokio::time::timeout(Duration::from_secs(2), async {
        while host.list()[0].state != ModuleState::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("H14");
    let stopped_event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = state_changes.recv().await.expect("H25");
            if message.body.get(2).and_then(serde_json::Value::as_str) == Some("stopped") {
                break message;
            }
        }
    })
    .await
    .expect("H26");
    assert!(
        stopped_event.body == serde_json::json!(["clock", "ready", "stopped", null]),
        "H27"
    );
    broker_task.abort();
}

async fn controlled_native_transport() -> (
    Arc<ModuleTransport>,
    crate::module::abi::TbHostVtable,
    Arc<super::super::transport::tests::NativeBarrier>,
    LoadedArtifact,
    PathBuf,
) {
    let (transport, view, init, barrier) = super::super::transport::tests::native_fixture();
    barrier.state.lock().unwrap().1 = true;
    transport.defer_initialize(init, view);
    transport.ensure_initialized().await.unwrap();
    barrier.entered.notified().await;
    let path = PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTROLLED_INIT_MODULE").unwrap());
    let artifact = loader::load(&path, false).unwrap();
    (transport, view, barrier, artifact, path)
}

fn attach_controlled_native(
    host: &ModuleHost,
    transport: Arc<ModuleTransport>,
    artifact: LoadedArtifact,
    path: &Path,
) -> Result<ModuleInfo> {
    let mut admitted = host.validate(path, &artifact.descriptor, &artifact.manifest)?;
    admitted.state = ModuleState::Initializing;
    host.attach_transport(
        path,
        admitted,
        artifact.manifest,
        transport,
        Activation {
            lazy_init: false,
            lazy_load: false,
            descriptor_info: None,
            pinned_sha256: None,
            pinned_library_sha256: None,
        },
    )
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_host_drop_keeps_terminal_tombstone_and_other_instance_live() {
    let (transport, view, barrier, artifact, path) = controlled_native_transport().await;
    let (other, other_view, other_barrier, _, _) = controlled_native_transport().await;
    let host = ModuleHost::new(Broker::new());
    attach_controlled_native(&host, transport.clone(), artifact, &path).unwrap();
    drop(host);
    assert_ne!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        1
    );
    assert_eq!(
        unsafe { other_view.native_terminal_state.unwrap()(other_view.host_ctx) },
        1
    );
    tokio::time::timeout(Duration::from_secs(2), barrier.joined.notified())
        .await
        .unwrap();
    let stopped = transport.clone().stop(Duration::from_secs(2)).await;
    assert!(matches!(stopped, Err(StopError::NotStarted(_))));
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        10
    );
    drop(transport);
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        10
    );
    other.request_terminal(false);
    tokio::time::timeout(Duration::from_secs(2), other_barrier.joined.notified())
        .await
        .unwrap();
    assert!(barrier.state.lock().unwrap().2);
    assert!(other_barrier.state.lock().unwrap().2);
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_reservation_rollback_retains_one_cleanup_and_fault_tombstone() {
    let (first, first_view, first_barrier, artifact, path) = controlled_native_transport().await;
    let (rejected, view, barrier, other_artifact, _) = controlled_native_transport().await;
    let host = ModuleHost::new(Broker::new());
    attach_controlled_native(&host, first, artifact, &path).unwrap();
    assert!(attach_controlled_native(&host, rejected.clone(), other_artifact, &path).is_err());
    assert_ne!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        1
    );
    assert_eq!(
        unsafe { first_view.native_terminal_state.unwrap()(first_view.host_ctx) },
        1
    );
    tokio::time::timeout(Duration::from_secs(2), barrier.joined.notified())
        .await
        .unwrap();
    let _observed = rejected.clone().stop(Duration::from_secs(2)).await;
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        14
    );
    assert!(rejected.ensure_initialized().await.is_err());
    drop(rejected);
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        14
    );
    drop(host);
    tokio::time::timeout(Duration::from_secs(2), first_barrier.joined.notified())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_fault_during_ordinary_stop_keeps_fault_state_and_single_join() {
    let (transport, view, barrier, artifact, path) = controlled_native_transport().await;
    let host = Arc::new(ModuleHost::new(Broker::new()));
    let info = attach_controlled_native(&host, transport.clone(), artifact, &path).unwrap();
    let (release, release_rx) = std::sync::mpsc::channel();
    *barrier.finish_gate.lock().unwrap() = Some(release_rx);
    let stopping = tokio::spawn({
        let host = host.clone();
        async move {
            host.inner
                .stop_inner(&info.name, Duration::from_secs(2))
                .await
        }
    });
    barrier.joined.notified().await;
    assert!(transport.stop_task_spawned());
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        2,
        "joined worker is not yet successful opaque return"
    );
    unsafe {
        (view.fault)(view.host_ctx, std::ptr::null(), 0);
    }
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        6
    );
    release.send(()).unwrap();
    let result = stopping.await.unwrap().unwrap();
    assert!(
        matches!(result.state, ModuleState::Faulted { .. }),
        "late ordinary completion cannot replace actual fault"
    );
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        14
    );
    assert!(barrier.state.lock().unwrap().2);
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_rejected_stop_submission_retains_guard_through_host_drop() {
    let (transport, view, barrier, artifact, path) = controlled_native_transport().await;
    let host = ModuleHost::new(Broker::new());
    attach_controlled_native(&host, transport.clone(), artifact, &path).unwrap();
    transport.reject_worker_spawn.store(true, Ordering::Release);
    assert!(matches!(
        transport.clone().stop(Duration::from_secs(1)).await,
        Err(StopError::Started(_))
    ));
    let weak = Arc::downgrade(&transport);
    drop(host);
    drop(transport);
    let retained = weak
        .upgrade()
        .expect("external anchor retains physical owner through real Host Drop");
    assert!(retained.lifecycle_is_owned_for_test());
    assert!(retained.stop_task_spawned());
    assert!(!retained.is_faulted());
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        2
    );
    assert!(
        !barrier.state.lock().unwrap().2,
        "rejected submission is not physical completion"
    );
    retained.reject_worker_spawn.store(false, Ordering::Release);
    retained.request_terminal(false);
    assert!(
        !barrier.state.lock().unwrap().2,
        "Host Drop cannot refund and resubmit an uncertain owner"
    );
    // This one uncertain physical owner retains its actual controller.
    // It makes no process-wide leak or backing-Drop claim.
    let controller = Arc::downgrade(&barrier);
    drop(barrier);
    assert!(controller.upgrade().is_some());
}

#[test]
fn search_skip_matches_file_stems_only() {
    let skip = vec!["libclock".to_string(), "clock".to_string()];
    assert!(is_search_skipped(Path::new("/m/libclock.so"), &skip));
    assert!(is_search_skipped(Path::new("/m/clock.dll"), &skip));
    assert!(!is_search_skipped(Path::new("/m/libclocks.so"), &skip));
    assert!(!is_search_skipped(
        Path::new("/m/libclock"),
        &["clock".into()]
    ));
    assert!(!is_search_skipped(Path::new("/m/libclock.so"), &[]));
}

#[test]
fn an_explicit_search_path_config_preserves_env_first_order_without_product_defaults() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let env_dir = root.path().join("operator-modules");
    std::fs::create_dir(&env_dir).unwrap();
    let env_var = "TINYBUS_TEST_MODULE_SEARCH_PATHS";
    unsafe { std::env::set_var(env_var, &env_dir) };
    let app_dir = PathBuf::from("tinydemo/modules");

    let host = ModuleHost::new(Broker::new())
        .with_search_path_config(SearchPathConfig::new(env_var, app_dir.clone()));
    let paths = host.search_paths();
    unsafe { std::env::remove_var(env_var) };

    let mut expected = vec![env_dir];
    #[cfg(target_os = "linux")]
    {
        if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
            expected.push(PathBuf::from(path).join(&app_dir));
        } else if let Some(path) = std::env::var_os("HOME") {
            expected.push(PathBuf::from(path).join(".local/share").join(&app_dir));
        }
        expected.push(PathBuf::from("/usr/lib").join(&app_dir));
    }
    #[cfg(target_os = "macos")]
    expected.push(PathBuf::from("/usr/local/lib").join(&app_dir));
    #[cfg(windows)]
    if let Some(path) = std::env::var_os("LOCALAPPDATA") {
        expected.push(PathBuf::from(path).join(&app_dir));
    }

    assert_eq!(paths, expected);
    assert!(!paths.iter().any(|path| path.ends_with("openhuman/modules")));
}

#[test]
fn a_host_without_search_path_config_has_no_implicit_search_paths() {
    let host = ModuleHost::new(Broker::new());

    assert!(host.search_paths().is_empty());
}

#[test]
fn rescan_with_no_explicit_paths_uses_the_host_search_path_config() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let directory = root.path().join("rescan-modules");
    super::super::windows_acl::create_private_dir_all(&directory).unwrap();
    let file_name = if cfg!(windows) {
        "rescan-invalid.dll"
    } else if cfg!(target_os = "macos") {
        "rescan-invalid.dylib"
    } else {
        "rescan-invalid.so"
    };
    std::fs::write(directory.join(file_name), b"not a shared library").unwrap();
    let env_var = "TINYBUS_TEST_RESCAN_SEARCH_PATH";
    unsafe { std::env::set_var(env_var, &directory) };
    let host = ModuleHost::new(Broker::new())
        .with_search_path_config(SearchPathConfig::new(env_var, "tinybus-rescan/modules"));

    let (modules, transitions) = host.inner.clone().rescan(Vec::new(), true).unwrap();
    unsafe { std::env::remove_var(env_var) };

    assert!(transitions.is_empty());
    assert!(
        matches!(modules.as_slice(), [module] if matches!(module.state, ModuleState::Rejected { .. }))
    );
}

#[test]
fn load_search_paths_except_skips_an_artifact_before_opening_it() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let directory = root.path().join("private-modules");
    super::super::windows_acl::create_private_dir_all(&directory).unwrap();
    let file_name = if cfg!(windows) {
        "skip-before-open.dll"
    } else if cfg!(target_os = "macos") {
        "skip-before-open.dylib"
    } else {
        "skip-before-open.so"
    };
    // Invalid bytes make admission fail if the skip filter opens the artifact.
    std::fs::write(directory.join(file_name), b"not a shared library").unwrap();
    let env_var = "TINYBUS_TEST_SKIP_SEARCH_PATH";
    unsafe { std::env::set_var(env_var, &directory) };
    let host = ModuleHost::new(Broker::new())
        .with_search_path_config(SearchPathConfig::new(env_var, "tinybus-test/modules"));

    let outcomes = host.load_search_paths_except(&["skip-before-open".to_string()]);
    unsafe { std::env::remove_var(env_var) };

    assert!(outcomes.is_empty(), "the skipped file must never be opened");
}
