//! Host module tests, including raw-vtable fixtures and real-loader cases.

use super::*;
use crate::Connection;
use crate::module::abi::TbAbiDescriptor;
use crate::transport::memory::MemoryBus;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static INIT_RAN: AtomicBool = AtomicBool::new(false);
static LAZY_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static FAILED_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static FAKE_MODULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        };
    }
    TB_OK
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

fn manifest() -> ModuleManifest {
    named_manifest("clock", "Clock")
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
async fn a_module_whose_init_fails_is_terminal_and_is_never_initialized_again() {
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
        .unwrap()
        .with_timeout(Duration::from_millis(20));
    let error = proxy.call::<()>("Hang", ()).await.unwrap_err();
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
    let stopped = control
        .stop("clock", Duration::from_millis(1))
        .await
        .unwrap();
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
    assert!(text.contains(crate::VERSION), "{text}");
    assert!(text.contains("99.1.0"), "{text}");
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
            metadata.permissions().mode(),
            metadata.uid()
        ),
        Some("module directory is writable by another user")
    );
}

#[cfg(unix)]
#[test]
fn a_module_directory_owned_by_another_user_is_refused() {
    assert_eq!(
        unix_directory_refusal(1_001, 0o755, 1_000),
        Some("module directory is owned by another user")
    );
    assert_eq!(unix_directory_refusal(0, 0o755, 1_000), None);
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
    assert_eq!(states.iter().map(state_name).collect::<Vec<_>>().len(), 11);
    assert_eq!(state_detail(&states[1]), Some("no"));
    assert_eq!(state_detail(&states[0]), None);
    assert_eq!(
        sanitized_field(b"clock\0ignored"),
        Some("clock".to_string())
    );
    assert_eq!(sanitized_field(b"bad\nname"), None);
    assert_eq!(safe_file_name(Path::new("/private/clock.so")), "clock.so");
    assert_eq!(safe_file_name(Path::new("/")), "module");
    assert!(has_library_extension(Path::new(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    })));
    assert!(!has_library_extension(Path::new("clock.txt")));

    let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let module = directory.path().join(if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    });
    std::fs::write(&module, b"module bytes").unwrap();
    assert!(check_file(&module).is_ok());
    std::fs::write(directory.path().join("modules.toml"), "other = \"00\"\n").unwrap();
    assert!(
        check_file(&module)
            .unwrap_err()
            .to_string()
            .contains("absent")
    );
    std::fs::write(
        directory.path().join("modules.toml"),
        "clock = \"not-a-hash\"\n",
    )
    .unwrap();
    assert!(
        check_file(&module)
            .unwrap_err()
            .to_string()
            .contains("invalid hash")
    );
    let refused = Error::module_refused(&module, "nope");
    let info = rejection_info(&refused);
    assert!(matches!(info.state, ModuleState::Rejected { .. }));
}

#[test]
fn an_allowlist_with_a_mismatched_hash_refuses_the_file() {
    let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    let path = directory.path().join(format!("clock.{extension}"));
    std::fs::write(&path, b"not a module").unwrap();
    std::fs::write(
        directory.path().join("modules.toml"),
        format!("clock.{extension} = \"{}\"\n", "0".repeat(64)),
    )
    .unwrap();
    let error = check_file(&path).unwrap_err();
    assert!(error.to_string().contains("hash does not match"), "{error}");
}

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
    let name_change = tokio::time::timeout(Duration::from_secs(2), name_changes.recv())
        .await
        .unwrap()
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
