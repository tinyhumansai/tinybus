use super::*;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
use std::sync::{OnceLock, mpsc::SyncSender};

static HOST_SEND_CODE: AtomicI32 = AtomicI32::new(TB_OK);
static HOST_WAKES: AtomicUsize = AtomicUsize::new(0);
static HOST_LOGS: AtomicUsize = AtomicUsize::new(0);
static HOST_READY: AtomicBool = AtomicBool::new(false);
static HOST_FAULTED: AtomicBool = AtomicBool::new(false);
static START_OUTGOING: OnceLock<StdMutex<Option<SyncSender<Vec<u8>>>>> = OnceLock::new();
// These tests share host callbacks and the module's process-global runtime
// capture, so overlapping tests would make their assertions order-dependent.
static HOST_STATE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn host_state_guard() -> tokio::sync::MutexGuard<'static, ()> {
    HOST_STATE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn blocking_host_state_guard() -> tokio::sync::MutexGuard<'static, ()> {
    HOST_STATE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .blocking_lock()
}

unsafe extern "C" fn host_send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    HOST_SEND_CODE.load(Ordering::Acquire)
}

unsafe extern "C" fn capture_host_send(_: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    let Some(sender) = START_OUTGOING
        .get()
        .expect("startup capture is initialized")
        .lock()
        .expect("startup capture lock")
        .clone()
    else {
        return TB_CLOSED;
    };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    sender.send(bytes).map_or(TB_CLOSED, |_| TB_OK)
}

unsafe extern "C" fn host_wake(_: *mut c_void) {
    HOST_WAKES.fetch_add(1, Ordering::AcqRel);
}

unsafe extern "C" fn host_log(_: *mut c_void, level: u32, _: *const u8, _: usize) {
    HOST_LOGS.store(level as usize, Ordering::Release);
}

unsafe extern "C" fn host_fault(_: *mut c_void, _: *const u8, _: usize) {
    HOST_FAULTED.store(true, Ordering::Release);
}

unsafe extern "C" fn host_ready(_: *mut c_void) {
    HOST_READY.store(true, Ordering::Release);
}

fn host(config: &[u8]) -> TbHostVtable {
    TbHostVtable {
        size: size_of::<TbHostVtable>() as u32,
        _reserved: 0,
        host_ctx: std::ptr::null_mut(),
        send: host_send,
        wake: host_wake,
        log: host_log,
        fault: host_fault,
        config: tinybus::module::abi::TbSlice {
            ptr: config.as_ptr(),
            len: config.len(),
        },
        ready: host_ready,
        broker_routing: None,
    }
}

fn message() -> Message {
    Message::signal(
        "/org/example/Module".parse().unwrap(),
        "org.example.Module".parse().unwrap(),
        "Changed".parse().unwrap(),
        serde_json::Value::Null,
    )
}

#[test]
fn a_module_whose_queue_is_full_reports_backpressure_rather_than_blocking_the_broker() {
    let (sender, _receiver) = mpsc::channel(1);
    sender.try_send(vec![1]).unwrap();
    let state = RuntimeState {
        inbound: StdMutex::new(Some(sender)),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let bytes = b"{}";
    let code = unsafe {
        deliver(
            std::ptr::from_ref(&state).cast_mut().cast(),
            bytes.as_ptr(),
            bytes.len(),
        )
    };
    assert_eq!(code, TB_BACKPRESSURE);
}

#[test]
fn a_frame_over_the_size_cap_is_rejected_rather_than_truncated() {
    let state = RuntimeState {
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let code = unsafe {
        deliver(
            std::ptr::from_ref(&state).cast_mut().cast(),
            std::ptr::NonNull::<u8>::dangling().as_ptr(),
            MAX_FRAME_LEN + 1,
        )
    };
    assert_eq!(code, TB_BAD_ARGUMENT);
}

#[test]
fn a_new_module_never_writes_past_an_older_hosts_vtable_capacity() {
    #[repr(C)]
    struct GuardedPrefix {
        table: TbModuleVtableV1Prefix,
        guard: u64,
    }

    let mut output = GuardedPrefix {
        table: TbModuleVtableV1Prefix {
            size: TB_MODULE_VTABLE_BASE_SIZE,
            _reserved: 0,
            module_ctx: std::ptr::null_mut(),
            deliver,
            shutdown,
        },
        guard: 0xfeed_face_dead_beef,
    };
    let capacity = output.table.size;
    unsafe {
        write_module_vtable(
            std::ptr::from_mut(&mut output.table).cast(),
            capacity,
            std::ptr::dangling_mut(),
            true,
        );
    }
    assert_eq!(output.table.size, TB_MODULE_VTABLE_BASE_SIZE);
    assert_eq!(output.table.module_ctx, std::ptr::dangling_mut());
    assert_eq!(output.guard, 0xfeed_face_dead_beef);
}

fn invalid_manifest_declaration() -> ManifestDeclaration<'static> {
    ManifestDeclaration {
        name: "invalid",
        version: "not-semver",
        provides: &[],
        methods: &[],
        signals: &[],
        requires: &[],
        optional: &[],
        lazy: false,
        worker_threads: 1,
    }
}

#[test]
fn a_manifest_declaration_exports_the_declared_surface_and_dependencies() {
    let invalid = manifest_slice(invalid_manifest_declaration());
    assert!(invalid.ptr.is_null());
    assert_eq!(invalid.len, 0);
    let slice = manifest_slice(ManifestDeclaration {
        name: "clock",
        version: "1.2.3",
        provides: &["ai.tinyhumans.module.Clock", "ai.tinyhumans.module.Time"],
        methods: &["Now"],
        signals: &["Changed"],
        requires: &["ai.tinyhumans.module.System"],
        optional: &["ai.tinyhumans.module.Optional"],
        lazy: true,
        worker_threads: 2,
    });
    let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
    let manifest: tinybus::module::manifest::ModuleManifest =
        serde_json::from_slice(bytes).unwrap();
    assert_eq!(manifest.module.name, "clock");
    assert_eq!(manifest.provides.len(), 2);
    assert_eq!(manifest.provides[0].methods[0].as_str(), "Now");
    assert_eq!(manifest.provides[0].signals[0].as_str(), "Changed");
    assert_eq!(manifest.requires.len(), 2);
    assert!(manifest.requires[1].optional);
    assert!(manifest.lazy_init);
}

#[test]
fn a_panicking_shutdown_callback_reports_panicked_not_timed_out() {
    let state = RuntimeState {
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _guard = state.runtime.lock().unwrap();
        panic!("poison runtime lock");
    }));
    let code = unsafe { shutdown(std::ptr::from_ref(&state).cast_mut().cast(), 1) };
    assert_eq!(code, TB_PANICKED);
}

#[test]
fn deliver_and_shutdown_validate_their_arguments_and_closed_state() {
    assert_eq!(
        unsafe { deliver(std::ptr::null_mut(), std::ptr::null(), 0) },
        TB_BAD_ARGUMENT
    );
    assert_eq!(
        unsafe { shutdown(std::ptr::null_mut(), 1) },
        TB_BAD_ARGUMENT
    );
    let state = RuntimeState {
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let bytes = b"{}";
    assert_eq!(
        unsafe {
            deliver(
                std::ptr::from_ref(&state).cast_mut().cast(),
                bytes.as_ptr(),
                bytes.len(),
            )
        },
        TB_CLOSED
    );
    assert_eq!(
        unsafe { shutdown(std::ptr::from_ref(&state).cast_mut().cast(), 1) },
        TB_CLOSED
    );
}

#[tokio::test]
async fn module_transport_maps_host_results_and_wakes_after_receiving() {
    let _host_state = host_state_guard().await;
    let (sender, receiver) = mpsc::channel(2);
    let transport = ModuleTransport {
        host: HostCalls(host(&[])),
        inbound: Mutex::new(receiver),
        detach_on_panic: true,
    };
    HOST_SEND_CODE.store(TB_BACKPRESSURE, Ordering::Release);
    assert!(matches!(
        transport.send(message()).await,
        Err(Error::Backpressure)
    ));
    HOST_SEND_CODE.store(TB_CLOSED, Ordering::Release);
    assert!(matches!(
        transport.send(message()).await,
        Err(Error::ConnectionClosed)
    ));
    HOST_SEND_CODE.store(TB_BAD_ARGUMENT, Ordering::Release);
    assert!(matches!(
        transport.send(message()).await,
        Err(Error::Transport { .. })
    ));
    HOST_SEND_CODE.store(TB_OK, Ordering::Release);
    HOST_FAULTED.store(false, Ordering::Release);
    let mut panic_message = message();
    panic_message.header.error_name = Some(MODULE_PANIC_ERROR.to_string());
    transport.send(panic_message).await.unwrap();
    assert!(HOST_FAULTED.load(Ordering::Acquire));
    HOST_WAKES.store(0, Ordering::Release);
    sender
        .send(serde_json::to_vec(&message()).unwrap())
        .await
        .unwrap();
    assert_eq!(transport.recv().await.unwrap().unwrap(), message());
    assert_eq!(HOST_WAKES.load(Ordering::Acquire), 1);
    transport.close().await.unwrap();
    assert!(transport.recv().await.unwrap().is_none());
    assert_eq!(transport.describe(), "module");
}

#[tokio::test]
async fn host_calls_and_subscriber_forward_logs_at_their_original_level() {
    let _host_state = host_state_guard().await;
    let calls = HostCalls(host(&[]));
    HOST_SEND_CODE.store(TB_OK, Ordering::Release);
    HOST_WAKES.store(0, Ordering::Release);
    HOST_LOGS.store(0, Ordering::Release);
    HOST_READY.store(false, Ordering::Release);
    HOST_FAULTED.store(false, Ordering::Release);
    assert_eq!(calls.send(b"frame"), TB_OK);
    calls.wake();
    calls.log(4, b"debug");
    calls.ready();
    calls.fault();
    assert_eq!(HOST_WAKES.load(Ordering::Acquire), 1);
    assert_eq!(HOST_LOGS.load(Ordering::Acquire), 4);
    assert!(HOST_READY.load(Ordering::Acquire));
    assert!(HOST_FAULTED.load(Ordering::Acquire));

    let subscriber = HostSubscriber {
        host: calls,
        next_span: AtomicU64::new(1),
        max_level: tracing::level_filters::LevelFilter::TRACE,
    };
    tracing::subscriber::with_default(subscriber, || {
        tracing::error!("module error");
        tracing::warn!("module warning");
        tracing::info!(answer = 42, "module log");
        tracing::debug!("module debug");
        tracing::trace!("module trace");
    });
    let span = tracing::span::Id::from_u64(1);
    let subscriber = HostSubscriber {
        host: calls,
        next_span: AtomicU64::new(1),
        max_level: tracing::level_filters::LevelFilter::TRACE,
    };
    tracing::Subscriber::record_follows_from(&subscriber, &span, &span);
    tracing::Subscriber::enter(&subscriber, &span);
    tracing::Subscriber::exit(&subscriber, &span);
    assert_eq!(HOST_LOGS.load(Ordering::Acquire), 5);
}

#[tokio::test]
async fn start_functions_reject_invalid_host_and_config_before_spawning_a_runtime() {
    let _host_state = host_state_guard().await;
    let mut out = TbModuleVtable::default();
    assert_eq!(
        unsafe { start_module(std::ptr::null(), &mut out, 1, true, |_| async { Ok(()) }) },
        TB_BAD_ARGUMENT
    );
    let mut short = host(&[]);
    short.size = 0;
    assert_eq!(
        unsafe { start_module(&short, &mut out, 1, true, |_| async { Ok(()) }) },
        TB_BAD_ARGUMENT
    );
    let config = b"not json";
    let invalid_config = host(config);
    assert_eq!(
        unsafe {
            start_module_with_config::<u32, _, _>(
                &invalid_config,
                &mut out,
                1,
                true,
                |_, _| async { Ok(()) },
            )
        },
        TB_BAD_ARGUMENT
    );
    let (_tx, only_once) = std::sync::mpsc::channel::<()>();
    assert_eq!(
        unsafe {
            start_module_with_config::<serde_json::Value, _, _>(
                std::ptr::null(),
                &mut out,
                1,
                true,
                move |_, _| async move {
                    drop(only_once);
                    Ok(())
                },
            )
        },
        TB_BAD_ARGUMENT
    );
}

#[test]
fn configured_reinitializations_are_serialized_bounded_and_keep_the_runtime_alive() {
    let _host_state = blocking_host_state_guard();
    HOST_READY.store(false, Ordering::Release);
    HOST_SEND_CODE.store(TB_OK, Ordering::Release);
    HOST_FAULTED.store(false, Ordering::Release);
    let config = br#"{"answer":42}"#;
    let (outgoing_tx, outgoing_rx) = std::sync::mpsc::sync_channel(2);
    let capture = START_OUTGOING.get_or_init(|| StdMutex::new(None));
    *capture.lock().expect("startup capture lock") = Some(outgoing_tx);
    let mut host = host(config);
    host.send = capture_host_send;
    let mut out = TbModuleVtable::default();
    let observed = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let setup_observed = observed.clone();
    let setup_active = active.clone();
    let setup_max_active = max_active.clone();
    let code = unsafe {
        start_reconfigurable_module::<serde_json::Value, _, _>(
            &host,
            &mut out,
            1,
            true,
            move |_, parsed| {
                let observed = setup_observed.clone();
                let active = setup_active.clone();
                let max_active = setup_max_active.clone();
                async move {
                    if parsed["hang"].as_bool() == Some(true) {
                        std::future::pending::<()>().await;
                    }
                    let waits = parsed["wait"].as_bool() == Some(true);
                    if waits {
                        let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                        max_active.fetch_max(now, Ordering::AcqRel);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        active.fetch_sub(1, Ordering::AcqRel);
                    }
                    observed.store(
                        parsed["answer"].as_u64().unwrap() as usize,
                        Ordering::Release,
                    );
                    Ok(())
                }
            },
        )
    };
    assert_eq!(code, TB_OK);
    let captured = outgoing_rx.recv_timeout(Duration::from_secs(1));
    let hello: Message =
        serde_json::from_slice(&captured.expect("module did not send Hello")).unwrap();
    let reply =
        Message::method_return(&hello.header, serde_json::Value::String(":1.1".to_string()));
    let reply = serde_json::to_vec(&reply).unwrap();
    assert_eq!(
        unsafe { (out.deliver)(out.module_ctx, reply.as_ptr(), reply.len()) },
        TB_OK
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !HOST_READY.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "module did not become ready"
        );
        std::thread::yield_now();
    }
    assert_eq!(observed.load(Ordering::Acquire), 42);
    let callback = out.reinitialize.expect("configured module supports reinit");
    let replacement = br#"{"answer":7}"#;
    let replacement_code =
        unsafe { callback(out.module_ctx, replacement.as_ptr(), replacement.len()) };
    assert_eq!(replacement_code, TB_OK);
    assert_eq!(observed.load(Ordering::Acquire), 7);
    let context = out.module_ctx as usize;
    let first = std::thread::spawn(move || {
        let config = br#"{"answer":8,"wait":true}"#;
        unsafe { callback(context as *mut c_void, config.as_ptr(), config.len()) }
    });
    let context = out.module_ctx as usize;
    let second = std::thread::spawn(move || {
        let config = br#"{"answer":9,"wait":true}"#;
        unsafe { callback(context as *mut c_void, config.as_ptr(), config.len()) }
    });
    let first_code = first.join().unwrap();
    let second_code = second.join().unwrap();
    assert_eq!(first_code, TB_OK);
    assert_eq!(second_code, TB_OK);
    assert_eq!(max_active.load(Ordering::Acquire), 1);
    let hanging = br#"{"answer":10,"hang":true}"#;
    let hanging_code = unsafe { callback(out.module_ctx, hanging.as_ptr(), hanging.len()) };
    assert_eq!(hanging_code, TB_TIMEOUT);
    assert_eq!(
        unsafe { callback(out.module_ctx, b"bad".as_ptr(), 3) },
        TB_BAD_ARGUMENT
    );
    assert_eq!(
        unsafe { (out.deliver)(out.module_ctx, std::ptr::null(), 0) },
        TB_BAD_ARGUMENT
    );
    assert_eq!(unsafe { (out.shutdown)(out.module_ctx, 10) }, TB_OK);
    assert_eq!(unsafe { (out.shutdown)(out.module_ctx, 10) }, TB_CLOSED);

    // The first start above installed this module copy's global subscriber, so
    // a second dynamic start cannot replace it and refuses with `TB_CLOSED`
    // rather than running a module with the wrong logging hook.
    let mut second = TbModuleVtable::default();
    let again = self::host(config);
    assert_eq!(
        unsafe {
            start_module_with_config::<serde_json::Value, _, _>(
                &again,
                &mut second,
                1,
                true,
                |_, _| async { Ok(()) },
            )
        },
        TB_CLOSED
    );
}

/// Startup refuses a host vtable it cannot trust, before it builds anything.
///
/// Every branch here returns before a runtime, a panic hook or a transport
/// exists, which is the property worth pinning: this is the first code a
/// `dlopen`ed module runs, it is handed a raw pointer by the host, and the
/// refusals are what stand between a malformed descriptor and a
/// dereference. A regression would not fail loudly — it would read off the
/// end of a struct the host never filled in.
///
/// Deliberately takes no host-state guard: none of these paths touches the
/// shared statics or the module's process-global runtime capture, because
/// none of them gets far enough to.
#[test]
fn configured_startup_refuses_a_host_vtable_it_cannot_trust() {
    fn attempt(host: *const TbHostVtable) -> i32 {
        let mut out = TbModuleVtable::default();
        unsafe {
            start_module_with_config::<serde_json::Value, _, _>(
                host,
                &mut out,
                1,
                true,
                // Never runs: every case below is refused before setup.
                |_, _| async { Ok(()) },
            )
        }
    }

    assert_eq!(
        attempt(std::ptr::null()),
        TB_BAD_ARGUMENT,
        "a null host vtable is refused rather than dereferenced"
    );

    // A host built against an older, smaller descriptor. Reading our
    // fields out of it would run off the end of what it allocated.
    let mut truncated = host(b"{}");
    truncated.size = tinybus::module::abi::TB_HOST_VTABLE_BASE_SIZE - 1;
    assert_eq!(
        attempt(&truncated),
        TB_BAD_ARGUMENT,
        "a vtable shorter than this ABI's is refused"
    );

    // A length with no buffer behind it.
    let mut dangling = host(b"{}");
    dangling.config.ptr = std::ptr::null();
    dangling.config.len = 8;
    assert_eq!(
        attempt(&dangling),
        TB_BAD_ARGUMENT,
        "a config length with a null pointer is refused"
    );

    // Past the 1 MiB config cap. The pointer is valid; only the claimed
    // length is not, so this asserts the cap and not the null check.
    let real = b"{}";
    let mut oversized = host(real);
    oversized.config.len = 1024 * 1024 + 1;
    assert_eq!(
        attempt(&oversized),
        TB_BAD_ARGUMENT,
        "a config past the size cap is refused"
    );

    // Well-formed descriptor, unparseable config for the declared type.
    let malformed = host(b"not json");
    assert_eq!(
        attempt(&malformed),
        TB_BAD_ARGUMENT,
        "a config that does not deserialize is refused"
    );

    // A config the module cannot deserialize into its own type is the same
    // refusal, which is what stops a type mismatch reaching `setup`.
    let wrong_shape = host(br#"{"answer":42}"#);
    let mut out = TbModuleVtable::default();
    assert_eq!(
        unsafe {
            start_module_with_config::<Vec<String>, _, _>(
                &wrong_shape,
                &mut out,
                1,
                true,
                |_, _| async { Ok(()) },
            )
        },
        TB_BAD_ARGUMENT,
        "an object is not a Vec<String>, and that is caught before setup"
    );
}

#[test]
fn deliver_reports_closed_when_the_modules_queue_receiver_is_gone() {
    let (tx, rx) = mpsc::channel::<Vec<u8>>(1);
    drop(rx);
    let state = RuntimeState {
        inbound: StdMutex::new(Some(tx)),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let bytes = b"{}";
    assert_eq!(
        unsafe {
            deliver(
                std::ptr::from_ref(&state).cast_mut().cast(),
                bytes.as_ptr(),
                bytes.len(),
            )
        },
        TB_CLOSED
    );
}

#[test]
fn reinitialize_validates_its_arguments_and_reports_closed_without_a_handler() {
    let bytes = b"{}";
    assert_eq!(
        unsafe { reinitialize(std::ptr::null_mut(), bytes.as_ptr(), bytes.len()) },
        TB_BAD_ARGUMENT
    );
    let state = RuntimeState {
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: None,
    };
    let ctx = std::ptr::from_ref(&state).cast_mut().cast();
    assert_eq!(
        unsafe { reinitialize(ctx, std::ptr::null(), 1) },
        TB_BAD_ARGUMENT
    );
    assert_eq!(
        unsafe { reinitialize(ctx, bytes.as_ptr(), 1024 * 1024 + 1) },
        TB_BAD_ARGUMENT
    );
    assert_eq!(
        unsafe { reinitialize(ctx, bytes.as_ptr(), bytes.len()) },
        TB_CLOSED
    );
}

#[tokio::test]
async fn the_host_subscriber_hands_out_distinct_span_ids_and_accepts_recorded_fields() {
    let _host_state = host_state_guard().await;
    let subscriber = HostSubscriber {
        host: HostCalls(host(&[])),
        next_span: AtomicU64::new(1),
        max_level: tracing::level_filters::LevelFilter::TRACE,
    };
    tracing::subscriber::with_default(subscriber, || {
        let first = tracing::info_span!("first", value = tracing::field::Empty);
        let second = tracing::info_span!("second");
        first.record("value", 7);
        assert_ne!(first.id(), second.id());
    });
}

#[test]
fn an_empty_host_config_parses_as_an_empty_object() {
    let vtable = host(&[]);
    let parsed = unsafe { parse_config::<serde_json::Value>(&vtable) }.unwrap();
    assert_eq!(parsed, serde_json::json!({}));
}

#[test]
fn a_linked_reconfigurable_module_refuses_bad_hosts_and_reports_closed_before_it_connects() {
    let _host_state = blocking_host_state_guard();
    HOST_SEND_CODE.store(TB_OK, Ordering::Release);
    let mut out = TbModuleVtable::default();
    assert_eq!(
        unsafe {
            start_linked_reconfigurable_module::<serde_json::Value, _, _>(
                std::ptr::null(),
                &mut out,
                1,
                true,
                |_, _| async { Ok(()) },
            )
        },
        TB_BAD_ARGUMENT
    );

    // An output table too small to hold the frozen prefix is refused.
    let valid = host(&[]);
    let mut too_small = TbModuleVtable {
        size: 0,
        ..TbModuleVtable::default()
    };
    assert_eq!(
        unsafe { start_linked_module(&valid, &mut too_small, 1, true, |_| async { Ok(()) }) },
        TB_BAD_ARGUMENT
    );

    // The host never answers `Hello`, so the module has no connection yet and
    // a reinitialization (here with an empty body, defaulting to `{}`) has
    // nothing to configure.
    let mut out = TbModuleVtable::default();
    assert_eq!(
        unsafe {
            start_linked_reconfigurable_module::<serde_json::Value, _, _>(
                &valid,
                &mut out,
                1,
                true,
                |_, _| async { Ok(()) },
            )
        },
        TB_OK
    );
    let callback = out.reinitialize.expect("configured module supports reinit");
    assert_eq!(
        unsafe { callback(out.module_ctx, std::ptr::null(), 0) },
        TB_CLOSED
    );
    assert_eq!(unsafe { (out.shutdown)(out.module_ctx, 10) }, TB_OK);
}

#[test]
fn an_older_host_prefix_is_copied_without_reading_the_additive_tail() {
    let current = host(b"{}");
    let prefix = tinybus::module::abi::TbHostVtableV1Prefix {
        size: tinybus::module::abi::TB_HOST_VTABLE_BASE_SIZE,
        _reserved: current._reserved,
        host_ctx: current.host_ctx,
        send: current.send,
        wake: current.wake,
        log: current.log,
        fault: current.fault,
        config: current.config,
        ready: current.ready,
    };
    let ptr = std::ptr::from_ref(&prefix).cast::<TbHostVtable>();
    let normalized = unsafe { TbHostVtable::read_compatible(ptr) }.unwrap();
    assert!(normalized.broker_routing.is_none());
    assert_eq!(
        unsafe { parse_config::<serde_json::Value>(ptr) }.unwrap(),
        serde_json::json!({})
    );
    assert_eq!(
        tinybus::module::abi::TB_HOST_VTABLE_BASE_SIZE as usize,
        std::mem::offset_of!(TbHostVtable, broker_routing)
    );
}

#[test]
fn an_incomplete_host_tail_never_reads_the_callback() {
    let mut current = host(b"{}");
    current.size = size_of::<TbHostVtable>() as u32 - 1;
    let normalized = unsafe { TbHostVtable::read_compatible(&current) }.unwrap();
    assert!(normalized.broker_routing.is_none());
    assert!(unsafe { TbHostVtable::read_compatible(std::ptr::null()) }.is_none());
    current.size = tinybus::module::abi::TB_HOST_VTABLE_BASE_SIZE - 1;
    assert!(unsafe { TbHostVtable::read_compatible(&current) }.is_none());
}

unsafe extern "C" fn routing_assertion(ctx: *mut c_void) -> u32 {
    unsafe { ctx.cast::<u32>().read() }
}

#[test]
fn native_routing_requires_an_explicit_affirmative_host_assertion() {
    let mut table = host(b"{}");
    assert!(!HostCalls(table).is_broker_routed());
    table.broker_routing = Some(routing_assertion);
    for value in [0u32, 2, u32::MAX] {
        table.host_ctx = std::ptr::from_ref(&value).cast_mut().cast();
        assert!(!HostCalls(table).is_broker_routed());
    }
    // Counterfeit host callbacks can assert 1: that is why start_module and
    // __attach_brokered_module are unsafe trusted-host contracts, never safe
    // generic transport authentication APIs.
    let affirmative = 1u32;
    table.host_ctx = std::ptr::from_ref(&affirmative).cast_mut().cast();
    assert!(HostCalls(table).is_broker_routed());
}
