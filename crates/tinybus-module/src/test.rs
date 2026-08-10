    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
    use std::sync::{OnceLock, mpsc::SyncSender};

    static HOST_SEND_CODE: AtomicI32 = AtomicI32::new(TB_OK);
    static HOST_WAKES: AtomicUsize = AtomicUsize::new(0);
    static HOST_LOGS: AtomicUsize = AtomicUsize::new(0);
    static HOST_READY: AtomicBool = AtomicBool::new(false);
    static HOST_FAULTED: AtomicBool = AtomicBool::new(false);
    static START_OUTGOING: OnceLock<StdMutex<Option<SyncSender<Vec<u8>>>>> = OnceLock::new();

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

    #[test]
    fn host_calls_and_subscriber_forward_logs_at_their_original_level() {
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
        assert_eq!(HOST_LOGS.load(Ordering::Acquire), 5);
    }

    #[test]
    fn start_functions_reject_invalid_host_and_config_before_spawning_a_runtime() {
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
    }

    #[test]
    fn configured_startup_builds_a_runtime_announces_ready_and_shuts_down() {
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
        let code = unsafe {
            start_module_with_config::<serde_json::Value, _, _>(
                &host,
                &mut out,
                1,
                true,
                |_, parsed| async move {
                    assert_eq!(parsed["answer"], 42);
                    Ok(())
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
        assert_eq!(
            unsafe { (out.deliver)(out.module_ctx, std::ptr::null(), 0) },
            TB_BAD_ARGUMENT
        );
        assert_eq!(unsafe { (out.shutdown)(out.module_ctx, 10) }, TB_OK);
        assert_eq!(unsafe { (out.shutdown)(out.module_ctx, 10) }, TB_CLOSED);
    }
