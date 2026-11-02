use super::*;
use std::sync::atomic::AtomicUsize;
use tinybus::module::abi::TbSlice;
unsafe extern "C" fn send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}
unsafe extern "C" fn notify(_: *mut c_void) {}
unsafe extern "C" fn fault(_: *mut c_void, _: *const u8, _: usize) {}
unsafe extern "C" fn log(_: *mut c_void, _: u32, _: *const u8, _: usize) {}
fn host() -> TbHostVtable {
    TbHostVtable {
        size: size_of::<TbHostVtable>() as u32,
        _reserved: 0,
        host_ctx: std::ptr::null_mut(),
        send,
        wake: notify,
        log,
        ready: notify,
        fault,
        config: TbSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        broker_routing: None,
        native_terminal_state: None,
        trusted_host_binding: None,
    }
}
fn output() -> TbModuleVtable {
    TbModuleVtable {
        size: size_of::<TbModuleVtable>() as u32,
        _reserved: 0,
        module_ctx: std::ptr::null_mut(),
        deliver,
        shutdown,
        reinitialize: None,
    }
}
#[test]
fn terminal_callbacks_reject_zero_and_unknown_context_tokens_as_closed() {
    let bytes = b"{}";
    for address in [0, 1, 255, usize::MAX] {
        let ctx = std::ptr::with_exposed_provenance_mut(address);
        let results = unsafe {
            [
                (
                    "deliver",
                    terminal_deliver(ctx, bytes.as_ptr(), bytes.len()),
                ),
                ("shutdown", terminal_shutdown(ctx, 1)),
                (
                    "reinitialize",
                    terminal_reinitialize(ctx, bytes.as_ptr(), bytes.len()),
                ),
            ]
        };
        assert_eq!(
            results,
            [
                ("deliver", TB_CLOSED),
                ("shutdown", TB_CLOSED),
                ("reinitialize", TB_CLOSED),
            ],
            "context token {address} with otherwise valid arguments"
        );
    }
}

#[test]
fn legacy_callbacks_keep_null_context_bad_argument() {
    let bytes = b"{}";
    let ctx = std::ptr::null_mut();
    let mut out = output();
    let results = unsafe {
        [
            (
                "init",
                start_module(std::ptr::null(), &mut out, 1, false, |_| async { Ok(()) }),
            ),
            ("deliver", deliver(ctx, bytes.as_ptr(), bytes.len())),
            ("shutdown", shutdown(ctx, 1)),
            (
                "reinitialize",
                reinitialize(ctx, bytes.as_ptr(), bytes.len()),
            ),
        ]
    };
    assert_eq!(
        results,
        [
            ("init", TB_BAD_ARGUMENT),
            ("deliver", TB_BAD_ARGUMENT),
            ("shutdown", TB_BAD_ARGUMENT),
            ("reinitialize", TB_BAD_ARGUMENT),
        ]
    );
}

#[test]
fn terminal_callbacks_keep_invalid_frame_and_config_bad_argument() {
    let ctx = std::ptr::null_mut();
    let bytes = b"{}";
    let results = unsafe {
        [
            terminal_deliver(ctx, std::ptr::null(), 0),
            terminal_deliver(ctx, bytes.as_ptr(), MAX_FRAME_LEN + 1),
            terminal_reinitialize(ctx, std::ptr::null(), 1),
            terminal_reinitialize(ctx, bytes.as_ptr(), 1024 * 1024 + 1),
        ]
    };
    assert_eq!(results, [TB_BAD_ARGUMENT; 4]);
}

#[test]
fn native_query_is_required_before_terminal_factory_evaluation() {
    use tinybus::module::abi::*;
    // Exactly LIVE is the sole admitted value. Missing, empty, terminal,
    // combined and unknown flags must not evaluate arbitrary factory code.
    for flags in [
        None,
        Some(0),
        Some(TB_NATIVE_TERMINAL_STOP_INTENT),
        Some(TB_NATIVE_TERMINAL_FAULT),
        Some(TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED),
        Some(TB_NATIVE_TERMINAL_LIVE | TB_NATIVE_TERMINAL_STOP_INTENT),
        Some(TB_NATIVE_TERMINAL_LIVE | TB_NATIVE_TERMINAL_FAULT),
        Some(TB_NATIVE_TERMINAL_LIVE | (1 << 31)),
        Some(u32::MAX),
    ] {
        let native = std::sync::atomic::AtomicU32::new(flags.unwrap_or(0));
        let mut host = host();
        host.host_ctx = (&native as *const std::sync::atomic::AtomicU32)
            .cast_mut()
            .cast();
        host.native_terminal_state =
            flags.map(|_| native_query as unsafe extern "C" fn(*mut c_void) -> u32);
        let calls = AtomicUsize::new(0);
        let mut out = output();
        let code = unsafe {
            start_terminal_reconfigurable_module_with_factory::<serde_json::Value, _, _>(
                &host,
                &mut out,
                1,
                true,
                |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        |_: Connection, _: serde_json::Value| async { Ok(()) },
                        RuntimeHooks::new(|_| {}, || TerminalCompletion::PhysicallyStopped),
                    ))
                },
            )
        };
        assert_eq!(code, TB_CLOSED, "query {flags:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "query {flags:?}");
        assert!(
            out.module_ctx.is_null(),
            "denied init cannot publish output"
        );
    }
}

#[test]
fn invalid_initial_config_rejects_before_factory_or_output_publication() {
    let native = std::sync::atomic::AtomicU32::new(tinybus::module::abi::TB_NATIVE_TERMINAL_LIVE);
    let bytes = b"{";
    let mut host = host();
    host.host_ctx = (&native as *const std::sync::atomic::AtomicU32)
        .cast_mut()
        .cast();
    host.native_terminal_state = Some(native_query);
    host.config = TbSlice {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    };
    let calls = AtomicUsize::new(0);
    let mut out = output();
    let code = unsafe {
        start_terminal_reconfigurable_module_with_factory::<serde_json::Value, _, _>(
            &host,
            &mut out,
            1,
            true,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok((
                    |_: Connection, _: serde_json::Value| async { Ok(()) },
                    RuntimeHooks::new(|_| {}, || TerminalCompletion::PhysicallyStopped),
                ))
            },
        )
    };
    assert_eq!(code, TB_BAD_ARGUMENT);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(out.module_ctx.is_null());
}

unsafe extern "C" fn native_query(ctx: *mut c_void) -> u32 {
    unsafe { &*ctx.cast::<std::sync::atomic::AtomicU32>() }.load(Ordering::Acquire)
}
#[test]
fn shutdown_rechecks_actual_native_fault_after_local_stop_and_physical_finish() {
    static FLAGS: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(tinybus::module::abi::TB_NATIVE_TERMINAL_LIVE);
    let flags = &FLAGS;
    let mut host = host();
    host.host_ctx = (flags as *const std::sync::atomic::AtomicU32)
        .cast_mut()
        .cast();
    host.native_terminal_state = Some(native_query);
    let state = terminal::TerminalState::admit(HostCalls(host), None).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let worker_release = release.clone();
    state.register(RuntimeHooks::new(
        |_| {},
        move || {
            entered_tx.send(()).unwrap();
            worker_release.wait();
            TerminalCompletion::PhysicallyStopped
        },
    ));
    state.mark_vtable_published();
    state.publish();
    let ptr = state.context().addr();
    let caller = std::thread::spawn(move || unsafe { terminal_shutdown(ptr as *mut c_void, 3000) });
    entered_rx.recv().unwrap();
    flags.store(
        tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT,
        Ordering::Release,
    );
    release.wait();
    let code = caller.join().unwrap();
    assert_eq!(
        code, TB_PANICKED,
        "actual native Fault cannot disappear behind local Stop"
    );
    drop(state);
    assert!(
        terminal::TerminalState::lookup(std::ptr::with_exposed_provenance_mut(ptr)).is_some(),
        "published Fault must retain the exact anchor instead of refunding its permit"
    );
}

mod controlled {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    #[derive(serde::Deserialize)]
    pub struct Config {
        pub mode: u8,
        #[serde(default, deserialize_with = "parse_gate")]
        #[serde(rename = "parse_gate")]
        _parse_gate: bool,
        #[serde(default)]
        record_drop: bool,
        #[serde(skip, default = "config_fixture")]
        fixture: std::sync::Weak<Fixture>,
    }
    static CONFIG_FIXTURE: StdMutex<std::sync::Weak<Fixture>> =
        StdMutex::new(std::sync::Weak::new());
    fn config_fixture() -> std::sync::Weak<Fixture> {
        CONFIG_FIXTURE.lock().unwrap().clone()
    }
    fn parse_gate<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<bool, D::Error> {
        let enabled = <bool as serde::Deserialize>::deserialize(deserializer)?;
        if enabled {
            let fixture = config_fixture().upgrade().unwrap();
            let barrier = fixture.config_parse_control.lock().unwrap().take().unwrap();
            fixture.events.send(Event::ConfigParsing).unwrap();
            barrier.wait();
        }
        Ok(enabled)
    }
    impl Drop for Config {
        fn drop(&mut self) {
            if self.record_drop {
                let Some(fixture) = self.fixture.upgrade() else {
                    return;
                };
                fixture.config_drops.fetch_add(1, Ordering::SeqCst);
                let _ = fixture.events.send(Event::ConfigDrop(
                    std::thread::current()
                        .name()
                        .unwrap_or("unnamed")
                        .to_owned(),
                ));
            }
        }
    }
    #[derive(Debug)]
    pub enum Event {
        Poll(u8),
        Request,
        FutureDrop(String),
        ConfigDrop(String),
        ConfigParsing,
        TimerConstruct(std::thread::ThreadId, String, tokio::runtime::Id),
        TimerDrop(Option<tokio::runtime::Id>),
        Finish,
        FactoryDrop(String),
    }
    pub struct Fixture {
        pub events: std::sync::mpsc::Sender<Event>,
        pub view: StdMutex<Option<NativeInstanceLiveness>>,
        wake: (StdMutex<bool>, std::sync::Condvar),
        worker: StdMutex<Option<std::thread::JoinHandle<bool>>>,
        pub requests: AtomicUsize,
        pub joins: AtomicUsize,
        pub workers_started: AtomicUsize,
        pub finishes: AtomicUsize,
        pub hook_failure: AtomicUsize,
        pub factory_failure: AtomicUsize,
        pub startup_failure: AtomicUsize,
        pub payload_drops: AtomicUsize,
        pub config_drops: AtomicUsize,
        pub config_parse_control: StdMutex<Option<Arc<std::sync::Barrier>>>,
        pub initial_poll_gate: StdMutex<Option<Arc<std::sync::Barrier>>>,
        pub request_gate:
            StdMutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
        pub drop_off_lock: AtomicBool,
        pub request_off_lock: AtomicBool,
        pub future_drop_control: StdMutex<Option<(Arc<std::sync::Barrier>, bool)>>,
    }
    static NEXT: StdMutex<Option<Arc<Fixture>>> = StdMutex::new(None);
    pub static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    pub fn fixture() -> (Arc<Fixture>, std::sync::mpsc::Receiver<Event>) {
        let (events, receiver) = std::sync::mpsc::channel();
        let fixture = Arc::new(Fixture {
            events,
            view: StdMutex::new(None),
            wake: (StdMutex::new(false), std::sync::Condvar::new()),
            worker: StdMutex::new(None),
            requests: AtomicUsize::new(0),
            joins: AtomicUsize::new(0),
            workers_started: AtomicUsize::new(0),
            finishes: AtomicUsize::new(0),
            hook_failure: AtomicUsize::new(0),
            factory_failure: AtomicUsize::new(0),
            startup_failure: AtomicUsize::new(0),
            payload_drops: AtomicUsize::new(0),
            config_drops: AtomicUsize::new(0),
            config_parse_control: StdMutex::new(None),
            initial_poll_gate: StdMutex::new(None),
            request_gate: StdMutex::new(None),
            drop_off_lock: AtomicBool::new(true),
            request_off_lock: AtomicBool::new(true),
            future_drop_control: StdMutex::new(None),
        });
        *CONFIG_FIXTURE.lock().unwrap() = Arc::downgrade(&fixture);
        *NEXT.lock().unwrap() = Some(fixture.clone());
        (fixture, receiver)
    }
    struct HookPayload(Arc<Fixture>);
    impl Drop for HookPayload {
        fn drop(&mut self) {
            self.0.payload_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Fixture {
        pub fn settle_rejected_worker(&self) {
            *self.wake.0.lock().unwrap() = true;
            self.wake.1.notify_all();
            if let Some(worker) = self.worker.lock().unwrap().take() {
                let _ = worker.join().unwrap();
                self.joins.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    struct Capture(Arc<Fixture>);
    impl Drop for Capture {
        fn drop(&mut self) {
            let _ = self.0.events.send(Event::FactoryDrop(
                std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_owned(),
            ));
        }
    }
    struct SetupFuture {
        fixture: Arc<Fixture>,
        mode: u8,
        first: bool,
        timer: Option<Pin<Box<tokio::time::Sleep>>>,
    }
    impl Future for SetupFuture {
        type Output = Result<()>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            if self.first {
                self.first = false;
                self.fixture.events.send(Event::Poll(self.mode)).unwrap();
                if let Some(gate) = self.fixture.initial_poll_gate.lock().unwrap().take() {
                    gate.wait();
                }
            }
            match self.mode {
                0 => Poll::Ready(Ok(())),
                5 => self
                    .timer
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .poll(cx)
                    .map(|()| Ok(())),
                2 => panic!("controlled setup poll"),
                3 => Poll::Ready(Err(Error::failed("controlled setup failure"))),
                _ => Poll::Pending,
            }
        }
    }
    impl Drop for SetupFuture {
        fn drop(&mut self) {
            if self.mode == 5 {
                let _ = self.fixture.events.send(Event::TimerDrop(
                    tokio::runtime::Handle::try_current()
                        .ok()
                        .map(|handle| handle.id()),
                ));
            }
            if let Some(view) = self.fixture.view.lock().unwrap().as_ref() {
                let slot = view.inner.resources.lock().unwrap().reinitialize.clone();
                if slot.try_lock().is_err() {
                    self.fixture.drop_off_lock.store(false, Ordering::Release);
                }
            }
            let _ = self.fixture.events.send(Event::FutureDrop(
                std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_owned(),
            ));
            let control = self.fixture.future_drop_control.lock().unwrap().take();
            if let Some((release, panic)) = control {
                release.wait();
                if panic {
                    panic!("controlled prior reinit future Drop");
                }
            }
        }
    }
    type FixtureSetup = Box<dyn Fn(Connection, Config) -> terminal::SetupFuture + Send + Sync>;
    fn factory(view: NativeInstanceLiveness) -> Result<(FixtureSetup, RuntimeHooks)> {
        let fixture = NEXT.lock().unwrap().take().unwrap();
        *fixture.view.lock().unwrap() = Some(view.clone());
        view.inner
            .reject_startup_for_test(fixture.startup_failure.load(Ordering::Acquire) as u8);
        match fixture.factory_failure.load(Ordering::Acquire) {
            1 => {
                return Err(Error::failed(
                    "controlled factory failure before hook transfer",
                ));
            }
            2 => std::panic::panic_any(HookPayload(fixture.clone())),
            _ => {}
        }
        let (request, finish) = (fixture.clone(), fixture.clone());
        let capture = Capture(fixture.clone());
        let setup = move |_: Connection, config: Config| {
            let _ = &capture;
            let mut worker = fixture.worker.lock().unwrap();
            if worker.is_none() {
                let work = fixture.clone();
                let worker_view = view.clone();
                *worker = Some(
                    std::thread::Builder::new()
                        .name("controlled-module-worker".into())
                        .spawn(move || {
                            let mut cancelled = work.wake.0.lock().unwrap();
                            while !*cancelled {
                                cancelled = work.wake.1.wait(cancelled).unwrap();
                            }
                            drop(cancelled);
                            !worker_view.is_live()
                        })
                        .unwrap(),
                );
                fixture.workers_started.fetch_add(1, Ordering::Release);
            }
            drop(worker);
            if config.mode == 4 {
                panic!("controlled setup construction");
            }
            let timer = (config.mode == 5).then(|| {
                // Construct synchronously in F, before returning its future.
                let timer = Box::pin(tokio::time::sleep(Duration::from_millis(1)));
                fixture
                    .events
                    .send(Event::TimerConstruct(
                        std::thread::current().id(),
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed")
                            .to_owned(),
                        tokio::runtime::Handle::current().id(),
                    ))
                    .unwrap();
                timer
            });
            Box::pin(SetupFuture {
                fixture: fixture.clone(),
                mode: config.mode,
                first: true,
                timer,
            }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        };
        let hooks = RuntimeHooks::new(
            move |_| {
                if let Some(view) = request.view.lock().unwrap().as_ref() {
                    let slot = view.inner.resources.lock().unwrap().reinitialize.clone();
                    if slot.try_lock().is_err() {
                        request.request_off_lock.store(false, Ordering::Release);
                    }
                }
                request.requests.fetch_add(1, Ordering::SeqCst);
                *request.wake.0.lock().unwrap() = true;
                request.wake.1.notify_all();
                request.events.send(Event::Request).unwrap();
                if let Some((entered, release)) = request.request_gate.lock().unwrap().take() {
                    let _ = entered.send(());
                    let _ = release.recv();
                }
                if request.hook_failure.load(Ordering::Acquire) == 1 {
                    std::panic::panic_any(HookPayload(request.clone()));
                }
            },
            move || {
                let worker = finish.worker.lock().unwrap().take();
                if let Some(worker) = worker {
                    let denied = worker.join().unwrap();
                    finish.joins.fetch_add(1, Ordering::SeqCst);
                    if !denied {
                        return TerminalCompletion::Uncertain;
                    }
                }
                finish.finishes.fetch_add(1, Ordering::SeqCst);
                finish.events.send(Event::Finish).unwrap();
                match finish.hook_failure.load(Ordering::Acquire) {
                    2 => std::panic::panic_any(HookPayload(finish.clone())),
                    3 => TerminalCompletion::Uncertain,
                    _ => TerminalCompletion::PhysicallyStopped,
                }
            },
        );
        Ok((Box::new(setup), hooks))
    }
    module_export_static! {
        terminal_setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.Terminal"], methods = [], signals = [],
        requires = [], optional = [], lazy = false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_posttransfer_startup_panics_keep_received_captures_on_physical_worker() {
    let _serial = controlled::TEST_LOCK.lock().await;
    // Stages correspond to real attach boundaries in start_terminal_factory_mode:
    // returned F/hooks, initial setup, reinit factory, Runtime, inbound/transport,
    // installed callback, sole JoinHandle, immediately before output publication.
    for stage in 1..=8 {
        let (fixture, events) = controlled::fixture();
        fixture.startup_failure.store(stage, Ordering::Release);
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        let rejected = host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":1,"record_drop":true}),
        );
        let view = fixture.view.lock().unwrap().take().unwrap();
        let completion = view.inner.wait(Duration::from_secs(5));
        host.shutdown(Duration::from_secs(3)).await;
        let observations: Vec<_> = events.try_iter().collect();
        let native = view.inner.host.0;
        let flags = unsafe { native.native_terminal_state.unwrap()(native.host_ctx) };
        // Actual setup execution is aborted/joined before original finish and
        // before received F/C disposal; early stages legitimately launch no work.
        assert!(rejected.is_err(), "stage {stage}");
        assert_eq!(completion, TerminalCompletion::Uncertain, "stage {stage}");
        assert!(view.inner.request_done_for_test());
        assert_eq!(fixture.requests.load(Ordering::Acquire), 1, "stage {stage}");
        assert_eq!(fixture.finishes.load(Ordering::Acquire), 1, "stage {stage}");
        assert_eq!(
            fixture.config_drops.load(Ordering::Acquire),
            1,
            "stage {stage}"
        );
        assert!(
            observations.iter().any(|event| matches!(event,
            controlled::Event::FactoryDrop(name) if name == "tinybus-terminal")),
            "stage {stage}"
        );
        assert!(
            observations
                .iter()
                .filter_map(|event| match event {
                    controlled::Event::ConfigDrop(name) | controlled::Event::FutureDrop(name) =>
                        Some(name),
                    _ => None,
                })
                .all(|name| name == "tinybus-terminal"),
            "stage {stage}"
        );
        assert_eq!(
            fixture.joins.load(Ordering::Acquire),
            fixture.workers_started.load(Ordering::Acquire),
            "stage {stage}"
        );
        assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        assert_eq!(
            flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        assert!(terminal::TerminalState::lookup(view.inner.context()).is_some());
        if stage == 5 || stage == 6 {
            assert_eq!(
                view.inner.transport_drop_thread_for_test().as_deref(),
                Some("tinybus-terminal"),
                "actual startup transport stage {stage}"
            );
        }
        let resources = view.inner.resources.lock().unwrap();
        assert!(resources.runtime.is_none());
        assert!(resources.initial.is_none());
        assert!(resources.retained_factory.is_none());
        // Sticky Startup primary keeps exactly this bounded State anchored.
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_factory_failure_disposes_received_config_on_physical_worker() {
    let _serial = controlled::TEST_LOCK.lock().await;
    for mode in [1, 2] {
        let (fixture, events) = controlled::fixture();
        fixture.factory_failure.store(mode, Ordering::Release);
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        let rejected = host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":1,"record_drop":true}),
        );
        let view = fixture.view.lock().unwrap().take().unwrap();
        let completed = view.inner.wait(Duration::from_secs(5));
        host.shutdown(Duration::from_secs(3)).await;
        let observations: Vec<_> = events.try_iter().collect();
        let native = view.inner.host.0;
        let flags = unsafe { native.native_terminal_state.unwrap()(native.host_ctx) };
        // No controller hooks have transferred, and no setup task was launched.
        // Parsed C already belongs to SDK: its actual Drop must occur on the
        // original physical job even though no output vtable was returned.
        assert!(rejected.is_err());
        assert_eq!(
            completed,
            if mode == 1 {
                TerminalCompletion::PhysicallyStopped
            } else {
                TerminalCompletion::Uncertain
            }
        );
        assert_eq!(fixture.config_drops.load(Ordering::Acquire), 1);
        assert_eq!(fixture.requests.load(Ordering::Acquire), 0);
        assert_eq!(fixture.finishes.load(Ordering::Acquire), 0);
        assert_eq!(fixture.joins.load(Ordering::Acquire), 0);
        assert_eq!(fixture.payload_drops.load(Ordering::Acquire), 0);
        assert!(observations.iter().all(|event| matches!(event,
            controlled::Event::ConfigDrop(name) if name == "tinybus-terminal")));
        assert_eq!(observations.len(), 1);
        assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        assert_eq!(
            flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0,
            "SDK failed-init cleanup cannot fabricate host physical completion"
        );
        assert!(!view.is_live());
        if mode == 1 {
            // Completion is published before the worker retires the registry
            // anchor; wait for that separate cleanup before asserting absence.
            tokio::time::timeout(Duration::from_secs(5), async {
                while terminal::TerminalState::lookup(view.inner.context()).is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("physical completion must retire its terminal anchor");
            assert!(terminal::TerminalState::lookup(view.inner.context()).is_none());
        } else {
            assert!(terminal::TerminalState::lookup(view.inner.context()).is_some());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_hook_panics_and_uncertain_finish_preserve_once_join_and_anchor() {
    let _serial = controlled::TEST_LOCK.lock().await;
    for mode in [1, 2, 3] {
        let (fixture, events) = controlled::fixture();
        fixture.hook_failure.store(mode, Ordering::Release);
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":1}),
        )
        .unwrap();
        let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
        let view = fixture.view.lock().unwrap().take().unwrap();
        let context = view.inner.context();
        // Real setup has launched the independent controller worker. Actual
        // request closes/wakes it, and original finish really joins it even
        // when request panic has already made uncertainty sticky.
        view.inner.signal(TerminalReason::Stop);
        host.shutdown(Duration::from_secs(5)).await;
        let completed = view.inner.wait(Duration::from_secs(5));
        let observations: Vec<_> = events.try_iter().collect();
        let native = view.inner.host.0;
        let flags = unsafe { native.native_terminal_state.unwrap()(native.host_ctx) };
        // Controlled work and actual original finish/join have settled before
        // assertions. Uncertain primary payload/resource ownership stays anchored.
        assert!(matches!(entered, controlled::Event::Poll(1)));
        assert_eq!(completed, TerminalCompletion::Uncertain, "hook mode {mode}");
        assert!(view.inner.request_done_for_test());
        assert_eq!(fixture.requests.load(Ordering::Acquire), 1);
        assert_eq!(fixture.finishes.load(Ordering::Acquire), 1);
        assert_eq!(fixture.joins.load(Ordering::Acquire), 1);
        assert_eq!(fixture.payload_drops.load(Ordering::Acquire), 0);
        assert!(!view.is_live());
        assert!(terminal::TerminalState::lookup(context).is_some());
        if mode != 3 {
            assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        }
        let future_drop = observations
            .iter()
            .position(|event| {
                matches!(event,
            controlled::Event::FutureDrop(name) if name == "tinybus-terminal")
            })
            .unwrap();
        let finish = observations
            .iter()
            .position(|event| matches!(event, controlled::Event::Finish))
            .unwrap();
        assert!(future_drop < finish);
        // Neither a later cause upgrade nor shutdown retry spends hooks again.
        view.request_fault();
        host.shutdown(Duration::from_millis(20)).await;
        assert_eq!(fixture.requests.load(Ordering::Acquire), 1);
        assert_eq!(fixture.finishes.load(Ordering::Acquire), 1);
        assert_eq!(fixture.payload_drops.load(Ordering::Acquire), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_rejected_physical_spawn_retains_received_resources_without_retry() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let poll_gate = Arc::new(std::sync::Barrier::new(2));
    *fixture.initial_poll_gate.lock().unwrap() = Some(poll_gate.clone());
    let (request_entered, request_entered_rx) = std::sync::mpsc::channel();
    let (request_release_tx, request_release_rx) = std::sync::mpsc::channel();
    *fixture.request_gate.lock().unwrap() = Some((request_entered, request_release_rx));
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":1}),
    )
    .unwrap();
    let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    assert!(matches!(entered, controlled::Event::Poll(1)));
    let future_was_stored_before_its_poll_returned = view
        .inner
        .resources
        .lock()
        .unwrap()
        .initial_future
        .lock()
        .unwrap()
        .is_some();
    // Poll is sent from inside SetupFuture::poll, while TerminalSetupPoll has
    // temporarily taken the future out of its storage slot.
    poll_gate.wait();
    assert!(!future_was_stored_before_its_poll_returned);
    view.inner.wait_initial_future_restored_for_test().await;
    assert!(
        view.inner
            .resources
            .lock()
            .unwrap()
            .initial_future
            .lock()
            .unwrap()
            .is_some()
    );
    let context = view.inner.context();
    view.inner.reject_worker_spawn_for_test();

    // Hold the first request callback after its bounded wake signal. A second
    // terminal cause then hits the already-running callback and rejects the
    // physical worker submission.
    let fault_view = view.clone();
    let fault = std::thread::spawn(move || fault_view.request_fault());
    request_entered_rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap();
    host.shutdown(Duration::from_secs(3)).await;
    // Submission failure cannot execute original finish. The fixture alone
    // releases/joins its independent worker; this is not SDK finish evidence.
    fixture.settle_rejected_worker();
    let completed = view.inner.wait(Duration::from_secs(1));
    let observations: Vec<_> = events.try_iter().collect();
    let native = view.inner.host.0;
    let flags = unsafe { native.native_terminal_state.unwrap()(native.host_ctx) };
    assert_eq!(completed, TerminalCompletion::Uncertain);
    assert!(!view.inner.request_done_for_test());
    assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
    assert_eq!(fixture.requests.load(Ordering::Acquire), 1);
    assert_eq!(fixture.finishes.load(Ordering::Acquire), 0);
    assert_eq!(fixture.joins.load(Ordering::Acquire), 1);
    assert!(!observations.iter().any(|event| matches!(
        event,
        controlled::Event::FactoryDrop(_)
            | controlled::Event::FutureDrop(_)
            | controlled::Event::Finish
    )));
    assert!(!view.is_live());
    drop(host);
    view.request_fault();
    assert_eq!(
        view.inner.wait(Duration::from_millis(20)),
        TerminalCompletion::Uncertain
    );
    assert_eq!(fixture.requests.load(Ordering::Acquire), 1);
    assert_eq!(fixture.finishes.load(Ordering::Acquire), 0);
    let anchored = terminal::TerminalState::lookup(context).unwrap();
    // A setup poll takes the future out of its slot and puts it back once the
    // poll returns; wait for it to be reattached instead of sampling the slot
    // while a poll still holds it.
    let initial_future = anchored.resources.lock().unwrap().initial_future.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        while initial_future.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the retained initial future must be reattached to its slot");
    let resources = anchored.resources.lock().unwrap();
    assert!(resources.retained_factory.is_some());
    assert!(resources.runtime.is_some());
    assert!(resources.initial.is_some());
    assert!(resources.initial_future.lock().unwrap().is_some());
    drop(resources);

    // The sender disconnects on assertion failure, so this test-only gate
    // cannot strand the request thread during unwinding.
    let _ = request_release_tx.send(());
    fault.join().unwrap();
    // A re-entrant host shutdown may own and finish this one-shot callback on
    // its physical worker; joining the signal trigger need not join that worker.
    assert!(
        view.inner
            .wait_request_done_for_test(Duration::from_secs(3))
    );
    assert!(view.inner.request_done_for_test());
    // One deliberately retained, bounded original State/job/Runtime remains.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_initial_cancellation_drops_future_and_joins_worker_on_physical_thread() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":1}),
    )
    .unwrap();
    let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    let shutdown = host.shutdown(Duration::from_secs(5));
    shutdown.await;
    let remainder: Vec<_> = events.try_iter().collect();
    let request_count = fixture.requests.load(Ordering::SeqCst);
    let joins = fixture.joins.load(Ordering::SeqCst);
    // All actual workers/futures settled before asserting behavior.
    assert!(matches!(entered, controlled::Event::Poll(1)));
    assert_eq!(request_count, 1);
    assert_eq!(joins, 1);
    assert!(!view.is_live());
    let future_drop = remainder.iter().position(|event| matches!(event, controlled::Event::FutureDrop(name) if name == "tinybus-terminal")).unwrap();
    let finish = remainder
        .iter()
        .position(|event| matches!(event, controlled::Event::Finish))
        .unwrap();
    assert!(future_drop < finish);
    assert!(remainder.iter().any(
        |event| matches!(event, controlled::Event::FactoryDrop(name) if name == "tinybus-terminal")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_setup_construction_and_poll_panics_still_finish_real_worker_once() {
    let _serial = controlled::TEST_LOCK.lock().await;
    for mode in [2, 4] {
        let (fixture, events) = controlled::fixture();
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":mode}),
        )
        .unwrap();
        // Consume through actual join notification before assertions or host teardown.
        let mut seen = Vec::new();
        loop {
            let event = events.recv_timeout(Duration::from_secs(5)).unwrap();
            let finished = matches!(event, controlled::Event::Finish);
            seen.push(event);
            if finished {
                break;
            }
        }
        host.shutdown(Duration::from_secs(5)).await;
        let view = fixture.view.lock().unwrap().take().unwrap();
        assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
        assert!(!view.is_live());
        if mode == 2 {
            assert!(seen.iter().any(|event| matches!(event, controlled::Event::FutureDrop(name) if name == "tinybus-terminal")));
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NativeControls {
    ctx: *mut c_void,
    event: unsafe extern "C" fn(*mut c_void, u32, u32),
}
unsafe extern "C" fn native_event(ctx: *mut c_void, event: u32, value: u32) {
    let sender = unsafe { &*ctx.cast::<std::sync::mpsc::Sender<(u32, u32)>>() };
    let _ = sender.send((event, value));
}

/// Invokes a stale-context ABI probe through a non-inlined test helper.
fn call_probe(token: usize, probe: extern "C" fn(usize) -> i32) -> i32 {
    probe(token)
}

// Only fixture symbols are looked up; TinyBus itself verifies/adopts the pinned
// private artifact through the actual ModuleHost loader first. It remains mapped.
#[cfg(unix)]
unsafe fn terminal_fixture_symbols(
    path: &std::path::Path,
) -> (
    extern "C" fn() -> usize,
    extern "C" fn(usize) -> i32,
    extern "C" fn() -> usize,
    unsafe extern "C" fn(*mut c_void, unsafe extern "C" fn(*mut c_void, u32, u32)),
    extern "C" fn() -> u32,
) {
    use std::os::unix::ffi::OsStrExt;
    #[cfg_attr(target_os = "linux", link(name = "dl"))]
    unsafe extern "C" {
        fn dlopen(path: *const std::ffi::c_char, flags: i32) -> *mut c_void;
        fn dlsym(handle: *mut c_void, name: *const std::ffi::c_char) -> *mut c_void;
        fn dlclose(handle: *mut c_void) -> i32;
    }
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let handle = unsafe { dlopen(path.as_ptr(), 2) };
    assert!(!handle.is_null());
    let context = unsafe { dlsym(handle, c"terminal_test_context".as_ptr()) };
    let probe = unsafe { dlsym(handle, c"terminal_test_old_context".as_ptr()) };
    let drops = unsafe { dlsym(handle, c"terminal_test_drops".as_ptr()) };
    let early = unsafe { dlsym(handle, c"terminal_test_early".as_ptr()) };
    let flags = unsafe { dlsym(handle, c"terminal_test_native_flags".as_ptr()) };
    assert!(
        !context.is_null()
            && !probe.is_null()
            && !drops.is_null()
            && !early.is_null()
            && !flags.is_null()
    );
    unsafe {
        dlclose(handle);
    }
    unsafe {
        (
            std::mem::transmute::<*mut c_void, extern "C" fn() -> usize>(context),
            std::mem::transmute::<*mut c_void, extern "C" fn(usize) -> i32>(probe),
            std::mem::transmute::<*mut c_void, extern "C" fn() -> usize>(drops),
            std::mem::transmute::<
                *mut c_void,
                unsafe extern "C" fn(*mut c_void, unsafe extern "C" fn(*mut c_void, u32, u32)),
            >(early),
            std::mem::transmute::<*mut c_void, extern "C" fn() -> u32>(flags),
        )
    }
}
#[cfg(windows)]
unsafe fn terminal_fixture_symbols(
    path: &std::path::Path,
) -> (
    extern "C" fn() -> usize,
    extern "C" fn(usize) -> i32,
    extern "C" fn() -> usize,
    unsafe extern "C" fn(*mut c_void, unsafe extern "C" fn(*mut c_void, u32, u32)),
    extern "C" fn() -> u32,
) {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LoadLibraryW(path: *const u16) -> *mut c_void;
        fn GetProcAddress(handle: *mut c_void, name: *const u8) -> *mut c_void;
        fn FreeLibrary(handle: *mut c_void) -> i32;
    }
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let handle = unsafe { LoadLibraryW(path.as_ptr()) };
    assert!(!handle.is_null());
    let context = unsafe { GetProcAddress(handle, c"terminal_test_context".as_ptr().cast()) };
    let probe = unsafe { GetProcAddress(handle, c"terminal_test_old_context".as_ptr().cast()) };
    let drops = unsafe { GetProcAddress(handle, c"terminal_test_drops".as_ptr().cast()) };
    let early = unsafe { GetProcAddress(handle, c"terminal_test_early".as_ptr().cast()) };
    let flags = unsafe { GetProcAddress(handle, c"terminal_test_native_flags".as_ptr().cast()) };
    assert!(
        !context.is_null()
            && !probe.is_null()
            && !drops.is_null()
            && !early.is_null()
            && !flags.is_null()
    );
    unsafe {
        FreeLibrary(handle);
    }
    unsafe {
        (
            std::mem::transmute::<*mut c_void, extern "C" fn() -> usize>(context),
            std::mem::transmute::<*mut c_void, extern "C" fn(usize) -> i32>(probe),
            std::mem::transmute::<*mut c_void, extern "C" fn() -> usize>(drops),
            std::mem::transmute::<
                *mut c_void,
                unsafe extern "C" fn(*mut c_void, unsafe extern "C" fn(*mut c_void, u32, u32)),
            >(early),
            std::mem::transmute::<*mut c_void, extern "C" fn() -> u32>(flags),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a privately pinned TINYBUS_TEST_TERMINAL_MODULE compiled cdylib"]
async fn compiled_native_terminal_owns_initial_cancellation_panics_join_and_stale_context() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let digest = tinybus::module::sha256_file(&path).unwrap();
    let name = path.file_name().unwrap().to_str().unwrap();
    let pins = std::fs::read_to_string(path.parent().unwrap().join("modules.toml")).unwrap();
    assert!(pins.contains(&format!("\"{name}\" = \"{digest}\"")));
    let mut old_context = None;
    let mut dropped_total = 0;
    for mode in [1u8, 0, 2, 4] {
        let (events, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
        // If a controlled failure cannot prove teardown, conservatively retain
        // this one fixture callback owner; never free live foreign context.
        let events = Box::into_raw(Box::new(events));
        let controls = NativeControls {
            ctx: events.cast(),
            event: native_event,
        };
        let encoded = unsafe {
            std::slice::from_raw_parts(
                (&controls as *const NativeControls).cast::<u8>(),
                size_of::<NativeControls>(),
            )
        }
        .to_vec();
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        host.load_file_with_config(
            &path,
            serde_json::json!({"mode":mode,"test_controls":encoded}),
        )
        .unwrap();
        let (context, old_probe, drops, _, _) = unsafe { terminal_fixture_symbols(&path) };
        let dropped_before = dropped_total;
        let actual_context = context();
        let mut seen = Vec::new();
        let actual_entered = if mode == 4 { 16 } else { 2 };
        loop {
            let event = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            seen.push(event);
            if event.0 == actual_entered {
                break;
            }
        }
        let stale_result = old_context.map(|token| call_probe(token, old_probe));
        host.shutdown(Duration::from_secs(5)).await;
        while !seen.iter().any(|(event, _)| *event == 6) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while drops() == dropped_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        dropped_total = drops();
        unsafe {
            drop(Box::from_raw(events));
        }
        // All actual controller workers and SDK captures are destroyed before assertions.
        if let Some(code) = stale_result {
            assert_eq!(
                code, TB_CLOSED,
                "retired successful context must not act on replacement mode {mode}"
            );
        }
        // The instrumented fixture compares Arc identity internally. Only
        // booleans cross its controls; neither TLS nor these events authorize work.
        assert!(
            seen.iter()
                .any(|(event, bits)| *event == 10 && bits & 1 != 0)
        );
        assert!(
            seen.iter()
                .any(|(event, bits)| *event == 12 && bits & 1 != 0)
        );
        assert!(
            seen.iter()
                .any(|(event, bits)| *event == 13 && bits & 7 == 7)
        );
        assert!(
            seen.iter()
                .any(|(event, bits)| *event == 15 && bits & 7 == 7)
        );
        if mode != 4 {
            assert!(
                seen.iter()
                    .any(|(event, bits)| *event == 11 && bits & 9 == 9)
            );
            assert!(
                seen.iter()
                    .any(|(event, bits)| *event == 14 && bits & 1 != 0)
            );
        }
        assert_eq!(seen.iter().filter(|(event, _)| *event == 3).count(), 1);
        assert_eq!(
            seen.iter()
                .filter(|(event, value)| *event == 5 && *value == 1)
                .count(),
            1
        );
        assert_eq!(
            seen.iter()
                .filter(|(event, value)| *event == 7 && *value == 1)
                .count(),
            1
        );
        assert!(seen.iter().any(|(event, value)| *event == 6 && *value == 1));
        if mode != 4 {
            let drop = seen
                .iter()
                .position(|(event, value)| *event == 4 && *value == 1)
                .unwrap();
            let finish = seen.iter().position(|(event, _)| *event == 5).unwrap();
            assert!(drop < finish);
        }
        if mode == 1 {
            old_context = Some(actual_context);
        }
    }

    // The image was admitted by the real loader above. Symbol lookup releases
    // its temporary OS handle: only the canonical loader pins the code now.
    let (_, _, drops, early, native_flags) = unsafe { terminal_fixture_symbols(&path) };
    let (sender, receiver) = std::sync::mpsc::channel();
    let controls = Box::into_raw(Box::new(HeldNativeControls {
        sender,
        release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
    }));
    unsafe {
        early(controls.cast(), held_native_event);
    }
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = tinybus::module::ModuleHost::new(broker);
    let result =
        host.load_file_with_config(&path, serde_json::json!({"mode":0,"test_controls":[]}));
    let mut seen = Vec::new();
    let entered = loop {
        match receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(event) => {
                seen.push(event);
                if event.0 == 8 {
                    break true;
                }
            }
            Err(_) => break false,
        }
    };
    let flags_before_drop = native_flags();
    drop(host);
    let flags_after_host_drop = native_flags();
    // Release the actual physical callback before any assertion, even if
    // rejected admission or host bookkeeping disagrees with the contract.
    unsafe {
        *(*controls)
            .release
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        (*controls).release.1.notify_all();
    }
    while !seen.iter().any(|event| event.0 == 6) {
        match receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(event) => seen.push(event),
            Err(_) => break,
        }
    }
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        while drops() == dropped_total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let final_flags = native_flags();
    // On an unresolved controlled failure retain this one callback context.
    // Otherwise the Acquire fence proves its final foreign use returned.
    if settled {
        unsafe {
            drop(Box::from_raw(controls));
        }
    }
    assert!(
        result.is_err(),
        "source Fault before vtable publication must reject init"
    );
    assert!(
        entered && settled,
        "actual SDK finish and resource Drop must settle independently"
    );
    for flags in [flags_before_drop, flags_after_host_drop, final_flags] {
        assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        assert_eq!(
            flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0,
            "no accepted vtable means no host physical-worker evidence"
        );
    }
    assert_eq!(seen.iter().filter(|event| event.0 == 3).count(), 1);
    assert!(seen.contains(&(5, 1)));
    assert!(seen.contains(&(6, 1)));
}

struct HeldNativeControls {
    sender: std::sync::mpsc::Sender<(u32, u32)>,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
}
unsafe extern "C" fn held_native_event(ctx: *mut c_void, event: u32, value: u32) {
    let controls = unsafe { &*ctx.cast::<HeldNativeControls>() };
    let _ = controls.sender.send((event, value));
    if event == 8 {
        let mut release = controls
            .release
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*release {
            release = controls
                .release
                .1
                .wait(release)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_linked_reinit_terminal_cancellation_settles_before_worker_join() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    let initial = events.recv_timeout(Duration::from_secs(3)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host
            .list()
            .iter()
            .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let reinit_host = host.clone();
    let reinit = tokio::spawn(async move {
        reinit_host
            .reinitialize("tinybus-module", serde_json::json!({"mode":1}))
            .await
    });
    let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    view.request_fault();
    let reinit_result = reinit.await.unwrap();
    host.shutdown(Duration::from_secs(5)).await;
    let mut remainder = Vec::new();
    while !remainder
        .iter()
        .any(|event| matches!(event, controlled::Event::FactoryDrop(_)))
    {
        remainder.push(events.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    assert!(matches!(initial, controlled::Event::Poll(0)));
    assert!(matches!(entered, controlled::Event::Poll(1)));
    assert!(reinit_result.is_err());
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
    assert!(!view.is_live());
    let drops: Vec<_> = remainder
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event, controlled::Event::FutureDrop(name) if name == "tinybus-terminal")
                .then_some(index)
        })
        .collect();
    let finish = remainder
        .iter()
        .position(|event| matches!(event, controlled::Event::Finish))
        .unwrap();
    assert_eq!(drops.len(), 2);
    assert!(drops.into_iter().all(|index| index < finish));
}

struct MethodPayloadGate {
    state: std::sync::Weak<terminal::TerminalState>,
    entered: std::sync::mpsc::Sender<(u32, bool, usize, tokio::runtime::Id)>,
    release: Arc<std::sync::Barrier>,
    settled: std::sync::mpsc::Sender<()>,
}
struct BlockingMethodPayload(Arc<MethodPayloadGate>);
impl Drop for BlockingMethodPayload {
    fn drop(&mut self) {
        let state = self.0.state.upgrade().unwrap();
        let host = state.host.0;
        let flags = unsafe { host.native_terminal_state.unwrap()(host.host_ctx) };
        // This sample is the first foreign payload-disposal action. It reads
        // actual copied native authority and the real registered request state.
        self.0
            .entered
            .send((
                flags,
                state.request_done_for_test(),
                state.sdk_owner_count_for_test(),
                tokio::runtime::Handle::current().id(),
            ))
            .unwrap();
        drop(state);
        self.0.release.wait();
        self.0.settled.send(()).unwrap();
    }
}
struct MethodPayloadObject(Arc<MethodPayloadGate>);
#[async_trait]
impl tinybus::Interface for MethodPayloadObject {
    fn name(&self) -> tinybus::InterfaceName {
        "ai.tinyhumans.tinybus.Payload".parse().unwrap()
    }
    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Panic".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> Result<serde_json::Value> {
        std::panic::panic_any(BlockingMethodPayload(self.0.clone()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_method_observer_fences_native_fault_and_request_done_before_blocked_payload_drop() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    let bus = tinybus::transport::memory::MemoryBus::new();
    broker.clone().spawn(bus.clone());
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (settled_tx, settled_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let gate = Arc::new(MethodPayloadGate {
        state: Arc::downgrade(&view.inner),
        entered: entered_tx,
        release: release.clone(),
        settled: settled_tx,
    });
    let (connection, module_runtime) = {
        let resources = view.inner.resources.lock().unwrap();
        let connection = resources.connection.lock().unwrap().clone().unwrap();
        (
            connection,
            resources.runtime.as_ref().unwrap().handle().id(),
        )
    };
    let destination = connection.unique_name().unwrap().to_string();
    connection
        .serve_at("/payload".parse().unwrap(), MethodPayloadObject(gate))
        .await
        .unwrap();
    drop(connection);
    let call = tokio::spawn(async move {
        client
            .proxy(&destination, "/payload", "ai.tinyhumans.tinybus.Payload")
            .unwrap()
            .with_timeout(Duration::from_secs(8))
            .call::<serde_json::Value>("Panic", ())
            .await
    });
    let sample = entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Actual method task is held in foreign payload Drop. Its Connection owner
    // survives runtime shutdown_timeout and therefore denies SDK completion.
    host.shutdown(Duration::from_secs(8)).await;
    let completion = view.inner.wait(Duration::ZERO);
    let anchored = terminal::TerminalState::lookup(view.inner.context()).is_some();
    let retained_sdk = view.inner.sdk_owner_count_for_test();
    let native = view.inner.host.0;
    let flags = unsafe { native.native_terminal_state.unwrap()(native.host_ctx) };
    release.wait();
    settled_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let call_result = call.await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while view.inner.sdk_owner_count_for_test() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // All controlled payload work, actual method caller and controller worker
    // have settled before assertions; the conservative original anchor remains.
    assert_ne!(sample.0 & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
    assert!(
        sample.1,
        "installed observer must finish bounded request before foreign Drop"
    );
    assert!(
        sample.2 > 0,
        "actual method Connection/task must retain SDK State owner"
    );
    assert_eq!(sample.3, module_runtime);
    assert_eq!(completion, TerminalCompletion::Uncertain);
    assert!(anchored && retained_sdk > 0);
    assert_eq!(
        flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
    assert_eq!(fixture.requests.load(Ordering::Acquire), 1);
    assert_eq!(fixture.finishes.load(Ordering::Acquire), 1);
    assert_eq!(fixture.joins.load(Ordering::Acquire), 1);
    assert!(call_result.is_err());
    assert!(terminal::TerminalState::lookup(view.inner.context()).is_some());
}

// A real served object, rather than a synthetic transport owner: Inner destroys
// its transport before its ObjectTree, so the old count can reach zero here.
struct TeardownObject {
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<std::sync::Barrier>,
    settled: std::sync::mpsc::Sender<()>,
    panic: bool,
}
#[async_trait]
impl tinybus::Interface for TeardownObject {
    fn name(&self) -> tinybus::InterfaceName {
        "ai.tinyhumans.tinybus.Teardown".parse().unwrap()
    }
    fn members(&self) -> Vec<tinybus::MemberName> {
        Vec::new()
    }
    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }
}
impl Drop for TeardownObject {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        self.release.wait();
        self.settled.send(()).unwrap();
        if self.panic {
            panic!("controlled ObjectTree teardown");
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_connection_object_drop_after_runtime_timeout_denies_physical_completion() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (settled_tx, settled_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let connection = view
        .inner
        .resources
        .lock()
        .unwrap()
        .connection
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    connection
        .serve_at(
            "/teardown".parse().unwrap(),
            TeardownObject {
                entered: entered_tx,
                release: release.clone(),
                settled: settled_tx,
                panic: false,
            },
        )
        .await
        .unwrap();
    drop(connection);
    let stopping_host = host.clone();
    let stopping = tokio::spawn(async move {
        stopping_host.shutdown(Duration::from_secs(8)).await;
    });
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    stopping.await.unwrap(); // The SDK's real shutdown_timeout has elapsed.
    let completion = view.inner.wait(Duration::ZERO);
    let flags =
        unsafe { (view.inner.host.0.native_terminal_state.unwrap())(view.inner.host.0.host_ctx) };
    let still_anchored = terminal::TerminalState::lookup(view.inner.context()).is_some();
    // Release and settle actual foreign work before reporting a failed proof.
    release.wait();
    settled_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Only the retained registry anchor and this test's view may remain.
    // This waits for actual SDK Connection/task owners to finish dropping,
    // rather than treating the object's last event as whole-Inner settlement.
    tokio::time::timeout(Duration::from_secs(3), async {
        while Arc::strong_count(&view.inner) > 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(completion, TerminalCompletion::Uncertain);
    assert_eq!(
        flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
    assert!(
        still_anchored,
        "blocked whole-Connection teardown cannot refund admission"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_connection_object_drop_panic_fences_fault_after_owner_suppression() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().take().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (settled_tx, settled_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let connection = view
        .inner
        .resources
        .lock()
        .unwrap()
        .connection
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    connection
        .serve_at(
            "/teardown".parse().unwrap(),
            TeardownObject {
                entered: entered_tx,
                release: release.clone(),
                settled: settled_tx,
                panic: true,
            },
        )
        .await
        .unwrap();
    drop(connection);
    let stopping_host = host.clone();
    let stopping = tokio::spawn(async move {
        stopping_host.shutdown(Duration::from_secs(8)).await;
    });
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    release.wait();
    settled_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    stopping.await.unwrap(); // Runtime teardown settles the real panicking destructor.
    let completion = view.inner.wait(Duration::ZERO);
    let flags =
        unsafe { (view.inner.host.0.native_terminal_state.unwrap())(view.inner.host.0.host_ctx) };
    let still_anchored = terminal::TerminalState::lookup(view.inner.context()).is_some();

    // Only the retained registry anchor and this test's view may remain.
    // This waits for actual SDK Connection/task owners to finish dropping,
    // rather than treating the object's last event as whole-Inner settlement.
    tokio::time::timeout(Duration::from_secs(3), async {
        while Arc::strong_count(&view.inner) > 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(completion, TerminalCompletion::Uncertain);
    assert_eq!(
        flags & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
    assert!(
        still_anchored,
        "panicking Connection teardown cannot refund admission"
    );
    assert_ne!(flags & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_repeated_reinit_disposes_prior_future_on_same_physical_worker_off_lock() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host
            .list()
            .iter()
            .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let first = host
        .reinitialize("tinybus-module", serde_json::json!({"mode":0}))
        .await;
    let second = host
        .reinitialize("tinybus-module", serde_json::json!({"mode":0}))
        .await;
    host.shutdown(Duration::from_secs(8)).await;
    let drops: Vec<_> = events
        .try_iter()
        .filter_map(|event| {
            if let controlled::Event::FutureDrop(name) = event {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(first.is_ok() && second.is_ok());
    assert_eq!(drops.len(), 3);
    assert!(
        drops.iter().all(|name| name == "tinybus-terminal"),
        "all completed futures must be destroyed by the sole physical worker: {drops:?}"
    );
    assert!(fixture.drop_off_lock.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_reinit_source_error_and_panic_request_outside_callback_exclusion() {
    let _serial = controlled::TEST_LOCK.lock().await;
    for mode in [3, 2, 4] {
        let (fixture, events) = controlled::fixture();
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":0}),
        )
        .unwrap();
        events.recv_timeout(Duration::from_secs(3)).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !host
                .list()
                .iter()
                .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let result = host
            .reinitialize("tinybus-module", serde_json::json!({"mode":mode}))
            .await;
        host.shutdown(Duration::from_secs(8)).await;
        loop {
            if matches!(
                events.recv_timeout(Duration::from_secs(5)).unwrap(),
                controlled::Event::Finish
            ) {
                break;
            }
        }
        assert!(result.is_err());
        assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
        assert!(
            fixture.request_off_lock.load(Ordering::Acquire),
            "source fault request held reinitializer mutex in mode {mode}"
        );
    }
}

async fn controlled_prior_reinit_drop(panic: bool) {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host
            .list()
            .iter()
            .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    host.reinitialize("tinybus-module", serde_json::json!({"mode":0}))
        .await
        .unwrap();
    let _ = events.try_iter().count();
    let release = Arc::new(std::sync::Barrier::new(2));
    *fixture.future_drop_control.lock().unwrap() = Some((release.clone(), panic));
    let caller_host = host.clone();
    let caller = tokio::spawn(async move {
        caller_host
            .reinitialize(
                "tinybus-module",
                serde_json::json!({"mode":0,"record_drop":true}),
            )
            .await
    });
    let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().as_ref().unwrap().clone();
    view.inner.signal(TerminalReason::Stop);
    let not_finished_while_blocked = fixture.joins.load(Ordering::SeqCst) == 0;
    let no_next_setup = !events
        .try_iter()
        .any(|event| matches!(event, controlled::Event::Poll(_)));
    release.wait();
    let result = caller.await.unwrap();
    loop {
        if matches!(
            events.recv_timeout(Duration::from_secs(5)).unwrap(),
            controlled::Event::Finish
        ) {
            break;
        }
    }
    host.shutdown(Duration::from_secs(8)).await;
    let completion = view.inner.wait(Duration::from_secs(5));
    assert_eq!(
        fixture.config_drops.load(Ordering::SeqCst),
        0,
        "rejected reinit must not receive a config before prior retirement and LIVE admission"
    );
    assert!(matches!(entered, controlled::Event::FutureDrop(name) if name == "tinybus-terminal"));
    assert!(not_finished_while_blocked && no_next_setup);
    assert!(result.is_err());
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
    assert!(fixture.drop_off_lock.load(Ordering::Acquire));
    assert!(fixture.request_off_lock.load(Ordering::Acquire));
    if panic {
        assert_eq!(completion, TerminalCompletion::Uncertain);
        assert!(view.inner.has_fault());
        assert!(terminal::TerminalState::lookup(view.inner.context()).is_some());
        assert!(
            !events
                .try_iter()
                .any(|event| matches!(event, controlled::Event::FactoryDrop(_))),
            "remaining foreign captures must stay anchored after first cleanup panic"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_prior_reinit_drop_blocks_next_setup_then_stop_settles_same_job() {
    controlled_prior_reinit_drop(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_prior_reinit_drop_panic_retains_resources_but_attempts_finish_once() {
    controlled_prior_reinit_drop(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_reinit_config_is_destroyed_on_physical_worker_including_constructor_panic() {
    let _serial = controlled::TEST_LOCK.lock().await;
    for mode in [0, 4] {
        let (fixture, events) = controlled::fixture();
        let broker = tinybus::broker::Broker::new();
        broker
            .clone()
            .spawn(tinybus::transport::memory::MemoryBus::new());
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        host.attach_linked_with_config(
            controlled::linked_module().unwrap(),
            serde_json::json!({"mode":0}),
        )
        .unwrap();
        events.recv_timeout(Duration::from_secs(3)).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !host
                .list()
                .iter()
                .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let result = host
            .reinitialize(
                "tinybus-module",
                serde_json::json!({"mode":mode,"record_drop":true}),
            )
            .await;
        host.shutdown(Duration::from_secs(8)).await;
        let mut names = Vec::new();
        loop {
            match events.recv_timeout(Duration::from_secs(5)).unwrap() {
                controlled::Event::ConfigDrop(name) => names.push(name),
                controlled::Event::Finish => break,
                _ => {}
            }
        }
        let view = fixture.view.lock().unwrap().as_ref().unwrap().clone();
        let _completion = view.inner.wait(Duration::from_secs(5));
        assert_eq!(result.is_ok(), mode == 0);
        assert_eq!(fixture.config_drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            names,
            ["tinybus-terminal"],
            "accepted generic config Drop must run on the one physical worker"
        );
        assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_during_reinit_parse_retains_received_config_until_physical_cleanup() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":0}),
    )
    .unwrap();
    events.recv_timeout(Duration::from_secs(3)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host
            .list()
            .iter()
            .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let release = Arc::new(std::sync::Barrier::new(2));
    *fixture.config_parse_control.lock().unwrap() = Some(release.clone());
    let caller_host = host.clone();
    let caller = tokio::spawn(async move {
        caller_host
            .reinitialize(
                "tinybus-module",
                serde_json::json!({"mode":0,"record_drop":true,"parse_gate":true}),
            )
            .await
    });
    let entered = events.recv_timeout(Duration::from_secs(3)).unwrap();
    let view = fixture.view.lock().unwrap().as_ref().unwrap().clone();
    view.inner.signal(TerminalReason::Stop);
    let drops_while_blocked = fixture.config_drops.load(Ordering::SeqCst);
    let joins_while_blocked = fixture.joins.load(Ordering::SeqCst);
    release.wait();
    let result = caller.await.unwrap();
    loop {
        if matches!(
            events.recv_timeout(Duration::from_secs(5)).unwrap(),
            controlled::Event::Finish
        ) {
            break;
        }
    }
    host.shutdown(Duration::from_secs(8)).await;
    let completion = view.inner.wait(Duration::from_secs(5));
    let remainder: Vec<_> = events.try_iter().collect();
    assert!(matches!(entered, controlled::Event::ConfigParsing));
    assert_eq!(drops_while_blocked, 0);
    assert_eq!(joins_while_blocked, 0);
    assert!(result.is_err());
    assert_eq!(completion, TerminalCompletion::PhysicallyStopped);
    assert_eq!(fixture.config_drops.load(Ordering::SeqCst), 1);
    assert!(remainder.iter().any(
        |event| matches!(event, controlled::Event::ConfigDrop(name) if name == "tinybus-terminal")
    ));
    assert!(
        !remainder
            .iter()
            .any(|event| matches!(event, controlled::Event::Poll(_)))
    );
    assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linked_reinit_synchronous_timer_uses_retained_runtime_on_same_worker() {
    let _serial = controlled::TEST_LOCK.lock().await;
    let caller_runtime = tokio::runtime::Handle::current().id();
    let (fixture, events) = controlled::fixture();
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    let host = Arc::new(tinybus::module::ModuleHost::new(broker));
    host.attach_linked_with_config(
        controlled::linked_module().unwrap(),
        serde_json::json!({"mode":5}),
    )
    .unwrap();
    let mut observed = vec![events.recv_timeout(Duration::from_secs(3)).unwrap()];
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host
            .list()
            .iter()
            .any(|info| matches!(info.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let first = host
        .reinitialize("tinybus-module", serde_json::json!({"mode":5}))
        .await;
    let second = host
        .reinitialize("tinybus-module", serde_json::json!({"mode":5}))
        .await;
    let caller_restored = tokio::runtime::Handle::current().id() == caller_runtime;
    let view = fixture.view.lock().unwrap().as_ref().unwrap().clone();
    host.shutdown(Duration::from_secs(8)).await;
    let completion = view.inner.wait(Duration::from_secs(5));
    let after_stop = host
        .reinitialize("tinybus-module", serde_json::json!({"mode":5}))
        .await;
    observed.extend(events.try_iter());
    let constructors: Vec<_> = observed
        .iter()
        .filter_map(|event| match event {
            controlled::Event::TimerConstruct(thread, name, runtime) => {
                Some((*thread, name.as_str(), *runtime))
            }
            _ => None,
        })
        .collect();
    // Actual caller, controller worker and SDK cleanup have settled before assertions,
    // including the unfixed constructor-panic path.
    assert!(
        first.is_ok() && second.is_ok(),
        "synchronous timer reinit failed: {first:?}, {second:?}"
    );
    assert_eq!(completion, TerminalCompletion::PhysicallyStopped);
    assert_eq!(fixture.joins.load(Ordering::SeqCst), 1);
    assert_eq!(constructors.len(), 3);
    assert_eq!(constructors[1].1, "tinybus-terminal");
    assert_eq!(constructors[2].1, "tinybus-terminal");
    assert_eq!(constructors[1].0, constructors[2].0);
    assert!(
        constructors
            .iter()
            .all(|entry| entry.2 == constructors[0].2)
    );
    assert_ne!(constructors[0].2, caller_runtime);
    assert!(caller_restored);
    assert!(after_stop.is_err());
    let cleanup_contexts: Vec<_> = observed
        .iter()
        .filter_map(|event| match event {
            controlled::Event::TimerDrop(context) => Some(context),
            _ => None,
        })
        .collect();
    assert_eq!(cleanup_contexts.len(), 3);
    assert!(
        cleanup_contexts.iter().all(|context| context.is_none()),
        "constructor runtime guard must be restored before later physical-worker cleanup"
    );
    assert_eq!(
        observed
            .iter()
            .filter(|event| matches!(event, controlled::Event::Poll(5)))
            .count(),
        3
    );
}

// The genuine ModuleHost loader must have pinned the artifact before this helper.
// Temporary OS symbol handles are released; controls never grant product authority.
unsafe fn native_fixture_symbol<T: Copy>(path: &std::path::Path, symbol: &std::ffi::CStr) -> T {
    #[cfg(unix)]
    let raw = {
        use std::os::unix::ffi::OsStrExt;
        #[cfg_attr(target_os = "linux", link(name = "dl"))]
        unsafe extern "C" {
            fn dlopen(path: *const std::ffi::c_char, flags: i32) -> *mut c_void;
            fn dlsym(handle: *mut c_void, name: *const std::ffi::c_char) -> *mut c_void;
            fn dlclose(handle: *mut c_void) -> i32;
        }
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        let handle = unsafe { dlopen(path.as_ptr(), 2) };
        assert!(!handle.is_null());
        let raw = unsafe { dlsym(handle, symbol.as_ptr()) };
        unsafe {
            dlclose(handle);
        }
        raw
    };
    #[cfg(windows)]
    let raw = {
        use std::os::windows::ffi::OsStrExt;
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn LoadLibraryW(path: *const u16) -> *mut c_void;
            fn GetProcAddress(handle: *mut c_void, name: *const u8) -> *mut c_void;
            fn FreeLibrary(handle: *mut c_void) -> i32;
        }
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let handle = unsafe { LoadLibraryW(path.as_ptr()) };
        assert!(!handle.is_null());
        let raw = unsafe { GetProcAddress(handle, symbol.as_ptr().cast()) };
        unsafe {
            FreeLibrary(handle);
        }
        raw
    };
    assert!(!raw.is_null());
    assert_eq!(size_of::<T>(), size_of::<*mut c_void>());
    unsafe { std::mem::transmute_copy(&raw) }
}
fn native_controls_config(mode: u8, controls: &NativeControls) -> serde_json::Value {
    let bytes = unsafe {
        std::slice::from_raw_parts(
            (controls as *const NativeControls).cast::<u8>(),
            size_of::<NativeControls>(),
        )
    };
    serde_json::json!({"mode": mode, "test_controls": bytes})
}
fn native_fixture_host() -> Arc<tinybus::module::ModuleHost> {
    let broker = tinybus::broker::Broker::new();
    broker
        .clone()
        .spawn(tinybus::transport::memory::MemoryBus::new());
    Arc::new(tinybus::module::ModuleHost::new(broker))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_factory_and_hook_failures_preserve_original_finish_and_fault() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    let host = native_fixture_host();
    host.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let (_, _, drops, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    let before = drops();
    host.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    tokio::time::timeout(Duration::from_secs(5), async {
        while drops() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    unsafe {
        drop(Box::from_raw(sender));
    }
    let configure: unsafe extern "C" fn(
        usize,
        *mut c_void,
        unsafe extern "C" fn(*mut c_void, u32, u32),
    ) = unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    for mode in [1usize, 2, 10, 11, 12] {
        let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
        // Uncertain primary/control resources intentionally retain this bounded
        // fixture context; a manual join is never substituted for SDK completion.
        let sender = Box::into_raw(Box::new(sender));
        let controls = NativeControls {
            ctx: sender.cast(),
            event: native_event,
        };
        unsafe {
            configure(mode, sender.cast(), native_event);
        }
        let host = native_fixture_host();
        let result =
            host.load_file_with_config(&path, native_controls_config(mode as u8, &controls));
        let mut seen = Vec::new();
        let target = if mode < 3 { 19 } else { 2 };
        loop {
            let event = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            seen.push(event);
            if event.0 == target {
                break;
            }
        }
        if mode >= 10 {
            host.shutdown(Duration::from_secs(5)).await;
            while !seen.iter().any(|event| event.0 == 5) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        drop(host);
        let native = flags();
        seen.extend(receiver.try_iter());
        // All blocking controller work is joined by the original SDK finish
        // before these assertions. Factory failures never launch a controller.
        if mode < 3 {
            assert!(result.is_err());
            assert!(seen.contains(&(19, 1)));
            assert!(!seen.iter().any(|event| matches!(event.0, 2 | 3 | 5 | 7)));
        } else {
            assert!(result.is_ok());
            assert_eq!(seen.iter().filter(|event| event.0 == 3).count(), 1);
            assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
            assert_eq!(seen.iter().filter(|event| **event == (7, 1)).count(), 1);
            assert!(seen.iter().any(|event| event.0 == 13 && event.1 & 7 == 7));
        }
        assert!(
            !seen.iter().any(|event| event.0 == 17),
            "primary payload must remain retained"
        );
        assert_eq!(
            native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        if mode != 12 {
            assert_ne!(native & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_retained_views_bound_256_and_release_only_at_actual_last_drop() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    let bootstrap = native_fixture_host();
    bootstrap
        .load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let (context, probe, drops, _, _) = unsafe { terminal_fixture_symbols(&path) };
    let before = drops();
    bootstrap.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    tokio::time::timeout(Duration::from_secs(5), async {
        while drops() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    unsafe {
        drop(Box::from_raw(sender));
    }
    drop(bootstrap);
    let configure: unsafe extern "C" fn(
        usize,
        *mut c_void,
        unsafe extern "C" fn(*mut c_void, u32, u32),
    ) = unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let factories: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_factories") };
    let retained_live: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_retained_live") };
    let release_one: extern "C" fn() -> bool =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_one_view") };
    let release_all: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_views") };
    let mut first_token = None;
    let mut admitted = 0;
    let mut all_settled = true;
    for _ in 0..256 {
        let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
        let sender = Box::into_raw(Box::new(sender));
        let controls = NativeControls {
            ctx: sender.cast(),
            event: native_event,
        };
        unsafe {
            configure(3, sender.cast(), native_event);
        }
        let host = native_fixture_host();
        let result = host.load_file_with_config(&path, native_controls_config(0, &controls));
        if result.is_err() {
            unsafe {
                drop(Box::from_raw(sender));
            }
            break;
        }
        first_token.get_or_insert(context());
        admitted += 1;
        let before = drops();
        host.shutdown(Duration::from_secs(5)).await;
        let mut settled = false;
        while let Ok(event) = receiver.recv_timeout(Duration::from_secs(5)) {
            if event.0 == 6 {
                settled = true;
                break;
            }
        }
        settled &= tokio::time::timeout(Duration::from_secs(5), async {
            while drops() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        all_settled &= settled;
        drop(host);
        if settled {
            unsafe {
                drop(Box::from_raw(sender));
            }
        } else {
            break;
        }
    }
    let before_factory = factories();
    let denied_host = native_fixture_host();
    let denied =
        denied_host.load_file_with_config(&path, serde_json::json!({"mode":0,"test_controls":[]}));
    drop(denied_host);
    let factories_after_denial = factories();
    let live_after_host_drops = retained_live();
    let released_one = release_one();
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    unsafe {
        configure(3, sender.cast(), native_event);
    }
    let replacement = native_fixture_host();
    let replacement_result =
        replacement.load_file_with_config(&path, native_controls_config(0, &controls));
    let new_token = context();
    let stale = first_token.map(|token| call_probe(token, probe));
    let before = drops();
    replacement.shutdown(Duration::from_secs(5)).await;
    let mut replacement_settled = false;
    if replacement_result.is_ok() {
        while let Ok(event) = receiver.recv_timeout(Duration::from_secs(5)) {
            if event.0 == 6 {
                replacement_settled = true;
                break;
            }
        }
        replacement_settled &= tokio::time::timeout(Duration::from_secs(5), async {
            while drops() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
    }
    drop(replacement);
    if replacement_settled {
        unsafe {
            drop(Box::from_raw(sender));
        }
    }
    let released_rest = release_all();
    // All actual controller joins and SDK capture destruction precede assertions.
    // No uncertain anchor was reset; this filter starts in a fresh process.
    assert!(all_settled && replacement_settled);
    assert_eq!(admitted, 256);
    assert!(denied.is_err());
    assert_eq!(factories_after_denial, before_factory);
    assert_eq!(live_after_host_drops, 0);
    assert!(released_one && replacement_result.is_ok());
    assert_eq!(
        new_token & 255,
        first_token.unwrap() & 255,
        "the exact freed slot must be reusable"
    );
    assert_ne!(new_token, first_token.unwrap());
    assert_eq!(stale, Some(TB_CLOSED));
    assert_eq!(released_rest, 256);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_spawn_rejection_retains_received_runtime_task_and_original_finish() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    // Keep the library loaded while resolving its fixture controls.
    let warm = native_fixture_host();
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let configure: unsafe extern "C" fn(
        usize,
        *mut c_void,
        unsafe extern "C" fn(*mut c_void, u32, u32),
    ) = unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let resources: extern "C" fn() -> u32 =
        unsafe { native_fixture_symbol(&path, c"terminal_test_resources") };
    let join: extern "C" fn() -> bool =
        unsafe { native_fixture_symbol(&path, c"terminal_test_external_join") };
    let (_, _, _, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    // The following control context is deliberately retained with the uncertain
    // original SDK owner. The warm context is independent.
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    unsafe {
        configure(20, sender.cast(), native_event);
    }
    let host = native_fixture_host();
    host.load_file_with_config(&path, native_controls_config(1, &controls))
        .unwrap();
    let mut seen = Vec::new();
    loop {
        let event = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        seen.push(event);
        if event.0 == 2 {
            break;
        }
    }
    host.shutdown(Duration::from_millis(30)).await;
    // Physical spawn failed. Only this fixture cleanup joins its independently
    // received controller; original finish and SDK settlement never run.
    let externally_joined = join();
    let held = resources();
    host.shutdown(Duration::from_millis(30)).await;
    drop(host);
    let after = resources();
    seen.extend(receiver.try_iter());
    assert!(externally_joined);
    assert_eq!(seen.iter().filter(|event| event.0 == 21).count(), 1);
    assert_eq!(seen.iter().filter(|event| event.0 == 3).count(), 1);
    assert!(!seen.iter().any(|event| matches!(event.0, 4 | 5 | 6 | 20)));
    // Actual retained F, Runtime, transport, reinit callback, sole initial
    // JoinHandle and already-polled owned future. No timeout/refund inference.
    let expected = 1 | 8 | 16 | 32 | 64 | 256;
    assert_eq!(held & expected, expected);
    assert_eq!(after & expected, expected);
    assert_ne!(flags() & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
    assert_eq!(
        flags() & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_startup_transfers_and_runtime_build_error_dispose_on_physical_worker() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let warm = native_fixture_host();
    let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let sender = Box::into_raw(Box::new(sender));
    let controls = NativeControls {
        ctx: sender.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let configure: unsafe extern "C" fn(
        usize,
        *mut c_void,
        unsafe extern "C" fn(*mut c_void, u32, u32),
    ) = unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let resources: extern "C" fn() -> u32 =
        unsafe { native_fixture_symbol(&path, c"terminal_test_resources") };
    let transport_drop: extern "C" fn() -> bool =
        unsafe { native_fixture_symbol(&path, c"terminal_test_transport_drop") };
    let release: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_views") };
    let (_, _, drops, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    for (stage, expected) in [
        (1, 129u32),
        (2, 3),
        (3, 7),
        (4, 15),
        (5, 31),
        (6, 59),
        (7, 123),
        (8, 123),
        (9, 7),
    ] {
        let (sender, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
        let sender = Box::into_raw(Box::new(sender));
        let controls = NativeControls {
            ctx: sender.cast(),
            event: native_event,
        };
        unsafe {
            configure(20 + stage, sender.cast(), native_event);
        }
        let before = drops();
        let host = native_fixture_host();
        let result = host.load_file_with_config(&path, native_controls_config(1, &controls));
        let mut seen = Vec::new();
        // Both real F and SDK-owned C destruction plus controller last Drop
        // settle before assertions. No task has entered trusted F at stages7/8.
        while ![6, 19, 20]
            .iter()
            .all(|kind| seen.iter().any(|event: &(u32, u32)| event.0 == *kind))
        {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while drops() == before || resources() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let dropped_transport = transport_drop();
        host.shutdown(Duration::from_secs(5)).await;
        drop(host);
        let native = flags();
        release();
        seen.extend(receiver.try_iter());
        unsafe {
            drop(Box::from_raw(sender));
        }
        assert!(result.is_err(), "stage {stage}");
        assert!(
            seen.contains(&(22, expected)),
            "received stage {stage}: {seen:?}"
        );
        assert_eq!(seen.iter().filter(|event| event.0 == 3).count(), 1);
        assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
        assert!(seen.contains(&(6, 1)));
        assert!(seen.contains(&(19, 1)));
        assert!(seen.contains(&(20, 1)));
        assert!(!seen.iter().any(|event| event.0 == 2));
        assert_eq!(dropped_transport, (5..=8).contains(&stage), "stage {stage}");
        assert_ne!(native & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
        assert_eq!(
            native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
    }
}

struct NativeBarrierControls {
    events: std::sync::mpsc::Sender<(u32, u32)>,
    blocked_event: u32,
    release: (StdMutex<bool>, std::sync::Condvar),
}
impl NativeBarrierControls {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
unsafe extern "C" fn native_barrier_event(ctx: *mut c_void, event: u32, value: u32) {
    let controls = unsafe { &*ctx.cast::<NativeBarrierControls>() };
    let _ = controls.events.send((event, value));
    if event == controls.blocked_event {
        let mut released = controls.release.0.lock().unwrap();
        while !*released {
            released = controls.release.1.wait(released).unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_entered_initial_and_reinit_work_settle_before_original_finish() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    // Modes40/41 block actual synchronous construction/poll; mode42 blocks
    // actual retained future Drop. Reinit43 returns a genuine ready Err only
    // after the same poll has observed an already-committed real host Stop.
    for (reinitializing, mode) in [
        (false, 40u8),
        (false, 41),
        (false, 42),
        (false, 44),
        (false, 45),
        (true, 40),
        (true, 41),
        (true, 42),
        (true, 43),
        (true, 44),
        (true, 45),
    ] {
        let (events, receiver) = std::sync::mpsc::channel();
        let barrier = Box::into_raw(Box::new(NativeBarrierControls {
            events,
            blocked_event: match mode {
                40 | 44 => 23,
                42 => 25,
                _ => 24,
            },
            release: (StdMutex::new(false), std::sync::Condvar::new()),
        }));
        let controls = NativeControls {
            ctx: barrier.cast(),
            event: native_barrier_event,
        };
        let host = native_fixture_host();
        host.load_file_with_config(
            &path,
            native_controls_config(if reinitializing { 0 } else { mode }, &controls),
        )
        .unwrap();
        let (context, _, drops, _, flags) = unsafe { terminal_fixture_symbols(&path) };
        let reinit: unsafe extern "C" fn(usize, *const u8, usize) -> i32 =
            unsafe { native_fixture_symbol(&path, c"terminal_test_reinit") };
        let before = drops();
        let mut seen = Vec::new();
        if reinitializing {
            while !seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        let call = reinitializing.then(|| {
            let token = context();
            let bytes = serde_json::to_vec(&native_controls_config(mode, &controls)).unwrap();
            std::thread::spawn(move || unsafe { reinit(token, bytes.as_ptr(), bytes.len()) })
        });
        // Drop42 is first entered on actual physical cleanup after shutdown.
        if mode != 42 {
            while !seen
                .iter()
                .any(|event| event.0 == unsafe { &*barrier }.blocked_event)
            {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        } else if !reinitializing {
            while !seen.iter().any(|event| event.0 == 2) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        } else {
            while seen.iter().filter(|event| event.0 == 2).count() != 2 {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        host.shutdown(Duration::from_millis(30)).await;
        if mode == 42 {
            while !seen.iter().any(|event| event.0 == 25) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        let blocked_native = flags();
        seen.extend(receiver.try_iter());
        let finish_before_release = seen.iter().any(|event| event.0 == 5);
        let drop_before_release = drops();
        // Release before any assertion; join the real ABI caller, actual
        // initial task/future and original independent controller below.
        unsafe { &*barrier }.release();
        let reinit_result = call.map(|call| call.join().unwrap());
        host.shutdown(Duration::from_secs(5)).await;
        while !seen.iter().any(|event| event.0 == 6) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while drops() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let settled_native = flags();
        seen.extend(receiver.try_iter());
        drop(host);
        unsafe {
            drop(Box::from_raw(barrier));
        }
        assert!(
            !finish_before_release,
            "entered mode {mode}, reinit {reinitializing}"
        );
        assert_eq!(drop_before_release, before);
        assert_ne!(
            blocked_native & tinybus::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT,
            0
        );
        assert_eq!(
            blocked_native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
        assert_eq!(seen.iter().filter(|event| **event == (7, 1)).count(), 1);
        assert!(seen.iter().any(|event| event.0 == 13 && event.1 & 7 == 7));
        assert!(seen.iter().any(|event| event.0 == 16 && event.1 & 1 == 1));
        assert!(
            seen.iter()
                .filter(|event| event.0 == 26)
                .all(|event| event.1 == 1)
        );
        if let Some(result) = reinit_result {
            assert_eq!(result, if mode == 44 { TB_PANICKED } else { TB_CLOSED });
        }
        if mode == 43 || mode == 44 || mode == 45 {
            assert_ne!(
                settled_native & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT,
                0,
                "genuine source error/panic must dominate simultaneous ready Stop"
            );
            assert_eq!(
                settled_native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
                0
            );
            assert!(!seen.iter().any(|event| event.0 == 17));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_method_payload_and_whole_connection_drop_retain_actual_sdk_owners() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let warm = native_fixture_host();
    let (events, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let events = Box::into_raw(Box::new(events));
    let controls = NativeControls {
        ctx: events.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let configure: unsafe extern "C" fn(
        usize,
        *mut c_void,
        unsafe extern "C" fn(*mut c_void, u32, u32),
    ) = unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let owners: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_sdk_owners") };
    let release_views: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_views") };
    let (_, _, drops, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    for mode in [50u8, 51, 52] {
        let (events, receiver) = std::sync::mpsc::channel();
        let barrier = Box::into_raw(Box::new(NativeBarrierControls {
            events,
            blocked_event: if mode == 50 { 30 } else { 32 },
            release: (StdMutex::new(false), std::sync::Condvar::new()),
        }));
        let controls = NativeControls {
            ctx: barrier.cast(),
            event: native_barrier_event,
        };
        unsafe {
            configure(46, barrier.cast(), native_barrier_event);
        }
        let broker = tinybus::broker::Broker::new();
        let bus = tinybus::transport::memory::MemoryBus::new();
        broker.clone().spawn(bus.clone());
        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let host = Arc::new(tinybus::module::ModuleHost::new(broker));
        let before = drops();
        host.load_file_with_config(&path, native_controls_config(mode, &controls))
            .unwrap();
        let mut seen = Vec::new();
        while !seen.iter().any(|event: &(u32, u32)| event.0 == 31) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        let call = (mode == 50).then(|| {
            tokio::spawn(async move {
                client
                    .proxy(
                        "ai.tinyhumans.TerminalFixture",
                        "/terminal",
                        "ai.tinyhumans.TerminalFixture",
                    )
                    .unwrap()
                    .with_timeout(Duration::from_secs(10))
                    .call::<serde_json::Value>("Panic", ())
                    .await
            })
        });
        if mode == 50 {
            while !seen.iter().any(|event| event.0 == 30) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        let stopping_host = host.clone();
        let stopping = tokio::spawn(async move {
            stopping_host.shutdown(Duration::from_secs(8)).await;
        });
        if mode != 50 {
            while !seen.iter().any(|event| event.0 == 32) {
                seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
            }
        }
        stopping.await.unwrap();
        // Host shutdown has a deadline and can return before the physical
        // worker finishes. Observe the original finish/join directly while
        // the SDK-owned destructor is still held by the fixture barrier.
        while !seen.contains(&(5, 1)) || !seen.contains(&(7, 1)) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        let held_owners = owners();
        let timed_out_native = flags();
        seen.extend(receiver.try_iter());
        let actual_finish = seen.iter().filter(|event| **event == (5, 1)).count();
        let actual_join = seen.iter().filter(|event| **event == (7, 1)).count();
        let retained_controller = drops() == before;
        // Always settle controlled work before asserting: release real foreign
        // Drop, await actual call and actual counted last Connection observer.
        unsafe { &*barrier }.release();
        let call_result = match call {
            Some(call) => Some(call.await.unwrap()),
            None => None,
        };
        while !seen.iter().any(|event| event.0 == 6) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while owners() != 0 || drops() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let settled_native = flags();
        release_views();
        seen.extend(receiver.try_iter());
        drop(host);
        unsafe {
            drop(Box::from_raw(barrier));
        }
        assert!(
            held_owners > 0,
            "whole SDK-owned Connection task/Drop mode {mode}"
        );
        assert!(retained_controller);
        assert_eq!(
            actual_finish, 1,
            "actual original controller finish is separate"
        );
        assert_eq!(actual_join, 1);
        assert_eq!(
            timed_out_native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        assert_eq!(
            settled_native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        if mode == 50 {
            assert!(
                seen.contains(&(29, 7)),
                "Fault/request-done/actualSDKowner before first payload Drop"
            );
            assert!(call_result.unwrap().is_err());
            assert_ne!(
                timed_out_native & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT,
                0
            );
        } else {
            assert!(
                seen.iter().any(|event| event.0 == 35 && event.1 & 6 == 6),
                "owner already stopped, bounded request done before whole Connection Drop"
            );
            if mode == 52 {
                assert_ne!(
                    settled_native & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT,
                    0,
                    "explicit resource-drop observer fences Fault even after owner suppression"
                );
            }
        }
    }
}

type NativeConfigure =
    unsafe extern "C" fn(usize, *mut c_void, unsafe extern "C" fn(*mut c_void, u32, u32));
type NativeReinit = unsafe extern "C" fn(usize, *const u8, usize) -> i32;
struct NativeNestedControls {
    events: std::sync::mpsc::Sender<(u32, u32)>,
    path: std::path::PathBuf,
    host: Arc<tinybus::module::ModuleHost>,
    other: NativeControls,
    configure: NativeConfigure,
    reinit: NativeReinit,
    context: extern "C" fn() -> usize,
    token: std::sync::atomic::AtomicUsize,
    phase: std::sync::atomic::AtomicUsize,
}
unsafe extern "C" fn native_nested_event(ctx: *mut c_void, event: u32, value: u32) {
    let controls = unsafe { &*ctx.cast::<NativeNestedControls>() };
    let _ = controls.events.send((event, value));
    if event == 10 {
        unsafe {
            (controls.configure)(46, controls.other.ctx, controls.other.event);
        }
        let loaded = controls
            .host
            .load_file_with_config(&controls.path, native_controls_config(0, &controls.other));
        controls
            .token
            .store((controls.context)(), Ordering::Release);
        let _ = controls.events.send((36, u32::from(loaded.is_ok())));
    }
    if event == 26 && controls.phase.load(Ordering::Acquire) == 1 {
        let bytes = serde_json::to_vec(&native_controls_config(0, &controls.other)).unwrap();
        let result = unsafe {
            (controls.reinit)(
                controls.token.load(Ordering::Acquire),
                bytes.as_ptr(),
                bytes.len(),
            )
        };
        let _ = controls.events.send((37, u32::from(result == TB_OK)));
    }
}
struct NativeLogCount(Arc<std::sync::atomic::AtomicUsize>);
impl tracing::Subscriber for NativeLogCount {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "tinybus_module" {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_nested_factory_reinit_restore_exact_owner_and_suppress_captured_dispatch()
{
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let warm = native_fixture_host();
    let (events, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let events = Box::into_raw(Box::new(events));
    let controls = NativeControls {
        ctx: events.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let configure: NativeConfigure =
        unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let reinit: NativeReinit = unsafe { native_fixture_symbol(&path, c"terminal_test_reinit") };
    let dispatch: extern "C" fn(usize) -> bool =
        unsafe { native_fixture_symbol(&path, c"terminal_test_dispatch") };
    let unowned: extern "C" fn() -> bool =
        unsafe { native_fixture_symbol(&path, c"terminal_test_unowned") };
    let release_views: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_views") };
    let retained_live: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_retained_live") };
    let (context, old_probe, drops, _, _) = unsafe { terminal_fixture_symbols(&path) };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    let (b_events, b_receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let b_events = Box::into_raw(Box::new(b_events));
    let b_controls = NativeControls {
        ctx: b_events.cast(),
        event: native_event,
    };
    let host_b = native_fixture_host();
    let (a_events, a_receiver) = std::sync::mpsc::channel();
    let nested = Box::into_raw(Box::new(NativeNestedControls {
        events: a_events,
        path: path.clone(),
        host: host_b.clone(),
        other: b_controls,
        configure,
        reinit,
        context,
        token: std::sync::atomic::AtomicUsize::new(0),
        phase: std::sync::atomic::AtomicUsize::new(0),
    }));
    let a_controls = NativeControls {
        ctx: nested.cast(),
        event: native_nested_event,
    };
    unsafe {
        configure(46, nested.cast(), native_nested_event);
    }
    let host_a = native_fixture_host();
    host_a
        .load_file_with_config(&path, native_controls_config(0, &a_controls))
        .unwrap();
    let token_a = context();
    let token_b = unsafe { &*nested }.token.load(Ordering::Acquire);
    let mut a_seen = Vec::new();
    let mut b_seen = Vec::new();
    while !a_seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
        a_seen.push(a_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    while !b_seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
        b_seen.push(b_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    unsafe { &*nested }.phase.store(1, Ordering::Release);
    let bytes = serde_json::to_vec(&native_controls_config(0, &a_controls)).unwrap();
    let first_reinit = unsafe { reinit(token_a, bytes.as_ptr(), bytes.len()) };
    let restored_external = unowned();
    let logs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let probe = |index| {
        let before = logs.load(Ordering::Acquire);
        let enabled =
            tracing::subscriber::with_default(NativeLogCount(logs.clone()), || dispatch(index));
        (enabled, logs.load(Ordering::Acquire) - before)
    };
    let a_live_probe = probe(0);
    let b_live_probe = probe(1);
    let before = drops();
    host_a.shutdown(Duration::from_secs(5)).await;
    while !a_seen.iter().any(|event| event.0 == 6) {
        a_seen.push(a_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while drops() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let a_stopped_probe = probe(0);
    let b_after_a_probe = probe(1);
    let live_after_a = retained_live();
    let b_bytes = serde_json::to_vec(&native_controls_config(0, &b_controls)).unwrap();
    let b_after_a_reinit = unsafe { reinit(token_b, b_bytes.as_ptr(), b_bytes.len()) };
    host_b.shutdown(Duration::from_secs(5)).await;
    while !b_seen.iter().any(|event| event.0 == 6) {
        b_seen.push(b_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    let b_stopped_probe = probe(1);
    // A newly loaded third native instance has its own exact owner; retired
    // opaque tokens cannot select it, and old factory dispatchers remain quiet.
    let (c_events, c_receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let c_events = Box::into_raw(Box::new(c_events));
    let c_controls = NativeControls {
        ctx: c_events.cast(),
        event: native_event,
    };
    unsafe {
        configure(46, c_events.cast(), native_event);
    }
    let host_c = native_fixture_host();
    host_c
        .load_file_with_config(&path, native_controls_config(0, &c_controls))
        .unwrap();
    let mut c_seen = Vec::new();
    while !c_seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
        c_seen.push(c_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    let c_live_probe = probe(2);
    let old_a = old_probe(token_a);
    let old_b = old_probe(token_b);
    let old_dispatch_after_reload = [probe(0), probe(1)];
    let before = drops();
    host_c.shutdown(Duration::from_secs(5)).await;
    while !c_seen.iter().any(|event| event.0 == 6) {
        c_seen.push(c_receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while drops() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    a_seen.extend(a_receiver.try_iter());
    b_seen.extend(b_receiver.try_iter());
    c_seen.extend(c_receiver.try_iter());
    let finally_unowned = unowned();
    release_views();
    drop(host_a);
    drop(host_b);
    drop(host_c);
    // All real original finishes/joins and F/controller captures settled.
    unsafe {
        drop(Box::from_raw(nested));
        drop(Box::from_raw(b_events));
        drop(Box::from_raw(c_events));
    }
    assert_eq!(first_reinit, TB_OK);
    assert_eq!(b_after_a_reinit, TB_OK);
    assert!(restored_external && finally_unowned);
    assert!(a_seen.contains(&(36, 1)) && a_seen.contains(&(37, 1)));
    assert!(
        a_seen.iter().any(|event| event.0 == 27 && event.1 & 3 == 1),
        "factory B scope restores actual factory A"
    );
    assert!(
        a_seen
            .iter()
            .filter(|event| event.0 == 28)
            .all(|event| event.1 & 3 == 1),
        "nested B reinit restores actual worker A"
    );
    assert_eq!(a_live_probe, (true, 1));
    assert_eq!(b_live_probe, (true, 1));
    assert_eq!(a_stopped_probe, (false, 0));
    assert_eq!(b_after_a_probe, (true, 1));
    assert_eq!(b_stopped_probe, (false, 0));
    assert_eq!(c_live_probe, (true, 1));
    assert_eq!(old_dispatch_after_reload, [(false, 0), (false, 0)]);
    assert_eq!(live_after_a, 1);
    assert_eq!((old_a, old_b), (TB_CLOSED, TB_CLOSED));
    for seen in [a_seen, b_seen, c_seen] {
        assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
        assert_eq!(seen.iter().filter(|event| **event == (7, 1)).count(), 1);
        assert!(seen.iter().any(|event| event.0 == 13 && event.1 & 7 == 7));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_first_foreign_cleanup_panic_keeps_later_resources_and_attempts_original_finish()
 {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let warm = native_fixture_host();
    let (warm_events, warm_receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let warm_events = Box::into_raw(Box::new(warm_events));
    let warm_controls = NativeControls {
        ctx: warm_events.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &warm_controls))
        .unwrap();
    let configure: NativeConfigure =
        unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let resources: extern "C" fn() -> u32 =
        unsafe { native_fixture_symbol(&path, c"terminal_test_resources") };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(
        warm_receiver.recv_timeout(Duration::from_secs(5)),
        Ok((6, _))
    ) {}
    let (events, receiver) = std::sync::mpsc::channel();
    // This callback owner deliberately stays retained with the SDK's uncertain
    // untouched later F/Runtime/callback resources after the first Drop failure.
    let barrier = Box::into_raw(Box::new(NativeBarrierControls {
        events,
        blocked_event: 25,
        release: (StdMutex::new(false), std::sync::Condvar::new()),
    }));
    let controls = NativeControls {
        ctx: barrier.cast(),
        event: native_barrier_event,
    };
    unsafe {
        configure(46, barrier.cast(), native_barrier_event);
    }
    let host = native_fixture_host();
    host.load_file_with_config(&path, native_controls_config(47, &controls))
        .unwrap();
    let (_, _, _, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    let mut seen = Vec::new();
    while !seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
        seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    host.shutdown(Duration::from_millis(30)).await;
    while !seen.iter().any(|event| event.0 == 25) {
        seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    let before_release = flags();
    seen.extend(receiver.try_iter());
    let finished_early = seen.iter().any(|event| event.0 == 5);
    unsafe { &*barrier }.release();
    while !seen.iter().any(|event| event.0 == 5) {
        seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    host.shutdown(Duration::from_secs(5)).await;
    host.shutdown(Duration::from_millis(30)).await;
    drop(host);
    let after = flags();
    let held_resources = resources();
    seen.extend(receiver.try_iter());
    assert!(!finished_early);
    assert_eq!(held_resources & (1 | 8 | 16 | 32), 1 | 8 | 16 | 32);
    assert_eq!(
        before_release & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
    assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
    assert_eq!(seen.iter().filter(|event| **event == (7, 1)).count(), 1);
    assert!(seen.iter().any(|event| event.0 == 13 && event.1 & 7 == 7));
    assert!(!seen.iter().any(|event| matches!(event.0, 6 | 17 | 20)));
    assert_ne!(after & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
    assert_eq!(
        after & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh-process privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_prior_reinit_future_drop_restores_owner_and_retains_settlement() {
    let path = std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    let warm = native_fixture_host();
    let (events, receiver) = std::sync::mpsc::channel::<(u32, u32)>();
    let events = Box::into_raw(Box::new(events));
    let controls = NativeControls {
        ctx: events.cast(),
        event: native_event,
    };
    warm.load_file_with_config(&path, native_controls_config(0, &controls))
        .unwrap();
    let configure: NativeConfigure =
        unsafe { native_fixture_symbol(&path, c"terminal_test_factory_mode") };
    let reinit: NativeReinit = unsafe { native_fixture_symbol(&path, c"terminal_test_reinit") };
    let resources: extern "C" fn() -> u32 =
        unsafe { native_fixture_symbol(&path, c"terminal_test_resources") };
    let release_views: extern "C" fn() -> usize =
        unsafe { native_fixture_symbol(&path, c"terminal_test_release_views") };
    let (context, _, drops, _, flags) = unsafe { terminal_fixture_symbols(&path) };
    warm.shutdown(Duration::from_secs(5)).await;
    while !matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok((6, _))) {}
    for mode in [48u8, 49] {
        let (events, receiver) = std::sync::mpsc::channel();
        let barrier = Box::into_raw(Box::new(NativeBarrierControls {
            events,
            blocked_event: 25,
            release: (StdMutex::new(false), std::sync::Condvar::new()),
        }));
        let controls = NativeControls {
            ctx: barrier.cast(),
            event: native_barrier_event,
        };
        unsafe {
            configure(46, barrier.cast(), native_barrier_event);
        }
        let host = native_fixture_host();
        let before = drops();
        host.load_file_with_config(&path, native_controls_config(0, &controls))
            .unwrap();
        let mut seen = Vec::new();
        while !seen.iter().any(|event: &(u32, u32)| event.0 == 2) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        let token = context();
        let bytes = serde_json::to_vec(&native_controls_config(mode, &controls)).unwrap();
        let first_result = unsafe { reinit(token, bytes.as_ptr(), bytes.len()) };
        let next_bytes = serde_json::to_vec(&native_controls_config(0, &controls)).unwrap();
        let next = std::thread::spawn(move || unsafe {
            reinit(token, next_bytes.as_ptr(), next_bytes.len())
        });
        while !seen.iter().any(|event| event.0 == 25) {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        host.shutdown(Duration::from_millis(30)).await;
        let held_runtime = resources() & 8 != 0;
        let held_native = flags();
        seen.extend(receiver.try_iter());
        let finish_before_release = seen.iter().any(|event| event.0 == 5);
        // Real previous-future Drop owns the sole pending retirement operation.
        // Release it and join real second ABI caller before asserting context.
        unsafe { &*barrier }.release();
        let second_result = next.join().unwrap();
        host.shutdown(Duration::from_secs(5)).await;
        while !seen
            .iter()
            .any(|event| event.0 == if mode == 48 { 6 } else { 5 })
        {
            seen.push(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        if mode == 48 {
            tokio::time::timeout(Duration::from_secs(5), async {
                while drops() == before {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        let after = flags();
        let later_resources = resources();
        seen.extend(receiver.try_iter());
        drop(host);
        release_views();
        if mode == 48 {
            unsafe {
                drop(Box::from_raw(barrier));
            }
        }
        // Mode49 context stays conservatively retained with later resources.
        assert_eq!(first_result, TB_OK);
        assert_eq!(second_result, TB_CLOSED);
        assert!(held_runtime && !finish_before_release);
        assert_ne!(
            held_native & tinybus::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT,
            0
        );
        assert_eq!(
            held_native & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
            0
        );
        assert_eq!(seen.iter().filter(|event| **event == (5, 1)).count(), 1);
        assert_eq!(seen.iter().filter(|event| **event == (7, 1)).count(), 1);
        assert!(
            seen.contains(&(38, 0)),
            "construction Handle guard ends before its ack and later foreign Drop: {seen:?}"
        );
        assert!(seen.iter().any(|event| event.0 == 39 && event.1 & 1 == 1));
        assert!(
            seen.contains(&(25, 1)),
            "actual previous Drop on the same physical worker"
        );
        if mode == 49 {
            assert_eq!(later_resources & (1 | 8 | 16 | 32), 1 | 8 | 16 | 32);
            assert!(!seen.iter().any(|event| matches!(event.0, 6 | 17 | 20)));
            assert_ne!(after & tinybus::module::abi::TB_NATIVE_TERMINAL_FAULT, 0);
            assert_eq!(
                after & tinybus::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
                0
            );
        }
    }
}
