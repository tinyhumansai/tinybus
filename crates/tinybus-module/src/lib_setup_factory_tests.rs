use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tinybus::{broker::Broker, module::ModuleHost, transport::memory::MemoryBus};

type SetupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type SetupFn = fn(Connection, Config) -> SetupFuture;
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static FACTORIES: AtomicUsize = AtomicUsize::new(0);
#[derive(serde::Deserialize)]
struct Config {
    restricted: bool,
}
struct Service {
    restricted: bool,
}
#[async_trait::async_trait]
impl tinybus::service::Interface for Service {
    fn name(&self) -> tinybus::InterfaceName {
        "ai.tinyhumans.tinybus.Factory".try_into().unwrap()
    }
    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Mode".try_into().unwrap()]
    }
    async fn call(
        &self,
        _member: &tinybus::MemberName,
        _args: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Ok(serde_json::json!(self.restricted))
    }
}
async fn legacy_setup(connection: Connection, config: Config) -> Result<()> {
    connection
        .serve_at(
            "/factory".try_into()?,
            Service {
                restricted: config.restricted,
            },
        )
        .await?;
    connection
        .request_name("ai.tinyhumans.tinybus.Factory")
        .await?;
    Ok(())
}
fn setup_factory()
-> impl Fn(Connection, Config) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Clone + Send + Sync
{
    FACTORIES.fetch_add(1, Ordering::SeqCst);
    let mode = Arc::new(tokio::sync::Mutex::new(None));
    move |connection, config: Config| {
        let mode = Arc::clone(&mode);
        Box::pin(async move {
            let mut admitted = mode.lock().await;
            if admitted.is_some_and(|previous| previous != config.restricted) {
                return Err(tinybus::Error::failed("immutable mode"));
            }
            *admitted = Some(config.restricted);
            legacy_setup(connection, config).await
        })
    }
}
module_export_static! {
    setup_factory = setup_factory,
    config = Config,
    worker_threads = 1,
    provides = ["ai.tinyhumans.tinybus.Factory"],
    methods = ["Mode"], signals = [], requires = [], optional = [], lazy = false,
}
async fn instance(restricted: bool) -> (ModuleHost, Connection, tinybus::Proxy) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    host.attach_linked_with_config(
        linked_module().unwrap(),
        serde_json::json!({"restricted":restricted}),
    )
    .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "ai.tinyhumans.tinybus.Factory",
            "/factory",
            "ai.tinyhumans.tinybus.Factory",
        )
        .unwrap();
    assert_eq!(proxy.call::<bool>("Mode", ()).await.unwrap(), restricted);
    (host, caller, proxy)
}
#[tokio::test(flavor = "current_thread")]
async fn compiled_factory_is_per_instance_and_reinitialization_keeps_its_admission() {
    let _guard = TEST_LOCK.lock().await;
    let before = FACTORIES.load(Ordering::SeqCst);
    let ((first, _first_caller, first_proxy), (second, _second_caller, second_proxy)) =
        tokio::join!(instance(true), instance(false));
    assert_eq!(
        FACTORIES.load(Ordering::SeqCst) - before,
        2,
        "factory must run once per ABI init"
    );
    let (same, changed) = tokio::join!(
        first.reinitialize("tinybus-module", serde_json::json!({"restricted":true})),
        second.reinitialize("tinybus-module", serde_json::json!({"restricted":true})),
    );
    assert!(same.is_ok());
    assert!(changed.is_err());
    assert!(first_proxy.call::<bool>("Mode", ()).await.unwrap());
    assert!(!second_proxy.call::<bool>("Mode", ()).await.unwrap());
    assert_eq!(
        FACTORIES.load(Ordering::SeqCst) - before,
        2,
        "reinitialization must reuse factory state"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn compiled_instance_refuses_mode_change_without_replacing_live_service() {
    let _guard = TEST_LOCK.lock().await;
    let (host, _caller, proxy) = instance(true).await;
    let result = host
        .reinitialize("tinybus-module", serde_json::json!({"restricted":false}))
        .await;
    assert!(
        result.is_err(),
        "setup factory admission must survive reinitialization"
    );
    assert!(proxy.call::<bool>("Mode", ()).await.unwrap());
}
#[tokio::test]
async fn captured_admission_serializes_concurrent_setup_attempts() {
    let _guard = TEST_LOCK.lock().await;
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let setup = setup_factory();
    let (first, second) = tokio::join!(
        setup(connection.clone(), Config { restricted: true }),
        setup(connection.clone(), Config { restricted: false })
    );
    assert_ne!(first.is_ok(), second.is_ok());
}

mod panicking {
    use super::*;
    fn factory() -> SetupFn {
        panic!("factory failed")
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.PanickingFactory"],
        methods = [], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[test]
fn factory_panic_is_a_native_initialization_refusal() {
    let broker = Broker::new();
    let host = ModuleHost::new(broker);
    let error = host
        .attach_linked_with_config(
            panicking::linked_module().unwrap(),
            serde_json::json!({"restricted":true}),
        )
        .unwrap_err();
    assert!(matches!(error, tinybus::Error::ModuleRefused { .. }));
}
#[tokio::test]
async fn invalid_initial_configuration_does_not_construct_factory_state() {
    let _guard = TEST_LOCK.lock().await;
    let before = FACTORIES.load(Ordering::SeqCst);
    let host = ModuleHost::new(Broker::new());
    assert!(
        host.attach_linked_with_config(
            linked_module().unwrap(),
            serde_json::json!({"restricted":"invalid"})
        )
        .is_err()
    );
    assert_eq!(FACTORIES.load(Ordering::SeqCst), before);
}

#[test]
fn native_factory_starter_validates_host_before_constructing_state() {
    let code = unsafe {
        start_reconfigurable_module_with_factory::<Config, _, _>(
            std::ptr::null(),
            std::ptr::null_mut(),
            1,
            true,
            || {
                panic!("invalid host must be rejected before factory");
                #[allow(unreachable_code)]
                setup_factory()
            },
        )
    };
    assert_eq!(code, TB_BAD_ARGUMENT);
}

mod copying_state {
    use super::*;
    #[derive(Default)]
    struct Mode(std::sync::Mutex<Option<bool>>);
    impl Clone for Mode {
        fn clone(&self) -> Self {
            Self(std::sync::Mutex::new(*self.0.lock().unwrap()))
        }
    }
    impl Mode {
        fn admit(&self, requested: bool) -> bool {
            let mut mode = self.0.lock().unwrap();
            if mode.is_some_and(|previous| previous != requested) {
                return false;
            }
            *mode = Some(requested);
            true
        }
    }
    fn factory() -> impl Fn(Connection, Config) -> SetupFuture + Clone + Send + Sync {
        let mode = Mode::default();
        move |connection, config: Config| {
            let accepted = mode.admit(config.restricted);
            Box::pin(async move {
                if !accepted {
                    return Err(tinybus::Error::failed("immutable mode"));
                }
                legacy_setup(connection, config).await
            })
        }
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.Factory"],
        methods = ["Mode"], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn factory_callback_with_copying_clone_keeps_initial_admission_on_reinitialize() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    host.attach_linked_with_config(
        copying_state::linked_module().unwrap(),
        serde_json::json!({"restricted":true}),
    )
    .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "ai.tinyhumans.tinybus.Factory",
            "/factory",
            "ai.tinyhumans.tinybus.Factory",
        )
        .unwrap();
    assert!(proxy.call::<bool>("Mode", ()).await.unwrap());
    assert!(
        host.reinitialize("tinybus-module", serde_json::json!({"restricted":false}))
            .await
            .is_err(),
        "a copying callback Clone must not reset the original admission"
    );
    assert!(proxy.call::<bool>("Mode", ()).await.unwrap());
}

mod context_factory_export {
    #![deny(missing_docs)]

    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Counts constructions so the test can distinguish instances from setups.
    pub(super) static FACTORIES: AtomicUsize = AtomicUsize::new(0);

    #[derive(serde::Deserialize)]
    struct Config {
        restricted: bool,
    }

    struct Service {
        mode: bool,
        generation: usize,
        trusted_host: TrustedHost,
    }

    #[async_trait::async_trait]
    impl tinybus::service::Interface for Service {
        fn name(&self) -> tinybus::InterfaceName {
            "org.example.ContextFactory".parse().unwrap()
        }

        fn members(&self) -> Vec<tinybus::MemberName> {
            vec!["Inspect".parse().unwrap()]
        }

        fn requires_confidential(&self, _: &tinybus::MemberName) -> bool {
            true
        }

        async fn call(
            &self,
            _: &tinybus::MemberName,
            _: serde_json::Value,
        ) -> tinybus::Result<serde_json::Value> {
            Err(tinybus::Error::failed("context-aware dispatch required"))
        }

        async fn call_with_context(
            &self,
            member: &tinybus::MemberName,
            args: serde_json::Value,
            context: &tinybus::CallContext,
        ) -> tinybus::Result<serde_json::Value> {
            if !self.trusted_host.authorizes(context) {
                return Err(tinybus::Error::failed(
                    "factory context authority unavailable",
                ));
            }
            if member.as_str() != "Inspect" {
                return Err(tinybus::Error::failed("unknown method"));
            }
            if args != serde_json::json!([]) {
                return Err(tinybus::Error::failed("expected no arguments"));
            }
            Ok(serde_json::json!({
                "restricted": self.mode,
                "generation": self.generation,
            }))
        }
    }

    fn factory() -> impl Fn(ModuleSetupContext, Config) -> SetupFuture + Send + Sync + 'static {
        FACTORIES.fetch_add(1, Ordering::SeqCst);
        let mode = Arc::new(tokio::sync::Mutex::new(None));
        let generation = Arc::new(AtomicUsize::new(0));
        move |context, config: Config| {
            let mode = Arc::clone(&mode);
            let generation = Arc::clone(&generation);
            Box::pin(async move {
                let mut admitted = mode.lock().await;
                if admitted.is_some_and(|initial| initial != config.restricted) {
                    return Err(tinybus::Error::failed("immutable mode"));
                }
                *admitted = Some(config.restricted);
                drop(admitted);
                let trusted_host = context
                    .trusted_host()
                    .cloned()
                    .ok_or_else(|| tinybus::Error::failed("trusted host unavailable"))?;
                let generation = generation.fetch_add(1, Ordering::SeqCst) + 1;
                context
                    .connection()
                    .serve_at(
                        "/context_factory".parse()?,
                        Service {
                            mode: config.restricted,
                            generation,
                            trusted_host,
                        },
                    )
                    .await?;
                context
                    .connection()
                    .request_name("org.example.ContextFactory")
                    .await
            })
        }
    }

    crate::module_export_static! {
        setup_factory_with_context = factory,
        config = Config,
        worker_threads = 1,
        provides = ["org.example.ContextFactory"],
        methods = ["Inspect"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }

    #[test]
    fn invalid_initial_configuration_does_not_construct_context_factory_state() {
        let _guard = TEST_LOCK.blocking_lock();
        let before = FACTORIES.load(Ordering::SeqCst);
        let host = ModuleHost::new(Broker::new());
        assert!(
            host.attach_linked_with_config(
                linked_module().unwrap(),
                serde_json::json!({"restricted":"invalid"}),
            )
            .is_err()
        );
        assert_eq!(FACTORIES.load(Ordering::SeqCst), before);
    }

    #[test]
    fn invalid_host_is_rejected_before_context_factory_construction() {
        let _guard = TEST_LOCK.blocking_lock();
        let before = FACTORIES.load(Ordering::SeqCst);
        let result = unsafe {
            start_linked_reconfigurable_module_with_context_factory::<Config, _, _>(
                std::ptr::null(),
                std::ptr::null_mut(),
                1,
                true,
                factory,
            )
        };
        assert_eq!(result, TB_BAD_ARGUMENT);
        assert_eq!(FACTORIES.load(Ordering::SeqCst), before);
    }
}

async fn contextual_factory_instance(
    restricted: bool,
) -> (ModuleHost, Broker, Connection, tinybus::Proxy) {
    use tinybus::transport::memory::MemoryTransport;

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
    let host = ModuleHost::new(broker.clone());
    host.attach_linked_with_config(
        context_factory_export::linked_module().unwrap(),
        serde_json::json!({"restricted":restricted}),
    )
    .unwrap();
    let proxy = controller
        .proxy(
            "org.example.ContextFactory",
            "/context_factory",
            "org.example.ContextFactory",
        )
        .unwrap();
    (host, broker, controller, proxy)
}

#[tokio::test(flavor = "current_thread")]
async fn contextual_factory_is_per_instance_authorized_confidential_and_reinit_stable() {
    let _guard = TEST_LOCK.lock().await;
    use tinybus::transport::memory::MemoryTransport;

    let before = context_factory_export::FACTORIES.load(Ordering::SeqCst);
    let ((first, broker, controller, first_proxy), (_second, _, _, second_proxy)) = tokio::join!(
        contextual_factory_instance(true),
        contextual_factory_instance(false)
    );
    assert_eq!(
        context_factory_export::FACTORIES.load(Ordering::SeqCst) - before,
        2,
        "the context factory must run once for each ABI instance"
    );
    assert!(first_proxy.attestation().await.unwrap().is_some());
    assert!(
        first_proxy
            .call::<serde_json::Value>("Inspect", ())
            .await
            .is_err()
    );
    assert_eq!(
        first_proxy
            .call_confidential::<serde_json::Value>("Inspect", ())
            .await
            .unwrap(),
        serde_json::json!({"restricted":true,"generation":1})
    );
    assert_eq!(
        second_proxy
            .call_confidential::<serde_json::Value>("Inspect", ())
            .await
            .unwrap(),
        serde_json::json!({"restricted":false,"generation":1})
    );

    let (outsider_server, outsider_client) = MemoryTransport::pair();
    broker.attach(Arc::new(outsider_server));
    let outsider = Connection::connect(Box::new(outsider_client))
        .await
        .unwrap();
    let outsider_proxy = outsider
        .proxy(
            "org.example.ContextFactory",
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
    let malformed = first_proxy
        .call_confidential::<serde_json::Value>("Inspect", serde_json::json!("malformed"))
        .await
        .unwrap_err();
    assert!(malformed.to_string().contains("expected no arguments"));

    first
        .reinitialize("tinybus-module", serde_json::json!({"restricted":true}))
        .await
        .unwrap();
    assert_eq!(
        first_proxy
            .call_confidential::<serde_json::Value>("Inspect", ())
            .await
            .unwrap(),
        serde_json::json!({"restricted":true,"generation":2})
    );
    assert!(
        first
            .reinitialize("tinybus-module", serde_json::json!({"restricted":false}))
            .await
            .is_err()
    );
    assert_eq!(
        first_proxy
            .call_confidential::<serde_json::Value>("Inspect", ())
            .await
            .unwrap(),
        serde_json::json!({"restricted":true,"generation":2}),
        "a refused reinitialization must leave the live service and its trusted context intact"
    );
    assert_eq!(
        second_proxy
            .call_confidential::<serde_json::Value>("Inspect", ())
            .await
            .unwrap(),
        serde_json::json!({"restricted":false,"generation":1})
    );
    controller.close().await.unwrap();
}

mod failed_setup {
    use super::*;
    pub(super) static DROPPED: AtomicUsize = AtomicUsize::new(0);
    struct Capture;
    impl Drop for Capture {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn factory() -> impl Fn(Connection, Config) -> SetupFuture + Send + Sync {
        let capture = Capture;
        move |_, _| {
            let _ = &capture;
            Box::pin(async { Err(tinybus::Error::failed("setup refused")) })
        }
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.FailedFactory"],
        methods = [], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn failed_initial_setup_releases_factory_capture_when_host_stops_instance() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus);
    let host = ModuleHost::new(broker);
    let before = failed_setup::DROPPED.load(Ordering::SeqCst);
    host.attach_linked_with_config(
        failed_setup::linked_module().unwrap(),
        serde_json::json!({"restricted":true}),
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if host.list().iter().any(|m| {
                matches!(
                    m.state,
                    tinybus::module::ModuleState::Failed { .. }
                        | tinybus::module::ModuleState::Faulted { .. }
                )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    host.shutdown(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        failed_setup::DROPPED.load(Ordering::SeqCst) - before,
        1,
        "failed instance tombstone must release callback capture"
    );
}

mod released_setup {
    use super::*;
    pub(super) static DROPPED: AtomicUsize = AtomicUsize::new(0);
    struct Capture;
    impl Drop for Capture {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn factory() -> impl Fn(Connection, Config) -> SetupFuture + Send + Sync {
        let capture = Capture;
        move |connection, config| {
            let _ = &capture;
            Box::pin(legacy_setup(connection, config))
        }
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.Factory"],
        methods = ["Mode"], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn compiled_successful_shutdown_releases_factory_capture() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let before = released_setup::DROPPED.load(Ordering::SeqCst);
    host.attach_linked_with_config(
        released_setup::linked_module().unwrap(),
        serde_json::json!({"restricted":true}),
    )
    .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "ai.tinyhumans.tinybus.Factory",
            "/factory",
            "ai.tinyhumans.tinybus.Factory",
        )
        .unwrap();
    assert!(proxy.call::<bool>("Mode", ()).await.unwrap());
    assert_eq!(released_setup::DROPPED.load(Ordering::SeqCst), before);
    host.shutdown(std::time::Duration::from_secs(1)).await;
    assert_eq!(released_setup::DROPPED.load(Ordering::SeqCst) - before, 1);
}
mod panicking_setup {
    use super::*;
    pub(super) static DROPPED: AtomicUsize = AtomicUsize::new(0);
    struct Capture;
    impl Drop for Capture {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn factory() -> impl Fn(Connection, Config) -> SetupFuture + Send + Sync {
        let capture = Capture;
        move |_, _| {
            let _ = &capture;
            Box::pin(async { panic!("harmless initial setup fixture") })
        }
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.PanickingFactory"],
        methods = [], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn panicking_initial_setup_releases_factory_capture_and_reports_fault() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus);
    let host = ModuleHost::new(broker);
    let before = panicking_setup::DROPPED.load(Ordering::SeqCst);
    host.attach_linked_with_config(
        panicking_setup::linked_module().unwrap(),
        serde_json::json!({"restricted":true}),
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if host
                .list()
                .iter()
                .any(|m| matches!(m.state, tinybus::module::ModuleState::Faulted { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(panicking_setup::DROPPED.load(Ordering::SeqCst) - before, 1);
}

mod worker_progress_setup {
    use super::*;
    pub(super) type Channels = (
        std::sync::mpsc::Sender<()>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    );
    pub(super) static CHANNELS: StdMutex<Option<Channels>> = StdMutex::new(None);
    struct Capture {
        started: std::sync::mpsc::Sender<()>,
        release: StdMutex<std::sync::mpsc::Receiver<()>>,
        notify: Arc<tokio::sync::Notify>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            self.started.send(()).unwrap();
            self.notify.notify_one();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
    fn factory() -> impl Fn(Connection, Config) -> SetupFuture + Send + Sync {
        let (started, release, progress) = CHANNELS.lock().unwrap().take().unwrap();
        let notify = Arc::new(tokio::sync::Notify::new());
        let capture = Capture {
            started,
            release: StdMutex::new(release),
            notify: notify.clone(),
        };
        move |_, _| {
            let _ = &capture;
            let notify = notify.clone();
            let progress = progress.clone();
            Box::pin(async move {
                tokio::spawn(async move {
                    notify.notified().await;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    progress.send(()).unwrap();
                });
                Err(tinybus::Error::failed("fixture setup refused"))
            })
        }
    }
    module_export_static! {
        setup_factory = factory, config = Config, worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.WorkerFactory"],
        methods = [], signals = [], requires = [], optional = [], lazy = false,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn compiled_single_worker_disposal_advances_timer_before_reporting_fault() {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    *worker_progress_setup::CHANNELS.lock().unwrap() = Some((started_tx, release_rx, progress_tx));
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus);
    let host = ModuleHost::new(broker);
    host.attach_linked_with_config(
        worker_progress_setup::linked_module().unwrap(),
        serde_json::json!({"restricted":true}),
    )
    .unwrap();
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(std::time::Duration::from_secs(5)))
        .await
        .unwrap()
        .unwrap();
    let progressed = tokio::task::spawn_blocking(move || {
        progress_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_ok()
    })
    .await
    .unwrap();
    let fault_before_completion = host
        .list()
        .iter()
        .any(|m| matches!(m.state, tinybus::module::ModuleState::Faulted { .. }));
    // Release disposal even on assertion failures.
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if host
                .list()
                .iter()
                .any(|m| matches!(m.state, tinybus::module::ModuleState::Faulted { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        progressed,
        "compiled module's sole timer worker must remain available"
    );
    assert!(
        !fault_before_completion,
        "Faulted must follow observed disposal completion"
    );
}
