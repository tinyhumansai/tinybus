use super::*;
#[cfg(feature = "static-link")]
use std::future::Future;
#[cfg(feature = "static-link")]
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
use std::sync::{OnceLock, mpsc::SyncSender};

static HOST_SEND_CODE: AtomicI32 = AtomicI32::new(TB_OK);
static HOST_WAKES: AtomicUsize = AtomicUsize::new(0);
static HOST_LOGS: AtomicUsize = AtomicUsize::new(0);
static HOST_READY: AtomicBool = AtomicBool::new(false);
static HOST_FAULTED: AtomicBool = AtomicBool::new(false);
static START_OUTGOING: OnceLock<StdMutex<Option<SyncSender<Vec<u8>>>>> = OnceLock::new();
static TRUSTED_HOST_REPLY: AtomicUsize = AtomicUsize::new(0);
static CAPTURED_TRUSTED_HOST: OnceLock<StdMutex<Option<TrustedHost>>> = OnceLock::new();
static CAPTURED_MODULE_NAME: OnceLock<StdMutex<Option<BusName>>> = OnceLock::new();
static CAPTURED_MODULE_CONTEXT: OnceLock<StdMutex<Option<CallContext>>> = OnceLock::new();
#[cfg(feature = "static-link")]
static FOREIGN_CONTEXT_SENDER: OnceLock<
    StdMutex<Option<tokio::sync::oneshot::Sender<CallContext>>>,
> = OnceLock::new();
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
        native_terminal_state: None,
        trusted_host_binding: None,
    }
}

unsafe extern "C" fn test_trusted_host_binding(
    _: *mut c_void,
    out: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32 {
    if out_len.is_null() {
        return TB_BAD_ARGUMENT;
    }
    let (code, bytes): (i32, &[u8]) = match TRUSTED_HOST_REPLY.load(Ordering::Acquire) {
        0 => (TB_OK, b":1.2"),
        1 => (TB_OK, b":1.3"),
        2 => (TB_CLOSED, b""),
        3 => {
            unsafe { out_len.write(capacity + 1) };
            return TB_OK;
        }
        4 => (TB_OK, b"\xff"),
        5 => (TB_OK, b"org.example.Controller"),
        _ => (91, b""),
    };
    unsafe { out_len.write(bytes.len()) };
    if code != TB_OK {
        return code;
    }
    if bytes.len() > capacity || (out.is_null() && !bytes.is_empty()) {
        return TB_BAD_ARGUMENT;
    }
    if !bytes.is_empty() {
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len()) };
    }
    TB_OK
}

unsafe extern "C" fn assert_test_broker_routed(_: *mut c_void) -> u32 {
    1
}

async fn setup_with_context(context: ModuleSetupContext) -> Result<()> {
    let trusted_host = context.trusted_host().cloned();
    *CAPTURED_TRUSTED_HOST
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("captured trusted host lock") = trusted_host.clone();
    *CAPTURED_MODULE_NAME
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("captured module name lock") = context.connection().unique_name();
    context
        .connection()
        .serve_at(
            "/authorization".parse().unwrap(),
            ContextAuthorizationProbe { trusted_host },
        )
        .await?;
    Ok(())
}

struct ContextAuthorizationProbe {
    trusted_host: Option<TrustedHost>,
}

#[async_trait::async_trait]
impl tinybus::service::Interface for ContextAuthorizationProbe {
    fn name(&self) -> tinybus::InterfaceName {
        "org.example.TrustedHostProbe".parse().unwrap()
    }

    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Authorize".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Err(Error::failed("context-aware dispatch required"))
    }

    async fn call_with_context(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
        context: &CallContext,
    ) -> tinybus::Result<serde_json::Value> {
        *CAPTURED_MODULE_CONTEXT
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("captured module context lock") = Some(context.clone());
        Ok(serde_json::json!(self.trusted_host.as_ref().is_some_and(
            |trusted_host| trusted_host.authorizes(context)
        )))
    }
}

#[cfg(feature = "static-link")]
async fn setup_foreign_context_capture(connection: Connection) -> Result<()> {
    connection
        .serve_at("/capture".parse().unwrap(), ForeignContextCapture)
        .await?;
    Ok(())
}

#[cfg(feature = "static-link")]
struct ForeignContextCapture;

#[cfg(feature = "static-link")]
#[async_trait::async_trait]
impl tinybus::service::Interface for ForeignContextCapture {
    fn name(&self) -> tinybus::InterfaceName {
        "org.example.ForeignContext".parse().unwrap()
    }

    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Capture".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Err(Error::failed("context-aware dispatch required"))
    }

    async fn call_with_context(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
        context: &CallContext,
    ) -> tinybus::Result<serde_json::Value> {
        FOREIGN_CONTEXT_SENDER
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("foreign context sender lock")
            .take()
            .expect("one foreign context capture")
            .send(context.clone())
            .map_err(|_| Error::failed("foreign context receiver closed"))?;
        Ok(serde_json::Value::Null)
    }
}

#[cfg(feature = "static-link")]
mod dynamic_context_export {
    crate::module_export! {
        setup_factory_with_context = super::setup_context_factory,
        config = super::ContextFactoryConfig,
        worker_threads = 1,
        provides = ["org.example.DynamicContext"],
        methods = ["Authorize"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[cfg(feature = "static-link")]
unsafe extern "C" fn plain_context_setup_entry(
    host: *const TbHostVtable,
    out: *mut TbModuleVtable,
) -> i32 {
    unsafe { start_module_with_context(host, out, 1, true, setup_with_context) }
}

#[cfg(feature = "static-link")]
static CONTEXT_FACTORY_CREATIONS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "static-link")]
static CONTEXT_FACTORY_SETUPS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "static-link")]
#[derive(serde::Deserialize)]
struct ContextFactoryConfig {}

#[cfg(feature = "static-link")]
type ContextFactorySetupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

#[cfg(feature = "static-link")]
fn setup_context_factory()
-> impl Fn(ModuleSetupContext, ContextFactoryConfig) -> ContextFactorySetupFuture + Send + Sync + 'static
{
    CONTEXT_FACTORY_CREATIONS.fetch_add(1, Ordering::SeqCst);
    let setups = Arc::new(AtomicUsize::new(0));
    move |context, _config| {
        let setups = Arc::clone(&setups);
        Box::pin(async move {
            setups.fetch_add(1, Ordering::SeqCst);
            CONTEXT_FACTORY_SETUPS.fetch_add(1, Ordering::SeqCst);
            setup_with_context(context).await
        })
    }
}

#[allow(dead_code)]
mod static_context_export {
    crate::module_export_static! {
        setup_with_context = super::setup_with_context,
        worker_threads = 1,
        provides = ["org.example.StaticContext"],
        methods = ["Authorize"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[cfg(feature = "static-link")]
mod foreign_context_export {
    crate::module_export_static! {
        setup = super::setup_foreign_context_capture,
        worker_threads = 1,
        provides = ["org.example.ForeignContext"],
        methods = ["Capture"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

fn start_context_export(
    init: unsafe extern "C" fn(*const TbHostVtable, *mut TbModuleVtable) -> i32,
    include_binding: bool,
    include_routing: bool,
) -> (TbModuleVtable, std::sync::mpsc::Receiver<Vec<u8>>) {
    HOST_READY.store(false, Ordering::Release);
    HOST_FAULTED.store(false, Ordering::Release);
    let (outgoing_tx, outgoing_rx) = std::sync::mpsc::sync_channel(8);
    *START_OUTGOING
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("startup capture lock") = Some(outgoing_tx);
    let mut host = host(b"{}");
    host.send = capture_host_send;
    if include_binding {
        host.trusted_host_binding = Some(test_trusted_host_binding);
    }
    if include_routing {
        host.broker_routing = Some(assert_test_broker_routed);
    }
    if !include_binding && !include_routing {
        host.size = tinybus::module::abi::TB_HOST_VTABLE_BASE_SIZE;
    }
    let mut out = TbModuleVtable::default();
    assert_eq!(unsafe { init(&host, &mut out) }, TB_OK);
    let hello_bytes = outgoing_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("contextual module sends Hello");
    let hello: Message = serde_json::from_slice(&hello_bytes).unwrap();
    let reply = Message::method_return(&hello.header, serde_json::json!(":1.1"));
    let reply_bytes = serde_json::to_vec(&reply).unwrap();
    assert_eq!(
        unsafe { (out.deliver)(out.module_ctx, reply_bytes.as_ptr(), reply_bytes.len()) },
        TB_OK
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !HOST_READY.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "contextual setup did not become ready"
        );
        std::thread::yield_now();
    }
    (out, outgoing_rx)
}

fn send_context_probe_call(
    out: &TbModuleVtable,
    receiver: &BusName,
    sender: &str,
    confidential: bool,
    outgoing: &std::sync::mpsc::Receiver<Vec<u8>>,
) -> serde_json::Value {
    let destination = receiver.clone();
    let path = "/authorization".parse().unwrap();
    let interface = "org.example.TrustedHostProbe".parse().unwrap();
    let member = "Authorize".parse().unwrap();
    let mut call = if confidential {
        Message::confidential_call(
            destination,
            path,
            interface,
            member,
            serde_json::Value::Null,
        )
    } else {
        Message::method_call(
            destination,
            path,
            interface,
            member,
            serde_json::Value::Null,
        )
    };
    call.header.sender = Some(sender.parse().unwrap());
    call.header.serial = 42;
    let bytes = serde_json::to_vec(&call).unwrap();
    assert_eq!(
        unsafe { (out.deliver)(out.module_ctx, bytes.as_ptr(), bytes.len()) },
        TB_OK
    );
    let reply = outgoing
        .recv_timeout(Duration::from_secs(1))
        .expect("contextual call reply");
    let reply: Message = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply.header.reply_serial, Some(42));
    reply.body
}

#[tokio::test]
async fn trusted_host_binding_parser_fails_closed_for_missing_or_malformed_callbacks() {
    let _host_state = host_state_guard().await;
    let missing = HostCalls(host(b"{}"));
    assert!(missing.trusted_host_peer().is_none());

    let mut table = host(b"{}");
    table.trusted_host_binding = Some(test_trusted_host_binding);
    let calls = HostCalls(table);

    TRUSTED_HOST_REPLY.store(0, Ordering::Release);
    assert_eq!(calls.trusted_host_peer().unwrap().as_str(), ":1.2");
    TRUSTED_HOST_REPLY.store(1, Ordering::Release);
    assert_eq!(calls.trusted_host_peer().unwrap().as_str(), ":1.3");
    for invalid in [2, 3, 4, 5, 6] {
        TRUSTED_HOST_REPLY.store(invalid, Ordering::Release);
        assert!(calls.trusted_host_peer().is_none(), "reply mode {invalid}");
    }
}

#[cfg(feature = "static-link")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contextual_dynamic_export_pins_and_revalidates_the_broker_controller() {
    let _host_state = host_state_guard().await;
    let before_factory = CONTEXT_FACTORY_CREATIONS.load(Ordering::SeqCst);
    let before_setups = CONTEXT_FACTORY_SETUPS.load(Ordering::SeqCst);
    TRUSTED_HOST_REPLY.store(0, Ordering::Release);
    *CAPTURED_TRUSTED_HOST
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;
    *CAPTURED_MODULE_CONTEXT
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;

    let init: unsafe extern "C" fn(*const TbHostVtable, *mut TbModuleVtable) -> i32 =
        dynamic_context_export::tinybus_module_init_v1;
    let (out, outgoing) = start_context_export(init, true, true);
    assert_eq!(
        CONTEXT_FACTORY_CREATIONS.load(Ordering::SeqCst) - before_factory,
        1,
        "the context factory runs once for this ABI instance"
    );
    assert_eq!(
        CONTEXT_FACTORY_SETUPS.load(Ordering::SeqCst) - before_setups,
        1
    );
    let receiver = CAPTURED_MODULE_NAME
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone()
        .expect("setup connection has its handshake name");
    let trusted_host = CAPTURED_TRUSTED_HOST
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone()
        .expect("valid host callback issues a trusted capability");

    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", true, &outgoing),
        serde_json::json!(true),
        "the confidential call from the selected peer on the setup connection is authorized"
    );
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", false, &outgoing),
        serde_json::json!(false),
        "a non-confidential call cannot use the migration capability"
    );
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.3", true, &outgoing),
        serde_json::json!(false),
        "a different authenticated sender cannot use the cached controller identity"
    );

    TRUSTED_HOST_REPLY.store(1, Ordering::Release);
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", true, &outgoing),
        serde_json::json!(false),
        "a fresh callback that names a replacement controller revokes the cached binding"
    );
    TRUSTED_HOST_REPLY.store(2, Ordering::Release);
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", true, &outgoing),
        serde_json::json!(false),
        "a disconnected controller revokes the cached binding"
    );

    TRUSTED_HOST_REPLY.store(0, Ordering::Release);
    let (foreign_context, _foreign_caller, _foreign_host, _foreign_broker) =
        capture_foreign_confidential_context().await;
    assert_eq!(
        foreign_context.authenticated_sender().unwrap().as_str(),
        ":1.2"
    );
    assert!(foreign_context.is_confidential());
    assert!(
        !trusted_host.authorizes(&foreign_context),
        "a confidential context from another live receiver cannot match even with the same sender name"
    );

    let captured = CAPTURED_MODULE_CONTEXT
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone()
        .expect("authorization probe retained an actual routed context");
    let shutdown = out.shutdown;
    let context = out.module_ctx as usize;
    let shutdown_code =
        tokio::task::spawn_blocking(move || unsafe { shutdown(context as *mut c_void, 1000) })
            .await
            .unwrap();
    assert_eq!(shutdown_code, TB_OK, "contextual module shutdown code");
    assert!(
        !trusted_host.authorizes(&captured),
        "the cloned capability cannot outlive its module connection"
    );
}

#[cfg(feature = "static-link")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_context_setup_entry_receives_the_selected_controller_binding() {
    let _host_state = host_state_guard().await;
    TRUSTED_HOST_REPLY.store(0, Ordering::Release);
    let (out, outgoing) = start_context_export(plain_context_setup_entry, true, true);
    let receiver = CAPTURED_MODULE_NAME
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone()
        .expect("plain context setup receives its attachment connection");
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", true, &outgoing),
        serde_json::json!(true)
    );
    let shutdown = out.shutdown;
    let context = out.module_ctx as usize;
    assert_eq!(
        tokio::task::spawn_blocking(move || unsafe { shutdown(context as *mut c_void, 1000) })
            .await
            .unwrap(),
        TB_OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contextual_static_export_accepts_an_old_host_without_the_optional_binding_tail() {
    let _host_state = host_state_guard().await;
    *CAPTURED_TRUSTED_HOST
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap() = None;

    let init: unsafe extern "C" fn(*const TbHostVtable, *mut TbModuleVtable) -> i32 =
        static_context_export::tinybus_module_init_v1;
    let (out, outgoing) = start_context_export(init, false, false);
    let receiver = CAPTURED_MODULE_NAME
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone()
        .expect("old host still completes ordinary setup");
    assert!(
        CAPTURED_TRUSTED_HOST
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap()
            .is_none(),
        "an old host with no trusted-binding tail gives setup no capability"
    );
    assert_eq!(
        send_context_probe_call(&out, &receiver, ":1.2", true, &outgoing),
        serde_json::json!(false)
    );
    let shutdown = out.shutdown;
    let context = out.module_ctx as usize;
    assert_eq!(
        tokio::task::spawn_blocking(move || unsafe { shutdown(context as *mut c_void, 1000) })
            .await
            .unwrap(),
        TB_OK
    );
}

#[cfg(feature = "static-link")]
async fn capture_foreign_confidential_context() -> (
    CallContext,
    Connection,
    tinybus::module::ModuleHost,
    tokio::task::JoinHandle<Result<()>>,
) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let module_host = tinybus::module::ModuleHost::new(broker);
    let linked = foreign_context_export::linked_module().unwrap();
    let info = module_host
        .attach_linked_with_config(linked, serde_json::json!({}))
        .unwrap();
    module_host
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
        .unwrap();
    let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
    *FOREIGN_CONTEXT_SENDER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("foreign context sender lock") = Some(captured_tx);
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let expected_sender = BusName::new(":1.2").unwrap();
    assert_eq!(caller.unique_name(), Some(expected_sender));
    caller
        .proxy(
            info.manifest.bus_name.as_str(),
            "/capture",
            "org.example.ForeignContext",
        )
        .unwrap()
        .call_confidential::<serde_json::Value>("Capture", ())
        .await
        .unwrap();
    (captured_rx.await.unwrap(), caller, module_host, broker_task)
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
    sender.try_send((vec![1], false)).unwrap();
    let state = RuntimeState {
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(Some(sender)),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
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
        .send((serde_json::to_vec(&message()).unwrap(), false))
        .await
        .unwrap();
    assert_eq!(transport.recv().await.unwrap().unwrap(), message());
    assert_eq!(HOST_WAKES.load(Ordering::Acquire), 1);
    transport.close().await.unwrap();
    assert!(transport.recv().await.unwrap().is_none());
    assert_eq!(transport.describe(), "module");
}

#[tokio::test]
async fn module_transport_close_wakes_an_idle_pending_receive() {
    let _host_state = host_state_guard().await;
    let (sender, receiver) = mpsc::channel(1);
    let transport = Arc::new(ModuleTransport {
        host: HostCalls(host(&[])),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    });
    let receiving_transport = transport.clone();
    let (receive_started, receive_started_rx) = tokio::sync::oneshot::channel();
    let receiving = tokio::spawn(async move {
        let _ = receive_started.send(());
        receiving_transport.recv().await
    });
    receive_started_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if transport.inbound.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the module receive is pending while it owns the receiver");

    let closing_transport = transport.clone();
    let (close_started, close_started_rx) = tokio::sync::oneshot::channel();
    let mut closing = tokio::spawn(async move {
        let _ = close_started.send(());
        closing_transport.close().await
    });
    close_started_rx.await.unwrap();
    match tokio::time::timeout(Duration::from_secs(2), &mut closing).await {
        Ok(result) => assert!(result.unwrap().is_ok()),
        Err(_) => {
            // Negative control for the old implementation: one inbound frame
            // releases the receiver mutex that close is incorrectly waiting on.
            let frame = serde_json::to_vec(&message()).unwrap();
            sender.send((frame, false)).await.unwrap();
            assert!(receiving.await.unwrap().unwrap().is_some());
            assert!(closing.await.unwrap().is_ok());
            panic!("close must wake an idle module receive without another frame");
        }
    }
    assert!(receiving.await.unwrap().unwrap().is_none());
    transport.close().await.unwrap();
    assert!(
        sender
            .try_send((serde_json::to_vec(&message()).unwrap(), false))
            .is_err()
    );
}

#[tokio::test]
async fn module_transport_close_before_recv_drains_queued_frames_and_rejects_new_ones() {
    let _host_state = host_state_guard().await;
    let (sender, receiver) = mpsc::channel(2);
    let transport = ModuleTransport {
        host: HostCalls(host(&[])),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    };
    let first = message();
    let mut second = message();
    second.header.serial = 2;
    for frame in [&first, &second] {
        sender
            .send((serde_json::to_vec(frame).unwrap(), false))
            .await
            .unwrap();
    }

    transport.close().await.unwrap();
    transport.close().await.unwrap();
    assert_eq!(transport.recv().await.unwrap(), Some(first));
    assert_eq!(transport.recv().await.unwrap(), Some(second));
    assert!(transport.recv().await.unwrap().is_none());
    assert!(
        sender
            .try_send((serde_json::to_vec(&message()).unwrap(), false))
            .is_err()
    );
}

#[tokio::test]
async fn module_transport_send_after_close_is_refused_before_host_delivery() {
    let _host_state = host_state_guard().await;
    let (_sender, receiver) = mpsc::channel(1);
    let transport = ModuleTransport {
        host: HostCalls(host(&[])),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    };
    HOST_SEND_CODE.store(TB_OK, Ordering::Release);
    transport.close().await.unwrap();

    assert!(matches!(
        transport.send(message()).await,
        Err(Error::ConnectionClosed)
    ));
}

#[tokio::test]
async fn a_cancelled_module_transport_close_leaves_a_persistent_wakeup() {
    let _host_state = host_state_guard().await;
    let (sender, receiver) = mpsc::channel(1);
    let transport = Arc::new(ModuleTransport {
        host: HostCalls(host(&[])),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    });
    let receiver_lock = transport.inbound.lock().await;
    let closing_transport = transport.clone();
    let closing = tokio::spawn(async move { closing_transport.close().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !*transport.closed.borrow() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("close publishes its wakeup before waiting for the receiver lock");
    closing.abort();
    let _ = closing.await;
    drop(receiver_lock);

    assert!(transport.recv().await.unwrap().is_none());
    assert!(sender.is_closed());
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

    // A stopped instance does not own the next instance's execution context.
    // Starting the same SDK again must succeed without retaining its log hook.
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
        TB_OK
    );
    assert_eq!(unsafe { (second.shutdown)(second.module_ctx, 1000) }, TB_OK);
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
    let (tx, rx) = mpsc::channel::<(Vec<u8>, bool)>(1);
    drop(rx);
    let state = RuntimeState {
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(Some(tx)),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
        owner: None,
        host: HostCalls(host(b"{}")),
        inbound: StdMutex::new(None),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
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
    assert!(normalized.native_terminal_state.is_none());
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
    current.size = (std::mem::offset_of!(TbHostVtable, broker_routing)
        + size_of::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>()
        - 1) as u32;
    let normalized = unsafe { TbHostVtable::read_compatible(&current) }.unwrap();
    assert!(normalized.broker_routing.is_none());
    assert!(normalized.native_terminal_state.is_none());
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

unsafe extern "C" fn atomic_routing_assertion(ctx: *mut c_void) -> u32 {
    unsafe { &*ctx.cast::<AtomicBool>() }.load(Ordering::Acquire) as u32
}

#[tokio::test]
async fn admission_never_promotes_a_frame_already_queued_without_provenance() {
    let _guard = host_state_guard().await;
    let admitted = AtomicBool::new(false);
    let mut table = host(b"{}");
    table.host_ctx = std::ptr::from_ref(&admitted).cast_mut().cast();
    table.broker_routing = Some(atomic_routing_assertion);
    let (sender, receiver) = mpsc::channel(2);
    let state = RuntimeState {
        owner: None,
        host: HostCalls(table),
        inbound: StdMutex::new(Some(sender)),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(None)),
    };
    let transport = ModuleTransport {
        host: HostCalls(table),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    };
    let mut frame = message();
    frame.header.sender = Some(":1.99".parse().unwrap());
    let bytes = serde_json::to_vec(&frame).unwrap();
    let enqueue = || unsafe {
        deliver(
            std::ptr::from_ref(&state).cast_mut().cast(),
            bytes.as_ptr(),
            bytes.len(),
        )
    };
    assert_eq!(enqueue(), TB_OK);
    admitted.store(true, Ordering::Release);
    assert_eq!(enqueue(), TB_OK);
    assert_eq!(transport.recv().await.unwrap().unwrap(), frame);
    assert!(!transport.provenance.load(Ordering::Acquire));
    assert_eq!(transport.recv().await.unwrap().unwrap(), frame);
    assert!(transport.provenance.load(Ordering::Acquire));
}

#[tokio::test]
async fn old_hosts_retain_sender_headers_without_authenticating_them() {
    let _guard = host_state_guard().await;
    let (sender, receiver) = mpsc::channel(1);
    let transport = ModuleTransport {
        host: HostCalls(host(b"{}")),
        inbound: Mutex::new(receiver),
        closed: close_signal(),
        provenance: Arc::new(AtomicBool::new(false)),
        detach_on_panic: false,
    };
    let mut frame = message();
    frame.header.sender = Some(":1.99".parse().unwrap());
    sender
        .send((serde_json::to_vec(&frame).unwrap(), false))
        .await
        .unwrap();
    assert_eq!(transport.recv().await.unwrap().unwrap(), frame);
    // No routing callback means startup selects ordinary Connection::attach;
    // sender headers remain protocol data, not incoming-call authority.
    assert!(transport.host.0.broker_routing.is_none());
}

struct UnverifiedContext;
#[async_trait]
impl tinybus::Interface for UnverifiedContext {
    fn name(&self) -> tinybus::InterfaceName {
        "org.example.Context".parse().unwrap()
    }
    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Inspect".parse().unwrap()]
    }
    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> Result<serde_json::Value> {
        Err(Error::failed("context required"))
    }
    async fn call_with_context(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
        context: &tinybus::CallContext,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::json!(context.authenticated_sender().is_some()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unverified_native_frames_preserve_distinct_stream_owners_and_unverified_context() {
    let _guard = host_state_guard().await;
    for compatible_callback in [false, true] {
        let admitted = AtomicBool::new(false);
        let mut table = host(b"{}");
        table.send = capture_host_send;
        if compatible_callback {
            table.host_ctx = std::ptr::from_ref(&admitted).cast_mut().cast();
            table.broker_routing = Some(atomic_routing_assertion);
        }
        let (outgoing_tx, mut outgoing_rx) = std::sync::mpsc::sync_channel(2);
        *START_OUTGOING
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = Some(outgoing_tx);
        let (sender, receiver) = mpsc::channel(2);
        let state = RuntimeState {
            owner: None,
            host: HostCalls(table),
            inbound: StdMutex::new(Some(sender)),
            runtime: StdMutex::new(None),
            reinitialize: Arc::new(StdMutex::new(None)),
        };
        let transport = Arc::new(ModuleTransport {
            host: HostCalls(table),
            inbound: Mutex::new(receiver),
            closed: close_signal(),
            provenance: Arc::new(AtomicBool::new(false)),
            detach_on_panic: false,
        });
        let connection = module_connection(transport);
        connection
            .serve_at("/context".parse().unwrap(), UnverifiedContext)
            .await
            .unwrap();
        let mut serial = 0;
        for (member, args, peer) in [
            ("Open", serde_json::json!([{}]), ":1.1"),
            ("Write", serde_json::json!(["s1", 0, "eA=="]), ":1.2"),
            ("Write", serde_json::json!(["s1", 0, "eA=="]), ":1.1"),
            ("Inspect", serde_json::json!([]), ":1.1"),
            ("Inspect", serde_json::json!([]), ":1.2"),
        ] {
            serial += 1;
            let inspect = member == "Inspect";
            let mut frame = Message::method_call(
                ":1.99".parse().unwrap(),
                if inspect {
                    "/context"
                } else {
                    tinybus::stream::STREAM_PATH
                }
                .parse()
                .unwrap(),
                if inspect {
                    "org.example.Context"
                } else {
                    tinybus::stream::STREAM_INTERFACE
                }
                .parse()
                .unwrap(),
                member.parse().unwrap(),
                args,
            );
            frame.header.serial = serial;
            frame.header.sender = Some(peer.parse().unwrap());
            let bytes = serde_json::to_vec(&frame).unwrap();
            assert_eq!(
                unsafe {
                    deliver(
                        std::ptr::from_ref(&state).cast_mut().cast(),
                        bytes.as_ptr(),
                        bytes.len(),
                    )
                },
                TB_OK
            );
            let (bytes, receiver) = tokio::task::spawn_blocking(move || {
                let bytes = outgoing_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                (bytes, outgoing_rx)
            })
            .await
            .unwrap();
            outgoing_rx = receiver;
            let reply: Message = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(reply.header.reply_serial, Some(serial));
            if peer == ":1.2" && member == "Write" {
                assert_eq!(
                    reply.header.error_name.as_deref(),
                    Some("ai.tinyhumans.tinybus.Error.UnknownStream"),
                    "wrong peer rejected with callback={compatible_callback}"
                );
            } else {
                assert!(reply.header.error_name.is_none(), "{reply:?}");
                if inspect {
                    assert_eq!(reply.body, serde_json::json!(false));
                }
            }
        }
        drop(connection);
    }
}
