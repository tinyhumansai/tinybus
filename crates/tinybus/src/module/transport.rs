//! Host-side transport bridge over the module C vtables.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, OnceCell, mpsc};

use crate::error::{Error, Result};
use crate::message::codec::MAX_FRAME_LEN;
use crate::message::{Message, MessageKind};
use crate::module::abi::{
    TB_BACKPRESSURE, TB_BAD_ARGUMENT, TB_CLOSED, TB_OK, TbHostVtable, TbModuleVtable,
};
use crate::ports::Transport;

const HOST_QUEUE_CAPACITY: usize = 256;
const MODULE_INIT_DEADLINE: Duration = Duration::from_secs(5);

/// How long the host waits for a module to drain its queue before declaring it
/// faulted. Bounded so a module that stops calling `wake` cannot park the
/// delivery task for the process lifetime; the security boundary applies to
/// in-process modules too ("every call has a deadline").
const BACKPRESSURE_DEADLINE: Duration = Duration::from_secs(5);

struct HostContext {
    inbound: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    wake: Arc<Notify>,
    config: StdMutex<Vec<u8>>,
    faulted: AtomicBool,
    init_failed: AtomicBool,
    init_started: AtomicBool,
    init_notify: Notify,
    ready: AtomicBool,
    ready_notify: Notify,
    inflight: AtomicUsize,
}

/// The broker-facing side of one loaded module.
pub(crate) struct ModuleTransport {
    self_ref: Weak<ModuleTransport>,
    module: StdMutex<Option<TbModuleVtable>>,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
    context: &'static HostContext,
    label: String,
    initializer: StdMutex<Option<(crate::module::abi::TbModuleInit, TbHostVtable)>>,
    init_result: OnceCell<std::result::Result<(), String>>,
    pending: Mutex<VecDeque<Message>>,
    drain_started: AtomicBool,
}

// `module_ctx` is opaque and all access to it goes through callbacks whose ABI
// contract requires thread safety. The vtable itself is immutable after init.
unsafe impl Send for ModuleTransport {}
unsafe impl Sync for ModuleTransport {}

struct SendHostVtable(TbHostVtable);
struct SendModuleVtable(TbModuleVtable);

// The opaque host context is process-lifetime state and every callback is
// required by the ABI to be thread-safe.
unsafe impl Send for SendHostVtable {}
unsafe impl Send for SendModuleVtable {}

impl SendHostVtable {
    fn initialize(self, init: crate::module::abi::TbModuleInit) -> (i32, SendModuleVtable) {
        let mut module = TbModuleVtable::default();
        let code = unsafe { init(&self.0, &mut module) };
        (code, SendModuleVtable(module))
    }
}

impl ModuleTransport {
    pub(crate) fn new(label: String, config: Vec<u8>) -> (Arc<Self>, TbHostVtable) {
        let (inbound_tx, inbound_rx) = mpsc::channel(HOST_QUEUE_CAPACITY);
        let context = Box::leak(Box::new(HostContext {
            inbound: StdMutex::new(Some(inbound_tx)),
            wake: Arc::new(Notify::new()),
            config: StdMutex::new(config),
            faulted: AtomicBool::new(false),
            init_failed: AtomicBool::new(false),
            init_started: AtomicBool::new(false),
            init_notify: Notify::new(),
            ready: AtomicBool::new(false),
            ready_notify: Notify::new(),
            inflight: AtomicUsize::new(0),
        }));
        let transport = Arc::new_cyclic(|self_ref| Self {
            self_ref: self_ref.clone(),
            module: StdMutex::new(None),
            inbound: Mutex::new(inbound_rx),
            context,
            label,
            initializer: StdMutex::new(None),
            init_result: OnceCell::new(),
            pending: Mutex::new(VecDeque::new()),
            drain_started: AtomicBool::new(false),
        });
        let config = context.config.lock().expect("module config lock");
        let config_slice = crate::module::abi::TbSlice {
            ptr: config.as_ptr(),
            len: config.len(),
        };
        drop(config);
        let vtable = TbHostVtable {
            size: size_of::<TbHostVtable>() as u32,
            _reserved: 0,
            host_ctx: std::ptr::from_ref(context).cast_mut().cast(),
            send: host_send,
            wake: host_wake,
            log: host_log,
            fault: host_fault,
            config: config_slice,
            ready: host_ready,
        };
        (transport, vtable)
    }

    pub(crate) fn initialize(&self, module: TbModuleVtable) -> Result<()> {
        if module.size < size_of::<TbModuleVtable>() as u32 || module.module_ctx.is_null() {
            return Err(Error::transport("module returned an incomplete vtable"));
        }
        *self.module.lock().expect("module vtable lock") = Some(module);
        Ok(())
    }

    pub(crate) fn defer_initialize(
        &self,
        init: crate::module::abi::TbModuleInit,
        host: TbHostVtable,
    ) {
        *self.initializer.lock().expect("module initializer lock") = Some((init, host));
    }

    async fn ensure_initialized(&self) -> Result<()> {
        let result = self
            .init_result
            .get_or_init(|| async {
                self.context.init_started.store(true, Ordering::Release);
                self.context.init_notify.notify_waiters();
                let Some((init, host)) = self
                    .initializer
                    .lock()
                    .expect("module initializer lock")
                    .take()
                else {
                    return if self.module.lock().expect("module vtable lock").is_some() {
                        Ok(())
                    } else {
                        Err("module has no initializer".to_string())
                    };
                };
                // Module init is opaque synchronous code. Keep it off Tokio's
                // worker threads and bound how long the broker waits; a timed
                // out blocking task may remain wedged, so its borrowed config
                // remains allocated rather than being invalidated underneath it.
                let host = SendHostVtable(host);
                let initialized = tokio::time::timeout(
                    MODULE_INIT_DEADLINE,
                    tokio::task::spawn_blocking(move || host.initialize(init)),
                )
                .await;
                let (code, module) = match initialized {
                    Ok(Ok(initialized)) => initialized,
                    Ok(Err(_)) => return Err("module initialization panicked".to_string()),
                    Err(_) => return Err("module initialization exceeded its deadline".to_string()),
                };
                self.clear_config();
                if code != TB_OK {
                    return Err("module initialization failed".to_string());
                }
                self.initialize(module.0)
                    .map_err(|_| "module returned an invalid vtable".to_string())?;
                Ok(())
            })
            .await;
        result.clone().map_err(Error::transport)
    }

    pub(crate) async fn wait_ready(&self) {
        loop {
            let notified = self.context.ready_notify.notified();
            if self.context.ready.load(Ordering::Acquire)
                || self.context.faulted.load(Ordering::Acquire)
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.context.ready.load(Ordering::Acquire)
    }

    pub(crate) fn clear_config(&self) {
        let mut config = self.context.config.lock().expect("module config lock");
        config.fill(0);
        config.clear();
        config.shrink_to_fit();
    }

    pub(crate) fn is_faulted(&self) -> bool {
        self.context.faulted.load(Ordering::Acquire)
    }

    pub(crate) fn init_failed(&self) -> bool {
        self.context.init_failed.load(Ordering::Acquire)
    }

    pub(crate) fn init_started(&self) -> bool {
        self.context.init_started.load(Ordering::Acquire)
    }

    pub(crate) fn inflight(&self) -> usize {
        self.context.inflight.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_initializing(&self) {
        loop {
            let notified = self.context.init_notify.notified();
            if self.init_started() {
                return;
            }
            notified.await;
        }
    }

    async fn deliver_now(&self, message: Message) -> Result<()> {
        if self.context.faulted.load(Ordering::Acquire) {
            return Err(Error::ConnectionClosed);
        }
        let bytes = serde_json::to_vec(&message)?;
        if bytes.len() > MAX_FRAME_LEN {
            return Err(Error::protocol("module frame exceeds the size cap"));
        }

        loop {
            let notified = self.context.wake.notified();
            let module = *self.module.lock().expect("module vtable lock");
            let Some(module) = module else {
                return Err(Error::ConnectionClosed);
            };
            let code = unsafe { (module.deliver)(module.module_ctx, bytes.as_ptr(), bytes.len()) };
            match code {
                TB_OK => return Ok(()),
                TB_BACKPRESSURE => {
                    // A module that stops draining its queue and never calls
                    // `wake` must not park this delivery task — or the caller
                    // behind it — for the process lifetime. Expiry faults the
                    // module downstream so later callers get `ModuleUnavailable`
                    // instead of waiting out their own deadline.
                    if tokio::time::timeout(BACKPRESSURE_DEADLINE, notified)
                        .await
                        .is_err()
                    {
                        self.context.faulted.store(true, Ordering::Release);
                        self.context.ready_notify.notify_waiters();
                        self.context
                            .inbound
                            .lock()
                            .expect("host inbound lock")
                            .take();
                        return Err(Error::transport(
                            "module stopped draining its queue within the deadline",
                        ));
                    }
                }
                TB_CLOSED => return Err(Error::ConnectionClosed),
                TB_BAD_ARGUMENT => return Err(Error::protocol("module refused a valid frame")),
                _ => return Err(Error::transport("module delivery callback failed")),
            }
        }
    }

    async fn drain_pending(self: Arc<Self>) {
        self.wait_ready().await;
        loop {
            while !self.is_faulted() {
                let message = self.pending.lock().await.pop_front();
                let Some(message) = message else {
                    break;
                };
                if self.deliver_now(message.clone()).await.is_err() {
                    self.fault_pending(message).await;
                    return;
                }
            }
            if self.is_faulted() {
                self.drain_started.store(false, Ordering::Release);
                return;
            }
            // A `send` that pushed while the loop observed an empty queue saw
            // `drain_started == true` and skipped spawning its own drainer, so
            // it is this loop's job to pick the message up. Clear the flag only
            // while holding the queue lock, and return only once the queue is
            // genuinely empty; the lock makes the check-and-clear atomic with a
            // concurrent push.
            let restart = {
                let pending = self.pending.lock().await;
                let empty = pending.is_empty();
                if empty {
                    self.drain_started.store(false, Ordering::Release);
                }
                !empty
            };
            if !restart {
                return;
            }
            // Fall back into the drain loop for the newly queued messages.
        }
    }

    /// Answer every still-queued call — and the one that just failed to
    /// deliver — with `ModuleUnavailable`, then fault the module, so callers
    /// learn the delivery failed instead of waiting out their deadline.
    async fn fault_pending(&self, failed: Message) {
        let cleared = {
            let mut pending = self.pending.lock().await;
            std::mem::take(&mut *pending)
        };
        // Each of these calls incremented `inflight` when it was queued. The
        // synthesized error replies below travel back through `recv`, which
        // refunds one slot per reply, so no manual adjustment is needed.
        let sender = self
            .context
            .inbound
            .lock()
            .expect("host inbound lock")
            .clone();
        if let Some(sender) = sender {
            for queued in cleared.iter().chain([&failed]) {
                if queued.header.kind == MessageKind::MethodCall {
                    let error = Error::ModuleUnavailable {
                        module: self.label.clone(),
                        state: "faulted".to_string(),
                        detail: "module stopped accepting calls".to_string(),
                    };
                    let reply = Message::error_reply(&queued.header, &error);
                    if let Ok(bytes) = serde_json::to_vec(&reply) {
                        let _ = sender.try_send(bytes);
                    }
                }
            }
        }
        self.context.faulted.store(true, Ordering::Release);
        self.context.ready_notify.notify_waiters();
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
    }

    pub(crate) fn shutdown_sync(&self, deadline: Duration) -> i32 {
        let module = *self.module.lock().expect("module vtable lock");
        match module {
            Some(module) => unsafe {
                (module.shutdown)(module.module_ctx, deadline.as_millis() as u64)
            },
            None => TB_CLOSED,
        }
    }

    pub(crate) fn stop_sync(&self, deadline: Duration) -> i32 {
        let code = self.shutdown_sync(deadline);
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
        code
    }
}

#[async_trait]
impl Transport for ModuleTransport {
    async fn send(&self, message: Message) -> Result<()> {
        if self.ensure_initialized().await.is_err() {
            self.context.init_failed.store(true, Ordering::Release);
            self.context.faulted.store(true, Ordering::Release);
            self.context.ready_notify.notify_waiters();
            if message.header.kind == MessageKind::MethodCall {
                let error = Error::ModuleUnavailable {
                    module: self.label.clone(),
                    state: "failed".to_string(),
                    detail: "module initialization failed".to_string(),
                };
                let reply = Message::error_reply(&message.header, &error);
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    let sender = self
                        .context
                        .inbound
                        .lock()
                        .expect("host inbound lock")
                        .take();
                    if let Some(sender) = sender {
                        let _ = sender.try_send(bytes);
                    }
                }
                return Ok(());
            }
            return Err(Error::ConnectionClosed);
        }
        let is_call = message.header.kind == MessageKind::MethodCall;
        if is_call {
            self.context.inflight.fetch_add(1, Ordering::AcqRel);
        }
        if message.header.kind == MessageKind::MethodCall && !self.is_ready() {
            self.pending.lock().await.push_back(message);
            if !self.drain_started.swap(true, Ordering::AcqRel) {
                let transport = self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?;
                tokio::spawn(transport.drain_pending());
            }
            return Ok(());
        }
        let result = self.deliver_now(message).await;
        if result.is_err() && is_call {
            self.context.inflight.fetch_sub(1, Ordering::AcqRel);
        }
        result
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let bytes = self.inbound.lock().await.recv().await;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let message: Message = serde_json::from_slice(&bytes)?;
        if matches!(
            message.header.kind,
            MessageKind::MethodReturn | MessageKind::Error
        ) {
            let _ =
                self.context
                    .inflight
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        count.checked_sub(1)
                    });
        }
        Ok(Some(message))
    }

    async fn close(&self) -> Result<()> {
        // The module's `shutdown` callback may block up to its deadline. Run it
        // on a blocking thread so a wedged shutdown cannot stall the broker
        // task that is closing this transport.
        let transport = self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?;
        tokio::task::spawn_blocking(move || transport.stop_sync(Duration::from_secs(5)))
            .await
            .map_err(|_| Error::transport("module shutdown task was cancelled"))?;
        Ok(())
    }

    fn describe(&self) -> String {
        format!("module:{}", self.label)
    }
}

unsafe extern "C" fn host_send(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ctx.is_null() || ptr.is_null() || len > MAX_FRAME_LEN {
            return TB_BAD_ARGUMENT;
        }
        let context = unsafe { &*(ctx.cast::<HostContext>()) };
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        let sender = context.inbound.lock().expect("host inbound lock").clone();
        match sender {
            // This callback runs on a module-owned runtime thread. Blocking it
            // applies bounded backpressure only to that module and gives the
            // async SDK a reliable completion without adding a fifth callback
            // to the frozen v1 ABI.
            Some(sender) => match sender.blocking_send(bytes) {
                Ok(()) => TB_OK,
                Err(_) => TB_CLOSED,
            },
            None => TB_CLOSED,
        }
    }))
    .unwrap_or(TB_BACKPRESSURE)
}

unsafe extern "C" fn host_wake(ctx: *mut c_void) {
    // A panic escaping a plain `extern "C"` frame aborts the process, so each
    // host callback that a module can reach must contain its own unwinds.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            context.wake.notify_one();
        }
    }));
}

unsafe extern "C" fn host_log(ctx: *mut c_void, level: u32, ptr: *const u8, len: usize) {
    // `host_log` is the most exposed callback: the `tracing` macros run
    // arbitrary subscriber code chosen by the embedding host, so a panicking
    // subscriber must not be able to abort the process through the module.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ctx.is_null() || ptr.is_null() {
            return;
        }
        let len = len.min(4096);
        let message = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(ptr, len) });
        match level {
            1 => tracing::error!(target: "tinybus_module", message = %message),
            2 => tracing::warn!(target: "tinybus_module", message = %message),
            3 => tracing::info!(target: "tinybus_module", message = %message),
            4 => tracing::debug!(target: "tinybus_module", message = %message),
            _ => tracing::trace!(target: "tinybus_module", message = %message),
        }
    }));
}

unsafe extern "C" fn host_fault(ctx: *mut c_void, _: *const u8, _: usize) {
    // `host_fault` exists so a misbehaving module is detached instead of
    // taking the host down; an abort inside it would defeat that goal.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            context.faulted.store(true, Ordering::Release);
            context.ready_notify.notify_waiters();
            context.inbound.lock().expect("host inbound lock").take();
        }
    }));
}

unsafe extern "C" fn host_ready(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            context.ready.store(true, Ordering::Release);
            context.ready_notify.notify_waiters();
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;

    use crate::{BusName, InterfaceName, MemberName, ObjectPath};

    static DELIVERY_CODE: AtomicI32 = AtomicI32::new(TB_OK);
    static DELIVERIES: AtomicUsize = AtomicUsize::new(0);
    static SHUTDOWN_CODE: AtomicI32 = AtomicI32::new(TB_OK);

    unsafe extern "C" fn deliver(_: *mut c_void, _: *const u8, _: usize) -> i32 {
        DELIVERIES.fetch_add(1, Ordering::AcqRel);
        DELIVERY_CODE.load(Ordering::Acquire)
    }

    unsafe extern "C" fn shutdown(_: *mut c_void, _: u64) -> i32 {
        SHUTDOWN_CODE.load(Ordering::Acquire)
    }

    unsafe extern "C" fn initialize_ok(_: *const TbHostVtable, out: *mut TbModuleVtable) -> i32 {
        unsafe {
            *out = TbModuleVtable {
                size: size_of::<TbModuleVtable>() as u32,
                _reserved: 0,
                module_ctx: std::ptr::dangling_mut(),
                deliver,
                shutdown,
            };
        }
        TB_OK
    }

    unsafe extern "C" fn initialize_fails(_: *const TbHostVtable, _: *mut TbModuleVtable) -> i32 {
        TB_BAD_ARGUMENT
    }

    fn call() -> Message {
        Message::method_call(
            "org.example.Module".parse::<BusName>().unwrap(),
            "/org/example/Module".parse::<ObjectPath>().unwrap(),
            "org.example.Module".parse::<InterfaceName>().unwrap(),
            "Call".parse::<MemberName>().unwrap(),
            serde_json::Value::Array(Vec::new()),
        )
    }

    struct CaptureLevel(Arc<AtomicBool>);

    impl tracing::Subscriber for CaptureLevel {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::ERROR {
                self.0.store(true, Ordering::Release);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn a_module_log_line_reaches_the_hosts_subscriber_with_its_level() {
        let (_transport, host) = ModuleTransport::new("logger".to_string(), Vec::new());
        let observed = Arc::new(AtomicBool::new(false));
        let bytes = b"module log line";
        tracing::subscriber::with_default(CaptureLevel(observed.clone()), || unsafe {
            (host.log)(host.host_ctx, 1, bytes.as_ptr(), bytes.len());
        });
        assert!(observed.load(Ordering::Acquire));
    }

    #[test]
    fn incomplete_module_vtables_are_rejected_and_shutdown_reports_closed() {
        let (transport, _) = ModuleTransport::new("broken".to_string(), Vec::new());
        let incomplete = TbModuleVtable::default();
        assert!(transport.initialize(incomplete).is_err());
        assert_eq!(transport.shutdown_sync(Duration::ZERO), TB_CLOSED);
    }

    #[tokio::test]
    async fn an_uninitialized_module_answers_calls_and_rejects_signals() {
        let (transport, _) = ModuleTransport::new("missing".to_string(), Vec::new());
        transport.send(call()).await.unwrap();
        let reply = transport.recv().await.unwrap().unwrap();
        assert_eq!(reply.header.kind, MessageKind::Error);
        assert!(transport.init_failed());
        assert!(transport.is_faulted());

        let (transport, _) = ModuleTransport::new("missing".to_string(), Vec::new());
        let signal = Message::signal(
            "/org/example/Module".parse().unwrap(),
            "org.example.Module".parse().unwrap(),
            "Changed".parse().unwrap(),
            serde_json::Value::Null,
        );
        assert!(matches!(
            transport.send(signal).await,
            Err(Error::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn deferred_initialization_waits_for_ready_then_delivers_pending_calls() {
        DELIVERY_CODE.store(TB_OK, Ordering::Release);
        DELIVERIES.store(0, Ordering::Release);
        let (transport, host) =
            ModuleTransport::new("deferred".to_string(), br#"{"key":1}"#.to_vec());
        transport.defer_initialize(initialize_ok, host);
        transport.send(call()).await.unwrap();
        transport.wait_initializing().await;
        assert_eq!(transport.inflight(), 1);
        unsafe { (host.ready)(host.host_ctx) };
        tokio::time::timeout(Duration::from_secs(1), async {
            while DELIVERIES.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(transport.is_ready());
        assert!(transport.context.config.lock().unwrap().is_empty());
        assert_eq!(transport.stop_sync(Duration::from_millis(1)), TB_OK);
    }

    #[tokio::test]
    async fn a_failed_initializer_is_reported_to_callers() {
        let (transport, host) = ModuleTransport::new("fails-init".to_string(), Vec::new());
        transport.defer_initialize(initialize_fails, host);
        transport.send(call()).await.unwrap();
        assert_eq!(
            transport.recv().await.unwrap().unwrap().header.kind,
            MessageKind::Error
        );
        assert!(transport.init_failed());
    }

    #[tokio::test]
    async fn a_pending_call_is_failed_when_the_module_closes_its_delivery_queue() {
        DELIVERY_CODE.store(TB_CLOSED, Ordering::Release);
        let (transport, host) = ModuleTransport::new("closed".to_string(), Vec::new());
        transport.defer_initialize(initialize_ok, host);
        transport.send(call()).await.unwrap();
        unsafe { (host.ready)(host.host_ctx) };
        let reply = tokio::time::timeout(Duration::from_secs(1), transport.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reply.header.kind, MessageKind::Error);
        assert!(transport.is_faulted());
        DELIVERY_CODE.store(TB_OK, Ordering::Release);
    }

    #[tokio::test]
    async fn host_callbacks_accept_messages_and_make_the_transport_closed_on_fault() {
        let (transport, host) = ModuleTransport::new("callbacks".to_string(), Vec::new());
        let outgoing = serde_json::to_vec(&call()).unwrap();
        let host_ctx = host.host_ctx as usize;
        let send = host.send;
        let outbound = outgoing.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || unsafe {
                send(host_ctx as *mut c_void, outbound.as_ptr(), outbound.len())
            })
            .await
            .unwrap(),
            TB_OK
        );
        assert_eq!(transport.recv().await.unwrap().unwrap(), call());
        assert_eq!(
            unsafe { (host.send)(host.host_ctx, std::ptr::null(), 1) },
            TB_BAD_ARGUMENT
        );
        unsafe { (host.fault)(host.host_ctx, std::ptr::null(), 0) };
        assert!(transport.is_faulted());
        assert!(transport.recv().await.unwrap().is_none());
        assert_eq!(
            unsafe { (host.send)(host.host_ctx, outgoing.as_ptr(), outgoing.len()) },
            TB_CLOSED
        );
    }
}
