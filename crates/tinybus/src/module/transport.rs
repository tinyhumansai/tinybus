//! Host-side transport bridge over the module C vtables.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
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

struct HostContext {
    inbound: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    wake: Arc<Notify>,
    config: StdMutex<Vec<u8>>,
    faulted: AtomicBool,
    init_failed: AtomicBool,
    ready: AtomicBool,
    ready_notify: Notify,
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

impl ModuleTransport {
    pub(crate) fn new(label: String, config: Vec<u8>) -> (Arc<Self>, TbHostVtable) {
        let (inbound_tx, inbound_rx) = mpsc::channel(HOST_QUEUE_CAPACITY);
        let context = Box::leak(Box::new(HostContext {
            inbound: StdMutex::new(Some(inbound_tx)),
            wake: Arc::new(Notify::new()),
            config: StdMutex::new(config),
            faulted: AtomicBool::new(false),
            init_failed: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            ready_notify: Notify::new(),
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
                let mut module = TbModuleVtable::default();
                let code = unsafe { init(&host, &mut module) };
                self.clear_config();
                if code != TB_OK {
                    return Err("module initialization failed".to_string());
                }
                self.initialize(module)
                    .map_err(|_| "module returned an invalid vtable".to_string())?;
                Ok(())
            })
            .await;
        result.clone().map_err(Error::transport)
    }

    pub(crate) async fn wait_ready(&self) {
        while !self.context.ready.load(Ordering::Acquire)
            && !self.context.faulted.load(Ordering::Acquire)
        {
            self.context.ready_notify.notified().await;
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
                TB_BACKPRESSURE => notified.await,
                TB_CLOSED => return Err(Error::ConnectionClosed),
                TB_BAD_ARGUMENT => return Err(Error::protocol("module refused a valid frame")),
                _ => return Err(Error::transport("module delivery callback failed")),
            }
        }
    }

    async fn drain_pending(self: Arc<Self>) {
        self.wait_ready().await;
        while !self.is_faulted() {
            let message = self.pending.lock().await.pop_front();
            let Some(message) = message else {
                break;
            };
            if self.deliver_now(message).await.is_err() {
                break;
            }
        }
        self.drain_started.store(false, Ordering::Release);
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
        if message.header.kind == MessageKind::MethodCall && !self.is_ready() {
            self.pending.lock().await.push_back(message);
            if !self.drain_started.swap(true, Ordering::AcqRel) {
                let transport = self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?;
                tokio::spawn(transport.drain_pending());
            }
            return Ok(());
        }
        self.deliver_now(message).await
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let bytes = self.inbound.lock().await.recv().await;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn close(&self) -> Result<()> {
        let _ = self.stop_sync(Duration::from_secs(5));
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
    if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
        context.wake.notify_one();
    }
}

unsafe extern "C" fn host_log(ctx: *mut c_void, level: u32, ptr: *const u8, len: usize) {
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
}

unsafe extern "C" fn host_fault(ctx: *mut c_void, _: *const u8, _: usize) {
    if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
        context.faulted.store(true, Ordering::Release);
        context.ready_notify.notify_waiters();
        context.inbound.lock().expect("host inbound lock").take();
    }
}

unsafe extern "C" fn host_ready(ctx: *mut c_void) {
    if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
        context.ready.store(true, Ordering::Release);
        context.ready_notify.notify_waiters();
    }
}
