//! Host-side transport bridge over the module C vtables.

use std::ffi::c_void;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::error::{Error, Result};
use crate::message::Message;
use crate::message::codec::MAX_FRAME_LEN;
use crate::module::abi::{
    TB_BACKPRESSURE, TB_BAD_ARGUMENT, TB_CLOSED, TB_OK, TbHostVtable, TbModuleVtable,
};
use crate::ports::Transport;

const HOST_QUEUE_CAPACITY: usize = 256;

struct HostContext {
    inbound: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    wake: Arc<Notify>,
}

/// The broker-facing side of one loaded module.
pub(crate) struct ModuleTransport {
    module: StdMutex<Option<TbModuleVtable>>,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
    context: &'static HostContext,
    label: String,
}

// `module_ctx` is opaque and all access to it goes through callbacks whose ABI
// contract requires thread safety. The vtable itself is immutable after init.
unsafe impl Send for ModuleTransport {}
unsafe impl Sync for ModuleTransport {}

impl ModuleTransport {
    pub(crate) fn new(label: String) -> (Arc<Self>, TbHostVtable) {
        let (inbound_tx, inbound_rx) = mpsc::channel(HOST_QUEUE_CAPACITY);
        let context = Box::leak(Box::new(HostContext {
            inbound: StdMutex::new(Some(inbound_tx)),
            wake: Arc::new(Notify::new()),
        }));
        let transport = Arc::new(Self {
            module: StdMutex::new(None),
            inbound: Mutex::new(inbound_rx),
            context,
            label,
        });
        let vtable = TbHostVtable {
            size: size_of::<TbHostVtable>() as u32,
            _reserved: 0,
            host_ctx: std::ptr::from_ref(context).cast_mut().cast(),
            send: host_send,
            wake: host_wake,
            log: host_log,
            fault: host_fault,
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

    pub(crate) fn shutdown_sync(&self, deadline: Duration) -> i32 {
        let module = *self.module.lock().expect("module vtable lock");
        match module {
            Some(module) => unsafe {
                (module.shutdown)(module.module_ctx, deadline.as_millis() as u64)
            },
            None => TB_CLOSED,
        }
    }
}

#[async_trait]
impl Transport for ModuleTransport {
    async fn send(&self, message: Message) -> Result<()> {
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
                TB_BAD_ARGUMENT => {
                    return Err(Error::protocol("module refused a valid frame"));
                }
                _ => return Err(Error::transport("module delivery callback failed")),
            }
        }
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let bytes = self.inbound.lock().await.recv().await;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn close(&self) -> Result<()> {
        let _ = self.shutdown_sync(Duration::from_secs(5));
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
        Ok(())
    }

    fn describe(&self) -> String {
        format!("module:{}", self.label)
    }
}

unsafe extern "C" fn host_send(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || ptr.is_null() || len > MAX_FRAME_LEN {
        return TB_BAD_ARGUMENT;
    }
    let context = unsafe { &*(ctx.cast::<HostContext>()) };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    let sender = context.inbound.lock().expect("host inbound lock").clone();
    match sender {
        Some(sender) => match sender.try_send(bytes) {
            Ok(()) => TB_OK,
            Err(mpsc::error::TrySendError::Full(_)) => TB_BACKPRESSURE,
            Err(mpsc::error::TrySendError::Closed(_)) => TB_CLOSED,
        },
        None => TB_CLOSED,
    }
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
        context.inbound.lock().expect("host inbound lock").take();
    }
}
