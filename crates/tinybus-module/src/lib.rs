//! Module-side runtime for trusted tinybus `cdylib` integrations.
//!
//! Each module owns a Tokio runtime. A statically linked `cdylib` has its own
//! Tokio thread-locals, so attempting to borrow the host runtime is both
//! incorrect and capable of silently blocking a host worker thread.

use std::ffi::c_void;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use tinybus::message::Message;
use tinybus::message::codec::MAX_FRAME_LEN;
use tinybus::module::abi::{
    TB_BACKPRESSURE, TB_BAD_ARGUMENT, TB_CLOSED, TB_OK, TB_PANICKED, TB_TIMEOUT, TbHostVtable,
    TbModuleVtable,
};
use tinybus::{Connection, Error, Result, Transport};
use tokio::sync::{Mutex, mpsc};

const MODULE_QUEUE_CAPACITY: usize = 256;

#[derive(Clone, Copy)]
struct HostCalls(TbHostVtable);

// The opaque pointer belongs to the host and is explicitly valid for the
// process lifetime. Calls are required to be thread-safe by the ABI contract.
unsafe impl Send for HostCalls {}
unsafe impl Sync for HostCalls {}

impl HostCalls {
    fn send(&self, bytes: &[u8]) -> i32 {
        unsafe { (self.0.send)(self.0.host_ctx, bytes.as_ptr(), bytes.len()) }
    }

    fn wake(&self) {
        unsafe { (self.0.wake)(self.0.host_ctx) }
    }

    fn fault(&self) {
        unsafe { (self.0.fault)(self.0.host_ctx, std::ptr::null(), 0) }
    }

    fn log(&self, level: u32, message: &[u8]) {
        unsafe {
            (self.0.log)(
                self.0.host_ctx,
                level,
                message.as_ptr(),
                message.len(),
            )
        }
    }
}

struct ModuleTransport {
    host: HostCalls,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
}

#[async_trait]
impl Transport for ModuleTransport {
    async fn send(&self, message: Message) -> Result<()> {
        let bytes = serde_json::to_vec(&message)?;
        if bytes.len() > MAX_FRAME_LEN {
            return Err(Error::protocol("module frame exceeds the size cap"));
        }
        match self.host.send(&bytes) {
            TB_OK => Ok(()),
            TB_BACKPRESSURE => Err(Error::Backpressure),
            TB_CLOSED => Err(Error::ConnectionClosed),
            _ => Err(Error::transport("module host refused a frame")),
        }
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let bytes = self.inbound.lock().await.recv().await;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        self.host.wake();
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn close(&self) -> Result<()> {
        self.inbound.lock().await.close();
        Ok(())
    }

    fn describe(&self) -> String {
        "module".to_string()
    }
}

struct RuntimeState {
    inbound: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    runtime: StdMutex<Option<tokio::runtime::Runtime>>,
}

unsafe extern "C" fn deliver(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    match catch_unwind(AssertUnwindSafe(|| {
        if ctx.is_null() || ptr.is_null() || len > MAX_FRAME_LEN {
            return TB_BAD_ARGUMENT;
        }
        let state = unsafe { &*(ctx.cast::<RuntimeState>()) };
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        let sender = state.inbound.lock().expect("module inbound lock").clone();
        match sender {
            Some(sender) => match sender.try_send(bytes) {
                Ok(()) => TB_OK,
                Err(mpsc::error::TrySendError::Full(_)) => TB_BACKPRESSURE,
                Err(mpsc::error::TrySendError::Closed(_)) => TB_CLOSED,
            },
            None => TB_CLOSED,
        }
    })) {
        Ok(code) => code,
        Err(_) => TB_PANICKED,
    }
}

unsafe extern "C" fn shutdown(ctx: *mut c_void, deadline_ms: u64) -> i32 {
    match catch_unwind(AssertUnwindSafe(|| {
        if ctx.is_null() {
            return TB_BAD_ARGUMENT;
        }
        let state = unsafe { &*(ctx.cast::<RuntimeState>()) };
        state.inbound.lock().expect("module inbound lock").take();
        match state.runtime.lock().expect("module runtime lock").take() {
            Some(runtime) => {
                runtime.shutdown_timeout(Duration::from_millis(deadline_ms));
                TB_OK
            }
            None => TB_CLOSED,
        }
    })) {
        Ok(code) => code,
        Err(_) => TB_TIMEOUT,
    }
}

/// Initialize the module runtime and start its async setup function.
///
/// This is public only for [`module_export!`] expansions. Module authors call
/// the macro, not this function directly.
#[doc(hidden)]
pub unsafe fn start_module<F, Fut>(
    host: *const TbHostVtable,
    out: *mut TbModuleVtable,
    worker_threads: usize,
    setup: F,
) -> i32
where
    F: FnOnce(Connection) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    match catch_unwind(AssertUnwindSafe(|| {
        if host.is_null() || out.is_null() || worker_threads == 0 {
            return TB_BAD_ARGUMENT;
        }
        let host = HostCalls(unsafe { *host });
        if host.0.size < size_of::<TbHostVtable>() as u32 {
            return TB_BAD_ARGUMENT;
        }

        let panic_host = host;
        std::panic::set_hook(Box::new(move |panic| {
            let location = panic.location().map_or_else(
                || "module panicked at an unknown location".to_string(),
                |location| {
                    format!(
                        "module panicked at {}:{}:{}",
                        location.file(),
                        location.line(),
                        location.column()
                    )
                },
            );
            // The payload is intentionally neither formatted nor forwarded:
            // it may contain arguments, credentials, or recovery material.
            panic_host.log(1, location.as_bytes());
            panic_host.fault();
        }));

        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => return TB_CLOSED,
        };
        let (inbound_tx, inbound_rx) = mpsc::channel(MODULE_QUEUE_CAPACITY);
        let transport = Box::new(ModuleTransport {
            host,
            inbound: Mutex::new(inbound_rx),
        });

        runtime.spawn(async move {
            let outcome = match Connection::connect(transport).await {
                Ok(connection) => setup(connection).await,
                Err(error) => Err(error),
            };
            if let Err(error) = outcome {
                tracing::error!(error = %error, "tinybus module failed");
                host.fault();
            }
        });

        let state = Box::new(RuntimeState {
            inbound: StdMutex::new(Some(inbound_tx)),
            runtime: StdMutex::new(Some(runtime)),
        });
        let state = Box::into_raw(state).cast::<c_void>();
        unsafe {
            out.write(TbModuleVtable {
                size: size_of::<TbModuleVtable>() as u32,
                _reserved: 0,
                module_ctx: state,
                deliver,
                shutdown,
            });
        }
        TB_OK
    })) {
        Ok(code) => code,
        Err(_) => TB_PANICKED,
    }
}

/// Export a tinybus module's descriptor, manifest and initialization entrypoint.
///
/// ```ignore
/// tinybus_module::module_export! { setup = setup, worker_threads = 1 }
/// ```
#[macro_export]
macro_rules! module_export {
    (setup = $setup:path, worker_threads = $threads:expr $(,)?) => {
        #[unsafe(no_mangle)]
        pub static TINYBUS_MODULE_ABI_V1: ::tinybus::module::abi::TbAbiDescriptor =
            ::tinybus::module::abi::TbAbiDescriptor::current(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            );

        #[unsafe(no_mangle)]
        pub extern "C" fn tinybus_module_manifest_v1() -> ::tinybus::module::abi::TbSlice {
            static MANIFEST: &str = concat!(
                "{\"name\":\"",
                env!("CARGO_PKG_NAME"),
                "\",\"version\":\"",
                env!("CARGO_PKG_VERSION"),
                "\",\"provides\":[],\"requires\":[],\"optional\":[],\"lazy\":false}"
            );
            ::tinybus::module::abi::TbSlice {
                ptr: MANIFEST.as_ptr(),
                len: MANIFEST.len(),
            }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn tinybus_module_init_v1(
            host: *const ::tinybus::module::abi::TbHostVtable,
            out: *mut ::tinybus::module::abi::TbModuleVtable,
        ) -> i32 {
            unsafe { $crate::start_module(host, out, $threads, $setup) }
        }
    };
}
