//! Module-side runtime for trusted tinybus `cdylib` integrations.
//!
//! Each module owns a Tokio runtime. A statically linked `cdylib` has its own
//! Tokio thread-locals, so attempting to borrow the host runtime is both
//! incorrect and capable of silently blocking a host worker thread.

use std::ffi::c_void;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex as StdMutex, OnceLock};
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
static MANIFEST_BYTES: OnceLock<Vec<u8>> = OnceLock::new();

/// Build and retain the exported manifest bytes for the process lifetime.
#[doc(hidden)]
pub fn manifest_slice(
    name: &str,
    version: &str,
    provides: &[&str],
    methods: &[&str],
    signals: &[&str],
    requires: &[&str],
    optional: &[&str],
    lazy: bool,
    worker_threads: u32,
) -> tinybus::module::abi::TbSlice {
    use tinybus::module::manifest::{
        Dependency, MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy, ProvidedInterface,
    };
    use tinybus::{BusName, InterfaceName, InterfaceVersion, ObjectPath, Version};

    let bytes = MANIFEST_BYTES.get_or_init(|| {
        let package_version = Version::parse(version).expect("package version is semver");
        let provided = |(index, interface): (usize, &&str)| ProvidedInterface {
            version: InterfaceVersion::provided(
                InterfaceName::new(*interface).expect("provided interface is valid"),
                package_version.clone(),
            ),
            methods: if index == 0 {
                methods
                    .iter()
                    .map(|member| tinybus::MemberName::new(*member).expect("method is valid"))
                    .collect()
            } else {
                Vec::new()
            },
            signals: if index == 0 {
                signals
                    .iter()
                    .map(|member| tinybus::MemberName::new(*member).expect("signal is valid"))
                    .collect()
            } else {
                Vec::new()
            },
        };
        let dependency = |interface: &&str, optional| Dependency {
            interface: InterfaceVersion::consumed(
                InterfaceName::new(*interface).expect("dependency interface is valid"),
                package_version.clone(),
            ),
            optional,
            reason: String::new(),
        };
        let bus_name = provides
            .first()
            .copied()
            .unwrap_or("ai.tinyhumans.module.Empty");
        let object_path = format!("/{}", bus_name.replace('.', "/"));
        serde_json::to_vec(&ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: name.to_string(),
                version: package_version.clone(),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(bus_name).expect("provided interface is a bus name"),
            object_path: ObjectPath::new(object_path).expect("derived object path is valid"),
            provides: provides.iter().enumerate().map(provided).collect(),
            requires: requires
                .iter()
                .map(|interface| dependency(interface, false))
                .chain(optional.iter().map(|interface| dependency(interface, true)))
                .collect(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: lazy,
            worker_threads,
            on_panic: PanicPolicy::Detach,
        })
        .expect("module manifest is serializable")
    });
    tinybus::module::abi::TbSlice {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    }
}

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
        unsafe { (self.0.log)(self.0.host_ctx, level, message.as_ptr(), message.len()) }
    }
}

struct HostSubscriber {
    host: HostCalls,
    next_span: AtomicU64,
}

impl tracing::Subscriber for HostSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write as _;

        let metadata = event.metadata();
        let mut visitor = LogVisitor(String::new());
        event.record(&mut visitor);
        let mut line = String::new();
        let _ = write!(line, "{} {}", metadata.target(), visitor.0);
        let level = match *metadata.level() {
            tracing::Level::ERROR => 1,
            tracing::Level::WARN => 2,
            tracing::Level::INFO => 3,
            tracing::Level::DEBUG => 4,
            tracing::Level::TRACE => 5,
        };
        self.host.log(level, line.as_bytes());
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

struct LogVisitor(String);

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;

        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(self.0, "{}={value:?}", field.name());
    }
}

struct ModuleTransport {
    host: HostCalls,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
    detach_on_panic: bool,
}

#[async_trait]
impl Transport for ModuleTransport {
    async fn send(&self, message: Message) -> Result<()> {
        let panicked = message.header.error_name.as_deref()
            == Some("ai.tinyhumans.tinybus.Error.ModulePanicked");
        let bytes = serde_json::to_vec(&message)?;
        if bytes.len() > MAX_FRAME_LEN {
            return Err(Error::protocol("module frame exceeds the size cap"));
        }
        match self.host.send(&bytes) {
            TB_OK => {
                if panicked && self.detach_on_panic {
                    self.host.fault();
                }
                Ok(())
            }
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
    detach_on_panic: bool,
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

        let _ = tracing::subscriber::set_global_default(HostSubscriber {
            host,
            next_span: AtomicU64::new(1),
        });

        let panic_host = host;
        let panic_location = std::sync::Arc::new(StdMutex::new(None::<String>));
        let hook_location = panic_location.clone();
        std::panic::set_hook(Box::new(move |panic| {
            let location = panic.location().map_or_else(
                || "module panicked at an unknown location".to_string(),
                |location| {
                    let file = std::path::Path::new(location.file())
                        .file_name()
                        .and_then(|file| file.to_str())
                        .unwrap_or("module");
                    format!(
                        "module panicked at {}:{}:{}",
                        file,
                        location.line(),
                        location.column()
                    )
                },
            );
            // The payload is intentionally neither formatted nor forwarded:
            // it may contain arguments, credentials, or recovery material.
            *hook_location.lock().expect("panic location lock") = Some(location.clone());
            panic_host.log(1, location.as_bytes());
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
            detach_on_panic,
        });

        runtime.spawn(async move {
            let outcome = match Connection::connect(transport).await {
                Ok(connection) => {
                    connection.__set_panic_handler(std::sync::Arc::new(move || {
                        let location = panic_location
                            .lock()
                            .expect("panic location lock")
                            .take()
                            .unwrap_or_else(|| "an unknown location".to_string());
                        Error::MethodFailed {
                            name: "ai.tinyhumans.tinybus.Error.ModulePanicked".to_string(),
                            message: format!("a module method panicked at {location}"),
                        }
                    }));
                    setup(connection).await
                }
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

/// Initialize a module whose setup function accepts typed JSON configuration.
#[doc(hidden)]
pub unsafe fn start_module_with_config<C, F, Fut>(
    host: *const TbHostVtable,
    out: *mut TbModuleVtable,
    worker_threads: usize,
    detach_on_panic: bool,
    setup: F,
) -> i32
where
    C: serde::de::DeserializeOwned + Send + 'static,
    F: FnOnce(Connection, C) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let parsed = catch_unwind(AssertUnwindSafe(|| {
        if host.is_null() {
            return Err(TB_BAD_ARGUMENT);
        }
        let host_ref = unsafe { &*host };
        if host_ref.size < size_of::<TbHostVtable>() as u32
            || host_ref.config.len > 1024 * 1024
            || (host_ref.config.ptr.is_null() && host_ref.config.len != 0)
        {
            return Err(TB_BAD_ARGUMENT);
        }
        let bytes = if host_ref.config.len == 0 {
            b"{}".as_slice()
        } else {
            unsafe { std::slice::from_raw_parts(host_ref.config.ptr, host_ref.config.len) }
        };
        serde_json::from_slice::<C>(bytes).map_err(|_| TB_BAD_ARGUMENT)
    }));
    let config = match parsed {
        Ok(Ok(config)) => config,
        Ok(Err(code)) => return code,
        Err(_) => return TB_PANICKED,
    };
    unsafe {
        start_module(
            host,
            out,
            worker_threads,
            detach_on_panic,
            move |connection| setup(connection, config),
        )
    }
}

/// Export a tinybus module's descriptor, manifest and initialization entrypoint.
///
/// ```ignore
/// tinybus_module::module_export! { setup = setup, worker_threads = 1 }
/// ```
#[macro_export]
macro_rules! module_export {
    (
        setup = $setup:path,
        config = $config:ty,
        worker_threads = $threads:expr,
        provides = [$($provides:literal),* $(,)?],
        methods = [$($methods:literal),* $(,)?],
        signals = [$($signals:literal),* $(,)?],
        requires = [$($requires:literal),* $(,)?],
        optional = [$($optional:literal),* $(,)?],
        lazy = $lazy:expr $(,)?
    ) => {
        #[unsafe(no_mangle)]
        pub static TINYBUS_MODULE_ABI_V1: ::tinybus::module::abi::TbAbiDescriptor =
            ::tinybus::module::abi::TbAbiDescriptor::current(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            );

        #[unsafe(no_mangle)]
        pub extern "C" fn tinybus_module_manifest_v1() -> ::tinybus::module::abi::TbSlice {
            $crate::manifest_slice(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
                &[$($provides),*],
                &[$($methods),*],
                &[$($signals),*],
                &[$($requires),*],
                &[$($optional),*],
                $lazy,
                $threads as u32,
            )
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn tinybus_module_init_v1(
            host: *const ::tinybus::module::abi::TbHostVtable,
            out: *mut ::tinybus::module::abi::TbModuleVtable,
        ) -> i32 {
            unsafe {
                $crate::start_module_with_config::<$config, _, _>(
                    host,
                    out,
                    $threads,
                    true,
                    $setup,
                )
            }
        }
    };
    (setup = $setup:path, worker_threads = $threads:expr $(,)?) => {
        $crate::module_export! {
            setup = $setup,
            worker_threads = $threads,
            provides = [],
            methods = [],
            signals = [],
            requires = [],
            optional = [],
            lazy = false,
        }
    };
    (
        setup = $setup:path,
        worker_threads = $threads:expr,
        provides = [$($provides:literal),* $(,)?],
        methods = [$($methods:literal),* $(,)?],
        signals = [$($signals:literal),* $(,)?],
        requires = [$($requires:literal),* $(,)?],
        optional = [$($optional:literal),* $(,)?],
        lazy = $lazy:expr $(,)?
    ) => {
        #[unsafe(no_mangle)]
        pub static TINYBUS_MODULE_ABI_V1: ::tinybus::module::abi::TbAbiDescriptor =
            ::tinybus::module::abi::TbAbiDescriptor::current(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            );

        #[unsafe(no_mangle)]
        pub extern "C" fn tinybus_module_manifest_v1() -> ::tinybus::module::abi::TbSlice {
            $crate::manifest_slice(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
                &[$($provides),*],
                &[$($methods),*],
                &[$($signals),*],
                &[$($requires),*],
                &[$($optional),*],
                $lazy,
                $threads as u32,
            )
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn tinybus_module_init_v1(
            host: *const ::tinybus::module::abi::TbHostVtable,
            out: *mut ::tinybus::module::abi::TbModuleVtable,
        ) -> i32 {
            unsafe { $crate::start_module(host, out, $threads, true, $setup) }
        }
    };
}
