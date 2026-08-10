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
    TB_BACKPRESSURE, TB_BAD_ARGUMENT, TB_CLOSED, TB_OK, TB_PANICKED, TbHostVtable, TbModuleVtable,
};
use tinybus::{Connection, Error, Result, Transport};
use tokio::sync::{Mutex, mpsc};

const MODULE_QUEUE_CAPACITY: usize = 256;
const MODULE_PANIC_ERROR: &str = "ai.tinyhumans.tinybus.Error.ModulePanicked";
static MANIFEST_BYTES: OnceLock<Vec<u8>> = OnceLock::new();

/// Build and retain the exported manifest bytes for the process lifetime.
#[doc(hidden)]
pub struct ManifestDeclaration<'a> {
    pub name: &'a str,
    pub version: &'a str,
    pub provides: &'a [&'a str],
    pub methods: &'a [&'a str],
    pub signals: &'a [&'a str],
    pub requires: &'a [&'a str],
    pub optional: &'a [&'a str],
    pub lazy: bool,
    pub worker_threads: u32,
}

/// Build and retain the exported manifest bytes for the process lifetime.
#[doc(hidden)]
pub fn manifest_slice(declaration: ManifestDeclaration<'_>) -> tinybus::module::abi::TbSlice {
    catch_unwind(AssertUnwindSafe(|| build_manifest_slice(declaration))).unwrap_or(
        tinybus::module::abi::TbSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
    )
}

fn build_manifest_slice(declaration: ManifestDeclaration<'_>) -> tinybus::module::abi::TbSlice {
    use tinybus::module::manifest::{
        Dependency, MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy, ProvidedInterface,
    };
    use tinybus::{BusName, InterfaceName, InterfaceVersion, ObjectPath, Version};

    let bytes = MANIFEST_BYTES.get_or_init(|| {
        let package_version =
            Version::parse(declaration.version).expect("package version is semver");
        let provided = |(index, interface): (usize, &&str)| ProvidedInterface {
            version: InterfaceVersion::provided(
                InterfaceName::new(*interface).expect("provided interface is valid"),
                package_version.clone(),
            ),
            methods: if index == 0 {
                declaration
                    .methods
                    .iter()
                    .map(|member| tinybus::MemberName::new(*member).expect("method is valid"))
                    .collect()
            } else {
                Vec::new()
            },
            signals: if index == 0 {
                declaration
                    .signals
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
        let bus_name = declaration
            .provides
            .first()
            .copied()
            .unwrap_or("ai.tinyhumans.module.Empty");
        let object_path = format!("/{}", bus_name.replace('.', "/"));
        serde_json::to_vec(&ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: declaration.name.to_string(),
                version: package_version.clone(),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(bus_name).expect("provided interface is a bus name"),
            object_path: ObjectPath::new(object_path).expect("derived object path is valid"),
            provides: declaration
                .provides
                .iter()
                .enumerate()
                .map(provided)
                .collect(),
            requires: declaration
                .requires
                .iter()
                .map(|interface| dependency(interface, false))
                .chain(
                    declaration
                        .optional
                        .iter()
                        .map(|interface| dependency(interface, true)),
                )
                .collect(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: declaration.lazy,
            worker_threads: declaration.worker_threads,
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

    fn ready(&self) {
        unsafe { (self.0.ready)(self.0.host_ctx) }
    }
}

struct HostSubscriber {
    host: HostCalls,
    next_span: AtomicU64,
    max_level: tracing::level_filters::LevelFilter,
}

impl tracing::Subscriber for HostSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        self.max_level >= *metadata.level()
    }

    fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
        Some(self.max_level)
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
        let panicked = message.header.error_name.as_deref() == Some(MODULE_PANIC_ERROR);
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
        Err(_) => TB_PANICKED,
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
        // `size` is the frozen prefix field; do not copy the full vtable until
        // the host has proved that all v1 fields are present.
        if unsafe { host.cast::<u32>().read() } < size_of::<TbHostVtable>() as u32 {
            return TB_BAD_ARGUMENT;
        }
        let host = HostCalls(unsafe { *host });

        // A cdylib carries its own statically linked `tracing` and `std` state;
        // this registration is global to the module's copy, not the embedding
        // host's. Failure therefore means this module runtime was initialized
        // more than once and cannot safely replace the existing subscriber.
        if tracing::subscriber::set_global_default(HostSubscriber {
            host,
            next_span: AtomicU64::new(1),
            max_level: tracing::level_filters::LevelFilter::TRACE,
        })
        .is_err()
        {
            return TB_CLOSED;
        }

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
                            name: MODULE_PANIC_ERROR.to_string(),
                            message: format!("a module method panicked at {location}"),
                        }
                    }));
                    match setup(connection.clone()).await {
                        Ok(()) => {
                            host.ready();
                            // Keep the connection (and therefore the served
                            // object tree and transport) alive until shutdown
                            // stops this runtime. Setup returning means ready,
                            // not that the module has finished serving.
                            std::future::pending::<()>().await;
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
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
        if unsafe { host.cast::<u32>().read() } < size_of::<TbHostVtable>() as u32 {
            return Err(TB_BAD_ARGUMENT);
        }
        let host_ref = unsafe { &*host };
        if host_ref.config.len > 1024 * 1024
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
    (@common
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
            $crate::manifest_slice($crate::ManifestDeclaration {
                name: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
                provides: &[$($provides),*],
                methods: &[$($methods),*],
                signals: &[$($signals),*],
                requires: &[$($requires),*],
                optional: &[$($optional),*],
                lazy: $lazy,
                worker_threads: $threads as u32,
            })
        }
    };
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
        $crate::module_export! {
            @common
            worker_threads = $threads,
            provides = [$($provides),*],
            methods = [$($methods),*],
            signals = [$($signals),*],
            requires = [$($requires),*],
            optional = [$($optional),*],
            lazy = $lazy,
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
        $crate::module_export! {
            @common
            worker_threads = $threads,
            provides = [$($provides),*],
            methods = [$($methods),*],
            signals = [$($signals),*],
            requires = [$($requires),*],
            optional = [$($optional),*],
            lazy = $lazy,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
    use std::sync::{OnceLock, mpsc::SyncSender};

    static HOST_SEND_CODE: AtomicI32 = AtomicI32::new(TB_OK);
    static HOST_WAKES: AtomicUsize = AtomicUsize::new(0);
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

    unsafe extern "C" fn host_log(_: *mut c_void, _: u32, _: *const u8, _: usize) {}

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
}
