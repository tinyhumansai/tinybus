//! The module runtime: a broker, a connection and a loader on a dedicated thread.
//!
//! # Why a runtime of its own
//!
//! A host's event bus usually constructs its broker internally and never hands
//! it out, while [`ModuleHost::new`] needs a [`Broker`] to attach loaded modules
//! to. Modules therefore run on a second in-process broker. A module on it can
//! serve requests but cannot publish into the host's own event flow.
//!
//! # What loading a module means
//!
//! `dlopen` runs code before anything can inspect it. The ABI descriptor,
//! manifest and digest gates decide whether a module is *admitted*, not whether
//! it is *safe*: once loaded it shares the process's address space, and a native
//! fault in it takes the host down. A library is never unloaded, so a module that
//! fails is failed until restart; [`super::Resolver`] caches failures for that
//! reason instead of retrying.
//!
//! The broker lives on a dedicated, process-lifetime Tokio runtime. A module
//! therefore stays usable across short-lived caller runtimes (notably
//! `#[tokio::test]`), and an embedder cannot tie its lifetime to an independently
//! managed application runtime.
//!
//! Admission is the default permissive mode. Strict mode additionally refuses a
//! module whose `rustc` version string differs from the host's, which refuses
//! real CI-built artifacts outright. The ABI revision, descriptor layout, target
//! triple, pointer width, endianness, feature bits and the refusal of a
//! `panic=abort` module are all still enforced.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::broker::Broker;
use crate::connection::ObjectRegistration;
use crate::module::ModuleHost;
use crate::transport::memory::{MemoryBus, MemoryTransport};
use crate::{Connection, Interface, Proxy};

/// A per-attachment setup callback for a statically linked module.
///
/// Unlike the module's export-time setup function, this value belongs to one
/// admission only and can capture host state without process-global slots.
pub type LinkedSetup =
    Box<dyn FnOnce(Connection) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send>> + Send>;

/// Rust-only entrypoint emitted by `tinybus_module::module_export_static!`.
///
/// Dynamic modules continue to use the stable C ABI [`crate::module::abi::TbModuleInit`].
/// This entrypoint is only for linked exports and replaces that export's
/// default setup callback for one initialization.
#[doc(hidden)]
pub type LinkedModuleInitWithSetup = unsafe fn(
    *const crate::module::abi::TbHostVtable,
    *mut crate::module::abi::TbModuleVtable,
    LinkedSetup,
) -> i32;

/// Work run once on the new runtime, before the first module can load.
///
/// A host uses it to install the callback services its modules call back into,
/// so they are being served before any module resolves.
pub type RuntimeSetup = Box<
    dyn for<'a> FnOnce(
            Connection,
            &'a ModuleHost,
        ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>>
        + Send,
>;

/// Thread names for a [`ModuleRuntime`], so a host's threads are recognisable.
#[derive(Debug, Clone)]
pub struct RuntimeNames {
    /// Name of the thread that owns the Tokio runtime.
    pub thread: String,
    /// Name of the runtime's worker threads.
    pub worker: String,
}

impl Default for RuntimeNames {
    fn default() -> Self {
        Self {
            thread: "tinybus-module-bus".to_string(),
            worker: "tinybus-module-worker".to_string(),
        }
    }
}

/// Options for the module runtime's dedicated Tokio worker threads.
#[derive(Debug, Clone, Default)]
pub struct RuntimeOptions {
    /// Override the stack size of each Tokio worker thread.
    ///
    /// `None` preserves Tokio's default stack size.
    pub thread_stack_size: Option<usize>,
}

/// The broker, the host's own connection to it, and the module loader.
pub struct ModuleRuntime {
    /// The loader. Owns every admitted module for the process lifetime.
    host: ModuleHost,
    /// This process's client connection, used to call into loaded modules.
    connection: Connection,
    /// Handle for work that must outlive the caller's runtime.
    handle: tokio::runtime::Handle,
    host_objects: std::sync::Arc<tokio::sync::Mutex<HostObjectRegistry>>,
}

pub(super) struct HostObjectRegistry {
    broker: Broker,
    connection: Connection,
    names: std::collections::HashMap<crate::BusName, HostNameLease>,
    #[cfg(test)]
    before_name_request: Option<HostObjectRegistrationBarrier>,
    #[cfg(test)]
    after_name_claim: Option<HostObjectRegistrationBarrier>,
}

#[cfg(test)]
#[derive(Clone)]
struct HostObjectRegistrationBarrier {
    entered: std::sync::Arc<tokio::sync::Semaphore>,
    release: std::sync::Arc<tokio::sync::Semaphore>,
}

struct HostNameLease {
    registrations: usize,
    release_when_empty: bool,
}

/// Owns one host object export and its share of a bus-name claim.
///
/// Dropping the guard schedules cleanup on its [`ModuleRuntime`]'s dedicated
/// runtime, so it is safe to drop from synchronous code or another runtime.
pub struct HostObjectGuard {
    registry: std::sync::Arc<tokio::sync::Mutex<HostObjectRegistry>>,
    handle: tokio::runtime::Handle,
    name: crate::BusName,
    registration: Option<ObjectRegistration>,
}

impl Drop for HostObjectGuard {
    fn drop(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        let registry = self.registry.clone();
        let name = self.name.clone();
        let handle = self.handle.clone();
        self.handle.spawn(async move {
            remove_host_object(registry, name, registration, handle).await;
        });
    }
}

impl HostObjectGuard {
    /// Withdraw this exact registration and wait for the local broker to remove it.
    pub(super) async fn remove(mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        remove_host_object(
            self.registry.clone(),
            self.name.clone(),
            registration,
            self.handle.clone(),
        )
        .await;
    }
}

async fn remove_host_object(
    registry: std::sync::Arc<tokio::sync::Mutex<HostObjectRegistry>>,
    name: crate::BusName,
    registration: ObjectRegistration,
    handle: tokio::runtime::Handle,
) {
    let mut state = registry.lock().await;
    let detached = state.connection.take_if_owned(&registration).await;
    let release = if let Some(lease) = state.names.get_mut(&name) {
        lease.registrations = lease.registrations.saturating_sub(1);
        if lease.registrations == 0 {
            let release = lease.release_when_empty;
            state.names.remove(&name);
            release
        } else {
            false
        }
    } else {
        false
    };
    if release {
        if let Some(unique) = state.connection.unique_name() {
            let _ = state.broker.release_host_name(&unique, &name);
        }
    }
    drop(state);
    if let Some(interface) = detached {
        // Interface destructors are user code. Keep this parent cleanup task
        // alive to publish completion even if the destructor unwinds.
        let _ = handle.spawn(async move { drop(interface) }).await;
    }
}

impl ModuleRuntime {
    /// Start a runtime on its own thread and wait until it is ready.
    ///
    /// The thread, the broker and the Tokio runtime live for the process: there
    /// is no shutdown path because there is nothing a shutdown could reclaim.
    /// Call it once and keep the result in a `static`.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread or Tokio runtime cannot be built, the
    /// in-memory transport cannot be connected, or `setup` fails.
    pub fn start(names: &RuntimeNames, setup: Option<RuntimeSetup>) -> crate::Result<Self> {
        Self::start_with_options(names, setup, RuntimeOptions::default())
    }

    /// Start a runtime with explicit worker-thread options.
    ///
    /// The process-lifetime behavior and setup ordering match [`Self::start`].
    /// Options only override values supplied as `Some`.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread or Tokio runtime cannot be built, the
    /// in-memory transport cannot be connected, or `setup` fails.
    pub fn start_with_options(
        names: &RuntimeNames,
        setup: Option<RuntimeSetup>,
        options: RuntimeOptions,
    ) -> crate::Result<Self> {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let worker = names.worker.clone();
        std::thread::Builder::new()
            .name(names.thread.clone())
            .spawn(move || {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder.enable_all().thread_name(worker);
                if let Some(stack_size) = options.thread_stack_size {
                    builder.thread_stack_size(stack_size);
                }
                let tokio_runtime = match builder.build() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(crate::Error::Transport(error.to_string())));
                        return;
                    }
                };
                match tokio_runtime.block_on(build(setup)) {
                    Ok(runtime) => {
                        let _ = ready_tx.send(Ok(runtime));
                        tokio_runtime.block_on(std::future::pending::<()>());
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .map_err(|error| crate::Error::Transport(error.to_string()))?;
        ready_rx
            .recv()
            .map_err(|error| crate::Error::Transport(error.to_string()))?
    }

    /// The loader.
    #[must_use]
    pub fn host(&self) -> &ModuleHost {
        &self.host
    }

    /// The connection modules are called over.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Spawn `future` with process lifetime.
    ///
    /// For work that other callers wait on, a module resolution, and that must
    /// therefore outlive whichever caller runtime started it.
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Run module admission work on this runtime's blocking pool.
    ///
    /// # Errors
    ///
    /// The work's own error, or a message if the loader did not finish.
    pub async fn blocking<F>(&self, work: F) -> Result<(), String>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        self.handle
            .spawn_blocking(work)
            .await
            .map_err(|error| format!("the module loader did not finish: {error}"))?
    }

    /// A proxy for one object on a loaded module.
    ///
    /// # Errors
    ///
    /// Returns an error if `bus_name` or `object_path` is not well formed, which
    /// for a registry entry means the table is wrong rather than the module.
    pub fn proxy(&self, bus_name: &str, object_path: &str) -> crate::Result<Proxy> {
        self.connection.proxy(bus_name, object_path, bus_name)
    }

    /// Serve one ordinary host interface on the module runtime's bus.
    ///
    /// Same-name registrations share one name claim. Each guard owns only its
    /// exact interface registration, so dropping an older guard cannot remove
    /// a later replacement made through this helper or [`Connection::serve_at`].
    /// A name already owned by this connection is preserved after the final
    /// guard is dropped; a name claimed by another peer is refused.
    ///
    /// This is an ordinary host object registration. It does not attest this
    /// peer or grant the interface confidential-call authority.
    pub async fn serve_host_object(
        &self,
        name: crate::BusName,
        path: crate::ObjectPath,
        interface: impl Interface,
    ) -> crate::Result<HostObjectGuard> {
        let registry = self.host_objects.clone();
        let handle = self.handle.clone();
        let (response, result) = tokio::sync::oneshot::channel();
        self.handle.spawn(async move {
            let guard =
                serve_host_object_transaction(registry, handle, name, path, interface).await;
            // If the caller canceled, dropping this failed send drops the guard;
            // its cleanup is still owned by the dedicated runtime.
            let _ = response.send(guard);
        });
        result.await.map_err(|_| crate::Error::ConnectionClosed)?
    }

    pub(super) fn lease_host_object_registry(
        &self,
    ) -> std::sync::Arc<tokio::sync::Mutex<HostObjectRegistry>> {
        self.host_objects.clone()
    }
}

pub(super) async fn serve_host_object_transaction(
    registry: std::sync::Arc<tokio::sync::Mutex<HostObjectRegistry>>,
    handle: tokio::runtime::Handle,
    name: crate::BusName,
    path: crate::ObjectPath,
    interface: impl Interface,
) -> crate::Result<HostObjectGuard> {
    let mut state = registry.lock().await;
    let own_unique = state.connection.unique_name();
    let unique = own_unique.as_ref().ok_or(crate::Error::ConnectionClosed)?;
    // Preserve the public connection's fail-closed behavior for an already
    // closed runtime connection. The following atomic local claim, rather
    // than this observation, determines whether this helper acquired the name.
    state.connection.name_owner(name.as_str()).await?;
    #[cfg(test)]
    if let Some(barrier) = state.before_name_request.clone() {
        barrier.entered.add_permits(1);
        let _permit = barrier
            .release
            .acquire()
            .await
            .expect("name-request barrier");
    }
    let newly_claimed = state
        .broker
        .claim_host_name(unique, name.clone())
        .map_err(|_| crate::Error::failed("host object bus name is already owned"))?;
    let release_when_empty = newly_claimed
        || state
            .names
            .get(&name)
            .is_some_and(|lease| lease.release_when_empty);
    #[cfg(test)]
    if let Some(barrier) = state.after_name_claim.clone() {
        barrier.entered.add_permits(1);
        let _permit = barrier
            .release
            .acquire()
            .await
            .expect("registration barrier");
    }
    // Run the user-provided interface registration in an owned child task so
    // a panic in `Interface::name` becomes a registration error here. The
    // transaction still owns the name claim and can roll it back below.
    let registration_connection = state.connection.clone();
    let registration_task = handle.spawn(async move {
        registration_connection
            .serve_at_owned(path, interface)
            .await
    });
    let registration = match registration_task.await {
        Ok(registration) => registration,
        Err(_) => Err(crate::Error::failed(
            "host object interface registration task failed",
        )),
    };
    let registration = match registration {
        Ok(registration) => registration,
        Err(error) => {
            if release_when_empty {
                if let Some(lease) = state.names.get_mut(&name) {
                    lease.release_when_empty = true;
                } else {
                    if let Some(unique) = state.connection.unique_name() {
                        let _ = state.broker.release_host_name(&unique, &name);
                    }
                }
            }
            return Err(error);
        }
    };
    let lease = state.names.entry(name.clone()).or_insert(HostNameLease {
        registrations: 0,
        release_when_empty,
    });
    lease.release_when_empty |= release_when_empty;
    lease.registrations += 1;
    Ok(HostObjectGuard {
        registry: registry.clone(),
        handle,
        name,
        registration: Some(registration),
    })
}

async fn build(setup: Option<RuntimeSetup>) -> crate::Result<ModuleRuntime> {
    let transport = MemoryBus::new();
    let broker = Broker::new_restricted_module_control();
    // The broker task is deliberately not retained: it lives as long as the
    // process, and an abort handle must never be called, because a module whose
    // transport disappears faults and cannot be reloaded without a restart.
    broker.spawn(transport.clone());

    // This runtime is the private host for the modules it admits. Select its
    // own controller before constructing the loader so context-bearing module
    // setup receives an attachment-scoped trusted-host capability.
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    broker.attach_module_controller(Arc::new(broker_transport))?;
    let connection = Connection::connect(Box::new(controller_transport)).await?;

    let host = ModuleHost::new(broker.clone());
    let host_objects = std::sync::Arc::new(tokio::sync::Mutex::new(HostObjectRegistry {
        broker,
        connection: connection.clone(),
        names: std::collections::HashMap::new(),
        #[cfg(test)]
        before_name_request: None,
        #[cfg(test)]
        after_name_claim: None,
    }));
    if let Some(setup) = setup {
        setup(connection.clone(), &host).await?;
    }
    Ok(ModuleRuntime {
        host,
        connection,
        handle: tokio::runtime::Handle::current(),
        host_objects,
    })
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
