//! Module admission, dependency ordering, attachment, and lifecycle.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::broker::Broker;
use crate::build_info;
use crate::error::{Error, Result, sanitize_untrusted};
use crate::module::abi::{TB_OK, TbAbiDescriptor, TbModuleInit, TbModuleVtable, field_bytes};
use crate::module::github::CachedRelease;
use crate::module::host::callbacks::HostCallbackBinding;
use crate::module::loader::{self, LoadedArtifact};
use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy};
use crate::module::transport::{ModuleTransport, StopError};
use crate::module::{LinkedModule, LinkedModuleInitWithSetup, LinkedSetup, ModuleId};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::ports::Transport;
use crate::version::Version;

const LAZY_MANIFEST_SUFFIX: &str = ".manifest.json";
const LAZY_MANIFEST_MAX_LEN: u64 = 1024 * 1024;
const MAX_PRE_READY_CONFIDENTIAL_ROUTES: usize = 64;

/// Current lifecycle state of a discovered module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum ModuleState {
    /// File passed every check that can run before the platform loader.
    Discovered,
    /// ABI or manifest admission failed. Terminal.
    Rejected {
        /// Safe refusal reason.
        reason: String,
    },
    /// Dependencies or bus-name collisions prevent initialization.
    Unresolved {
        /// Safe resolution reason.
        reason: String,
    },
    /// Gated and ordered, waiting for eager or lazy initialization.
    Resolved,
    /// Initialization is running exactly once.
    Initializing,
    /// Owns its name and is waiting for calls.
    Ready,
    /// At least one method call is in flight.
    Serving,
    /// A panic or explicit fault detached the module. Terminal.
    Faulted {
        /// Safe fault reason.
        reason: String,
    },
    /// Initialization returned failure. Terminal.
    Failed {
        /// Safe initialization reason.
        reason: String,
    },
    /// Explicitly stopped. Its library remains mapped until process exit.
    Stopped,
    /// Operator disabled the module before initialization.
    Disabled,
}

/// Safe, serializable facts about one module. No absolute artifact path leaks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleInfo {
    /// Descriptor/manifest module name after sanitization.
    pub name: String,
    /// Declared module version after sanitization.
    pub version: String,
    /// Artifact basename only.
    pub file: String,
    /// Current lifecycle state and safe detail, flattened for CLI JSON.
    #[serde(flatten)]
    pub state: ModuleState,
    /// Module dependency and surface declaration.
    pub manifest: ModuleManifest,
    /// Toolchain recorded by the descriptor, sanitized for display. Empty
    /// until a truly lazy module is first loaded.
    pub rustc_version: String,
    /// Whether this module's toolchain differs from the host. This becomes
    /// meaningful once a truly lazy module has been loaded.
    pub rustc_mismatch: bool,
    /// Whether discovery should admit this module on future scans.
    pub enabled: bool,
}

struct LoadedModule {
    info: ModuleInfo,
    module_id: ModuleId,
    transport: Arc<ModuleTransport>,
    unique_name: BusName,
    transition_from: Option<ModuleState>,
    descriptor_info: Option<DescriptorInfo>,
    lifecycle_busy: bool,
    callback_config: Option<[u8; 32]>,
    // Keep operation accounting after routing revocation removes the binding.
    callback_state: Option<Arc<callbacks::CallbackState>>,
    // Keep the owning capability alive for as long as this admitted module can
    // issue callbacks. The binding holds only a weak reference back to the host.
    _callback_binding: Option<Arc<HostCallbackBinding>>,
}

/// Fingerprints only bytes produced by serde_json::to_vec, never arbitrary JSON.
/// Sorted object keys give stable serialization. Float signed zero is the sole
/// serialization difference between equal Values: normalize its numeric token
/// while hashing borrowed segments, without another plaintext allocation.
fn callback_config_fingerprint(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    let mut quoted = false;
    let mut index = 0;
    let mut segment = 0;
    while index < bytes.len() {
        if quoted {
            match bytes[index] {
                b'\\' => index += 1,
                b'"' => quoted = false,
                _ => {}
            }
        } else if bytes[index] == b'"' {
            quoted = true;
        } else if bytes[index..].starts_with(b"-0.0")
            && (index == 0 || matches!(bytes[index - 1], b'[' | b',' | b':'))
            && (index + 4 == bytes.len() || matches!(bytes[index + 4], b']' | b',' | b'}'))
        {
            hash.update(&bytes[segment..index]);
            segment = index + 1;
            index += 3;
        }
        index += 1;
    }
    hash.update(&bytes[segment..]);
    hash.finalize().into()
}

enum PendingModule {
    Loaded(Box<LoadedArtifact>),
    Lazy(Box<ModuleManifest>),
}

type DescriptorInfo = Arc<Mutex<Option<DescriptorMetadata>>>;

#[derive(Clone)]
struct DescriptorMetadata {
    rustc_version: String,
    rustc_mismatch: bool,
}

struct Activation {
    lazy_init: bool,
    lazy_load: bool,
    descriptor_info: Option<DescriptorInfo>,
    /// The digest a caller pinned for this artifact, where the artifact did not
    /// come from a directory carrying a `modules.toml`.
    ///
    /// Set by the verified release path or linked-code path. The release path
    /// records the pinned archive digest; linked code records the host
    /// executable digest. `None` for raw attachments and ordinary local files.
    pinned_sha256: Option<String>,
    pinned_library_sha256: Option<String>,
}

impl PendingModule {
    fn manifest(&self) -> &ModuleManifest {
        match self {
            Self::Loaded(artifact) => &artifact.manifest,
            Self::Lazy(manifest) => manifest,
        }
    }
}

#[derive(Clone, Copy)]
enum RefusalClass {
    Rejected,
    Unresolved,
    Failed,
}

impl LoadedModule {
    fn snapshot(&self) -> ModuleInfo {
        let mut info = self.info.clone();
        if let Some(metadata) = self
            .descriptor_info
            .as_ref()
            .and_then(|value| value.lock().expect("module descriptor lock").clone())
        {
            info.rustc_version = metadata.rustc_version;
            info.rustc_mismatch = metadata.rustc_mismatch;
        }
        if self.transport.init_failed()
            && !matches!(info.state, ModuleState::Stopped | ModuleState::Disabled)
        {
            info.state = ModuleState::Failed {
                reason: "module initialization failed".to_string(),
            };
        } else if self.transport.is_faulted()
            && !matches!(info.state, ModuleState::Stopped | ModuleState::Disabled)
        {
            info.state = ModuleState::Faulted {
                reason: "module reported an unrecoverable fault".to_string(),
            };
        } else if (self.transport.is_ready()
            && matches!(
                info.state,
                ModuleState::Resolved | ModuleState::Initializing
            ))
            || matches!(info.state, ModuleState::Ready | ModuleState::Serving)
        {
            info.state = if self.transport.inflight() == 0 {
                ModuleState::Ready
            } else {
                ModuleState::Serving
            };
        } else if self.transport.init_started() && matches!(info.state, ModuleState::Resolved) {
            info.state = ModuleState::Initializing;
        }
        info
    }
}

/// Loads trusted cdylib modules into one embedded broker.
pub struct ModuleHost {
    inner: Arc<ModuleHostInner>,
}

struct ModuleHostInner {
    broker: Broker,
    // Callback bindings identify their owning host through this separate
    // token. Keeping the back-reference out of `ModuleHostInner` avoids a
    // recursive auto-trait proof through `loaded` bindings.
    callback_owner: Arc<()>,
    strict: AtomicBool,
    // Admission spans duplicate validation through final insertion; a narrower
    // lock lets two concurrent loads both pass the name check.
    admission: Mutex<()>,
    loaded: Mutex<Vec<LoadedModule>>,
    rejected: Mutex<Vec<ModuleInfo>>,
    directories: Mutex<Vec<PathBuf>>,
    configs: Mutex<HashMap<String, serde_json::Value>>,
    // Extracted release directories stay alive because a module may need to
    // resolve sibling files for the lifetime of its mapped library.
    artifacts: Mutex<Vec<tempfile::TempDir>>,
    warned: AtomicBool,
    search_path_config: RwLock<Option<SearchPathConfig>>,
    pre_ready_confidential_routes: RwLock<Arc<[PreReadyConfidentialRoute]>>,
    #[cfg(test)]
    after_loaded_publish: Mutex<Option<LoadedPublishBarrier>>,
}

/// One exact method route that a selected module controller may call
/// confidentially while an admitted module is initializing.
///
/// The list is empty by default and can only be configured before the host
/// admits a module. This grants no authority to ordinary module clients.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PreReadyConfidentialRoute {
    /// Object path of the host method.
    pub path: ObjectPath,
    /// Interface containing the host method.
    pub interface: InterfaceName,
    /// Method member name.
    pub member: MemberName,
}

#[cfg(test)]
struct LoadedPublishBarrier {
    reached: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

/// Owned, read-only access to a host's admitted unique-peer lookup.
///
/// The handle does not keep the host alive. It can be captured by a host
/// callback that must outlive the borrow used to create it; after the host is
/// dropped, lookups fail closed. It performs no broker request and grants no
/// module or attestation authority.
#[derive(Clone)]
pub struct ModuleSenderLookup {
    inner: Weak<ModuleHostInner>,
}

impl ModuleSenderLookup {
    /// Resolve a broker-authenticated unique peer to its exact admitted module
    /// identifier, or return `None` if the host is gone or the peer is no
    /// longer admitted.
    #[must_use]
    pub fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        self.inner
            .upgrade()
            .and_then(|inner| module_for_sender(&inner, sender))
    }

    /// Resolve a canonical module ID to its current unique peer, if admitted.
    #[must_use]
    pub fn sender_for_module(&self, module_id: &str) -> Option<BusName> {
        self.inner
            .upgrade()
            .and_then(|inner| sender_for_module(&inner, module_id))
    }
}

fn module_for_sender(inner: &ModuleHostInner, sender: &BusName) -> Option<ModuleId> {
    if !sender.is_unique() {
        return None;
    }
    let loaded = inner.loaded.lock().expect("module list lock");
    let module = loaded.iter().find(|module| &module.unique_name == sender)?;
    if module.transport.is_faulted()
        || module.transport.init_failed()
        || module.transport.is_stopped()
        || module.transport.admission_closed()
    {
        return None;
    }
    matches!(
        module.snapshot().state,
        ModuleState::Resolved
            | ModuleState::Initializing
            | ModuleState::Ready
            | ModuleState::Serving
    )
    .then(|| module.module_id.clone())
}

fn sender_for_module(inner: &ModuleHostInner, module_id: &str) -> Option<BusName> {
    if module_id.is_empty() {
        return None;
    }
    let loaded = inner.loaded.lock().expect("module list lock");
    let mut candidates = loaded.iter().filter(|module| {
        module.module_id.as_str() == module_id
            && !module.transport.is_faulted()
            && !module.transport.init_failed()
            && !module.transport.is_stopped()
            && !module.transport.admission_closed()
            && matches!(
                module.snapshot().state,
                ModuleState::Initializing | ModuleState::Ready | ModuleState::Serving
            )
    });
    let sender = candidates.next()?.unique_name.clone();
    candidates.next().is_none().then_some(sender)
}

/// Product configuration for directories that `ModuleHost` scans for modules.
/// TinyBus has no product-specific default; hosts opt in by naming the
/// environment variable and application directory they own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchPathConfig {
    /// Environment variable containing path-list entries searched first.
    pub env_var: String,
    /// App-specific suffix beneath each platform data directory.
    pub app_dir: PathBuf,
}

impl SearchPathConfig {
    /// Configure module search paths for a host.
    #[must_use]
    pub fn new(env_var: impl Into<String>, app_dir: impl Into<PathBuf>) -> Self {
        Self {
            env_var: env_var.into(),
            app_dir: app_dir.into(),
        }
    }
}

/// Clears a module's host-side lifecycle reservation when its operation ends.
///
/// The reservation is separate from the transport's mutex: the latter keeps
/// opaque callbacks from overlapping, while this guard ensures cancellation of
/// a public host future cannot leave the module permanently unavailable.
struct LifecycleBusyGuard<'a> {
    host: &'a ModuleHostInner,
    name: String,
    stop_transport: Option<Arc<ModuleTransport>>,
}

/// Keeps a linked setup admission owned until the module publishes readiness.
/// Dropping or timing out the async attach request revokes the broker peer and
/// lets the existing native lifecycle path finish teardown; it never drops a
/// module-owned future from the caller's runtime.
struct LinkedSetupAdmissionGuard(Option<Arc<ModuleTransport>>);

impl LinkedSetupAdmissionGuard {
    fn new(transport: Arc<ModuleTransport>) -> Self {
        Self(Some(transport))
    }

    fn ready(&mut self) {
        self.0.take();
    }
}

impl Drop for LinkedSetupAdmissionGuard {
    fn drop(&mut self) {
        if let Some(transport) = self.0.take() {
            transport.request_terminal(true);
        }
    }
}

impl<'a> LifecycleBusyGuard<'a> {
    fn for_reinitialize(host: &'a ModuleHostInner, name: &str) -> Self {
        Self {
            host,
            name: name.to_string(),
            stop_transport: None,
        }
    }

    fn for_stop(host: &'a ModuleHostInner, name: &str, transport: Arc<ModuleTransport>) -> Self {
        Self {
            host,
            name: name.to_string(),
            stop_transport: Some(transport),
        }
    }
}

impl Drop for LifecycleBusyGuard<'_> {
    fn drop(&mut self) {
        if let Some(module) = self
            .host
            .loaded
            .lock()
            .expect("module list lock")
            .iter_mut()
            .find(|module| module.info.name == self.name)
        {
            module.lifecycle_busy = false;
            // A stop cancelled before it acquires the transport lifecycle
            // mutex has no task left to consume this transition. Clear it
            // so a later fault is not mistaken for that abandoned stop. Once
            // a task exists, its eventual peer detach owns the transition.
            if self
                .stop_transport
                .as_ref()
                .is_some_and(|transport| !transport.stop_task_spawned())
            {
                module.transition_from = None;
            }
        }
    }
}

/// The broker's private control hook. Kept behind a weak pointer so an unused
/// broker does not keep a module host alive.
#[async_trait::async_trait]
pub(crate) trait ModuleControl: Send + Sync {
    fn list(&self) -> Vec<ModuleInfo>;
    fn load(
        self: Arc<Self>,
        path: PathBuf,
        config: serde_json::Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)>;
    fn load_github(
        self: Arc<Self>,
        release_url: String,
        asset_name: String,
        sha256: String,
        config: serde_json::Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)>;
    async fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
    async fn stop_after_lifecycle(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
    async fn stop_inner(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
    async fn reinitialize(&self, name: &str, config: serde_json::Value) -> Result<ModuleInfo>;
    fn enable(&self, name: &str, enabled: bool) -> Result<(ModuleInfo, Option<ModuleTransition>)>;
    fn rescan(
        self: Arc<Self>,
        paths: Vec<PathBuf>,
        dry_run: bool,
    ) -> Result<(Vec<ModuleInfo>, Vec<ModuleTransition>)>;
    fn peer_detached(&self, unique_name: &BusName) -> Option<ModuleTransition>;
    fn unavailable_for(&self, bus_name: &BusName) -> Option<Error>;
}

pub(crate) type ModuleTransition = (String, ModuleState, ModuleState);

impl ModuleHost {
    /// Create a host. Loading is permissive about rustc drift by default.
    pub fn new(broker: Broker) -> Self {
        let inner = Arc::new(ModuleHostInner {
            broker: broker.clone(),
            callback_owner: Arc::new(()),
            strict: AtomicBool::new(false),
            admission: Mutex::new(()),
            loaded: Mutex::new(Vec::new()),
            rejected: Mutex::new(Vec::new()),
            directories: Mutex::new(Vec::new()),
            configs: Mutex::new(HashMap::new()),
            artifacts: Mutex::new(Vec::new()),
            warned: AtomicBool::new(false),
            search_path_config: RwLock::new(None),
            pre_ready_confidential_routes: RwLock::new(Arc::from([])),
            #[cfg(test)]
            after_loaded_publish: Mutex::new(None),
        });
        let control: Arc<dyn ModuleControl> = inner.clone();
        broker.set_module_control(Arc::downgrade(&control));
        Self { inner }
    }

    /// Builder form of [`Self::set_search_path_config`].
    #[must_use]
    pub fn with_search_path_config(self, config: SearchPathConfig) -> Self {
        self.set_search_path_config(Some(config));
        self
    }

    /// Set or clear this host's module search paths.
    pub fn set_search_path_config(&self, config: Option<SearchPathConfig>) {
        *self
            .inner
            .search_path_config
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = config;
    }

    /// Configure exact confidential host methods that the selected module
    /// controller may call while a module is initializing.
    ///
    /// The default is an empty list. Configuration is accepted only before
    /// any module admission or rejection, and at most 64 distinct routes may
    /// be configured. Calls outside this exact list retain normal readiness
    /// behavior. The routes do not broaden the authority of module peers.
    pub fn set_pre_ready_confidential_routes(
        &self,
        routes: Vec<PreReadyConfidentialRoute>,
    ) -> Result<()> {
        if routes.len() > MAX_PRE_READY_CONFIDENTIAL_ROUTES {
            return Err(Error::protocol("too many pre-ready confidential routes"));
        }
        let mut unique = HashSet::with_capacity(routes.len());
        if routes.iter().any(|route| !unique.insert(route)) {
            return Err(Error::protocol("duplicate pre-ready confidential route"));
        }

        let _admission = self.inner.admission.lock().expect("module admission lock");
        if !self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .is_empty()
            || !self
                .inner
                .rejected
                .lock()
                .expect("rejected module list lock")
                .is_empty()
        {
            return Err(Error::protocol(
                "pre-ready confidential routes must be set before module admission",
            ));
        }
        *self
            .inner
            .pre_ready_confidential_routes
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::from(routes);
        Ok(())
    }

    /// Create an owned weak handle for resolving authenticated module peers.
    ///
    /// The returned handle can be stored by runtime setup callbacks without a
    /// back-reference to the [`ModuleHost`] or a global runtime slot. It uses
    /// the same local admission table and terminal-state checks as
    /// [`Self::module_for_sender`].
    #[must_use]
    pub fn sender_lookup(&self) -> ModuleSenderLookup {
        ModuleSenderLookup {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(crate) fn configure_search_path_config(&self, config: Option<SearchPathConfig>) {
        self.set_search_path_config(config);
    }

    /// Refuse modules built by a different rustc release.
    #[must_use]
    pub fn strict(self, strict: bool) -> Self {
        self.inner.strict.store(strict, Ordering::Release);
        self
    }

    /// Set JSON configuration used when `load_dir` initializes this module.
    pub fn set_config(&self, module: impl Into<String>, config: serde_json::Value) {
        self.inner
            .configs
            .lock()
            .expect("module config lock")
            .insert(module.into(), config);
    }

    /// Builder form of [`ModuleHost::set_config`].
    #[must_use]
    pub fn with_config(self, module: impl Into<String>, config: serde_json::Value) -> Self {
        self.set_config(module, config);
        self
    }

    /// Apply replacement configuration to a running module.
    ///
    /// Reinitialization reuses the admitted module and its existing bus
    /// connection. The module remains available while its setup function
    /// applies the new values; a rejected configuration leaves the runtime
    /// running with whatever state its setup function had already committed.
    /// If a lazy module is still resolved and has not started initialization,
    /// the replacement becomes its initialization configuration; its first
    /// method call then starts the module with those values.
    pub async fn reinitialize(
        &self,
        name: impl AsRef<str>,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        self.inner.reinitialize(name.as_ref(), config).await
    }

    /// Snapshot all admitted modules.
    pub fn list(&self) -> Vec<ModuleInfo> {
        let mut modules = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(LoadedModule::snapshot)
            .collect::<Vec<_>>();
        modules.extend(
            self.inner
                .rejected
                .lock()
                .expect("rejected module list lock")
                .iter()
                .cloned(),
        );
        modules
    }

    /// Resolve a broker-authenticated unique peer to its exact admitted module
    /// identifier.
    ///
    /// Well-known names are routing aliases and never identify a module
    /// sender. This is a local lookup over the admission table; it performs no
    /// broker request and accepts initialization-time callbacks while the
    /// admitted transport remains live.
    #[must_use]
    pub fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        module_for_sender(&self.inner, sender)
    }

    /// Resolve a canonical module ID to its current unique peer, if admitted.
    #[must_use]
    pub fn sender_for_module(&self, module_id: &str) -> Option<BusName> {
        sender_for_module(&self.inner, module_id)
    }

    /// Initialize a loaded module and wait for its host lifecycle readiness.
    ///
    /// This host-owned preparation carries no business message. It is the
    /// readiness barrier used before a caller asks the broker to attest a
    /// lazily registered recipient.
    ///
    /// # Errors
    ///
    /// Returns a closed lifecycle error when the name is not a loaded module,
    /// initialization fails, or the module does not reach `READY`.
    pub async fn ensure_ready(&self, bus_name: &str) -> Result<()> {
        self.ensure_ready_within(bus_name, Duration::from_secs(5))
            .await
    }

    pub(crate) async fn ensure_ready_within(
        &self,
        bus_name: &str,
        deadline: Duration,
    ) -> Result<()> {
        let transport = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .find(|module| module.info.manifest.bus_name.as_str() == bus_name)
            .map(|module| Arc::clone(&module.transport))
            .ok_or_else(|| Error::ModuleUnavailable {
                module: "registered module".to_string(),
                state: "unavailable".to_string(),
                detail: "module is not loaded".to_string(),
            })?;
        transport.ensure_initialized().await?;
        if tokio::time::timeout(deadline, transport.wait_ready())
            .await
            .is_err()
        {
            return Err(Error::ModuleUnavailable {
                module: "registered module".to_string(),
                state: "initializing".to_string(),
                detail: "module did not announce READY before the deadline".to_string(),
            });
        }
        if transport.is_ready() {
            Ok(())
        } else {
            Err(Error::ModuleUnavailable {
                module: "registered module".to_string(),
                state: "unavailable".to_string(),
                detail: "module did not become ready".to_string(),
            })
        }
    }

    /// Wait until the module that owns `bus_name` has finished setup, then
    /// snapshot it.
    ///
    /// "Loaded" is not "serving": an eager module's `init` can return while
    /// its setup (opening a socket, starting a server) is still running, and
    /// that setup can fault afterwards. A caller that reports the module usable
    /// at load time reports a module that is about to lose its name. This
    /// returns once the module signalled ready, faulted, failed or was closed,
    /// or after `deadline`; a module still `Initializing` in the snapshot timed
    /// out. A lazy module that has not been asked to initialise yet is
    /// returned at once, since nothing is running whose outcome could be
    /// awaited. `None` when no loaded module owns the name.
    pub(crate) async fn settle(&self, bus_name: &str, deadline: Duration) -> Option<ModuleInfo> {
        let transport = {
            let loaded = self.inner.loaded.lock().expect("module list lock");
            let module = loaded
                .iter()
                .find(|module| module.info.manifest.bus_name.as_str() == bus_name)?;
            if matches!(module.snapshot().state, ModuleState::Resolved) {
                return Some(module.snapshot());
            }
            module.transport.clone()
        };
        let _ = tokio::time::timeout(deadline, transport.wait_ready()).await;
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .find(|module| module.info.manifest.bus_name.as_str() == bus_name)
            .map(LoadedModule::snapshot)
    }

    /// Return the broker-assigned unique peer name for a live, ready module.
    ///
    /// `module_name` is the module's sanitized descriptor name as returned by
    /// [`ModuleInfo::name`], not its well-known bus name. The returned name is
    /// the identity the broker stamps onto ordinary calls from that exact
    /// loaded instance. This lookup is not an attestation or confidential-call
    /// grant; confidential delivery continues to require the existing pinned
    /// admission and callback-binding checks.
    ///
    /// Returns `None` for unknown, disabled, initializing, stopped, faulted,
    /// or lifecycle-transitioning modules. A caller must not retain a name as
    /// authority beyond the invocation that needs it.
    #[must_use]
    pub fn authenticated_peer(&self, module_name: &str) -> Option<BusName> {
        let loaded = self.inner.loaded.lock().expect("module list lock");
        let module = loaded
            .iter()
            .find(|module| module.info.name == module_name)?;
        if !module.info.enabled
            || module.lifecycle_busy
            || module.transport.native_lifecycle_pending()
            || !module.transport.is_ready()
            || module.transport.is_faulted()
            || module.transport.init_failed()
        {
            return None;
        }
        matches!(
            module.snapshot().state,
            ModuleState::Ready | ModuleState::Serving
        )
        .then(|| module.unique_name.clone())
    }

    /// Load one newly installed module.
    ///
    /// A verified operator allowlist admits confidential delivery to the exact
    /// manifest name and this module's fixed unique peer. It does not attest
    /// extra aliases or grant caller authority. See [`crate::attest`].
    pub fn load_file(&self, path: impl AsRef<Path>) -> Result<ModuleInfo> {
        self.load_file_with_config(path, serde_json::json!({}))
    }

    /// Download and load one verified GitHub release asset.
    ///
    /// `release_url` must be a tag URL such as
    /// `https://github.com/tinyhumansai/rust-template/releases/tag/v0.1.2`.
    /// The release must publish a `checksum.toml` or `checksum.json` asset
    /// containing the SHA-256 for `asset_name`. When supplied, the host's
    /// expected digest must agree with the release manifest as well.
    ///
    /// Supplying `expected_sha256` is what makes the module an **attested
    /// recipient**, eligible to be sent a confidential message. A host that
    /// compiles a digest in has made the same statement an operator makes by
    /// writing one into `modules.toml`, and it is a stronger one: the value
    /// cannot be edited on the machine running it. Omitting the argument
    /// leaves the release's own checksum manifest as the only claim about
    /// these bytes — which is the publisher vouching for itself, not an
    /// operator vouching for the publisher — so the module loads and is
    /// refused secrets.
    pub fn load_github_release(
        &self,
        release_url: impl AsRef<str>,
        asset_name: impl AsRef<str>,
        expected_sha256: Option<&str>,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let (directory, module) = crate::module::github::acquire(
            release_url.as_ref(),
            asset_name.as_ref(),
            expected_sha256,
        )?;
        let info = self.load_file_pinned(
            &module,
            config,
            expected_sha256.map(str::to_ascii_lowercase),
            None,
        )?;
        self.inner
            .artifacts
            .lock()
            .expect("module artifact lock")
            .push(directory);
        Ok(info)
    }

    /// [`ModuleHost::load_github_release`] through a persistent, verified cache.
    ///
    /// The first load of a release downloads it into `release.cache_dir` —
    /// the archive, its extraction, and the digest the release manifest
    /// published — after the same two-sided check as the uncached path. Every
    /// later load re-hashes the archive on disk against the host's pin and
    /// maps the library without touching the network, which is what makes a
    /// module cheap to load at every launch rather than once per process.
    ///
    /// Attestation preserves the archive digest and carries the separately
    /// verified extracted-library digest when the release pins one.
    ///
    /// Nothing is retained here. The directory outlives the process by design,
    /// so the sibling files a mapped library may resolve stay where they are.
    ///
    /// # Errors
    ///
    /// Returns an error if the release cannot be acquired — see
    /// [`CachedRelease`] for when a miss is a refusal — or if the artifact is not
    /// admitted.
    pub fn load_github_release_cached(
        &self,
        release: &CachedRelease<'_>,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let (module, sha256) = crate::module::github::acquire_cached(release)?;
        self.load_file_pinned(
            &module,
            config,
            Some(sha256),
            release.expected_library_sha256.map(str::to_ascii_lowercase),
        )
    }

    /// Load an installer-bundle entry whose archive was replaced by its digest
    /// marker. See [`crate::module::cache::find_marked`] for the trust model;
    /// crate-private so the only caller stays [`super::load_first_admitted`],
    /// which reaches it for the installer-shipped directory alone.
    ///
    /// The marker proves the archive pin only. It is not a library digest
    /// proof: notarization may have rewritten the extracted library after the
    /// archive was built. Attestation therefore comes only from the sealed
    /// `modules.toml` beside the library, which this path requires and
    /// activation re-hashes against the mapped file.
    ///
    /// # Errors
    ///
    /// Returns a refusal if the marker is missing, malformed or not equal to
    /// `pin`, or the extraction is unusable; otherwise any admission error.
    pub(crate) fn load_bundled_marked(
        &self,
        dir: &Path,
        asset_name: &str,
        pin: &str,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let hit = crate::module::cache::find_marked(dir, asset_name, pin).ok_or_else(|| {
            Error::module_refused(
                dir,
                "bundled digest marker does not match the pin, or the extraction is unusable",
            )
        })?;
        self.load_file_pinned(&hit.module, config, None, None)
    }

    /// Admit and initialize an already-resolved module without calling the
    /// platform loader.
    ///
    /// This is the testable seam beneath `dlopen`: callers must ensure `init`
    /// and every pointer reachable through it remain valid for the process
    /// lifetime, exactly as the real loader does by leaking its handle.
    /// No digest is supplied by this API: any recipient attestation comes from
    /// an allowlist beside `file`, not from the descriptor or callback itself.
    /// Broker attachment establishes delivery provenance separately.
    ///
    /// # Safety
    ///
    /// `init` and every function pointer or context it returns must remain
    /// valid and thread-safe until process exit.
    pub unsafe fn attach_raw(
        &self,
        file: impl AsRef<Path>,
        descriptor: TbAbiDescriptor,
        manifest: ModuleManifest,
        init: TbModuleInit,
    ) -> Result<ModuleInfo> {
        unsafe {
            self.attach_raw_with_config(file, descriptor, manifest, init, serde_json::json!({}))
        }
    }

    /// Configured form of [`ModuleHost::attach_raw`].
    ///
    /// # Safety
    ///
    /// The caller must uphold the lifetime and thread-safety contract stated
    /// on [`ModuleHost::attach_raw`].
    pub unsafe fn attach_raw_with_config(
        &self,
        file: impl AsRef<Path>,
        descriptor: TbAbiDescriptor,
        manifest: ModuleManifest,
        init: TbModuleInit,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let artifact = LoadedArtifact {
            descriptor,
            manifest,
            init,
            library_sha256: None,
        };
        // No pin: the caller handed over an already-resolved artifact rather
        // than bytes this host read and hashed, so there is nothing to vouch
        // for. Attestation, if any, comes from an allowlist beside the file.
        self.activate(file.as_ref(), artifact, config, None, None)
    }

    /// Admit a module whose code is linked into the host executable.
    ///
    /// The executable's digest identifies the admitted code in the ordinary
    /// attestation response. This is host trust, not a digest of a separate
    /// library or a claim made by the module itself. The record applies to the
    /// manifest name and fixed unique peer, not additional aliases; recipient
    /// admission alone does not grant incoming callers permission.
    ///
    /// # Errors
    /// Returns an error if the host executable cannot be hashed or the module
    /// fails the normal descriptor, manifest, dependency, or init checks.
    pub fn attach_linked_with_config(
        &self,
        module: LinkedModule,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        self.attach_linked(module, config, None)
    }

    /// Admit linked code and replace its export-time setup with state owned by
    /// this one attachment.
    ///
    /// The generated static export supplies a Rust-only initializer hook; the
    /// hook receives this callback in place of its default setup function. Its
    /// module connection remains the admitted, broker-authenticated peer, and
    /// the ordinary descriptor, dependency, executable-digest and readiness
    /// paths still apply. Dynamic modules cannot use this API.
    ///
    /// # Errors
    /// Returns an error if the export has no linked setup hook, normal admission
    /// fails, setup fails, or the module does not become ready before its
    /// initialization deadline.
    pub async fn attach_linked_with_setup<F, Fut>(
        &self,
        module: LinkedModule,
        config: serde_json::Value,
        setup: F,
    ) -> Result<ModuleInfo>
    where
        F: FnOnce(crate::Connection) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let init = module
            .init_with_setup
            .ok_or_else(|| Error::failed("linked export has no stateful setup entrypoint"))?;
        let setup: LinkedSetup = Box::new(move |connection| Box::pin(setup(connection)));
        let name = sanitize_untrusted(&module.manifest.module.name);
        let info = self.attach_linked(module, config, Some((init, setup)))?;
        let transport = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .find(|loaded| loaded.info.name == info.name)
            .map(|loaded| loaded.transport.clone())
            .ok_or_else(|| Error::failed("linked module did not retain its admitted transport"))?;
        let mut setup_admission = LinkedSetupAdmissionGuard::new(transport.clone());
        transport.ensure_initialized().await?;
        if tokio::time::timeout(Duration::from_secs(5), transport.wait_ready())
            .await
            .is_err()
            || !transport.is_ready()
        {
            return Err(Error::module_refused(
                Path::new(&format!("linked-{name}")),
                "linked module setup did not become ready",
            ));
        }
        setup_admission.ready();
        Ok(self
            .list()
            .into_iter()
            .find(|module| module.name == info.name)
            .unwrap_or(info))
    }

    fn attach_linked(
        &self,
        module: LinkedModule,
        config: serde_json::Value,
        linked_setup: Option<(LinkedModuleInitWithSetup, LinkedSetup)>,
    ) -> Result<ModuleInfo> {
        static EXECUTABLE_DIGEST: OnceLock<String> = OnceLock::new();
        let digest = match EXECUTABLE_DIGEST.get() {
            Some(digest) => digest.clone(),
            None => {
                let executable = std::env::current_exe()
                    .map_err(|_| Error::failed("linked module host executable is unavailable"))?;
                let digest = crate::module::sha256_file(executable)
                    .map_err(|_| Error::failed("linked module host executable cannot be hashed"))?;
                let _ = EXECUTABLE_DIGEST.set(digest.clone());
                digest
            }
        };
        let name = sanitize_untrusted(&module.manifest.module.name);
        let path = PathBuf::from(format!("linked-{name}"));
        self.ensure_dependencies(&module.manifest, &path)?;
        self.activate(
            &path,
            LoadedArtifact {
                descriptor: module.descriptor,
                manifest: module.manifest,
                init: module.init,
                library_sha256: None,
            },
            config,
            Some(digest),
            linked_setup,
        )
    }

    /// Load one module and pass JSON configuration to its setup function.
    ///
    /// The bytes are copied by the module during initialization and are never
    /// retained as a host allocation across the ABI boundary.
    pub fn load_file_with_config(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        self.load_file_pinned(path, config, None, None)
    }

    /// Load an artifact only if its actual manifest satisfies host requirements.
    ///
    /// The actual loaded artifact is retained through activation. Lazy sidecars
    /// are refused; an embedded lazy-init declaration remains supported and is
    /// checked before its initializer is registered. This does not prevent
    /// platform library constructors from running while loading trusted code.
    ///
    /// # Errors
    ///
    /// Returns a native admission error for unsafe files, provisional sidecars,
    /// incompatible requirements, missing dependencies, or initialization failure.
    /// No configuration callback runs on a requirements mismatch.
    pub fn load_file_with_requirements(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
        requirements: &super::ModuleRequirements,
    ) -> Result<ModuleInfo> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        check_directory(parent)?;
        check_file(path)?;
        // Sidecars are provisional declarations: this admission path needs the
        // manifest and init pointer from the same actual loaded library.
        if read_lazy_manifest(path)?.is_some() {
            return Err(Error::module_refused(
                path,
                "required activation refuses lazy sidecars",
            ));
        }
        let allowlisted = allowlisted_hash(
            path,
            std::fs::File::open(path)
                .map_err(|_| Error::module_refused(path, "module library could not be opened"))?,
        )?;
        let artifact = loader::load_verified(
            path,
            self.inner.strict.load(Ordering::Acquire),
            allowlisted.as_deref(),
        )?;
        self.activate_required(path, artifact, config, requirements)
    }

    fn activate_required(
        &self,
        path: &Path,
        artifact: LoadedArtifact,
        config: serde_json::Value,
        requirements: &super::ModuleRequirements,
    ) -> Result<ModuleInfo> {
        requirements.check(&artifact.manifest).map_err(|_| {
            Error::module_refused(path, "artifact does not satisfy required module surface")
        })?;
        self.activate(path, artifact, config, None, None)
    }

    /// [`ModuleHost::load_file_with_config`], carrying a digest the caller has
    /// already verified for an artifact that has no `modules.toml` beside it.
    ///
    /// `pinned_sha256` is recorded as the archive attestation without being re-checked
    /// here, because there is nothing left on disk to re-check it against — the
    /// bytes it names are the archive, which was verified and then extracted.
    /// The verification therefore lives entirely in the caller, and this stays
    /// private for that reason: exposing it would let a caller declare an
    /// artifact attested without anyone having hashed anything. The only
    /// callers are [`ModuleHost::load_github_release`] and
    /// [`ModuleHost::load_github_release_cached`], both of which hash the
    /// archive against the pin first; keep it that way, or move the check down
    /// here first. ([`ModuleHost::load_bundled_marked`] calls it with `None`.)
    fn load_file_pinned(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
        pinned_sha256: Option<String>,
        pinned_library_sha256: Option<String>,
    ) -> Result<ModuleInfo> {
        let path = path.as_ref();
        let result = (|| {
            if let Some(parent) = path.parent() {
                check_directory(if parent.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    parent
                })?;
            }
            check_file(path)?;
            let allowlisted = allowlisted_hash(
                path,
                std::fs::File::open(path).map_err(|_| {
                    Error::module_refused(path, "module library could not be opened")
                })?,
            )?;
            if pinned_library_sha256
                .as_ref()
                .zip(allowlisted.as_ref())
                .is_some_and(|(pinned, local)| !pinned.eq_ignore_ascii_case(local))
            {
                return Err(Error::module_refused(
                    path,
                    "release library pin disagrees with the local module allowlist",
                ));
            }
            let library_pin = pinned_library_sha256.clone().or(allowlisted);
            if let Some(manifest) = read_lazy_manifest(path)? {
                self.ensure_dependencies(&manifest, path)?;
                return self.register_lazy(
                    path,
                    manifest,
                    config,
                    pinned_sha256.clone(),
                    library_pin,
                );
            }
            let artifact = loader::load_verified(
                path,
                self.inner.strict.load(Ordering::Acquire),
                library_pin.as_deref(),
            )?;
            let rejected_manifest = artifact.manifest.clone();
            if let Err(error) = self.ensure_dependencies(&artifact.manifest, path) {
                self.record_manifest_rejection(&error, rejected_manifest, RefusalClass::Unresolved);
                return Err(error);
            }
            self.activate(path, artifact, config, pinned_sha256, None)
        })();
        if let Err(error) = &result {
            self.record_rejection(error);
        }
        result
    }

    /// Register a module without mapping its library into this process.
    ///
    /// `manifest` must be the same manifest embedded in the library and must
    /// set `lazy_init`. The first method call loads the artifact, verifies the
    /// embedded declaration against this copy, and initializes it exactly once.
    pub fn register_lazy_file(
        &self,
        path: impl AsRef<Path>,
        manifest: ModuleManifest,
    ) -> Result<ModuleInfo> {
        self.register_lazy_file_with_config(path, manifest, serde_json::json!({}))
    }

    /// Configured form of [`ModuleHost::register_lazy_file`].
    pub fn register_lazy_file_with_config(
        &self,
        path: impl AsRef<Path>,
        manifest: ModuleManifest,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            check_directory(if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            })?;
        }
        check_file(path)?;
        self.ensure_dependencies(&manifest, path)?;
        // No pin: a caller naming a path on disk vouches for it with the
        // `modules.toml` beside it, if at all.
        let library_sha256 = allowlisted_hash(
            path,
            std::fs::File::open(path)
                .map_err(|_| Error::module_refused(path, "module library could not be opened"))?,
        )?;
        self.register_lazy(path, manifest, config, None, library_sha256)
    }

    /// Discover and load every platform library in a private directory.
    ///
    /// Refusals are returned per artifact so one bad file cannot prevent the
    /// remaining modules from loading.
    pub fn load_dir(&self, directory: impl AsRef<Path>) -> Result<Vec<Result<ModuleInfo>>> {
        self.load_dir_inner(directory.as_ref(), None, &[])
    }

    /// Load modules from a directory while requiring each discovered manifest
    /// to declare `expected_name` before it can be registered with the host.
    #[cfg(feature = "test-support")]
    pub(crate) fn load_dir_expected(
        &self,
        directory: impl AsRef<Path>,
        expected_name: &str,
    ) -> Result<Vec<Result<ModuleInfo>>> {
        self.load_dir_inner(directory.as_ref(), Some(expected_name), &[])
    }

    fn load_dir_inner(
        &self,
        directory: &Path,
        expected_name: Option<&str>,
        skip: &[String],
    ) -> Result<Vec<Result<ModuleInfo>>> {
        check_directory(directory)?;
        let mut directories = self
            .inner
            .directories
            .lock()
            .expect("module directory lock");
        if !directories.iter().any(|known| known == directory) {
            directories.push(directory.to_path_buf());
        }
        drop(directories);
        let mut paths = std::fs::read_dir(directory)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| has_library_extension(path))
            .collect::<Vec<_>>();
        paths.sort();
        // Skipped artifacts are dropped before anything opens them, so the
        // remaining modules still resolve and initialise as one batch with
        // their dependency order and configuration.
        paths.retain(|path| {
            let skipped = is_search_skipped(path, skip);
            if skipped {
                tracing::debug!("[tinybus::module] directory artifact skipped by host");
            }
            !skipped
        });
        let loaded_files = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.info.file.clone())
            .collect::<HashSet<_>>();
        paths.retain(|path| !loaded_files.contains(&safe_file_name(path)));

        let mut outcomes = Vec::new();
        let mut pending = Vec::new();
        for path in paths {
            let inspected = check_file(&path).and_then(|()| {
                if let Some(manifest) = read_lazy_manifest(&path)? {
                    Ok(PendingModule::Lazy(Box::new(manifest)))
                } else {
                    let library_pin =
                        allowlisted_hash(&path, std::fs::File::open(&path).map_err(Error::from)?)?;
                    loader::load_verified(
                        &path,
                        self.inner.strict.load(Ordering::Acquire),
                        library_pin.as_deref(),
                    )
                    .map(|artifact| PendingModule::Loaded(Box::new(artifact)))
                }
            });
            match inspected {
                Ok(module)
                    if expected_name.is_some_and(|expected| {
                        sanitize_untrusted(&module.manifest().module.name) != expected
                    }) =>
                {
                    outcomes.push(Err(Error::module_refused(
                        &path,
                        "module name does not match expected test identity",
                    )));
                }
                Ok(module) => pending.push((path, module)),
                Err(error) => outcomes.push(Err(error)),
            }
        }

        let manifests = pending
            .iter()
            .map(|(_, module)| module.manifest().clone())
            .collect::<Vec<_>>();
        let resolution = crate::module::resolve::resolve(&manifests, &self.provided_interfaces());
        let mut pending = pending.into_iter().map(Some).collect::<Vec<_>>();
        for (index, reason) in resolution.unresolved {
            let (path, module) = pending[index].take().expect("resolver index is valid");
            let error = Error::module_refused(&path, reason);
            self.record_manifest_rejection(
                &error,
                module.manifest().clone(),
                RefusalClass::Unresolved,
            );
            outcomes.push(Err(error));
        }
        for index in resolution.order {
            let (path, module) = pending[index].take().expect("resolver index is valid");
            let config = self
                .inner
                .configs
                .lock()
                .expect("module config lock")
                .get(&module.manifest().module.name)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let result = self
                .ensure_dependencies(module.manifest(), &path)
                .inspect_err(|error| {
                    self.record_manifest_rejection(
                        error,
                        module.manifest().clone(),
                        RefusalClass::Unresolved,
                    );
                })
                // No pin: these came from scanning a directory, so the
                // `modules.toml` in it is the operator's statement about them.
                .and_then(|()| match module {
                    PendingModule::Loaded(artifact) => {
                        self.activate(&path, *artifact, config, None, None)
                    }
                    PendingModule::Lazy(manifest) => {
                        let library_sha256 = std::fs::File::open(&path)
                            .map_err(Error::from)
                            .and_then(|file| allowlisted_hash(&path, file))?;
                        self.register_lazy(&path, *manifest, config, None, library_sha256)
                    }
                });
            outcomes.push(result);
        }
        for error in outcomes.iter().filter_map(|outcome| outcome.as_ref().err()) {
            self.record_rejection(error);
        }
        Ok(outcomes)
    }

    /// Search paths in precedence order for this host. An unconfigured host
    /// has no implicit search paths.
    pub fn search_paths(&self) -> Vec<PathBuf> {
        let config = self
            .inner
            .search_path_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(config) = config else {
            return Vec::new();
        };
        let mut paths = std::env::var_os(&config.env_var)
            .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .unwrap_or_default();
        #[cfg(target_os = "linux")]
        {
            if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
                paths.push(PathBuf::from(path).join(&config.app_dir));
            } else if let Some(path) = std::env::var_os("HOME") {
                paths.push(
                    PathBuf::from(path)
                        .join(".local/share")
                        .join(&config.app_dir),
                );
            }
            paths.push(PathBuf::from("/usr/lib").join(&config.app_dir));
        }
        #[cfg(target_os = "macos")]
        paths.push(PathBuf::from("/usr/local/lib").join(&config.app_dir));
        #[cfg(windows)]
        if let Some(path) = std::env::var_os("LOCALAPPDATA") {
            paths.push(PathBuf::from(path).join(&config.app_dir));
        }
        paths
    }

    /// Load every existing directory in [`ModuleHost::search_paths`] order.
    pub fn load_search_paths(&self) -> Vec<Result<ModuleInfo>> {
        let mut outcomes = Vec::new();
        for path in self.search_paths().into_iter().filter(|path| path.is_dir()) {
            match self.load_dir(path) {
                Ok(results) => outcomes.extend(results),
                Err(error) => outcomes.push(Err(error)),
            }
        }
        outcomes
    }

    /// [`Self::load_search_paths`], minus artifacts whose file stem is in `skip`.
    ///
    /// A skipped artifact is never opened, so a library the host has switched
    /// off is not mapped (and never unloaded) just by sitting on the path. The
    /// remaining artifacts of each directory load exactly as
    /// [`Self::load_dir`] loads them.
    pub fn load_search_paths_except(&self, skip: &[String]) -> Vec<Result<ModuleInfo>> {
        let mut outcomes = Vec::new();
        for directory in self.search_paths().into_iter().filter(|path| path.is_dir()) {
            match self.load_dir_inner(&directory, None, skip) {
                Ok(results) => outcomes.extend(results),
                Err(error) => outcomes.push(Err(error)),
            }
        }
        outcomes
    }

    /// Inspect a directory without initializing or attaching any module.
    pub fn scan_dir(&self, directory: impl AsRef<Path>) -> Result<Vec<ModuleInfo>> {
        let directory = directory.as_ref();
        check_directory(directory)?;
        let mut paths = std::fs::read_dir(directory)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| has_library_extension(path))
            .collect::<Vec<_>>();
        paths.sort();
        let mut results = Vec::new();
        let mut admitted = Vec::new();
        for path in paths {
            let inspected = check_file(&path).and_then(|()| {
                if let Some(manifest) = read_lazy_manifest(&path)? {
                    let info = provisional_info(&path, &manifest)?;
                    Ok((PendingModule::Lazy(Box::new(manifest)), info))
                } else {
                    let library_pin =
                        allowlisted_hash(&path, std::fs::File::open(&path).map_err(Error::from)?)?;
                    let artifact = loader::load_verified(
                        &path,
                        self.inner.strict.load(Ordering::Acquire),
                        library_pin.as_deref(),
                    )?;
                    let info = self.validate(&path, &artifact.descriptor, &artifact.manifest)?;
                    Ok((PendingModule::Loaded(Box::new(artifact)), info))
                }
            });
            match inspected {
                Ok((artifact, info)) => admitted.push((artifact, info)),
                Err(error) => results.push(rejection_info(&error)),
            }
        }
        let manifests = admitted
            .iter()
            .map(|(module, _)| module.manifest().clone())
            .collect::<Vec<_>>();
        let resolution = crate::module::resolve::resolve(&manifests, &self.provided_interfaces());
        let mut admitted = admitted.into_iter().map(Some).collect::<Vec<_>>();
        for (index, reason) in resolution.unresolved {
            let (_, mut info) = admitted[index].take().expect("resolver index is valid");
            info.state = ModuleState::Unresolved { reason };
            results.push(info);
        }
        for index in resolution.order {
            let (_, info) = admitted[index].take().expect("resolver index is valid");
            results.push(info);
        }
        Ok(results)
    }

    /// Stop every module within the supplied deadline per module.
    pub async fn shutdown(&self, deadline: Duration) {
        let names = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.info.name.clone())
            .collect::<Vec<_>>();
        for name in names {
            // A failed stop leaves the module's observed state intact. The
            // callback may still own its bus name, so claiming it stopped
            // would make the host's lifecycle report lie to its caller.
            let _ = self.inner.stop_after_lifecycle(&name, deadline).await;
        }
    }

    fn activate(
        &self,
        path: &Path,
        artifact: LoadedArtifact,
        config: serde_json::Value,
        pinned_sha256: Option<String>,
        linked_setup: Option<(LinkedModuleInitWithSetup, LinkedSetup)>,
    ) -> Result<ModuleInfo> {
        let pinned_library_sha256 = artifact.library_sha256.clone();
        let _admission = self.inner.admission.lock().expect("module admission lock");
        let manifest = artifact.manifest.clone();
        let mut admitted = self
            .validate(path, &artifact.descriptor, &artifact.manifest)
            .inspect_err(|error| {
                self.record_manifest_rejection(error, manifest.clone(), RefusalClass::Rejected);
            })?;
        if self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .any(|loaded| loaded.info.name == admitted.name)
        {
            let error = Error::module_refused(path, "module name is already loaded");
            self.record_manifest_rejection(&error, manifest, RefusalClass::Unresolved);
            return Err(error);
        }

        let config = serde_json::to_vec(&config).map_err(|_| {
            let error = Error::module_refused(path, "module configuration is invalid");
            self.record_manifest_rejection(&error, manifest.clone(), RefusalClass::Rejected);
            error
        })?;
        let trusted_host = self.inner.broker.restricted_module_controller_binding();
        let pre_ready_routes = self
            .inner
            .pre_ready_confidential_routes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (transport, host_vtable) = ModuleTransport::new_with_controller_binding(
            admitted.name.clone(),
            config,
            trusted_host,
            pre_ready_routes,
        );
        match linked_setup {
            Some((init, setup)) => {
                admitted.state = ModuleState::Resolved;
                transport.defer_initializer(host_vtable, move |host| {
                    let mut module = TbModuleVtable::default();
                    let code = unsafe { init(&host, &mut module, setup) };
                    if code == TB_OK {
                        Ok(module)
                    } else {
                        Err("linked module initialization failed".to_string())
                    }
                });
            }
            None if artifact.manifest.lazy_init => {
                admitted.state = ModuleState::Resolved;
                transport.defer_initialize(artifact.init, host_vtable);
            }
            None => {
                admitted.state = ModuleState::Initializing;
                if let Err(failure) = transport.eager_initialize(artifact.init, host_vtable) {
                    let reason = if failure.to_string().contains("vtable") {
                        "module returned an invalid vtable"
                    } else {
                        "module initialization failed"
                    };
                    let error = Error::module_refused(path, reason);
                    self.record_manifest_rejection(&error, manifest, RefusalClass::Failed);
                    return Err(error);
                }
            }
        }

        self.attach_transport(
            path,
            admitted,
            manifest,
            transport,
            Activation {
                lazy_init: artifact.manifest.lazy_init,
                lazy_load: false,
                descriptor_info: None,
                pinned_sha256,
                pinned_library_sha256,
            },
        )
    }

    fn register_lazy(
        &self,
        path: &Path,
        manifest: ModuleManifest,
        config: serde_json::Value,
        pinned_sha256: Option<String>,
        pinned_library_sha256: Option<String>,
    ) -> Result<ModuleInfo> {
        let _admission = self.inner.admission.lock().expect("module admission lock");
        let admitted = provisional_info(path, &manifest).inspect_err(|error| {
            self.record_manifest_rejection(error, manifest.clone(), RefusalClass::Rejected);
        })?;
        if self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .any(|loaded| loaded.info.name == admitted.name)
        {
            let error = Error::module_refused(path, "module name is already loaded");
            self.record_manifest_rejection(&error, manifest.clone(), RefusalClass::Unresolved);
            return Err(error);
        }
        let config = serde_json::to_vec(&config).map_err(|_| {
            let error = Error::module_refused(path, "module configuration is invalid");
            self.record_manifest_rejection(&error, manifest.clone(), RefusalClass::Rejected);
            error
        })?;
        let trusted_host = self.inner.broker.restricted_module_controller_binding();
        let pre_ready_routes = self
            .inner
            .pre_ready_confidential_routes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (transport, host_vtable) = ModuleTransport::new_with_controller_binding(
            admitted.name.clone(),
            config,
            trusted_host,
            pre_ready_routes,
        );
        let expected = manifest.clone();
        let artifact_path = path.to_path_buf();
        let strict = self.inner.strict.load(Ordering::Acquire);
        let expected_library = pinned_library_sha256.clone();
        let descriptor_info = Arc::new(Mutex::new(None));
        let loaded_descriptor_info = descriptor_info.clone();
        transport.defer_initializer(host_vtable, move |host| {
            // Registration may precede the first call by hours. Re-run file
            // admission immediately before the platform loader so an artifact
            // replaced after discovery cannot inherit the old hash attestation.
            check_file(&artifact_path)
                .map_err(|_| "module library no longer passes admission".to_string())?;
            let artifact =
                loader::load_verified(&artifact_path, strict, expected_library.as_deref())
                    .map_err(|_| "module library could not be loaded".to_string())?;
            let rustc = sanitized_field(&artifact.descriptor.rustc_version);
            if artifact.manifest != expected
                || loaded_identity(&artifact.descriptor, &artifact.manifest).is_none()
                || rustc.is_none()
            {
                return Err("loaded module does not match its lazy manifest".to_string());
            }
            *loaded_descriptor_info
                .lock()
                .expect("module descriptor lock") = Some(DescriptorMetadata {
                rustc_version: rustc.expect("checked above"),
                rustc_mismatch: field_bytes(&artifact.descriptor.rustc_version)
                    != build_info::RUSTC_VERSION.as_bytes(),
            });
            let mut module = TbModuleVtable::default();
            let code = unsafe { (artifact.init)(&host, &mut module) };
            if code == TB_OK {
                Ok(module)
            } else {
                Err("module initialization failed".to_string())
            }
        });
        self.attach_transport(
            path,
            admitted,
            manifest,
            transport,
            Activation {
                lazy_init: true,
                lazy_load: true,
                descriptor_info: Some(descriptor_info),
                pinned_sha256,
                pinned_library_sha256,
            },
        )
    }

    fn attach_transport(
        &self,
        path: &Path,
        admitted: ModuleInfo,
        manifest: ModuleManifest,
        transport: Arc<ModuleTransport>,
        activation: Activation,
    ) -> Result<ModuleInfo> {
        let transport_for_broker: Arc<dyn Transport> = transport.clone();
        let staged = self.inner.broker.attach_module_staged(transport_for_broker);
        let unique = staged.unique_name().clone();
        let reserved_change = match self
            .inner
            .broker
            .reserve_module_name(&unique, admitted.manifest.bus_name.clone())
        {
            Ok(change) => change,
            Err(_) => {
                transport.commit_terminal(true);
                transport.request_terminal(true);
                let error = Error::module_refused(path, "module bus name is already owned");
                self.record_manifest_rejection(&error, manifest, RefusalClass::Unresolved);
                return Err(error);
            }
        };
        // A verified release, an allowlisted local file, or linked code can
        // become an attested recipient. The linked case identifies the
        // executable containing its code, not a separate release artifact.
        //
        // On disk, it is `modules.toml` beside the artifact, re-read here
        // rather than plumbed down from the gate so that an artifact which
        // changed underneath us no longer matches and does not become attested.
        //
        // From a pinned release, it is the digest the caller compiled in, which
        // `acquire` checked against the release's own checksum manifest and
        // against the downloaded bytes before extracting anything. There is no
        // `modules.toml` to re-read in that case — the artifact lives in a
        // private temporary directory this host created moments ago — so the
        // value is carried down instead. Linked code carries the executable
        // digest through the same field. Without one of these sources there is
        // no attestation.
        let archive_is_pinned = activation.pinned_sha256.is_some();
        let vouched = match activation.pinned_sha256 {
            Some(pinned) => Some(pinned),
            None => std::fs::File::open(path)
                .map_err(Error::from)
                .and_then(|file| allowlisted_hash(path, file))
                .ok()
                .flatten(),
        };
        let library_sha256 = activation.pinned_library_sha256;
        let library_matches_vouch = archive_is_pinned
            || library_sha256
                .as_ref()
                .is_none_or(|library| vouched.as_ref() == Some(library));
        if let (Some(sha256), true) = (vouched, library_matches_vouch) {
            self.inner.broker.attest_module(
                &unique,
                crate::attest::Attestation {
                    name: admitted.manifest.bus_name.clone(),
                    sha256,
                    library_sha256,
                },
            );
        }

        let lazy_init = activation.lazy_init;
        let lazy_load = activation.lazy_load;
        let descriptor_info = activation.descriptor_info;
        if !self.inner.warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                modules = 1,
                "in-process modules are inside the host trust boundary"
            );
        }
        if lazy_load {
            tracing::info!(module = %admitted.name, "module registered for lazy loading");
        } else {
            tracing::info!(module = %admitted.name, "module loaded");
        }
        let ready_transport = transport.clone();
        let module_name = admitted.name.clone();
        let previous_state = state_name(&admitted.state);
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .push(LoadedModule {
                info: admitted.clone(),
                module_id: ModuleId::new(manifest.module.name.clone()),
                transport,
                unique_name: unique.clone(),
                transition_from: None,
                descriptor_info,
                lifecycle_busy: false,
                callback_config: None,
                callback_state: None,
                _callback_binding: None,
            });
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| staged.start())).is_err() {
            let mut loaded = self.inner.loaded.lock().expect("module list lock");
            if let Some(index) = loaded.iter().position(|module| {
                module.unique_name == unique && Arc::ptr_eq(&module.transport, &ready_transport)
            }) {
                loaded.remove(index);
            }
            drop(loaded);
            ready_transport.commit_terminal(true);
            ready_transport.request_terminal(true);
            let error = Error::module_refused(path, "module transport could not start");
            self.record_manifest_rejection(&error, manifest, RefusalClass::Failed);
            return Err(error);
        }
        let broker = self.inner.broker.clone();
        tokio::spawn(async move {
            if lazy_init {
                ready_transport.wait_initializing().await;
                if !ready_transport.init_started() || ready_transport.is_faulted() {
                    return;
                }
                broker
                    .announce_module_state(serde_json::json!([
                        module_name.clone(),
                        "resolved",
                        "initializing",
                        null
                    ]))
                    .await;
            }
            ready_transport.wait_ready().await;
            if ready_transport.is_ready() {
                broker.announce_name_change(reserved_change).await;
                broker
                    .announce_module_state(serde_json::json!([
                        module_name,
                        previous_state,
                        "ready",
                        null
                    ]))
                    .await;
            }
        });
        #[cfg(test)]
        if let Some(barrier) = self
            .inner
            .after_loaded_publish
            .lock()
            .expect("loaded publication barrier lock")
            .take()
        {
            barrier
                .reached
                .send(())
                .expect("test observes loaded publication barrier");
            barrier
                .release
                .recv()
                .expect("test releases loaded publication barrier");
        }
        Ok(admitted)
    }

    fn validate(
        &self,
        path: &Path,
        descriptor: &TbAbiDescriptor,
        manifest: &ModuleManifest,
    ) -> Result<ModuleInfo> {
        let refuse = |reason| Error::module_refused(path, reason);
        loader::gate_descriptor(path, descriptor, self.inner.strict.load(Ordering::Acquire))?;

        let rustc = sanitized_field(&descriptor.rustc_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let rustc_mismatch =
            field_bytes(&descriptor.rustc_version) != build_info::RUSTC_VERSION.as_bytes();
        if rustc_mismatch {
            tracing::warn!(module = %sanitize_untrusted(&manifest.module.name), "module rustc differs from host");
        }

        let descriptor_name = std::str::from_utf8(field_bytes(&descriptor.module_name))
            .map_err(|_| refuse("descriptor identity is invalid"))?;
        if !canonical_module_id(&manifest.module.name) || descriptor_name != manifest.module.name {
            return Err(refuse("manifest identity does not match descriptor"));
        }
        let name = sanitize_untrusted(descriptor_name);
        let version = sanitized_field(&descriptor.module_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        if manifest.schema != MANIFEST_SCHEMA
            || sanitize_untrusted(&manifest.module.version.to_string()) != version
            || manifest.module.name.len() > 64
            || manifest.module.version.to_string().len() > 32
        {
            return Err(refuse("manifest identity does not match descriptor"));
        }

        Ok(ModuleInfo {
            name,
            version,
            file: safe_file_name(path),
            state: ModuleState::Resolved,
            manifest: manifest.clone(),
            rustc_version: rustc,
            rustc_mismatch,
            enabled: true,
        })
    }

    fn ensure_dependencies(&self, manifest: &ModuleManifest, path: &Path) -> Result<()> {
        let available = self.provided_interfaces();
        if manifest
            .requires
            .iter()
            .filter(|dependency| !dependency.optional)
            .any(|dependency| !available.contains(dependency.interface.interface.as_str()))
        {
            return Err(Error::module_refused(
                path,
                "a required interface has no provider",
            ));
        }
        Ok(())
    }

    fn provided_interfaces(&self) -> HashSet<String> {
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .flat_map(|module| {
                module
                    .info
                    .manifest
                    .provides
                    .iter()
                    .map(|provided| provided.version.interface.to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn record_rejection(&self, error: &Error) {
        let Error::ModuleRefused { .. } = error else {
            return;
        };
        let info = rejection_info(error);
        let mut rejected = self
            .inner
            .rejected
            .lock()
            .expect("rejected module list lock");
        if !rejected.iter().any(|known| known.file == info.file) {
            rejected.push(info);
        }
    }

    fn record_manifest_rejection(
        &self,
        error: &Error,
        manifest: ModuleManifest,
        class: RefusalClass,
    ) {
        let Error::ModuleRefused { file, reason } = error else {
            return;
        };
        let state = match class {
            RefusalClass::Rejected => ModuleState::Rejected {
                reason: reason.clone(),
            },
            RefusalClass::Unresolved => ModuleState::Unresolved {
                reason: reason.clone(),
            },
            RefusalClass::Failed => ModuleState::Failed {
                reason: reason.clone(),
            },
        };
        let info = ModuleInfo {
            name: sanitize_untrusted(&manifest.module.name),
            version: sanitize_untrusted(&manifest.module.version.to_string()),
            file: file.clone(),
            state,
            manifest,
            rustc_version: String::new(),
            rustc_mismatch: false,
            enabled: false,
        };
        let mut rejected = self
            .inner
            .rejected
            .lock()
            .expect("rejected module list lock");
        if let Some(existing) = rejected.iter_mut().find(|known| known.file == info.file) {
            *existing = info;
        } else {
            rejected.push(info);
        }
    }
}

#[async_trait::async_trait]
impl ModuleControl for ModuleHostInner {
    fn list(&self) -> Vec<ModuleInfo> {
        let mut modules = self
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(LoadedModule::snapshot)
            .collect::<Vec<_>>();
        modules.extend(
            self.rejected
                .lock()
                .expect("rejected module list lock")
                .iter()
                .cloned(),
        );
        modules
    }

    fn load(
        self: Arc<Self>,
        path: PathBuf,
        config: serde_json::Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        let info = ModuleHost { inner: self }.load_file_with_config(path, config)?;
        let transition = Some((
            info.name.clone(),
            ModuleState::Discovered,
            info.state.clone(),
        ));
        Ok((info, transition))
    }

    fn load_github(
        self: Arc<Self>,
        release_url: String,
        asset_name: String,
        sha256: String,
        config: serde_json::Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        let info = ModuleHost { inner: self }.load_github_release(
            release_url,
            asset_name,
            Some(&sha256),
            config,
        )?;
        let transition = Some((
            info.name.clone(),
            ModuleState::Discovered,
            info.state.clone(),
        ));
        Ok((info, transition))
    }

    /// Stops the loaded module named `name`, bounding shutdown by `deadline`.
    async fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        self.stop_inner(name, deadline).await
    }

    async fn stop_after_lifecycle(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        let started = std::time::Instant::now();
        tokio::time::timeout(deadline, async {
            loop {
                let busy = self
                    .loaded
                    .lock()
                    .expect("module list lock")
                    .iter()
                    .find(|module| module.info.name == name)
                    .ok_or_else(|| Error::failed("module is not loaded"))?
                    .lifecycle_busy;
                if !busy {
                    return Ok::<(), Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| Error::failed("module lifecycle operation exceeded its deadline"))??;
        self.stop_inner(name, deadline.saturating_sub(started.elapsed()))
            .await
    }

    async fn stop_inner(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        let started = std::time::Instant::now();
        let (transport, callbacks) = {
            let mut loaded = self.loaded.lock().expect("module list lock");
            let module = loaded
                .iter_mut()
                .find(|module| module.info.name == name)
                .ok_or_else(|| Error::failed("module is not loaded"))?;
            let old = module.snapshot().state;
            if module.lifecycle_busy {
                return Err(Error::failed(
                    "module lifecycle operation is already running",
                ));
            }
            if matches!(
                &old,
                ModuleState::Stopped | ModuleState::Faulted { .. } | ModuleState::Failed { .. }
            ) {
                return Err(Error::ModuleUnavailable {
                    module: module.info.name.clone(),
                    state: state_name(&old).to_string(),
                    detail: state_detail(&old)
                        .unwrap_or("module is terminal")
                        .to_string(),
                });
            }
            self.broker
                .router
                .lock()
                .expect("router lock")
                .revoke_callbacks_for_peer(&module.unique_name);
            // Revoke native admission immediately; provider drain owns the
            // later shutdown handoff, so this must not submit a physical job.
            module.transport.commit_terminal(false);
            module.transition_from.get_or_insert(old);
            module.lifecycle_busy = true;
            (module.transport.clone(), module.callback_state.clone())
        };
        let _lifecycle_busy = LifecycleBusyGuard::for_stop(self, name, transport.clone());
        if let Some(callbacks) = callbacks {
            if let Err(error) = callbacks
                .drain(deadline.saturating_sub(started.elapsed()))
                .await
            {
                let mut loaded = self.loaded.lock().expect("module list lock");
                if let Some(module) = loaded.iter_mut().find(|module| module.info.name == name) {
                    module.transition_from = None;
                }
                return Err(error);
            }
        }
        let stopped = transport
            .stop(deadline.saturating_sub(started.elapsed()))
            .await;
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not loaded"))?;
        match stopped {
            Ok(_) => {}
            Err(StopError::NotStarted(error)) => {
                if !module.transport.stop_task_spawned() {
                    module.transition_from = None;
                }
                return Err(error);
            }
            Err(StopError::Started(error)) => return Err(error),
        }
        module.info.state = if module.transport.is_faulted() {
            if module.transport.init_failed() {
                ModuleState::Failed {
                    reason: "module initialization failed".into(),
                }
            } else {
                ModuleState::Faulted {
                    reason: "module reported an unrecoverable fault".into(),
                }
            }
        } else {
            ModuleState::Stopped
        };
        Ok(module.info.clone())
    }

    async fn reinitialize(&self, name: &str, config: serde_json::Value) -> Result<ModuleInfo> {
        let bytes = crate::Secret::new(
            serde_json::to_vec(&config)
                .map_err(|_| Error::failed("module configuration is invalid"))?,
        );
        drop(config);
        let fingerprint = callback_config_fingerprint(bytes.expose_secret());
        let transport = {
            let mut loaded = self.loaded.lock().expect("module list lock");
            let module = loaded
                .iter_mut()
                .find(|module| module.info.name == name)
                .ok_or_else(|| Error::failed("module is not loaded"))?;
            let state = module.snapshot().state;
            if module
                .callback_config
                .as_ref()
                .is_some_and(|initial| initial != &fingerprint)
            {
                return Err(Error::failed("scoped callback configuration is immutable"));
            }
            if module.lifecycle_busy {
                return Err(Error::failed(
                    "module lifecycle operation is already running",
                ));
            }
            let deferred_initialization = matches!(state, ModuleState::Resolved)
                && module.transport.has_deferred_initializer();
            if !matches!(state, ModuleState::Ready | ModuleState::Serving)
                && !deferred_initialization
            {
                return Err(Error::ModuleUnavailable {
                    module: module.info.name.clone(),
                    state: state_name(&state).to_string(),
                    detail: "module must be ready before reinitialization".to_string(),
                });
            }
            module.lifecycle_busy = true;
            module.transport.clone()
        };
        let _lifecycle_busy = LifecycleBusyGuard::for_reinitialize(self, name);
        let reinitialized = transport.reinitialize(bytes).await;
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not loaded"))?;
        reinitialized?;
        Ok(module.snapshot())
    }

    fn enable(&self, name: &str, enabled: bool) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not known"))?;
        // Held through this synchronous mutation, atomic with lifecycle
        // reservation. The public waiter can end before its native task.
        if module.lifecycle_busy || module.transport.native_lifecycle_pending() {
            return Err(Error::failed(
                "module lifecycle operation is already running",
            ));
        }
        let old = module.snapshot().state;
        if matches!(
            &old,
            ModuleState::Stopped | ModuleState::Failed { .. } | ModuleState::Faulted { .. }
        ) {
            return Err(Error::ModuleUnavailable {
                module: module.info.name.clone(),
                state: state_name(&old).to_string(),
                detail: state_detail(&old)
                    .unwrap_or("module is terminal")
                    .to_string(),
            });
        }
        module.info.enabled = enabled;
        module.info.state = if enabled {
            if module.transport.is_ready() {
                ModuleState::Ready
            } else {
                ModuleState::Resolved
            }
        } else {
            ModuleState::Disabled
        };
        let info = module.snapshot();
        let transition = (old != info.state).then(|| (info.name.clone(), old, info.state.clone()));
        Ok((info, transition))
    }

    fn rescan(
        self: Arc<Self>,
        paths: Vec<PathBuf>,
        dry_run: bool,
    ) -> Result<(Vec<ModuleInfo>, Vec<ModuleTransition>)> {
        let directories = if paths.is_empty() {
            let configured = self
                .directories
                .lock()
                .expect("module directory lock")
                .clone();
            if configured.is_empty() {
                ModuleHost {
                    inner: Arc::clone(&self),
                }
                .search_paths()
                .into_iter()
                .filter(|path| path.is_dir())
                .collect()
            } else {
                configured
            }
        } else {
            paths
        };
        let host = ModuleHost { inner: self };
        let mut loaded = Vec::new();
        for directory in directories {
            if dry_run {
                loaded.extend(host.scan_dir(directory)?);
                continue;
            }
            for outcome in host.load_dir(directory)? {
                match outcome {
                    Ok(info) => loaded.push(info),
                    Err(error) => tracing::warn!(error = %error, "module refused during rescan"),
                }
            }
        }
        let transitions = if dry_run {
            Vec::new()
        } else {
            loaded
                .iter()
                .map(|info| {
                    (
                        info.name.clone(),
                        ModuleState::Discovered,
                        info.state.clone(),
                    )
                })
                .collect()
        };
        Ok((loaded, transitions))
    }

    /// Updates module state after `unique_name` disconnects and returns any transition to publish.
    fn peer_detached(&self, unique_name: &BusName) -> Option<(String, ModuleState, ModuleState)> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| &module.unique_name == unique_name)?;
        if !module.transport.is_faulted() {
            if let Some(old) = module.transition_from.take() {
                module.info.state = ModuleState::Stopped;
                return Some((module.info.name.clone(), old, ModuleState::Stopped));
            }
        }
        if !module.transport.is_faulted()
            || matches!(
                module.info.state,
                ModuleState::Stopped | ModuleState::Disabled
            )
        {
            return None;
        }
        let old = if module.transport.is_ready() {
            if module.transport.inflight() == 0 {
                ModuleState::Ready
            } else {
                ModuleState::Serving
            }
        } else if module.transport.init_started()
            && matches!(module.info.state, ModuleState::Resolved)
        {
            ModuleState::Initializing
        } else {
            module.info.state.clone()
        };
        let new = if module.transport.init_failed() {
            ModuleState::Failed {
                reason: "module initialization failed".to_string(),
            }
        } else {
            ModuleState::Faulted {
                reason: "module reported an unrecoverable fault".to_string(),
            }
        };
        module.info.state = new.clone();
        Some((module.info.name.clone(), old, new))
    }

    fn unavailable_for(&self, bus_name: &BusName) -> Option<Error> {
        let loaded = self.loaded.lock().expect("module list lock");
        let loaded_info = loaded
            .iter()
            .find(|module| {
                if bus_name.is_unique() {
                    &module.unique_name == bus_name
                } else {
                    &module.info.manifest.bus_name == bus_name
                }
            })
            .map(LoadedModule::snapshot);
        drop(loaded);
        let info = loaded_info.or_else(|| {
            self.rejected
                .lock()
                .expect("rejected module list lock")
                .iter()
                .find(|module| &module.manifest.bus_name == bus_name)
                .cloned()
        })?;
        let detail = state_detail(&info.state)
            .unwrap_or("module is not accepting calls")
            .to_string();
        matches!(
            info.state,
            ModuleState::Rejected { .. }
                | ModuleState::Faulted { .. }
                | ModuleState::Failed { .. }
                | ModuleState::Stopped
                | ModuleState::Disabled
        )
        .then(|| Error::ModuleUnavailable {
            module: info.name,
            state: state_name(&info.state).to_string(),
            detail,
        })
    }
}

fn lazy_manifest_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    path.with_file_name(format!("{name}{LAZY_MANIFEST_SUFFIX}"))
}

fn read_lazy_manifest(path: &Path) -> Result<Option<ModuleManifest>> {
    let sidecar = lazy_manifest_path(path);
    let sidecar_metadata = match std::fs::symlink_metadata(&sidecar) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Error::module_refused(path, "lazy manifest is unreadable")),
    };
    if !sidecar_metadata.file_type().is_file() || sidecar_metadata.len() > LAZY_MANIFEST_MAX_LEN {
        return Err(Error::module_refused(
            path,
            "lazy manifest is not a regular file below the 1 MiB limit",
        ));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;

        #[cfg(target_os = "macos")]
        const O_NOFOLLOW: i32 = 0x100;
        #[cfg(not(target_os = "macos"))]
        const O_NOFOLLOW: i32 = 0x2_0000;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(&sidecar)
            .map_err(|_| Error::module_refused(path, "lazy manifest is unreadable"))?
    };
    #[cfg(windows)]
    let file = std::fs::File::open(&sidecar)
        .map_err(|_| Error::module_refused(path, "lazy manifest is unreadable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| Error::module_refused(path, "lazy manifest metadata is unavailable"))?;
    if !metadata.file_type().is_file() || metadata.len() > LAZY_MANIFEST_MAX_LEN {
        return Err(Error::module_refused(
            path,
            "lazy manifest is not a regular file below the 1 MiB limit",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(LAZY_MANIFEST_MAX_LEN + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::module_refused(path, "lazy manifest is unreadable"))?;
    if bytes.len() as u64 > LAZY_MANIFEST_MAX_LEN {
        return Err(Error::module_refused(
            path,
            "lazy manifest is not a regular file below the 1 MiB limit",
        ));
    }
    let manifest: ModuleManifest = serde_json::from_slice(&bytes)
        .map_err(|_| Error::module_refused(path, "lazy manifest is not valid JSON"))?;
    if !manifest.lazy_init {
        return Err(Error::module_refused(
            path,
            "lazy manifest must set lazy_init",
        ));
    }
    Ok(Some(manifest))
}

fn provisional_info(path: &Path, manifest: &ModuleManifest) -> Result<ModuleInfo> {
    let name = sanitize_untrusted(&manifest.module.name);
    let version = sanitize_untrusted(&manifest.module.version.to_string());
    if !manifest.lazy_init
        || manifest.schema != MANIFEST_SCHEMA
        || !canonical_module_id(&manifest.module.name)
        || version != manifest.module.version.to_string()
        || manifest.module.name.len() > 64
        || manifest.module.version.to_string().len() > 32
    {
        return Err(Error::module_refused(
            path,
            "lazy manifest identity is invalid",
        ));
    }
    Ok(ModuleInfo {
        name,
        version,
        file: safe_file_name(path),
        state: ModuleState::Resolved,
        manifest: manifest.clone(),
        // These descriptor facts are unavailable until the first call maps the
        // library. They remain empty/false rather than pretending to be known.
        rustc_version: String::new(),
        rustc_mismatch: false,
        enabled: true,
    })
}

fn loaded_identity(
    descriptor: &TbAbiDescriptor,
    manifest: &ModuleManifest,
) -> Option<(String, String)> {
    let descriptor_name = std::str::from_utf8(field_bytes(&descriptor.module_name)).ok()?;
    let name = sanitize_untrusted(descriptor_name);
    let version = sanitized_field(&descriptor.module_version)?;
    (manifest.schema == MANIFEST_SCHEMA
        && canonical_module_id(&manifest.module.name)
        && descriptor_name == manifest.module.name
        && sanitize_untrusted(&manifest.module.version.to_string()) == version
        && manifest.module.name.len() <= 64
        && manifest.module.version.to_string().len() <= 32)
        .then_some((name, version))
}

/// Module identifiers are package names, not log-safe display labels. Keep the
/// full validated identifier for authorization while allowing the summary name
/// to remain capped by `sanitize_untrusted`.
fn canonical_module_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
}

fn rejection_info(error: &Error) -> ModuleInfo {
    let (file, reason) = match error {
        Error::ModuleRefused { file, reason } => (file.clone(), reason.clone()),
        _ => ("module".to_string(), "module inspection failed".to_string()),
    };
    ModuleInfo {
        name: file.clone(),
        version: String::new(),
        file: file.clone(),
        state: ModuleState::Rejected { reason },
        manifest: ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: file,
                version: Version::new(0, 0, 0),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new("ai.tinyhumans.module.Rejected").expect("literal bus name"),
            object_path: ObjectPath::new("/ai/tinyhumans/module/Rejected")
                .expect("literal object path"),
            provides: Vec::new(),
            requires: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        },
        rustc_version: String::new(),
        rustc_mismatch: false,
        enabled: false,
    }
}

pub(crate) fn state_name(state: &ModuleState) -> &'static str {
    match state {
        ModuleState::Discovered => "discovered",
        ModuleState::Rejected { .. } => "rejected",
        ModuleState::Unresolved { .. } => "unresolved",
        ModuleState::Resolved => "resolved",
        ModuleState::Initializing => "initializing",
        ModuleState::Ready => "ready",
        ModuleState::Serving => "serving",
        ModuleState::Faulted { .. } => "faulted",
        ModuleState::Failed { .. } => "failed",
        ModuleState::Stopped => "stopped",
        ModuleState::Disabled => "disabled",
    }
}

pub(crate) fn state_detail(state: &ModuleState) -> Option<&str> {
    match state {
        ModuleState::Rejected { reason }
        | ModuleState::Unresolved { reason }
        | ModuleState::Faulted { reason }
        | ModuleState::Failed { reason } => Some(reason),
        _ => None,
    }
}

fn sanitized_field<const N: usize>(field: &[u8; N]) -> Option<String> {
    let raw = std::str::from_utf8(field_bytes(field)).ok()?;
    let sanitized = sanitize_untrusted(raw);
    (!sanitized.is_empty() && sanitized == raw).then_some(sanitized)
}

fn safe_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_untrusted)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "module".to_string())
}

fn check_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;

        #[cfg(target_os = "macos")]
        const O_NOFOLLOW: i32 = 0x100;
        #[cfg(not(target_os = "macos"))]
        const O_NOFOLLOW: i32 = 0x2_0000;
        // There is no portable fd-based dlopen. O_NOFOLLOW closes the obvious
        // symlink path, while the checked parent permissions are what prevent
        // a hostile swap in the unavoidable check-to-dlopen interval.
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(path)
            .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?
    };
    #[cfg(unix)]
    let metadata = file
        .metadata()
        .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?;
    #[cfg(windows)]
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?;
    if !metadata.file_type().is_file() {
        return Err(Error::module_refused(
            path,
            "artifact is not a regular file",
        ));
    }
    if metadata.len() > 512 * 1024 * 1024 {
        return Err(Error::module_refused(
            path,
            "artifact exceeds the 512 MiB size cap",
        ));
    }
    if !has_library_extension(path) {
        return Err(Error::module_refused(
            path,
            "artifact extension is not loadable",
        ));
    }
    #[cfg(windows)]
    if windows_path_grants_untrusted_write(path)? {
        return Err(Error::module_refused(
            path,
            "artifact is writable by another user",
        ));
    }
    #[cfg(windows)]
    let file = std::fs::File::open(path)
        .map_err(|_| Error::module_refused(path, "artifact is unreadable"))?;
    check_allowlist(path, file)
}

/// Refusal reasons about where an artifact sits, not what it is.
///
/// These say nothing about the bytes: the digest and allowlist gates have not
/// been asked. A loader that has another, independently verified copy of the
/// same pinned release may therefore try that copy instead.
const PLACEMENT_REFUSALS: &[&str] = &[
    "module directory is writable by another user",
    "module directory is owned by another user",
    "artifact is writable by another user",
];

/// Whether `error` refused an artifact for its location rather than its content.
pub(crate) fn is_placement_refusal(error: &Error) -> bool {
    // A unix refusal goes on to name the ancestor that failed, so match the
    // phrase it starts with.
    matches!(error, Error::ModuleRefused { reason, .. }
        if PLACEMENT_REFUSALS.iter().any(|phrase| reason.starts_with(phrase)))
}

fn check_allowlist(path: &Path, file: std::fs::File) -> Result<()> {
    allowlisted_hash(path, file).map(|_| ())
}

/// The artifact's verified SHA-256, or `None` where the directory carries no
/// allowlist at all.
///
/// Splitting the value out of the gate is what lets a loaded module become an
/// attested recipient: the hash the operator vouched for is exactly the fact a
/// confidential sender needs, and recomputing it later from a file that may
/// since have changed would attest something nobody checked.
fn allowlisted_hash(path: &Path, file: std::fs::File) -> Result<Option<String>> {
    let Some(directory) = path.parent() else {
        return Ok(None);
    };
    let allowlist = directory.join("modules.toml");
    if !allowlist.exists() {
        return Ok(None);
    }
    let source = std::fs::read_to_string(&allowlist)
        .map_err(|_| Error::module_refused(path, "module allowlist is unreadable"))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let file_stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let expected = crate::attest::parse_allowlist(&source)
        .find(|(key, _)| key == file_name || key == file_stem)
        .map(|(_, value)| value);
    let Some(expected) = expected else {
        return Err(Error::module_refused(
            path,
            "artifact is absent from the module allowlist",
        ));
    };
    if !crate::attest::is_hex_sha256(&expected) {
        return Err(Error::module_refused(
            path,
            "module allowlist contains an invalid hash",
        ));
    }
    let actual = crate::module::remembered_hash::file_hex_remembered(file)
        .map_err(|_| Error::module_refused(path, "artifact hash could not be read"))?;
    if actual != expected {
        return Err(Error::module_refused(
            path,
            "artifact hash does not match the module allowlist",
        ));
    }
    Ok(Some(actual))
}

/// Whether `path`'s file stem is one of `skip`.
fn is_search_skipped(path: &Path, skip: &[String]) -> bool {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| skip.iter().any(|s| s == stem))
}

fn has_library_extension(path: &Path) -> bool {
    let expected = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    path.extension().and_then(|value| value.to_str()) == Some(expected)
}

#[cfg(unix)]
fn check_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?
            .join(path)
    };
    // SAFETY: getuid cannot fail and touches no memory.
    let uid = unsafe { libc::getuid() };
    // Looked up once per walk, and only if some ancestor is group-writable:
    // a directory-service (LDAP, sssd) lookup is not free.
    let private_gid = std::cell::OnceCell::new();
    for component in absolute.ancestors() {
        let metadata = std::fs::symlink_metadata(component)
            .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?;
        if !metadata.file_type().is_dir() {
            return Err(Error::module_refused(
                path,
                "module search path contains a non-directory component",
            ));
        }
        let mode = metadata.mode();
        // Only a directory that is group-writable, not sticky, and owned by the
        // user or root can reach the private-group exception, so everything
        // else (`/tmp`) skips the account lookup.
        let needs_private_gid = mode & 0o020 != 0
            && mode & 0o1000 == 0
            && (metadata.uid() == uid || metadata.uid() == 0)
            && !root_equivalent_group(metadata.uid(), metadata.gid());
        let private_gid = if needs_private_gid {
            // An ACL can grant another account what the mode does not show.
            (!has_extended_acl(component))
                .then(|| *private_gid.get_or_init(|| current_user_private_gid(uid)))
                .flatten()
        } else {
            None
        };
        if let Some(reason) =
            unix_directory_refusal(metadata.uid(), metadata.gid(), mode, uid, private_gid)
        {
            let mode = mode & 0o7777;
            let label = ancestor_label(component, &absolute);
            tracing::debug!(
                directory = %label,
                mode = format_args!("{mode:04o}"),
                reason,
                "module directory ancestor refused"
            );
            return Err(Error::module_refused(
                path,
                format!("{reason} at {label} mode {mode:04o}"),
            ));
        }
    }
    Ok(())
}

/// Top-level system directories that are safe to report; matched only
/// directly beneath the filesystem root, never by basename elsewhere.
#[cfg(unix)]
const REPORTABLE_ROOT_CHILDREN: &[&str] = &[
    "bin",
    "boot",
    "etc",
    "home",
    "lib",
    "lib64",
    "Library",
    "media",
    "mnt",
    "opt",
    "private",
    "root",
    "run",
    "sbin",
    "srv",
    "tmp",
    "usr",
    "var",
    "Users",
    "Applications",
    "Volumes",
];

/// How a refusal names the ancestor that failed.
///
/// A refusal reaches telemetry, so it never carries a name that could belong
/// to an account or a user-made directory. Only the filesystem root, the
/// module directory itself, `$HOME`, and a fixed top-level system directory
/// are named; every other component is reported as `an ancestor directory`.
#[cfg(unix)]
fn ancestor_label(component: &Path, module_directory: &Path) -> String {
    if component.parent().is_none() {
        return "the filesystem root".to_string();
    }
    if component == module_directory {
        return "the directory itself".to_string();
    }
    if std::env::var_os("HOME").is_some_and(|home| Path::new(&home) == component) {
        return "the home directory".to_string();
    }
    let top_level = component.parent() == Some(Path::new("/"));
    match component.file_name().and_then(|name| name.to_str()) {
        Some(name) if top_level && REPORTABLE_ROOT_CHILDREN.contains(&name) => name.to_string(),
        _ => "an ancestor directory".to_string(),
    }
}

/// `private_gid` is the current user's private group (see
/// [`is_user_private_group`]), or `None` when they have none.
#[cfg(unix)]
fn unix_directory_refusal(
    owner: u32,
    group: u32,
    mode: u32,
    current_uid: u32,
    private_gid: Option<u32>,
) -> Option<&'static str> {
    let world_writable = mode & 0o002 != 0;
    let group_writable = mode & 0o020 != 0;
    let sticky = mode & 0o1000 != 0;
    // Only consulted for a directory this user or root owns, where group
    // write by the user's private group grants nobody but that user.
    let group_grants_only_the_user =
        root_equivalent_group(owner, group) || private_gid == Some(group);
    if owner != current_uid && owner != 0 {
        Some("module directory is owned by another user")
    } else if sticky {
        None
    } else if world_writable || (group_writable && !group_grants_only_the_user) {
        Some("module directory is writable by another user")
    } else {
        None
    }
}

/// Whether `etc/nsswitch.conf` text resolves users and groups from local
/// files only (`files`, `systemd`; `compat` can import NIS entries, so it is not local), the one case where `/etc/passwd`
/// and `/etc/group` are the whole account database.
#[cfg(unix)]
fn accounts_are_local(nsswitch: &str) -> bool {
    let mut seen = [false; 4];
    for line in nsswitch.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        let Some((database, sources)) = line.split_once(':') else {
            continue;
        };
        let index = match database.trim() {
            "passwd" => 0,
            "group" => 1,
            // Supplementary memberships can come from here as well; when it
            // is stated it must be local too, and an absent line follows
            // `group`.
            "initgroups" => 2,
            // The subordinate id provider must be local too (`/etc/subgid`).
            "subid" => 3,
            _ => continue,
        };
        seen[index] = true;
        // `[NOTFOUND=return]` style actions are not sources.
        let local = sources
            .split_whitespace()
            .filter(|source| !source.starts_with('['))
            .all(|source| matches!(source, "files" | "systemd"));
        if !local {
            return false;
        }
    }
    seen[0] && seen[1]
}

/// Whether `gid` falls inside any subordinate-gid range of `/etc/subgid`
/// (`name:start:count`). A user namespace can map such a gid for another
/// account, so it is not private. Unreadable records count as covering.
#[cfg(unix)]
fn gid_is_delegated(subgid: &str, gid: u32) -> bool {
    subgid.lines().any(|line| {
        if line.trim().is_empty() || line.starts_with('#') {
            return false;
        }
        let fields: Vec<&str> = line.split(':').collect();
        match (
            fields.get(1).and_then(|start| start.parse::<u64>().ok()),
            fields.get(2).and_then(|count| count.parse::<u64>().ok()),
        ) {
            (Some(start), Some(count)) if fields.len() == 3 => {
                (start..start.saturating_add(count)).contains(&u64::from(gid))
            }
            _ => true,
        }
    })
}

/// The gid of `uid`'s private group, from the text of `/etc/passwd` and
/// `/etc/group`, or `None`.
///
/// Debian, Ubuntu and Fedora give every account a group of its own, named
/// after it, as its primary group, and pair that with umask 002, so `$HOME`
/// and `~/.cache` are routinely `user:user 0775`. Group write there grants
/// nobody but the user, and refusing it refused the per-user release cache
/// for good. The rule is deliberately narrow: the group must be the user's
/// primary group, carry the user's own name, list no member, and be the
/// primary group of no other account. Anything unproven is shared.
#[cfg(unix)]
fn private_group_in(passwd: &str, group: &str, uid: u32) -> Option<u32> {
    let mut user = None;
    let mut others_with_gid = Vec::new();
    let mut names = std::collections::HashSet::new();
    for line in passwd.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        // A record that cannot be read could be the account that shares the
        // gid, so any malformed line makes the whole answer unavailable.
        if fields.len() < 4 {
            return None;
        }
        let (Ok(entry_uid), Ok(entry_gid)) = (fields[2].parse::<u32>(), fields[3].parse::<u32>())
        else {
            return None;
        };
        // A second record under the same name would resolve to this group's
        // member entry as well.
        if !names.insert(fields[0]) {
            return None;
        }
        if entry_uid == uid {
            if user.is_some() {
                return None;
            }
            user = Some((fields[0], entry_gid));
        } else {
            others_with_gid.push(entry_gid);
        }
    }
    let (user_name, user_gid) = user?;
    if user_name.is_empty() || others_with_gid.contains(&user_gid) {
        return None;
    }
    let mut records = Vec::new();
    for line in group.lines() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split(':').collect();
        // As for passwd: a record that cannot be read could share the gid.
        if fields.len() < 4 || fields[2].parse::<u32>().is_err() {
            return None;
        }
        records.push(fields);
    }
    let mut matches = records
        .into_iter()
        .filter(|fields| fields[2].parse::<u32>().ok() == Some(user_gid));
    let entry = matches.next()?;
    if matches.next().is_some() || entry[0] != user_name {
        return None;
    }
    // Supplementary members can write through the group too.
    let members_ok = entry[3]
        .split(',')
        .filter(|member| !member.is_empty())
        .all(|member| member == user_name);
    members_ok.then_some(user_gid)
}

/// Where nss-systemd reads user and group records from.
#[cfg(unix)]
const SYSTEMD_USER_RECORD_DIRECTORIES: &[&str] = &[
    "/etc/userdb",
    "/run/userdb",
    "/run/systemd/userdb",
    "/run/host/userdb",
    "/usr/lib/userdb",
    "/var/lib/systemd/home",
    "/run/systemd/home",
];

/// A missing directory has no entries; one that cannot be read is assumed to.
#[cfg(unix)]
fn directory_has_no_entries(path: &Path) -> bool {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_none(),
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    }
}

/// The current user's private group. Reads only the local account files and
/// only when `nsswitch.conf` shows no directory service, so no NSS call is
/// made, nothing blocks on the network, and no process-wide libc cursor is
/// shared. Any doubt answers `None`, which keeps group write refused.
#[cfg(unix)]
fn current_user_private_gid(uid: u32) -> Option<u32> {
    let read = |path| std::fs::read_to_string(path).ok();
    let nsswitch = read("/etc/nsswitch.conf")?;
    if !accounts_are_local(&nsswitch) {
        return None;
    }
    let mut gid = private_group_in(&read("/etc/passwd")?, &read("/etc/group")?, uid);
    // A gid delegated through /etc/subgid can be mapped by another account.
    if let Some(candidate) = gid {
        match std::fs::read_to_string("/etc/subgid") {
            Ok(subgid) if gid_is_delegated(&subgid, candidate) => gid = None,
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => gid = None,
            _ => {}
        }
    }
    // `nss-systemd` serves dynamic users, whose gids are in reserved ranges,
    // and user records (`memberOf` can add supplementary groups) from the
    // userdb and homed directories. Trust it only for a regular-user gid with
    // no such records on disk.
    if nsswitch.contains("systemd")
        && !(gid.is_some_and(|gid| (1_000..60_000).contains(&gid))
            && SYSTEMD_USER_RECORD_DIRECTORIES
                .iter()
                .all(|directory| directory_has_no_entries(Path::new(directory))))
    {
        gid = None;
    }
    tracing::debug!(
        uid,
        private = gid.is_some(),
        "module directory gate checked the user's primary group"
    );
    gid
}

/// Whether a directory carries a POSIX ACL beyond its mode bits, which could
/// grant another account write access the mode and group do not show. An
/// unreadable answer counts as an ACL.
#[cfg(target_os = "linux")]
fn has_extended_acl(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return true;
    };
    // SAFETY: both pointers are valid for the call; a null buffer with size 0
    // only asks for the attribute's length.
    let length = unsafe {
        libc::getxattr(
            path.as_ptr(),
            c"system.posix_acl_access".as_ptr(),
            std::ptr::null_mut(),
            0,
        )
    };
    if length >= 0 {
        return true;
    }
    !matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ENODATA) | Some(libc::ENOTSUP)
    )
}

/// Other unixes: without a probe for ACLs, the private-group exception is off.
#[cfg(all(unix, not(target_os = "linux")))]
fn has_extended_acl(_path: &Path) -> bool {
    true
}

/// Whether group write on a root-owned directory grants nothing beyond root.
///
/// macOS only. It ships `/Applications` as `root:admin 0775`, and refusing it
/// refused every module bundled in an app installed the normal way. An `admin`
/// member who can rewrite an app there can equally replace the host's own
/// executable, which runs before any module gate, so refusing the module
/// protects nothing the host's code is not already exposed to. `wheel` (gid
/// 0) holds only root on macOS. Elsewhere gid 0 is not guaranteed to be
/// root-only and no installer needs it, so group write stays refused.
#[cfg(unix)]
fn root_equivalent_group(owner: u32, group: u32) -> bool {
    #[cfg(target_os = "macos")]
    {
        /// `wheel` and `admin`.
        const TRUSTED_GIDS: [u32; 2] = [0, 80];
        owner == 0 && TRUSTED_GIDS.contains(&group)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (owner, group);
        false
    }
}

#[cfg(windows)]
fn check_directory(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?;
    if !metadata.file_type().is_dir() {
        return Err(Error::module_refused(
            path,
            "module search path is not a directory",
        ));
    }
    if windows_path_grants_untrusted_write(path)? {
        return Err(Error::module_refused(
            path,
            "module directory is writable by another user",
        ));
    }
    Ok(())
}

/// `S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464`
/// (NT SERVICE\TrustedInstaller) in binary SID form.
#[cfg(windows)]
fn trusted_installer_sid() -> Vec<u32> {
    const SUB_AUTHORITIES: [u32; 6] = [
        80,
        956_008_885,
        3_418_522_649,
        1_831_038_044,
        1_853_292_631,
        2_271_478_464,
    ];
    // Revision 1, six sub-authorities, NT authority (5) big-endian.
    let mut bytes = vec![1u8, SUB_AUTHORITIES.len() as u8, 0, 0, 0, 0, 0, 5];
    for value in SUB_AUTHORITIES {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    // Held in `u32`s so the SID is aligned the way the API expects.
    bytes
        .chunks_exact(4)
        .map(|chunk| u32::from_ne_bytes(chunk.try_into().unwrap()))
        .collect()
}

#[cfg(windows)]
pub(super) fn windows_path_grants_untrusted_write(path: &Path) -> Result<bool> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;

    #[repr(C)]
    struct Acl {
        revision: u8,
        sbz1: u8,
        size: u16,
        ace_count: u16,
        sbz2: u16,
    }
    #[repr(C)]
    struct AceHeader {
        ace_type: u8,
        ace_flags: u8,
        ace_size: u16,
    }
    #[repr(C)]
    struct AccessAllowedAce {
        header: AceHeader,
        mask: u32,
        sid_start: u32,
    }
    #[repr(C)]
    struct SidAndAttributes {
        sid: *mut c_void,
        attributes: u32,
    }

    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn GetNamedSecurityInfoW(
            name: *mut u16,
            object_type: u32,
            security_info: u32,
            owner: *mut *mut c_void,
            group: *mut *mut c_void,
            dacl: *mut *mut Acl,
            sacl: *mut *mut Acl,
            descriptor: *mut *mut c_void,
        ) -> u32;
        fn GetAce(acl: *const Acl, index: u32, ace: *mut *mut c_void) -> i32;
        fn EqualSid(first: *const c_void, second: *const c_void) -> i32;
        fn CreateWellKnownSid(
            kind: u32,
            domain: *const c_void,
            sid: *mut c_void,
            size: *mut u32,
        ) -> i32;
        fn OpenProcessToken(process: *mut c_void, access: u32, token: *mut *mut c_void) -> i32;
        fn GetTokenInformation(
            token: *mut c_void,
            class: u32,
            information: *mut c_void,
            length: u32,
            returned_length: *mut u32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
        fn GetCurrentProcess() -> *mut c_void;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    const SE_FILE_OBJECT: u32 = 1;
    const OWNER_SECURITY_INFORMATION: u32 = 0x1;
    const DACL_SECURITY_INFORMATION: u32 = 0x4;
    const WIN_CREATOR_OWNER_SID: u32 = 3;
    const WIN_LOCAL_SYSTEM_SID: u32 = 22;
    const WIN_BUILTIN_ADMINISTRATORS_SID: u32 = 26;
    const TOKEN_QUERY: u32 = 0x8;
    const TOKEN_USER: u32 = 1;

    let mut wide = path
        .as_os_str()
        .encode_wide()
        .chain([0])
        .collect::<Vec<_>>();
    let mut owner = std::ptr::null_mut();
    let mut dacl = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide.as_mut_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 || descriptor.is_null() || owner.is_null() {
        if !descriptor.is_null() {
            unsafe { LocalFree(descriptor) };
        }
        return Err(Error::module_refused(
            path,
            "module directory ACL is unavailable",
        ));
    }

    let result = (|| {
        if dacl.is_null() {
            return true;
        }
        let mut admin_sid = [0u32; 17];
        let mut admin_len = size_of_val(&admin_sid) as u32;
        let mut system_sid = [0u32; 17];
        let mut system_len = size_of_val(&system_sid) as u32;
        let mut creator_owner_sid = [0u32; 17];
        let mut creator_owner_len = size_of_val(&creator_owner_sid) as u32;
        if unsafe {
            CreateWellKnownSid(
                WIN_BUILTIN_ADMINISTRATORS_SID,
                std::ptr::null(),
                admin_sid.as_mut_ptr().cast(),
                &mut admin_len,
            )
        } == 0
            || unsafe {
                CreateWellKnownSid(
                    WIN_LOCAL_SYSTEM_SID,
                    std::ptr::null(),
                    system_sid.as_mut_ptr().cast(),
                    &mut system_len,
                )
            } == 0
            || unsafe {
                CreateWellKnownSid(
                    WIN_CREATOR_OWNER_SID,
                    std::ptr::null(),
                    creator_owner_sid.as_mut_ptr().cast(),
                    &mut creator_owner_len,
                )
            } == 0
        {
            return true;
        }
        let mut token = std::ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return true;
        }
        let mut user_len = 0;
        unsafe { GetTokenInformation(token, TOKEN_USER, std::ptr::null_mut(), 0, &mut user_len) };
        if (user_len as usize) < size_of::<SidAndAttributes>() {
            unsafe { CloseHandle(token) };
            return true;
        }
        let mut user = vec![0usize; (user_len as usize).div_ceil(size_of::<usize>())];
        let user_read = unsafe {
            GetTokenInformation(
                token,
                TOKEN_USER,
                user.as_mut_ptr().cast(),
                user_len,
                &mut user_len,
            )
        };
        unsafe { CloseHandle(token) };
        if user_read == 0 {
            return true;
        }
        let user_sid = unsafe { (*user.as_ptr().cast::<SidAndAttributes>()).sid };
        if user_sid.is_null() {
            return true;
        }
        // An owner can rewrite the DACL even without an explicit write ACE.
        // Module files may inherit CREATOR OWNER from their parent, so their
        // actual owner must be trusted before that placeholder can be accepted.
        // NT SERVICE\TrustedInstaller, the OS servicing identity. Every folder
        // under `C:\Program Files` inherits Full Control for it, so a
        // per-machine (MSI) install was refused for it. It is no more another
        // user than SYSTEM is. A service SID has no `CreateWellKnownSid` kind,
        // so it is built from its fixed value.
        let trusted_installer_sid = trusted_installer_sid();
        let trusted_installer = trusted_installer_sid.as_ptr().cast::<c_void>();
        let trusted_owner = unsafe { EqualSid(owner, user_sid) } != 0
            || unsafe { EqualSid(owner, admin_sid.as_ptr().cast()) } != 0
            || unsafe { EqualSid(owner, system_sid.as_ptr().cast()) } != 0
            || unsafe { EqualSid(owner, trusted_installer) } != 0;
        if !trusted_owner {
            return true;
        }
        let ace_count = unsafe { (*dacl).ace_count };
        for index in 0..u32::from(ace_count) {
            let mut ace = std::ptr::null_mut();
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
                return true;
            }
            let ace = ace.cast::<AccessAllowedAce>();
            let ace_type = unsafe { (*ace).header.ace_type };
            let ace_flags = unsafe { (*ace).header.ace_flags };
            let mask = unsafe { (*ace).mask };
            // Whether an ACE counts is `windows_acl::ace_grants_untrusted_write`:
            // inherit-only entries seed children (each module file is checked
            // against its own ACL), and entries without a write right or that
            // are not allow-ACEs grant nothing here.
            let sid = unsafe { std::ptr::addr_of!((*ace).sid_start) }.cast();
            let principal_trusted = unsafe { EqualSid(sid, user_sid) } != 0
                || unsafe { EqualSid(sid, admin_sid.as_ptr().cast()) } != 0
                || unsafe { EqualSid(sid, system_sid.as_ptr().cast()) } != 0
                || unsafe { EqualSid(sid, trusted_installer) } != 0
                // CREATOR OWNER is an inheritable placeholder for the owner
                // of each child. Check each module file's actual owner and
                // ACL too, before accepting this ACE on its parent directory.
                || unsafe { EqualSid(sid, creator_owner_sid.as_ptr().cast()) } != 0;
            if super::windows_acl::ace_grants_untrusted_write(
                ace_type,
                ace_flags,
                mask,
                principal_trusted,
            ) {
                return true;
            }
        }
        false
    })();
    unsafe { LocalFree(descriptor) };
    Ok(result)
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "host_admission_tests.rs"]
mod admission_tests;

#[path = "host_callbacks.rs"]
pub(crate) mod callbacks;

#[cfg(test)]
#[path = "host_required_tests.rs"]
mod required_tests;

#[cfg(test)]
#[path = "host_reload_tests.rs"]
mod reload_tests;
