//! Module admission, dependency ordering, attachment, and lifecycle.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::broker::Broker;
use crate::build_info;
use crate::error::{Error, Result, sanitize_untrusted};
use crate::module::abi::{
    ABI_MAGIC, ABI_REVISION, DESCRIPTOR_PREFIX_SIZE, MAX_DESCRIPTOR_SIZE, TB_OK, TbAbiDescriptor,
    TbModuleInit, TbModuleVtable, field_bytes,
};
use crate::module::loader::{self, LoadedArtifact};
use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy};
use crate::module::transport::ModuleTransport;
use crate::name::{BusName, ObjectPath};
use crate::ports::Transport;
use crate::version::Version;

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
    /// Toolchain recorded by the descriptor, sanitized for display.
    pub rustc_version: String,
    /// Whether this module's toolchain differs from the host.
    pub rustc_mismatch: bool,
    /// Whether discovery should admit this module on future scans.
    pub enabled: bool,
}

struct LoadedModule {
    info: ModuleInfo,
    transport: Arc<ModuleTransport>,
    unique_name: BusName,
}

impl LoadedModule {
    fn snapshot(&self) -> ModuleInfo {
        let mut info = self.info.clone();
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
        } else if self.transport.is_ready()
            && matches!(
                info.state,
                ModuleState::Resolved | ModuleState::Initializing
            )
        {
            info.state = ModuleState::Ready;
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
    strict: AtomicBool,
    loaded: Mutex<Vec<LoadedModule>>,
    rejected: Mutex<Vec<ModuleInfo>>,
    directories: Mutex<Vec<PathBuf>>,
    configs: Mutex<HashMap<String, serde_json::Value>>,
    warned: AtomicBool,
}

/// The broker's private control hook. Kept behind a weak pointer so an unused
/// broker does not keep a module host alive.
pub(crate) trait ModuleControl: Send + Sync {
    fn list(&self) -> Vec<ModuleInfo>;
    fn load(self: Arc<Self>, path: PathBuf, config: serde_json::Value) -> Result<ModuleInfo>;
    fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
    fn enable(&self, name: &str, enabled: bool) -> Result<ModuleInfo>;
    fn rescan(self: Arc<Self>, paths: Vec<PathBuf>, dry_run: bool) -> Result<Vec<ModuleInfo>>;
    fn peer_detached(&self, unique_name: &BusName) -> Option<(String, ModuleState, ModuleState)>;
    fn unavailable_for(&self, bus_name: &BusName) -> Option<Error>;
}

impl ModuleHost {
    /// Create a host. Loading is permissive about rustc drift by default.
    pub fn new(broker: Broker) -> Self {
        let inner = Arc::new(ModuleHostInner {
            broker: broker.clone(),
            strict: AtomicBool::new(false),
            loaded: Mutex::new(Vec::new()),
            rejected: Mutex::new(Vec::new()),
            directories: Mutex::new(Vec::new()),
            configs: Mutex::new(HashMap::new()),
            warned: AtomicBool::new(false),
        });
        let control: Arc<dyn ModuleControl> = inner.clone();
        broker.set_module_control(Arc::downgrade(&control));
        Self { inner }
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

    /// Load one newly installed module.
    pub fn load_file(&self, path: impl AsRef<Path>) -> Result<ModuleInfo> {
        self.load_file_with_config(path, serde_json::json!({}))
    }

    /// Admit and initialize an already-resolved module without calling the
    /// platform loader.
    ///
    /// This is the testable seam beneath `dlopen`: callers must ensure `init`
    /// and every pointer reachable through it remain valid for the process
    /// lifetime, exactly as the real loader does by leaking its handle.
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
        let rejected_manifest = manifest.clone();
        let artifact = LoadedArtifact {
            descriptor,
            manifest,
            init,
        };
        let result = self.activate(file.as_ref(), artifact, config);
        if let Err(error) = &result {
            self.record_manifest_rejection(error, rejected_manifest);
        }
        result
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
            let artifact = loader::load(path)?;
            self.ensure_dependencies(&artifact.manifest, path)?;
            self.activate(path, artifact, config)
        })();
        if let Err(error) = &result {
            self.record_rejection(error);
        }
        result
    }

    /// Discover and load every platform library in a private directory.
    ///
    /// Refusals are returned per artifact so one bad file cannot prevent the
    /// remaining modules from loading.
    pub fn load_dir(&self, directory: impl AsRef<Path>) -> Result<Vec<Result<ModuleInfo>>> {
        let directory = directory.as_ref();
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
            match check_file(&path).and_then(|()| loader::load(&path)) {
                Ok(artifact) => pending.push((path, artifact)),
                Err(error) => outcomes.push(Err(error)),
            }
        }

        let manifests = pending
            .iter()
            .map(|(_, artifact)| artifact.manifest.clone())
            .collect::<Vec<_>>();
        let resolution = crate::module::resolve::resolve(&manifests, &self.provided_interfaces());
        let mut pending = pending.into_iter().map(Some).collect::<Vec<_>>();
        for (index, reason) in resolution.unresolved {
            let (path, _) = pending[index].take().expect("resolver index is valid");
            outcomes.push(Err(Error::module_refused(&path, reason)));
        }
        for index in resolution.order {
            let (path, artifact) = pending[index].take().expect("resolver index is valid");
            let config = self
                .inner
                .configs
                .lock()
                .expect("module config lock")
                .get(&artifact.manifest.module.name)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let result = self
                .ensure_dependencies(&artifact.manifest, &path)
                .and_then(|()| self.activate(&path, artifact, config));
            outcomes.push(result);
        }
        for error in outcomes.iter().filter_map(|outcome| outcome.as_ref().err()) {
            self.record_rejection(error);
        }
        Ok(outcomes)
    }

    /// Search paths in precedence order for the current platform.
    pub fn search_paths() -> Vec<PathBuf> {
        let mut paths = std::env::var_os("OPENHUMAN_MODULE_PATH")
            .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .unwrap_or_default();
        #[cfg(target_os = "linux")]
        {
            if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
                paths.push(PathBuf::from(path).join("openhuman/modules"));
            } else if let Some(path) = std::env::var_os("HOME") {
                paths.push(PathBuf::from(path).join(".local/share/openhuman/modules"));
            }
            paths.push(PathBuf::from("/usr/lib/openhuman/modules"));
        }
        #[cfg(target_os = "macos")]
        paths.push(PathBuf::from("/usr/local/lib/openhuman/modules"));
        #[cfg(windows)]
        if let Some(path) = std::env::var_os("LOCALAPPDATA") {
            paths.push(PathBuf::from(path).join("openhuman/modules"));
        }
        paths
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
        Ok(paths
            .into_iter()
            .map(|path| {
                let inspected = check_file(&path)
                    .and_then(|()| loader::load(&path))
                    .and_then(|artifact| {
                        let mut info =
                            self.validate(&path, &artifact.descriptor, &artifact.manifest)?;
                        if let Err(error) = self.ensure_dependencies(&artifact.manifest, &path) {
                            info.state = ModuleState::Unresolved {
                                reason: error.to_string(),
                            };
                        }
                        Ok(info)
                    });
                match inspected {
                    Ok(info) => info,
                    Err(error) => rejection_info(&error),
                }
            })
            .collect())
    }

    /// Stop every module within the supplied deadline per module.
    pub async fn shutdown(&self, deadline: Duration) {
        let transports = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.transport.clone())
            .collect::<Vec<_>>();
        for transport in transports {
            let _ = tokio::task::spawn_blocking(move || transport.shutdown_sync(deadline)).await;
        }
        for module in self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter_mut()
        {
            module.info.state = ModuleState::Stopped;
        }
    }

    fn activate(
        &self,
        path: &Path,
        artifact: LoadedArtifact,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
        let mut admitted = self.validate(path, &artifact.descriptor, &artifact.manifest)?;
        if self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .any(|loaded| loaded.info.name == admitted.name)
        {
            return Err(Error::module_refused(path, "module name is already loaded"));
        }

        let config = serde_json::to_vec(&config)
            .map_err(|_| Error::module_refused(path, "module configuration is invalid"))?;
        let (transport, host_vtable) = ModuleTransport::new(admitted.name.clone(), config);
        if artifact.manifest.lazy_init {
            admitted.state = ModuleState::Resolved;
            transport.defer_initialize(artifact.init, host_vtable);
        } else {
            admitted.state = ModuleState::Initializing;
            let mut module_vtable = TbModuleVtable::default();
            let code = unsafe { (artifact.init)(&host_vtable, &mut module_vtable) };
            transport.clear_config();
            if code != TB_OK {
                return Err(Error::module_refused(path, "module initialization failed"));
            }
            transport
                .initialize(module_vtable)
                .map_err(|_| Error::module_refused(path, "module returned an invalid vtable"))?;
        }

        let transport_for_broker: Arc<dyn Transport> = transport.clone();
        let unique = self.inner.broker.attach(transport_for_broker);
        let reserved_change = if artifact.manifest.lazy_init {
            Some(
                self.inner
                    .broker
                    .reserve_module_name(&unique, admitted.manifest.bus_name.clone())?,
            )
        } else {
            None
        };
        let broker = self.inner.broker.clone();
        let ready_transport = transport.clone();
        let module_name = admitted.name.clone();
        let previous_state = state_name(&admitted.state);
        tokio::spawn(async move {
            ready_transport.wait_ready().await;
            if ready_transport.is_ready() {
                if let Some(change) = reserved_change {
                    broker.announce_name_change(change).await;
                }
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
        if !self.inner.warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                modules = 1,
                "in-process modules are inside the host trust boundary"
            );
        }
        tracing::info!(module = %admitted.name, "module loaded");
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .push(LoadedModule {
                info: admitted.clone(),
                transport,
                unique_name: unique,
            });
        Ok(admitted)
    }

    fn validate(
        &self,
        path: &Path,
        descriptor: &TbAbiDescriptor,
        manifest: &ModuleManifest,
    ) -> Result<ModuleInfo> {
        let refuse = |reason| Error::module_refused(path, reason);
        if descriptor.magic != ABI_MAGIC {
            return Err(refuse("ABI magic does not match"));
        }
        if descriptor.abi_revision != ABI_REVISION {
            return Err(refuse("ABI revision does not match"));
        }
        if !(DESCRIPTOR_PREFIX_SIZE..=MAX_DESCRIPTOR_SIZE).contains(&descriptor.descriptor_size) {
            return Err(refuse("descriptor size is invalid"));
        }
        if descriptor.descriptor_size < size_of::<TbAbiDescriptor>() as u32 {
            return Err(refuse("descriptor is too small"));
        }
        if descriptor.pointer_width != usize::BITS {
            return Err(refuse("pointer width does not match"));
        }
        let module_little_endian = descriptor.flags & (1 << 2) != 0;
        if module_little_endian != cfg!(target_endian = "little") {
            return Err(refuse("target endianness does not match"));
        }
        if field_bytes(&descriptor.target_triple) != build_info::TARGET.as_bytes() {
            return Err(refuse("target triple does not match"));
        }
        if descriptor.flags & 1 == 0 {
            return Err(refuse("module was built with panic abort"));
        }

        let host_version = Version::parse(crate::VERSION).expect("crate version is semver");
        let module_version = Version::new(
            descriptor.tinybus_major.into(),
            descriptor.tinybus_minor.into(),
            descriptor.tinybus_patch.into(),
        );
        if !host_version.compatible_series().accepts(&module_version) {
            return Err(Error::module_refused(
                path,
                format!(
                    "tinybus version is incompatible: host {host_version}, module {module_version}"
                ),
            ));
        }
        let missing_features = descriptor.tinybus_feature_bits & !build_info::FEATURE_BITS;
        if missing_features != 0 {
            let bit = 1u64 << missing_features.trailing_zeros();
            return Err(Error::module_refused(
                path,
                format!(
                    "module requires unavailable tinybus feature {}",
                    build_info::feature_name(bit)
                ),
            ));
        }

        let rustc = sanitized_field(&descriptor.rustc_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let rustc_mismatch =
            field_bytes(&descriptor.rustc_version) != build_info::RUSTC_VERSION.as_bytes();
        if self.inner.strict.load(Ordering::Acquire) && rustc_mismatch {
            return Err(refuse("rustc version does not match in strict mode"));
        }
        if rustc_mismatch {
            tracing::warn!(module = %sanitize_untrusted(&manifest.module.name), "module rustc differs from host");
        }

        let name = sanitized_field(&descriptor.module_name)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let version = sanitized_field(&descriptor.module_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        if manifest.schema != MANIFEST_SCHEMA
            || sanitize_untrusted(&manifest.module.name) != name
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
        if let Some(existing) = rejected.iter_mut().find(|known| known.file == info.file) {
            *existing = info;
        } else {
            rejected.push(info);
        }
    }

    fn record_manifest_rejection(&self, error: &Error, manifest: ModuleManifest) {
        let Error::ModuleRefused { file, reason } = error else {
            return;
        };
        let info = ModuleInfo {
            name: sanitize_untrusted(&manifest.module.name),
            version: sanitize_untrusted(&manifest.module.version.to_string()),
            file: file.clone(),
            state: ModuleState::Rejected {
                reason: reason.clone(),
            },
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

    fn load(self: Arc<Self>, path: PathBuf, config: serde_json::Value) -> Result<ModuleInfo> {
        ModuleHost { inner: self }.load_file_with_config(path, config)
    }

    fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not loaded"))?;
        let _ = module.transport.stop_sync(deadline);
        module.info.state = ModuleState::Stopped;
        Ok(module.info.clone())
    }

    fn enable(&self, name: &str, enabled: bool) -> Result<ModuleInfo> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not known"))?;
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
        Ok(module.snapshot())
    }

    fn rescan(self: Arc<Self>, paths: Vec<PathBuf>, dry_run: bool) -> Result<Vec<ModuleInfo>> {
        let directories = if paths.is_empty() {
            self.directories
                .lock()
                .expect("module directory lock")
                .clone()
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
        Ok(loaded)
    }

    fn peer_detached(&self, unique_name: &BusName) -> Option<(String, ModuleState, ModuleState)> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| &module.unique_name == unique_name)?;
        if !module.transport.is_faulted()
            || matches!(
                module.info.state,
                ModuleState::Stopped | ModuleState::Disabled
            )
        {
            return None;
        }
        let old = module.info.state.clone();
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
            .find(|module| &module.info.manifest.bus_name == bus_name)
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
    let metadata = {
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
            .and_then(|file| file.metadata())
            .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?
    };
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
    check_allowlist(path)
}

fn check_allowlist(path: &Path) -> Result<()> {
    let Some(directory) = path.parent() else {
        return Ok(());
    };
    let allowlist = directory.join("modules.toml");
    if !allowlist.exists() {
        return Ok(());
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
    let expected = source.lines().find_map(|line| {
        let line = line.split('#').next()?.trim();
        if line.is_empty() || line.starts_with('[') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        let key = key.trim().trim_matches(['"', '\'']);
        (key == file_name || key == file_stem)
            .then(|| value.trim().trim_matches(['"', '\'']).to_ascii_lowercase())
    });
    let Some(expected) = expected else {
        return Err(Error::module_refused(
            path,
            "artifact is absent from the module allowlist",
        ));
    };
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::module_refused(
            path,
            "module allowlist contains an invalid hash",
        ));
    }
    let file = std::fs::File::open(path)
        .map_err(|_| Error::module_refused(path, "artifact is unreadable"))?;
    let actual = crate::module::hash::file_hex(file)
        .map_err(|_| Error::module_refused(path, "artifact hash could not be read"))?;
    if actual != expected {
        return Err(Error::module_refused(
            path,
            "artifact hash does not match the module allowlist",
        ));
    }
    Ok(())
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

    unsafe extern "C" {
        fn getuid() -> u32;
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?
            .join(path)
    };
    let uid = unsafe { getuid() };
    for component in absolute.ancestors() {
        let metadata = std::fs::symlink_metadata(component)
            .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?;
        if !metadata.file_type().is_dir() {
            return Err(Error::module_refused(
                path,
                "module search path contains a non-directory component",
            ));
        }
        if metadata.uid() != uid && metadata.uid() != 0 {
            return Err(Error::module_refused(
                path,
                "module directory is owned by another user",
            ));
        }
        if metadata.mode() & 0o022 != 0 {
            return Err(Error::module_refused(
                path,
                "module directory is writable by another user",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use crate::module::abi::TbAbiDescriptor;
    use crate::transport::memory::MemoryBus;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static INIT_RAN: AtomicBool = AtomicBool::new(false);
    static LAZY_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
    static FAILED_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
    static FAKE_MODULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct FakeModule {
        tx: std::sync::mpsc::SyncSender<Vec<u8>>,
    }

    unsafe extern "C" fn fake_deliver(
        ctx: *mut std::ffi::c_void,
        ptr: *const u8,
        len: usize,
    ) -> i32 {
        if ctx.is_null() || ptr.is_null() {
            return crate::module::abi::TB_BAD_ARGUMENT;
        }
        let module = unsafe { &*ctx.cast::<FakeModule>() };
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        match module.tx.try_send(bytes) {
            Ok(()) => TB_OK,
            Err(std::sync::mpsc::TrySendError::Full(_)) => crate::module::abi::TB_BACKPRESSURE,
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => crate::module::abi::TB_CLOSED,
        }
    }

    unsafe extern "C" fn fake_shutdown(_: *mut std::ffi::c_void, _: u64) -> i32 {
        TB_OK
    }

    unsafe extern "C" fn lazy_echo_init(
        host: *const crate::module::abi::TbHostVtable,
        out: *mut TbModuleVtable,
    ) -> i32 {
        LAZY_INIT_COUNT.fetch_add(1, Ordering::AcqRel);
        let host = unsafe { *host };
        let host_ctx = host.host_ctx as usize;
        let send = host.send;
        let ready = host.ready;
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(8);
        std::thread::spawn(move || {
            unsafe { ready(host_ctx as *mut std::ffi::c_void) };
            while let Ok(bytes) = rx.recv() {
                let Ok(message) = serde_json::from_slice::<crate::Message>(&bytes) else {
                    continue;
                };
                if message.header.kind == crate::message::MessageKind::MethodCall {
                    let body = message
                        .body
                        .as_array()
                        .and_then(|values| values.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let reply = crate::Message::method_return(&message.header, body);
                    let bytes = serde_json::to_vec(&reply).expect("fake reply serializes");
                    let _ = unsafe {
                        send(
                            host_ctx as *mut std::ffi::c_void,
                            bytes.as_ptr(),
                            bytes.len(),
                        )
                    };
                }
            }
        });
        let module = Box::into_raw(Box::new(FakeModule { tx }));
        unsafe {
            *out = TbModuleVtable {
                size: size_of::<TbModuleVtable>() as u32,
                _reserved: 0,
                module_ctx: module.cast(),
                deliver: fake_deliver,
                shutdown: fake_shutdown,
            };
        }
        TB_OK
    }

    unsafe extern "C" fn failing_init(
        _: *const crate::module::abi::TbHostVtable,
        _: *mut TbModuleVtable,
    ) -> i32 {
        FAILED_INIT_COUNT.fetch_add(1, Ordering::AcqRel);
        crate::module::abi::TB_BAD_ARGUMENT
    }

    unsafe extern "C" fn init_that_must_not_run(
        _: *const crate::module::abi::TbHostVtable,
        _: *mut TbModuleVtable,
    ) -> i32 {
        INIT_RAN.store(true, Ordering::Release);
        TB_OK
    }

    fn manifest() -> ModuleManifest {
        named_manifest("clock", "Clock")
    }

    fn named_manifest(module_name: &str, surface_name: &str) -> ModuleManifest {
        ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: module_name.to_string(),
                version: Version::new(0, 1, 0),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(format!("ai.tinyhumans.module.{surface_name}")).unwrap(),
            object_path: ObjectPath::new(format!("/ai/tinyhumans/module/{surface_name}")).unwrap(),
            provides: vec![],
            requires: vec![],
            environment: vec![],
            capabilities: vec![],
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        }
    }

    #[tokio::test]
    async fn a_lazy_module_initializes_on_the_first_call_and_two_racing_callers_initialize_it_once()
    {
        let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
        LAZY_INIT_COUNT.store(0, Ordering::Release);
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let mut lazy_manifest = manifest();
        lazy_manifest.lazy_init = true;
        let info = unsafe {
            host.attach_raw(
                "clock.so",
                TbAbiDescriptor::current("clock", "0.1.0"),
                lazy_manifest,
                lazy_echo_init,
            )
        }
        .unwrap();
        assert_eq!(info.state, ModuleState::Resolved);
        assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 0);

        let connection = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let proxy = connection
            .proxy(
                "ai.tinyhumans.module.Clock",
                "/ai/tinyhumans/module/Clock",
                "ai.tinyhumans.module.Clock",
            )
            .unwrap();
        let first = proxy.call::<String>("Echo", ("first",));
        let second = proxy.call::<String>("Echo", ("second",));
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), "first");
        assert_eq!(second.unwrap(), "second");
        assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 1);
        assert_eq!(host.list()[0].state, ModuleState::Ready);
        broker_task.abort();
    }

    #[tokio::test]
    async fn a_module_whose_init_fails_is_terminal_and_is_never_initialized_again() {
        FAILED_INIT_COUNT.store(0, Ordering::Release);
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let mut lazy_manifest = manifest();
        lazy_manifest.lazy_init = true;
        unsafe {
            host.attach_raw(
                "clock.so",
                TbAbiDescriptor::current("clock", "0.1.0"),
                lazy_manifest,
                failing_init,
            )
        }
        .unwrap();
        let connection = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let proxy = connection
            .proxy(
                "ai.tinyhumans.module.Clock",
                "/ai/tinyhumans/module/Clock",
                "ai.tinyhumans.module.Clock",
            )
            .unwrap();
        let first = proxy.call::<()>("Call", ()).await.unwrap_err();
        assert_eq!(
            first.wire_name(),
            "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(host.list()[0].state, ModuleState::Failed { .. }) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = proxy.call::<()>("Call", ()).await.unwrap_err();
        assert_eq!(
            second.wire_name(),
            "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
        );
        assert_eq!(FAILED_INIT_COUNT.load(Ordering::Acquire), 1);
        broker_task.abort();
    }

    #[tokio::test]
    async fn a_call_to_a_rejected_module_names_the_state_rather_than_timing_out() {
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.magic = 0;
        unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) }
            .unwrap_err();

        let connection = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let proxy = connection
            .proxy(
                "ai.tinyhumans.module.Clock",
                "/ai/tinyhumans/module/Clock",
                "ai.tinyhumans.module.Clock",
            )
            .unwrap();
        let error = proxy.call::<()>("Call", ()).await.unwrap_err();
        assert_eq!(
            error.wire_name(),
            "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
        );
        assert!(error.to_string().contains("rejected"), "{error}");
        broker_task.abort();
    }

    #[tokio::test]
    async fn a_match_rule_on_a_lazy_modules_signal_does_not_initialize_it() {
        let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
        LAZY_INIT_COUNT.store(0, Ordering::Release);
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let mut lazy_manifest = manifest();
        lazy_manifest.lazy_init = true;
        unsafe {
            host.attach_raw(
                "clock.so",
                TbAbiDescriptor::current("clock", "0.1.0"),
                lazy_manifest,
                lazy_echo_init,
            )
        }
        .unwrap();
        let connection = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let _signals = connection
            .add_match(
                crate::router::MatchRule::parse("type=signal,sender=ai.tinyhumans.module.Clock")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(LAZY_INIT_COUNT.load(Ordering::Acquire), 0);
        broker_task.abort();
    }

    #[tokio::test]
    async fn stopping_one_module_leaves_the_other_serving() {
        let _test_guard = FAKE_MODULE_TEST_LOCK.lock().await;
        LAZY_INIT_COUNT.store(0, Ordering::Release);
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        for (module_name, surface_name) in [("one", "One"), ("two", "Two")] {
            let mut module_manifest = named_manifest(module_name, surface_name);
            module_manifest.lazy_init = true;
            unsafe {
                host.attach_raw(
                    format!("{module_name}.so"),
                    TbAbiDescriptor::current(module_name, "0.1.0"),
                    module_manifest,
                    lazy_echo_init,
                )
            }
            .unwrap();
        }
        let connection = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let proxy = |surface_name: &str| {
            connection
                .proxy(
                    format!("ai.tinyhumans.module.{surface_name}"),
                    format!("/ai/tinyhumans/module/{surface_name}"),
                    format!("ai.tinyhumans.module.{surface_name}"),
                )
                .unwrap()
        };
        assert_eq!(
            proxy("One").call::<String>("Echo", ("one",)).await.unwrap(),
            "one"
        );
        assert_eq!(
            proxy("Two").call::<String>("Echo", ("two",)).await.unwrap(),
            "two"
        );
        connection
            .stop_module("one", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            proxy("Two")
                .call::<String>("Echo", ("still serving",))
                .await
                .unwrap(),
            "still serving"
        );
        let stopped = proxy("One")
            .call::<()>("Echo", ("stopped",))
            .await
            .unwrap_err();
        assert_eq!(
            stopped.wire_name(),
            "ai.tinyhumans.tinybus.Error.ModuleUnavailable"
        );
        broker_task.abort();
    }

    #[test]
    fn a_module_compiled_with_panic_abort_is_refused_because_a_panic_would_kill_the_host() {
        let broker = Broker::new();
        let host = ModuleHost::new(broker);
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.flags &= !1;
        let error = host
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err();
        assert!(error.to_string().contains("panic abort"), "{error}");
    }

    #[test]
    fn a_descriptor_with_a_bad_magic_is_refused_without_reading_past_the_prefix() {
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.magic = 0;
        let error = host
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err();
        assert!(error.to_string().contains("magic"), "{error}");
    }

    #[test]
    fn a_module_built_for_an_older_abi_revision_is_refused_before_its_init_runs() {
        INIT_RAN.store(false, Ordering::Release);
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.abi_revision = 0;
        let error =
            unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) }
                .unwrap_err();
        assert!(error.to_string().contains("revision"), "{error}");
        assert!(!INIT_RAN.load(Ordering::Acquire));
    }

    #[test]
    fn a_descriptor_smaller_than_the_host_expects_is_refused_and_a_larger_one_is_accepted() {
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.descriptor_size = size_of::<TbAbiDescriptor>() as u32 - 1;
        assert!(
            host.validate(Path::new("clock.so"), &descriptor, &manifest())
                .is_err()
        );
        descriptor.descriptor_size = size_of::<TbAbiDescriptor>() as u32 + 64;
        assert!(
            host.validate(Path::new("clock.so"), &descriptor, &manifest())
                .is_ok()
        );
    }

    #[test]
    fn a_module_built_for_a_different_target_triple_is_refused() {
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.target_triple = [0; 64];
        descriptor.target_triple[..13].copy_from_slice(b"other-unknown");
        let error = host
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err();
        assert!(error.to_string().contains("target triple"), "{error}");
    }

    #[test]
    fn a_module_needing_a_feature_the_host_lacks_is_refused_and_names_the_feature() {
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.tinybus_feature_bits |= 1 << 63;
        let error = host
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err();
        assert!(
            error.to_string().contains("unavailable tinybus feature"),
            "{error}"
        );
    }

    #[test]
    fn a_module_built_against_an_incompatible_tinybus_is_refused_and_names_both_versions() {
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.tinybus_major = 99;
        let error = host
            .validate(Path::new("clock.so"), &descriptor, &manifest())
            .unwrap_err();
        let text = error.to_string();
        assert!(text.contains(crate::VERSION), "{text}");
        assert!(text.contains("99.1.0"), "{text}");
    }

    #[test]
    fn a_module_that_never_had_its_init_called_is_the_refusal_path() {
        INIT_RAN.store(false, Ordering::Release);
        let host = ModuleHost::new(Broker::new());
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.pointer_width = if usize::BITS == 64 { 32 } else { 64 };
        let _ =
            unsafe { host.attach_raw("clock.so", descriptor, manifest(), init_that_must_not_run) };
        assert!(!INIT_RAN.load(Ordering::Acquire));
    }

    #[test]
    fn a_module_built_by_a_different_rustc_is_refused_in_strict_mode_and_only_warned_about_otherwise()
     {
        let broker = Broker::new();
        let permissive = ModuleHost::new(broker.clone());
        let strict = ModuleHost::new(broker).strict(true);
        let mut descriptor = TbAbiDescriptor::current("clock", "0.1.0");
        descriptor.rustc_version = [0; 48];
        descriptor.rustc_version[..5].copy_from_slice(b"0.0.0");
        assert!(
            permissive
                .validate(Path::new("clock.so"), &descriptor, &manifest())
                .is_ok()
        );
        assert!(
            strict
                .validate(Path::new("clock.so"), &descriptor, &manifest())
                .unwrap_err()
                .to_string()
                .contains("strict mode")
        );
    }

    #[test]
    fn a_refusal_names_the_file_but_never_the_path_or_the_descriptor_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let extension = if cfg!(windows) {
            "dll"
        } else if cfg!(target_os = "macos") {
            "dylib"
        } else {
            "so"
        };
        let path = directory.path().join(format!("broken.{extension}"));
        std::fs::write(&path, b"not a dynamic library").unwrap();
        let host = ModuleHost::new(Broker::new());
        let error = host.load_file(&path).unwrap_err();
        assert!(
            !error
                .to_string()
                .contains(&directory.path().display().to_string())
        );
        let listed = host.list();
        assert_eq!(listed.len(), 1);
        assert!(matches!(
            listed[0].state,
            ModuleState::Rejected { ref reason } if !reason.is_empty()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn a_world_writable_module_directory_is_refused_before_any_dlopen() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let error = check_directory(directory.path()).unwrap_err();
        assert!(error.to_string().contains("writable by another user"));
    }

    #[test]
    fn a_file_that_is_not_a_regular_file_is_skipped() {
        let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        let error = check_file(directory.path()).unwrap_err();
        assert!(error.to_string().contains("not a regular file"));
    }

    #[test]
    fn an_allowlist_with_a_mismatched_hash_refuses_the_file() {
        let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
        let extension = if cfg!(windows) {
            "dll"
        } else if cfg!(target_os = "macos") {
            "dylib"
        } else {
            "so"
        };
        let path = directory.path().join(format!("clock.{extension}"));
        std::fs::write(&path, b"not a module").unwrap();
        std::fs::write(
            directory.path().join("modules.toml"),
            format!("clock.{extension} = \"{}\"\n", "0".repeat(64)),
        )
        .unwrap();
        let error = check_file(&path).unwrap_err();
        assert!(error.to_string().contains("hash does not match"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
    async fn a_real_cdylib_loads_and_serves_a_call() {
        let path = std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE");
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let task = broker.spawn(bus.clone());
        let modules = ModuleHost::new(broker);
        modules
            .load_file_with_config(path, serde_json::json!({ "prefix": "configured:" }))
            .unwrap();

        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client
                    .list_names()
                    .await
                    .unwrap()
                    .iter()
                    .any(|name| name.as_str() == "ai.tinyhumans.openhuman.Clock")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let clock = client
            .proxy(
                "ai.tinyhumans.openhuman.Clock",
                "/ai/tinyhumans/openhuman/Clock",
                "ai.tinyhumans.openhuman.Clock",
            )
            .unwrap();
        let value: String = clock.call("Now", ()).await.unwrap();
        assert!(value.starts_with("configured:"), "{value}");
        let control = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap();
        let listed: Vec<ModuleInfo> = control.call("ListModules", ()).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "tinybus");
        let mut state_changes = client
            .add_match(
                crate::router::MatchRule::parse(
                    "type=signal,interface=ai.tinyhumans.tinybus.Bus,member=ModuleStateChanged",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let mut name_changes = client
            .add_match(
                crate::router::MatchRule::parse(
                    "type=signal,interface=ai.tinyhumans.tinybus.Bus,member=NameOwnerChanged",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let panic_error = clock.call::<()>("Panic", ()).await.unwrap_err();
        let panic_text = panic_error.to_string();
        assert!(panic_text.contains("ModulePanicked"), "{panic_text}");
        assert!(panic_text.contains("module_clock.rs"), "{panic_text}");
        assert!(!panic_text.contains("secret-token"), "{panic_text}");
        let name_change = tokio::time::timeout(Duration::from_secs(2), name_changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(name_change.body[0], "ai.tinyhumans.openhuman.Clock");
        assert!(name_change.body[2].is_null());

        let stopped: ModuleInfo = control
            .call("StopModule", ("tinybus", 1_000u64))
            .await
            .unwrap();
        assert_eq!(stopped.state, ModuleState::Stopped);
        let state_change = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let message = state_changes.recv().await.unwrap();
                if message.header.member.as_ref().map(|member| member.as_str())
                    == Some("ModuleStateChanged")
                    && message.body.get(2).and_then(serde_json::Value::as_str) == Some("stopped")
                {
                    break message;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(state_change.body[0], "tinybus");
        assert_eq!(state_change.body[2], "stopped");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !client
                    .list_names()
                    .await
                    .unwrap()
                    .iter()
                    .any(|name| name.as_str() == "ai.tinyhumans.openhuman.Clock")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
    }

    #[test]
    #[ignore = "requires TINYBUS_TEST_MODULE and TINYBUS_TEST_MODULE_TWO"]
    fn two_modules_exporting_the_same_symbol_name_each_resolve_to_their_own() {
        let first =
            PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE"));
        let second = PathBuf::from(
            std::env::var_os("TINYBUS_TEST_MODULE_TWO").expect("TINYBUS_TEST_MODULE_TWO"),
        );
        let first = loader::load(&first).unwrap();
        let second = loader::load(&second).unwrap();
        assert_eq!(first.manifest.module.name, "tinybus");
        assert_eq!(second.manifest.module.name, "module-clock-two");
        assert_ne!(first.descriptor.module_name, second.descriptor.module_name);
    }

    #[test]
    #[ignore = "requires TINYBUS_TEST_WRONG_TARGET"]
    fn a_cdylib_built_for_a_different_target_is_refused_at_the_gate() {
        let path = PathBuf::from(
            std::env::var_os("TINYBUS_TEST_WRONG_TARGET").expect("TINYBUS_TEST_WRONG_TARGET"),
        );
        let host = ModuleHost::new(Broker::new());
        let error = host.load_file(path).unwrap_err();
        assert!(error.to_string().contains("target triple"), "{error}");
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
    // The loader uses LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR. ACL enforcement is
    // performed by the embedding application, which owns the install root.
    Ok(())
}
