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
use crate::module::abi::{TB_OK, TbAbiDescriptor, TbModuleInit, TbModuleVtable, field_bytes};
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
    transition_from: Option<ModuleState>,
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
    strict: AtomicBool,
    // Admission spans duplicate validation through final insertion; a narrower
    // lock lets two concurrent loads both pass the name check.
    admission: Mutex<()>,
    loaded: Mutex<Vec<LoadedModule>>,
    rejected: Mutex<Vec<ModuleInfo>>,
    directories: Mutex<Vec<PathBuf>>,
    configs: Mutex<HashMap<String, serde_json::Value>>,
    warned: AtomicBool,
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
    async fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
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
            strict: AtomicBool::new(false),
            admission: Mutex::new(()),
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
        let artifact = LoadedArtifact {
            descriptor,
            manifest,
            init,
        };
        self.activate(file.as_ref(), artifact, config)
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
            let artifact = loader::load(path, self.inner.strict.load(Ordering::Acquire))?;
            let rejected_manifest = artifact.manifest.clone();
            if let Err(error) = self.ensure_dependencies(&artifact.manifest, path) {
                self.record_manifest_rejection(&error, rejected_manifest, RefusalClass::Unresolved);
                return Err(error);
            }
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
            match check_file(&path)
                .and_then(|()| loader::load(&path, self.inner.strict.load(Ordering::Acquire)))
            {
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
            let (path, artifact) = pending[index].take().expect("resolver index is valid");
            let error = Error::module_refused(&path, reason);
            self.record_manifest_rejection(&error, artifact.manifest, RefusalClass::Unresolved);
            outcomes.push(Err(error));
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
                .inspect_err(|error| {
                    self.record_manifest_rejection(
                        error,
                        artifact.manifest.clone(),
                        RefusalClass::Unresolved,
                    );
                })
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

    /// Load every existing directory in [`ModuleHost::search_paths`] order.
    pub fn load_search_paths(&self) -> Vec<Result<ModuleInfo>> {
        let mut outcomes = Vec::new();
        for path in Self::search_paths()
            .into_iter()
            .filter(|path| path.is_dir())
        {
            match self.load_dir(path) {
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
            let inspected = check_file(&path)
                .and_then(|()| loader::load(&path, self.inner.strict.load(Ordering::Acquire)))
                .and_then(|artifact| {
                    let info = self.validate(&path, &artifact.descriptor, &artifact.manifest)?;
                    Ok((artifact, info))
                });
            match inspected {
                Ok((artifact, info)) => admitted.push((artifact, info)),
                Err(error) => results.push(rejection_info(&error)),
            }
        }
        let manifests = admitted
            .iter()
            .map(|(artifact, _)| artifact.manifest.clone())
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
        let transports = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.transport.clone())
            .collect::<Vec<_>>();
        for transport in transports {
            let _ = tokio::task::spawn_blocking(move || transport.stop_sync(deadline)).await;
        }
        for module in self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter_mut()
        {
            module.transition_from = Some(module.snapshot().state);
            module.info.state = ModuleState::Stopped;
        }
    }

    fn activate(
        &self,
        path: &Path,
        artifact: LoadedArtifact,
        config: serde_json::Value,
    ) -> Result<ModuleInfo> {
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
                let error = Error::module_refused(path, "module initialization failed");
                self.record_manifest_rejection(&error, manifest, RefusalClass::Failed);
                return Err(error);
            }
            if transport.initialize(module_vtable).is_err() {
                let error = Error::module_refused(path, "module returned an invalid vtable");
                self.record_manifest_rejection(&error, manifest, RefusalClass::Failed);
                return Err(error);
            }
        }

        let transport_for_broker: Arc<dyn Transport> = transport.clone();
        let unique = self.inner.broker.attach(transport_for_broker);
        let reserved_change = match self
            .inner
            .broker
            .reserve_module_name(&unique, admitted.manifest.bus_name.clone())
        {
            Ok(change) => change,
            Err(_) => {
                let _ = transport.stop_sync(Duration::from_millis(0));
                let error = Error::module_refused(path, "module bus name is already owned");
                self.record_manifest_rejection(&error, manifest, RefusalClass::Unresolved);
                return Err(error);
            }
        };
        let broker = self.inner.broker.clone();
        let ready_transport = transport.clone();
        let module_name = admitted.name.clone();
        let previous_state = state_name(&admitted.state);
        let lazy_init = artifact.manifest.lazy_init;
        tokio::spawn(async move {
            if lazy_init {
                ready_transport.wait_initializing().await;
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
                transition_from: None,
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
        loader::gate_descriptor(path, descriptor, self.inner.strict.load(Ordering::Acquire))?;

        let rustc = sanitized_field(&descriptor.rustc_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let rustc_mismatch =
            field_bytes(&descriptor.rustc_version) != build_info::RUSTC_VERSION.as_bytes();
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

    async fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        let transport = {
            let mut loaded = self.loaded.lock().expect("module list lock");
            let module = loaded
                .iter_mut()
                .find(|module| module.info.name == name)
                .ok_or_else(|| Error::failed("module is not loaded"))?;
            let old = module.snapshot().state;
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
            module.transition_from = Some(old);
            module.transport.clone()
        };
        tokio::task::spawn_blocking(move || transport.stop_sync(deadline))
            .await
            .map_err(|_| Error::failed("module stop task was cancelled"))?;
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not loaded"))?;
        module.info.state = ModuleState::Stopped;
        Ok(module.info.clone())
    }

    fn enable(&self, name: &str, enabled: bool) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.name == name)
            .ok_or_else(|| Error::failed("module is not known"))?;
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
                ModuleHost::search_paths()
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

    fn peer_detached(&self, unique_name: &BusName) -> Option<(String, ModuleState, ModuleState)> {
        let mut loaded = self.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| &module.unique_name == unique_name)?;
        if let Some(old) = module.transition_from.take() {
            module.info.state = ModuleState::Stopped;
            return Some((module.info.name.clone(), old, ModuleState::Stopped));
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
    #[cfg(windows)]
    let file = std::fs::File::open(path)
        .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?;
    let metadata = file
        .metadata()
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
    check_allowlist(path, file)
}

fn check_allowlist(path: &Path, file: std::fs::File) -> Result<()> {
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
        if let Some(reason) = unix_directory_refusal(metadata.uid(), metadata.mode(), uid) {
            return Err(Error::module_refused(path, reason));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn unix_directory_refusal(owner: u32, mode: u32, current_uid: u32) -> Option<&'static str> {
    if owner != current_uid && owner != 0 {
        Some("module directory is owned by another user")
    } else if mode & 0o022 != 0 {
        Some("module directory is writable by another user")
    } else {
        None
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
    if windows_directory_grants_untrusted_write(path)? {
        return Err(Error::module_refused(
            path,
            "module directory is writable by another user",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn windows_directory_grants_untrusted_write(path: &Path) -> Result<bool> {
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
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }

    const SE_FILE_OBJECT: u32 = 1;
    const OWNER_SECURITY_INFORMATION: u32 = 0x1;
    const DACL_SECURITY_INFORMATION: u32 = 0x4;
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    const WIN_LOCAL_SYSTEM_SID: u32 = 22;
    const WIN_BUILTIN_ADMINISTRATORS_SID: u32 = 26;
    const WRITE_MASK: u32 =
        0x2 | 0x4 | 0x10 | 0x100 | 0x1_0000 | 0x4_0000 | 0x8_0000 | 0x1000_0000 | 0x4000_0000;

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
        {
            return true;
        }
        let ace_count = unsafe { (*dacl).ace_count };
        for index in 0..u32::from(ace_count) {
            let mut ace = std::ptr::null_mut();
            if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
                return true;
            }
            let ace = ace.cast::<AccessAllowedAce>();
            if unsafe { (*ace).header.ace_type } != ACCESS_ALLOWED_ACE_TYPE
                || unsafe { (*ace).mask } & WRITE_MASK == 0
            {
                continue;
            }
            let sid = unsafe { std::ptr::addr_of!((*ace).sid_start) }.cast();
            let trusted = unsafe { EqualSid(sid, owner) } != 0
                || unsafe { EqualSid(sid, admin_sid.as_ptr().cast()) } != 0
                || unsafe { EqualSid(sid, system_sid.as_ptr().cast()) } != 0;
            if !trusted {
                return true;
            }
        }
        false
    })();
    unsafe { LocalFree(descriptor) };
    Ok(result)
}

#[cfg(test)]
#[path = "host_test.rs"]
mod tests;
