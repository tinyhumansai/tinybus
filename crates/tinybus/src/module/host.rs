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
use crate::module::abi::{TB_OK, TbAbiDescriptor, TbModuleVtable, field_bytes};
use crate::module::loader::{self, LoadedArtifact};
use crate::module::manifest::ModuleManifest;
use crate::module::transport::ModuleTransport;
use crate::ports::Transport;
use crate::version::Version;

/// Current lifecycle state of a discovered module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleState {
    /// ABI-gated, initialized, and attached to the broker.
    Loaded,
    /// Explicitly stopped. Its library remains mapped until process exit.
    Stopped,
    /// Initialization or runtime setup failed.
    Failed,
    /// Discovery found the artifact but admission refused it.
    Rejected,
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
    /// Current lifecycle state.
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
}

/// Loads trusted cdylib modules into one embedded broker.
pub struct ModuleHost {
    inner: Arc<ModuleHostInner>,
}

struct ModuleHostInner {
    broker: Broker,
    strict: AtomicBool,
    loaded: Mutex<Vec<LoadedModule>>,
    directories: Mutex<Vec<PathBuf>>,
    warned: AtomicBool,
}

/// The broker's private control hook. Kept behind a weak pointer so an unused
/// broker does not keep a module host alive.
pub(crate) trait ModuleControl: Send + Sync {
    fn list(&self) -> Vec<ModuleInfo>;
    fn load(self: Arc<Self>, path: PathBuf, config: serde_json::Value) -> Result<ModuleInfo>;
    fn stop(&self, name: &str, deadline: Duration) -> Result<ModuleInfo>;
    fn enable(&self, name: &str, enabled: bool) -> Result<ModuleInfo>;
    fn rescan(self: Arc<Self>) -> Result<Vec<ModuleInfo>>;
}

impl ModuleHost {
    /// Create a host. Loading is permissive about rustc drift by default.
    pub fn new(broker: Broker) -> Self {
        let inner = Arc::new(ModuleHostInner {
            broker: broker.clone(),
            strict: AtomicBool::new(false),
            loaded: Mutex::new(Vec::new()),
            directories: Mutex::new(Vec::new()),
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

    /// Snapshot all admitted modules.
    pub fn list(&self) -> Vec<ModuleInfo> {
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.info.clone())
            .collect()
    }

    /// Load one newly installed module.
    pub fn load_file(&self, path: impl AsRef<Path>) -> Result<ModuleInfo> {
        self.load_file_with_config(path, serde_json::json!({}))
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

        let mut outcomes = Vec::new();
        let mut pending = Vec::new();
        for path in paths {
            match check_file(&path).and_then(|()| loader::load(&path)) {
                Ok(artifact) => pending.push((path, artifact)),
                Err(error) => outcomes.push(Err(error)),
            }
        }

        let duplicate_names = duplicate_module_names(&pending);
        pending.retain(|(path, artifact)| {
            if duplicate_names.contains(&artifact.manifest.name) {
                outcomes.push(Err(Error::module_refused(
                    path,
                    "two artifacts declare the same module name",
                )));
                false
            } else {
                true
            }
        });

        let declared_providers: HashSet<String> = pending
            .iter()
            .flat_map(|(_, artifact)| artifact.manifest.provides.iter().cloned())
            .collect();
        let mut available = self.provided_interfaces();
        while !pending.is_empty() {
            let ready = pending.iter().position(|(_, artifact)| {
                artifact
                    .manifest
                    .requires
                    .iter()
                    .all(|dependency| available.contains(&dependency.interface))
            });
            if let Some(index) = ready {
                let (path, artifact) = pending.remove(index);
                let provides = artifact.manifest.provides.clone();
                let result = self.activate(&path, artifact, serde_json::json!({}));
                if result.is_ok() {
                    available.extend(provides);
                }
                outcomes.push(result);
                continue;
            }

            for (path, artifact) in pending.drain(..) {
                let missing = artifact.manifest.requires.iter().any(|dependency| {
                    !available.contains(&dependency.interface)
                        && !declared_providers.contains(&dependency.interface)
                });
                outcomes.push(Err(Error::module_refused(
                    &path,
                    if missing {
                        "a required interface has no provider"
                    } else {
                        "module dependency cycle detected"
                    },
                )));
            }
        }
        Ok(outcomes)
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
        let admitted = self.validate(path, &artifact.descriptor, &artifact.manifest)?;
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
        let mut module_vtable = TbModuleVtable::default();
        let code = unsafe { (artifact.init)(&host_vtable, &mut module_vtable) };
        if code != TB_OK {
            return Err(Error::module_refused(path, "module initialization failed"));
        }
        transport
            .initialize(module_vtable)
            .map_err(|_| Error::module_refused(path, "module returned an invalid vtable"))?;

        let transport_for_broker: Arc<dyn Transport> = transport.clone();
        self.inner.broker.attach(transport_for_broker);
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
            return Err(refuse("tinybus version is incompatible"));
        }
        if descriptor.tinybus_feature_bits & !build_info::FEATURE_BITS != 0 {
            return Err(refuse("module requires unavailable tinybus features"));
        }

        let rustc = sanitized_field(&descriptor.rustc_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let rustc_mismatch =
            field_bytes(&descriptor.rustc_version) != build_info::RUSTC_VERSION.as_bytes();
        if self.inner.strict.load(Ordering::Acquire) && rustc_mismatch {
            return Err(refuse("rustc version does not match in strict mode"));
        }
        if rustc_mismatch {
            tracing::warn!(module = %sanitize_untrusted(&manifest.name), "module rustc differs from host");
        }

        let name = sanitized_field(&descriptor.module_name)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        let version = sanitized_field(&descriptor.module_version)
            .ok_or_else(|| refuse("descriptor identity is invalid"))?;
        if sanitize_untrusted(&manifest.name) != name
            || sanitize_untrusted(&manifest.version) != version
            || manifest.name.len() > 64
            || manifest.version.len() > 32
        {
            return Err(refuse("manifest identity does not match descriptor"));
        }

        Ok(ModuleInfo {
            name,
            version,
            file: safe_file_name(path),
            state: ModuleState::Loaded,
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
            .any(|dependency| !available.contains(&dependency.interface))
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
            .flat_map(|module| module.info.manifest.provides.iter().cloned())
            .collect()
    }
}

impl ModuleControl for ModuleHostInner {
    fn list(&self) -> Vec<ModuleInfo> {
        self.loaded
            .lock()
            .expect("module list lock")
            .iter()
            .map(|module| module.info.clone())
            .collect()
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
        Ok(module.info.clone())
    }

    fn rescan(self: Arc<Self>) -> Result<Vec<ModuleInfo>> {
        let directories = self
            .directories
            .lock()
            .expect("module directory lock")
            .clone();
        let host = ModuleHost { inner: self };
        let mut loaded = Vec::new();
        for directory in directories {
            for outcome in host.load_dir(directory)? {
                match outcome {
                    Ok(info) => loaded.push(info),
                    Err(error) => tracing::warn!(error = %error, "module refused during rescan"),
                }
            }
        }
        Ok(loaded)
    }
}

fn duplicate_module_names(pending: &[(PathBuf, LoadedArtifact)]) -> HashSet<String> {
    let mut counts = HashMap::new();
    for (_, artifact) in pending {
        *counts
            .entry(artifact.manifest.name.clone())
            .or_insert(0usize) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(name, count)| (count > 1).then_some(name))
        .collect()
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
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::module_refused(path, "artifact metadata is unavailable"))?;
    if !metadata.file_type().is_file() {
        return Err(Error::module_refused(
            path,
            "artifact is not a regular file",
        ));
    }
    if !has_library_extension(path) {
        return Err(Error::module_refused(
            path,
            "artifact extension is not loadable",
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

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::module_refused(path, "module directory is unavailable"))?;
    if !metadata.file_type().is_dir() {
        return Err(Error::module_refused(
            path,
            "module search path is not a directory",
        ));
    }
    if metadata.uid() != unsafe { getuid() } {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use crate::module::abi::TbAbiDescriptor;
    use crate::transport::memory::MemoryBus;

    fn manifest() -> ModuleManifest {
        ModuleManifest {
            name: "clock".to_string(),
            version: "0.1.0".to_string(),
            provides: vec![],
            requires: vec![],
            optional: vec![],
            lazy: false,
        }
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

        let stopped: ModuleInfo = control
            .call("StopModule", ("tinybus", 1_000u64))
            .await
            .unwrap();
        assert_eq!(stopped.state, ModuleState::Stopped);
        let state_change = tokio::time::timeout(Duration::from_secs(2), state_changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state_change.body["state"], "stopped");
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
