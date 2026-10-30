//! ABI shared by module authors and hosts, plus the optional host loader.
//!
//! The ABI and manifest types are always compiled so a module can depend on
//! tinybus with no default features. Loading is behind `modules`: in-process
//! modules are trusted code with the host's full address-space privileges.
//! The coarse `Availability` status is available even when loading is disabled.

pub mod abi;
mod availability;
pub use availability::Availability;
#[cfg(feature = "modules")]
mod cache;
pub mod client;
pub mod failure;
#[cfg(feature = "modules")]
mod first_admitted;
#[cfg(feature = "modules")]
mod github;
mod hash;
pub mod lease;
#[cfg(feature = "modules")]
pub use lease::RuntimeLease;
pub use lease::{
    LeaseError, LeasePhase, LeaseReport, LeaseReportOutcome, RetryPolicy, RetryPolicyError,
    RuntimeLeaseDriver, StartOutcome, StopOutcome,
};
pub mod manifest;
pub mod platform;
#[cfg(feature = "modules")]
pub mod reconfigure;
mod release;
#[cfg(feature = "modules")]
pub mod resolution;
pub mod service_slot;
#[cfg(any(feature = "modules", feature = "cli"))]
mod zip_deflate;
#[cfg(any(feature = "modules", feature = "cli"))]
mod zip_descriptor;
#[cfg(any(feature = "modules", feature = "cli"))]
mod zip_extract;

#[cfg(feature = "modules")]
pub(crate) mod host;
#[cfg(feature = "modules")]
mod loader;
#[cfg(feature = "modules")]
mod remembered_hash;
#[cfg(feature = "modules")]
mod resolve;
#[cfg(feature = "modules")]
pub mod runtime;
#[cfg(feature = "modules")]
pub(crate) mod transport;
#[cfg(feature = "modules")]
mod windows_acl;

#[cfg(feature = "modules")]
pub use cache::{artifact_dir, is_safe_path_component, prune_stale_versions};
pub use client::{
    AccessFuture, BoundModuleClient, ModuleAccess, ModuleCallError, ModuleCallFailure,
    ModuleClient, ModuleObjectClient, PinnedModuleClient, PinnedModuleObjectClient, ProxyFailure,
    ReadinessFailure, ReconfigureFailure, UnloadedAccess, classify, report_proxy_failure,
    wire_kind,
};
pub use failure::{FailureReport, FailureReporter, FailureSink, Reason};
#[cfg(feature = "modules")]
pub use first_admitted::{ReleasePlan, load_first_admitted};
#[cfg(feature = "modules")]
pub use github::CachedRelease;
#[cfg(feature = "modules")]
pub use host::callbacks::{
    HostCallbackBinding, HostCallbackEndpoint, HostCallbackGuard, HostCallbackProxy,
    HostCallbackSignal, HostStreamCallbackEndpoint,
};
#[cfg(feature = "modules")]
pub use host::{
    ModuleHost, ModuleInfo, ModuleSenderLookup, ModuleState, PreReadyConfidentialRoute,
    SearchPathConfig,
};
#[cfg(feature = "modules")]
pub use reconfigure::{ConfigFingerprint, ReconfigurableModule, Reconfigure, ReconfigureGuard};
pub use release::{ReleaseAsset, ReleaseRecord};
#[cfg(feature = "modules")]
pub use resolve::{
    AsyncModuleConfigProvider, LinkedFactory, LinkedRegistry, LoadError, ModuleConfigProvider,
    ModuleGate, ModuleSource, ResolvedHook, Resolver, ResolverInputs, RuntimeFuture,
    RuntimeProvider, resolve_bundled_dir, set_bundled_releases_dir,
};
#[cfg(feature = "modules")]
pub use runtime::{
    HostObjectGuard, LinkedModuleInitWithSetup, LinkedSetup, ModuleRuntime, RuntimeNames,
    RuntimeOptions, RuntimeSetup,
};

/// Stable, validated identifier of one admitted module package.
///
/// This is independent of a module's display summary and bus aliases. It is
/// derived from the canonical module identity in the admitted manifest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModuleId(String);

impl ModuleId {
    #[cfg(feature = "modules")]
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    /// The canonical module package identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Entry points compiled into the host instead of loaded from a shared library.
///
/// A linked module is trusted as part of the host executable. The manifest and
/// descriptor still pass the ordinary TinyBus admission checks.
#[cfg(feature = "modules")]
pub struct LinkedModule {
    /// Descriptor compiled with the linked module.
    pub descriptor: abi::TbAbiDescriptor,
    /// Manifest exported by the linked module.
    pub manifest: manifest::ModuleManifest,
    /// Process-lifetime initialization entry point.
    pub init: abi::TbModuleInit,
    /// Rust-only initializer that accepts a per-attachment setup callback.
    /// Generated only for statically linked exports. Dynamic modules have no
    /// such hook and continue through the unchanged C ABI initializer.
    #[doc(hidden)]
    pub init_with_setup: Option<LinkedModuleInitWithSetup>,
}

#[cfg(feature = "modules")]
impl LinkedModule {
    /// Read the manifest exported by statically linked module code.
    ///
    /// # Errors
    /// Returns an error for an incompatible descriptor or malformed manifest.
    ///
    /// # Safety
    /// The exported function must return readable bytes for the duration of
    /// this call, and `init` and its reachable code must live until process exit.
    pub unsafe fn from_exports(
        descriptor: &abi::TbAbiDescriptor,
        manifest: unsafe extern "C" fn() -> abi::TbSlice,
        init: abi::TbModuleInit,
    ) -> crate::Result<Self> {
        loader::gate_descriptor(std::path::Path::new("linked"), descriptor, false)?;
        let slice = unsafe { manifest() };
        if slice.ptr.is_null() || slice.len > 1024 * 1024 {
            return Err(crate::Error::failed(
                "linked module manifest bytes are invalid",
            ));
        }
        let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
        let manifest = serde_json::from_slice(bytes)
            .map_err(|_| crate::Error::failed("linked module manifest is invalid"))?;
        Ok(Self {
            descriptor: *descriptor,
            manifest,
            init,
            init_with_setup: None,
        })
    }

    /// Construct a linked export with a Rust-only per-instance setup entrypoint.
    ///
    /// # Errors
    /// Returns an error for an incompatible descriptor or malformed manifest.
    ///
    /// # Safety
    /// The safety requirements are identical to [`Self::from_exports`], and
    /// `init_with_setup` must be generated for this exact linked module.
    #[doc(hidden)]
    pub unsafe fn from_exports_with_setup(
        descriptor: &abi::TbAbiDescriptor,
        manifest: unsafe extern "C" fn() -> abi::TbSlice,
        init: abi::TbModuleInit,
        init_with_setup: LinkedModuleInitWithSetup,
    ) -> crate::Result<Self> {
        let mut module = unsafe { Self::from_exports(descriptor, manifest, init)? };
        module.init_with_setup = Some(init_with_setup);
        Ok(module)
    }
}

/// Compute the lowercase SHA-256 digest of a release asset.
pub fn sha256_file(path: impl AsRef<std::path::Path>) -> crate::Result<String> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .map_err(|_| crate::Error::failed("release asset could not be opened"))?;
    hash::file_hex(file).map_err(|_| crate::Error::failed("release asset could not be hashed"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(feature = "modules")]
mod requirements;
/// Host-selected activation requirements.
#[cfg(feature = "modules")]
pub use requirements::{ModuleRequirements, RequiredInterface};

/// Return a ZIP module archive's library digest using the bounded release
/// extractor. The CLI uses this when producing cross-platform release
/// checksums, including Windows `.dll` assets on a Unix host.
#[cfg(feature = "cli")]
pub fn sha256_zip_module_library(path: impl AsRef<std::path::Path>) -> crate::Result<String> {
    zip_extract::library_sha256(path.as_ref())
}

#[cfg(any(feature = "modules", feature = "cli"))]
pub(crate) fn refused(reason: impl Into<String>) -> crate::Error {
    crate::Error::module_refused(std::path::Path::new("github-release"), reason)
}
