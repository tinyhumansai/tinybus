//! ABI shared by module authors and hosts, plus the optional host loader.
//!
//! The ABI and manifest types are always compiled so a module can depend on
//! tinybus with no default features. Loading is behind `modules`: in-process
//! modules are trusted code with the host's full address-space privileges.

pub mod abi;
#[cfg(feature = "modules")]
mod cache;
#[cfg(feature = "modules")]
mod first_admitted;
#[cfg(feature = "modules")]
mod github;
mod hash;
pub mod manifest;
pub mod platform;
#[cfg(feature = "modules")]
pub mod resolution;

#[cfg(feature = "modules")]
pub(crate) mod host;
#[cfg(feature = "modules")]
mod loader;
#[cfg(feature = "modules")]
mod remembered_hash;
#[cfg(feature = "modules")]
mod resolve;
#[cfg(feature = "modules")]
mod transport;
#[cfg(feature = "modules")]
mod windows_acl;

#[cfg(feature = "modules")]
pub use cache::{artifact_dir, is_safe_path_component, prune_stale_versions};
#[cfg(feature = "modules")]
pub use first_admitted::{ReleaseAsset, ReleasePlan, load_first_admitted};
#[cfg(feature = "modules")]
pub use github::CachedRelease;
#[cfg(feature = "modules")]
pub use host::{ModuleHost, ModuleInfo, ModuleState};

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
        })
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
