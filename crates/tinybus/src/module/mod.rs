//! ABI shared by module authors and hosts, plus the optional host loader.
//!
//! The ABI and manifest types are always compiled so a module can depend on
//! tinybus with no default features. Loading is behind `modules`: in-process
//! modules are trusted code with the host's full address-space privileges.

pub mod abi;
#[cfg(feature = "modules")]
mod github;
mod hash;
pub mod manifest;

#[cfg(feature = "modules")]
pub(crate) mod host;
#[cfg(feature = "modules")]
mod loader;
#[cfg(feature = "modules")]
mod resolve;
#[cfg(feature = "modules")]
mod transport;

#[cfg(feature = "modules")]
pub use host::{ModuleHost, ModuleInfo, ModuleState};

/// Compute the lowercase SHA-256 digest of a release asset.
pub fn sha256_file(path: impl AsRef<std::path::Path>) -> crate::Result<String> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .map_err(|_| crate::Error::failed("release asset could not be opened"))?;
    hash::file_hex(file).map_err(|_| crate::Error::failed("release asset could not be hashed"))
}
