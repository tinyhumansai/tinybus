//! ABI shared by module authors and hosts, plus the optional host loader.
//!
//! The ABI and manifest types are always compiled so a module can depend on
//! tinybus with no default features. Loading is behind `modules`: in-process
//! modules are trusted code with the host's full address-space privileges.

pub mod abi;
#[cfg(feature = "modules")]
mod cache;
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
pub use github::CachedRelease;
#[cfg(feature = "modules")]
pub use host::{ModuleHost, ModuleInfo, ModuleState};

/// Compute the lowercase SHA-256 digest of a release asset.
pub fn sha256_file(path: impl AsRef<std::path::Path>) -> crate::Result<String> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .map_err(|_| crate::Error::failed("release asset could not be opened"))?;
    hash::file_hex(file).map_err(|_| crate::Error::failed("release asset could not be hashed"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn public_file_hashing_uses_the_same_sha256_implementation_as_module_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("asset.tar.gz");
        std::fs::write(&path, b"asset").unwrap();
        assert_eq!(
            super::sha256_file(path).unwrap(),
            "d59386e0ae435e292fbe0ebcdb954b75ed5fb3922091277cb19f798fc5d50718"
        );
    }
}
