//! The vocabulary of a pinned release, compiled with or without the loader.
//!
//! A host registry names modules, their bus addresses and the digests of their
//! published artifacts whether or not it can load one, so a build without the
//! `modules` feature still identifies a module and reports it unavailable.

use crate::module::platform::host_candidates;

/// One published artifact of a release and the digest that makes it legitimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseAsset<'a> {
    /// Host identifier this artifact targets, e.g. `ubuntu-24.04-x86_64`.
    pub host_key: &'a str,
    /// Exact release asset name.
    pub archive: &'a str,
    /// Lowercase hex SHA-256 of the archive, pinned by the host.
    pub sha256: &'a str,
    /// Lowercase hex SHA-256 of the extracted library, from the release
    /// checksum manifest. `None` keeps older registries compatible but cannot
    /// authorize a strict attested call.
    pub library_sha256: Option<&'a str>,
}

/// A first-party module a host knows how to load.
///
/// Every field is supplied by the host's compiled registry rather than
/// discovered: which modules exist, which interface they claim and which bytes
/// are legitimate are build decisions, not whatever a release page serves today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseRecord {
    /// Stable identifier used by config, RPC and `ensure_loaded`.
    pub id: &'static str,
    /// Well-known bus name the module claims once it is serving.
    pub bus_name: &'static str,
    /// Object path the module serves its interface at.
    pub object_path: &'static str,
    /// Release version, matching the tag the assets come from.
    pub version: &'static str,
    /// GitHub release tag URL the artifacts are published under.
    pub release_url: &'static str,
    /// Per-host artifacts and the digests that make them legitimate.
    pub assets: &'static [ReleaseAsset<'static>],
}

impl ReleaseRecord {
    /// The asset for `host_key`, if this release publishes one.
    #[must_use]
    pub fn asset_for(&self, host_key: &str) -> Option<&'static ReleaseAsset<'static>> {
        self.assets.iter().find(|asset| asset.host_key == host_key)
    }

    /// Whether this release publishes an artifact any candidate key of this
    /// host can run.
    #[must_use]
    pub fn supports_this_host(&self) -> bool {
        host_candidates()
            .iter()
            .any(|key| self.asset_for(key).is_some())
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
