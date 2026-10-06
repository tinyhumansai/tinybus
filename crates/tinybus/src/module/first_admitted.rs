//! Loading a pinned release for this host: try each candidate artifact, newest
//! first, until one is admitted.
//!
//! A host pins a release and lists one [`ReleaseAsset`] per host key it was built
//! for ([`super::platform::host_candidates`] says which keys can run here,
//! newest first). [`load_first_admitted`] walks them in that order and falls
//! through on an admission failure: a host newer than the newest published build
//! runs that build, and one whose toolchain the newest artifact does not match
//! falls back to the next. Each artifact has its own directory under the version
//! ([`super::artifact_dir`]), so two builds of one release never share an
//! extraction.
//!
//! An installer-shipped bundle is consulted first and is authoritative: if any
//! candidate archive — or the archive's digest marker, which an installer may
//! ship in its place (see [`super::cache::find_marked`]) — is present there, the release cache is never touched, and a
//! bundle that fails admission is reported instead of silently replaced by a
//! download.

use std::path::Path;

use super::cache::digest_marker_path;
use super::{CachedRelease, ModuleHost, ModuleInfo, artifact_dir};

/// One published artifact of a release and the digest that makes it legitimate.
#[derive(Debug, Clone, Copy)]
pub struct ReleaseAsset<'a> {
    /// Host identifier this artifact targets, e.g. `ubuntu-24.04-x86_64`.
    pub host_key: &'a str,
    /// Exact release asset name.
    pub archive: &'a str,
    /// Lowercase hex SHA-256 of the archive, pinned by the host.
    pub sha256: &'a str,
}

/// The pinned release and where its artifacts live on this machine.
#[derive(Debug, Clone, Copy)]
pub struct ReleasePlan<'a> {
    /// Module id (first cache path component).
    pub id: &'a str,
    /// Release version (second cache path component).
    pub version: &'a str,
    /// The `https://github.com/<owner>/<repo>/releases/tag/<tag>` page.
    pub release_url: &'a str,
    /// Candidate artifacts in preference order, newest first.
    pub assets: &'a [ReleaseAsset<'a>],
    /// The user-writable release cache root.
    pub install_root: &'a Path,
    /// A read-only installer-shipped cache root, consulted before the user cache.
    pub bundled_root: Option<&'a Path>,
    /// Whether a release-cache miss may reach the network.
    pub allow_download: bool,
}

/// Load the first candidate artifact of `plan` that the host admits.
///
/// # Errors
///
/// A user-facing message when no candidate exists for this platform, when an
/// installer bundle is present but none of its archives is admitted, when
/// downloads are disabled and nothing is cached, or when every candidate is
/// refused. Messages are sanitised: tinybus's own errors carry only a basename
/// and a fixed reason, and nothing here adds a path or a URL.
pub fn load_first_admitted(
    host: &ModuleHost,
    plan: &ReleasePlan<'_>,
    module_config: &serde_json::Value,
) -> Result<ModuleInfo, String> {
    let id = plan.id;
    if plan.assets.is_empty() {
        return Err(format!(
            "module '{id}' is not available for this platform, so the feature it provides is \
             unavailable in this build"
        ));
    }

    let mut last_error = String::new();
    let mut found_bundled = false;
    if let Some(bundled_root) = plan.bundled_root {
        for asset in plan.assets {
            let Some(cache_dir) = artifact_dir(bundled_root, id, plan.version, asset.host_key)
            else {
                continue;
            };
            // An installer may ship the archive, or — where the archive's own
            // contents cannot ship, as in a notarized macOS app — the archive's
            // digest marker in its place. The archive decides when present.
            let has_archive = cache_dir.join(asset.archive).is_file();
            if !has_archive && !digest_marker_path(&cache_dir, asset.archive).is_file() {
                continue;
            }
            found_bundled = true;
            let loaded = if has_archive {
                let release = CachedRelease {
                    release_url: plan.release_url,
                    asset_name: asset.archive,
                    expected_sha256: Some(asset.sha256),
                    cache_dir: &cache_dir,
                    allow_download: false,
                };
                host.load_github_release_cached(&release, module_config.clone())
            } else {
                host.load_bundled_marked(
                    &cache_dir,
                    asset.archive,
                    asset.sha256,
                    module_config.clone(),
                )
            };
            match loaded {
                Ok(info) => {
                    tracing::info!("[modules] loaded '{id}' from the installer bundle");
                    return Ok(info);
                }
                Err(err) => {
                    last_error = err.to_string();
                    tracing::warn!(
                        "[modules] bundled '{id}' artifact for {} was not admitted: {last_error}",
                        asset.host_key
                    );
                }
            }
        }
    }
    if found_bundled {
        return Err(format!(
            "module '{id}' could not be loaded from the installer bundle: {last_error}. \
             Restart the app after repairing the installation"
        ));
    }

    for asset in plan.assets {
        let Some(cache_dir) = artifact_dir(plan.install_root, id, plan.version, asset.host_key)
        else {
            last_error =
                "the module's cache path could not be built from its registry entry".to_string();
            continue;
        };
        let release = CachedRelease {
            release_url: plan.release_url,
            asset_name: asset.archive,
            expected_sha256: Some(asset.sha256),
            cache_dir: &cache_dir,
            allow_download: plan.allow_download,
        };
        match host.load_github_release_cached(&release, module_config.clone()) {
            Ok(info) => {
                tracing::info!(
                    "[modules] loaded '{id}' {} ({}) through the release cache",
                    plan.version,
                    asset.host_key
                );
                return Ok(info);
            }
            Err(err) => {
                last_error = err.to_string();
                tracing::warn!(
                    "[modules] '{id}' artifact for {} was not admitted: {last_error}",
                    asset.host_key
                );
            }
        }
    }
    if !plan.allow_download {
        tracing::debug!(
            "[modules] '{id}' release cache miss with downloads disabled: {last_error}"
        );
        return Err(format!(
            "module '{id}' is unavailable: no local artifact is installed and downloads are \
             disabled in configuration"
        ));
    }
    Err(format!(
        "module '{id}' could not be loaded: {last_error}. This is terminal for the running \
         process; restart the app to try again"
    ))
}

#[cfg(test)]
#[path = "first_admitted_tests.rs"]
mod tests;
