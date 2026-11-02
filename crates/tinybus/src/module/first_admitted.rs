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
//! download. The one exception is a bundle refused only for where it sits —
//! its directory is writable or owned by another account — which says nothing
//! about its bytes. The release cache then gets the same pinned release, held
//! to the same digest, in a directory this user owns.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use super::cache::{digest_marker_path, secure_release_cache};
use super::host::is_placement_refusal;
use super::{CachedRelease, ModuleHost, ModuleInfo, ReleaseAsset, artifact_dir};

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
    let mut bundle_refused_for_placement_only = true;
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
                    expected_library_sha256: asset.library_sha256,
                    cache_dir: &cache_dir,
                    allow_download: false,
                };
                host.load_github_release_cached(&release, module_config.clone())
            } else {
                // The marker records the signed archive, but notarization may
                // rewrite its library. The bundle's sealed, rehashed local
                // allowlist is the trust source for the final bytes.
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
                    bundle_refused_for_placement_only &= is_placement_refusal(&err);
                    last_error = err.to_string();
                    tracing::warn!(
                        "[modules] bundled '{id}' artifact for {} was not admitted: {last_error}",
                        asset.host_key
                    );
                }
            }
        }
    }
    if found_bundled && bundle_refused_for_placement_only {
        tracing::warn!(
            "[modules] bundled '{id}' was refused for its location only; \
             trying the release cache"
        );
    } else if found_bundled {
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
        secure_release_cache(plan.install_root, &cache_dir);
        let release = CachedRelease {
            release_url: plan.release_url,
            asset_name: asset.archive,
            expected_sha256: Some(asset.sha256),
            expected_library_sha256: asset.library_sha256,
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
            "module '{id}' is unavailable: no usable local artifact is installed and downloads are \
             disabled in configuration ({last_error})"
        ));
    }
    Err(format!(
        "module '{id}' could not be loaded: {last_error}. This is terminal for the running \
         process; restart the app to try again"
    ))
}

/// Load the first admitted pinned artifact while preparing a fresh callback
/// binding for each candidate. A failed load consumes only its own binding;
/// the successful binding is retained with the exact registry peer.
type PreparedCallbacks = Result<
    (
        std::sync::Arc<crate::module::HostCallbackBinding>,
        serde_json::Value,
    ),
    String,
>;
type BoxedCallbackPreparer<'a> =
    dyn FnMut(usize) -> Pin<Box<dyn Future<Output = PreparedCallbacks> + Send + 'a>> + Send + 'a;

pub async fn load_first_admitted_with_host_callbacks<F, Fut>(
    host: &ModuleHost,
    plan: &ReleasePlan<'_>,
    mut prepare: F,
    expected_bus_name: &str,
    expected_object_path: &str,
) -> Result<
    (
        ModuleInfo,
        std::sync::Arc<crate::module::HostCallbackBinding>,
    ),
    String,
>
where
    F: FnMut(usize) -> Fut + Send,
    Fut: std::future::Future<Output = PreparedCallbacks> + Send,
{
    let mut boxed_prepare = |attempt| {
        Box::pin(prepare(attempt)) as Pin<Box<dyn Future<Output = PreparedCallbacks> + Send + '_>>
    };
    load_first_admitted_with_host_callbacks_inner(
        host,
        plan,
        &mut boxed_prepare,
        expected_bus_name,
        expected_object_path,
    )
    .await
}

async fn load_first_admitted_with_host_callbacks_inner<'a>(
    host: &ModuleHost,
    plan: &ReleasePlan<'_>,
    prepare: &'a mut BoxedCallbackPreparer<'a>,
    expected_bus_name: &str,
    expected_object_path: &str,
) -> Result<
    (
        ModuleInfo,
        std::sync::Arc<crate::module::HostCallbackBinding>,
    ),
    String,
> {
    let id = plan.id;
    if plan.assets.is_empty() {
        return Err(format!(
            "module '{id}' is not available for this platform, so the feature it provides is unavailable in this build"
        ));
    }

    let mut attempt = 0;
    let mut last_error = String::new();
    let mut found_bundled = false;
    let mut bundle_refused_for_placement_only = true;
    if let Some(bundled_root) = plan.bundled_root {
        for asset in plan.assets {
            let Some(cache_dir) = artifact_dir(bundled_root, id, plan.version, asset.host_key)
            else {
                continue;
            };
            let has_archive = cache_dir.join(asset.archive).is_file();
            let has_marker = digest_marker_path(&cache_dir, asset.archive).is_file();
            if !has_archive && !has_marker {
                continue;
            }
            found_bundled = true;
            let result = if has_archive {
                let release = CachedRelease {
                    release_url: plan.release_url,
                    asset_name: asset.archive,
                    expected_sha256: Some(asset.sha256),
                    expected_library_sha256: asset.library_sha256,
                    cache_dir: &cache_dir,
                    allow_download: false,
                };
                let (binding, config) = prepare(attempt).await?;
                attempt += 1;
                host.load_github_release_cached_with_host_callbacks(
                    &release,
                    config,
                    &binding,
                    expected_bus_name,
                    expected_object_path,
                )
                .await
                .map(|info| (info, binding))
            } else {
                let (binding, config) = prepare(attempt).await?;
                attempt += 1;
                host.load_bundled_marked_with_host_callbacks(
                    &cache_dir,
                    asset.archive,
                    asset.sha256,
                    config,
                    &binding,
                    (expected_bus_name, expected_object_path),
                )
                .await
                .map(|info| (info, binding))
            };
            match result {
                Ok((info, binding)) => {
                    host.retain_host_callback_binding(expected_bus_name, Arc::clone(&binding))
                        .map_err(|error| {
                            format!("module '{id}' callback retention failed: {error}")
                        })?;
                    return Ok((info, binding));
                }
                Err(error) => {
                    bundle_refused_for_placement_only &= is_placement_refusal(&error);
                    last_error = error.to_string();
                    tracing::warn!(
                        "[modules] bundled '{id}' artifact for {} was not admitted: {last_error}",
                        asset.host_key
                    );
                }
            }
        }
    }
    if found_bundled && !bundle_refused_for_placement_only {
        return Err(format!(
            "module '{id}' could not be loaded from the installer bundle: {last_error}. Restart the app after repairing the installation"
        ));
    }

    for asset in plan.assets {
        let Some(cache_dir) = artifact_dir(plan.install_root, id, plan.version, asset.host_key)
        else {
            last_error =
                "the module's cache path could not be built from its registry entry".into();
            continue;
        };
        secure_release_cache(plan.install_root, &cache_dir);
        let release = CachedRelease {
            release_url: plan.release_url,
            asset_name: asset.archive,
            expected_sha256: Some(asset.sha256),
            expected_library_sha256: asset.library_sha256,
            cache_dir: &cache_dir,
            allow_download: plan.allow_download,
        };
        let (binding, config) = prepare(attempt).await?;
        attempt += 1;
        match host
            .load_github_release_cached_with_host_callbacks(
                &release,
                config,
                &binding,
                expected_bus_name,
                expected_object_path,
            )
            .await
        {
            Ok(info) => {
                host.retain_host_callback_binding(expected_bus_name, Arc::clone(&binding))
                    .map_err(|error| format!("module '{id}' callback retention failed: {error}"))?;
                return Ok((info, binding));
            }
            Err(error) => {
                last_error = error.to_string();
                tracing::warn!(
                    "[modules] '{id}' artifact for {} was not admitted: {last_error}",
                    asset.host_key
                );
            }
        }
    }

    if !plan.allow_download {
        Err(format!(
            "module '{id}' is unavailable: no usable local artifact is installed and downloads are disabled in configuration ({last_error})"
        ))
    } else {
        Err(format!(
            "module '{id}' could not be loaded: {last_error}. This is terminal for the running process; restart the app to try again"
        ))
    }
}

#[cfg(test)]
#[path = "first_admitted_tests.rs"]
mod tests;
