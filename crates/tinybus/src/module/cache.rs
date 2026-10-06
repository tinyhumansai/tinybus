//! Persistent, verified storage for release artifacts. Feature `modules`.
//!
//! [`super::github::acquire`] downloads a release into a temporary directory
//! that lives exactly as long as the process. That was the whole design once:
//! the bytes were verified moments ago, the host keeps the `TempDir` alive
//! because a mapped library may resolve sibling files, and nothing was written
//! anywhere an operator would find it. The cost showed up in the field. Every
//! launch of a host paid every download again, one archive per module, over a
//! network it does not control; and because a temporary directory is only
//! cleaned when its owner drops it, a process that exited any other way left the
//! extraction behind — gigabytes of them on a developer machine after a few days.
//!
//! This module is the on-disk half of the fix: a directory the host names,
//! holding the archive it verified, the extraction, and the digest the release
//! manifest published for the archive. The next launch finds it, re-hashes the
//! archive against the digest the host compiled in, and loads without a network
//! round-trip. What it does *not* do is decide policy: the host chooses the
//! directory, whether a download may happen at all, and when an old version is
//! removed.
//!
//! # Layout
//!
//! ```text
//! <dir>/<asset>            the archive, kept so the next launch can re-verify it
//! <dir>/<asset>.sha256     the digest the release manifest published for it
//! <dir>/lib<x>_module.so   the extracted module — exactly one per directory
//! <dir>/modules.toml       the module's own allowlist, when the release ships one
//! ```
//!
//! # Why the archive stays on disk
//!
//! The digest a host compiles in names the *archive*, not the library inside
//! it. Keeping the archive is what lets a later launch check the same thing the
//! first launch checked, against the same pin, instead of trusting that the
//! extraction is still what it was. The extracted library is additionally held
//! to the release's own `modules.toml` — here before the directory is accepted,
//! and again by the allowlist gate every load goes through — so a corrupted
//! extraction beside an intact archive is a cache miss, not a permanent refusal.
//!
//! # Why a download is staged beside the directory
//!
//! A half-written cache must never be mistaken for a whole one. Everything is
//! downloaded and extracted into a sibling staging directory and moved into
//! place with one rename on the same filesystem, so the directory either holds
//! the complete verified set or does not exist. Two hosts racing to fill the
//! same directory are safe: the loser's rename fails, and it re-reads the
//! winner's files.

use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::module::hash::file_hex;
use crate::module::remembered_hash::file_hex_remembered;

/// Suffix of the marker beside an archive holding the digest its release
/// manifest published.
const DIGEST_MARKER_SUFFIX: &str = ".sha256";

/// Prefix of the sibling directory a download is assembled in before it is
/// committed. Dot-prefixed so a directory listing reads as "in progress".
const STAGING_PREFIX: &str = ".staging-";

/// A release found intact in a cache directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedArtifact {
    /// Canonical path of the single platform module in the directory.
    pub module: PathBuf,
    /// Lowercase hex SHA-256 of the archive, as re-verified on this lookup.
    pub sha256: String,
}

/// The file extension a platform module carries on this target.
pub(crate) fn library_extension() -> &'static str {
    if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    }
}

/// Look for `asset_name` in `dir` and prove it before answering.
///
/// `Some` only when the archive is present, hashes to `expected_sha256` — or,
/// when the host pinned nothing, to the digest recorded beside it — and the
/// directory holds exactly one platform module whose own allowlist, if the
/// release shipped one, still matches. Anything less is `None`: a cache that
/// cannot be proven is treated as absent rather than as broken, so the caller
/// falls through to a fresh download instead of failing a launch over a stale or
/// half-written directory. Each reason is logged, because a cache that misses on
/// every launch is a bug worth seeing.
pub(crate) fn find_verified(
    dir: &Path,
    asset_name: &str,
    expected_sha256: Option<&str>,
) -> Option<CachedArtifact> {
    let archive = dir.join(asset_name);
    if !archive.is_file() {
        debug!(asset = asset_name, "release cache: no archive");
        return None;
    }
    let expected = match expected_sha256 {
        Some(pin) => pin.to_ascii_lowercase(),
        None => read_digest_marker(dir, asset_name)?,
    };
    if !crate::attest::is_hex_sha256(&expected) {
        warn!(
            asset = asset_name,
            "release cache: expected digest is not a SHA-256"
        );
        return None;
    }
    let Ok(file) = std::fs::File::open(&archive) else {
        warn!(asset = asset_name, "release cache: archive is unreadable");
        return None;
    };
    let Ok(actual) = file_hex(file) else {
        warn!(
            asset = asset_name,
            "release cache: archive could not be hashed"
        );
        return None;
    };
    if actual != expected {
        warn!(
            asset = asset_name,
            "release cache: archive digest does not match the pin; will download again"
        );
        return None;
    }
    let module = usable_extraction(dir, asset_name)?;
    debug!(asset = asset_name, "release cache: hit");
    Some(CachedArtifact {
        module,
        sha256: expected,
    })
}

/// Look for an installer-bundle entry that ships the archive's digest marker
/// in place of the archive.
///
/// `Some` only when the archive is absent, the marker beside it is well formed
/// and equal to `pin`, and the directory holds exactly one platform module,
/// resolving inside it, that the release's `modules.toml` names with its
/// current hash. Unlike the archive path, the allowlist is required here.
///
/// # Trust model
///
/// Nothing here hashes the archive, because it is not on disk: the bundle's
/// build step verified it against this same pin, extracted it, and replaced it
/// with the marker. The marker is therefore a claim made by the installer, and
/// it is honoured only for the installer-shipped directory —
/// [`super::load_first_admitted`] is the sole caller and passes only the
/// bundled root. The user-writable download cache never reaches this function:
/// a writable directory that could vouch for itself with a text file would
/// make the pin meaningless. On macOS the installer directory is inside the
/// signed `.app`, so the extracted library is covered by the bundle's
/// code-signature seal instead; that is the reason this path exists, since
/// notarization rejects the unsigned Mach-O inside a pinned archive and
/// signing it would change the pinned bytes.
///
/// What is mapped is still checked. The extracted library is held to the
/// release's `modules.toml` here and again by the allowlist gate on every
/// load, exactly as on the hashing path. An installer that rewrites the
/// library (the macOS signer does) must re-pin that entry, under the same
/// seal that covers the marker, or the bundle is refused.
pub(crate) fn find_marked(dir: &Path, asset_name: &str, pin: &str) -> Option<CachedArtifact> {
    if dir.join(asset_name).exists() {
        debug!(
            asset = asset_name,
            "installer bundle: archive present; it decides"
        );
        return None;
    }
    let pin = pin.to_ascii_lowercase();
    if !crate::attest::is_hex_sha256(&pin) {
        warn!(asset = asset_name, "installer bundle: pin is not a SHA-256");
        return None;
    }
    let Some(recorded) = read_digest_marker(dir, asset_name) else {
        debug!(
            asset = asset_name,
            "installer bundle: no archive and no digest marker"
        );
        return None;
    };
    if recorded != pin {
        warn!(
            asset = asset_name,
            "installer bundle: digest marker does not match the pin"
        );
        return None;
    }
    let module = usable_extraction(dir, asset_name)?;
    // The marker vouches for an archive nobody hashes here, so the file that
    // is mapped must be pinned by something that is hashed: the release's own
    // allowlist. Without one, nothing on this path would check the library.
    if !module
        .parent()
        .is_some_and(|parent| parent.join("modules.toml").is_file())
    {
        warn!(
            asset = asset_name,
            "installer bundle: a marker entry needs a modules.toml pinning its library"
        );
        return None;
    }
    debug!(
        asset = asset_name,
        "installer bundle: marker matches the pin"
    );
    Some(CachedArtifact {
        module,
        sha256: pin,
    })
}

/// The single platform module in `dir`, canonicalized, if it agrees with the
/// `modules.toml` beside it.
///
/// The module must resolve inside `dir`. It is canonicalized before it is
/// handed on, so a symlink would otherwise carry the load (and the loader's
/// no-follow open) to a file the directory's own protection does not cover.
fn usable_extraction(dir: &Path, asset_name: &str) -> Option<PathBuf> {
    let module = match find_module(dir) {
        Ok(module) => module,
        Err(error) => {
            warn!(asset = asset_name, error = %error, "release cache: extraction is unusable");
            return None;
        }
    };
    let (Ok(module), Ok(root)) = (std::fs::canonicalize(&module), std::fs::canonicalize(dir))
    else {
        warn!(
            asset = asset_name,
            "release cache: module path could not be canonicalized"
        );
        return None;
    };
    if !module.starts_with(&root) {
        warn!(
            asset = asset_name,
            "release cache: module resolves outside its directory"
        );
        return None;
    }
    if !sidecar_matches(&module) {
        warn!(
            asset = asset_name,
            "release cache: module does not match its allowlist; will download again"
        );
        return None;
    }
    Some(module)
}

/// Whether `module` agrees with the `modules.toml` beside it.
///
/// No allowlist is not a disagreement: a release that ships none is verified
/// by its archive digest alone, exactly as on the first download. An allowlist
/// that does not name the module, cannot be read, or names a different hash is
/// a disagreement — the load gate would refuse the file, so the cache must not
/// claim it.
fn sidecar_matches(module: &Path) -> bool {
    let Some(directory) = module.parent() else {
        return false;
    };
    let allowlist = directory.join("modules.toml");
    if !allowlist.exists() {
        return true;
    }
    let Ok(source) = std::fs::read_to_string(&allowlist) else {
        return false;
    };
    let file_name = module
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let file_stem = module
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let Some(expected) = crate::attest::parse_allowlist(&source)
        .find(|(key, _)| key == file_name || key == file_stem)
        .map(|(_, value)| value)
    else {
        return false;
    };
    if !crate::attest::is_hex_sha256(&expected) {
        return false;
    }
    let Ok(file) = std::fs::File::open(module) else {
        return false;
    };
    file_hex_remembered(file).is_ok_and(|actual| actual == expected)
}

/// Where the digest marker for `asset_name` lives in `dir`.
pub(crate) fn digest_marker_path(dir: &Path, asset_name: &str) -> PathBuf {
    dir.join(format!("{asset_name}{DIGEST_MARKER_SUFFIX}"))
}

/// The digest recorded beside the archive, when there is a well-formed one.
fn read_digest_marker(dir: &Path, asset_name: &str) -> Option<String> {
    let recorded = std::fs::read_to_string(digest_marker_path(dir, asset_name)).ok()?;
    let recorded = recorded.trim().to_ascii_lowercase();
    if crate::attest::is_hex_sha256(&recorded) {
        Some(recorded)
    } else {
        warn!(
            asset = asset_name,
            "release cache: digest marker is malformed"
        );
        None
    }
}

/// Record the digest the release manifest published for `asset_name`.
///
/// # Errors
///
/// Returns an error if the marker cannot be written.
pub(crate) fn write_digest_marker(dir: &Path, asset_name: &str, sha256: &str) -> Result<()> {
    std::fs::write(
        digest_marker_path(dir, asset_name),
        format!("{}\n", sha256.to_ascii_lowercase()),
    )
    .map_err(|_| refused(dir, "release digest marker could not be written"))
}

/// The single platform module under `root`, searched recursively.
///
/// # Errors
///
/// Returns an error if `root` cannot be walked, holds no module, or holds more
/// than one — a release archive names exactly one library, and anything else is
/// a packaging fault rather than a choice for this crate to make.
pub(crate) fn find_module(root: &Path) -> Result<PathBuf> {
    let mut found = Vec::new();
    collect_modules(root, &mut found)
        .map_err(|_| refused(root, "release archive could not be inspected"))?;
    match found.as_slice() {
        [module] => Ok(module.clone()),
        [] => Err(refused(root, "release archive contains no platform module")),
        _ => Err(refused(
            root,
            "release archive contains multiple platform modules",
        )),
    }
}

fn collect_modules(path: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_modules(&path, found)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some(library_extension()) {
            found.push(path);
        }
    }
    Ok(())
}

/// A fresh staging directory outside the version directory containing `dir`.
///
/// Staging sits in the module directory, above the version directory, so
/// pruning a stale version cannot remove an active download. It remains on the
/// same filesystem as the target, preserving atomic rename. Parents are created
/// if missing, since the first launch has no cache tree yet.
///
/// # Errors
///
/// Returns an error if the parent cannot be created or the staging directory
/// cannot be made.
pub(crate) fn stage(dir: &Path) -> Result<tempfile::TempDir> {
    let parent = dir
        .parent()
        .ok_or_else(|| refused(dir, "release cache directory has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|_| refused(dir, "release cache directory could not be created"))?;
    let staging_parent = parent.parent().unwrap_or(parent);
    tempfile::Builder::new()
        .prefix(STAGING_PREFIX)
        .tempdir_in(staging_parent)
        .map_err(|_| refused(dir, "release staging directory could not be created"))
}

/// Move a fully assembled staging directory into place as `dir`.
///
/// Whatever was at `dir` — a previous extraction of the same version that failed
/// verification, or debris — is removed first, then the staging directory is
/// renamed over it. On failure the staging directory is deleted, so a failed
/// commit leaves nothing behind; the caller decides whether a sibling's
/// concurrent fill is acceptable by looking at `dir` again.
///
/// # Errors
///
/// Returns an error if the old directory cannot be removed or the rename fails.
pub(crate) fn commit(staging: tempfile::TempDir, dir: &Path) -> Result<()> {
    // From here on this function owns cleanup: `keep` stops `TempDir` from
    // deleting the directory on drop, which a successful rename would otherwise
    // race against.
    let staged = staging.keep();
    let outcome = (|| {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => {}
            // Nothing there to replace — including a target whose parent is
            // not a directory, which the rename below reports on its own.
            Err(_) if !dir.exists() => {}
            Err(_) => {
                return Err(refused(
                    dir,
                    "previous release directory could not be replaced",
                ));
            }
        }
        std::fs::rename(&staged, dir)
            .map_err(|_| refused(dir, "verified release could not be moved into place"))
    })();
    if outcome.is_err() {
        let _ = std::fs::remove_dir_all(&staged);
    }
    outcome
}

/// The fixed download URL of one asset of a GitHub release.
///
/// Built rather than looked up: the path is a documented contract, and reaching
/// it through the REST API costs an unauthenticated request against a budget
/// shared by everyone behind one address.
pub(crate) fn release_download_url(owner: &str, repo: &str, tag: &str, asset_name: &str) -> String {
    format!("https://github.com/{owner}/{repo}/releases/download/{tag}/{asset_name}")
}

fn refused(path: &Path, reason: &'static str) -> Error {
    Error::module_refused(path, reason)
}

/// Whether `component` is safe to use as one directory name.
///
/// The three values that build a cache path (a module id, its version, and a
/// host key) are compiled-in data in a typical host, so nothing reaches this
/// with a separator in it. It is checked anyway because of what sits at the end
/// of the path: [`prune_stale_versions`] calls `remove_dir_all` on what these
/// build. A registry edit or a future value that carried `..` or a separator
/// would turn a cache tidy-up into deleting somewhere else entirely, and a rule
/// that has to hold for a delete is worth stating rather than inferring from
/// where the data happens to come from today.
#[must_use]
pub fn is_safe_path_component(component: &str) -> bool {
    !component.is_empty()
        && component != "."
        && component != ".."
        && !component.contains('/')
        && !component.contains('\\')
        && !component.contains('\0')
        // Colons can introduce drive prefixes on Windows even without a slash.
        && !component.contains(':')
        // A leading dot would collide with the `.staging-*` directories a
        // concurrent download is filling.
        && !component.starts_with('.')
}

/// Where one artifact of one module version is cached, when all three
/// components are usable as directory names.
///
/// `None` rather than a sanitised path: a registry entry that cannot name a
/// directory is a build-time mistake, and quietly rewriting it would hide the
/// mistake behind a cache that silently never hits.
#[must_use]
pub fn artifact_dir(
    install_root: &Path,
    id: &str,
    version: &str,
    host_key: &str,
) -> Option<PathBuf> {
    for component in [id, version, host_key] {
        if !is_safe_path_component(component) {
            tracing::error!(
                module = id,
                "a cache path component cannot name a directory; refusing to build a cache path \
                 from it"
            );
            return None;
        }
    }
    Some(install_root.join(id).join(version).join(host_key))
}

/// Remove cached versions of module `id` other than `pinned_version`.
///
/// Best-effort and after the fact: a version that is no longer pinned will never
/// be loaded again, so keeping it only costs disk. Staging directories are left
/// alone (a concurrent process may be filling one) and every removal is logged,
/// because a cache that empties itself is worth noticing.
pub fn prune_stale_versions(install_root: &Path, id: &str, pinned_version: &str) {
    // Both sides of the comparison below have to be real directory names, or
    // "everything that is not the pinned version" is not a set this function
    // should be handing to `remove_dir_all`.
    if !is_safe_path_component(id) || !is_safe_path_component(pinned_version) {
        tracing::error!(
            module = id,
            "refusing to prune: its id or version cannot name a directory"
        );
        return;
    }
    let module_root = install_root.join(id);
    let Ok(entries) = std::fs::read_dir(&module_root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // `read_dir` never yields `.` or `..`, and the leading-dot skip covers
        // the staging directories; the guard is here so the delete depends on
        // this function's own check rather than on that being remembered.
        if !path.is_dir()
            || name == pinned_version
            || !is_safe_path_component(&name)
            || contains_staging_directory(&path)
        {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => tracing::info!(
                module = id,
                "removed cached version {name}; {pinned_version} is pinned"
            ),
            Err(error) => tracing::warn!(
                module = id,
                "could not remove cached version {name}: {error}"
            ),
        }
    }
}

/// Do not remove a version while another process stages an artifact inside it.
fn contains_staging_directory(version: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(version) else {
        return true;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return true;
        };
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(STAGING_PREFIX) {
            return true;
        }
        let Ok(file_type) = entry.file_type() else {
            return true;
        };
        if file_type.is_dir() && contains_staging_directory(&entry.path()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
