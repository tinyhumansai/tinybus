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
    let module = match find_module(dir) {
        Ok(module) => module,
        Err(error) => {
            warn!(asset = asset_name, error = %error, "release cache: extraction is unusable");
            return None;
        }
    };
    if !sidecar_matches(&module) {
        warn!(
            asset = asset_name,
            "release cache: module does not match its allowlist; will download again"
        );
        return None;
    }
    let Ok(module) = std::fs::canonicalize(&module) else {
        warn!(
            asset = asset_name,
            "release cache: module path could not be canonicalized"
        );
        return None;
    };
    debug!(asset = asset_name, "release cache: hit");
    Some(CachedArtifact {
        module,
        sha256: expected,
    })
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
    file_hex(file).is_ok_and(|actual| actual == expected)
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

/// A fresh staging directory beside `dir`.
///
/// Beside, not inside: the commit is a rename, and a rename is only atomic
/// within one filesystem. The parent is created if it is missing, since the
/// first launch on a machine has no cache tree at all.
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
    tempfile::Builder::new()
        .prefix(STAGING_PREFIX)
        .tempdir_in(parent)
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

#[cfg(test)]
mod tests {
    use super::*;

    const ASSET: &str = "demo-module-1.0.0.tar.gz";

    fn write(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, bytes).unwrap();
    }

    fn module_name() -> String {
        format!("libdemo_module.{}", library_extension())
    }

    /// A directory holding a verified-looking archive and one module.
    fn populated(root: &Path) -> (PathBuf, String) {
        let dir = root.join("demo").join("1.0.0");
        write(&dir.join(ASSET), b"archive bytes");
        write(&dir.join(module_name()), b"library bytes");
        let sha = crate::module::sha256_file(dir.join(ASSET)).unwrap();
        (dir, sha)
    }

    #[test]
    fn a_missing_archive_is_not_a_hit() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            find_verified(root.path(), ASSET, Some(&"a".repeat(64))),
            None
        );
    }

    #[test]
    fn an_archive_matching_the_pin_beside_one_module_is_a_hit() {
        let root = tempfile::tempdir().unwrap();
        let (dir, sha) = populated(root.path());
        let hit = find_verified(&dir, ASSET, Some(&sha.to_ascii_uppercase())).expect("hit");
        assert_eq!(hit.sha256, sha);
        assert_eq!(
            hit.module,
            std::fs::canonicalize(dir.join(module_name())).unwrap()
        );
    }

    #[test]
    fn an_archive_that_does_not_match_the_pin_is_not_a_hit() {
        let root = tempfile::tempdir().unwrap();
        let (dir, _) = populated(root.path());
        assert_eq!(find_verified(&dir, ASSET, Some(&"b".repeat(64))), None);
    }

    #[test]
    fn a_pin_that_is_not_a_digest_is_not_a_hit() {
        let root = tempfile::tempdir().unwrap();
        let (dir, _) = populated(root.path());
        assert_eq!(find_verified(&dir, ASSET, Some("not-a-digest")), None);
    }

    #[test]
    fn without_a_pin_the_recorded_digest_decides() {
        let root = tempfile::tempdir().unwrap();
        let (dir, sha) = populated(root.path());
        assert_eq!(
            find_verified(&dir, ASSET, None),
            None,
            "nothing recorded yet"
        );

        write_digest_marker(&dir, ASSET, &sha.to_ascii_uppercase()).unwrap();
        let hit = find_verified(&dir, ASSET, None).expect("hit from the marker");
        assert_eq!(hit.sha256, sha);

        write(&digest_marker_path(&dir, ASSET), b"garbage\n");
        assert_eq!(
            find_verified(&dir, ASSET, None),
            None,
            "a malformed marker is ignored"
        );

        write_digest_marker(&dir, ASSET, &"c".repeat(64)).unwrap();
        assert_eq!(
            find_verified(&dir, ASSET, None),
            None,
            "a marker that disagrees with the archive is not trusted"
        );
    }

    #[test]
    fn a_directory_without_exactly_one_module_is_not_a_hit() {
        let root = tempfile::tempdir().unwrap();
        let (dir, sha) = populated(root.path());

        write(&dir.join("nested").join(module_name()), b"second");
        assert_eq!(find_verified(&dir, ASSET, Some(&sha)), None, "two modules");

        std::fs::remove_dir_all(dir.join("nested")).unwrap();
        std::fs::remove_file(dir.join(module_name())).unwrap();
        assert_eq!(find_verified(&dir, ASSET, Some(&sha)), None, "no module");
    }

    #[test]
    fn a_nested_module_is_found() {
        let root = tempfile::tempdir().unwrap();
        let (dir, sha) = populated(root.path());
        let nested = dir.join("lib").join(module_name());
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::rename(dir.join(module_name()), &nested).unwrap();
        let hit = find_verified(&dir, ASSET, Some(&sha)).expect("hit");
        assert_eq!(hit.module, std::fs::canonicalize(nested).unwrap());
    }

    #[test]
    fn an_allowlist_beside_the_module_is_honoured() {
        let root = tempfile::tempdir().unwrap();
        let (dir, sha) = populated(root.path());
        let module_sha = crate::module::sha256_file(dir.join(module_name())).unwrap();

        write(
            &dir.join("modules.toml"),
            format!("\"{}\" = \"{module_sha}\"\n", module_name()).as_bytes(),
        );
        assert!(
            find_verified(&dir, ASSET, Some(&sha)).is_some(),
            "agreeing allowlist"
        );

        write(
            &dir.join("modules.toml"),
            format!("\"{}\" = \"{}\"\n", module_name(), "d".repeat(64)).as_bytes(),
        );
        assert_eq!(
            find_verified(&dir, ASSET, Some(&sha)),
            None,
            "disagreeing allowlist"
        );

        write(&dir.join("modules.toml"), b"\"other.so\" = \"ee\"\n");
        assert_eq!(
            find_verified(&dir, ASSET, Some(&sha)),
            None,
            "module absent from allowlist"
        );

        write(
            &dir.join("modules.toml"),
            format!("\"{}\" = \"not-hex\"\n", module_name()).as_bytes(),
        );
        assert_eq!(
            find_verified(&dir, ASSET, Some(&sha)),
            None,
            "invalid allowlist hash"
        );
    }

    #[test]
    fn staging_is_committed_by_one_rename_and_replaces_what_was_there() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("demo").join("1.0.0");
        write(&dir.join("stale"), b"old");

        let staging = stage(&dir).unwrap();
        assert_eq!(
            staging.path().parent().unwrap(),
            dir.parent().unwrap(),
            "staging is a sibling of the target"
        );
        write(&staging.path().join(ASSET), b"new archive");
        let staged_path = staging.path().to_path_buf();

        commit(staging, &dir).unwrap();
        assert!(!staged_path.exists(), "the staging directory moved");
        assert!(dir.join(ASSET).is_file(), "the new content is in place");
        assert!(!dir.join("stale").exists(), "the old content is gone");
    }

    #[test]
    fn a_commit_onto_a_missing_target_creates_it() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("fresh").join("2.0.0");
        let staging = stage(&dir).unwrap();
        write(&staging.path().join(ASSET), b"bytes");
        commit(staging, &dir).unwrap();
        assert!(dir.join(ASSET).is_file());
    }

    #[test]
    fn a_failed_commit_leaves_no_staging_directory_behind() {
        let root = tempfile::tempdir().unwrap();
        let good = root.path().join("demo").join("1.0.0");
        let staging = stage(&good).unwrap();
        let staged_path = staging.path().to_path_buf();
        write(&staged_path.join(ASSET), b"bytes");

        // A target whose parent is a regular file cannot be renamed into.
        let blocker = root.path().join("blocker");
        write(&blocker, b"file");
        let error = commit(staging, &blocker.join("1.0.0")).expect_err("rename must fail");
        assert!(error.to_string().contains("moved into place"), "{error}");
        assert!(!staged_path.exists(), "the staging directory is cleaned up");
    }

    #[test]
    fn a_staging_directory_needs_a_parent() {
        assert!(stage(Path::new("/")).is_err());
    }

    #[test]
    fn download_urls_follow_the_release_asset_path() {
        assert_eq!(
            release_download_url("tinyhumansai", "tinymemory", "v1.13.7", "checksum.toml"),
            "https://github.com/tinyhumansai/tinymemory/releases/download/v1.13.7/checksum.toml"
        );
    }

    #[test]
    fn errors_name_the_reason_not_the_path() {
        let root = tempfile::tempdir().unwrap();
        let error = find_module(root.path()).expect_err("empty directory");
        let rendered = error.to_string();
        assert!(rendered.contains("no platform module"), "{rendered}");
        assert!(
            !rendered.contains(&root.path().display().to_string()),
            "{rendered}"
        );
    }
}
