//! Directory admission against each runner's real filesystem.
//!
//! The rules in `unix_directory_refusal` and `windows_path_grants_untrusted_write`
//! are only as good as their fit to the directories real installs land in, so
//! these tests run them against the operating system rather than against mode
//! bits typed into a test. The ignored ones need a private temporary directory
//! and run in CI's `modules` job on Linux, macOS and Windows.

use super::*;
use crate::module::{ReleaseAsset, ReleasePlan, artifact_dir, load_first_admitted};

/// Give every account write access to `directory`, as a shared or mis-set
/// install location would.
fn open_to_other_accounts(directory: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    #[cfg(windows)]
    {
        // S-1-1-0 is Everyone.
        let output = std::process::Command::new("icacls")
            .arg(directory)
            .args(["/grant", "*S-1-1-0:(OI)(CI)(M)"])
            .output()
            .expect("icacls is installed on Windows");
        assert!(output.status.success(), "icacls failed: {output:?}");
    }
}

/// Write a lazy library (admitted without being mapped) into `dir`, with the
/// allowlist that pins it.
fn write_lazy_library(dir: &Path) -> PathBuf {
    let artifact = dir.join(format!(
        "clock.{}",
        crate::module::cache::library_extension()
    ));
    std::fs::write(&artifact, b"not loaded until the first call").unwrap();
    let mut lazy_manifest = super::tests::manifest();
    lazy_manifest.lazy_init = true;
    std::fs::write(
        lazy_manifest_path(&artifact),
        serde_json::to_vec(&lazy_manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join("modules.toml"),
        format!(
            "{:?} = {:?}\n",
            artifact.file_name().unwrap().to_str().unwrap(),
            crate::module::sha256_file(&artifact).unwrap()
        ),
    )
    .unwrap();
    artifact
}

/// The production failure end to end: the installer bundle sits in a
/// directory other accounts can write, so the gate refuses it for its location;
/// the same pinned release, already verified into this user's cache, loads.
#[ignore = "needs a private temporary directory; run by CI's modules job on every OS"]
#[test]
fn a_bundle_others_can_write_falls_back_to_the_verified_release_cache() {
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let archive_bytes = b"the pinned release archive";
    let pin = {
        let path = user_cache.path().join("pin-probe");
        std::fs::write(&path, archive_bytes).unwrap();
        let pin = crate::module::sha256_file(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        pin
    };
    let asset = ReleaseAsset {
        host_key: "test-host",
        archive: "clock-test-host.tar.gz",
        sha256: &pin,
    };

    // The bundle: a marker equal to the pin and an admissible extraction, in
    // a directory the gate must refuse for its location alone.
    let bundle_dir = artifact_dir(bundled.path(), "clock", "0.1.0", asset.host_key).unwrap();
    std::fs::create_dir_all(&bundle_dir).unwrap();
    write_lazy_library(&bundle_dir);
    std::fs::write(
        crate::module::cache::digest_marker_path(&bundle_dir, asset.archive),
        format!("{pin}\n"),
    )
    .unwrap();
    open_to_other_accounts(&bundle_dir);
    let refusal = check_directory(&bundle_dir).unwrap_err();
    assert!(is_placement_refusal(&refusal), "{refusal}");

    // The release cache: the archive that hashes to the pin, and its extraction.
    let cache_dir = artifact_dir(user_cache.path(), "clock", "0.1.0", asset.host_key).unwrap();
    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::write(cache_dir.join(asset.archive), archive_bytes).unwrap();
    let cached = write_lazy_library(&cache_dir);

    let host = ModuleHost::new(crate::broker::Broker::new());
    let assets = [asset];
    let plan = ReleasePlan {
        id: "clock",
        version: "0.1.0",
        release_url: "https://github.com/tinyhumansai/clock/releases/tag/v0.1.0",
        assets: &assets,
        install_root: user_cache.path(),
        bundled_root: Some(bundled.path()),
        allow_download: false,
    };
    let info = load_first_admitted(&host, &plan, &serde_json::json!({})).unwrap();
    assert_eq!(info.state, ModuleState::Resolved);
    assert_eq!(
        info.file,
        cached.file_name().unwrap().to_str().unwrap(),
        "the cached copy, not the bundled one, was admitted"
    );
}

/// A private directory this user created passes the gate on every OS: the
/// premise the release cache relies on.
#[ignore = "needs a private temporary directory; run by CI's modules job on every OS"]
#[test]
fn a_private_directory_this_user_created_is_admitted() {
    let directory = tempfile::tempdir().unwrap();
    let nested = directory.path().join("clock").join("0.1.0").join("test-host");
    std::fs::create_dir_all(&nested).unwrap();
    crate::module::cache::secure_release_cache(directory.path(), &nested);
    check_directory(&nested).unwrap();
}

/// `/Applications` ships as `root:admin 0775`. Refusing it refused every module
/// bundled in a normally installed app.
#[cfg(target_os = "macos")]
#[test]
fn the_real_macos_applications_directory_is_admitted() {
    check_directory(Path::new("/Applications")).unwrap();
}

/// Release-cache directories stay owner-only under a permissive umask.
///
/// Umask is process-wide, so this runs only when `TINYBUS_TEST_UMASK` is set,
/// in a CI step of its own with one test thread.
#[cfg(unix)]
#[test]
fn release_cache_directories_are_private_under_a_permissive_umask() {
    use std::os::unix::fs::PermissionsExt;

    unsafe extern "C" {
        fn umask(mask: u32) -> u32;
    }

    if std::env::var_os("TINYBUS_TEST_UMASK").is_none() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let dir = artifact_dir(root.path(), "clock", "0.1.0", "test-host").unwrap();
    let previous = unsafe { umask(0o002) };
    let staged = crate::module::cache::stage(&dir);
    unsafe { umask(previous) };
    drop(staged.unwrap());

    let version = dir.parent().unwrap();
    for directory in [version, version.parent().unwrap()] {
        let mode = std::fs::metadata(directory).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{} is {mode:o}", directory.display());
    }
    check_directory(version).unwrap();
}
