use super::{ReleaseAsset, ReleasePlan, load_first_admitted};
use crate::broker::Broker;
use crate::module::{ModuleHost, artifact_dir};

const ASSET: ReleaseAsset<'static> = ReleaseAsset {
    host_key: "ubuntu-24.04-x86_64",
    archive: "demo-ubuntu-24.04-x86_64.tar.gz",
    sha256: "0000000000000000000000000000000000000000000000000000000000000000",
};

fn plan<'a>(
    assets: &'a [ReleaseAsset<'a>],
    install_root: &'a std::path::Path,
    bundled_root: Option<&'a std::path::Path>,
) -> ReleasePlan<'a> {
    ReleasePlan {
        id: "demo",
        version: "1.0.0",
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        assets,
        install_root,
        bundled_root,
        allow_download: false,
    }
}

#[test]
fn no_candidates_means_unavailable_on_this_platform() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let error = load_first_admitted(&host, &plan(&[], root.path(), None), &serde_json::json!({}))
        .unwrap_err();
    assert!(error.contains("not available for this platform"), "{error}");
}

#[test]
fn an_invalid_installer_bundle_is_reported_without_falling_back_to_the_cache() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(ASSET.archive), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("repairing the installation"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn an_absent_installer_bundle_uses_the_release_cache_path() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn an_unbuildable_cache_path_falls_through_then_reports_the_miss() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, root.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}

#[test]
fn an_unbuildable_bundle_path_is_skipped_and_the_release_cache_is_used() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn a_terminal_failure_with_downloads_enabled_says_to_restart() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let mut release = plan(&assets, root.path(), None);
    release.allow_download = true;
    let error = load_first_admitted(&host, &release, &serde_json::json!({})).unwrap_err();
    assert!(error.contains("could not be loaded"), "{error}");
    assert!(error.contains("restart the app"), "{error}");
}

#[test]
fn a_bundle_marker_that_does_not_match_the_pin_is_refused_without_falling_back() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", "f".repeat(64)),
    )
    .unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("does not match the pin"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn a_bundle_marker_is_never_honoured_from_the_download_cache() {
    let host = ModuleHost::new(Broker::new());
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(user_cache.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", ASSET.sha256),
    )
    .unwrap();
    std::fs::write(dir.join("libdemo_module.so"), b"").unwrap();
    std::fs::write(dir.join("libdemo_module.dylib"), b"").unwrap();
    std::fs::write(dir.join("demo_module.dll"), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}
