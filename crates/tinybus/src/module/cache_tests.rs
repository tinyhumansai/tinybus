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
    assert_eq!(staging.path().parent().unwrap(), root.path());
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

#[test]
fn a_component_that_cannot_name_a_directory_yields_no_cache_path() {
    for bad in ["..", ".", "", "a/b", "a\\b", ".hidden", "a\0b", "C:temp"] {
        assert!(!is_safe_path_component(bad), "{bad:?} must be refused");
    }
    for good in [
        "tinydocs",
        "0.1.15",
        "macos-26-arm64",
        "ubuntu-22.04-x86_64",
    ] {
        assert!(is_safe_path_component(good), "{good:?} is a real name");
    }
    let root = Path::new("/cache/modules");
    assert_eq!(artifact_dir(root, "tinydocs", "0.1.15", ".."), None);
    assert_eq!(artifact_dir(root, "tinydocs", "0.1.15", "a/b"), None);
}

#[test]
fn each_artifact_of_a_version_has_its_own_cache_directory() {
    let root = Path::new("/cache/modules");
    let dir = artifact_dir(root, "tinydocs", "0.1.15", "macos-26-arm64").expect("usable");
    assert_eq!(
        dir,
        root.join("tinydocs").join("0.1.15").join("macos-26-arm64")
    );
    assert_ne!(
        Some(dir),
        artifact_dir(root, "tinydocs", "0.1.15", "macos-15-arm64")
    );
}

#[test]
fn pruning_keeps_the_pinned_version_and_anything_still_being_staged() {
    let install = tempfile::tempdir().expect("temp install dir");
    let module_root = install.path().join("tinydocs");
    let pinned = module_root.join("0.1.15");
    let stale = module_root.join("0.0.1");
    let staging = module_root.join(".staging-abc123");
    let nested_staging = stale.join(".staging-download");
    for dir in [&pinned, &stale, &staging, &nested_staging] {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("marker"), b"x").unwrap();
    }
    // A stray file beside the version directories is not a version.
    std::fs::write(module_root.join("notes.txt"), b"x").unwrap();

    prune_stale_versions(install.path(), "tinydocs", "0.1.15");

    assert!(pinned.join("marker").is_file(), "the pinned version stays");
    assert!(staging.join("marker").is_file(), "staging stays");
    assert!(
        stale.join("marker").is_file(),
        "a version with active nested staging stays"
    );
    assert!(
        nested_staging.join("marker").is_file(),
        "nested staging stays"
    );
    assert!(module_root.join("notes.txt").is_file());

    // A module that was never cached has nothing to prune.
    prune_stale_versions(&install.path().join("never"), "tinydocs", "0.1.15");
    // An unsafe id or version prunes nothing.
    prune_stale_versions(install.path(), "..", "0.1.15");
    assert!(pinned.join("marker").is_file());
}

#[cfg(unix)]
#[test]
fn pruning_does_not_follow_directory_symlinks_while_looking_for_staging() {
    let install = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let stale = install.path().join("tinydocs").join("0.0.1");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::create_dir_all(outside.path().join(".staging-active")).unwrap();
    std::os::unix::fs::symlink(outside.path(), stale.join("external")).unwrap();

    prune_stale_versions(install.path(), "tinydocs", "0.1.15");

    assert!(!stale.exists(), "the stale version is pruned");
    assert!(outside.path().join(".staging-active").is_dir());
}

#[test]
fn an_unreadable_allowlist_is_a_disagreement_not_a_pass() {
    let root = tempfile::tempdir().unwrap();
    let (dir, sha) = populated(root.path());
    // `modules.toml` exists but cannot be read as a file: the load gate would
    // refuse it, so the cache must not claim the module.
    std::fs::create_dir_all(dir.join("modules.toml")).unwrap();
    assert_eq!(find_verified(&dir, ASSET, Some(&sha)), None);
}

#[test]
fn a_version_directory_that_cannot_be_listed_is_treated_as_being_staged() {
    let root = tempfile::tempdir().unwrap();
    assert!(contains_staging_directory(&root.path().join("missing")));
}

/// An installer-bundle directory: the extraction, its allowlist, and the
/// archive's digest marker, with the archive itself removed.
fn marked(root: &Path, pin: &str) -> PathBuf {
    let dir = root.join("demo").join("1.0.0");
    write(&dir.join(module_name()), b"library bytes");
    let sha = crate::module::sha256_file(dir.join(module_name())).unwrap();
    write(
        &dir.join("modules.toml"),
        format!("\"{}\" = \"{sha}\"\n", module_name()).as_bytes(),
    );
    write_digest_marker(&dir, ASSET, pin).unwrap();
    dir
}

#[test]
fn a_marker_equal_to_the_pin_without_its_archive_is_a_bundle_hit() {
    let root = tempfile::tempdir().unwrap();
    let pin = "c".repeat(64);
    let dir = marked(root.path(), &pin);
    let hit = find_marked(&dir, ASSET, &pin.to_ascii_uppercase()).expect("hit");
    assert_eq!(hit.sha256, pin);
    assert_eq!(
        hit.module,
        std::fs::canonicalize(dir.join(module_name())).unwrap()
    );
}

#[test]
fn a_marker_that_does_not_match_the_pin_is_not_a_bundle_hit() {
    let root = tempfile::tempdir().unwrap();
    let dir = marked(root.path(), &"c".repeat(64));
    assert_eq!(find_marked(&dir, ASSET, &"d".repeat(64)), None);
}

#[test]
fn without_a_marker_or_an_archive_there_is_no_bundle_hit() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("demo").join("1.0.0");
    write(&dir.join(module_name()), b"library bytes");
    assert_eq!(find_marked(&dir, ASSET, &"c".repeat(64)), None);
}

#[test]
fn a_present_archive_is_never_bypassed_by_its_marker() {
    let root = tempfile::tempdir().unwrap();
    let (dir, sha) = populated(root.path());
    write_digest_marker(&dir, ASSET, &sha).unwrap();
    assert_eq!(
        find_marked(&dir, ASSET, &sha),
        None,
        "with the archive on disk, only hashing it may decide"
    );
    // Tampered archive, matching marker: the hashing path still refuses it.
    write(&dir.join(ASSET), b"tampered");
    assert_eq!(find_verified(&dir, ASSET, Some(&sha)), None);
}

#[test]
fn a_bundle_marker_beside_a_mismatched_allowlist_is_not_a_hit() {
    let root = tempfile::tempdir().unwrap();
    let pin = "c".repeat(64);
    let dir = marked(root.path(), &pin);
    write(
        &dir.join("modules.toml"),
        format!("\"{}\" = \"{}\"\n", module_name(), "e".repeat(64)).as_bytes(),
    );
    assert_eq!(find_marked(&dir, ASSET, &pin), None);
}

#[test]
fn a_bundle_marker_without_an_allowlist_is_not_a_hit() {
    let root = tempfile::tempdir().unwrap();
    let pin = "c".repeat(64);
    let dir = marked(root.path(), &pin);
    std::fs::remove_file(dir.join("modules.toml")).unwrap();
    assert_eq!(
        find_marked(&dir, ASSET, &pin),
        None,
        "nothing on the marker path would hash the mapped library"
    );
}

#[cfg(unix)]
#[test]
fn a_module_symlinked_outside_its_directory_is_never_a_hit() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join(module_name());
    write(&target, b"library bytes");
    let sha = crate::module::sha256_file(&target).unwrap();

    // Marker path: allowlist and marker would both pass for the target.
    let pin = "c".repeat(64);
    let dir = marked(root.path(), &pin);
    std::fs::remove_file(dir.join(module_name())).unwrap();
    std::os::unix::fs::symlink(&target, dir.join(module_name())).unwrap();
    write(
        &dir.join("modules.toml"),
        format!("\"{}\" = \"{sha}\"\n", module_name()).as_bytes(),
    );
    assert_eq!(find_marked(&dir, ASSET, &pin), None);

    // Archive path: an intact archive does not vouch for an escaping module.
    let (cache, archive_sha) = populated(&root.path().join("cache"));
    std::fs::remove_file(cache.join(module_name())).unwrap();
    std::os::unix::fs::symlink(&target, cache.join(module_name())).unwrap();
    assert_eq!(find_verified(&cache, ASSET, Some(&archive_sha)), None);

    // A symlink that stays inside the directory is still accepted.
    let (inside, inside_sha) = populated(&root.path().join("inside"));
    std::fs::rename(inside.join(module_name()), inside.join("real.bin")).unwrap();
    std::os::unix::fs::symlink(inside.join("real.bin"), inside.join(module_name())).unwrap();
    assert!(find_verified(&inside, ASSET, Some(&inside_sha)).is_some());
}
