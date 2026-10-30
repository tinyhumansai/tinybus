use super::*;
use std::io::Write;

#[test]
fn github_tag_urls_parse_without_accepting_other_hosts_or_paths() {
    assert_eq!(
        parse_release_url("https://github.com/tinyhumansai/rust-template/releases/tag/v0.1.2")
            .unwrap(),
        ("tinyhumansai", "rust-template", "v0.1.2")
    );
    assert!(parse_release_url("https://example.com/a/b/releases/tag/v1").is_err());
    assert!(parse_release_url("https://github.com/a/b/releases/latest").is_err());
}

#[test]
fn checksum_manifests_accept_the_documented_toml_and_json_shapes() {
    let expected = "a".repeat(64);
    let toml = format!("[sha256]\n\"module.tar.gz\" = \"{expected}\"\n");
    assert_eq!(
        parse_checksums("checksum.toml", toml.as_bytes())
            .unwrap()
            .sha256["module.tar.gz"],
        expected
    );
    let json = format!(
        r###"{{"sha256":{{"module.tar.gz":"{}"}}}}"###,
        "b".repeat(64)
    );
    assert_eq!(
        parse_checksums("checksum.json", json.as_bytes())
            .unwrap()
            .sha256["module.tar.gz"],
        "b".repeat(64)
    );
}

#[test]
fn module_paths_are_canonicalized_before_admission() {
    let directory = tempfile::tempdir().unwrap();
    let module = directory.path().join(if cfg!(windows) {
        "module.dll"
    } else if cfg!(target_os = "macos") {
        "module.dylib"
    } else {
        "module.so"
    });
    std::fs::write(&module, b"module").unwrap();

    let canonical = canonical_module(module.clone()).unwrap();
    assert_eq!(canonical, std::fs::canonicalize(module).unwrap());
}

#[test]
fn the_manifest_digest_must_agree_with_the_host_pin() {
    let digest = "c".repeat(64);
    let located = Located {
        manifest_name: "checksum.toml".to_string(),
        manifest: format!(
            "[sha256]\n\"m.tar.gz\" = \"{}\"\n",
            digest.to_ascii_uppercase()
        )
        .into_bytes(),
        archive_url: String::new(),
    };
    assert_eq!(manifest_digest(&located, "m.tar.gz", None).unwrap(), digest);
    assert_eq!(
        manifest_digest(&located, "m.tar.gz", Some(&digest)).unwrap(),
        digest
    );
    assert!(manifest_digest(&located, "m.tar.gz", Some(&"d".repeat(64))).is_err());
    assert!(manifest_digest(&located, "other.tar.gz", None).is_err());
    assert!(manifest_digest(&located, "m.tar.gz", Some("short")).is_err());
}

#[test]
fn a_library_digest_is_correlated_to_its_archive_and_host_pin() {
    let archive = "a".repeat(64);
    let library = "b".repeat(64);
    let located = Located {
        manifest_name: "checksum.toml".to_string(),
        manifest: format!(
            "[sha256]\n\"module.tar.gz\" = \"{archive}\"\n\
             [library_sha256]\n\"module.tar.gz\" = \"{library}\"\n"
        )
        .into_bytes(),
        archive_url: String::new(),
    };
    assert_eq!(
        manifest_library_digest(&located, "module.tar.gz", Some(&library)).unwrap(),
        Some(library.clone())
    );
    assert!(manifest_library_digest(&located, "module.tar.gz", Some(&"c".repeat(64))).is_err());
    assert!(manifest_library_digest(&located, "missing.tar.gz", Some(&library)).is_err());
}

#[test]
fn a_cache_miss_with_downloads_disabled_is_refused_without_the_network() {
    let directory = tempfile::tempdir().unwrap();
    let error = acquire_cached(&CachedRelease {
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        asset_name: "demo-1.0.0.tar.gz",
        expected_sha256: None,
        expected_library_sha256: None,
        cache_dir: &directory.path().join("demo").join("1.0.0"),
        allow_download: false,
    })
    .expect_err("nothing cached and no download allowed");
    assert!(
        error.to_string().contains("downloads are disabled"),
        "{error}"
    );
}

#[test]
fn an_asset_name_that_leaves_the_cache_directory_is_refused() {
    // A verified archive one level above the version directory: a `..` asset
    // name used to reach it and answer from outside the cache.
    let directory = tempfile::tempdir().unwrap();
    let cache_dir = directory.path().join("demo").join("1.0.0");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let outside = directory.path().join("demo").join("escape.tar.gz");
    std::fs::write(&outside, b"archive").unwrap();
    std::fs::write(
        cache_dir.join(format!("libdemo_module.{}", cache::library_extension())),
        b"library",
    )
    .unwrap();
    let digest = crate::module::sha256_file(&outside).unwrap();

    let error = acquire_cached(&CachedRelease {
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        asset_name: "../escape.tar.gz",
        expected_sha256: Some(&digest),
        expected_library_sha256: None,
        cache_dir: &cache_dir,
        allow_download: false,
    })
    .expect_err("an asset name outside the cache directory must be refused");
    assert!(
        error.to_string().contains("not a plain file name"),
        "{error}"
    );
}

#[test]
fn a_concurrent_fill_that_verifies_is_kept_rather_than_replaced() {
    // A sibling process committed and verified this release while we were
    // downloading it; it may have the module mapped right now.
    let directory = tempfile::tempdir().unwrap();
    let cache_dir = directory.path().join("demo").join("1.0.0");
    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::write(cache_dir.join("demo-1.0.0.tar.gz"), b"archive").unwrap();
    let module = cache_dir.join(format!("libdemo_module.{}", cache::library_extension()));
    std::fs::write(&module, b"library").unwrap();
    let sibling_mapped = cache_dir.join("in-use-by-sibling");
    std::fs::write(&sibling_mapped, b"").unwrap();
    let digest = crate::module::sha256_file(cache_dir.join("demo-1.0.0.tar.gz")).unwrap();

    let staging = cache::stage(&cache_dir).unwrap();
    std::fs::write(staging.path().join("demo-1.0.0.tar.gz"), b"archive").unwrap();
    std::fs::write(staging.path().join(module.file_name().unwrap()), b"library").unwrap();
    let release = CachedRelease {
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        asset_name: "demo-1.0.0.tar.gz",
        expected_sha256: Some(&digest),
        expected_library_sha256: None,
        cache_dir: &cache_dir,
        allow_download: true,
    };
    let relative = module.strip_prefix(&cache_dir).unwrap().to_path_buf();
    let (found, sha256) = commit_or_reuse(staging, &release, &relative, digest.clone()).unwrap();

    assert!(
        sibling_mapped.exists(),
        "the sibling's verified cache directory was replaced"
    );
    assert_eq!(found, std::fs::canonicalize(module).unwrap());
    assert_eq!(sha256, digest);
}

#[test]
fn a_verified_cache_answers_without_the_network() {
    let directory = tempfile::tempdir().unwrap();
    let cache_dir = directory.path().join("demo").join("1.0.0");
    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::write(cache_dir.join("demo-1.0.0.tar.gz"), b"archive").unwrap();
    let module = cache_dir.join(format!("libdemo_module.{}", cache::library_extension()));
    std::fs::write(&module, b"library").unwrap();
    let digest = crate::module::sha256_file(cache_dir.join("demo-1.0.0.tar.gz")).unwrap();
    let library_digest = crate::module::sha256_file(&module).unwrap();

    let (found, sha256) = acquire_cached(&CachedRelease {
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        asset_name: "demo-1.0.0.tar.gz",
        expected_sha256: Some(&digest),
        expected_library_sha256: Some(&library_digest),
        cache_dir: &cache_dir,
        // Downloads are off, so a hit is the only way this can succeed.
        allow_download: false,
    })
    .unwrap();
    assert_eq!(found, std::fs::canonicalize(module).unwrap());
    assert_eq!(sha256, digest);
}

#[test]
fn a_cached_archive_cannot_vouch_for_a_different_extracted_library() {
    let directory = tempfile::tempdir().unwrap();
    let cache_dir = directory.path().join("demo").join("1.0.0");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let archive = cache_dir.join("demo-1.0.0.tar.gz");
    std::fs::write(&archive, b"archive").unwrap();
    let module = cache_dir.join(format!("libdemo_module.{}", cache::library_extension()));
    std::fs::write(&module, b"library").unwrap();
    let archive_digest = crate::module::sha256_file(&archive).unwrap();
    let wrong_library_digest = "c".repeat(64);

    let error = acquire_cached(&CachedRelease {
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        asset_name: "demo-1.0.0.tar.gz",
        expected_sha256: Some(&archive_digest),
        expected_library_sha256: Some(&wrong_library_digest),
        cache_dir: &cache_dir,
        allow_download: false,
    })
    .unwrap_err();
    assert!(error.to_string().contains("extracted library hash"));
}

/// Extracts a valid archive while preserving its directory structure and bytes.
#[test]
fn zip_archives_extract_only_enclosed_regular_files_and_directories() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("release.zip");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
        .add_directory("nested/", zip::write::SimpleFileOptions::default())
        .unwrap();
    archive
        .start_file("nested/module.so", zip::write::SimpleFileOptions::default())
        .unwrap();
    archive.write_all(b"module bytes").unwrap();
    archive.finish().unwrap();

    extract("release.zip", &archive_path, &destination).unwrap();

    assert!(destination.join("nested").is_dir());
    assert_eq!(
        std::fs::read(destination.join("nested/module.so")).unwrap(),
        b"module bytes"
    );
}

#[cfg(unix)]
#[test]
fn release_extraction_tempdirs_are_private_regardless_of_umask() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o775)).unwrap();
    restrict_extraction_directory(directory.path()).unwrap();

    let mode = std::fs::metadata(directory.path())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o700,
        "the extraction root must be owner-only"
    );
}

#[cfg(unix)]
#[test]
fn tar_root_entry_cannot_widen_release_extraction_directory() {
    use std::os::unix::fs::PermissionsExt;

    let parent = tempfile::tempdir().unwrap();
    let archive_path = parent.path().join("release.tar");
    let destination = parent.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();
    restrict_extraction_directory(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = tar::Builder::new(file);
    let archived_root = parent.path().join("archived-root");
    std::fs::create_dir(&archived_root).unwrap();
    std::fs::set_permissions(&archived_root, std::fs::Permissions::from_mode(0o775)).unwrap();
    archive.append_dir("./", &archived_root).unwrap();
    archive.finish().unwrap();

    extract_private("release.tar", &archive_path, &destination).unwrap();

    assert_eq!(
        std::fs::metadata(destination).unwrap().permissions().mode() & 0o777,
        0o700,
        "archive root metadata must not widen the extraction directory"
    );
}

/// Rejects traversal entries without writing outside the extraction directory.
#[test]
fn zip_archives_refuse_paths_outside_the_destination() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("release.zip");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
        .start_file("../outside", zip::write::SimpleFileOptions::default())
        .unwrap();
    archive.write_all(b"outside").unwrap();
    archive.finish().unwrap();

    let error = extract("release.zip", &archive_path, &destination).unwrap_err();
    assert!(error.to_string().contains("unsafe path"), "{error}");
    assert!(!directory.path().join("outside").exists());
}

/// Rejects symlink entries without creating a link or following its target.
#[cfg(unix)]
#[test]
fn zip_archives_refuse_symlink_entries() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("release.zip");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
        .add_symlink(
            "module.so",
            "../../outside",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
    archive.finish().unwrap();

    let error = extract("release.zip", &archive_path, &destination).unwrap_err();
    assert!(
        error.to_string().contains("unsupported entry type"),
        "{error}"
    );
    assert!(!directory.path().join("outside").exists());
    assert!(!destination.join("module.so").exists());
}

/// Rejects Unix special-file entries instead of materializing them.
#[cfg(unix)]
#[test]
fn zip_archives_refuse_special_file_entries() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("release.zip");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
        .start_file(
            "module.so",
            zip::write::SimpleFileOptions::default().external_attributes((0o010644_u32) << 16),
        )
        .unwrap();
    archive.write_all(b"not a regular file").unwrap();
    archive.finish().unwrap();

    let error = extract("release.zip", &archive_path, &destination).unwrap_err();
    assert!(
        error.to_string().contains("unsupported entry type"),
        "{error}"
    );
    assert!(!destination.join("module.so").exists());
}

/// Extracts regular files without applying archived executable or setuid bits.
#[cfg(unix)]
#[test]
fn zip_archives_do_not_apply_setuid_or_executable_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("release.zip");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
        .start_file(
            "module.so",
            zip::write::SimpleFileOptions::default().unix_permissions(0o4755),
        )
        .unwrap();
    archive.write_all(b"module bytes").unwrap();
    archive.finish().unwrap();

    extract("release.zip", &archive_path, &destination).unwrap();

    let mode = std::fs::metadata(destination.join("module.so"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o7000, 0, "special permission bits are not applied");
    assert_eq!(mode & 0o111, 0, "executable permission is not applied");
}

#[cfg(unix)]
#[test]
/// Rejects tar symlink entries the same way the zip branch does.
fn tar_archives_refuse_symlink_entries() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("m.tar.gz");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        file,
        flate2::Compression::default(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_mode(0o777);
    builder
        .append_link(&mut header, "libx_module.dylib", "/outside.dylib")
        .unwrap();
    builder.into_inner().unwrap().finish().unwrap();

    let result = extract("m.tar.gz", &archive_path, &destination);
    let link = destination.join("libx_module.dylib");
    assert!(
        result.is_err(),
        "tar symlink accepted; link exists={} -> {:?}",
        link.symlink_metadata().is_ok(),
        std::fs::read_link(&link).ok()
    );
    assert!(link.symlink_metadata().is_err());
}

#[test]
/// Rejects tar hard-link entries, which can also alias a file outside the archive.
fn tar_archives_refuse_hard_link_entries() {
    let directory = tempfile::tempdir().unwrap();
    let archive_path = directory.path().join("m.tar");
    let destination = directory.path().join("extracted");
    std::fs::create_dir(&destination).unwrap();

    let file = std::fs::File::create(&archive_path).unwrap();
    let mut builder = tar::Builder::new(file);
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Link);
    header.set_size(0);
    header.set_mode(0o644);
    builder
        .append_link(&mut header, "libx_module.dylib", "../outside.dylib")
        .unwrap();
    builder.finish().unwrap();

    let error = extract("m.tar", &archive_path, &destination).unwrap_err();
    assert!(
        error.to_string().contains("unsupported entry type"),
        "{error}"
    );
    assert!(
        destination
            .join("libx_module.dylib")
            .symlink_metadata()
            .is_err()
    );
}

/// Serves one archive request: headers announcing 20 bytes, then `sent` bytes
/// one every 50 ms, then silence for two seconds before closing.
fn slow_archive_server(sent: usize) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/m.tar.gz", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = std::io::Read::read(&mut stream, &mut buf);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n")
            .unwrap();
        for _ in 0..sent {
            std::thread::sleep(Duration::from_millis(50));
            if stream.write_all(b"x").is_err() {
                return;
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    });
    (url, server)
}

#[test]
/// The body budget is an idle timeout: a body that keeps arriving (one byte
/// every 50 ms, about one second in all) outlives a 300 ms stall budget.
fn a_slow_but_progressing_download_is_not_cut_off() {
    let (url, server) = slow_archive_server(20);
    let directory = tempfile::tempdir().unwrap();
    let dest = directory.path().join("m.tar.gz");
    let started = std::time::Instant::now();
    let result = download(&agent_with(Duration::from_millis(300)), &url, &dest);
    let elapsed = started.elapsed();
    let _ = server.join();
    assert!(result.is_ok(), "after {elapsed:?}: {:?}", result.err());
    assert_eq!(std::fs::metadata(&dest).unwrap().len(), 20);
}

#[test]
/// A transfer that stops delivering fails within the stall budget rather than
/// waiting for the server to give up.
fn a_stalled_download_fails_within_the_stall_budget() {
    let (url, server) = slow_archive_server(5);
    let directory = tempfile::tempdir().unwrap();
    let dest = directory.path().join("m.tar.gz");
    let started = std::time::Instant::now();
    let result = download(&agent_with(Duration::from_millis(300)), &url, &dest);
    let elapsed = started.elapsed();
    assert!(result.is_err(), "a stalled body was accepted");
    assert!(elapsed < Duration::from_millis(1500), "took {elapsed:?}");
    let _ = server.join();
}

/// Bytes that do not hash to the manifest digest are refused and leave neither
/// a cache directory nor staging debris behind.
#[test]
fn a_download_that_disagrees_with_its_manifest_is_refused_and_cleaned_up() {
    use std::io::{BufRead as _, Write as _};
    let pinned = "e".repeat(64);
    let manifest = format!("[sha256]\n\"m.tar.gz\" = \"{pinned}\"\n");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            std::io::BufReader::new(&stream)
                .read_line(&mut line)
                .unwrap();
            let body: &[u8] = if line.contains("checksum.toml") {
                manifest.as_bytes()
            } else {
                b"truncated archive bytes"
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(body);
        }
    });
    cache::TEST_RELEASE_BASE.with(|slot| *slot.borrow_mut() = Some(base));
    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("m").join("1.0.0").join("host");

    let error = acquire_cached(&CachedRelease {
        release_url: "https://github.com/o/r/releases/tag/v1",
        asset_name: "m.tar.gz",
        expected_sha256: Some(&pinned),
        expected_library_sha256: None,
        cache_dir: &cache_dir,
        allow_download: true,
    })
    .expect_err("bytes disagree with the manifest");

    assert!(
        error
            .to_string()
            .contains("does not match its checksum manifest"),
        "{error}"
    );
    assert!(!cache_dir.exists());
    let debris: Vec<_> = walk(root.path())
        .into_iter()
        .filter(|path| path.to_string_lossy().contains(".staging"))
        .collect();
    assert!(debris.is_empty(), "{debris:?}");
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        out.push(entry.path());
        if entry.path().is_dir() {
            out.extend(walk(&entry.path()));
        }
    }
    out
}
