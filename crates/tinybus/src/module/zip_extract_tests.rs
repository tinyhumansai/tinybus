use super::*;

fn extract_fixture(name: &str) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let bytes: &[u8] = match name {
        "compat_store.zip" => include_bytes!("fixtures/compat_store.zip"),
        "compat_deflate_descriptor.zip" => {
            include_bytes!("fixtures/compat_deflate_descriptor.zip")
        }
        "unsafe_parent.zip" => include_bytes!("fixtures/unsafe_parent.zip"),
        "windows_drive.zip" => include_bytes!("fixtures/windows_drive.zip"),
        "windows_backslash.zip" => include_bytes!("fixtures/windows_backslash.zip"),
        "unix_link_mode.zip" => include_bytes!("fixtures/unix_link_mode.zip"),
        "suid_mode.zip" => include_bytes!("fixtures/suid_mode.zip"),
        "duplicate.zip" => include_bytes!("fixtures/duplicate.zip"),
        "archive_collision.zip" => include_bytes!("fixtures/archive_collision.zip"),
        "case_collision.zip" => include_bytes!("fixtures/case_collision.zip"),
        "parent_case_collision.zip" => include_bytes!("fixtures/parent_case_collision.zip"),
        "file_directory_collision.zip" => include_bytes!("fixtures/file_directory_collision.zip"),
        "canonical_parent_collision.zip" => {
            include_bytes!("fixtures/canonical_parent_collision.zip")
        }
        "canonical_leaf_collision.zip" => {
            include_bytes!("fixtures/canonical_leaf_collision.zip")
        }
        "casefold_collision.zip" => include_bytes!("fixtures/casefold_collision.zip"),
        "unicode_paths.zip" => include_bytes!("fixtures/unicode_paths.zip"),
        "compat_zip64_local.zip" => include_bytes!("fixtures/compat_zip64_local.zip"),
        "encrypted.zip" => include_bytes!("fixtures/encrypted.zip"),
        "unsupported_method.zip" => include_bytes!("fixtures/unsupported_method.zip"),
        "overlap.zip" => include_bytes!("fixtures/overlap.zip"),
        "one_module.zip" => include_bytes!("fixtures/one_module.zip"),
        "compat_deflate_descriptor_zip64_local.zip" => {
            include_bytes!("fixtures/compat_deflate_descriptor_zip64_local.zip")
        }
        "deep_path.zip" => include_bytes!("fixtures/deep_path.zip"),
        _ => panic!("unknown test fixture"),
    };
    fs::write(&archive, bytes).unwrap();
    let result = extract(&archive, &destination);
    (root, destination, result)
}

fn extract_fixture_bytes(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(&archive, bytes).unwrap();
    let result = extract(&archive, &destination);
    (root, destination, result)
}

fn extract_fixture_bytes_with_limits(
    bytes: &[u8],
    limits: ExtractionLimits,
) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(&archive, bytes).unwrap();
    let result = extract_with_limits(&archive, &destination, limits);
    (root, destination, result)
}

fn extract_through_cache_staging(
    bytes: &[u8],
) -> (tempfile::TempDir, Result<()>, Option<PathBuf>, bool) {
    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("cache");
    fs::create_dir(&cache_dir).unwrap();
    let mut staging_path = None;
    let mut prior_module_was_extracted = false;
    let result = (|| {
        let staging = crate::module::cache::stage(&cache_dir)?;
        staging_path = Some(staging.path().to_path_buf());
        let archive = staging.path().join("fixture.zip");
        fs::write(&archive, bytes).map_err(|_| refused("test fixture write failed"))?;
        let extraction = extract(&archive, staging.path());
        prior_module_was_extracted = staging.path().join("folder/module.so").is_file();
        extraction?;
        crate::module::cache::commit(staging, &cache_dir)
    })();
    (root, result, staging_path, prior_module_was_extracted)
}

#[test]
fn extracts_independent_stored_and_deflated_archives() {
    let (_root, destination, result) = extract_fixture("compat_store.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("folder/module.so")).unwrap(),
        b"module-payload"
    );
    assert_eq!(fs::read(destination.join("empty")).unwrap(), b"");

    let (_root, destination, result) = extract_fixture("compat_deflate_descriptor.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("folder/module.so")).unwrap(),
        b"deflated-module-payload".repeat(16)
    );
}

#[test]
fn extracts_a_small_archive_with_zip64_local_size_fields() {
    let (_root, destination, result) = extract_fixture("compat_zip64_local.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("module.so")).unwrap(),
        b"zip64-local-module"
    );
}

#[test]
fn accepts_streaming_zip64_local_headers_with_ordinary_central_sizes() {
    let (_root, destination, result) = extract_fixture("compat_deflate_descriptor_zip64_local.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("module.so")).unwrap(),
        b"zip64 streaming descriptor"
    );
}

#[test]
fn excessive_path_depth_is_rejected_before_filesystem_extraction() {
    let (_root, destination, result) = extract_fixture("deep_path.zip");
    assert!(result.is_err());
    assert!(fs::read_dir(destination).unwrap().next().is_none());
}

#[test]
fn rejects_encryption_and_unsupported_methods() {
    for fixture in ["encrypted.zip", "unsupported_method.zip"] {
        let (_root, _destination, result) = extract_fixture(fixture);
        assert!(result.is_err(), "accepted fixture {fixture}");
    }
}

#[test]
fn rejects_overlapping_compressed_entry_ranges() {
    let (_root, _destination, result) = extract_fixture("overlap.zip");
    assert!(result.is_err());
}

#[test]
fn central_directory_entry_count_must_match_the_entries_read() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    let eocd = bytes
        .windows(4)
        .position(|signature| signature == b"PK\x05\x06")
        .unwrap();
    bytes[eocd + 10..eocd + 12].copy_from_slice(&0u16.to_le_bytes());
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn path_policy_rejects_unportable_or_ambiguous_names_before_normalization() {
    for name in [
        "../escape",
        "a/../escape",
        "/absolute",
        "C:/drive",
        "C:relative-drive",
        r"..\escape",
        r"\\server\share\escape",
        "a:stream",
        "NUL.txt",
        "COM1",
        "LPT9.log",
        "COM¹.log",
        "LPT³",
        "trailing.",
        "trailing ",
        "a//b",
        "./file",
        "a\u{0001}b",
        "",
    ] {
        assert!(
            checked_path(name, MAX_PATH_DEPTH).is_err(),
            "accepted {name:?}"
        );
    }
    assert!(checked_path(&"a".repeat(MAX_NAME_BYTES + 1), MAX_PATH_DEPTH).is_err());
    let (root, _destination, result) = extract_fixture("unsafe_parent.zip");
    assert!(result.is_err());
    assert!(!root.path().join("outside.txt").exists());
}

#[test]
fn windows_drive_and_backslash_paths_are_rejected_on_every_platform() {
    for fixture in ["windows_drive.zip", "windows_backslash.zip"] {
        let (root, _destination, result) = extract_fixture(fixture);
        assert!(result.is_err(), "accepted fixture {fixture}");
        assert!(!root.path().join("outside.txt").exists());
    }
}

#[test]
fn duplicate_names_are_refused_before_any_file_is_written() {
    let (_root, destination, result) = extract_fixture("duplicate.zip");
    assert!(result.is_err());
    assert!(!destination.join("module.so").exists());
}

#[test]
fn case_colliding_paths_are_refused_portably() {
    let (_root, destination, result) = extract_fixture("case_collision.zip");
    assert!(result.is_err());
    assert!(!destination.join("foo/a").exists());
    assert!(!destination.join("FOO/b").exists());
}

#[test]
fn implicit_directory_case_collisions_and_file_directory_conflicts_are_refused() {
    for fixture in ["parent_case_collision.zip", "file_directory_collision.zip"] {
        let (_root, _destination, result) = extract_fixture(fixture);
        assert!(result.is_err(), "accepted fixture {fixture}");
    }
}

#[test]
fn canonically_equivalent_unicode_components_are_refused_portably() {
    let mut trie = PathNode::default();
    let mut key_bytes = 0;
    assert!(
        trie.insert(
            Path::new("é/one"),
            false,
            &mut key_bytes,
            MAX_PATH_METADATA_BYTES
        )
        .unwrap()
    );
    assert!(
        !trie
            .insert(
                Path::new("e\u{301}/two"),
                false,
                &mut key_bytes,
                MAX_PATH_METADATA_BYTES,
            )
            .unwrap()
    );

    let mut trie = PathNode::default();
    let mut key_bytes = 0;
    assert!(
        trie.insert(
            Path::new("dir/café"),
            false,
            &mut key_bytes,
            MAX_PATH_METADATA_BYTES,
        )
        .unwrap()
    );
    assert!(
        !trie
            .insert(
                Path::new("dir/cafe\u{301}"),
                false,
                &mut key_bytes,
                MAX_PATH_METADATA_BYTES,
            )
            .unwrap()
    );

    for fixture in [
        "canonical_parent_collision.zip",
        "canonical_leaf_collision.zip",
    ] {
        let (_root, _destination, result) = extract_fixture(fixture);
        assert!(
            result.is_err(),
            "accepted canonical alias fixture {fixture}"
        );
    }
}

#[test]
fn full_unicode_casefold_aliases_are_refused_portably() {
    let mut trie = PathNode::default();
    let mut key_bytes = 0;
    assert!(
        trie.insert(
            Path::new("dir/Straße"),
            false,
            &mut key_bytes,
            MAX_PATH_METADATA_BYTES,
        )
        .unwrap()
    );
    assert!(
        !trie
            .insert(
                Path::new("dir/STRASSE"),
                false,
                &mut key_bytes,
                MAX_PATH_METADATA_BYTES,
            )
            .unwrap()
    );

    let (_root, _destination, result) = extract_fixture("casefold_collision.zip");
    assert!(result.is_err());
}

#[test]
fn distinct_unicode_paths_keep_their_archive_spelling() {
    let decomposed = "cafe\u{301}/module.so";
    let (path, _is_dir, _components) = checked_path(decomposed, MAX_PATH_DEPTH).unwrap();
    assert_eq!(path, PathBuf::from(decomposed));

    let (_root, destination, result) = extract_fixture("unicode_paths.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("café/module.so")).unwrap(),
        b"accented"
    );
    assert_eq!(
        fs::read(destination.join("数据/module.so")).unwrap(),
        b"cjk"
    );
}

#[test]
fn casefold_key_expansion_counts_toward_the_path_metadata_limit() {
    let mut trie = PathNode::default();
    let mut metadata_bytes = 0;
    let result = trie.insert(Path::new("İ"), false, &mut metadata_bytes, 2);
    assert!(result.is_err());
    assert!(trie.children.is_empty());
}

#[test]
fn link_and_special_mode_metadata_never_create_links_or_privileges() {
    let (_root, destination, result) = extract_fixture("unix_link_mode.zip");
    assert!(result.is_err(), "symbolic-link metadata must be refused");
    assert!(!destination.join("link").exists());

    let mut special = include_bytes!("fixtures/compat_store.zip").to_vec();
    let central = special
        .windows(4)
        .position(|signature| signature == b"PK\x01\x02")
        .unwrap();
    // Mark the first regular entry as a Unix FIFO. The extractor must reject
    // the metadata before materializing the entry as a regular file.
    special[central + 4..central + 6].copy_from_slice(&((3_u16 << 8) | 20).to_le_bytes());
    special[central + 38..central + 42].copy_from_slice(&(0o010644_u32 << 16).to_le_bytes());
    let (_root, destination, result) = extract_fixture_bytes(&special);
    assert!(result.is_err(), "special-file metadata must be refused");
    assert!(fs::read_dir(destination).unwrap().next().is_none());

    let (_root, destination, result) = extract_fixture("suid_mode.zip");
    result.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(destination.join("module.so"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o6000, 0, "setuid and setgid bits are not restored");
    }
}

#[test]
fn an_archive_entry_cannot_overwrite_the_host_created_archive() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let archive = destination.join("fixture.zip");
    fs::write(&archive, include_bytes!("fixtures/archive_collision.zip")).unwrap();
    let original = fs::read(&archive).unwrap();

    assert!(extract(&archive, &destination).is_err());
    assert_eq!(fs::read(&archive).unwrap(), original);
}

#[test]
fn inflated_output_is_bounded_even_when_the_archive_is_small() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(
        &archive,
        include_bytes!("fixtures/compat_deflate_descriptor.zip"),
    )
    .unwrap();

    let result = extract_with_limits(
        &archive,
        &destination,
        ExtractionLimits {
            entry_bytes: 8,
            total_bytes: 16,
            entries: 8,
            ..DEFAULT_LIMITS
        },
    );
    assert!(result.is_err());
    assert!(!destination.join("folder/module.so").exists());
}

#[test]
fn path_metadata_budget_is_checked_before_output_creation() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(&archive, include_bytes!("fixtures/compat_store.zip")).unwrap();

    let result = extract_with_limits(
        &archive,
        &destination,
        ExtractionLimits {
            path_metadata_bytes: 4,
            ..DEFAULT_LIMITS
        },
    );
    assert!(result.is_err());
    assert!(fs::read_dir(destination).unwrap().next().is_none());
}

#[test]
fn path_component_budget_is_checked_before_trie_insertion() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(&archive, include_bytes!("fixtures/compat_store.zip")).unwrap();

    let result = extract_with_limits(
        &archive,
        &destination,
        ExtractionLimits {
            path_components: 1,
            ..DEFAULT_LIMITS
        },
    );
    assert!(result.is_err());
    assert!(fs::read_dir(destination).unwrap().next().is_none());
}

#[test]
fn entry_count_and_expanded_bytes_limits_fail_before_extracting() {
    for limits in [
        ExtractionLimits {
            entries: 0,
            ..DEFAULT_LIMITS
        },
        ExtractionLimits {
            entry_bytes: 1,
            ..DEFAULT_LIMITS
        },
        ExtractionLimits {
            total_bytes: 1,
            ..DEFAULT_LIMITS
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("fixture.zip");
        let destination = root.path().join("staging");
        fs::create_dir(&destination).unwrap();
        fs::write(&archive, include_bytes!("fixtures/one_module.zip")).unwrap();

        assert!(extract_with_limits(&archive, &destination, limits).is_err());
        assert!(fs::read_dir(destination).unwrap().next().is_none());
    }
}

#[test]
fn an_entry_limit_applies_to_the_observed_count_not_only_the_eocd_hint() {
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    let eocd = bytes
        .windows(4)
        .position(|signature| signature == b"PK\x05\x06")
        .unwrap();
    bytes[eocd + 10..eocd + 12].copy_from_slice(&1u16.to_le_bytes());
    let (_root, _destination, result) = extract_fixture_bytes_with_limits(
        &bytes,
        ExtractionLimits {
            entries: 1,
            ..DEFAULT_LIMITS
        },
    );
    assert!(result.is_err());
}

#[test]
fn corrupt_central_crc_is_refused_without_promoting_partial_output() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    let central = bytes
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .unwrap();
    bytes[central + 16] ^= 0x80;
    bytes[14] ^= 0x80;
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
}

#[test]
fn central_and_local_size_claims_must_match_the_decompressed_bytes() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    set_member_size(&mut bytes, "module.so", 6);
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn central_and_local_names_must_agree() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    let central = bytes
        .windows(4)
        .position(|signature| signature == b"PK\x01\x02")
        .unwrap();
    let name = central + 46;
    bytes[name..name + 9].copy_from_slice(b"module.xx");
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn central_compressed_size_must_match_the_local_data_range() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    set_central_compressed_size(&mut bytes, "module.so", 8);
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn local_headers_must_match_central_metadata() {
    let mut ordinary = include_bytes!("fixtures/one_module.zip").to_vec();
    set_local_crc(&mut ordinary, "module.so", 1);
    let (_root, _destination, result) = extract_fixture_bytes(&ordinary);
    assert!(result.is_err());

    let mut streaming = include_bytes!("fixtures/compat_deflate_descriptor.zip").to_vec();
    set_local_crc(&mut streaming, "folder/module.so", 1);
    let (_root, _destination, result) = extract_fixture_bytes(&streaming);
    assert!(result.is_err());
}

#[test]
fn reserved_flags_are_refused() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    set_member_flags(&mut bytes, "module.so", 0x10);
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn crc_zero_is_still_verified_for_an_empty_file() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    set_member_crc(&mut bytes, "empty", 1);
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
}

#[test]
fn corrupt_crc_never_promotes_partial_cache_staging() {
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    set_member_crc(&mut bytes, "empty", 1);

    let (root, result, staging_path, prior_module_was_extracted) =
        extract_through_cache_staging(&bytes);
    assert!(result.is_err());
    assert!(prior_module_was_extracted);
    assert!(!staging_path.unwrap().exists());
    assert_eq!(fs::read_dir(root.path().join("cache")).unwrap().count(), 0);
}

#[test]
fn data_descriptor_must_match_the_central_directory() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_deflate_descriptor.zip").to_vec();
    let descriptor = bytes
        .windows(4)
        .position(|window| window == b"PK\x07\x08")
        .unwrap();
    bytes[descriptor + 4] ^= 0x80;
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
    assert!(!destination.join("folder/module.so").exists());
}

fn set_member_crc(bytes: &mut [u8], name: &str, crc: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 14..local + 18].copy_from_slice(&crc.to_le_bytes());
            break;
        }
        offset = name_start + name_len;
    }

    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 16..central + 20].copy_from_slice(&crc.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}

fn set_member_size(bytes: &mut [u8], name: &str, size: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 22..local + 26].copy_from_slice(&size.to_le_bytes());
            break;
        }
        offset = name_start + name_len;
    }

    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 24..central + 28].copy_from_slice(&size.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}

fn set_local_crc(bytes: &mut [u8], name: &str, crc: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 14..local + 18].copy_from_slice(&crc.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}

fn set_central_compressed_size(bytes: &mut [u8], name: &str, size: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 20..central + 24].copy_from_slice(&size.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}

fn set_member_flags(bytes: &mut [u8], name: &str, flags: u16) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 6..local + 8].copy_from_slice(&flags.to_le_bytes());
            break;
        }
        offset = name_start + name_len;
    }

    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 8..central + 10].copy_from_slice(&flags.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}
