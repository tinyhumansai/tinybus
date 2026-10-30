//! Bounded ZIP extraction for module release archives.
//!
//! `rawzip` parses ZIP structure but intentionally leaves extraction policy to
//! its callers. This adapter accepts portable paths, STORE and DEFLATE entries,
//! and writes regular files into the host-owned staging directory without
//! applying archive permissions or creating links. Unicode normalization and
//! full case folding are used only for alias detection; output keeps archive
//! spelling.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use icu_casemap::CaseMapper;
use rawzip::{CompressionMethod, ZipArchive, ZipArchiveEntryWayfinder};
use unicode_normalization::UnicodeNormalization;

use crate::error::Result;
use crate::module::refused;
use crate::module::zip_deflate::{StrictDeflate, copy_bounded};
use crate::module::zip_descriptor::validate_data_descriptor;

const MAX_ENTRY_COUNT: u64 = 100_000;
/// A single uncompressed module file may be large (debug symbols, for example),
/// but cannot exceed one GiB even when the download itself is compressed.
const MAX_ENTRY_EXPANDED_BYTES: u64 = 1024 * 1024 * 1024;
/// The current release-download limit is 512 MiB compressed; a 2 GiB expanded
/// archive leaves room for substantial decompression while bounding disk use.
const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_NAME_BYTES: usize = 4096;
/// NFC plus full case-fold may expand a component; keep even the transient key
/// bounded by a fixed multiple of the existing archive-name limit.
const MAX_PATH_KEY_COMPONENT_BYTES: usize = MAX_NAME_BYTES * 16;
const MAX_PATH_DEPTH: usize = 128;
const MAX_PATH_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_PATH_COMPONENTS: usize = 250_000;

#[derive(Clone, Copy)]
struct ExtractionLimits {
    entry_bytes: u64,
    total_bytes: u64,
    entries: u64,
    path_metadata_bytes: usize,
    path_depth: usize,
    path_components: usize,
}

const DEFAULT_LIMITS: ExtractionLimits = ExtractionLimits {
    entry_bytes: MAX_ENTRY_EXPANDED_BYTES,
    total_bytes: MAX_ARCHIVE_EXPANDED_BYTES,
    entries: MAX_ENTRY_COUNT,
    path_metadata_bytes: MAX_PATH_METADATA_BYTES,
    path_depth: MAX_PATH_DEPTH,
    path_components: MAX_PATH_COMPONENTS,
};

#[derive(Default)]
struct PathNode {
    children: HashMap<String, PathChild>,
    explicit_entry: Option<bool>,
}

struct PathChild {
    spelling: String,
    node: PathNode,
}

impl PathNode {
    fn insert(
        &mut self,
        path: &Path,
        is_dir: bool,
        path_metadata_bytes: &mut usize,
        max_path_metadata_bytes: usize,
    ) -> Result<bool> {
        let mut node = self;
        for component in path.components() {
            if node.explicit_entry == Some(false) {
                return Ok(false);
            }
            let Component::Normal(name) = component else {
                return Ok(false);
            };
            let spelling = name
                .to_str()
                .ok_or_else(|| refused("release archive contains an unsafe path"))?;
            let folded = collision_key_component(spelling)?;
            // Bound the derived trie key alongside original path metadata so
            // case-fold expansion cannot grow archive-controlled bookkeeping.
            *path_metadata_bytes = path_metadata_bytes
                .checked_add(folded.len())
                .ok_or_else(|| refused("release archive path metadata exceeds its limit"))?;
            if *path_metadata_bytes > max_path_metadata_bytes {
                return Err(refused("release archive path metadata exceeds its limit"));
            }
            let child = node.children.entry(folded).or_insert_with(|| PathChild {
                spelling: spelling.to_owned(),
                node: PathNode::default(),
            });
            if child.spelling != spelling {
                return Ok(false);
            }
            node = &mut child.node;
        }
        if node.explicit_entry.is_some() || (!is_dir && !node.children.is_empty()) {
            return Ok(false);
        }
        node.explicit_entry = Some(is_dir);
        Ok(true)
    }
}

fn collision_key_component(component: &str) -> Result<String> {
    // Normalize both sides of full folding so canonical aliases share a key;
    // the returned string is never used as the extracted filesystem spelling.
    let normalized: String = component.nfc().collect();
    let folded = CaseMapper::new().fold_string(&normalized);
    let key: String = folded.nfc().collect();
    if key.is_empty() || key.len() > MAX_PATH_KEY_COMPONENT_BYTES {
        return Err(refused("release archive path key exceeds its limit"));
    }
    Ok(key)
}

#[derive(Clone)]
struct EntryPlan {
    wayfinder: ZipArchiveEntryWayfinder,
    path: PathBuf,
    is_dir: bool,
    method: CompressionMethod,
    compressed_size: u64,
    expanded_size: u64,
    data_range: (u64, u64),
}

/// Extract a ZIP archive into a fresh private staging directory.
///
/// The caller owns `destination` and must discard it if this function fails.
pub(super) fn extract(archive_path: &Path, destination: &Path) -> Result<()> {
    extract_with_limits(archive_path, destination, DEFAULT_LIMITS)
}

/// Safely extract a ZIP archive and hash its sole shared module library.
#[cfg(feature = "cli")]
pub(super) fn library_sha256(archive_path: &Path) -> Result<String> {
    let directory = tempfile::tempdir()
        .map_err(|_| refused("checksum ZIP extraction directory is unavailable"))?;
    extract(archive_path, directory.path())?;
    let mut pending = vec![directory.path().to_path_buf()];
    let mut library = None;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .map_err(|_| refused("checksum ZIP extraction could not be read"))?
        {
            let entry = entry.map_err(|_| refused("checksum ZIP extraction is invalid"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|_| refused("checksum ZIP extraction is invalid"))?;
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            if path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "so" | "dylib" | "dll"))
                && library.replace(path).is_some()
            {
                return Err(refused(
                    "checksum ZIP contains more than one module library",
                ));
            }
        }
    }
    let library = library.ok_or_else(|| refused("checksum ZIP contains no module library"))?;
    crate::module::sha256_file(library)
}

fn extract_with_limits(
    archive_path: &Path,
    destination: &Path,
    limits: ExtractionLimits,
) -> Result<()> {
    let file = File::open(archive_path).map_err(|_| refused("release archive is unreadable"))?;
    let file_len = file
        .metadata()
        .map_err(|_| refused("release archive is unreadable"))?
        .len();
    let mut buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(file, &mut buffer)
        .map_err(|_| refused("release archive is invalid"))?;
    let expected_count = archive.entries_hint();
    if expected_count > limits.entries {
        return Err(refused("release archive contains too many entries"));
    }

    let mut plans = Vec::with_capacity(expected_count as usize);
    let mut hinted_expanded_total = 0u64;
    let mut path_metadata_bytes = 0usize;
    let mut path_component_count = 0usize;
    let mut path_trie = PathNode::default();
    let mut local_header_buffer = vec![0; rawzip::MAX_CENTRAL_DIRECTORY_RECORD_SIZE];
    let mut entries = archive.entries(&mut buffer);
    while let Some(entry) = entries
        .next_entry()
        .map_err(|_| refused("release archive is invalid"))?
    {
        if plans.len() as u64 >= limits.entries {
            return Err(refused("release archive contains too many entries"));
        }

        let raw_path = entry.file_path();
        let original = std::str::from_utf8(raw_path.as_ref())
            .map_err(|_| refused("release archive contains an unsafe path"))?;
        path_metadata_bytes = path_metadata_bytes
            .checked_add(original.len())
            .ok_or_else(|| refused("release archive path metadata exceeds its limit"))?;
        if path_metadata_bytes > limits.path_metadata_bytes {
            return Err(refused("release archive path metadata exceeds its limit"));
        }
        let (path, is_dir, components) = checked_path(original, limits.path_depth)?;
        let creator_system = entry.version_made_by().creator_system();
        if matches!(
            creator_system,
            rawzip::CreatorSystem::UNIX | rawzip::CreatorSystem::MACOS
        ) {
            // Inspect the raw type bits: rawzip's normalized mode synthesizes
            // S_IFREG when a Unix archive omits the type, which would combine
            // with a slash-marked directory as a false special-file type.
            let file_type = (entry.external_attributes() >> 16) & 0o170000;
            let allowed_type = if is_dir { 0o040000 } else { 0o100000 };
            if file_type != 0 && file_type != allowed_type {
                return Err(refused(
                    "release archive contains an unsupported entry type",
                ));
            }
        }
        path_component_count = path_component_count
            .checked_add(components)
            .ok_or_else(|| refused("release archive path metadata exceeds its limit"))?;
        if path_component_count > limits.path_components {
            return Err(refused("release archive path metadata exceeds its limit"));
        }
        if !path_trie.insert(
            &path,
            is_dir,
            &mut path_metadata_bytes,
            limits.path_metadata_bytes,
        )? {
            return Err(refused(
                "release archive contains duplicate or case-colliding paths",
            ));
        }

        let method = entry.compression_method();
        let flags = entry.flags();
        validate_flags(flags, method)?;
        if method != CompressionMethod::STORE && method != CompressionMethod::DEFLATE {
            return Err(refused(
                "release archive uses an unsupported compression method",
            ));
        }

        let compressed_size = entry.compressed_size_hint();
        let expanded_size = entry.uncompressed_size_hint();
        if expanded_size > limits.entry_bytes {
            return Err(refused(
                "release archive entry exceeds the extraction limit",
            ));
        }
        if is_dir && expanded_size != 0 {
            return Err(refused("release archive directory entry is not empty"));
        }
        hinted_expanded_total = hinted_expanded_total
            .checked_add(expanded_size)
            .ok_or_else(|| refused("release archive exceeds the extraction limit"))?;
        if hinted_expanded_total > limits.total_bytes {
            return Err(refused("release archive exceeds the extraction limit"));
        }
        let local = archive
            .get_entry(entry.wayfinder())
            .map_err(|_| refused("release archive is invalid"))?;
        let range = local.compressed_data_range();
        if range.0 > range.1
            || range.0 < entry.local_header_offset()
            || range.1 > file_len
            || range.1 > archive.directory_offset()
            || range.1 - range.0 != compressed_size
        {
            return Err(refused("release archive contains invalid entry offsets"));
        }
        let local_header = local
            .local_header(&mut local_header_buffer)
            .map_err(|_| refused("release archive is invalid"))?;
        if local_header.file_path().as_ref() != raw_path.as_ref()
            || local_header.compression_method() != method
            || local_header.flags() != flags
        {
            return Err(refused("release archive headers disagree"));
        }
        if flags.has_data_descriptor() {
            let local_sizes = (
                local_header.crc32(),
                local_header.compressed_size_hint(),
                local_header.uncompressed_size_hint(),
            );
            let all_zero = local_sizes == (0, 0, 0);
            let matches_central = local_sizes == (entry.crc32(), compressed_size, expanded_size);
            if !all_zero && !matches_central {
                return Err(refused("release archive headers disagree"));
            }
            let local_has_zip64_extra = local_header
                .extra_fields()
                .any(|(id, _)| id == rawzip::extra_fields::ExtraFieldId::ZIP64);
            if !validate_data_descriptor(
                archive_path,
                range.1,
                archive.directory_offset().min(file_len),
                local_has_zip64_extra
                    || compressed_size >= u32::MAX as u64
                    || expanded_size >= u32::MAX as u64,
                entry.crc32(),
                compressed_size,
                expanded_size,
            )? {
                return Err(refused("release archive data descriptor disagrees"));
            }
        } else if local_header.crc32() != entry.crc32()
            || local_header.compressed_size_hint() != compressed_size
            || local_header.uncompressed_size_hint() != expanded_size
        {
            return Err(refused("release archive headers disagree"));
        }

        plans.push(EntryPlan {
            wayfinder: entry.wayfinder(),
            path,
            is_dir,
            method,
            compressed_size,
            expanded_size,
            data_range: range,
        });
    }
    if plans.len() as u64 != expected_count {
        return Err(refused("release archive entry count is inconsistent"));
    }

    let mut ranges: Vec<_> = plans
        .iter()
        .map(|plan| plan.data_range)
        .filter(|(start, end)| start < end)
        .collect();
    ranges.sort_unstable_by_key(|range| range.0);
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(refused("release archive contains overlapping entries"));
    }

    let mut expanded_total = 0u64;
    for plan in plans {
        let target = destination.join(&plan.path);
        if plan.is_dir {
            fs::create_dir_all(&target)
                .map_err(|_| refused("release archive extraction failed"))?;
        } else if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|_| refused("release archive extraction failed"))?;
        }
        let mut output: Box<dyn Write> = if plan.is_dir {
            Box::new(io::sink())
        } else {
            Box::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)
                    .map_err(|_| refused("release archive extraction failed"))?,
            )
        };
        let local = archive
            .get_entry(plan.wayfinder)
            .map_err(|_| refused("release archive is invalid"))?;
        if local.compressed_data_range() != plan.data_range {
            return Err(refused("release archive entry offsets changed"));
        }
        let reader = local.reader();
        let mut actual = 0u64;
        match plan.method {
            CompressionMethod::STORE => {
                let mut verified = local.verifying_reader(reader);
                copy_bounded(
                    &mut verified,
                    &mut output,
                    limits.entry_bytes.min(limits.total_bytes - expanded_total),
                    &mut actual,
                )?;
            }
            CompressionMethod::DEFLATE => {
                let decoder = StrictDeflate::new(reader, plan.compressed_size);
                let mut verified = local.verifying_reader(decoder);
                copy_bounded(
                    &mut verified,
                    &mut output,
                    limits.entry_bytes.min(limits.total_bytes - expanded_total),
                    &mut actual,
                )?;
            }
            _ => {
                return Err(refused(
                    "release archive uses an unsupported compression method",
                ));
            }
        }
        if actual != plan.expanded_size {
            return Err(refused("release archive entry size is inconsistent"));
        }
        expanded_total = expanded_total
            .checked_add(actual)
            .ok_or_else(|| refused("release archive exceeds the extraction limit"))?;
    }
    Ok(())
}

fn validate_flags(flags: rawzip::EntryFlags, method: CompressionMethod) -> Result<()> {
    if flags.is_encrypted() || flags.has_strong_encryption() || flags.is_masked() {
        return Err(refused("release archive contains encrypted entries"));
    }
    let allowed: u16 = 1 << 3
        | 1 << 11
        | if method == CompressionMethod::DEFLATE {
            0b110
        } else {
            0
        };
    if flags.bits() & !allowed != 0 {
        return Err(refused("release archive contains unsupported entry flags"));
    }
    Ok(())
}

fn checked_path(original: &str, max_depth: usize) -> Result<(PathBuf, bool, usize)> {
    if original.is_empty()
        || original.len() > MAX_NAME_BYTES
        || original.as_bytes().contains(&0)
        || original.starts_with('/')
        || original.contains('\\')
        || original.contains(':')
        || original
            .chars()
            .any(|ch| ch.is_control() || "<>|?*\"".contains(ch))
    {
        return Err(refused("release archive contains an unsafe path"));
    }
    let is_dir = original.ends_with('/');
    let body = if is_dir {
        original.strip_suffix('/').unwrap_or_default()
    } else {
        original
    };
    if body.split('/').any(|part| part == "..") {
        return Err(refused("release archive contains an unsafe path"));
    }
    if body.split('/').count() > max_depth {
        return Err(refused("release archive path exceeds the depth limit"));
    }
    let normalized = rawzip::path::ZipFilePath::from_bytes(original.as_bytes())
        .try_normalize()
        .map_err(|_| refused("release archive contains an unsafe path"))?;
    if normalized.as_ref() != original {
        return Err(refused("release archive contains an unsafe path"));
    }
    let parts: Vec<_> = body.split('/').collect();
    if parts.is_empty()
        || parts
            .iter()
            .any(|part| part.is_empty() || *part == "." || *part == "..")
    {
        return Err(refused("release archive contains an unsafe path"));
    }
    for part in &parts {
        if part.ends_with('.') || part.ends_with(' ') || is_windows_device_name(part) {
            return Err(refused("release archive contains an unsafe path"));
        }
    }

    let mut path = PathBuf::new();
    for part in &parts {
        path.push(part);
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(refused("release archive contains an unsafe path"));
    }
    Ok((path, is_dir, parts.len()))
}

fn is_windows_device_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches(['.', ' ']);
    let upper = stem.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ["COM", "LPT"].iter().any(|prefix| {
            upper.strip_prefix(prefix).is_some_and(|number| {
                matches!(
                    number,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
        })
}

#[cfg(all(test, feature = "modules"))]
#[path = "zip_extract_tests.rs"]
mod tests;
