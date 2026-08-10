//! GitHub release acquisition for verified dynamic modules.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::module::hash::file_hex;

const MAX_RELEASE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct Release {
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Deserialize)]
struct Checksums {
    #[serde(default)]
    sha256: HashMap<String, String>,
}

/// Download and extract one platform module from a GitHub release.
pub(crate) fn acquire(
    release_url: &str,
    asset_name: &str,
    expected_sha256: Option<&str>,
) -> Result<(tempfile::TempDir, PathBuf)> {
    let (owner, repo, tag) = parse_release_url(release_url)?;
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}");
    let release: Release = get_json(&api_url)?;
    let assets = release
        .assets
        .iter()
        .map(|asset| (asset.name.as_str(), asset))
        .collect::<HashMap<_, _>>();
    let archive = assets
        .get(asset_name)
        .ok_or_else(|| refused("requested release asset is missing"))?;
    let checksum_asset = ["checksum.toml", "checksum.json"]
        .iter()
        .find_map(|name| assets.get(name))
        .ok_or_else(|| refused("release checksum manifest is missing"))?;
    let checksum_bytes = get_bytes(&checksum_asset.browser_download_url)?;
    let checksums = parse_checksums(checksum_asset.name.as_str(), &checksum_bytes)?;
    let manifest_sha = checksums
        .get(asset_name)
        .ok_or_else(|| refused("release asset is absent from its checksum manifest"))?;
    validate_hash(manifest_sha)?;
    if let Some(expected) = expected_sha256 {
        validate_hash(expected)?;
        if !manifest_sha.eq_ignore_ascii_case(expected) {
            return Err(refused("host checksum disagrees with release checksum"));
        }
    }

    let archive_bytes = get_bytes(&archive.browser_download_url)?;
    let temp =
        tempfile::tempdir().map_err(|_| refused("module extraction directory is unavailable"))?;
    let archive_path = temp.path().join(asset_name);
    std::fs::write(&archive_path, &archive_bytes)
        .map_err(|_| refused("release asset could not be stored"))?;
    let actual = file_hex(
        std::fs::File::open(&archive_path)
            .map_err(|_| refused("release asset could not be read"))?,
    )
    .map_err(|_| refused("release asset hash could not be read"))?;
    if !actual.eq_ignore_ascii_case(manifest_sha) {
        return Err(refused(
            "release asset hash does not match its checksum manifest",
        ));
    }
    extract(asset_name, &archive_path, temp.path())?;
    let module = find_module(temp.path())?;
    Ok((temp, module))
}

fn parse_release_url(url: &str) -> Result<(&str, &str, &str)> {
    let prefix = "https://github.com/";
    let rest = url
        .strip_prefix(prefix)
        .ok_or_else(|| refused("GitHub release URL must use https"))?;
    let mut parts = rest.trim_end_matches('/').split('/');
    let owner = parts.next().filter(|value| !value.is_empty());
    let repo = parts.next().filter(|value| !value.is_empty());
    if parts.next() != Some("releases") || parts.next() != Some("tag") {
        return Err(refused("GitHub URL must point to a release tag"));
    }
    let tag = parts.next().filter(|value| !value.is_empty());
    if parts.next().is_some() {
        return Err(refused("GitHub release URL has an invalid path"));
    }
    match (owner, repo, tag) {
        (Some(owner), Some(repo), Some(tag)) => Ok((owner, repo, tag)),
        _ => Err(refused("GitHub release URL is incomplete")),
    }
}

fn get_json<T: for<'de> Deserialize<'de>>(url: &str) -> Result<T> {
    let response = ureq::get(url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "tinybus-module-loader")
        .call()
        .map_err(|_| refused("GitHub release metadata could not be downloaded"))?;
    response
        .into_body()
        .read_to_string()
        .map_err(|_| refused("GitHub release metadata could not be read"))
        .and_then(|body| {
            serde_json::from_str(&body).map_err(|_| refused("GitHub release metadata is invalid"))
        })
}

fn get_bytes(url: &str) -> Result<Vec<u8>> {
    let response = ureq::get(url)
        .header("User-Agent", "tinybus-module-loader")
        .call()
        .map_err(|_| refused("GitHub release asset could not be downloaded"))?;
    let mut body = response.into_body();
    let bytes = body
        .with_config()
        .limit(MAX_RELEASE_BYTES + 1)
        .read_to_vec()
        .map_err(|_| refused("GitHub release asset could not be read"))?;
    if bytes.len() as u64 > MAX_RELEASE_BYTES {
        return Err(refused("GitHub release asset exceeds the 512 MiB size cap"));
    }
    Ok(bytes)
}

fn parse_checksums(name: &str, bytes: &[u8]) -> Result<HashMap<String, String>> {
    if name.ends_with(".toml") {
        let source = std::str::from_utf8(bytes).map_err(|_| refused("checksum.toml is invalid"))?;
        let parsed: Checksums =
            toml::from_str(source).map_err(|_| refused("checksum.toml is invalid"))?;
        return Ok(parsed.sha256);
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| refused("checksum.json is invalid"))?;
    let object = value.get("sha256").unwrap_or(&value);
    serde_json::from_value(object.clone())
        .map_err(|_| refused("checksum.json has an invalid shape"))
}

fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(refused("checksum is not a SHA-256 digest"));
    }
    Ok(())
}

fn extract(name: &str, archive: &Path, destination: &Path) -> Result<()> {
    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        let file =
            std::fs::File::open(archive).map_err(|_| refused("release archive is unreadable"))?;
        let decoder = flate2::read::GzDecoder::new(file);
        tar::Archive::new(decoder)
            .unpack(destination)
            .map_err(|_| refused("release archive extraction failed"))?;
    } else if name.ends_with(".tar") {
        let file =
            std::fs::File::open(archive).map_err(|_| refused("release archive is unreadable"))?;
        tar::Archive::new(file)
            .unpack(destination)
            .map_err(|_| refused("release archive extraction failed"))?;
    } else if name.ends_with(".zip") {
        let file =
            std::fs::File::open(archive).map_err(|_| refused("release archive is unreadable"))?;
        let mut zip =
            zip::ZipArchive::new(file).map_err(|_| refused("release archive is invalid"))?;
        for index in 0..zip.len() {
            let mut entry = zip
                .by_index(index)
                .map_err(|_| refused("release archive is invalid"))?;
            let Some(relative) = entry.enclosed_name() else {
                return Err(refused("release archive contains an unsafe path"));
            };
            let target = destination.join(relative);
            if entry.is_dir() {
                std::fs::create_dir_all(&target)
                    .map_err(|_| refused("release archive extraction failed"))?;
            } else {
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|_| refused("release archive extraction failed"))?;
                }
                let mut output = std::fs::File::create(&target)
                    .map_err(|_| refused("release archive extraction failed"))?;
                std::io::copy(&mut entry, &mut output)
                    .map_err(|_| refused("release archive extraction failed"))?;
            }
        }
    } else {
        return Err(refused("release asset is not a supported archive"));
    }
    Ok(())
}

fn find_module(root: &Path) -> Result<PathBuf> {
    let mut found = Vec::new();
    collect_modules(root, &mut found)
        .map_err(|_| refused("release archive could not be inspected"))?;
    match found.as_slice() {
        [module] => Ok(module.clone()),
        [] => Err(refused("release archive contains no platform module")),
        _ => Err(refused(
            "release archive contains multiple platform modules",
        )),
    }
}

fn collect_modules(path: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_modules(&path, found)?;
        } else if path.extension().and_then(|value| value.to_str())
            == Some(if cfg!(windows) {
                "dll"
            } else if cfg!(target_os = "macos") {
                "dylib"
            } else {
                "so"
            })
        {
            found.push(path);
        }
    }
    Ok(())
}

fn refused(reason: impl Into<String>) -> Error {
    Error::module_refused(Path::new("github-release"), reason)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            parse_checksums("checksum.toml", toml.as_bytes()).unwrap()["module.tar.gz"],
            expected
        );
        let json = format!(
            r###"{{"sha256":{{"module.tar.gz":"{}"}}}}"###,
            "b".repeat(64)
        );
        assert_eq!(
            parse_checksums("checksum.json", json.as_bytes()).unwrap()["module.tar.gz"],
            "b".repeat(64)
        );
    }
}
