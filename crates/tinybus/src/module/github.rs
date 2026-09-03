//! GitHub release acquisition for verified dynamic modules. Feature `modules`.
//!
//! Two paths share one verification. [`acquire`] is the original: fetch,
//! verify, extract into a temporary directory the caller keeps alive for the
//! process. [`acquire_cached`] is what a long-lived host wants: the same
//! verification into a directory that survives the process and is found again
//! on the next launch without the network — see [`super::cache`].
//!
//! # Why the client carries timeouts
//!
//! `ureq`'s default agent has none, and a TCP connect to an address that drops
//! SYNs waits for the operating system to give up — 75 seconds on macOS, longer
//! on Linux — before the next address is tried. GitHub's asset CDN publishes
//! several addresses and a resolver hands them out in a fixed order, so one
//! unreachable address cost every download that full wait, once per module, on
//! every launch. With a connect budget the client divides it across the
//! addresses it was given and moves on to one that answers.
//!
//! # Why asset URLs are built rather than looked up
//!
//! A release asset lives at a fixed path, `releases/download/<tag>/<asset>`.
//! Looking it up through the REST API costs an unauthenticated request against
//! a budget of sixty per hour per address — shared by everyone behind one NAT —
//! and a refusal there is cached by a host as a terminal failure. The API is
//! now the fallback for a release whose direct path answers 404, and nothing
//! else reaches it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::module::cache;
use crate::module::hash::file_hex;

/// Largest release asset this loader will store.
const MAX_RELEASE_BYTES: u64 = 512 * 1024 * 1024;
/// Largest checksum manifest or API listing this loader will read.
const MAX_METADATA_BYTES: u64 = 4 * 1024 * 1024;
const USER_AGENT: &str = "tinybus-module-loader";
/// Manifest names a release may publish, in the order they are tried.
const CHECKSUM_NAMES: [&str; 2] = ["checksum.toml", "checksum.json"];

/// Budgets for one HTTP exchange. The connect budget is what a stalled address
/// costs; it is shared across the resolved addresses, so a dead first address
/// costs roughly half of it rather than the operating system's SYN timeout.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const RECV_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bulk transfer of an archive on whatever link the user has. Generous because
/// a slow link is a fact, not a fault; the other budgets bound the stalls.
const RECV_BODY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// A pinned release a host wants loaded through a persistent, verified cache.
#[derive(Debug, Clone)]
pub struct CachedRelease<'a> {
    /// The `https://github.com/<owner>/<repo>/releases/tag/<tag>` page.
    pub release_url: &'a str,
    /// The archive to load from that release.
    pub asset_name: &'a str,
    /// The archive digest compiled into the host, if it pins one. Checked
    /// against the release's own manifest and against the bytes.
    pub expected_sha256: Option<&'a str>,
    /// Where the verified archive and its extraction live between launches.
    /// One directory per module version; the host names it.
    pub cache_dir: &'a Path,
    /// Whether a cache miss may reach the network. `false` makes a miss a
    /// refusal, for a host whose operator disabled downloads.
    pub allow_download: bool,
}

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

/// Where one release's manifest and archive were found.
struct Located {
    manifest_name: String,
    manifest: Vec<u8>,
    archive_url: String,
}

/// Download and extract one platform module from a GitHub release into a
/// temporary directory the caller must keep alive while the module is mapped.
pub(crate) fn acquire(
    release_url: &str,
    asset_name: &str,
    expected_sha256: Option<&str>,
) -> Result<(tempfile::TempDir, PathBuf)> {
    let (owner, repo, tag) = parse_release_url(release_url)?;
    let agent = agent();
    let located = locate(&agent, owner, repo, tag, asset_name)?;
    let manifest_sha = manifest_digest(&located, asset_name, expected_sha256)?;

    let temp =
        tempfile::tempdir().map_err(|_| refused("module extraction directory is unavailable"))?;
    let archive_path = temp.path().join(asset_name);
    download(&agent, &located.archive_url, &archive_path)?;
    verify_archive(&archive_path, &manifest_sha)?;
    extract(asset_name, &archive_path, temp.path())?;
    let module = canonical_module(cache::find_module(temp.path())?)?;
    Ok((temp, module))
}

/// Load one platform module from a GitHub release through a persistent cache.
///
/// Answers from `cache_dir` when it holds the archive intact — verified against
/// the host's pin, or the recorded manifest digest when there is none — and
/// exactly one module. Otherwise, and only when `allow_download` permits,
/// downloads into a staging directory beside `cache_dir`, verifies, extracts,
/// and commits it with one rename. Returns the module's canonical path and the
/// archive digest that was verified, for the caller to carry into attestation.
///
/// # Errors
///
/// Returns an error if the release URL is malformed, the cache misses while
/// downloads are disabled, the release cannot be fetched or fails verification,
/// or the cache directory cannot be written.
pub(crate) fn acquire_cached(release: &CachedRelease<'_>) -> Result<(PathBuf, String)> {
    let (owner, repo, tag) = parse_release_url(release.release_url)?;
    if let Some(hit) = cache::find_verified(
        release.cache_dir,
        release.asset_name,
        release.expected_sha256,
    ) {
        info!(
            asset = release.asset_name,
            "release loaded from the local cache"
        );
        return Ok((hit.module, hit.sha256));
    }
    if !release.allow_download {
        return Err(refused("release is not cached and downloads are disabled"));
    }

    let agent = agent();
    let located = locate(&agent, owner, repo, tag, release.asset_name)?;
    let manifest_sha = manifest_digest(&located, release.asset_name, release.expected_sha256)?;

    let staging = cache::stage(release.cache_dir)?;
    let archive_path = staging.path().join(release.asset_name);
    download(&agent, &located.archive_url, &archive_path)?;
    verify_archive(&archive_path, &manifest_sha)?;
    extract(release.asset_name, &archive_path, staging.path())?;
    let module = cache::find_module(staging.path())?;
    let relative = module
        .strip_prefix(staging.path())
        .map_err(|_| refused("release module is outside its staging directory"))?
        .to_path_buf();
    cache::write_digest_marker(staging.path(), release.asset_name, &manifest_sha)?;

    if let Err(error) = cache::commit(staging, release.cache_dir) {
        // Another process may have committed the same release first; its files
        // are as good as ours if they verify. Otherwise the failure stands.
        if let Some(hit) = cache::find_verified(
            release.cache_dir,
            release.asset_name,
            release.expected_sha256,
        ) {
            warn!(
                asset = release.asset_name,
                "release cache: commit lost a race; using the concurrent fill"
            );
            return Ok((hit.module, hit.sha256));
        }
        return Err(error);
    }
    info!(
        asset = release.asset_name,
        "release downloaded, verified and cached"
    );
    let module = canonical_module(release.cache_dir.join(relative))?;
    Ok((module, manifest_sha))
}

/// One agent per acquisition, so the three requests share a connection pool
/// and every one of them carries the same budgets.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_resolve(Some(RESOLVE_TIMEOUT))
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_send_request(Some(SEND_REQUEST_TIMEOUT))
        .timeout_recv_response(Some(RECV_RESPONSE_TIMEOUT))
        .timeout_recv_body(Some(RECV_BODY_TIMEOUT))
        .build()
        .new_agent()
}

/// Find the release's checksum manifest and the archive's URL.
///
/// The direct download path is tried first, for each manifest name a release
/// may use. Only a 404 there falls back to the REST listing; any other failure
/// is reported, because cascading a stalled connection into further requests
/// would multiply the wait the budgets exist to bound.
fn locate(
    agent: &ureq::Agent,
    owner: &str,
    repo: &str,
    tag: &str,
    asset_name: &str,
) -> Result<Located> {
    for name in CHECKSUM_NAMES {
        let url = cache::release_download_url(owner, repo, tag, name);
        match send(agent, &url, None) {
            Ok(response) => {
                debug!(
                    manifest = name,
                    "release manifest found at its download path"
                );
                return Ok(Located {
                    manifest_name: name.to_string(),
                    manifest: read_metadata(response)?,
                    archive_url: cache::release_download_url(owner, repo, tag, asset_name),
                });
            }
            Err(ureq::Error::StatusCode(404)) => continue,
            Err(error) => {
                warn!(error = %error, "release manifest could not be downloaded");
                return Err(refused("release checksum manifest could not be downloaded"));
            }
        }
    }

    debug!("release manifest not at its download path; listing the release");
    let api_url = format!("https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}");
    let listing = send(agent, &api_url, Some("application/vnd.github+json")).map_err(|error| {
        warn!(error = %error, "release listing could not be downloaded");
        refused("GitHub release metadata could not be downloaded")
    })?;
    let release: Release = serde_json::from_slice(&read_metadata(listing)?)
        .map_err(|_| refused("GitHub release metadata is invalid"))?;
    let assets = release
        .assets
        .iter()
        .map(|asset| (asset.name.as_str(), asset))
        .collect::<HashMap<_, _>>();
    let archive = assets
        .get(asset_name)
        .ok_or_else(|| refused("requested release asset is missing"))?;
    let checksum_asset = CHECKSUM_NAMES
        .iter()
        .find_map(|name| assets.get(name))
        .ok_or_else(|| refused("release checksum manifest is missing"))?;
    let manifest = send(agent, &checksum_asset.browser_download_url, None)
        .map_err(|error| {
            warn!(error = %error, "release manifest could not be downloaded");
            refused("release checksum manifest could not be downloaded")
        })
        .and_then(read_metadata)?;
    Ok(Located {
        manifest_name: checksum_asset.name.clone(),
        manifest,
        archive_url: archive.browser_download_url.clone(),
    })
}

/// The archive digest the manifest publishes, checked against the host's pin.
fn manifest_digest(
    located: &Located,
    asset_name: &str,
    expected_sha256: Option<&str>,
) -> Result<String> {
    let checksums = parse_checksums(&located.manifest_name, &located.manifest)?;
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
    Ok(manifest_sha.to_ascii_lowercase())
}

fn send(
    agent: &ureq::Agent,
    url: &str,
    accept: Option<&str>,
) -> std::result::Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let mut request = agent.get(url).header("User-Agent", USER_AGENT);
    if let Some(accept) = accept {
        request = request.header("Accept", accept);
    }
    request.call()
}

/// Read a small response — a manifest or a listing — into memory.
fn read_metadata(response: ureq::http::Response<ureq::Body>) -> Result<Vec<u8>> {
    let mut body = response.into_body();
    let bytes = body
        .with_config()
        .limit(MAX_METADATA_BYTES + 1)
        .read_to_vec()
        .map_err(|_| refused("GitHub release metadata could not be read"))?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err(refused("GitHub release metadata exceeds the size cap"));
    }
    Ok(bytes)
}

/// Stream an archive to `destination`, never holding it in memory.
fn download(agent: &ureq::Agent, url: &str, destination: &Path) -> Result<()> {
    let response = send(agent, url, None).map_err(|error| {
        warn!(error = %error, "release asset could not be downloaded");
        refused("GitHub release asset could not be downloaded")
    })?;
    let mut body = response.into_body();
    let mut reader = body.with_config().limit(MAX_RELEASE_BYTES + 1).reader();
    let mut file = std::fs::File::create(destination)
        .map_err(|_| refused("release asset could not be stored"))?;
    let written = std::io::copy(&mut reader, &mut file)
        .map_err(|_| refused("GitHub release asset could not be read"))?;
    if written > MAX_RELEASE_BYTES {
        return Err(refused("GitHub release asset exceeds the 512 MiB size cap"));
    }
    debug!(bytes = written, "release asset downloaded");
    Ok(())
}

fn verify_archive(archive_path: &Path, manifest_sha: &str) -> Result<()> {
    let actual = file_hex(
        std::fs::File::open(archive_path)
            .map_err(|_| refused("release asset could not be read"))?,
    )
    .map_err(|_| refused("release asset hash could not be read"))?;
    if !actual.eq_ignore_ascii_case(manifest_sha) {
        return Err(refused(
            "release asset hash does not match its checksum manifest",
        ));
    }
    Ok(())
}

fn canonical_module(path: PathBuf) -> Result<PathBuf> {
    std::fs::canonicalize(path)
        .map_err(|_| refused("release module path could not be canonicalized"))
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
    fn a_cache_miss_with_downloads_disabled_is_refused_without_the_network() {
        let directory = tempfile::tempdir().unwrap();
        let error = acquire_cached(&CachedRelease {
            release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
            asset_name: "demo-1.0.0.tar.gz",
            expected_sha256: None,
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
    fn a_verified_cache_answers_without_the_network() {
        let directory = tempfile::tempdir().unwrap();
        let cache_dir = directory.path().join("demo").join("1.0.0");
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::write(cache_dir.join("demo-1.0.0.tar.gz"), b"archive").unwrap();
        let module = cache_dir.join(format!("libdemo_module.{}", cache::library_extension()));
        std::fs::write(&module, b"library").unwrap();
        let digest = crate::module::sha256_file(cache_dir.join("demo-1.0.0.tar.gz")).unwrap();

        let (found, sha256) = acquire_cached(&CachedRelease {
            release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
            asset_name: "demo-1.0.0.tar.gz",
            expected_sha256: Some(&digest),
            cache_dir: &cache_dir,
            // Downloads are off, so a hit is the only way this can succeed.
            allow_download: false,
        })
        .unwrap();
        assert_eq!(found, std::fs::canonicalize(module).unwrap());
        assert_eq!(sha256, digest);
    }
}
