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

use super::refused;
use crate::error::Result;
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
/// How long one read may wait for bytes. It restarts on every read, so a slow
/// link that keeps delivering is never cut off, while a stalled transfer fails
/// within this budget instead of waiting out the whole body ceiling.
const STALL_TIMEOUT: Duration = Duration::from_secs(60);
/// Outer bound on one body transfer. ponytail: a peer that trickles one byte
/// per [`STALL_TIMEOUT`] is only stopped here; resume with `Range` if that
/// ever matters.
const RECV_BODY_CEILING: Duration = Duration::from_secs(60 * 60);

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
    /// The expected extracted-library digest, correlated with the archive in
    /// the release checksum manifest.
    pub expected_library_sha256: Option<&'a str>,
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
    #[serde(default)]
    library_sha256: HashMap<String, String>,
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
    check_asset_name(asset_name)?;
    let (owner, repo, tag) = parse_release_url(release_url)?;
    let agent = agent();
    let located = locate(&agent, owner, repo, tag, asset_name)?;
    let manifest_sha = manifest_digest(&located, asset_name, expected_sha256)?;

    let temp = private_extraction_tempdir()?;
    let archive_path = temp.path().join(asset_name);
    download(&agent, &located.archive_url, &archive_path)?;
    verify_archive(&archive_path, &manifest_sha)?;
    extract_private(asset_name, &archive_path, temp.path())?;
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
    check_asset_name(release.asset_name)?;
    let (owner, repo, tag) = parse_release_url(release.release_url)?;
    if let Some(hit) = cache::find_verified(
        release.cache_dir,
        release.asset_name,
        release.expected_sha256,
    ) {
        verify_library_digest(&hit.module, release.expected_library_sha256)?;
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
    let _manifest_library_sha = manifest_library_digest(
        &located,
        release.asset_name,
        release.expected_library_sha256,
    )?;

    let staging = cache::stage(release.cache_dir)?;
    restrict_extraction_directory(staging.path())?;
    let archive_path = staging.path().join(release.asset_name);
    download(&agent, &located.archive_url, &archive_path)?;
    verify_archive(&archive_path, &manifest_sha)?;
    extract_private(release.asset_name, &archive_path, staging.path())?;
    let module = cache::find_module(staging.path())?;
    verify_library_digest(&module, release.expected_library_sha256)?;
    let relative = module
        .strip_prefix(staging.path())
        .map_err(|_| refused("release module is outside its staging directory"))?
        .to_path_buf();
    cache::write_digest_marker(staging.path(), release.asset_name, &manifest_sha)?;
    commit_or_reuse(staging, release, &relative, manifest_sha)
}

/// Move a verified staging directory into the cache, unless another process
/// filled it while this one downloaded. [`cache::commit`] replaces whatever is
/// there, and a directory that already verifies may hold a module a running
/// sibling has mapped; its files are as good as ours, so they are kept.
///
/// ponytail: a sibling can still commit between the check and the rename; a
/// cross-process `File::lock` closes that window once the Rust floor is 1.89.
fn commit_or_reuse(
    staging: tempfile::TempDir,
    release: &CachedRelease<'_>,
    relative: &Path,
    manifest_sha: String,
) -> Result<(PathBuf, String)> {
    let concurrent_fill = || {
        cache::find_verified(
            release.cache_dir,
            release.asset_name,
            release.expected_sha256,
        )
    };
    if let Some(hit) = concurrent_fill() {
        verify_library_digest(&hit.module, release.expected_library_sha256)?;
        info!(
            asset = release.asset_name,
            "release cache: a concurrent fill already verifies; keeping it"
        );
        return Ok((hit.module, hit.sha256));
    }
    if let Err(error) = cache::commit(staging, release.cache_dir) {
        // Another process may have committed the same release first; its files
        // are as good as ours if they verify. Otherwise the failure stands.
        if let Some(hit) = concurrent_fill() {
            verify_library_digest(&hit.module, release.expected_library_sha256)?;
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

fn verify_library_digest(path: &Path, expected: Option<&str>) -> Result<()> {
    let actual = file_hex(
        std::fs::File::open(path).map_err(|_| refused("release library could not be read"))?,
    )
    .map_err(|_| refused("release library hash could not be read"))?;
    if let Some(expected) = expected {
        validate_hash(expected)?;
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(refused(
                "extracted library hash does not match its release checksum",
            ));
        }
    }
    Ok(())
}

/// One agent per acquisition, so the three requests share a connection pool
/// and every one of them carries the same budgets.
fn agent() -> ureq::Agent {
    agent_with(STALL_TIMEOUT)
}

fn agent_with(stall: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_resolve(Some(RESOLVE_TIMEOUT))
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_send_request(Some(SEND_REQUEST_TIMEOUT))
        .timeout_recv_response(Some(RECV_RESPONSE_TIMEOUT))
        .timeout_per_read(Some(stall))
        .timeout_recv_body(Some(RECV_BODY_CEILING))
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
        .sha256
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

/// The optional extracted-library digest correlated to the same release asset.
fn manifest_library_digest(
    located: &Located,
    asset_name: &str,
    expected: Option<&str>,
) -> Result<Option<String>> {
    let checksums = parse_checksums(&located.manifest_name, &located.manifest)?;
    let Some(digest) = checksums.library_sha256.get(asset_name) else {
        return if expected.is_some() {
            Err(refused(
                "release asset has no library digest in its checksum manifest",
            ))
        } else {
            Ok(None)
        };
    };
    validate_hash(digest)?;
    if let Some(expected) = expected {
        validate_hash(expected)?;
        if !digest.eq_ignore_ascii_case(expected) {
            return Err(refused(
                "host library checksum disagrees with release checksum",
            ));
        }
    }
    Ok(Some(digest.to_ascii_lowercase()))
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
    let written = std::io::copy(&mut reader, &mut file).map_err(|error| {
        let error = ureq::Error::from(error);
        if let ureq::Error::BodyExceedsLimit(_) = error {
            return refused("GitHub release asset exceeds the 512 MiB size cap");
        }
        warn!(error = %error, "release asset could not be read");
        refused("GitHub release asset could not be read")
    })?;
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

fn parse_checksums(name: &str, bytes: &[u8]) -> Result<Checksums> {
    if name.ends_with(".toml") {
        let source = std::str::from_utf8(bytes).map_err(|_| refused("checksum.toml is invalid"))?;
        return toml::from_str(source).map_err(|_| refused("checksum.toml is invalid"));
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| refused("checksum.json is invalid"))?;
    let sha256 = value.get("sha256").unwrap_or(&value).clone();
    let library_sha256 = value
        .get("library_sha256")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    Ok(Checksums {
        sha256: serde_json::from_value(sha256)
            .map_err(|_| refused("checksum.json has an invalid shape"))?,
        library_sha256: serde_json::from_value(library_sha256)
            .map_err(|_| refused("checksum.json has an invalid library digest shape"))?,
    })
}

fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(refused("checksum is not a SHA-256 digest"));
    }
    Ok(())
}

/// Unpacks only regular files, the same entry types the ZIP branch accepts. A
/// symlink or hard link could point a module path outside `destination`, so it
/// is refused. Directory entries are skipped: `unpack_in` creates parents, and
/// an archived directory's metadata must not widen the extraction root.
fn unpack_tar(reader: impl std::io::Read, destination: &Path) -> Result<()> {
    let mut archive = tar::Archive::new(reader);
    let entries = archive
        .entries()
        .map_err(|_| refused("release archive is invalid"))?;
    for entry in entries {
        let mut entry = entry.map_err(|_| refused("release archive is invalid"))?;
        match entry.header().entry_type() {
            tar::EntryType::Directory => {}
            tar::EntryType::Regular | tar::EntryType::Continuous => {
                let inside = entry
                    .unpack_in(destination)
                    .map_err(|_| refused("release archive extraction failed"))?;
                if !inside {
                    return Err(refused("release archive contains an unsafe path"));
                }
            }
            _ => {
                return Err(refused(
                    "release archive contains an unsupported entry type",
                ));
            }
        }
    }
    Ok(())
}

/// Extracts a release archive into `destination`, refusing unsafe paths and entry types.
fn extract(name: &str, archive: &Path, destination: &Path) -> Result<()> {
    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        let file =
            std::fs::File::open(archive).map_err(|_| refused("release archive is unreadable"))?;
        unpack_tar(flate2::read::GzDecoder::new(file), destination)?;
    } else if name.ends_with(".tar") {
        let file =
            std::fs::File::open(archive).map_err(|_| refused("release archive is unreadable"))?;
        unpack_tar(file, destination)?;
    } else if name.ends_with(".zip") {
        super::zip_extract::extract(archive, destination)?;
    } else {
        return Err(refused("release asset is not a supported archive"));
    }
    Ok(())
}

/// Extracts an archive and restores the extraction root's private permissions.
fn extract_private(name: &str, archive: &Path, destination: &Path) -> Result<()> {
    let extraction = extract(name, archive, destination);
    let restriction = restrict_extraction_directory(destination);
    extraction?;
    restriction
}

/// The asset name is joined onto the cache and extraction directories, so it
/// must name one file inside them before anything touches the filesystem.
fn check_asset_name(asset_name: &str) -> Result<()> {
    if cache::is_safe_path_component(asset_name) {
        Ok(())
    } else {
        Err(refused(format!(
            "release asset name `{asset_name}` is not a plain file name"
        )))
    }
}

fn private_extraction_tempdir() -> Result<tempfile::TempDir> {
    let temp = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            tempfile::Builder::new()
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir()
        }
        #[cfg(not(unix))]
        {
            tempfile::tempdir()
        }
    }
    .map_err(|_| refused("module extraction directory is unavailable"))?;
    restrict_extraction_directory(temp.path())?;
    Ok(temp)
}

#[cfg(unix)]
fn restrict_extraction_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| refused("release extraction directory permissions could not be secured"))
}

#[cfg(not(unix))]
fn restrict_extraction_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "github_tests.rs"]
mod tests;
