//! Recipient attestation: what the broker checked before it will carry a secret.
//!
//! # Why this exists
//!
//! A confidential message — a private key, a recovery phrase, a session token —
//! is only as safe as the identity of whoever receives it. The bus already
//! guarantees that a method call reaches exactly one peer, but "exactly one
//! peer" is not a security property when any process that got to the socket
//! first could be holding the well-known name. This module is the missing half:
//! before the broker will deliver a message marked confidential, it must have
//! independently established *what binary* is on the receiving end.
//!
//! # What "independently" means, and what it does not
//!
//! The hash is computed by the broker, over bytes the broker read itself, and
//! compared against a store the operator installed. A peer is never asked what
//! it is; it could only lie. That is the whole reason the check lives here and
//! not in a handshake.
//!
//! What this is *not* is a signature. The trust store is a list of hashes an
//! operator put on disk, so the guarantee is "this is the artifact the operator
//! allowlisted", not "a release key vouched for this artifact". Signed release
//! manifests are the natural next layer and they slot in behind
//! [`TrustStore::verify`] without touching the wire format — the attestation a
//! verified signature produces is the same [`Attestation`] this produces.
//!
//! # Feature gating
//!
//! Always compiled. The routing rule this feeds is a security invariant, and a
//! slim `--no-default-features` broker that silently skipped it would be a
//! downgrade nobody could see from the outside.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::name::BusName;

/// How the broker came to believe a peer is what it claims to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AttestationSource {
    /// An in-process module whose artifact matched `modules.toml` at load time.
    Module,
    /// A peer across a transport whose executable the broker hashed itself.
    Executable,
}

/// The broker's own record of a verified recipient.
///
/// Held by the router against the peer, handed out by `GetAttestation`, and
/// checked on every confidential delivery. It deliberately carries no path: an
/// operator-facing hash and the name it was verified for are enough to audit a
/// decision, and a filesystem layout is not something to broadcast on a bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// The well-known name this artifact was verified *for*.
    ///
    /// Bound to the name rather than floating free, because "some allowlisted
    /// binary is on the bus" is not the question a sender is asking. The
    /// question is whether the binary answering to `…Wallet` is the one the
    /// operator allowlisted for `…Wallet`.
    pub name: BusName,
    /// Lowercase hex SHA-256 of the artifact the broker read.
    pub sha256: String,
    /// Which check produced this record.
    pub source: AttestationSource,
}

/// The operator's list of which artifact may answer to which name.
///
/// Loaded once, at broker construction, and never re-read: a store that
/// reloaded itself would let anyone who can write the file promote a peer
/// mid-session, and the file is exactly as trusted as the operator account.
#[derive(Debug, Clone, Default)]
pub struct TrustStore {
    entries: HashMap<String, String>,
}

impl TrustStore {
    /// An empty store. Nothing is attested, so every confidential message is
    /// refused — the safe direction to fail in.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load a trust store from a flat `name = "sha256"` file.
    ///
    /// ```text
    /// # peers.toml
    /// "ai.tinyhumans.openhuman.Wallet" = "41edece4…"   # 64 lowercase hex digits
    /// ```
    ///
    /// A missing file is an error rather than an empty store. An operator who
    /// pointed the broker at a path that is not there has a typo, and silently
    /// starting a bus on which every confidential send fails is a worse way to
    /// discover it than refusing to start.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path)
            .map_err(|e| Error::path(path, format!("trust store is unreadable: {e}")))?;
        let mut entries = HashMap::new();
        for (key, value) in parse_allowlist(&source) {
            if !is_hex_sha256(&value) {
                // Named, because an operator who fat-fingered a hash needs to
                // know which line — and a hash is not a secret.
                return Err(Error::path(
                    path,
                    format!("entry `{key}` is not a 64-digit hex SHA-256"),
                ));
            }
            BusName::new(&key)?;
            entries.insert(key, value);
        }
        Ok(Self { entries })
    }

    /// Whether any name is attested at all.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The hash the operator expects for `name`, if it is listed.
    pub fn expected(&self, name: &BusName) -> Option<&str> {
        self.entries.get(name.as_str()).map(String::as_str)
    }

    /// Verify the process behind `pid` is the artifact allowlisted for `name`.
    ///
    /// Returns `Ok(None)` when the name is simply not in the store — an
    /// unlisted service is a normal, non-confidential participant, not a fault.
    /// `Err` is reserved for a name that *is* listed and did not match, because
    /// that is either a misconfiguration or an impersonation attempt and an
    /// operator wants to see it either way.
    ///
    /// # Blocking
    ///
    /// Hashes a file. Call it off the runtime's core threads; the broker wraps
    /// it in `spawn_blocking` for exactly this reason.
    pub fn verify(&self, name: &BusName, pid: u32) -> Result<Option<Attestation>> {
        let Some(expected) = self.expected(name) else {
            return Ok(None);
        };
        let Some(executable) = executable_of(pid) else {
            return Err(Error::not_attested(
                name.clone(),
                "the peer's executable could not be identified on this platform",
            ));
        };
        let file = std::fs::File::open(&executable).map_err(|_| {
            Error::not_attested(name.clone(), "the peer's executable is unreadable")
        })?;
        let actual = crate::hash::file_hex(file).map_err(|_| {
            Error::not_attested(name.clone(), "the peer's executable could not be hashed")
        })?;
        if actual != expected {
            return Err(Error::not_attested(
                name.clone(),
                "the peer's executable does not match the trust store",
            ));
        }
        Ok(Some(Attestation {
            name: name.clone(),
            sha256: actual,
            source: AttestationSource::Executable,
        }))
    }
}

/// The executable behind a live pid, asked of the kernel.
///
/// Hashing is portable; *this* is the part that is not. There is no portable way
/// to ask what binary another process is running, and it has to be the kernel
/// that answers — a path the peer supplied would let it nominate any file on the
/// machine as itself, which is the whole check gone.
///
/// Both implementations resolve something fixed at `execve` and not rewritable
/// by the process afterwards, which is what makes hashing the result meaningful
/// rather than advisory.
///
/// `None` means the question could not be answered here, and every
/// executable-backed attestation then fails closed.
fn executable_of(pid: u32) -> Option<PathBuf> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // A kernel-maintained magic link, not a filesystem path the process
        // chose. Reading it follows to the inode that was executed even if the
        // file has since been renamed or deleted.
        std::fs::read_link(format!("/proc/{pid}/exe")).ok()
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        // libproc's `proc_pidpath`, declared rather than pulled in as a crate:
        // it lives in libSystem, which every macOS binary already links, so a
        // dependency to reach one symbol would be exactly the absorption this
        // project exists to avoid — the same reasoning as the CLI's `getuid`.
        unsafe extern "C" {
            fn proc_pidpath(pid: i32, buffer: *mut u8, buffersize: u32) -> i32;
        }
        // PROC_PIDPATHINFO_MAXSIZE, from <sys/proc_info.h>. `proc_pidpath`
        // refuses a smaller buffer outright rather than truncating, so this is
        // a required size and not a guess to grow on.
        const PROC_PIDPATHINFO_MAXSIZE: usize = 4 * 1024;

        let mut buffer = vec![0u8; PROC_PIDPATHINFO_MAXSIZE];
        // SAFETY: the buffer is at least PROC_PIDPATHINFO_MAXSIZE, which is what
        // the call requires, and its length is passed honestly.
        let written = unsafe {
            proc_pidpath(
                i32::try_from(pid).ok()?,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
            )
        };
        // Returns the byte length on success; zero or negative means the pid is
        // gone or unreadable, which fails closed.
        let written = usize::try_from(written).ok().filter(|n| *n > 0)?;
        let path = std::str::from_utf8(&buffer[..written]).ok()?;
        Some(PathBuf::from(path))
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    )))]
    {
        // Windows is the notable gap, and it is blocked upstream rather than
        // here: the named-pipe transport it would need does not exist yet, so
        // there is no peer to identify. `GetNamedPipeClientProcessId` plus
        // `QueryFullProcessImageNameW` is the shape it takes when that lands.
        let _ = pid;
        None
    }
}

/// Whether `value` is exactly 64 lowercase-comparable hex digits.
pub(crate) fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parse the flat `key = "value"` subset shared by `modules.toml` and the peer
/// trust store.
///
/// Deliberately not a TOML parser. The file is two columns of ASCII that an
/// operator hand-edits, and pulling a parser into the kernel's dependency graph
/// to read it would be precisely the absorption this project exists to stop.
/// Section headers are skipped rather than rejected so a store can be embedded
/// in a larger file.
pub(crate) fn parse_allowlist(source: &str) -> impl Iterator<Item = (String, String)> + '_ {
    source.lines().filter_map(|line| {
        let line = line.split('#').next()?.trim();
        if line.is_empty() || line.starts_with('[') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        Some((
            key.trim().trim_matches(['"', '\'']).to_string(),
            value
                .trim()
                .trim_matches(['"', '\''])
                .to_ascii_lowercase()
                .to_string(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(contents: &str) -> (tempfile::TempDir, TrustStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peers.toml");
        std::fs::write(&path, contents).unwrap();
        let store = TrustStore::load(&path).unwrap();
        (dir, store)
    }

    const HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn a_store_reads_names_and_ignores_comments_and_sections() {
        let (_dir, store) = store(&format!(
            "# a comment\n[section]\n\"ai.tinyhumans.openhuman.Wallet\" = \"{HASH}\" # trailing\n\n"
        ));
        assert_eq!(
            store.expected(&BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap()),
            Some(HASH)
        );
        assert!(!store.is_empty());
    }

    #[test]
    fn a_missing_store_refuses_to_start_rather_than_attesting_nothing() {
        let error = TrustStore::load("/nonexistent/peers.toml").unwrap_err();
        assert!(error.to_string().contains("unreadable"), "{error}");
    }

    #[test]
    fn a_malformed_hash_names_the_entry_that_is_wrong() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peers.toml");
        std::fs::write(&path, "\"ai.tinyhumans.openhuman.Wallet\" = \"nope\"\n").unwrap();
        let error = TrustStore::load(&path).unwrap_err();
        assert!(error.to_string().contains("Wallet"), "{error}");
    }

    #[test]
    fn an_invalid_bus_name_in_the_store_is_refused_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peers.toml");
        std::fs::write(&path, format!("\"not a bus name\" = \"{HASH}\"\n")).unwrap();
        assert!(TrustStore::load(&path).is_err());
    }

    #[test]
    fn an_unlisted_name_is_not_attested_and_is_not_an_error() {
        let (_dir, store) = store(&format!(
            "\"ai.tinyhumans.openhuman.Wallet\" = \"{HASH}\"\n"
        ));
        let other = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
        assert_eq!(store.verify(&other, std::process::id()).unwrap(), None);
    }

    #[test]
    fn an_empty_store_attests_nothing() {
        assert!(TrustStore::empty().is_empty());
        assert_eq!(
            TrustStore::empty().expected(&BusName::new("ai.tinyhumans.X").unwrap()),
            None
        );
    }

    #[test]
    fn a_listed_name_whose_binary_does_not_match_is_refused() {
        // This process is certainly not the empty file whose hash is listed.
        let (_dir, store) = store(&format!(
            "\"ai.tinyhumans.openhuman.Wallet\" = \"{HASH}\"\n"
        ));
        let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
        let error = store.verify(&name, std::process::id()).unwrap_err();
        assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_listed_name_matching_its_own_running_binary_attests() {
        let executable = std::fs::read_link(format!("/proc/{}/exe", std::process::id())).unwrap();
        let hash = crate::hash::file_hex(std::fs::File::open(executable).unwrap()).unwrap();
        let (_dir, store) = store(&format!(
            "\"ai.tinyhumans.openhuman.Wallet\" = \"{hash}\"\n"
        ));
        let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
        let attestation = store.verify(&name, std::process::id()).unwrap().unwrap();
        assert_eq!(attestation.sha256, hash);
        assert_eq!(attestation.name, name);
        assert_eq!(attestation.source, AttestationSource::Executable);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_pid_that_is_gone_fails_closed_rather_than_attesting() {
        let (_dir, store) = store(&format!(
            "\"ai.tinyhumans.openhuman.Wallet\" = \"{HASH}\"\n"
        ));
        let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
        // Above the default pid_max, so it cannot name a live process.
        assert!(store.verify(&name, u32::MAX).is_err());
    }
}
