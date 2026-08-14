//! Recipient attestation: what the broker verified before it will carry a secret.
//!
//! # What a confidential message is for
//!
//! Some payloads are the kind whose disclosure *is* the failure: a private key,
//! a recovery phrase, a bearer token. The recipient of one of those is a
//! **module loaded into the host's address space**, whose artifact the host
//! hashed against the operator's `modules.toml` before `dlopen` ever ran. A
//! secret handed to such a module never crosses a transport, never reaches a
//! separate process, and never touches a socket.
//!
//! Everything else on the bus — services in their own processes, CLI clients,
//! monitors — does not receive secrets. Not "receives them less safely":
//! a confidential message addressed to one is refused. That is the rule, not a
//! platform limitation, which is why there is no peer-identification machinery
//! here and no per-OS code to keep working.
//!
//! # What the check actually establishes
//!
//! An [`Attestation`] records that *this* well-known name is owned by a module
//! whose bytes hashed to *this* SHA-256, and that the operator listed that hash
//! as acceptable. The hash is computed by the host over bytes the host read
//! itself; nothing a module says about itself participates.
//!
//! # What it does not establish, and this matters
//!
//! An in-process module shares the host's address space. It can read host
//! memory directly, so a *malicious loaded module* is not contained by any
//! routing rule — it never needed the bus to reach a secret in the first place.
//! This is the invariant CLAUDE.md already states: in-process modules are
//! inside the trust boundary.
//!
//! What attestation buys is therefore **admission control**, not isolation:
//! only code whose hash an operator allowlisted is loaded at all, and only such
//! code is handed a secret through the bus. The bus's job is to refuse to be
//! the delivery mechanism for anything else. An integration whose compromise
//! must not reach the kernel's secrets belongs in a separate process — where it
//! is, by this design, ineligible to receive them.
//!
//! # Not a signature, yet
//!
//! `modules.toml` is a list of hashes an operator put on disk, so an
//! attestation means "this is the artifact the operator allowlisted", not "a
//! release key vouched for it". Signed release manifests are the natural next
//! layer: verification would produce this same [`Attestation`] and needs no
//! wire-format change.
//!
//! # Feature gating
//!
//! The type and the routing rule that consumes it are always compiled. A slim
//! `--no-default-features` broker still refuses confidential delivery to
//! everything, which is the correct answer for a build that cannot load a
//! module at all — and a silent downgrade is exactly what must not happen.

use serde::{Deserialize, Serialize};

use crate::name::BusName;

/// The host's own record of a recipient it verified.
///
/// Held by the router against the peer, served by the bus's `GetAttestation`,
/// and checked on every confidential delivery. It carries no path: the hash and
/// the name it was verified for are enough to audit the decision, and a
/// filesystem layout is not something to publish on a bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// The well-known name this artifact was verified *for*.
    ///
    /// Bound to the name rather than floating free, because "some allowlisted
    /// artifact is loaded" is not the question a sender is asking. The question
    /// is whether the code answering to `…Wallet` is what the operator
    /// allowlisted for `…Wallet`.
    pub name: BusName,
    /// Lowercase hex SHA-256 of the bytes the operator vouched for.
    ///
    /// For a module loaded from disk that is the library file itself, hashed
    /// against a `modules.toml` beside it. For one loaded from a pinned GitHub
    /// release it is the release *archive* the library was extracted from —
    /// the artifact named by the digest the host compiled in, and the only
    /// value in that path any operator ever asserted. Hashing the extracted
    /// library instead would report a number nobody had vouched for, computed
    /// by the same code that would then be trusting it.
    ///
    /// A sender that pinned the digest itself can therefore compare this
    /// against its own copy before parting with a secret, rather than taking
    /// the host's word that some check happened.
    pub sha256: String,
}

/// Whether `value` is exactly 64 hex digits.
#[cfg(feature = "modules")]
pub(crate) fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parse the flat `key = "value"` subset that `modules.toml` is written in.
///
/// Deliberately not a TOML parser. The file is two columns of ASCII that an
/// operator hand-edits, and pulling a parser into the kernel's dependency graph
/// to read it would be precisely the absorption this project exists to stop.
/// Section headers are skipped rather than rejected, so the allowlist can live
/// inside a larger file.
#[cfg(feature = "modules")]
pub(crate) fn parse_allowlist(source: &str) -> impl Iterator<Item = (String, String)> + '_ {
    source.lines().filter_map(|line| {
        let line = line.split('#').next()?.trim();
        if line.is_empty() || line.starts_with('[') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        Some((
            key.trim().trim_matches(['"', '\'']).to_string(),
            value.trim().trim_matches(['"', '\'']).to_ascii_lowercase(),
        ))
    })
}

#[cfg(all(test, feature = "modules"))]
mod tests {
    use super::*;

    #[test]
    fn an_allowlist_reads_entries_and_ignores_comments_and_sections() {
        let source = "# a comment\n[section]\n\"clock.so\" = \"AABB\" # trailing\n\n";
        let entries: Vec<_> = parse_allowlist(source).collect();
        assert_eq!(entries, vec![("clock.so".to_string(), "aabb".to_string())]);
    }

    #[test]
    fn an_allowlist_line_without_an_assignment_is_skipped_rather_than_guessed_at() {
        assert_eq!(parse_allowlist("garbage\n").count(), 0);
    }

    #[test]
    fn only_a_full_length_hex_digest_counts_as_a_hash() {
        assert!(is_hex_sha256(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        // Too short, and a plausible-looking typo that must not be accepted as
        // a digest — the allowlist is the only thing standing between an
        // arbitrary artifact and a private key.
        assert!(!is_hex_sha256("e3b0c442"));
        assert!(!is_hex_sha256(&"z".repeat(64)));
    }
}
