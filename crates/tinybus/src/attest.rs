//! Recipient attestation: what the broker verified before it will carry a secret.
//!
//! # What a confidential message is for
//!
//! Some payloads are the kind whose disclosure *is* the failure: a private key,
//! a recovery phrase, a bearer token. The recipient of one of those is a
//! **module loaded into the host's address space**, whose artifact the host
//! verified before `dlopen`, or whose code was linked into the host. A
//! secret handed to such a module never reaches a separate process or socket.
//! An explicit `ModuleHost` capability also permits exact confidential callbacks
//! from one verified loaded instance to an SDK-created local host connection.
//! That scoped exception is not artifact attestation and grants no permission
//! to aliases, ordinary connections, custom transports or other module instances.
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
//! whose admitted artifact hashed to *this* SHA-256. Release modules keep the
//! archive digest there and may carry a separately verified library digest;
//! local allowlisted libraries keep their file digest there. The host computes
//! these values from protected files or the verified release cache; nothing a
//! module says about itself participates.
//!
//! Admission also binds that record to the module's fixed unique peer. A unique
//! lookup retains the original manifest name and digest even after alias release;
//! extra aliases and successor peers inherit no attestation. Detach removes the
//! record. This recipient check is separate from incoming [`crate::CallContext`]:
//! neither an artifact digest nor a peer name authenticates a human.
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
//! code or its explicitly scoped local host callback is handed a secret through
//! the bus. The bus refuses all other recipient classes. An integration whose compromise
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
///
/// Querying an admitted unique peer returns its original manifest identity;
/// `name` does not become the unique destination used for that query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// The original well-known name this artifact was verified *for*.
    ///
    /// Bound to the name rather than floating free, because "some allowlisted
    /// artifact is loaded" is not the question a sender is asking. The question
    /// is whether the code answering to `…Wallet` is what the operator
    /// allowlisted for `…Wallet`.
    pub name: BusName,
    /// Lowercase hex SHA-256 of the loaded artifact or linked host executable.
    ///
    /// For a module loaded from disk that is the library file itself, hashed
    /// against a `modules.toml` beside it. For one loaded from a pinned GitHub
    /// release it is the release *archive* the library was extracted from —
    /// the artifact named by the digest the host compiled in, and the only
    /// value in that path any operator ever asserted for the artifact. The
    /// optional [`Self::library_sha256`] carries a separately published and
    /// checked library digest without changing this archive-digest meaning.
    ///
    /// A linked module reports the SHA-256 of the host executable containing
    /// its code, not a release archive pin. A sender of a secret must recognize
    /// linked registration explicitly before accepting that digest.
    pub sha256: String,
    /// Lowercase hex SHA-256 of the extracted library, when the host verified
    /// one separately from the admitted artifact digest.
    ///
    /// GitHub release attestations keep [`Self::sha256`] bound to the pinned
    /// archive and carry this value for the extracted library. Legacy peers
    /// omit it, which leaves strict library-pinned calls ineligible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_sha256: Option<String>,
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
#[path = "attest_tests.rs"]
mod tests;
