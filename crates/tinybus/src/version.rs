//! Peer version negotiation: who is on the bus, what they speak, what they accept.
//!
//! # The problem this solves
//!
//! Every peer on the bus is built separately. The kernel ships on its own
//! cadence; an integration is a different repository with a different release
//! schedule; a user may be running one of each from three months apart. Until
//! now the only thing tinybus said about that was "rename the interface on a
//! breaking change" — a rule with no enforcement, whose failure mode is a
//! decode error at the first call, in the *caller's* logs, blaming JSON.
//!
//! So each peer declares a [`PeerManifest`]: its own identity and version, and
//! for every interface it touches, the version it **speaks** and the range it
//! **accepts** from others. The broker holds those manifests and hands them
//! out, so a peer can find out whether it can talk to another peer *before*
//! sending anything, and can say precisely why not when it cannot.
//!
//! ```text
//! kernel                          voice service
//!   speaks  Voice 2.1.0             speaks  Voice 2.3.0
//!   accepts Voice >=2.0.0, <3.0.0   accepts Voice >=2.0.0, <3.0.0
//!                    ↓ compatible both ways ↓
//!
//! kernel                          wallet service
//!   speaks  Wallet 1.4.0            speaks  Wallet 2.0.0
//!   accepts Wallet >=1.0.0, <2.0.0  accepts Wallet >=2.0.0, <3.0.0
//!                    ↓ IncompatibleVersion, named at startup ↓
//! ```
//!
//! # Why the check is two-sided
//!
//! It would be simpler to compare one version against one range. But the two
//! peers are not symmetric in what they know: a caller can be newer than a
//! service *or* older, and only the party that is behind knows what it cannot
//! parse. Checking `a.speaks ∈ b.accepts` **and** `b.speaks ∈ a.accepts` is
//! what catches both directions; checking one catches half the skew and gives
//! false confidence about the rest.
//!
//! # Why a hand-rolled version type
//!
//! `semver` is a good crate and this is a deliberate subset of it: three
//! numbers and a half-open range. In a project whose entire purpose is removing
//! dependencies from a graph, adding one to compare three integers is the wrong
//! trade — and the subset is not a simplification of convenience, it is the
//! whole of what a range check on the bus needs. Pre-release and build metadata
//! are parsed and preserved so a version string round-trips, but they are not
//! ordered: a peer that needs to distinguish `2.0.0-rc1` from `2.0.0` is
//! describing two interfaces, not one.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::name::{BusName, InterfaceName};

/// A `major.minor.patch` version, with optional trailing metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Version {
    /// Breaking changes.
    pub major: u64,
    /// Backward-compatible additions.
    pub minor: u64,
    /// Backward-compatible fixes.
    pub patch: u64,
    /// Anything after `-` or `+`, preserved verbatim and ignored for ordering.
    pub tag: Option<String>,
}

impl Version {
    /// A version with no tag.
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            tag: None,
        }
    }

    /// Parse `major.minor.patch[-tag][+build]`.
    pub fn parse(input: &str) -> Result<Self> {
        let invalid = |reason: &str| Error::InvalidName {
            kind: "version",
            input: input.to_string(),
            reason: reason.to_string(),
        };

        // Split the tag off first so `1.2.3-rc.1` does not look like five
        // dot-separated numbers.
        let (core, tag) = match input.find(['-', '+']) {
            Some(index) => (&input[..index], Some(input[index + 1..].to_string())),
            None => (input, None),
        };

        let parts: Vec<&str> = core.split('.').collect();
        if parts.len() != 3 {
            return Err(invalid("expected major.minor.patch"));
        }
        let mut numbers = [0u64; 3];
        for (slot, part) in numbers.iter_mut().zip(parts) {
            *slot = part
                .parse::<u64>()
                .map_err(|_| invalid("each component must be a number"))?;
        }
        Ok(Self {
            major: numbers[0],
            minor: numbers[1],
            patch: numbers[2],
            tag,
        })
    }

    /// The first version that breaks compatibility with this one.
    ///
    /// `0.x` treats every minor as breaking, which is the convention pre-1.0
    /// crates actually follow.
    pub fn next_breaking(&self) -> Version {
        if self.major == 0 {
            Version::new(0, self.minor + 1, 0)
        } else {
            Version::new(self.major + 1, 0, 0)
        }
    }

    /// The range a **consumer** of this version accepts: anything from `self`
    /// up to the next breaking version.
    ///
    /// Lower-bounded at `self` because a caller written against 2.3 may use
    /// something 2.1 does not implement. This is semver's caret rule.
    pub fn caret(&self) -> VersionRange {
        VersionRange {
            min: Version::new(self.major, self.minor, self.patch),
            max_exclusive: Some(self.next_breaking()),
        }
    }

    /// The range a **provider** at this version accepts: the whole compatible
    /// series, not just versions at or above its own.
    ///
    /// The asymmetry with [`Version::caret`] is the whole point, and getting it
    /// wrong is the obvious mistake: minor versions are *additions*, so a
    /// provider at 2.3 serves a caller written against 2.0 perfectly well — the
    /// caller simply uses less of it. Defaulting a provider to caret would
    /// reject every client older than itself, which is every client, one
    /// release later.
    pub fn compatible_series(&self) -> VersionRange {
        let min = if self.major == 0 {
            Version::new(0, self.minor, 0)
        } else {
            Version::new(self.major, 0, 0)
        };
        VersionRange {
            min,
            max_exclusive: Some(self.next_breaking()),
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        // `tag` is deliberately not compared; see the module docs.
        (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(tag) = &self.tag {
            write!(f, "-{tag}")?;
        }
        Ok(())
    }
}

impl FromStr for Version {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl TryFrom<String> for Version {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::parse(&s)
    }
}

impl From<Version> for String {
    fn from(v: Version) -> String {
        v.to_string()
    }
}

/// A half-open version range: `min <= v < max_exclusive`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct VersionRange {
    /// Inclusive lower bound.
    pub min: Version,
    /// Exclusive upper bound. `None` means unbounded above.
    pub max_exclusive: Option<Version>,
}

impl VersionRange {
    /// A range accepting everything from `min` up to but excluding `max`.
    pub fn new(min: Version, max_exclusive: Option<Version>) -> Self {
        Self { min, max_exclusive }
    }

    /// Everything at or above `min`.
    pub fn at_least(min: Version) -> Self {
        Self {
            min,
            max_exclusive: None,
        }
    }

    /// Whether `version` falls inside.
    pub fn accepts(&self, version: &Version) -> bool {
        if version < &self.min {
            return false;
        }
        match &self.max_exclusive {
            Some(max) => version < max,
            None => true,
        }
    }

    /// Parse `>=1.2.0, <2.0.0`, `^1.2.0`, or a bare `1.2.0` (meaning caret).
    ///
    /// A bare version means caret rather than "exactly this", because exact
    /// pinning across a process boundary is almost never what someone means and
    /// is the reading that fails closed on every patch release.
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        let invalid = |reason: &str| Error::InvalidName {
            kind: "version range",
            input: input.to_string(),
            reason: reason.to_string(),
        };

        if let Some(rest) = input.strip_prefix('^') {
            return Ok(Version::parse(rest.trim())?.caret());
        }
        if !input.contains(['<', '>', '=']) {
            return Ok(Version::parse(input)?.caret());
        }

        let mut min = None;
        let mut max_exclusive = None;
        for clause in input.split(',') {
            let clause = clause.trim();
            if let Some(rest) = clause.strip_prefix(">=") {
                min = Some(Version::parse(rest.trim())?);
            } else if let Some(rest) = clause.strip_prefix('<') {
                max_exclusive = Some(Version::parse(rest.trim())?);
            } else {
                return Err(invalid("clauses must be `>=x.y.z` or `<x.y.z`"));
            }
        }
        Ok(Self {
            min: min.ok_or_else(|| invalid("needs a `>=` lower bound"))?,
            max_exclusive,
        })
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ">={}", self.min)?;
        if let Some(max) = &self.max_exclusive {
            write!(f, ", <{max}")?;
        }
        Ok(())
    }
}

impl TryFrom<String> for VersionRange {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::parse(&s)
    }
}

impl From<VersionRange> for String {
    fn from(v: VersionRange) -> String {
        v.to_string()
    }
}

/// What a peer speaks, and will accept, for one interface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceVersion {
    /// The interface this describes.
    pub interface: InterfaceName,
    /// The version this peer implements or calls.
    pub speaks: Version,
    /// The versions of the *other* side this peer can work with.
    pub accepts: VersionRange,
}

impl InterfaceVersion {
    /// Declare an interface this peer **implements**.
    ///
    /// Defaults to accepting the whole compatible series — see
    /// [`Version::compatible_series`] for why that differs from the consumer
    /// default.
    pub fn provided(interface: InterfaceName, version: Version) -> Self {
        Self {
            accepts: version.compatible_series(),
            speaks: version,
            interface,
        }
    }

    /// Declare an interface this peer **calls**.
    ///
    /// Defaults to the caret rule: at least the version it was written
    /// against, below the next breaking one.
    pub fn consumed(interface: InterfaceName, version: Version) -> Self {
        Self {
            accepts: version.caret(),
            speaks: version,
            interface,
        }
    }

    /// Override the accepted range, for a peer that supports more (or less)
    /// than the default implies.
    pub fn with_accepts(mut self, accepts: VersionRange) -> Self {
        self.accepts = accepts;
        self
    }
}

/// Everything a peer says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PeerManifest {
    /// A human-readable identity: the crate or product name.
    #[serde(default)]
    pub name: String,
    /// The peer's own release version, for diagnostics. Not used for matching —
    /// interfaces are what compatibility is actually about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    /// Interfaces this peer implements.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provides: Vec<InterfaceVersion>,
    /// Interfaces this peer calls.
    ///
    /// Declared separately from `provides` because the direction matters for
    /// diagnostics: "nothing provides what you consume" and "nobody consumes
    /// what you provide" are different problems.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumes: Vec<InterfaceVersion>,
}

impl PeerManifest {
    /// A manifest for a peer called `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set the peer's own release version.
    pub fn version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Declare an interface this peer implements.
    pub fn provides(mut self, interface: InterfaceVersion) -> Self {
        self.provides.push(interface);
        self
    }

    /// Declare an interface this peer calls.
    pub fn consumes(mut self, interface: InterfaceVersion) -> Self {
        self.consumes.push(interface);
        self
    }

    /// What this peer says it provides for `interface`.
    pub fn provided(&self, interface: &InterfaceName) -> Option<&InterfaceVersion> {
        self.provides.iter().find(|i| &i.interface == interface)
    }

    /// What this peer says it consumes for `interface`.
    pub fn consumed(&self, interface: &InterfaceName) -> Option<&InterfaceVersion> {
        self.consumes.iter().find(|i| &i.interface == interface)
    }
}

/// The result of checking one peer against another for one interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compatibility {
    /// Both sides accept the other's version.
    Compatible {
        /// What the provider speaks.
        provider_speaks: Version,
        /// What the consumer speaks.
        consumer_speaks: Version,
    },
    /// The provider speaks a version the consumer will not accept.
    ConsumerRejects {
        /// What the provider speaks.
        provider_speaks: Version,
        /// What the consumer will take.
        consumer_accepts: VersionRange,
    },
    /// The consumer speaks a version the provider will not accept.
    ProviderRejects {
        /// What the consumer speaks.
        consumer_speaks: Version,
        /// What the provider will take.
        provider_accepts: VersionRange,
    },
    /// The provider does not declare the interface at all.
    NotProvided,
}

impl Compatibility {
    /// Whether the two peers can talk.
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible { .. })
    }
}

impl fmt::Display for Compatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compatible {
                provider_speaks,
                consumer_speaks,
            } => write!(f, "compatible (provider {provider_speaks}, consumer {consumer_speaks})"),
            Self::ConsumerRejects {
                provider_speaks,
                consumer_accepts,
            } => write!(
                f,
                "the provider speaks {provider_speaks}, which the caller does not accept ({consumer_accepts})"
            ),
            Self::ProviderRejects {
                consumer_speaks,
                provider_accepts,
            } => write!(
                f,
                "the caller speaks {consumer_speaks}, which the provider does not accept ({provider_accepts})"
            ),
            Self::NotProvided => write!(f, "the peer does not declare this interface"),
        }
    }
}

/// Check whether `consumer` can call `interface` on `provider`.
///
/// Both directions are checked; see the module docs for why one is not enough.
/// A consumer that declares nothing for the interface is treated as accepting
/// anything, so adding manifests to an existing bus is incremental rather than
/// a flag day.
pub fn check(
    provider: &PeerManifest,
    consumer: &PeerManifest,
    interface: &InterfaceName,
) -> Compatibility {
    let Some(provided) = provider.provided(interface) else {
        return Compatibility::NotProvided;
    };
    let Some(consumed) = consumer.consumed(interface) else {
        return Compatibility::Compatible {
            provider_speaks: provided.speaks.clone(),
            consumer_speaks: provided.speaks.clone(),
        };
    };

    if !consumed.accepts.accepts(&provided.speaks) {
        return Compatibility::ConsumerRejects {
            provider_speaks: provided.speaks.clone(),
            consumer_accepts: consumed.accepts.clone(),
        };
    }
    if !provided.accepts.accepts(&consumed.speaks) {
        return Compatibility::ProviderRejects {
            consumer_speaks: consumed.speaks.clone(),
            provider_accepts: provided.accepts.clone(),
        };
    }
    Compatibility::Compatible {
        provider_speaks: provided.speaks.clone(),
        consumer_speaks: consumed.speaks.clone(),
    }
}

/// A manifest paired with the peer that published it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    /// The peer's unique name.
    pub peer: BusName,
    /// The well-known names it owns, at the time of the query.
    #[serde(default)]
    pub names: Vec<BusName>,
    /// What it declared.
    pub manifest: PeerManifest,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(name: &str) -> InterfaceName {
        InterfaceName::new(name).unwrap()
    }

    #[test]
    fn versions_parse_and_round_trip() {
        let v = Version::parse("2.3.1").unwrap();
        assert_eq!((v.major, v.minor, v.patch), (2, 3, 1));
        assert_eq!(v.to_string(), "2.3.1");
        assert_eq!(Version::parse("1.0.0-rc.1").unwrap().tag.as_deref(), Some("rc.1"));
        assert_eq!(Version::parse("1.0.0-rc.1").unwrap().to_string(), "1.0.0-rc.1");
    }

    #[test]
    fn malformed_versions_are_refused() {
        assert!(Version::parse("1.2").is_err());
        assert!(Version::parse("1.2.3.4").is_err());
        assert!(Version::parse("1.2.x").is_err());
        assert!(Version::parse("").is_err());
    }

    #[test]
    fn ordering_ignores_the_tag() {
        // A pre-release ordering nobody agreed on is worse than none: a peer
        // that needs to distinguish them is describing two interfaces.
        assert_eq!(
            Version::parse("1.0.0-rc1").unwrap().cmp(&Version::parse("1.0.0").unwrap()),
            Ordering::Equal
        );
        assert!(Version::parse("1.2.0").unwrap() < Version::parse("1.10.0").unwrap());
        assert!(Version::parse("2.0.0").unwrap() > Version::parse("1.99.99").unwrap());
    }

    #[test]
    fn a_provider_accepts_its_whole_series_but_a_consumer_only_looks_forward() {
        // The asymmetry, asserted directly: it is the single easiest thing to
        // get wrong here, and getting it wrong rejects every older client.
        let provider = Version::new(2, 3, 0).compatible_series();
        assert!(provider.accepts(&Version::new(2, 0, 0)), "an older caller still fits");
        assert!(provider.accepts(&Version::new(2, 3, 0)));
        assert!(!provider.accepts(&Version::new(3, 0, 0)));

        let consumer = Version::new(2, 3, 0).caret();
        assert!(!consumer.accepts(&Version::new(2, 0, 0)), "an older provider does not");
        assert!(consumer.accepts(&Version::new(2, 9, 0)));
    }

    #[test]
    fn the_caret_rule_stops_at_the_next_major() {
        let range = Version::new(1, 2, 3).caret();
        assert!(range.accepts(&Version::new(1, 2, 3)));
        assert!(range.accepts(&Version::new(1, 9, 0)));
        assert!(!range.accepts(&Version::new(2, 0, 0)));
        // ...and never below the declared version, because a 1.2.0 client may
        // be using something 1.1.0 does not have.
        assert!(!range.accepts(&Version::new(1, 2, 2)));
    }

    #[test]
    fn zero_x_treats_every_minor_as_breaking() {
        let range = Version::new(0, 3, 1).caret();
        assert!(range.accepts(&Version::new(0, 3, 9)));
        assert!(!range.accepts(&Version::new(0, 4, 0)));
    }

    #[test]
    fn ranges_parse_in_all_three_spellings() {
        let explicit = VersionRange::parse(">=1.2.0, <2.0.0").unwrap();
        assert_eq!(explicit, VersionRange::parse("^1.2.0").unwrap());
        assert_eq!(explicit, VersionRange::parse("1.2.0").unwrap());
        assert_eq!(explicit.to_string(), ">=1.2.0, <2.0.0");

        let open = VersionRange::parse(">=1.0.0").unwrap();
        assert!(open.accepts(&Version::new(99, 0, 0)));
        assert!(VersionRange::parse("<2.0.0").is_err(), "a range needs a lower bound");
        assert!(VersionRange::parse("~1.0.0").is_err());
    }

    fn provider(version: &str) -> PeerManifest {
        PeerManifest::new("voice-service").provides(InterfaceVersion::provided(
            iface("ai.tinyhumans.openhuman.Voice"),
            Version::parse(version).unwrap(),
        ))
    }

    fn consumer(version: &str) -> PeerManifest {
        PeerManifest::new("openhuman").consumes(InterfaceVersion::consumed(
            iface("ai.tinyhumans.openhuman.Voice"),
            Version::parse(version).unwrap(),
        ))
    }

    #[test]
    fn a_newer_compatible_provider_is_accepted() {
        let result = check(
            &provider("2.3.0"),
            &consumer("2.1.0"),
            &iface("ai.tinyhumans.openhuman.Voice"),
        );
        assert!(result.is_compatible(), "{result}");
    }

    #[test]
    fn a_major_bump_in_either_direction_is_caught() {
        let interface = iface("ai.tinyhumans.openhuman.Voice");

        // Provider ran ahead of the caller.
        let ahead = check(&provider("3.0.0"), &consumer("2.1.0"), &interface);
        assert!(!ahead.is_compatible(), "{ahead}");
        assert!(ahead.to_string().contains("does not accept"), "{ahead}");

        // Caller ran ahead of the provider. Both are caught by the consumer's
        // own range — it demands >=3.0.0 and is offered 2.1.0 — so the verdict
        // names the caller's requirement rather than the provider's, which is
        // the side that has to change.
        let behind = check(&provider("2.1.0"), &consumer("3.0.0"), &interface);
        assert!(matches!(behind, Compatibility::ConsumerRejects { .. }), "{behind}");
        assert!(behind.to_string().contains("3.0.0"), "{behind}");
    }

    #[test]
    fn an_older_provider_than_the_caller_needs_is_rejected() {
        // Same major, but the caller was written against 2.5 and the provider
        // only implements 2.1 — the caller may use something 2.1 lacks.
        let result = check(
            &provider("2.1.0"),
            &consumer("2.5.0"),
            &iface("ai.tinyhumans.openhuman.Voice"),
        );
        assert!(matches!(result, Compatibility::ConsumerRejects { .. }), "{result}");
    }

    #[test]
    fn a_provider_that_dropped_old_callers_says_so() {
        // The case only the provider-side check catches: the caller is happy
        // with what is offered, but the provider has narrowed its own support
        // window and will not serve a caller this old.
        let interface = iface("ai.tinyhumans.openhuman.Voice");
        let provider = PeerManifest::new("voice").provides(
            InterfaceVersion::provided(interface.clone(), Version::new(2, 3, 0))
                .with_accepts(VersionRange::parse(">=2.2.0, <3.0.0").unwrap()),
        );
        let consumer = PeerManifest::new("openhuman")
            .consumes(InterfaceVersion::consumed(interface.clone(), Version::new(2, 0, 0)));

        let result = check(&provider, &consumer, &interface);
        assert!(matches!(result, Compatibility::ProviderRejects { .. }), "{result}");
        assert!(result.to_string().contains(">=2.2.0"), "{result}");
    }

    #[test]
    fn an_undeclared_interface_is_reported_as_not_provided() {
        let result = check(
            &PeerManifest::new("voice-service"),
            &consumer("2.0.0"),
            &iface("ai.tinyhumans.openhuman.Voice"),
        );
        assert_eq!(result, Compatibility::NotProvided);
    }

    #[test]
    fn a_consumer_that_declares_nothing_accepts_anything() {
        // Manifests roll out incrementally: a peer that has not adopted them
        // must not be locked off the bus by peers that have.
        let result = check(
            &provider("9.9.9"),
            &PeerManifest::new("legacy"),
            &iface("ai.tinyhumans.openhuman.Voice"),
        );
        assert!(result.is_compatible(), "{result}");
    }

    #[test]
    fn an_explicit_range_can_widen_beyond_the_caret_default() {
        // A provider that genuinely kept 1.x compatibility across a major bump
        // can say so, rather than being forced into a rename.
        let provider = PeerManifest::new("voice").provides(
            InterfaceVersion::provided(
                iface("ai.tinyhumans.openhuman.Voice"),
                Version::new(2, 0, 0),
            )
            .with_accepts(VersionRange::parse(">=1.0.0, <3.0.0").unwrap()),
        );
        let consumer = PeerManifest::new("openhuman").consumes(
            InterfaceVersion::consumed(
                iface("ai.tinyhumans.openhuman.Voice"),
                Version::new(1, 5, 0),
            )
            .with_accepts(VersionRange::parse(">=1.0.0, <3.0.0").unwrap()),
        );
        let result = check(&provider, &consumer, &iface("ai.tinyhumans.openhuman.Voice"));
        assert!(result.is_compatible(), "{result}");
    }

    #[test]
    fn a_manifest_round_trips_through_json() {
        let manifest = PeerManifest::new("voice-service")
            .version(Version::new(0, 4, 2))
            .provides(InterfaceVersion::provided(
                iface("ai.tinyhumans.openhuman.Voice"),
                Version::new(2, 3, 0),
            ));
        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(
            serde_json::from_str::<PeerManifest>(&json).unwrap(),
            manifest
        );
        // Versions travel as strings, so a manifest is readable in `monitor`.
        assert!(json.contains("\"2.3.0\""), "{json}");
    }
}
