//! Declarative module identity, bus surface, dependencies, and trust metadata.

use serde::{Deserialize, Serialize};

use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::version::{InterfaceVersion, PeerManifest, Version};

/// Current JSON manifest schema.
pub const MANIFEST_SCHEMA: u32 = 1;

/// Package identity shown to operators and used for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleIdentity {
    /// Stable package name.
    pub name: String,
    /// Package release.
    pub version: Version,
    /// Human-readable purpose.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Project homepage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    /// SPDX license expression or package license string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub license: String,
}

/// One provided interface plus its introspectable surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvidedInterface {
    /// Interface version declaration shared with peer negotiation.
    #[serde(flatten)]
    pub version: InterfaceVersion,
    /// Methods dispatched by the served object.
    #[serde(default)]
    pub methods: Vec<MemberName>,
    /// Signals the module may emit.
    #[serde(default)]
    pub signals: Vec<MemberName>,
}

/// One interface dependency used for admission and load ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dependency {
    /// Required interface and compatible version range.
    pub interface: InterfaceVersion,
    /// Missing optional dependencies do not prevent resolution.
    #[serde(default)]
    pub optional: bool,
    /// Operator-facing explanation when this dependency is absent.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

/// Environment input declared by a module. Values never appear here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvSpec {
    /// Environment variable name.
    pub name: String,
    /// Whether setup cannot proceed without it.
    #[serde(default)]
    pub required: bool,
    /// Human-readable purpose, never its value.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// A declared privilege or sensitive operation for operator inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    /// Stable capability name.
    pub name: String,
    /// Human-readable scope.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// What to do after a module method panics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PanicPolicy {
    /// Reply with a redacted panic error, then detach the module.
    #[default]
    Detach,
    /// Reply with the redacted panic error and keep serving.
    Reply,
}

/// A module's declared bus surface and load behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleManifest {
    /// [`MANIFEST_SCHEMA`].
    #[serde(rename = "tinybus_manifest")]
    pub schema: u32,
    /// Package identity.
    pub module: ModuleIdentity,
    /// Single well-known name this module claims.
    pub bus_name: BusName,
    /// Object path hosting its interfaces.
    pub object_path: ObjectPath,
    /// Interfaces implemented by the module.
    #[serde(default)]
    pub provides: Vec<ProvidedInterface>,
    /// Interface dependencies, optional or required.
    #[serde(default)]
    pub requires: Vec<Dependency>,
    /// Declared environment inputs.
    #[serde(default)]
    pub environment: Vec<EnvSpec>,
    /// Declared privileges for operator inspection.
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    /// Defer setup until the first method call.
    #[serde(default)]
    pub lazy_init: bool,
    /// Tokio worker threads owned by this module.
    #[serde(default = "default_worker_threads")]
    pub worker_threads: u32,
    /// Panic behavior; detach is the safe default.
    #[serde(default)]
    pub on_panic: PanicPolicy,
}

impl ModuleManifest {
    /// Derive the runtime peer declaration from this single source of truth.
    pub fn peer_manifest(&self) -> PeerManifest {
        PeerManifest {
            name: self.module.name.clone(),
            version: Some(self.module.version.clone()),
            provides: self
                .provides
                .iter()
                .map(|provided| provided.version.clone())
                .collect(),
            consumes: self
                .requires
                .iter()
                .map(|dependency| dependency.interface.clone())
                .collect(),
        }
    }

    /// Find the declaration for `interface`.
    pub fn provided(&self, interface: &InterfaceName) -> Option<&ProvidedInterface> {
        self.provides
            .iter()
            .find(|provided| &provided.version.interface == interface)
    }
}

const fn default_worker_threads() -> u32 {
    1
}
