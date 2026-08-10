//! Declarative module identity and dependency metadata.

use serde::{Deserialize, Serialize};

/// A module's declared bus surface and load dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleManifest {
    /// Stable module name.
    pub name: String,
    /// Module package version.
    pub version: String,
    /// Interfaces this module provides.
    #[serde(default)]
    pub provides: Vec<String>,
    /// Interfaces that must be present before initialization.
    #[serde(default)]
    pub requires: Vec<ModuleDependency>,
    /// Missing optional dependencies do not prevent initialization.
    #[serde(default)]
    pub optional: Vec<ModuleDependency>,
    /// Whether initialization may be delayed until first use.
    #[serde(default)]
    pub lazy: bool,
}

/// One interface dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleDependency {
    /// Interface name required from another module.
    pub interface: String,
    /// Optional version requirement, retained for diagnostics and future gates.
    #[serde(default)]
    pub version: Option<String>,
}
