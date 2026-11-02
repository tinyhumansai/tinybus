use serde::{Deserialize, Serialize};

/// Coarse module status a host can report, whether or not it enables loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    /// Known to this build, not loaded yet.
    Available,
    /// Being downloaded, verified or initialised.
    Loading,
    /// Serving its interface.
    Ready,
    /// Cannot be loaded in this process, and will not be retried.
    Failed,
    /// No artifact is published for this host, or modules are disabled.
    Unsupported,
    /// Switched off by host-side policy.
    Disabled,
}
