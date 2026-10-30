//! Sanitised terminal module reports, deduplicated, behind a sink the host owns.
//!
//! A module that cannot load fails for the whole process, and the outcome is
//! cached, so a broken install should produce one report rather than one per
//! caller. The vocabulary is closed on purpose: no module error, path, URL or
//! payload ever reaches a [`FailureSink`].

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Closed reason vocabulary: never accept a module error, path or payload here.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Reason {
    /// The id is not in the host's registry.
    UnknownModule,
    /// Modules are disabled in configuration.
    Disabled,
    /// The host was built without the module loader.
    LoaderDisabled,
    /// Resolution ended without a serving module.
    ResolutionFailed,
    /// The module speaks a different wire contract.
    IncompatibleContract,
    /// The bus or its transport failed.
    TransportFailed,
    /// The module accepted the call and failed it.
    ModuleFault,
    /// The module (or the capability asked of it) is not available.
    ModuleUnavailable,
}

impl Reason {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::UnknownModule => "unknown_module",
            Self::Disabled => "disabled",
            Self::LoaderDisabled => "loader_disabled",
            Self::ResolutionFailed => "resolution_failed",
            Self::IncompatibleContract => "incompatible_contract",
            Self::TransportFailed => "transport_failed",
            Self::ModuleFault => "module_fault",
            Self::ModuleUnavailable => "module_unavailable",
        }
    }

    /// The stage of the module lifecycle the reason belongs to.
    #[must_use]
    pub fn stage(self) -> &'static str {
        match self {
            Self::UnknownModule => "registry",
            Self::Disabled | Self::LoaderDisabled => "availability",
            Self::ResolutionFailed => "resolve",
            Self::IncompatibleContract => "contract",
            Self::TransportFailed => "transport",
            Self::ModuleFault | Self::ModuleUnavailable => "execution",
        }
    }
}

/// One sanitised terminal outcome. Every field is registry or closed-vocabulary
/// data.
#[derive(Debug, Clone, Copy)]
pub struct FailureReport<'a> {
    /// Registry id, or `unregistered` for an unknown module.
    pub module: &'a str,
    /// Pinned release version, or `unknown`.
    pub version: &'a str,
    /// Why.
    pub reason: Reason,
    /// `<os>-<arch>` of this process.
    pub platform: &'a str,
}

/// Where reports go: a host's crash reporter.
pub trait FailureSink: Send + Sync {
    /// Whether a report can be delivered right now.
    ///
    /// A host that has not bound a reporting client yet should say `false`, so a
    /// report nobody can receive does not spend its deduplication key.
    fn ready(&self) -> bool {
        true
    }

    /// Deliver `report`. The sink must not attach request payloads or user
    /// paths; the report carries only the closed metadata above.
    fn capture(&self, report: &FailureReport<'_>);
}

/// Deduplicating front for a [`FailureSink`].
///
/// Keep one per process: the deduplication set is the reporter's, so a host that
/// built one per call would report every call.
pub struct FailureReporter {
    sink: Arc<dyn FailureSink>,
    reported: Mutex<HashSet<(String, Reason)>>,
}

impl FailureReporter {
    /// A reporter delivering to `sink`.
    #[must_use]
    pub fn new(sink: Arc<dyn FailureSink>) -> Self {
        Self {
            sink,
            reported: Mutex::new(HashSet::new()),
        }
    }

    /// Report a terminal outcome once per `(module, reason)`.
    ///
    /// There are finitely many registry records and reasons, so the set is
    /// bounded. Concurrent callers cannot duplicate the same terminal report.
    pub fn report(&self, module: &str, version: &str, reason: Reason) {
        self.deliver(module, version, reason, true);
    }

    /// Report a failure from this invocation. Independent executions can fail
    /// with the same reason and still be distinct terminal events.
    pub fn report_invocation(&self, module: &str, version: &str, reason: Reason) {
        self.deliver(module, version, reason, false);
    }

    /// Look up an identifier in the host's registry before reporting it.
    ///
    /// Unknown identifiers share the anonymous terminal report, so an
    /// untrusted caller cannot put arbitrary identity into diagnostics.
    pub fn report_for(&self, records: &[super::release::ReleaseRecord], id: &str, reason: Reason) {
        if let Some(record) = records.iter().find(|record| record.id == id) {
            self.report(record.id, record.version, reason);
        } else {
            self.report_unknown_module();
        }
    }

    /// Unknown identifiers never become metadata or deduplication keys.
    pub fn report_unknown_module(&self) {
        self.deliver("unregistered", "unknown", Reason::UnknownModule, true);
    }

    fn deliver(&self, module: &str, version: &str, reason: Reason, deduplicate: bool) {
        if deduplicate && self.sink.ready() {
            let first = self
                .reported
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert((module.to_string(), reason));
            if !first {
                return;
            }
        }
        let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
        tracing::debug!(
            module,
            reason = reason.code(),
            "[tinybus::module] reporting terminal module outcome"
        );
        self.sink.capture(&FailureReport {
            module,
            version,
            reason,
            platform: &platform,
        });
    }
}

#[cfg(test)]
#[path = "failure_tests.rs"]
mod tests;
