//! Adapter from the module resolver to configuration-aware module access.

use super::Resolver;
use crate::Proxy;
use crate::module::ReleaseRecord;
use crate::module::client::{
    AccessFuture, ModuleAccess, ProxyFailure, ReadinessFailure, ReconfigureFailure,
};
use crate::module::failure::FailureReporter;
use crate::module::resolution;

pub(in crate::module) fn reconfigure_failure(error: &crate::Error) -> ReconfigureFailure {
    match error {
        crate::Error::ModuleUnavailable { state, .. }
            if state.eq_ignore_ascii_case("initializing") =>
        {
            ReconfigureFailure::Initializing
        }
        crate::Error::ModuleUnavailable { .. } => ReconfigureFailure::Unavailable,
        crate::Error::MethodFailed { name, .. } if name == crate::Error::MODULE_UNAVAILABLE => {
            ReconfigureFailure::Unavailable
        }
        _ => ReconfigureFailure::Failed,
    }
}

impl ModuleAccess for Resolver {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        Resolver::find(self, id)
    }

    fn enabled(&self) -> bool {
        Resolver::enabled(self)
    }

    fn reporter(&self) -> &FailureReporter {
        Resolver::reporter(self)
    }

    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(Resolver::ensure_loaded(self, id))
    }

    fn ensure_loaded_within<'a>(
        &'a self,
        id: &'a str,
        within: Option<std::time::Duration>,
    ) -> AccessFuture<'a, Result<(), ReadinessFailure>> {
        Box::pin(async move {
            match Resolver::ensure_loaded_within(self, id, within).await {
                Ok(()) => Ok(()),
                Err(crate::module::LoadError::StillLoading) => Err(ReadinessFailure::TimedOut),
                Err(
                    crate::module::LoadError::Failed(_) | crate::module::LoadError::Disabled(_),
                ) => Err(ReadinessFailure::Unavailable),
            }
        })
    }

    fn prepare_attested<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(Resolver::prepare_attested(self, id))
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async move {
            let runtime = self.runtime().await.map_err(|_| ProxyFailure::Transport)?;
            runtime
                .proxy(bus_name, object_path)
                .map_err(|_| ProxyFailure::Contract)
        })
    }

    fn reinitialize<'a>(
        &'a self,
        id: &'a str,
        config: serde_json::Value,
    ) -> AccessFuture<'a, Result<(), ReconfigureFailure>> {
        Box::pin(async move {
            let runtime = self
                .runtime()
                .await
                .map_err(|_| ReconfigureFailure::Unavailable)?;
            runtime
                .connection()
                .reinitialize_module(id, config)
                .await
                .map(|_| ())
                .map_err(|error| reconfigure_failure(&error))
        })
    }

    fn mark_faulted(&self, id: &str) {
        resolution::global().mark_faulted(id, "module reconfiguration failed".to_owned());
    }
}

#[cfg(test)]
#[path = "resolve_access_tests.rs"]
mod tests;
