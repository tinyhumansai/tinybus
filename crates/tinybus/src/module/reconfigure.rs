//! Serialize configuration changes and proxy admission for a loaded module.
//!
//! A [`ReconfigurableModule`] serializes configuration transitions. Callers
//! that need the selected configuration to remain active for a whole bus call
//! hold [`ReconfigureGuard`] and obtain the proxy through the guard. A plain
//! [`ReconfigurableModule::proxy`] only serializes selection and reconfiguration;
//! it cannot protect later work performed through the returned proxy.

use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, MutexGuard};

use super::client::{ModuleAccess, ModuleCallError, ProxyFailure, ReconfigureFailure};
use super::failure::Reason;
use crate::Proxy;

/// A hash of serialized module configuration, with no configuration retained.
#[derive(Clone, Eq, PartialEq)]
pub struct ConfigFingerprint([u8; 32]);

impl ConfigFingerprint {
    /// Serialize and hash `config`; the serialized bytes are discarded before
    /// this function returns.
    ///
    /// # Errors
    /// Returns an error if `config` cannot be serialized as JSON.
    pub fn of(config: &impl Serialize) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(config)?;
        Ok(Self(Sha256::digest(bytes).into()))
    }
}

/// How a configuration change is delivered to the module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reconfigure {
    /// Use TinyBus' module lifecycle control operation.
    Lifecycle,
    /// Call the named module member with the configuration as its sole argument.
    Member(&'static str),
}

/// Coordinates configuration-sensitive access to one registered module.
///
/// For [`Reconfigure::Member`], a failed, canceled, or timed-out remote
/// configuration call leaves its completion uncertain: TinyBus stops waiting,
/// but cannot cancel a handler already running in the module. The coordinator
/// then refuses further access, even for the same fingerprint, until the host
/// has independently established that the module restarted and constructs a
/// new coordinator. This fail-closed behavior prevents a late handler from
/// overwriting a newer configuration.
pub struct ReconfigurableModule {
    access: Box<dyn ModuleAccess>,
    module_id: &'static str,
    strategy: Reconfigure,
    retry_attempts: usize,
    retry_delay: Duration,
    mark_faulted: bool,
    transition: Mutex<()>,
    loaded: std::sync::Mutex<ConfigurationState>,
}

#[derive(Default)]
struct ConfigurationState {
    fingerprint: Option<ConfigFingerprint>,
    member_uncertain: bool,
}

impl ReconfigurableModule {
    /// Create a coordinator for the registered module identified by `module_id`.
    #[must_use]
    pub fn new(access: impl ModuleAccess + 'static, module_id: &'static str) -> Self {
        Self {
            access: Box::new(access),
            module_id,
            strategy: Reconfigure::Lifecycle,
            retry_attempts: 1,
            retry_delay: Duration::ZERO,
            mark_faulted: false,
            transition: Mutex::new(()),
            loaded: std::sync::Mutex::new(ConfigurationState::default()),
        }
    }

    /// Select how a changed configuration is applied.
    #[must_use]
    pub fn with_strategy(mut self, strategy: Reconfigure) -> Self {
        self.strategy = strategy;
        self
    }

    /// Retry only typed `Initializing` responses, with `attempts` total tries.
    /// A value of zero is treated as one attempt.
    #[must_use]
    pub fn retry_while_initializing(mut self, attempts: usize, delay: Duration) -> Self {
        self.retry_attempts = attempts.max(1);
        self.retry_delay = delay;
        self
    }

    /// Mark the resolver's module slot terminal when reconfiguration reports
    /// the module unavailable.
    #[must_use]
    pub fn mark_faulted_on_unavailable(mut self, enabled: bool) -> Self {
        self.mark_faulted = enabled;
        self
    }

    /// Record the configuration supplied to the module's successful load.
    ///
    /// # Errors
    /// Returns an error if the configuration cannot be serialized.
    pub fn record_loaded_with(&self, config: &impl Serialize) -> Result<(), serde_json::Error> {
        let fingerprint = ConfigFingerprint::of(config)?;
        self.loaded_state().fingerprint = Some(fingerprint);
        Ok(())
    }

    /// Forget which configuration the module currently holds.
    ///
    /// This does not clear an uncertain member transition. Only an acknowledged
    /// member configuration call or a new coordinator after an established
    /// module restart can reopen access.
    pub fn invalidate(&self) {
        self.loaded_state().fingerprint = None;
    }

    /// Obtain a proxy after reconciling the module's configuration.
    ///
    /// Concurrent calls serialize their transitions. The returned proxy is no
    /// longer protected after this method returns; use [`Self::exclusive`] and
    /// [`ReconfigureGuard::proxy`] when a subsequent call must share the same
    /// configuration without another caller changing it in between.
    ///
    /// # Errors
    /// Returns a sanitized module outcome or a JSON serialization error.
    pub async fn proxy(&self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        let _guard = self.transition.lock().await;
        self.proxy_locked(config).await
    }

    /// Hold configuration admission exclusively through a caller's bus call.
    ///
    /// All callers using this module's guard must use `guard.proxy(config)`;
    /// calling [`Self::proxy`] while holding this guard would wait on the same
    /// lock.
    pub async fn exclusive(&self) -> ReconfigureGuard<'_> {
        ReconfigureGuard {
            module: self,
            _guard: self.transition.lock().await,
        }
    }

    async fn proxy_locked(&self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        if self.loaded_state().member_uncertain {
            return Err(
                self.report_failure(ModuleCallError::Unavailable, Reason::ModuleUnavailable)
            );
        }
        let Some(record) = self.access.find(self.module_id) else {
            return Err(self.report_failure(ModuleCallError::Unavailable, Reason::UnknownModule));
        };
        if !self.access.enabled() {
            return Err(self.report_failure(ModuleCallError::Unavailable, Reason::Disabled));
        }
        self.access.ensure_loaded(record.id).await.map_err(|_| {
            self.report_failure(ModuleCallError::Unavailable, Reason::ResolutionFailed)
        })?;

        let fingerprint = ConfigFingerprint::of(config)
            .map_err(|_| self.report_failure(ModuleCallError::ModuleFault, Reason::ModuleFault))?;
        let current = self.loaded_state().fingerprint.clone();
        if current.as_ref() != Some(&fingerprint) {
            // Clear before the operation: any failure leaves the held config
            // unknown, so reverting to the prior config still triggers a retry.
            self.loaded_state().fingerprint = None;
            let value = serde_json::to_value(config).map_err(|_| {
                self.report_failure(ModuleCallError::ModuleFault, Reason::ModuleFault)
            })?;
            let mut report_reason = None;
            self.reconfigure(value, fingerprint.clone(), &mut report_reason)
                .await
                .map_err(|error| {
                    self.report_failure(
                        error,
                        report_reason.unwrap_or_else(|| Self::reason_for(error)),
                    )
                })?;
            self.loaded_state().fingerprint = Some(fingerprint);
        }

        self.access
            .proxy(record.bus_name, record.object_path)
            .await
            .map_err(|failure| match failure {
                ProxyFailure::Transport => {
                    self.report_failure(ModuleCallError::TransportFailed, Reason::TransportFailed)
                }
                ProxyFailure::Contract => self.report_failure(
                    ModuleCallError::IncompatibleContract,
                    Reason::IncompatibleContract,
                ),
            })
    }

    async fn reconfigure(
        &self,
        config: serde_json::Value,
        fingerprint: ConfigFingerprint,
        report_reason: &mut Option<Reason>,
    ) -> Result<(), ModuleCallError> {
        let mut outcome = Err(ReconfigureFailure::Failed);
        for attempt in 0..self.retry_attempts {
            outcome = match self.strategy {
                Reconfigure::Lifecycle => {
                    self.access
                        .reinitialize(self.module_id, config.clone())
                        .await
                }
                Reconfigure::Member(member) => {
                    self.member_reconfigure(
                        member,
                        config.clone(),
                        fingerprint.clone(),
                        report_reason,
                    )
                    .await
                }
            };
            match outcome {
                Ok(()) => return Ok(()),
                Err(ReconfigureFailure::Initializing) if attempt + 1 < self.retry_attempts => {
                    if !self.retry_delay.is_zero() {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
                Err(_) => break,
            }
        }
        if matches!(
            outcome,
            Err(ReconfigureFailure::Unavailable | ReconfigureFailure::Initializing)
        ) && self.mark_faulted
        {
            self.access.mark_faulted(self.module_id);
        }
        Err(match outcome {
            Err(ReconfigureFailure::Unavailable | ReconfigureFailure::Initializing) => {
                ModuleCallError::Unavailable
            }
            Err(ReconfigureFailure::Unsupported | ReconfigureFailure::Failed) | Ok(()) => {
                ModuleCallError::ModuleFault
            }
        })
    }

    async fn member_reconfigure(
        &self,
        member: &'static str,
        config: serde_json::Value,
        fingerprint: ConfigFingerprint,
        report_reason: &mut Option<Reason>,
    ) -> Result<(), ReconfigureFailure> {
        let record = self
            .access
            .find(self.module_id)
            .ok_or(ReconfigureFailure::Unavailable)?;
        if !self.access.enabled() {
            return Err(ReconfigureFailure::Unavailable);
        }
        self.access.ensure_loaded(record.id).await.map_err(|_| {
            *report_reason = Some(Reason::ResolutionFailed);
            ReconfigureFailure::Unavailable
        })?;
        let proxy = self
            .access
            .proxy(record.bus_name, record.object_path)
            .await
            .map_err(|failure| match failure {
                ProxyFailure::Transport | ProxyFailure::Contract => ReconfigureFailure::Failed,
            })?;
        // TinyBus deadlines and caller cancellation stop waiting but do not
        // cancel a handler already running in the remote module. Fence future
        // admissions before polling the call, and keep the fence on every
        // ambiguous result. A new coordinator may be created only after the
        // host has independently established that the module was restarted.
        self.loaded_state().member_uncertain = true;
        match proxy
            .call_confidential::<serde_json::Value>(member, (config,))
            .await
        {
            Ok(_) => {
                let mut state = self.loaded_state();
                state.fingerprint = Some(fingerprint);
                state.member_uncertain = false;
                Ok(())
            }
            Err(error) => Err(super::resolve::access::reconfigure_failure(&error)),
        }
    }

    fn reason_for(error: ModuleCallError) -> Reason {
        match error {
            ModuleCallError::Unavailable => Reason::ModuleUnavailable,
            ModuleCallError::IncompatibleContract => Reason::IncompatibleContract,
            ModuleCallError::TransportFailed => Reason::TransportFailed,
            ModuleCallError::ModuleFault => Reason::ModuleFault,
        }
    }

    fn report_failure(&self, error: ModuleCallError, reason: Reason) -> ModuleCallError {
        if reason == Reason::UnknownModule {
            self.access.reporter().report_unknown_module();
            return error;
        }
        let Some(record) = self.access.find(self.module_id) else {
            self.access.reporter().report_unknown_module();
            return error;
        };
        if matches!(
            reason,
            Reason::Disabled | Reason::ResolutionFailed | Reason::ModuleUnavailable
        ) {
            self.access
                .reporter()
                .report(record.id, record.version, reason);
        } else {
            self.access
                .reporter()
                .report_invocation(record.id, record.version, reason);
        }
        error
    }

    fn loaded_state(&self) -> std::sync::MutexGuard<'_, ConfigurationState> {
        self.loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A configuration admission lock held across proxy use.
pub struct ReconfigureGuard<'a> {
    module: &'a ReconfigurableModule,
    _guard: MutexGuard<'a, ()>,
}

impl ReconfigureGuard<'_> {
    /// Reconcile the configuration while retaining exclusive access until this
    /// guard is dropped.
    pub async fn proxy(&mut self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        self.module.proxy_locked(config).await
    }
}

#[cfg(test)]
#[path = "reconfigure_tests.rs"]
mod tests;
