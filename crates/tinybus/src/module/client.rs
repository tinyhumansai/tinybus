//! Shared module access for hosts: a [`ModuleClient`] over a [`ModuleAccess`].
//!
//! Constructing a client starts nothing. The first call loads the module through
//! its [`ModuleAccess`] (a `Resolver`, with the `modules` feature), which
//! resolves once per process and caches the outcome.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};

use crate::module::failure::{FailureReporter, Reason};
use crate::module::release::ReleaseRecord;
use crate::{Error, Proxy};

/// A boxed future returned by a [`ModuleAccess`].
pub type AccessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a [`ModuleAccess`] could not hand out a proxy for a serving module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyFailure {
    /// The module bus could not be started or reached.
    Transport,
    /// The module's bus name or object path is not well formed.
    Contract,
}

/// Outcome when bounded readiness did not return a serving module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessFailure {
    /// The access layer reported that the module is unavailable.
    Unavailable,
    /// The caller's explicit preparation deadline elapsed.
    TimedOut,
}

/// What a [`ModuleClient`] needs from the host to reach a registry module.
///
/// With the `modules` feature the host's `Resolver` implements it, loading the
/// module lazily. A host built without the loader implements it over its bare
/// registry and refuses every load, so callers compile and behave the same way
/// (every call is unavailable and reported) with or without the loader.
pub trait ModuleAccess: Send + Sync {
    /// The registry record for `id`, if the host has one.
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord>;

    /// Whether modules are enabled at all.
    fn enabled(&self) -> bool;

    /// The reporter terminal outcomes go through.
    fn reporter(&self) -> &FailureReporter;

    /// Ensure `id` is loaded and serving.
    ///
    /// # Errors
    ///
    /// A message fit for a user or a model when the module cannot be loaded.
    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>>;

    /// Ensure `id` is ready, optionally bounding this caller's wait.
    ///
    /// Without a deadline this keeps the existing unbounded behavior. The
    /// default bounds the returned future when one is supplied. An access layer
    /// whose load work belongs to an independent process-lifetime task should
    /// override this method so timing out the caller does not cancel that work.
    fn ensure_loaded_within<'a>(
        &'a self,
        id: &'a str,
        within: Option<Duration>,
    ) -> AccessFuture<'a, Result<(), ReadinessFailure>> {
        Box::pin(async move {
            let loading = self.ensure_loaded(id);
            match within {
                Some(within) => tokio::time::timeout(within, loading)
                    .await
                    .map_err(|_| ReadinessFailure::TimedOut)?
                    .map_err(|_| ReadinessFailure::Unavailable),
                None => loading.await.map_err(|_| ReadinessFailure::Unavailable),
            }
        })
    }

    /// Perform host-owned readiness work before a strict recipient-attestation
    /// query. The default refuses because generic accessors cannot prove that
    /// a lazy native initializer has run without sending a business call.
    fn prepare_attested<'a>(&'a self, _id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async { Err("strict recipient preparation is unsupported".to_string()) })
    }

    /// A proxy for `object_path` on the serving module at `bus_name`.
    ///
    /// # Errors
    ///
    /// [`ProxyFailure`] when the bus cannot be reached or the names are invalid.
    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>>;

    /// Reinitialize a loaded module through its owning lifecycle host.
    ///
    /// Implementations without lifecycle support may keep the default, which
    /// reports [`ReconfigureFailure::Unsupported`].
    fn reinitialize<'a>(
        &'a self,
        _id: &'a str,
        _config: serde_json::Value,
    ) -> AccessFuture<'a, Result<(), ReconfigureFailure>> {
        Box::pin(async { Err(ReconfigureFailure::Unsupported) })
    }

    /// Record that reinitialization left a module unusable. Resolver-backed
    /// access makes this terminal for the current process; other accessors may
    /// leave it as a no-op.
    fn mark_faulted(&self, _id: &str) {}
}

/// Module access for hosts that compile the registry but omit the dynamic
/// loader. Known modules remain discoverable and report the closed
/// `loader_disabled` reason when requested; unknown identifiers are not copied
/// into diagnostics.
pub struct UnloadedAccess {
    records: Vec<&'static ReleaseRecord>,
    enabled: bool,
    reporter: Arc<FailureReporter>,
}

impl UnloadedAccess {
    /// Create access over the host's compiled module records without a loader.
    #[must_use]
    pub fn new(
        records: Vec<&'static ReleaseRecord>,
        enabled: bool,
        reporter: Arc<FailureReporter>,
    ) -> Self {
        Self {
            records,
            enabled,
            reporter,
        }
    }
}

impl ModuleAccess for UnloadedAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        self.records.iter().copied().find(|record| record.id == id)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if let Some(record) = self.find(id) {
                tracing::debug!(module = record.id, "module unavailable: loader is disabled");
                self.reporter
                    .report(record.id, record.version, Reason::LoaderDisabled);
                Err(format!(
                    "module '{}' is unavailable: this build has no module loader",
                    record.id
                ))
            } else {
                Err("module is unavailable: this build has no module loader".to_string())
            }
        })
    }

    fn proxy<'a>(
        &'a self,
        _bus_name: &'a str,
        _object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async { Err(ProxyFailure::Transport) })
    }
}

/// Sanitized lifecycle result used by configuration reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconfigureFailure {
    /// The module is still completing initialization and may become ready.
    Initializing,
    /// The module is unavailable in a terminal or unsupported state.
    Unavailable,
    /// The lifecycle operation failed for another reason.
    Failed,
    /// This access implementation does not expose module lifecycle control.
    Unsupported,
}

/// A sanitised terminal module outcome. Arguments and remote error text are
/// absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModuleCallError {
    /// The module is not available.
    Unavailable,
    /// The module speaks a different wire contract.
    IncompatibleContract,
    /// The bus or its transport failed.
    TransportFailed,
    /// The module accepted the call and failed it.
    ModuleFault,
}

impl std::fmt::Display for ModuleCallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::Unavailable => "unavailable",
            Self::IncompatibleContract => "incompatible contract",
            Self::TransportFailed => "transport failed",
            Self::ModuleFault => "execution failed",
        };
        // Product reporting recognises this prefix as an already reported
        // module failure, rather than producing a second terminal event.
        write!(formatter, "MODULE_CALL_REPORTED: module {reason}")
    }
}

impl std::error::Error for ModuleCallError {}

/// A detailed call failure with remote prose kept out of its renderings.
///
/// The fields are untrusted peer-provided data available to a product adapter
/// that has its own display policy. A remote message may contain arguments or
/// credentials. `Debug`, `Display`, and the failure reporter never include
/// these fields; callers should not log or display them without their own
/// redaction policy.
#[derive(Clone, PartialEq, Eq)]
pub struct ModuleCallFailure {
    /// The closed, sanitized module outcome.
    pub kind: ModuleCallError,
    /// The untrusted dotted name returned by a remote method failure, or a
    /// fixed local name for failures that did not come from a remote method.
    /// Treat remote values as sensitive and do not log them directly.
    pub wire_name: String,
    /// The exact untrusted remote method message, or a fixed local message.
    /// It may contain arguments or credentials; do not log or display it
    /// without applying a product-specific redaction policy.
    pub message: String,
}

impl ModuleCallFailure {
    fn local(kind: ModuleCallError) -> Self {
        let wire_name = match kind {
            ModuleCallError::Unavailable => "ai.tinyhumans.tinybus.Error.ModuleUnavailable",
            ModuleCallError::IncompatibleContract => {
                "ai.tinyhumans.tinybus.Error.IncompatibleVersion"
            }
            ModuleCallError::TransportFailed => "ai.tinyhumans.tinybus.Error.Transport",
            ModuleCallError::ModuleFault => "ai.tinyhumans.tinybus.Error.Failed",
        };
        Self {
            kind,
            wire_name: wire_name.to_string(),
            message: "module call failed".to_string(),
        }
    }

    fn from_error(error: &Error, kind: ModuleCallError) -> Self {
        match error {
            Error::MethodFailed { name, message } => Self {
                kind,
                wire_name: name.clone(),
                message: message.clone(),
            },
            _ => Self::local(kind),
        }
    }
}

impl std::fmt::Debug for ModuleCallFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModuleCallFailure")
            .field("kind", &self.kind)
            .field("wire_name", &"<redacted>")
            .field("message", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Display for ModuleCallFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{kind}", kind = self.kind)
    }
}

impl std::error::Error for ModuleCallFailure {}

/// Calls contract members on registry modules, loading them lazily.
#[derive(Clone)]
pub struct ModuleClient {
    access: Arc<dyn ModuleAccess>,
    timeout: Option<Duration>,
    /// Routes calls to a proxy instead of loading a module. For tests.
    fixture: Option<Proxy>,
}

impl ModuleClient {
    /// A client that reaches modules through `access` (a `Resolver`, with the
    /// `modules` feature).
    #[must_use]
    pub fn new(access: impl ModuleAccess + 'static) -> Self {
        Self {
            access: Arc::new(access),
            timeout: None,
            fixture: None,
        }
    }

    /// Route every call to `proxy` instead of loading a module. The module must
    /// still be in the registry and modules must still be enabled.
    #[must_use]
    pub fn with_fixture(mut self, proxy: Proxy) -> Self {
        self.fixture = Some(proxy);
        self
    }

    /// The fixture proxy, if one is set.
    #[must_use]
    pub fn fixture_proxy(&self) -> Option<Proxy> {
        self.fixture.clone()
    }

    /// Override the TinyBus deadline for every call made through this client,
    /// confidential calls and fixtures included.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Execute a contract member.
    ///
    /// No implementation fallback is provided. The caller owns approvals,
    /// cancellation and lifecycle cleanup.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`]; the cause is reported once through the
    /// resolver's reporter.
    pub async fn call<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        self.invoke(module, member, args, false).await
    }

    /// Execute a contract member and retain a remote method's wire failure.
    ///
    /// Remote details are available only in the returned fields. Its `Debug`
    /// and `Display` renderings, and the terminal report, remain sanitized.
    ///
    /// # Errors
    ///
    /// A detailed [`ModuleCallFailure`].
    pub async fn call_detailed<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        self.invoke_detailed(module, member, args, false).await
    }

    /// Execute a contract member while uploading its bulk payload as a stream.
    ///
    /// The argument builder receives the stream handle only after the module
    /// is validated and ready. Its serialized arguments, method call and
    /// payload use the single proxy selected for this invocation.
    ///
    /// # Errors
    ///
    /// A detailed [`ModuleCallFailure`]. Remote method details remain redacted
    /// from `Debug`, `Display`, and terminal reports.
    pub async fn call_with_stream_detailed<R, F, A>(
        &self,
        module: &str,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallFailure>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        let record = self.record(module).map_err(ModuleCallFailure::local)?;
        let member = crate::MemberName::new(member).map_err(|error| {
            report_proxy_failure_detailed(self.access.reporter(), record, &error)
        })?;
        let proxy = if let Some(fixture) = &self.fixture {
            fixture.clone()
        } else {
            self.load_proxy(record, record.object_path)
                .await
                .map_err(ModuleCallFailure::local)?
        };
        let proxy = self.with_deadline(proxy);
        proxy
            .connection()
            .call_with_stream_serialized(
                proxy.destination().clone(),
                proxy.path().clone(),
                proxy.interface().clone(),
                member,
                args,
                bytes,
                proxy.timeout(),
            )
            .await
            .map_err(|error| report_proxy_failure_detailed(self.access.reporter(), record, &error))
    }

    /// Sanitized counterpart to [`Self::call_with_stream_detailed`].
    ///
    /// # Errors
    ///
    /// A sanitized [`ModuleCallError`].
    pub async fn call_with_stream<R, F, A>(
        &self,
        module: &str,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallError>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        self.call_with_stream_detailed(module, member, args, bytes)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Execute a confidential member after TinyBus recipient attestation.
    ///
    /// A failure to attest the pinned artifact fails the call; ordinary delivery
    /// is never retried.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn call_confidential<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        self.invoke(module, member, args, true).await
    }

    /// Execute a confidential member only when the host registry pins both the
    /// release archive and extracted library and the same unique peer proves
    /// that exact pair. Lazy readiness is established without a business call,
    /// before this method queries the broker or serializes `args`.
    ///
    /// # Errors
    ///
    /// A sanitized [`ModuleCallError`]. Missing or mismatched proofs fail
    /// closed and are never retried through ordinary delivery.
    pub async fn call_attested<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        self.call_attested_detailed(module, member, args)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Bind to the exact unique peer that proves the registered artifact.
    ///
    /// Lazy readiness and strict attestation happen before this returns. Every
    /// call and held-object client from the result keeps the same private unique
    /// destination, even if the public well-known name later changes owners.
    ///
    /// # Errors
    ///
    /// A sanitized [`ModuleCallFailure`] when loading or attestation fails.
    pub async fn bind_attested(
        &self,
        module: &str,
    ) -> Result<BoundModuleClient, ModuleCallFailure> {
        let record = self.record(module).map_err(ModuleCallFailure::local)?;
        let candidates = crate::module::platform::host_candidates();
        // The resolver tries compatible release assets in host-candidate order
        // and may fall back after an admission failure. Keep each archive and
        // library pin paired from the trusted registry so the loaded fallback
        // can prove itself without allowing digest mixing across assets.
        let compatible_assets: Vec<_> = record
            .assets
            .iter()
            .filter(|asset| candidates.iter().any(|host| host == asset.host_key))
            .filter(|asset| {
                valid_sha256(asset.sha256) && asset.library_sha256.is_some_and(valid_sha256)
            })
            .collect();
        if compatible_assets.is_empty() {
            report(&*self.access, record, Reason::ModuleUnavailable);
            return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
        }
        let preparation = self.access.prepare_attested(record.id);
        let prepared = if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, preparation)
                .await
                .is_ok_and(|result| result.is_ok())
        } else {
            preparation.await.is_ok()
        };
        if self.fixture.is_some() || !prepared {
            report(&*self.access, record, Reason::ModuleUnavailable);
            return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
        }

        let name_proxy = self
            .load_proxy(record, record.object_path)
            .await
            .map_err(ModuleCallFailure::local)?;
        let metadata_timeout = self.timeout.unwrap_or(crate::connection::DEFAULT_TIMEOUT);
        let owner = match name_proxy
            .connection()
            .name_owner_with_timeout(record.bus_name, metadata_timeout)
            .await
        {
            Ok(Some(owner)) if owner.is_unique() => owner,
            Ok(_) => {
                report(&*self.access, record, Reason::ModuleUnavailable);
                return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
            }
            Err(error) => {
                return Err(report_proxy_failure_detailed(
                    self.access.reporter(),
                    record,
                    &error,
                ));
            }
        };
        let unique_proxy = name_proxy
            .connection()
            .proxy(owner.as_str(), record.object_path, record.bus_name)
            .map_err(|_| {
                report(&*self.access, record, Reason::IncompatibleContract);
                ModuleCallFailure::local(ModuleCallError::IncompatibleContract)
            })?;
        let attestation = match unique_proxy
            .connection()
            .attestation_with_timeout(owner.clone(), metadata_timeout)
            .await
        {
            Ok(Some(attestation)) => attestation,
            Ok(None) => {
                report(&*self.access, record, Reason::ModuleUnavailable);
                return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
            }
            Err(error) => {
                return Err(report_proxy_failure_detailed(
                    self.access.reporter(),
                    record,
                    &error,
                ));
            }
        };
        let expected_name = crate::name::BusName::new(record.bus_name)
            .expect("a release registry carries a validated bus name");
        if attestation.name != expected_name
            || !compatible_assets.iter().any(|asset| {
                attestation.sha256.eq_ignore_ascii_case(asset.sha256)
                    && attestation.library_sha256.as_deref().is_some_and(|digest| {
                        digest.eq_ignore_ascii_case(
                            asset.library_sha256.expect("filtered library pin"),
                        )
                    })
            })
        {
            report(&*self.access, record, Reason::ModuleUnavailable);
            return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
        }

        Ok(BoundModuleClient {
            access: Arc::clone(&self.access),
            record,
            proxy: self.with_deadline(unique_proxy),
            timeout: self.timeout,
        })
    }

    /// Bind ordinary calls to the currently serving peer for a module.
    ///
    /// The peer is resolved after lazy readiness and retained privately by the
    /// returned client. This is ordinary delivery and makes no artifact or
    /// confidentiality claim. Use [`Self::bind_attested`] for calls that
    /// require strict native artifact proof.
    ///
    /// Explicit fixture proxies follow the same owner pinning rule, while
    /// resolver-backed linked modules remain eligible without library hashes.
    ///
    /// # Errors
    ///
    /// A sanitized [`ModuleCallFailure`] when the module cannot be made ready,
    /// has no serving owner, or its bus metadata cannot be read.
    pub async fn bind(&self, module: &str) -> Result<PinnedModuleClient, ModuleCallFailure> {
        let record = self.record(module).map_err(ModuleCallFailure::local)?;
        let alias_proxy = if let Some(fixture) = &self.fixture {
            fixture.clone()
        } else {
            self.load_proxy_for_binding(record, record.object_path)
                .await?
        };
        // `ModuleAccess::proxy` and explicit fixtures may carry an integration-
        // specific deadline. An explicit client timeout overrides it; otherwise
        // preserve the deadline already attached to the selected proxy.
        let effective_timeout = self.timeout.unwrap_or(alias_proxy.timeout());
        let owner = match alias_proxy
            .connection()
            .name_owner_with_timeout(alias_proxy.destination().as_str(), effective_timeout)
            .await
        {
            Ok(Some(owner)) if owner.is_unique() => owner,
            Ok(_) => {
                report(&*self.access, record, Reason::ModuleUnavailable);
                return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
            }
            Err(error) => {
                return Err(report_proxy_failure_detailed(
                    self.access.reporter(),
                    record,
                    &error,
                ));
            }
        };
        let pinned_proxy = alias_proxy
            .connection()
            .proxy(
                owner.as_str(),
                alias_proxy.path().as_str(),
                alias_proxy.interface().as_str(),
            )
            .map_err(|_| {
                report(&*self.access, record, Reason::IncompatibleContract);
                ModuleCallFailure::local(ModuleCallError::IncompatibleContract)
            })?
            .with_timeout(effective_timeout);

        Ok(PinnedModuleClient {
            access: Arc::clone(&self.access),
            record,
            proxy: pinned_proxy,
            timeout: effective_timeout,
        })
    }

    /// Execute a confidential member only after strict artifact attestation.
    ///
    /// The bound peer is selected and proved before `args` are serialized.
    /// Remote failure fields remain available to the caller, while `Debug`,
    /// `Display`, and reports stay sanitized.
    pub async fn call_attested_detailed<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        self.bind_attested(module)
            .await?
            .call_confidential_detailed(member, args)
            .await
    }

    /// Call a member that returns a JSON-encoded string and deserialise that
    /// inner document. Malformed output is reported with only the registered
    /// module identity and the closed `module_fault` reason.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn call_json<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        let response: String = self.call(module, member, args).await?;
        self.decode_json(module, &response)
    }

    /// Confidential counterpart to [`Self::call_json`]. A decode failure is
    /// terminal; the call is never retried through ordinary delivery.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn call_confidential_json<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        let response: String = self.call_confidential(module, member, args).await?;
        self.decode_json(module, &response)
    }

    /// A validated proxy for an object path returned by an owning module.
    ///
    /// Object paths belong inside the host adapter; wrap this in a
    /// domain-specific client rather than exposing the path as product state.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn object_client(
        &self,
        module: &str,
        object_path: &str,
    ) -> Result<ModuleObjectClient, ModuleCallError> {
        let record = self.record(module)?;
        if let Some(fixture) = &self.fixture {
            let proxy = fixture
                .connection()
                .proxy(
                    fixture.destination().as_str(),
                    object_path,
                    fixture.interface().as_str(),
                )
                .map_err(|_| {
                    report(&*self.access, record, Reason::IncompatibleContract);
                    ModuleCallError::IncompatibleContract
                })?;
            return Ok(self.object(record, proxy));
        }
        let proxy = self.load_proxy(record, object_path).await?;
        Ok(self.object(record, proxy))
    }

    /// Report a structurally invalid reply from a known module invocation.
    pub fn report_malformed_reply(&self, module: &str) {
        if let Some(record) = self.access.find(module) {
            report_invocation(&*self.access, record, Reason::ModuleFault);
        }
    }

    /// Report a registered module that advertises an older wire contract.
    pub fn report_incompatible_contract(&self, module: &str) {
        if let Some(record) = self.access.find(module) {
            report(&*self.access, record, Reason::IncompatibleContract);
        }
    }

    /// Report a known module that does not provide the requested capability.
    pub fn report_unavailable(&self, module: &str) {
        if let Some(record) = self.access.find(module) {
            report(&*self.access, record, Reason::ModuleUnavailable);
        }
    }

    fn object(&self, record: &'static ReleaseRecord, proxy: Proxy) -> ModuleObjectClient {
        ModuleObjectClient {
            access: Arc::clone(&self.access),
            record,
            proxy: self.with_deadline(proxy),
            timeout: self.timeout,
        }
    }

    fn with_deadline(&self, proxy: Proxy) -> Proxy {
        self.timeout
            .map_or(proxy.clone(), |timeout| proxy.with_timeout(timeout))
    }

    fn record(&self, module: &str) -> Result<&'static ReleaseRecord, ModuleCallError> {
        let Some(record) = self.access.find(module) else {
            self.access.reporter().report_unknown_module();
            return Err(ModuleCallError::Unavailable);
        };
        if !self.access.enabled() {
            report(&*self.access, record, Reason::Disabled);
            return Err(ModuleCallError::Unavailable);
        }
        Ok(record)
    }

    /// Load `record` and build a proxy for `object_path` on it.
    async fn load_proxy(
        &self,
        record: &'static ReleaseRecord,
        object_path: &str,
    ) -> Result<Proxy, ModuleCallError> {
        self.access
            .ensure_loaded(record.id)
            .await
            .map_err(|_| ModuleCallError::Unavailable)?;
        self.access
            .proxy(record.bus_name, object_path)
            .await
            .map_err(|failure| match failure {
                ProxyFailure::Transport => {
                    report(&*self.access, record, Reason::TransportFailed);
                    ModuleCallError::TransportFailed
                }
                ProxyFailure::Contract => {
                    report(&*self.access, record, Reason::IncompatibleContract);
                    ModuleCallError::IncompatibleContract
                }
            })
    }

    /// Load a module for an ordinary persistent binding, honoring an explicit
    /// client timeout without making a terminal loader failure from a caller
    /// that merely stopped waiting.
    async fn load_proxy_for_binding(
        &self,
        record: &'static ReleaseRecord,
        object_path: &str,
    ) -> Result<Proxy, ModuleCallFailure> {
        match self
            .access
            .ensure_loaded_within(record.id, self.timeout)
            .await
        {
            Ok(()) => {}
            Err(ReadinessFailure::Unavailable) => {
                return Err(ModuleCallFailure::local(ModuleCallError::Unavailable));
            }
            Err(ReadinessFailure::TimedOut) => {
                return Err(report_proxy_failure_detailed(
                    self.access.reporter(),
                    record,
                    &Error::transport("module readiness deadline elapsed"),
                ));
            }
        }

        let proxy = self.access.proxy(record.bus_name, object_path);
        let proxy = if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, proxy).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(report_proxy_failure_detailed(
                        self.access.reporter(),
                        record,
                        &Error::transport("module proxy preparation deadline elapsed"),
                    ));
                }
            }
        } else {
            proxy.await
        };
        proxy.map_err(|failure| match failure {
            ProxyFailure::Transport => {
                report(&*self.access, record, Reason::TransportFailed);
                ModuleCallFailure::local(ModuleCallError::TransportFailed)
            }
            ProxyFailure::Contract => {
                report(&*self.access, record, Reason::IncompatibleContract);
                ModuleCallFailure::local(ModuleCallError::IncompatibleContract)
            }
        })
    }

    async fn invoke<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
        confidential: bool,
    ) -> Result<R, ModuleCallError> {
        let record = self.record(module)?;
        let proxy = if let Some(fixture) = &self.fixture {
            fixture.clone()
        } else {
            self.load_proxy(record, record.object_path).await?
        };
        let proxy = self.with_deadline(proxy);
        invoke_proxy(&*self.access, record, &proxy, member, args, confidential).await
    }

    async fn invoke_detailed<R: DeserializeOwned>(
        &self,
        module: &str,
        member: &str,
        args: impl Serialize,
        confidential: bool,
    ) -> Result<R, ModuleCallFailure> {
        let record = self.record(module).map_err(ModuleCallFailure::local)?;
        let proxy = if let Some(fixture) = &self.fixture {
            fixture.clone()
        } else {
            self.load_proxy(record, record.object_path)
                .await
                .map_err(ModuleCallFailure::local)?
        };
        let proxy = self.with_deadline(proxy);
        invoke_proxy_detailed(&*self.access, record, &proxy, member, args, confidential).await
    }

    fn decode_json<R: DeserializeOwned>(
        &self,
        module: &str,
        response: &str,
    ) -> Result<R, ModuleCallError> {
        serde_json::from_str(response).map_err(|_| {
            self.report_malformed_reply(module);
            ModuleCallError::ModuleFault
        })
    }
}

/// A client permanently bound to one attested module peer.
///
/// The selected unique bus destination is private and retained by every call
/// and object client created from this value. It is never resolved again by
/// well-known name.
#[derive(Clone)]
pub struct BoundModuleClient {
    access: Arc<dyn ModuleAccess>,
    record: &'static ReleaseRecord,
    proxy: Proxy,
    timeout: Option<Duration>,
}

impl BoundModuleClient {
    /// Call an ordinary member on the bound peer.
    pub async fn call<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        invoke_proxy(&*self.access, self.record, &self.proxy, member, args, false).await
    }

    /// Call an ordinary member and retain its remote wire failure fields.
    pub async fn call_detailed<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        invoke_proxy_detailed(&*self.access, self.record, &self.proxy, member, args, false).await
    }

    /// Call a confidential member on the bound peer.
    pub async fn call_confidential<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        invoke_proxy(&*self.access, self.record, &self.proxy, member, args, true).await
    }

    /// Call a confidential member and retain its remote wire failure fields.
    pub async fn call_confidential_detailed<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        invoke_proxy_detailed(&*self.access, self.record, &self.proxy, member, args, true).await
    }

    /// Upload a bulk payload while calling a member on this same unique peer.
    ///
    /// The stream open, method call, and payload retain the pinned destination
    /// even if the public bus name changes owners during argument construction.
    /// This is ordinary bus delivery; it does not add a confidential stream
    /// fallback.
    ///
    /// # Errors
    ///
    /// A detailed [`ModuleCallFailure`]. Its remote fields remain available to
    /// the caller while `Debug`, `Display`, and reports stay sanitized.
    pub async fn call_with_stream_detailed<R, F, A>(
        &self,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallFailure>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        let member = crate::MemberName::new(member).map_err(|error| {
            report_proxy_failure_detailed(self.access.reporter(), self.record, &error)
        })?;
        self.proxy
            .connection()
            .call_with_stream_serialized(
                self.proxy.destination().clone(),
                self.proxy.path().clone(),
                self.proxy.interface().clone(),
                member,
                args,
                bytes,
                self.proxy.timeout(),
            )
            .await
            .map_err(|error| {
                report_proxy_failure_detailed(self.access.reporter(), self.record, &error)
            })
    }

    /// Sanitized counterpart to [`Self::call_with_stream_detailed`].
    pub async fn call_with_stream<R, F, A>(
        &self,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallError>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        self.call_with_stream_detailed(member, args, bytes)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Create a held-object client pinned to this same unique module peer.
    pub fn object_client(&self, object_path: &str) -> Result<ModuleObjectClient, ModuleCallError> {
        let proxy = self
            .proxy
            .connection()
            .proxy(
                self.proxy.destination().as_str(),
                object_path,
                self.proxy.interface().as_str(),
            )
            .map_err(|_| {
                report(&*self.access, self.record, Reason::IncompatibleContract);
                ModuleCallError::IncompatibleContract
            })?;
        let proxy = self
            .timeout
            .map_or(proxy.clone(), |timeout| proxy.with_timeout(timeout));
        Ok(ModuleObjectClient {
            access: Arc::clone(&self.access),
            record: self.record,
            proxy,
            timeout: self.timeout,
        })
    }
}

/// A client pinned to one ordinary module peer.
///
/// The selected unique destination stays private and is reused by ordinary
/// root calls, streamed calls, and object clients. This type carries no native
/// artifact proof and has no confidential-call API.
#[derive(Clone)]
pub struct PinnedModuleClient {
    access: Arc<dyn ModuleAccess>,
    record: &'static ReleaseRecord,
    proxy: Proxy,
    timeout: Duration,
}

impl PinnedModuleClient {
    /// Call an ordinary member on the selected peer.
    pub async fn call<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        invoke_proxy(&*self.access, self.record, &self.proxy, member, args, false).await
    }

    /// Call an ordinary member and retain its remote wire failure fields.
    pub async fn call_detailed<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        invoke_proxy_detailed(&*self.access, self.record, &self.proxy, member, args, false).await
    }

    /// Upload a payload while calling a member on the selected peer.
    pub async fn call_with_stream_detailed<R, F, A>(
        &self,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallFailure>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        let member = crate::MemberName::new(member).map_err(|error| {
            report_proxy_failure_detailed(self.access.reporter(), self.record, &error)
        })?;
        self.proxy
            .connection()
            .call_with_stream_serialized(
                self.proxy.destination().clone(),
                self.proxy.path().clone(),
                self.proxy.interface().clone(),
                member,
                args,
                bytes,
                self.proxy.timeout(),
            )
            .await
            .map_err(|error| {
                report_proxy_failure_detailed(self.access.reporter(), self.record, &error)
            })
    }

    /// Sanitized counterpart to [`Self::call_with_stream_detailed`].
    pub async fn call_with_stream<R, F, A>(
        &self,
        member: &str,
        args: F,
        bytes: &[u8],
    ) -> Result<R, ModuleCallError>
    where
        R: DeserializeOwned,
        F: FnOnce(&crate::stream::StreamRef) -> A,
        A: Serialize,
    {
        self.call_with_stream_detailed(member, args, bytes)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Create an object client pinned to the same unique peer.
    pub fn object_client(
        &self,
        object_path: &str,
    ) -> Result<PinnedModuleObjectClient, ModuleCallError> {
        let proxy = self
            .proxy
            .connection()
            .proxy(
                self.proxy.destination().as_str(),
                object_path,
                self.proxy.interface().as_str(),
            )
            .map_err(|_| {
                report(&*self.access, self.record, Reason::IncompatibleContract);
                ModuleCallError::IncompatibleContract
            })?;
        let proxy = proxy.with_timeout(self.timeout);
        Ok(PinnedModuleObjectClient {
            access: Arc::clone(&self.access),
            record: self.record,
            proxy,
        })
    }
}

/// An ordinary client for an object owned by a pinned module peer.
///
/// Its destination is private, and the surface has no confidential-call API.
#[derive(Clone)]
pub struct PinnedModuleObjectClient {
    access: Arc<dyn ModuleAccess>,
    record: &'static ReleaseRecord,
    proxy: Proxy,
}

impl PinnedModuleObjectClient {
    /// Call an ordinary member of the object.
    pub async fn call<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        self.call_detailed(member, args)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Call the object and preserve an untrusted remote failure's wire details.
    pub async fn call_detailed<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        invoke_proxy_detailed(&*self.access, self.record, &self.proxy, member, args, false).await
    }
}

/// A validated proxy for a module-owned object path.
///
/// Bus object identities do not become product state or leak into logs and
/// controller responses.
#[derive(Clone)]
pub struct ModuleObjectClient {
    access: Arc<dyn ModuleAccess>,
    record: &'static ReleaseRecord,
    proxy: Proxy,
    timeout: Option<Duration>,
}

impl ModuleObjectClient {
    /// Pin this admitted object to its current unique peer, so replacing a
    /// well-known alias cannot redirect its lifecycle or derived children.
    ///
    /// # Errors
    /// Returns a sanitised bus error, or `Unavailable` if the alias has no owner.
    pub async fn pin_owner(&self) -> Result<Self, ModuleCallError> {
        let owner = self
            .proxy
            .connection()
            .name_owner_with_timeout(self.proxy.destination().as_str(), self.proxy.timeout())
            .await
            .map_err(|error| report_proxy_failure(self.access.reporter(), self.record, &error))?
            .ok_or_else(|| {
                report(&*self.access, self.record, Reason::ModuleUnavailable);
                ModuleCallError::Unavailable
            })?;
        self.derive(owner.as_str(), self.proxy.path().as_str())
    }

    /// Derive another object on this object's destination, preserving its
    /// admission, reporter, confidentiality policy and configured deadline.
    ///
    /// # Errors
    /// Returns a sanitised bus error if the object path is invalid.
    pub fn object_at(&self, path: &str) -> Result<Self, ModuleCallError> {
        self.derive(self.proxy.destination().as_str(), path)
    }

    fn derive(&self, destination: &str, path: &str) -> Result<Self, ModuleCallError> {
        let timeout = self.timeout.unwrap_or_else(|| self.proxy.timeout());
        let proxy = self
            .proxy
            .connection()
            .proxy(destination, path, self.proxy.interface().as_str())
            .map_err(|error| {
                report_proxy_failure_detailed(self.access.reporter(), self.record, &error).kind
            })?;
        Ok(Self {
            access: Arc::clone(&self.access),
            record: self.record,
            proxy: proxy.with_timeout(timeout),
            timeout: Some(timeout),
        })
    }

    /// Call a member of the object.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn call<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        self.call_detailed(member, args)
            .await
            .map_err(|failure| failure.kind)
    }

    /// Call this object and preserve an untrusted remote failure's wire details.
    ///
    /// `Debug`, `Display`, and the reporter stay sanitized; the returned
    /// `wire_name` and `message` fields require the caller's redaction policy.
    ///
    /// # Errors
    ///
    /// A detailed [`ModuleCallFailure`].
    pub async fn call_detailed<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallFailure> {
        invoke_proxy_detailed(&*self.access, self.record, &self.proxy, member, args, false).await
    }

    /// Call a confidential member of the object.
    ///
    /// # Errors
    ///
    /// A sanitised [`ModuleCallError`].
    pub async fn call_confidential<R: DeserializeOwned>(
        &self,
        member: &str,
        args: impl Serialize,
    ) -> Result<R, ModuleCallError> {
        invoke_proxy(&*self.access, self.record, &self.proxy, member, args, true).await
    }
}

async fn invoke_proxy<R: DeserializeOwned>(
    access: &dyn ModuleAccess,
    record: &'static ReleaseRecord,
    proxy: &Proxy,
    member: &str,
    args: impl Serialize,
    confidential: bool,
) -> Result<R, ModuleCallError> {
    invoke_proxy_detailed(access, record, proxy, member, args, confidential)
        .await
        .map_err(|failure| failure.kind)
}

async fn invoke_proxy_detailed<R: DeserializeOwned>(
    access: &dyn ModuleAccess,
    record: &'static ReleaseRecord,
    proxy: &Proxy,
    member: &str,
    args: impl Serialize,
    confidential: bool,
) -> Result<R, ModuleCallFailure> {
    let result = if confidential {
        proxy.call_confidential(member, args).await
    } else {
        proxy.call(member, args).await
    };
    result.map_err(|error| report_proxy_failure_detailed(access.reporter(), record, &error))
}

/// Report a bus failure without exposing its payload. Product adapters may
/// separately decode a structured provider refusal for display.
pub fn report_proxy_failure(
    reporter: &FailureReporter,
    record: &ReleaseRecord,
    error: &Error,
) -> ModuleCallError {
    report_proxy_failure_detailed(reporter, record, error).kind
}

fn report_proxy_failure_detailed(
    reporter: &FailureReporter,
    record: &ReleaseRecord,
    error: &Error,
) -> ModuleCallFailure {
    let (outcome, reason) = classify(error);
    if reason == Reason::ModuleUnavailable {
        reporter.report(record.id, record.version, reason);
    } else {
        reporter.report_invocation(record.id, record.version, reason);
    }
    ModuleCallFailure::from_error(error, outcome)
}

fn report(access: &dyn ModuleAccess, record: &ReleaseRecord, reason: Reason) {
    access.reporter().report(record.id, record.version, reason);
}

fn report_invocation(access: &dyn ModuleAccess, record: &ReleaseRecord, reason: Reason) {
    access
        .reporter()
        .report_invocation(record.id, record.version, reason);
}

/// Map a bus error to a sanitised outcome and its closed reason.
#[must_use]
pub fn classify(error: &Error) -> (ModuleCallError, Reason) {
    match error {
        Error::InvalidName { .. } => (
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        Error::ModuleUnavailable { .. } => {
            (ModuleCallError::Unavailable, Reason::ModuleUnavailable)
        }
        Error::IncompatibleVersion { .. }
        | Error::UnknownMethod { .. }
        | Error::UnknownInterface { .. }
        | Error::UnknownObject { .. }
        | Error::BadArguments { .. }
        | Error::Json(_) => (
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        Error::MethodFailed { name, .. } if name == Error::MODULE_UNAVAILABLE => {
            (ModuleCallError::Unavailable, Reason::ModuleUnavailable)
        }
        Error::MethodFailed { name, .. }
            if matches!(
                name.as_str(),
                Error::UNKNOWN_METHOD
                    | "ai.tinyhumans.tinybus.Error.IncompatibleVersion"
                    | "ai.tinyhumans.tinybus.Error.UnknownInterface"
                    | "ai.tinyhumans.tinybus.Error.UnknownObject"
                    | "ai.tinyhumans.tinybus.Error.BadArguments"
            ) =>
        {
            (
                ModuleCallError::IncompatibleContract,
                Reason::IncompatibleContract,
            )
        }
        Error::MethodFailed { name, .. }
            if matches!(
                name.as_str(),
                Error::NOT_ATTESTED
                    | Error::CONFIDENTIALITY_REQUIRED
                    | "ai.tinyhumans.tinybus.Error.Protocol"
            ) =>
        {
            (ModuleCallError::TransportFailed, Reason::TransportFailed)
        }
        Error::MethodFailed { .. } => (ModuleCallError::ModuleFault, Reason::ModuleFault),
        _ => (ModuleCallError::TransportFailed, Reason::TransportFailed),
    }
}

/// Normalize any bus error to the closed module-call classification.
#[must_use]
pub fn wire_kind(error: &Error) -> ModuleCallError {
    classify(error).0
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "client_stream_tests.rs"]
mod stream_tests;

#[cfg(test)]
#[path = "client_pinned_tests.rs"]
mod pinned_tests;
