//! A module reservation and host callback whose cleanup waits for STOP acknowledgement.

use std::{error::Error as StdError, sync::Arc, time::Duration};

#[cfg(feature = "modules")]
use super::runtime::ModuleRuntime;
#[cfg(feature = "modules")]
use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
#[cfg(feature = "modules")]
use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};

/// The lifecycle operation that failed. Values deliberately contain no resource identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePhase {
    /// The module did not provide a reservation.
    Reserve,
    /// The reservation failed host-side validation.
    Validate,
    /// The module object could not be constructed.
    Object,
    /// The host callback factory returned an error or panicked.
    Callback,
    /// The driver could not produce a callback address.
    Address,
    /// The callback could not be registered on the module runtime.
    Export,
    /// Starting the reserved module failed.
    Start,
    /// A stop attempt failed or was not acknowledged.
    Stop,
    /// The dedicated runtime could no longer accept cleanup work.
    Runtime,
}

/// Content-free outcome metadata sent to the caller-provided reporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseReportOutcome {
    /// A STOP attempt acknowledged shutdown.
    Acknowledged,
    /// A STOP attempt returned an error.
    Failed,
    /// A STOP attempt exceeded its configured timeout.
    TimedOut,
    /// The driver reported that the reservation had faulted.
    Faulted,
    /// The finite retry sequence ended without acknowledgement.
    Exhausted,
    /// The driver could not prove whether the operation settled.
    Unacknowledged,
}

/// Result of a driver STOP operation, with retry safety stated explicitly.
pub enum StopOutcome<E> {
    /// Native STOP acknowledged completion.
    Acknowledged,
    /// The operation is known to have settled without acknowledgement and can safely be retried.
    RetryableFailure(E),
    /// The operation may still be active or its completion is unknown; it must not be retried.
    Unacknowledged(E),
}

impl<E> std::fmt::Debug for StopOutcome<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            Self::Acknowledged => "Acknowledged",
            Self::RetryableFailure(_) => "RetryableFailure(<redacted>)",
            Self::Unacknowledged(_) => "Unacknowledged(<redacted>)",
        };
        formatter.write_str(variant)
    }
}

/// Result of a driver START operation, with remote settlement stated explicitly.
pub enum StartOutcome<E> {
    /// The driver has authoritative evidence that START completed successfully.
    Started,
    /// START completed without starting work, so rollback STOP is safe.
    SettledFailure(E),
    /// START may still be active or its completion is unknown; do not issue STOP.
    Unacknowledged(E),
}

impl<E> std::fmt::Debug for StartOutcome<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            Self::Started => "Started",
            Self::SettledFailure(_) => "SettledFailure(<redacted>)",
            Self::Unacknowledged(_) => "Unacknowledged(<redacted>)",
        };
        formatter.write_str(variant)
    }
}

/// Content-free lifecycle metadata; it never includes a reservation, object, or driver error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseReport {
    /// The lifecycle phase being reported.
    pub phase: LeasePhase,
    /// One-based STOP attempt number, when applicable.
    pub attempt: Option<usize>,
    /// The outcome of this event.
    pub outcome: LeaseReportOutcome,
}

/// Bounded STOP retry timing.
///
/// The first attempt begins immediately. Each delay starts one additional attempt, so the total
/// number of attempts is exactly `retry_delays.len() + 1`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    per_attempt_timeout: Duration,
    retry_delays: Vec<Duration>,
}

impl RetryPolicy {
    /// Create a policy with a timeout for every STOP call and a finite backoff sequence.
    ///
    /// # Errors
    /// Returns an error when a STOP attempt has no positive timeout.
    pub fn new(
        per_attempt_timeout: Duration,
        retry_delays: Vec<Duration>,
    ) -> Result<Self, RetryPolicyError> {
        if per_attempt_timeout.is_zero() {
            return Err(RetryPolicyError);
        }
        Ok(Self {
            per_attempt_timeout,
            retry_delays,
        })
    }

    /// Maximum duration of one STOP call.
    #[must_use]
    pub fn per_attempt_timeout(&self) -> Duration {
        self.per_attempt_timeout
    }

    /// The finite delays between STOP attempts.
    #[must_use]
    pub fn retry_delays(&self) -> &[Duration] {
        &self.retry_delays
    }
}

/// A retry policy was missing a positive per-attempt timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicyError;

impl std::fmt::Display for RetryPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("STOP attempt timeout must be positive")
    }
}

impl StdError for RetryPolicyError {}

/// A redacted lifecycle error retaining the original typed driver error for explicit inspection.
pub struct LeaseError<E> {
    phase: LeasePhase,
    attempt: Option<usize>,
    acknowledged: bool,
    driver_error: Option<Arc<E>>,
}

impl<E> LeaseError<E> {
    #[cfg(feature = "modules")]
    fn new(
        phase: LeasePhase,
        attempt: Option<usize>,
        acknowledged: bool,
        driver_error: Option<E>,
    ) -> Self {
        Self {
            phase,
            attempt,
            acknowledged,
            driver_error: driver_error.map(Arc::new),
        }
    }

    /// Lifecycle stage that failed.
    #[must_use]
    pub fn phase(&self) -> LeasePhase {
        self.phase
    }

    /// STOP attempt number associated with the error, if one occurred.
    #[must_use]
    pub fn attempt(&self) -> Option<usize> {
        self.attempt
    }

    /// Whether the native STOP operation was acknowledged.
    #[must_use]
    pub fn acknowledged(&self) -> bool {
        self.acknowledged
    }

    /// The original typed driver error, when a driver operation returned one.
    #[must_use]
    pub fn driver_error(&self) -> Option<&E> {
        self.driver_error.as_deref()
    }
}

impl<E> std::fmt::Debug for LeaseError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseError")
            .field("phase", &self.phase)
            .field("attempt", &self.attempt)
            .field("acknowledged", &self.acknowledged)
            .field(
                "driver_error",
                &self.driver_error.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl<E> std::fmt::Display for LeaseError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.attempt {
            Some(attempt) => write!(
                formatter,
                "module lease {:?} phase failed (attempt {attempt}, acknowledged={})",
                self.phase, self.acknowledged
            ),
            None => write!(
                formatter,
                "module lease {:?} phase failed (acknowledged={})",
                self.phase, self.acknowledged
            ),
        }
    }
}

impl<E: Send + Sync + 'static> StdError for LeaseError<E> {}

/// Host-side driver for one module's reservation protocol.
///
/// The implementation must bind every lifecycle operation to the peer that created the
/// reservation; a well-known-name replacement must never receive an old reservation handle.
#[async_trait::async_trait]
pub trait RuntimeLeaseDriver: Send + Sync + 'static {
    /// Opaque module reservation token.
    type Reservation: Send + Sync + 'static;
    /// Object or handle retained by the public lease.
    type Object: Send + Sync + 'static;
    /// Typed driver error, available through [`LeaseError::driver_error`].
    type Error: StdError + Send + Sync + 'static;

    /// Reserve module-side resources.
    async fn reserve(&self) -> Result<Self::Reservation, Self::Error>;
    /// Validate the reservation before any object construction or callback export.
    fn validate(&self, reservation: &Self::Reservation) -> Result<(), Self::Error>;
    /// Return the exact callback name and path for this reservation and object.
    fn callback_address(
        &self,
        reservation: &Self::Reservation,
        object: &Self::Object,
    ) -> Result<(crate::BusName, crate::ObjectPath), Self::Error>;
    /// Construct the host-side object or handle after validation.
    async fn object(&self, reservation: &Self::Reservation) -> Result<Self::Object, Self::Error>;
    /// Start work associated with this reservation.
    ///
    /// Only return [`StartOutcome::SettledFailure`] when the driver can prove
    /// that no START work remains in flight. Timeouts, lost replies,
    /// cancellation, panics, malformed responses, and opaque transport errors
    /// must be unacknowledged unless separate evidence proves settlement.
    async fn start(&self, reservation: &Self::Reservation) -> StartOutcome<Self::Error>;
    /// Stop work associated with this reservation.
    ///
    /// STOP must be idempotent for this reservation so a proven-settled failure can be retried.
    /// `RetryableFailure` is valid only when the driver has authoritative evidence that this
    /// operation settled and a later STOP cannot overlap it. Timeouts, lost replies, malformed
    /// responses, and faulted peers must be `Unacknowledged` unless separate evidence proves
    /// settlement. `Acknowledged` is the only outcome that permits callback withdrawal.
    async fn stop(&self, reservation: &Self::Reservation) -> StopOutcome<Self::Error>;
    /// Whether this reservation has reached a terminal fault that forbids another STOP attempt.
    fn failed(&self, reservation: &Self::Reservation) -> bool;
}

#[cfg(feature = "modules")]
type WorkerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[cfg(feature = "modules")]
type StartWorker<E> =
    Box<dyn FnOnce(watch::Sender<CloseState<E>>) -> WorkerFuture + Send + 'static>;

#[cfg(feature = "modules")]
enum CloseState<E> {
    Open,
    Running,
    Acknowledged,
    Unacknowledged(Arc<LeaseError<E>>),
}

#[cfg(feature = "modules")]
impl<E> Clone for CloseState<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Open => Self::Open,
            Self::Running => Self::Running,
            Self::Acknowledged => Self::Acknowledged,
            Self::Unacknowledged(error) => Self::Unacknowledged(error.clone()),
        }
    }
}

#[cfg(feature = "modules")]
struct LeaseControl<E> {
    runtime: Handle,
    users: AtomicUsize,
    state: watch::Sender<CloseState<E>>,
    starter: Mutex<Option<StartWorker<E>>>,
}

#[cfg(feature = "modules")]
impl<E: Send + Sync + 'static> LeaseControl<E> {
    fn new(runtime: Handle, starter: StartWorker<E>) -> Self {
        let (state, _) = watch::channel(CloseState::Open);
        Self {
            runtime,
            users: AtomicUsize::new(1),
            state,
            starter: Mutex::new(Some(starter)),
        }
    }

    fn start(&self) {
        let starter = self
            .starter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(starter) = starter {
            self.state.send_replace(CloseState::Running);
            let sender = self.state.clone();
            let worker = starter(sender);
            self.runtime.spawn(worker);
        }
    }

    fn park<T: Send + 'static>(self: &Arc<Self>, retained: T) {
        let control = self.clone();
        self.runtime.spawn(async move {
            let _control = control;
            let _retained = retained;
            std::future::pending::<()>().await;
        });
    }

    async fn wait(&self) -> Result<(), Arc<LeaseError<E>>> {
        let mut state = self.state.subscribe();
        self.start();
        loop {
            match state.borrow().clone() {
                CloseState::Acknowledged => return Ok(()),
                CloseState::Unacknowledged(error) => return Err(error),
                CloseState::Open | CloseState::Running => {}
            }
            if state.changed().await.is_err() {
                return Err(Arc::new(LeaseError::new(
                    LeasePhase::Runtime,
                    None,
                    false,
                    None,
                )));
            }
        }
    }
}

#[cfg(feature = "modules")]
struct ActivationRollback<E: Send + Sync + 'static> {
    control: Arc<LeaseControl<E>>,
    retained_object: Option<Arc<dyn Any + Send + Sync>>,
    armed: bool,
    start_in_flight: bool,
}

#[cfg(feature = "modules")]
impl<E: Send + Sync + 'static> ActivationRollback<E> {
    fn retain_object<T: Any + Send + Sync + 'static>(&mut self, object: Arc<T>) {
        self.retained_object = Some(object);
    }

    fn mark_start_in_flight(&mut self) {
        self.start_in_flight = true;
    }

    fn mark_start_settled(&mut self) {
        self.start_in_flight = false;
    }

    fn park(&mut self) {
        self.armed = false;
        match self.retained_object.take() {
            Some(object) => self.control.park(object),
            None => self.control.park(()),
        }
    }
}

#[cfg(feature = "modules")]
impl<E: Send + Sync + 'static> Drop for ActivationRollback<E> {
    fn drop(&mut self) {
        if self.armed {
            if self.start_in_flight {
                self.park();
            } else {
                self.control.start();
            }
        }
    }
}

/// A cloneable owner of a started module reservation and its callback object.
#[cfg(feature = "modules")]
pub struct RuntimeLease<T, E: Send + Sync + 'static> {
    object: Arc<T>,
    control: Arc<LeaseControl<E>>,
}

#[cfg(feature = "modules")]
impl<T, E: Send + Sync + 'static> RuntimeLease<T, E> {
    /// The object or host handle created for this reservation.
    #[must_use]
    pub fn object(&self) -> &T {
        &self.object
    }

    /// Wait for the one shared cleanup worker. Cancelling this future never cancels that worker.
    pub async fn close(&self) -> Result<(), Arc<LeaseError<E>>>
    where
        E: Send + Sync + 'static,
    {
        self.control.wait().await
    }
}

#[cfg(feature = "modules")]
impl<T, E: Send + Sync + 'static> Clone for RuntimeLease<T, E> {
    fn clone(&self) -> Self {
        self.control.users.fetch_add(1, Ordering::Relaxed);
        Self {
            object: self.object.clone(),
            control: self.control.clone(),
        }
    }
}

#[cfg(feature = "modules")]
impl<T, E: Send + Sync + 'static> Drop for RuntimeLease<T, E> {
    fn drop(&mut self) {
        if self.control.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.control.start();
        }
    }
}

#[cfg(feature = "modules")]
impl ModuleRuntime {
    /// Reserve, validate, export a callback, and start a module on this runtime's owned task.
    ///
    /// This is a normal function: it submits the owned activation transaction before returning
    /// the future. Cancelling the returned future only abandons its result; activation and any
    /// required rollback continue on the dedicated runtime.
    #[allow(clippy::type_complexity)] // The future must retain the driver's object and typed error.
    pub fn lease<D, F, I, R>(
        &self,
        driver: D,
        make_callback: F,
        policy: RetryPolicy,
        reporter: R,
    ) -> impl Future<Output = Result<RuntimeLease<D::Object, D::Error>, LeaseError<D::Error>>>
    + Send
    + 'static
    where
        D: RuntimeLeaseDriver,
        F: FnOnce(&D::Reservation, &D::Object) -> Result<I, D::Error> + Send + 'static,
        I: crate::Interface,
        R: Fn(LeaseReport) + Send + Sync + 'static,
    {
        let host_objects = self.lease_host_object_registry();
        let (response, result) = oneshot::channel();
        self.spawn(async move {
            let runtime = Handle::current();
            let activated = activate(
                runtime,
                host_objects,
                driver,
                make_callback,
                policy,
                reporter,
            )
            .await;
            let _ = response.send(activated);
        });
        async move {
            result
                .await
                .unwrap_or_else(|_| Err(LeaseError::new(LeasePhase::Runtime, None, false, None)))
        }
    }
}

#[cfg(feature = "modules")]
async fn activate<D, F, I, R>(
    runtime: Handle,
    host_objects: Arc<tokio::sync::Mutex<super::runtime::HostObjectRegistry>>,
    driver: D,
    make_callback: F,
    policy: RetryPolicy,
    reporter: R,
) -> Result<RuntimeLease<D::Object, D::Error>, LeaseError<D::Error>>
where
    D: RuntimeLeaseDriver,
    F: FnOnce(&D::Reservation, &D::Object) -> Result<I, D::Error> + Send + 'static,
    I: crate::Interface,
    R: Fn(LeaseReport) + Send + Sync + 'static,
{
    let driver = Arc::new(driver);
    let reservation = match driver.reserve().await {
        Ok(reservation) => Arc::new(reservation),
        Err(error) => {
            return Err(LeaseError::new(
                LeasePhase::Reserve,
                None,
                false,
                Some(error),
            ));
        }
    };

    // This cleanup owner exists before validation and before any later await. The returned lease
    // and every rollback path share this one worker starter and exact callback guard slot.
    let callback_guard = Arc::new(Mutex::new(None));
    let cleanup_driver = driver.clone();
    let cleanup_reservation = reservation.clone();
    let cleanup_policy = policy.clone();
    let cleanup_reporter = Arc::new(reporter);
    let cleanup_guard = callback_guard.clone();
    let starter: StartWorker<D::Error> = Box::new(move |state| {
        Box::pin(cleanup_worker(
            state,
            cleanup_driver,
            cleanup_reservation,
            cleanup_guard,
            cleanup_policy,
            cleanup_reporter,
        ))
    });
    let control = Arc::new(LeaseControl::new(runtime.clone(), starter));
    let mut rollback = ActivationRollback {
        control: control.clone(),
        retained_object: None,
        armed: true,
        start_in_flight: false,
    };

    if let Err(error) = driver.validate(&reservation) {
        let acknowledged = control.wait().await.is_ok();
        return Err(LeaseError::new(
            LeasePhase::Validate,
            None,
            acknowledged,
            Some(error),
        ));
    }

    let object = match driver.object(&reservation).await {
        Ok(object) => Arc::new(object),
        Err(error) => {
            let acknowledged = control.wait().await.is_ok();
            return Err(LeaseError::new(
                LeasePhase::Object,
                None,
                acknowledged,
                Some(error),
            ));
        }
    };
    rollback.retain_object(object.clone());

    let callback = match catch_unwind(AssertUnwindSafe(|| make_callback(&reservation, &object))) {
        Ok(Ok(callback)) => callback,
        Ok(Err(error)) => {
            let acknowledged = control.wait().await.is_ok();
            return Err(LeaseError::new(
                LeasePhase::Callback,
                None,
                acknowledged,
                Some(error),
            ));
        }
        Err(_) => {
            let acknowledged = control.wait().await.is_ok();
            return Err(LeaseError::new(
                LeasePhase::Callback,
                None,
                acknowledged,
                None,
            ));
        }
    };

    let (name, path) = match driver.callback_address(&reservation, &object) {
        Ok(address) => address,
        Err(error) => {
            let acknowledged = control.wait().await.is_ok();
            return Err(LeaseError::new(
                LeasePhase::Address,
                None,
                acknowledged,
                Some(error),
            ));
        }
    };
    let host_object = super::runtime::serve_host_object_transaction(
        host_objects,
        runtime.clone(),
        name,
        path,
        callback,
    )
    .await;
    match host_object {
        Ok(guard) => {
            *callback_guard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(guard)
        }
        Err(_) => {
            let acknowledged = control.wait().await.is_ok();
            return Err(LeaseError::new(
                LeasePhase::Export,
                None,
                acknowledged,
                None,
            ));
        }
    }

    rollback.mark_start_in_flight();
    let start_driver = driver.clone();
    let start_reservation = reservation.clone();
    let start = runtime.spawn(async move { start_driver.start(&start_reservation).await });
    match start.await {
        Ok(StartOutcome::Started) => {
            rollback.mark_start_settled();
            rollback.armed = false;
            Ok(RuntimeLease { object, control })
        }
        Ok(StartOutcome::SettledFailure(error)) => {
            rollback.mark_start_settled();
            let acknowledged = control.wait().await.is_ok();
            Err(LeaseError::new(
                LeasePhase::Start,
                None,
                acknowledged,
                Some(error),
            ))
        }
        Ok(StartOutcome::Unacknowledged(error)) => {
            rollback.park();
            Err(LeaseError::new(LeasePhase::Start, None, false, Some(error)))
        }
        Err(_) => {
            rollback.park();
            Err(LeaseError::new(LeasePhase::Start, None, false, None))
        }
    }
}

#[cfg(feature = "modules")]
async fn cleanup_worker<D, E>(
    state: watch::Sender<CloseState<E>>,
    driver: Arc<D>,
    reservation: Arc<D::Reservation>,
    callback_guard: Arc<Mutex<Option<super::runtime::HostObjectGuard>>>,
    policy: RetryPolicy,
    reporter: Arc<dyn Fn(LeaseReport) + Send + Sync>,
) where
    D: RuntimeLeaseDriver<Error = E>,
    E: StdError + Send + Sync + 'static,
{
    let max_attempts = policy.retry_delays.len() + 1;
    let mut attempt = 1;
    loop {
        if !check_fault(&*driver, &reservation, &state, &reporter, attempt) {
            std::future::pending::<()>().await;
        }

        // The child owns the driver future. A timeout never drops or aborts remote work, and this
        // parent retains the callback guard while joining the same operation for a late result.
        let stop_driver = driver.clone();
        let stop_reservation = reservation.clone();
        let mut stop = tokio::spawn(async move { stop_driver.stop(&stop_reservation).await });
        let result = match tokio::time::timeout(policy.per_attempt_timeout, &mut stop).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => {
                park_unacknowledged(
                    &state,
                    &reporter,
                    attempt,
                    None,
                    LeaseReportOutcome::Unacknowledged,
                )
                .await
            }
            Err(_) => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::TimedOut,
                    },
                );
                state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                    LeasePhase::Stop,
                    Some(attempt),
                    false,
                    None,
                ))));
                match stop.await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        park_unacknowledged(
                            &state,
                            &reporter,
                            attempt,
                            None,
                            LeaseReportOutcome::Unacknowledged,
                        )
                        .await
                    }
                }
            }
        };

        match result {
            StopOutcome::Acknowledged => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::Acknowledged,
                    },
                );
                let guard = callback_guard
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(guard) = guard {
                    guard.remove().await;
                }
                state.send_replace(CloseState::Acknowledged);
                return;
            }
            StopOutcome::RetryableFailure(error) => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::Failed,
                    },
                );
                if attempt == max_attempts {
                    park_unacknowledged(
                        &state,
                        &reporter,
                        attempt,
                        Some(error),
                        LeaseReportOutcome::Exhausted,
                    )
                    .await;
                }
            }
            StopOutcome::Unacknowledged(error) => {
                park_unacknowledged(
                    &state,
                    &reporter,
                    attempt,
                    Some(error),
                    LeaseReportOutcome::Unacknowledged,
                )
                .await;
            }
        }

        if attempt < max_attempts {
            if !check_fault(&*driver, &reservation, &state, &reporter, attempt) {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(policy.retry_delays[attempt - 1]).await;
            attempt += 1;
        }
    }
}

#[cfg(feature = "modules")]
fn check_fault<D, E>(
    driver: &D,
    reservation: &D::Reservation,
    state: &watch::Sender<CloseState<E>>,
    reporter: &Arc<dyn Fn(LeaseReport) + Send + Sync>,
    attempt: usize,
) -> bool
where
    D: RuntimeLeaseDriver<Error = E>,
    E: StdError + Send + Sync + 'static,
{
    match catch_unwind(AssertUnwindSafe(|| driver.failed(reservation))) {
        Ok(false) => true,
        Ok(true) => {
            mark_unacknowledged(state, reporter, attempt, None, LeaseReportOutcome::Faulted);
            false
        }
        Err(_) => {
            mark_unacknowledged(
                state,
                reporter,
                attempt,
                None,
                LeaseReportOutcome::Unacknowledged,
            );
            false
        }
    }
}

#[cfg(feature = "modules")]
fn mark_unacknowledged<E: Send + Sync + 'static>(
    state: &watch::Sender<CloseState<E>>,
    reporter: &Arc<dyn Fn(LeaseReport) + Send + Sync>,
    attempt: usize,
    error: Option<E>,
    outcome: LeaseReportOutcome,
) {
    report(
        reporter,
        LeaseReport {
            phase: LeasePhase::Stop,
            attempt: Some(attempt),
            outcome,
        },
    );
    state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
        LeasePhase::Stop,
        Some(attempt),
        false,
        error,
    ))));
}

#[cfg(feature = "modules")]
async fn park_unacknowledged<E: Send + Sync + 'static>(
    state: &watch::Sender<CloseState<E>>,
    reporter: &Arc<dyn Fn(LeaseReport) + Send + Sync>,
    attempt: usize,
    error: Option<E>,
    outcome: LeaseReportOutcome,
) -> ! {
    mark_unacknowledged(state, reporter, attempt, error, outcome);
    loop {
        std::future::pending::<()>().await;
    }
}

#[cfg(feature = "modules")]
fn report(reporter: &Arc<dyn Fn(LeaseReport) + Send + Sync>, event: LeaseReport) {
    // Host reporting is observational and cannot interrupt the cleanup owner.
    let _ = catch_unwind(AssertUnwindSafe(|| reporter(event)));
}

#[cfg(all(test, feature = "modules"))]
#[path = "lease_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lease_contract_tests.rs"]
mod contract_tests;
