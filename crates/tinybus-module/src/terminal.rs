//! Opted-in instance terminal authority and bounded physical cleanup ownership.
use super::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::sync::{Condvar, Weak};
use tinybus::module::abi::{
    TB_NATIVE_TERMINAL_FAULT, TB_NATIVE_TERMINAL_LIVE, TB_NATIVE_TERMINAL_STOP_INTENT,
};

/// Cause of irreversible instance admission closure. Genuine fault takes
/// precedence over ordinary stop or host disappearance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalReason {
    /// Ordinary stop intent; it does not assert worker completion.
    Stop,
    /// The native query is unavailable or no longer grants live admission.
    HostClosed,
    /// A genuine native or module fault, including a caught panic.
    Fault,
    /// The factory or setup returned an error; native fault is also reported.
    SetupFailed,
}
/// Actual controller cleanup outcome, independent of native admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalCompletion {
    /// The controller has fenced admission and joined its actual workers.
    /// This assertion alone proves neither SDK teardown nor backing last-Drop.
    PhysicallyStopped,
    /// Controller or SDK physical cleanup could not be proved. Even joined
    /// workers can leave uncertain SDK resources. The SDK retains the owner;
    /// a deadline cannot convert this into successful cleanup.
    Uncertain,
}
/// Controller callbacks supplied by the terminal factory.
///
/// The factory owns partial-failure cleanup until it returns these hooks. It
/// must launch independent workers only during setup, after hook registration.
/// The same controller remains responsible through every reinitialization.
pub struct RuntimeHooks {
    request: Arc<dyn Fn(TerminalReason) + Send + Sync>,
    finish: Box<dyn FnOnce() -> TerminalCompletion + Send>,
}
impl RuntimeHooks {
    /// `request` must only close admission and signal bounded cancel/wake. It
    /// must not join, perform I/O, reenter SDK APIs, or wait for a gate. `finish`
    /// independently fences and joins the controller after SDK setup cancellation
    /// has settled; it must not depend on cancelled futures or host shutdown.
    /// Returning `PhysicallyStopped` asserts actual fence/join, not a timeout.
    /// The SDK additionally requires its own resource teardown without sticky
    /// uncertainty before accepting physical completion. Trusted callbacks must
    /// uphold these obligations; the SDK cannot force arbitrary Rust to finish.
    pub fn new<R, F>(request: R, finish: F) -> Self
    where
        R: Fn(TerminalReason) + Send + Sync + 'static,
        F: FnOnce() -> TerminalCompletion + Send + 'static,
    {
        Self {
            request: Arc::new(request),
            finish: Box::new(finish),
        }
    }
}
/// A retained view of one actual admitted native instance. Its lifetime retains
/// the instance's admission permit even after successful physical cleanup.
/// Each SDK image admits at most 256 retained instances; views keep their slot
/// occupied until actual last-Drop. This does not bound controller resource bytes
/// or the host's existing context and mapped-library retention.
#[derive(Clone)]
pub struct NativeInstanceLiveness {
    pub(super) inner: Arc<TerminalState>,
}
impl NativeInstanceLiveness {
    /// Only a supported, exactly-live native query allows new admission.
    /// Observed terminal state never reopens. This is not a publication lease.
    /// A controller must check its immutable instance/receiver binding and local
    /// terminal flags, then this view, then local flags again under its physical
    /// phase gate. A cached `true` cannot authorize a later phase; a later stop
    /// cannot undo an operation whose admission already linearized.
    pub fn is_live(&self) -> bool {
        self.inner.is_live()
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_owner_is_current(&self) -> bool {
        self.inner
            .owner
            .as_ref()
            .is_some_and(ExecutionOwner::is_current_for_fixture)
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_owner_is_stopped(&self) -> bool {
        self.inner
            .owner
            .as_ref()
            .is_some_and(ExecutionOwner::is_stopped_for_fixture)
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_request_done(&self) -> bool {
        self.inner.request_done_for_test()
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_sdk_owners(&self) -> usize {
        self.inner.sdk_owner_count_for_test()
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_reject_worker_spawn(&self) {
        self.inner.reject_worker_spawn_for_test();
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_reject_startup(&self, stage: u8) {
        self.inner.reject_startup_for_test(stage);
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_resources(&self) -> u32 {
        self.inner.resources_for_fixture()
    }
    #[doc(hidden)]
    #[cfg(feature = "__native-fixture-support")]
    pub fn __fixture_transport_dropped_on_physical_worker(&self) -> bool {
        self.inner.transport_drop_thread.lock().unwrap().as_deref() == Some("tinybus-terminal")
    }
    /// Persist genuine fault and close independent native admission first.
    /// Signals the bounded request hook without joining. Call this after
    /// unwinding out of the controller's physical phase gate.
    pub fn request_fault(&self) {
        self.inner.signal(TerminalReason::Fault);
    }
}
const INSTANCE_LIMIT: usize = 256;
#[derive(Default)]
struct Slot {
    reserved: bool,
    exhausted: bool,
    generation: usize,
    anchor: Option<Arc<TerminalState>>,
}
struct Admission {
    slots: StdMutex<[Slot; INSTANCE_LIMIT]>,
}
impl Admission {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: StdMutex::new(std::array::from_fn(|_| Slot::default())),
        })
    }
}
static ADMISSION: OnceLock<Arc<Admission>> = OnceLock::new();
struct SlotLease {
    admission: Weak<Admission>,
    index: usize,
}
#[cfg(test)]
pub(super) struct AnchorRemovalGate {
    pub(super) entered: std::sync::Barrier,
    pub(super) release: std::sync::Barrier,
}
impl Drop for SlotLease {
    fn drop(&mut self) {
        if let Some(admission) = self.admission.upgrade() {
            let mut slots = admission
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // The permit ends only at actual State last-Drop, never at anchor retirement.
            slots[self.index].reserved = false;
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum RequestState {
    Unregistered,
    Idle,
    Running,
    Done,
    Panicked,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum FinishState {
    Idle,
    Submitted,
    Running,
    Complete,
    SpawnFailed,
}
struct FinishJob {
    state: Weak<TerminalState>,
}
impl FinishJob {
    fn run(self: Arc<Self>, state: Arc<TerminalState>) {
        state.run_finish();
    }
}
type Payload = Box<dyn Any + Send>;
#[derive(Default)]
struct PrimaryPayloads {
    request: Option<Payload>,
    finish: Option<Payload>,
    initial: Option<Payload>,
    reinit: Option<Payload>,
    factory: Option<Payload>,
    startup: Option<Payload>,
    cleanup: Option<Payload>,
}
#[derive(Clone, Copy)]
pub(super) enum PayloadKind {
    Request,
    Finish,
    Initial,
    Reinit,
    Factory,
    Startup,
    Cleanup,
}
impl PrimaryPayloads {
    fn cell(&mut self, kind: PayloadKind) -> &mut Option<Payload> {
        match kind {
            PayloadKind::Request => &mut self.request,
            PayloadKind::Finish => &mut self.finish,
            PayloadKind::Initial => &mut self.initial,
            PayloadKind::Reinit => &mut self.reinit,
            PayloadKind::Factory => &mut self.factory,
            PayloadKind::Startup => &mut self.startup,
            PayloadKind::Cleanup => &mut self.cleanup,
        }
    }
}
enum ReinitOperation {
    Retire(SetupFuture),
    Construct(Box<dyn FnOnce() -> i32 + Send>),
}
struct Control {
    request: Option<Arc<dyn Fn(TerminalReason) + Send + Sync>>,
    finish: Option<Box<dyn FnOnce() -> TerminalCompletion + Send>>,
    request_state: RequestState,
    finish_state: FinishState,
    published: bool,
    completion: Option<TerminalCompletion>,
    job: Option<Arc<FinishJob>>,
    payloads: PrimaryPayloads,
    active_reinit: bool,
    reinit_operation: Option<ReinitOperation>,
    construction_code: i32,
    prior_pending: bool,
    retirement_failed: bool,
}
pub(super) type Setup =
    Box<dyn FnOnce(Connection) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;
pub(super) type SetupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub(super) type SetupFutureSlot = Arc<StdMutex<Option<SetupFuture>>>;
pub(super) type TerminalReinitialize = Arc<dyn Fn(*const u8, usize) -> i32 + Send + Sync>;

/// Resources are attached here before the next fallible startup boundary.
/// Taking values is mechanical; all foreign destruction occurs off this lock.
pub(super) struct StartupResources {
    pub runtime: Option<tokio::runtime::Runtime>,
    pub setup: Option<Setup>,
    pub retained_factory: Option<Box<dyn Any + Send>>,
    pub config: Option<Box<dyn Any + Send>>,
    pub reinitializer_factory: Option<ReinitializeFactory>,
    pub reinitialize: Arc<StdMutex<Option<TerminalReinitialize>>>,
    pub connection: Arc<StdMutex<Option<Connection>>>,
    pub inbound: Option<mpsc::Sender<IncomingFrame>>,
    pub transport: Option<Arc<TerminalTransport>>,
    pub initial: Option<tokio::task::JoinHandle<Result<()>>>,
    pub initial_future: SetupFutureSlot,
    pub reinit_future: SetupFutureSlot,
}
impl Default for StartupResources {
    fn default() -> Self {
        Self {
            runtime: None,
            setup: None,
            retained_factory: None,
            config: None,
            reinitializer_factory: None,
            reinitialize: Arc::new(StdMutex::new(None)),
            connection: Arc::new(StdMutex::new(None)),
            inbound: None,
            transport: None,
            initial: None,
            initial_future: Arc::new(StdMutex::new(None)),
            reinit_future: Arc::new(StdMutex::new(None)),
        }
    }
}
pub(super) struct TerminalState {
    pub host: HostCalls,
    pub owner: Option<ExecutionOwner>,
    reason: AtomicU8,
    uncertain: AtomicBool,
    control: StdMutex<Control>,
    changed: Condvar,
    pub terminal_notify: tokio::sync::Notify,
    pub resources: StdMutex<StartupResources>,
    sdk_owners: AtomicUsize,
    vtable_published: AtomicBool,
    #[cfg(any(test, feature = "__native-fixture-support"))]
    reject_worker_spawn: AtomicBool,
    #[cfg(any(test, feature = "__native-fixture-support"))]
    startup_failure_stage: AtomicU8,
    #[cfg(any(test, feature = "__native-fixture-support"))]
    transport_drop_thread: StdMutex<Option<String>>,
    #[cfg(test)]
    before_anchor_removal: StdMutex<Option<Arc<AnchorRemovalGate>>>,
    #[cfg(test)]
    initial_future_restored: tokio::sync::Notify,
    lease: SlotLease,
    token: usize,
}
/// Every surviving SDK transport/Connection/task retains this actual State.
/// A pending runtime teardown may therefore never recycle its slot.
pub(super) struct SdkOwner {
    state: Arc<TerminalState>,
}
impl SdkOwner {
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn record_transport_drop_for_test(&self) {
        *self.state.transport_drop_thread.lock().unwrap() = Some(
            std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_owned(),
        );
    }
    pub fn caught_panic(&self) {
        self.state.mark_uncertain();
        self.state.signal(TerminalReason::Fault);
    }
}
impl Drop for SdkOwner {
    fn drop(&mut self) {
        // A last-field guard also runs when a preceding foreign destructor
        // unwinds. Counting that as clean disposal would fabricate completion.
        // The diagnostic owner may already be stopped; explicitly fence native
        // authority and preserve the anchor before releasing the resource count.
        if std::thread::panicking() {
            self.caught_panic();
        }
        self.state.sdk_owners.fetch_sub(1, Ordering::AcqRel);
    }
}
impl TerminalState {
    pub fn admit(host: HostCalls, owner: Option<ExecutionOwner>) -> Option<Arc<Self>> {
        Self::admit_in(ADMISSION.get_or_init(Admission::new), host, owner)
    }
    fn admit_in(
        admission: &Arc<Admission>,
        host: HostCalls,
        owner: Option<ExecutionOwner>,
    ) -> Option<Arc<Self>> {
        let mut slots = admission
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = slots.iter_mut().enumerate().find_map(|(index, slot)| {
            if slot.reserved || slot.exhausted {
                return None;
            }
            let generation = slot
                .generation
                .checked_add(1)
                .filter(|generation| *generation <= usize::MAX >> 8);
            match generation {
                Some(generation) => {
                    slot.generation = generation;
                    Some(index)
                }
                None => {
                    slot.exhausted = true;
                    None
                }
            }
        })?;
        slots[index].reserved = true;
        let token = (slots[index].generation << 8) | index;
        let state = Arc::new(Self {
            host,
            owner,
            reason: AtomicU8::new(0),
            uncertain: AtomicBool::new(false),
            control: StdMutex::new(Control {
                request: None,
                finish: None,
                request_state: RequestState::Unregistered,
                finish_state: FinishState::Idle,
                published: false,
                completion: None,
                job: None,
                payloads: PrimaryPayloads::default(),
                active_reinit: false,
                reinit_operation: None,
                construction_code: TB_CLOSED,
                prior_pending: false,
                retirement_failed: false,
            }),
            changed: Condvar::new(),
            terminal_notify: tokio::sync::Notify::new(),
            resources: StdMutex::new(StartupResources::default()),
            sdk_owners: AtomicUsize::new(0),
            vtable_published: AtomicBool::new(false),
            #[cfg(any(test, feature = "__native-fixture-support"))]
            reject_worker_spawn: AtomicBool::new(false),
            #[cfg(any(test, feature = "__native-fixture-support"))]
            startup_failure_stage: AtomicU8::new(0),
            #[cfg(any(test, feature = "__native-fixture-support"))]
            transport_drop_thread: StdMutex::new(None),
            #[cfg(test)]
            before_anchor_removal: StdMutex::new(None),
            #[cfg(test)]
            initial_future_restored: tokio::sync::Notify::new(),
            lease: SlotLease {
                admission: Arc::downgrade(admission),
                index,
            },
            token,
        });
        slots[index].anchor = Some(state.clone());
        Some(state)
    }
    /// Opaque address token only; no allocation or dereference occurs.
    pub fn context(&self) -> *mut c_void {
        std::ptr::with_exposed_provenance_mut(self.token)
    }
    pub fn lookup(context: *mut c_void) -> Option<Arc<Self>> {
        Self::lookup_in(ADMISSION.get()?, context.addr())
    }
    fn lookup_in(admission: &Arc<Admission>, token: usize) -> Option<Arc<Self>> {
        let generation = token >> 8;
        if generation == 0 {
            return None;
        }
        let slots = admission
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = &slots[token & (INSTANCE_LIMIT - 1)];
        if slot.generation != generation {
            return None;
        }
        // Strong exact State is acquired before releasing registry exclusion.
        slot.anchor
            .as_ref()
            .filter(|state| state.token == token)
            .cloned()
    }
    pub fn mark_vtable_published(&self) {
        self.vtable_published.store(true, Ordering::Release);
    }
    pub fn retain_uncertain_anchor(self: &Arc<Self>) {
        let mut control = self.lock();
        self.record_uncertain_locked(&mut control);
        drop(control);
        self.changed.notify_all();
    }
    pub fn view(self: &Arc<Self>) -> NativeInstanceLiveness {
        NativeInstanceLiveness {
            inner: self.clone(),
        }
    }
    pub fn sdk_owner(self: &Arc<Self>) -> SdkOwner {
        self.sdk_owners.fetch_add(1, Ordering::AcqRel);
        SdkOwner {
            state: self.clone(),
        }
    }
    pub fn is_terminal(&self) -> bool {
        self.reason.load(Ordering::Acquire) != 0
    }
    #[cfg(test)]
    pub async fn wait_initial_future_restored_for_test(&self) {
        self.initial_future_restored.notified().await;
    }
    #[cfg(test)]
    pub(super) fn initial_future_restored_for_test(&self) {
        self.initial_future_restored.notify_one();
    }
    pub fn is_live(self: &Arc<Self>) -> bool {
        if self.is_terminal() {
            return false;
        }
        self.observe_host();
        !self.is_terminal()
    }
    // Cleanup must sample even when local Stop already closed admission: a real
    // later native Fault is orthogonal to that local admission predicate.
    pub fn observe_host(self: &Arc<Self>) {
        let flags = self
            .host
            .0
            .native_terminal_state
            .map(|query| unsafe { query(self.host.0.host_ctx) });
        if flags != Some(TB_NATIVE_TERMINAL_LIVE) {
            let reason = if flags.is_some_and(|flags| flags & TB_NATIVE_TERMINAL_FAULT != 0) {
                TerminalReason::Fault
            } else if flags.is_some_and(|flags| flags & TB_NATIVE_TERMINAL_STOP_INTENT != 0) {
                TerminalReason::Stop
            } else {
                TerminalReason::HostClosed
            };
            self.signal(reason);
        }
    }
    fn reason(&self) -> TerminalReason {
        match self.reason.load(Ordering::Acquire) {
            1 => TerminalReason::Stop,
            2 => TerminalReason::HostClosed,
            4 => TerminalReason::SetupFailed,
            _ => TerminalReason::Fault,
        }
    }
    fn commit(&self, reason: TerminalReason) {
        let next = match reason {
            TerminalReason::Stop => 1,
            TerminalReason::HostClosed => 2,
            TerminalReason::Fault => 3,
            TerminalReason::SetupFailed => 4,
        };
        let mut old = self.reason.load(Ordering::Acquire);
        while old == 0 || (old < 3 && next >= 3) {
            match self
                .reason
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
        self.terminal_notify.notify_waiters();
    }
    pub fn signal(self: &Arc<Self>, reason: TerminalReason) {
        if matches!(reason, TerminalReason::Fault | TerminalReason::SetupFailed) {
            self.host.fault();
        }
        self.commit(reason);
        self.run_request();
        self.schedule();
    }
    pub fn source_panic(self: &Arc<Self>, kind: PayloadKind, payload: Payload) {
        // Atomic authority fence must precede payload-cell locks and foreign Drop.
        self.host.fault();
        self.commit(TerminalReason::Fault);
        self.retain_uncertain_anchor();
        self.keep_payload(kind, payload);
        self.run_request();
        self.schedule();
    }
    fn lock(self: &Arc<Self>) -> std::sync::MutexGuard<'_, Control> {
        match self.control.lock() {
            Ok(control) => control,
            Err(poison) => {
                let mut control = poison.into_inner();
                self.record_uncertain_locked(&mut control);
                drop(control);
                self.changed.notify_all();
                // Host callbacks are foreign code and must run after releasing
                // Control. Uncertainty and its anchor were recorded first.
                self.host.fault();
                self.commit(TerminalReason::Fault);
                self.control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            }
        }
    }
    fn record_uncertain_locked(self: &Arc<Self>, control: &mut Control) {
        self.uncertain.store(true, Ordering::Release);
        if control.completion == Some(TerminalCompletion::PhysicallyStopped) {
            control.completion = Some(TerminalCompletion::Uncertain);
        }
        if let Some(admission) = self.lease.admission.upgrade() {
            let mut slots = admission
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let slot = &mut slots[self.lease.index];
            // This State's still-live lease forbids reuse while a C callback or
            // retained view performs the final genuine-fault recheck.
            if slot.generation == self.token >> 8 && slot.anchor.is_none() {
                slot.anchor = Some(self.clone());
            }
        }
    }
    fn keep_payload(self: &Arc<Self>, kind: PayloadKind, payload: Payload) {
        let mut control = self.lock();
        let cell = control.payloads.cell(kind);
        // Once/terminal admission prevents a second primary in this category.
        // A violated internal invariant cannot discard a foreign payload here.
        // Call sites admit this category once and deny all later invocation.
        // No assertion/unwind, replacement retry, or payload disposal path.
        *cell = Some(payload);
    }
    pub fn register(self: &Arc<Self>, hooks: RuntimeHooks) {
        {
            let mut control = self.lock();
            control.request = Some(hooks.request);
            control.finish = Some(hooks.finish);
            control.request_state = RequestState::Idle;
        }
        if self.is_terminal() {
            self.run_request();
            self.schedule();
        }
    }
    fn run_request(self: &Arc<Self>) {
        let request = {
            let mut control = self.lock();
            if !self.is_terminal() || control.request_state != RequestState::Idle {
                return;
            }
            control.request_state = RequestState::Running;
            control.request.clone()
        };
        let _scope = self.owner.as_ref().map(ExecutionOwner::enter);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            if let Some(request) = request {
                request(self.reason());
            }
        }));
        match outcome {
            Ok(()) => {
                self.lock().request_state = RequestState::Done;
                self.changed.notify_all();
            }
            Err(payload) => {
                self.host.fault();
                self.commit(TerminalReason::Fault);
                let mut control = self.lock();
                self.record_uncertain_locked(&mut control);
                control.request_state = RequestState::Panicked;
                drop(control);
                self.changed.notify_all();
                self.keep_payload(PayloadKind::Request, payload);
            }
        }
    }
    pub fn publish(self: &Arc<Self>) {
        self.lock().published = true;
        self.changed.notify_all();
        if self.is_terminal() {
            self.run_request();
            self.schedule();
        }
    }
    pub fn begin_reinit(self: &Arc<Self>) -> bool {
        let mut control = self.lock();
        if self.is_terminal() || control.active_reinit {
            return false;
        }
        control.active_reinit = true;
        true
    }
    pub fn end_reinit(self: &Arc<Self>) {
        self.lock().active_reinit = false;
        self.changed.notify_all();
    }
    pub fn retire_reinit(self: &Arc<Self>, future: Option<SetupFuture>) -> bool {
        let Some(future) = future else {
            return true;
        };
        {
            let mut control = self.lock();
            // active_reinit admits one caller. It cannot construct the next
            // future until the worker has acknowledged this exact disposal.
            control.reinit_operation = Some(ReinitOperation::Retire(future));
            control.prior_pending = true;
        }
        self.schedule();
        self.changed.notify_all();
        let mut control = self.lock();
        while control.prior_pending && control.finish_state != FinishState::SpawnFailed {
            control = self
                .changed
                .wait(control)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        !control.retirement_failed && control.finish_state != FinishState::SpawnFailed
    }
    pub fn construct_reinit(self: &Arc<Self>, construct: Box<dyn FnOnce() -> i32 + Send>) -> i32 {
        {
            let mut control = self.lock();
            // The same sole cell owns construction captures before scheduling.
            // Spawn failure retains them; no ABI caller receives generic C.
            control.reinit_operation = Some(ReinitOperation::Construct(construct));
            control.prior_pending = true;
        }
        self.schedule();
        self.changed.notify_all();
        let mut control = self.lock();
        while control.prior_pending && control.finish_state != FinishState::SpawnFailed {
            control = self
                .changed
                .wait(control)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if control.finish_state == FinishState::SpawnFailed {
            TB_CLOSED
        } else {
            control.construction_code
        }
    }
    #[cfg(test)]
    pub(super) fn transport_drop_thread_for_test(&self) -> Option<String> {
        self.transport_drop_thread.lock().unwrap().clone()
    }
    #[cfg(feature = "__native-fixture-support")]
    fn resources_for_fixture(&self) -> u32 {
        let resources = self.resources.lock().unwrap();
        let flags = [
            resources.retained_factory.is_some(),
            resources.setup.is_some(),
            resources.reinitializer_factory.is_some(),
            resources.runtime.is_some(),
            resources.transport.is_some(),
            resources.reinitialize.lock().unwrap().is_some(),
            resources.initial.is_some(),
            resources.config.is_some(),
            resources.initial_future.lock().unwrap().is_some(),
            resources.reinit_future.lock().unwrap().is_some(),
        ];
        flags
            .iter()
            .enumerate()
            .fold(0, |mask, (index, held)| mask | (u32::from(*held) << index))
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn reject_runtime_build_for_fixture(&self) -> bool {
        self.startup_failure_stage.load(Ordering::Acquire) == 9
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn reject_startup_for_test(&self, stage: u8) {
        self.startup_failure_stage.store(stage, Ordering::Release);
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn hold_initial_before_transfer_for_test(&self) -> bool {
        matches!(self.startup_failure_stage.load(Ordering::Acquire), 7 | 8)
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn startup_checkpoint_for_test(&self, stage: u8) {
        if self.startup_failure_stage.load(Ordering::Acquire) == stage {
            panic!("controlled startup failure after transfer stage {stage}");
        }
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn reject_worker_spawn_for_test(&self) {
        self.reject_worker_spawn.store(true, Ordering::Release);
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn sdk_owner_count_for_test(&self) -> usize {
        self.sdk_owners.load(Ordering::Acquire)
    }
    #[cfg(any(test, feature = "__native-fixture-support"))]
    pub(super) fn request_done_for_test(self: &Arc<Self>) -> bool {
        matches!(
            self.lock().request_state,
            RequestState::Done | RequestState::Panicked
        )
    }
    #[cfg(test)]
    pub(super) fn wait_request_done_for_test(self: &Arc<Self>, timeout: Duration) -> bool {
        let control = self.lock();
        let (control, _) = self
            .changed
            .wait_timeout_while(control, timeout, |control| {
                !matches!(
                    control.request_state,
                    RequestState::Done | RequestState::Panicked
                )
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        matches!(
            control.request_state,
            RequestState::Done | RequestState::Panicked
        )
    }
    fn schedule(self: &Arc<Self>) {
        let job = {
            let mut control = self.lock();
            if control.finish_state != FinishState::Idle {
                return;
            }
            let job = Arc::new(FinishJob {
                state: Arc::downgrade(self),
            });
            control.job = Some(job.clone());
            control.finish_state = FinishState::Submitted;
            job
        };
        let state = job.state.upgrade().expect("anchored terminal state");
        // Private per-instance rejection exercises the real submitted-job ownership
        // path without changing process-wide thread creation or public authority.
        #[cfg(any(test, feature = "__native-fixture-support"))]
        let rejected = self.reject_worker_spawn.load(Ordering::Acquire);
        #[cfg(not(any(test, feature = "__native-fixture-support")))]
        let rejected = false;
        let submitted = if rejected {
            Err(std::io::Error::other(
                "controlled terminal worker spawn failure",
            ))
        } else {
            std::thread::Builder::new()
                .name("tinybus-terminal".into())
                .spawn(move || job.run(state))
        };
        if submitted.is_err() {
            let mut control = self.lock();
            self.record_uncertain_locked(&mut control);
            control.finish_state = FinishState::SpawnFailed;
            control.completion = Some(TerminalCompletion::Uncertain);
            drop(control);
            self.changed.notify_all();
            self.host.fault();
            self.commit(TerminalReason::Fault);
            self.run_request();
        }
    }
    fn take_anchor(&self) -> Option<Arc<Self>> {
        if let Some(admission) = self.lease.admission.upgrade() {
            let mut slots = admission
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slots[self.lease.index].anchor.take()
        } else {
            None
        }
    }
    #[cfg(test)]
    fn pause_before_anchor_removal_for_test(&self) {
        let gate = self
            .before_anchor_removal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(gate) = gate {
            gate.entered.wait();
            gate.release.wait();
        }
    }
    pub fn empty_factory_failure(self: &Arc<Self>) {
        // No callable/controller was transferred. Retained views still hold the permit.
        self.commit(TerminalReason::SetupFailed);
        drop(self.take_anchor());
    }
    fn run_finish(self: &Arc<Self>) {
        let _scope = self.owner.as_ref().map(ExecutionOwner::enter);
        loop {
            let mut control = self.lock();
            // Retirement takes precedence over waiting for active_reinit:
            // that caller may itself be awaiting this worker's acknowledgment.
            if let Some(operation) = control.reinit_operation.take() {
                drop(control);
                let (failed, code) = match operation {
                    ReinitOperation::Retire(future) => {
                        match catch_unwind(AssertUnwindSafe(|| drop(future))) {
                            Ok(()) => (false, TB_OK),
                            Err(payload) => {
                                self.source_panic(PayloadKind::Cleanup, payload);
                                (true, TB_CLOSED)
                            }
                        }
                    }
                    ReinitOperation::Construct(construct) => {
                        match catch_unwind(AssertUnwindSafe(construct)) {
                            Ok(code) => (false, code),
                            Err(payload) => {
                                self.source_panic(PayloadKind::Reinit, payload);
                                (false, TB_PANICKED)
                            }
                        }
                    }
                };
                let mut control = self.lock();
                control.retirement_failed |= failed;
                control.construction_code = code;
                control.prior_pending = false;
                drop(control);
                self.changed.notify_all();
                continue;
            }
            if control.published
                && !control.active_reinit
                && !matches!(
                    control.request_state,
                    RequestState::Idle | RequestState::Running
                )
            {
                control.finish_state = FinishState::Running;
                break;
            }
            drop(
                self.changed
                    .wait(control)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        // Execution settlement is distinct from foreign capture destruction.
        // A failed prior retirement retains all later captures, but does not
        // spend the controller's still-uninvoked physical finish callback.
        let execution_settled = self.settle_execution();
        let retirement_failed = self.lock().retirement_failed;
        let disposal_failed = if execution_settled && !retirement_failed {
            match catch_unwind(AssertUnwindSafe(|| self.dispose_setup())) {
                Ok(()) => false,
                Err(payload) => {
                    self.source_panic(PayloadKind::Cleanup, payload);
                    true
                }
            }
        } else {
            retirement_failed
        };
        let finish = self.lock().finish.take();
        let completed = if execution_settled {
            if let Some(owner) = &self.owner {
                owner.stop();
            }
            match catch_unwind(AssertUnwindSafe(|| {
                finish.map_or(TerminalCompletion::PhysicallyStopped, |finish| finish())
            })) {
                Ok(completion) => completion,
                Err(payload) => {
                    self.source_panic(PayloadKind::Finish, payload);
                    TerminalCompletion::Uncertain
                }
            }
        } else {
            // If actual task settlement could not be proved, retain the original finish owner.
            self.lock().finish = finish;
            TerminalCompletion::Uncertain
        };
        let released = if completed == TerminalCompletion::PhysicallyStopped && !disposal_failed {
            match catch_unwind(AssertUnwindSafe(|| self.release_resources())) {
                Ok(released) => released,
                Err(payload) => {
                    self.source_panic(PayloadKind::Cleanup, payload);
                    false
                }
            }
        } else {
            false
        };
        self.observe_host();
        let failed_final_checks =
            !released || (self.has_fault() && self.vtable_published.load(Ordering::Acquire));
        // Let a concurrent deadline/fault retain its uncertainty before taking
        // the finalization lock. The lock below linearizes anchor retirement and
        // completion publication against that re-anchor operation.
        #[cfg(test)]
        if completed == TerminalCompletion::PhysicallyStopped && !failed_final_checks {
            self.pause_before_anchor_removal_for_test();
        }
        let mut control = self.lock();
        if failed_final_checks {
            self.record_uncertain_locked(&mut control);
        }
        let completion = if self.uncertain.load(Ordering::Acquire) {
            TerminalCompletion::Uncertain
        } else {
            completed
        };
        let removed_anchor = if completion == TerminalCompletion::PhysicallyStopped {
            // Control -> registry is the common order with timeout retention.
            // Keep the removed strong reference until after both locks are free.
            self.take_anchor()
        } else {
            None
        };
        control.finish_state = FinishState::Complete;
        control.completion = Some(completion);
        drop(control);
        drop(removed_anchor);
        self.changed.notify_all();
    }
    fn settle_execution(self: &Arc<Self>) -> bool {
        let (initial, handle) = {
            let mut resources = self
                .resources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                resources.initial.take(),
                resources
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.handle().clone()),
            )
        };
        if let Some(initial) = initial {
            initial.abort();
            let Some(handle) = handle else {
                self.resources
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .initial = Some(initial);
                return false;
            };
            match handle.block_on(initial) {
                Ok(Ok(())) => {}
                Ok(Err(_error)) => self.signal(TerminalReason::SetupFailed),
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    if error.is_panic() {
                        self.source_panic(PayloadKind::Initial, error.into_panic());
                    } else {
                        self.signal(TerminalReason::SetupFailed);
                    }
                }
            }
        }
        true
    }
    fn dispose_setup(self: &Arc<Self>) {
        // The worker already observed active_reinit=false. Every last capture
        // is taken mechanically and destroyed after all bookkeeping unlocks.
        let (reinitialize, future, reinit_future) = {
            let resources = self
                .resources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                resources.reinitialize.clone(),
                resources.initial_future.clone(),
                resources.reinit_future.clone(),
            )
        };
        if let Some(owner) = &self.owner {
            owner.stop();
        }
        let future = future
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(future);
        let callback = reinitialize
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(callback);
        let request = self.lock().request.take();
        drop(request);
        let future = reinit_future
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(future);
    }
    fn release_resources(&self) -> bool {
        // Take one owner at a time. If its foreign Drop panics, the enclosing
        // single cleanup catch stops here and every later owner stays anchored.
        let setup = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .setup
            .take();
        drop(setup);
        let factory = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retained_factory
            .take();
        drop(factory);
        let config = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config
            .take();
        drop(config);
        let reinit_factory = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reinitializer_factory
            .take();
        drop(reinit_factory);
        let connection = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .connection
            .clone();
        let connection = connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(connection);
        let inbound = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .inbound
            .take();
        drop(inbound);
        let transport = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .transport
            .take();
        drop(transport);
        let runtime = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runtime
            .take();
        if let Some(runtime) = runtime {
            runtime.shutdown_timeout(Duration::from_secs(4));
        }
        // Timeout is not proof: every surviving SDK transport/Connection/method keeps State
        // and its resource count. Unknown surviving ownership stays anchored.
        self.sdk_owners.load(Ordering::Acquire) == 0
    }
    pub fn has_fault(&self) -> bool {
        self.reason.load(Ordering::Acquire) >= 3
    }
    pub fn mark_uncertain(self: &Arc<Self>) {
        self.retain_uncertain_anchor();
    }
    pub fn wait(self: &Arc<Self>, deadline: Duration) -> TerminalCompletion {
        let control = self.lock();
        let (control, _) = self
            .changed
            .wait_timeout_while(control, deadline, |control| control.completion.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        control.completion.unwrap_or(TerminalCompletion::Uncertain)
    }
}
#[cfg(test)]
#[path = "terminal_tests.rs"]
mod tests;
