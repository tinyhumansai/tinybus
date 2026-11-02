//! Host-side transport bridge over the module C vtables.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::broker::RestrictedModuleControllerBinding;
use crate::error::{Error, Result};
use crate::message::codec::MAX_FRAME_LEN;
use crate::message::{Message, MessageKind};
use crate::module::abi::{
    TB_BACKPRESSURE, TB_BAD_ARGUMENT, TB_BUFFER_TOO_SMALL, TB_CLOSED, TB_MODULE_VTABLE_BASE_SIZE,
    TB_NATIVE_TERMINAL_LIVE, TB_OK, TB_PANICKED, TB_TIMEOUT, TB_UNSUPPORTED, TbHostVtable,
    TbModuleVtable, TbSlice,
};
use crate::module::host::PreReadyConfidentialRoute;
use crate::ports::Transport;

const HOST_QUEUE_CAPACITY: usize = 256;
const MODULE_INIT_DEADLINE: Duration = Duration::from_secs(5);

/// How long the host waits for a module to drain its queue before declaring it
/// faulted. Bounded so a module that stops calling `wake` cannot park the
/// delivery task for the process lifetime; the security boundary applies to
/// in-process modules too ("every call has a deadline").
const BACKPRESSURE_DEADLINE: Duration = Duration::from_secs(5);

struct HostContext {
    inbound: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    wake: Arc<Notify>,
    config: StdMutex<Vec<u8>>,
    owner: StdMutex<Weak<ModuleTransport>>,
    native_terminal_state: AtomicU32,
    physical: StdMutex<PhysicalState>,
    physical_changed: Condvar,
    physical_notify: Notify,
    faulted: AtomicBool,
    init_failed: AtomicBool,
    stopped: AtomicBool,
    admission_closed: AtomicBool,
    init_started: AtomicBool,
    init_notify: Notify,
    ready: AtomicBool,
    ready_gate: StdMutex<()>,
    broker_routing: AtomicBool,
    trusted_host_binding: Option<RestrictedModuleControllerBinding>,
    pre_ready_confidential_routes: Arc<[PreReadyConfidentialRoute]>,
    ready_notify: Notify,
    inflight: AtomicUsize,
}

/// Admission errors remain faults; cancellation preserves an already committed
/// ordinary stop. Neither classification can clear a real, sticky Fault.
#[derive(Clone, Copy)]
pub(crate) enum AdmissionTermination {
    Failed,
    Canceled,
}

/// The broker-facing side of one loaded module.
pub(crate) struct ModuleTransport {
    self_ref: Weak<ModuleTransport>,
    module: StdMutex<Option<TbModuleVtable>>,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
    context: &'static HostContext,
    label: String,
    initializer: StdMutex<Option<DeferredInitializer>>,
    pending: StdMutex<VecDeque<Message>>,
    drain_started: AtomicBool,
    // Reconfiguration and shutdown both call into opaque module code. The
    // owned guard moves into the blocking task, so a host-side timeout cannot
    // let a second lifecycle callback overlap the still-running first one.
    lifecycle: Arc<Mutex<()>>,
    native_lifecycle_pending: Arc<AtomicBool>,
    stop_task_spawned: Arc<AtomicBool>,
    callbacks: CallbackGate,
    // One exact provider account. Its transport backlink is weak.
    host_callbacks: OnceLock<Arc<crate::module::host::callbacks::CallbackState>>,
    initialization_gate: StdMutex<Option<bool>>,
    initialization_gate_changed: Notify,
    #[cfg(test)]
    pub(crate) reject_worker_spawn: AtomicBool,
    #[cfg(test)]
    before_worker_lock: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    before_init_publication: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    after_cleanup_publication: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
    // Native fixtures pass a borrowed controller through C config. Retain its
    // actual owner with the physical transport, including uncertain jobs.
    #[cfg(test)]
    pub(crate) fixture_controller: StdMutex<Option<Arc<dyn Send + Sync>>>,
}

// Reservations cover physical callback execution, including signals. They are
// independent of unanswered method calls and remain owned by the actual caller.
#[derive(Default)]
struct CallbackState {
    closed: bool,
    active: usize,
}

#[derive(Default)]
struct CallbackGate {
    state: StdMutex<CallbackState>,
    drained: Condvar,
}

struct CallbackLease<'a>(&'a CallbackGate);

impl CallbackGate {
    /// Reserve entry atomically with physical stop's admission closure.
    fn enter(&self) -> Result<CallbackLease<'_>> {
        let mut state = self.state.lock().expect("module callback lock");
        if state.closed {
            return Err(Error::ConnectionClosed);
        }
        state.active += 1;
        Ok(CallbackLease(self))
    }

    /// Retain the shutdown worker until every reserved callback has returned.
    fn wait_drained(&self) {
        let mut state = self.state.lock().expect("module callback lock");
        while state.active != 0 {
            state = self.drained.wait(state).expect("module callback lock");
        }
    }
}

impl Drop for CallbackLease<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("module callback lock");
        state.active -= 1;
        if state.active == 0 {
            self.0.drained.notify_all();
        }
    }
}

// `module_ctx` is opaque and all access to it goes through callbacks whose ABI
// contract requires thread safety. The vtable itself is immutable after init.
unsafe impl Send for ModuleTransport {}
unsafe impl Sync for ModuleTransport {}

type Initializer =
    Box<dyn FnOnce(TbHostVtable) -> std::result::Result<TbModuleVtable, String> + Send + 'static>;

struct DeferredInitializer {
    initialize: Initializer,
    host: TbHostVtable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InitState {
    Deferred,
    Scheduled,
    Entered,
    PublishedValid,
    PublishedRejected,
    CanceledBeforeEntry,
    ExternalValid,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupState {
    Idle,
    Requested,
    Executing,
    Complete,
    Uncertain,
    NoCallbackRequired,
}

struct PhysicalState {
    init: InitState,
    cleanup: CleanupState,
    job: Option<Arc<PhysicalJob>>,
    result: Option<std::result::Result<(), InitFailure>>,
    code: Option<i32>,
}

struct PhysicalJob {
    transport: Arc<ModuleTransport>,
    initializer: StdMutex<Option<DeferredInitializer>>,
    guard: StdMutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
    deadline: Duration,
    fault_on_spawn_failure: bool,
    submitted: AtomicBool,
    started: AtomicBool,
}

struct InitiatingWaiter<'a> {
    transport: &'a ModuleTransport,
    armed: bool,
}
impl Drop for InitiatingWaiter<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.transport.abandon_initialization();
        }
    }
}

struct SendModuleVtable(TbModuleVtable);

#[derive(Clone)]
enum InitFailure {
    Closed,
    Failed(String),
}

// Native callbacks outlive canceled public waiters. Clear this marker before
// releasing the mutex so an older callback cannot clear its successor's flag.
struct NativeLifecyclePending(Arc<AtomicBool>);
impl NativeLifecyclePending {
    fn begin(pending: &Arc<AtomicBool>) -> Self {
        pending.store(true, Ordering::Release);
        Self(pending.clone())
    }
}
impl Drop for NativeLifecyclePending {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// The opaque host context is process-lifetime state and every callback is
// required by the ABI to be thread-safe.
unsafe impl Send for DeferredInitializer {}
unsafe impl Send for SendModuleVtable {}

impl SendModuleVtable {
    unsafe fn reinitialize(
        &self,
        callback: crate::module::abi::TbModuleReinitialize,
        bytes: &[u8],
    ) -> i32 {
        unsafe { callback(self.0.module_ctx, bytes.as_ptr(), bytes.len()) }
    }
}

impl DeferredInitializer {
    fn run(self) -> std::result::Result<SendModuleVtable, String> {
        (self.initialize)(self.host).map(SendModuleVtable)
    }
}

impl ModuleTransport {
    pub(crate) fn admit_broker(&self) {
        self.context.broker_routing.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn new(label: String, config: Vec<u8>) -> (Arc<Self>, TbHostVtable) {
        Self::new_with_controller_binding(label, config, None, Arc::from([]))
    }

    pub(crate) fn new_with_controller_binding(
        label: String,
        config: Vec<u8>,
        trusted_host_binding: Option<RestrictedModuleControllerBinding>,
        pre_ready_confidential_routes: Arc<[PreReadyConfidentialRoute]>,
    ) -> (Arc<Self>, TbHostVtable) {
        let (inbound_tx, inbound_rx) = mpsc::channel(HOST_QUEUE_CAPACITY);
        let context = Box::leak(Box::new(HostContext {
            inbound: StdMutex::new(Some(inbound_tx)),
            wake: Arc::new(Notify::new()),
            config: StdMutex::new(config),
            owner: StdMutex::new(Weak::new()),
            native_terminal_state: AtomicU32::new(crate::module::abi::TB_NATIVE_TERMINAL_LIVE),
            physical: StdMutex::new(PhysicalState {
                init: InitState::Deferred,
                cleanup: CleanupState::Idle,
                job: None,
                result: None,
                code: None,
            }),
            physical_changed: Condvar::new(),
            physical_notify: Notify::new(),
            faulted: AtomicBool::new(false),
            init_failed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            admission_closed: AtomicBool::new(false),
            init_started: AtomicBool::new(false),
            init_notify: Notify::new(),
            ready: AtomicBool::new(false),
            ready_gate: StdMutex::new(()),
            broker_routing: AtomicBool::new(false),
            trusted_host_binding,
            pre_ready_confidential_routes,
            ready_notify: Notify::new(),
            inflight: AtomicUsize::new(0),
        }));
        let transport = Arc::new_cyclic(|self_ref| Self {
            self_ref: self_ref.clone(),
            module: StdMutex::new(None),
            inbound: Mutex::new(inbound_rx),
            context,
            label,
            initializer: StdMutex::new(None),
            pending: StdMutex::new(VecDeque::new()),
            drain_started: AtomicBool::new(false),
            lifecycle: Arc::new(Mutex::new(())),
            native_lifecycle_pending: Arc::new(AtomicBool::new(false)),
            stop_task_spawned: Arc::new(AtomicBool::new(false)),
            callbacks: CallbackGate::default(),
            host_callbacks: OnceLock::new(),
            initialization_gate: StdMutex::new(Some(true)),
            initialization_gate_changed: Notify::new(),
            #[cfg(test)]
            reject_worker_spawn: AtomicBool::new(false),
            #[cfg(test)]
            before_worker_lock: StdMutex::new(None),
            #[cfg(test)]
            before_init_publication: StdMutex::new(None),
            #[cfg(test)]
            after_cleanup_publication: StdMutex::new(None),
            #[cfg(test)]
            fixture_controller: StdMutex::new(None),
        });
        *context.owner.lock().expect("physical owner lock") = Arc::downgrade(&transport);
        let config = context.config.lock().expect("module config lock");
        let config_slice = crate::module::abi::TbSlice {
            ptr: config.as_ptr(),
            len: config.len(),
        };
        drop(config);
        let vtable = TbHostVtable {
            size: size_of::<TbHostVtable>() as u32,
            _reserved: 0,
            host_ctx: std::ptr::from_ref(context).cast_mut().cast(),
            send: host_send,
            wake: host_wake,
            log: host_log,
            fault: host_fault,
            config: config_slice,
            ready: host_ready,
            broker_routing: Some(host_broker_routing),
            native_terminal_state: Some(host_native_terminal_state),
            trusted_host_binding: context.trusted_host_binding.as_ref().map(|_| {
                host_trusted_host_binding
                    as unsafe extern "C" fn(*mut c_void, *mut u8, usize, *mut usize) -> i32
            }),
        };
        (transport, vtable)
    }

    pub(crate) fn initialize(&self, module: TbModuleVtable) -> Result<()> {
        if module.size < TB_MODULE_VTABLE_BASE_SIZE
            || module.module_ctx.is_null()
            || (module.size >= size_of::<TbModuleVtable>() as u32 && module.reinitialize.is_none())
        {
            return Err(Error::transport("module returned an incomplete vtable"));
        }
        *self.module.lock().expect("module vtable lock") = Some(module);
        let mut physical = self.context.physical.lock().expect("physical state lock");
        if physical.init == InitState::Deferred {
            physical.init = InitState::ExternalValid;
            physical.result = Some(Ok(()));
        }
        Ok(())
    }

    pub(crate) fn defer_initialize(
        &self,
        init: crate::module::abi::TbModuleInit,
        host: TbHostVtable,
    ) {
        self.defer_initializer(host, move |host| {
            let mut module = TbModuleVtable::default();
            let code = unsafe { init(&host, &mut module) };
            if code == TB_OK {
                Ok(module)
            } else {
                Err("module initialization failed".to_string())
            }
        });
    }

    pub(crate) fn defer_initializer<F>(&self, host: TbHostVtable, initialize: F)
    where
        F: FnOnce(TbHostVtable) -> std::result::Result<TbModuleVtable, String> + Send + 'static,
    {
        *self.initializer.lock().expect("module initializer lock") = Some(DeferredInitializer {
            initialize: Box::new(initialize),
            host,
        });
    }

    pub(crate) fn pause_initialization(&self) {
        *self
            .initialization_gate
            .lock()
            .expect("initialization gate lock") = None;
    }

    pub(crate) fn release_initialization(&self, allowed: bool) {
        *self
            .initialization_gate
            .lock()
            .expect("initialization gate lock") = Some(allowed);
        self.initialization_gate_changed.notify_waiters();
        if !allowed {
            self.terminate_waiters();
        }
    }

    // Releasing rejected observers is cancellation, which may follow an actual
    // ordinary stop. Never fabricate READY or init_started to release them.
    fn terminate_waiters(&self) {
        let fault = self.commit_admission_termination(AdmissionTermination::Canceled);
        self.request_terminal(fault);
        self.context.init_notify.notify_waiters();
        self.context.ready_notify.notify_waiters();
        self.initialization_gate_changed.notify_waiters();
    }

    /// Close admission before teardown, without reporting physical completion
    /// or inventing a fault. The lifecycle owner alone invokes this operation.
    fn close_admission(&self) {
        let discarded = {
            let _initialization = self
                .initialization_gate
                .lock()
                .expect("initialization gate lock");
            let mut pending = self.pending.lock().expect("module pending lock");
            let mut callbacks = self.callbacks.state.lock().expect("module callback lock");
            callbacks.closed = true;
            let _ready = self.context.ready_gate.lock().expect("module ready lock");
            self.context.admission_closed.store(true, Ordering::Release);
            std::mem::take(&mut *pending)
        };
        // Queued confidential bodies are released outside internal locks.
        drop(discarded);
        self.context.init_notify.notify_waiters();
        self.context.ready_notify.notify_waiters();
        self.context.wake.notify_waiters();
        self.initialization_gate_changed.notify_waiters();
    }

    /// Publish physical completion only after the opaque shutdown returns.
    fn finish_stop(&self) {
        self.context.stopped.store(true, Ordering::Release);
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
        self.context.init_notify.notify_waiters();
        self.context.ready_notify.notify_waiters();
        self.context.wake.notify_waiters();
        self.initialization_gate_changed.notify_waiters();
    }

    pub(crate) fn admission_closed(&self) -> bool {
        self.context.admission_closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.context.stopped.load(Ordering::Acquire)
    }

    pub(crate) async fn ensure_initialized(&self) -> Result<()> {
        loop {
            let changed = self.initialization_gate_changed.notified();
            let terminal = self.context.init_notify.notified();
            let allowed = *self
                .initialization_gate
                .lock()
                .expect("initialization gate lock");
            if self.is_faulted() || self.admission_closed() {
                return Err(Error::ConnectionClosed);
            }
            match allowed {
                Some(true) => break,
                Some(false) => return Err(Error::transport("module callback admission failed")),
                None => tokio::select! {
                    _ = changed => {},
                    _ = terminal => {},
                },
            }
        }

        let initiating = self.schedule_initializer()?;
        let mut abandonment = InitiatingWaiter {
            transport: self,
            armed: initiating,
        };
        let wait = async {
            loop {
                let changed = self.context.physical_notify.notified();
                let result = self
                    .context
                    .physical
                    .lock()
                    .expect("physical state lock")
                    .result
                    .clone();
                if let Some(result) = result {
                    return result;
                }
                changed.await;
            }
        };
        let result = match tokio::time::timeout(MODULE_INIT_DEADLINE, wait).await {
            Ok(result) => {
                abandonment.armed = false;
                result
            }
            Err(_) => {
                if initiating {
                    self.abandon_initialization();
                }
                abandonment.armed = false;
                return Err(Error::transport(
                    "module initialization exceeded its deadline",
                ));
            }
        };
        match result {
            Ok(()) if self.admission_closed() || self.is_faulted() => Err(Error::ConnectionClosed),
            Ok(()) => Ok(()),
            Err(InitFailure::Closed) => Err(Error::ConnectionClosed),
            Err(InitFailure::Failed(reason)) => Err(Error::transport(reason)),
        }
    }

    fn notify_physical(&self) {
        self.context.physical_changed.notify_all();
        self.context.physical_notify.notify_waiters();
        self.context.init_notify.notify_waiters();
    }

    // Scheduling is synchronous: the externally anchored owner precedes submission.
    fn schedule_initializer(&self) -> Result<bool> {
        let gate = self
            .initialization_gate
            .lock()
            .expect("initialization gate lock");
        if *gate != Some(true) || self.is_faulted() || self.admission_closed() {
            return Err(Error::ConnectionClosed);
        }
        let mut state = self.context.physical.lock().expect("physical state lock");
        if state.result.is_some() || state.init != InitState::Deferred {
            return Ok(false);
        }
        let Some(initializer) = self
            .initializer
            .lock()
            .expect("module initializer lock")
            .take()
        else {
            state.result = Some(Err(InitFailure::Failed("module has no initializer".into())));
            return Ok(false);
        };
        let job = Arc::new(PhysicalJob {
            transport: self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?,
            initializer: StdMutex::new(Some(initializer)),
            guard: StdMutex::new(None),
            deadline: MODULE_INIT_DEADLINE,
            fault_on_spawn_failure: true,
            submitted: AtomicBool::new(false),
            started: AtomicBool::new(false),
        });
        state.init = InitState::Scheduled;
        state.job = Some(job.clone());
        self.context.init_started.store(true, Ordering::Release);
        drop(state);
        drop(gate);
        self.notify_physical();
        Self::submit_job(job);
        Ok(true)
    }

    fn submit_job(job: Arc<PhysicalJob>) {
        // Submission is spent even if the builder drops its closure. The
        // external anchor remains; no caller can silently resubmit this job.
        if job.submitted.swap(true, Ordering::AcqRel) {
            return;
        }
        let submitted = job.clone();
        #[cfg(test)]
        let rejected = job.transport.reject_worker_spawn.load(Ordering::Acquire);
        #[cfg(not(test))]
        let rejected = false;
        let worker = move || submitted.run();
        let spawn = if rejected {
            // Model Builder rejecting and dropping the submitted closure: only
            // the independent context anchor can preserve its physical owner.
            drop(worker);
            Err(std::io::Error::other("controlled worker rejection"))
        } else {
            std::thread::Builder::new()
                .name("tinybus-physical".into())
                .spawn(worker)
        };
        if spawn.is_err() {
            let transport = &job.transport;
            transport.commit_terminal(job.fault_on_spawn_failure);
            let mut state = transport
                .context
                .physical
                .lock()
                .expect("physical state lock");
            state.cleanup = CleanupState::Uncertain;
            state.result.get_or_insert(Err(InitFailure::Failed(
                "module physical worker could not start".into(),
            )));
            drop(state);
            transport.notify_physical();
        }
    }

    fn publish_terminal_flags(&self, fault: bool) {
        use crate::module::abi::{TB_NATIVE_TERMINAL_FAULT, TB_NATIVE_TERMINAL_STOP_INTENT};
        if fault {
            self.context.faulted.store(true, Ordering::Release);
        }
        let flags = &self.context.native_terminal_state;
        let mut old = flags.load(Ordering::Acquire);
        loop {
            let new = (old & !crate::module::abi::TB_NATIVE_TERMINAL_LIVE)
                | TB_NATIVE_TERMINAL_STOP_INTENT
                | if fault { TB_NATIVE_TERMINAL_FAULT } else { 0 };
            match flags.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(current) => old = current,
            }
        }
    }

    fn abandon_initialization(&self) {
        {
            let mut state = self.context.physical.lock().expect("physical state lock");
            // Linearize publication versus the initiating waiter's abandonment.
            // Atomic-only cause publication does not invert the host lock order.
            if state.result.is_some()
                || self.context.native_terminal_state.load(Ordering::Acquire)
                    != crate::module::abi::TB_NATIVE_TERMINAL_LIVE
            {
                return;
            }
            self.publish_terminal_flags(true);
            if state.cleanup == CleanupState::Idle {
                state.cleanup = CleanupState::Requested;
            }
        }
        self.request_terminal(true);
    }

    pub(crate) fn commit_admission_termination(&self, cause: AdmissionTermination) -> bool {
        let fault = match cause {
            AdmissionTermination::Failed => true,
            AdmissionTermination::Canceled => {
                let flags = self.context.native_terminal_state.load(Ordering::Acquire);
                flags & crate::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT == 0
                    || flags & crate::module::abi::TB_NATIVE_TERMINAL_FAULT != 0
            }
        };
        self.commit_terminal(fault);
        fault
    }

    pub(crate) fn commit_terminal(&self, fault: bool) {
        self.publish_terminal_flags(fault);
        self.close_admission();
    }

    pub(crate) fn request_terminal(&self, fault: bool) {
        self.commit_terminal(fault);
        let mut state = self.context.physical.lock().expect("physical state lock");
        if state.cleanup == CleanupState::Idle {
            state.cleanup = CleanupState::Requested;
        }
        if state.job.is_some() || state.cleanup != CleanupState::Requested {
            return;
        }
        if state.init == InitState::Deferred {
            state.init = InitState::CanceledBeforeEntry;
            state.cleanup = CleanupState::NoCallbackRequired;
            state.result = Some(Err(InitFailure::Closed));
            drop(state);
            self.initializer
                .lock()
                .expect("module initializer lock")
                .take();
            self.clear_config();
            self.finish_stop();
            self.notify_physical();
            return;
        }
        let Some(transport) = self.self_ref.upgrade() else {
            return;
        };
        let job = Arc::new(PhysicalJob {
            transport,
            initializer: StdMutex::new(None),
            guard: StdMutex::new(None),
            deadline: MODULE_INIT_DEADLINE,
            fault_on_spawn_failure: fault,
            submitted: AtomicBool::new(false),
            started: AtomicBool::new(false),
        });
        state.job = Some(job.clone());
        drop(state);
        Self::submit_job(job);
    }

    pub(crate) fn eager_initialize(
        &self,
        init: crate::module::abi::TbModuleInit,
        host: TbHostVtable,
    ) -> Result<()> {
        self.eager_initialize_with(host, move |host| {
            let mut module = TbModuleVtable::default();
            let code = unsafe { init(&host, &mut module) };
            if code == TB_OK {
                Ok(module)
            } else {
                Err("module initialization failed".to_string())
            }
        })
    }

    pub(crate) fn eager_initialize_with<F>(&self, host: TbHostVtable, initialize: F) -> Result<()>
    where
        F: FnOnce(TbHostVtable) -> std::result::Result<TbModuleVtable, String>,
    {
        let guard = self.lifecycle.clone().try_lock_owned().map_err(|_| {
            self.commit_terminal(true);
            self.context
                .physical
                .lock()
                .expect("physical state lock")
                .cleanup = CleanupState::Uncertain;
            Error::transport("module initialization lifecycle unavailable")
        })?;
        self.context
            .physical
            .lock()
            .expect("physical state lock")
            .init = InitState::Entered;
        self.context.init_started.store(true, Ordering::Release);
        let result = initialize(host)
            .map_err(Error::transport)
            .and_then(|module| self.initialize(module));
        self.clear_config();
        {
            let mut state = self.context.physical.lock().expect("physical state lock");
            state.init = if result.is_ok() {
                InitState::ExternalValid
            } else {
                InitState::PublishedRejected
            };
            state.result.get_or_insert(if result.is_ok() {
                Ok(())
            } else {
                Err(InitFailure::Failed(
                    "module returned an invalid vtable".into(),
                ))
            });
            if result.is_err() {
                state.cleanup = CleanupState::Uncertain;
                if state.job.is_none() {
                    state.job = self.self_ref.upgrade().map(|transport| {
                        Arc::new(PhysicalJob {
                            transport,
                            initializer: StdMutex::new(None),
                            guard: StdMutex::new(None),
                            deadline: MODULE_INIT_DEADLINE,
                            fault_on_spawn_failure: true,
                            submitted: AtomicBool::new(false),
                            started: AtomicBool::new(false),
                        })
                    });
                }
            }
        }
        self.notify_physical();
        drop(guard);
        if result.is_err() {
            self.request_terminal(true);
            return result;
        }
        if self.is_faulted() || self.admission_closed() {
            return Err(Error::ConnectionClosed);
        }
        result
    }

    pub(crate) async fn wait_ready(&self) {
        loop {
            let notified = self.context.ready_notify.notified();
            if self.context.ready.load(Ordering::Acquire)
                || self.context.faulted.load(Ordering::Acquire)
                || self.admission_closed()
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn bind_host_callbacks(
        &self,
        callbacks: Arc<crate::module::host::callbacks::CallbackState>,
    ) -> Result<()> {
        self.host_callbacks
            .set(callbacks)
            .map_err(|_| Error::failed("module callback binding is already installed"))
    }

    pub(crate) fn native_lifecycle_pending(&self) -> bool {
        self.native_lifecycle_pending.load(Ordering::Acquire)
            || (self.admission_closed() && !self.is_stopped())
            || self
                .context
                .physical
                .lock()
                .expect("physical state lock")
                .job
                .is_some()
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.context.ready.load(Ordering::Acquire) && !self.is_faulted() && !self.is_stopped()
    }

    fn authorized_pre_ready_peer(&self, message: &Message) -> Option<crate::BusName> {
        if message.header.kind != MessageKind::MethodCall || !message.header.confidential {
            return None;
        }

        // Serialize the authorization point with READY publication. A call
        // that passes this gate is admitted while initialization is active;
        // READY and terminal transitions prevent later calls from using it.
        let _ready = self.context.ready_gate.lock().expect("module ready lock");
        if self.context.ready.load(Ordering::Acquire)
            || !self.context.init_started.load(Ordering::Acquire)
            || self.admission_closed()
            || self.is_faulted()
            || self.context.init_failed.load(Ordering::Acquire)
            || self.is_stopped()
            || !self.context.broker_routing.load(Ordering::Acquire)
            || self.context.native_terminal_state.load(Ordering::Acquire)
                != crate::module::abi::TB_NATIVE_TERMINAL_LIVE
        {
            return None;
        }

        let binding = self.context.trusted_host_binding.as_ref()?;
        if message.header.sender.as_ref() != Some(binding.peer()) || !binding.is_live() {
            return None;
        }

        self.context
            .pre_ready_confidential_routes
            .iter()
            .any(|route| {
                message.header.path.as_ref() == Some(&route.path)
                    && message.header.interface.as_ref() == Some(&route.interface)
                    && message.header.member.as_ref() == Some(&route.member)
            })
            .then(|| binding.peer().clone())
    }

    /// Whether a lazy initializer is still available for a pre-init config update.
    pub(crate) fn has_deferred_initializer(&self) -> bool {
        let state = self.context.physical.lock().expect("physical state lock");
        !self.admission_closed()
            && !self.is_faulted()
            && state.init == InitState::Deferred
            && state.cleanup == CleanupState::Idle
            && self
                .initializer
                .lock()
                .expect("module initializer lock")
                .is_some()
    }

    /// Replace the configuration a deferred initializer will read, if it has
    /// not been scheduled. The physical-state lock is held through pointer
    /// replacement so `schedule_initializer` cannot take the old host vtable.
    fn replace_deferred_config(&self, bytes: &crate::Secret) -> Result<bool> {
        let state = self.context.physical.lock().expect("physical state lock");
        if self.admission_closed() || self.is_faulted() {
            return Err(Error::ConnectionClosed);
        }
        if state.init != InitState::Deferred || state.cleanup != CleanupState::Idle {
            return Ok(false);
        }
        let mut initializer = self.initializer.lock().expect("module initializer lock");
        let Some(initializer) = initializer.as_mut() else {
            return Ok(false);
        };
        let replacement = bytes.expose_secret().to_vec();
        let mut config = self.context.config.lock().expect("module config lock");
        config.fill(0);
        *config = replacement;
        initializer.host.config = TbSlice {
            ptr: config.as_ptr(),
            len: config.len(),
        };
        if self.admission_closed() || self.is_faulted() {
            return Err(Error::ConnectionClosed);
        }
        Ok(true)
    }

    pub(crate) fn clear_config(&self) {
        let mut config = self.context.config.lock().expect("module config lock");
        config.fill(0);
        config.clear();
        config.shrink_to_fit();
    }

    pub(crate) async fn reinitialize(self: &Arc<Self>, bytes: crate::Secret) -> Result<()> {
        if self.admission_closed() {
            return Err(Error::ConnectionClosed);
        }
        let started = std::time::Instant::now();
        let mut lifecycle =
            tokio::time::timeout(MODULE_INIT_DEADLINE, self.lifecycle.clone().lock_owned())
                .await
                .map_err(|_| Error::Timeout {
                    member: crate::MemberName::new("ReinitializeModule")
                        .expect("static member name is valid"),
                    timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
                })?;
        if self.admission_closed() || self.is_faulted() {
            return Err(Error::ConnectionClosed);
        }
        if self.replace_deferred_config(&bytes)? {
            return Ok(());
        }
        if self.module.lock().expect("module vtable lock").is_none() {
            let initialization_in_progress = matches!(
                self.context
                    .physical
                    .lock()
                    .expect("physical state lock")
                    .init,
                InitState::Scheduled | InitState::Entered
            );
            if !initialization_in_progress {
                return Err(Error::transport("module is not initialized"));
            }
            // An ordinary bus call may have scheduled lazy initialization
            // before this reconfiguration acquired the lifecycle lock. Let the
            // physical initializer run without holding either lifecycle or
            // physical-state locks, then use only the original deadline for
            // reacquisition and the native callback.
            drop(lifecycle);
            let remaining = MODULE_INIT_DEADLINE.saturating_sub(started.elapsed());
            tokio::time::timeout(remaining, self.ensure_initialized())
                .await
                .map_err(|_| Error::Timeout {
                    member: crate::MemberName::new("ReinitializeModule")
                        .expect("static member name is valid"),
                    timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
                })??;
            let remaining = MODULE_INIT_DEADLINE.saturating_sub(started.elapsed());
            lifecycle = tokio::time::timeout(remaining, self.lifecycle.clone().lock_owned())
                .await
                .map_err(|_| Error::Timeout {
                    member: crate::MemberName::new("ReinitializeModule")
                        .expect("static member name is valid"),
                    timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
                })?;
            if self.admission_closed() || self.is_faulted() {
                return Err(Error::ConnectionClosed);
            }
        }
        if !self.is_ready() {
            // The C initializer only publishes its vtable. SDK setup is
            // asynchronous and installs the reinitialization connection slot
            // immediately before host.ready, so invoking the callback earlier
            // can reject a valid replacement as closed. Readiness may wait on
            // module code; never retain the lifecycle lock across that wait.
            drop(lifecycle);
            let remaining = MODULE_INIT_DEADLINE.saturating_sub(started.elapsed());
            tokio::time::timeout(remaining, self.wait_ready())
                .await
                .map_err(|_| Error::Timeout {
                    member: crate::MemberName::new("ReinitializeModule")
                        .expect("static member name is valid"),
                    timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
                })?;
            if !self.is_ready() {
                return Err(Error::ConnectionClosed);
            }
            let remaining = MODULE_INIT_DEADLINE.saturating_sub(started.elapsed());
            lifecycle = tokio::time::timeout(remaining, self.lifecycle.clone().lock_owned())
                .await
                .map_err(|_| Error::Timeout {
                    member: crate::MemberName::new("ReinitializeModule")
                        .expect("static member name is valid"),
                    timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
                })?;
            if self.admission_closed() || self.is_faulted() || !self.is_ready() {
                return Err(Error::ConnectionClosed);
            }
        }
        let module = self
            .module
            .lock()
            .expect("module vtable lock")
            .ok_or_else(|| Error::transport("module is not initialized"))?;
        if module.size < size_of::<TbModuleVtable>() as u32 {
            return Err(Error::failed("module does not support reinitialization"));
        }
        let callback = module
            .reinitialize
            .ok_or_else(|| Error::failed("module does not support reinitialization"))?;
        let module = SendModuleVtable(module);
        let remaining = MODULE_INIT_DEADLINE.saturating_sub(started.elapsed());
        let pending = NativeLifecyclePending::begin(&self.native_lifecycle_pending);
        let task = tokio::task::spawn_blocking(move || {
            let _lifecycle = lifecycle;
            let _pending = pending;
            unsafe { module.reinitialize(callback, bytes.expose_secret()) }
        });
        let code = tokio::time::timeout(remaining, task)
            .await
            .map_err(|_| Error::Timeout {
                member: crate::MemberName::new("ReinitializeModule")
                    .expect("static member name is valid"),
                timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
            })?
            .map_err(|_| Error::transport("module reinitialization task was cancelled"))?;
        match code {
            TB_OK => Ok(()),
            TB_BAD_ARGUMENT => Err(Error::failed("module configuration is invalid")),
            TB_CLOSED => Err(Error::failed("module reinitialization failed")),
            TB_PANICKED => Err(Error::failed("module reinitialization panicked")),
            TB_TIMEOUT => Err(Error::Timeout {
                member: crate::MemberName::new("ReinitializeModule")
                    .expect("static member name is valid"),
                timeout_ms: MODULE_INIT_DEADLINE.as_millis() as u64,
            }),
            _ => Err(Error::failed("module reinitialization failed")),
        }
    }

    pub(crate) fn is_faulted(&self) -> bool {
        self.context.faulted.load(Ordering::Acquire)
    }

    pub(crate) fn init_failed(&self) -> bool {
        self.context.init_failed.load(Ordering::Acquire)
    }

    pub(crate) fn init_started(&self) -> bool {
        self.context.init_started.load(Ordering::Acquire)
    }

    pub(crate) fn inflight(&self) -> usize {
        self.context.inflight.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_initializing(&self) {
        loop {
            let notified = self.context.init_notify.notified();
            if self.init_started() || self.is_faulted() || self.admission_closed() {
                return;
            }
            notified.await;
        }
    }

    async fn deliver_now(&self, message: Message) -> Result<()> {
        self.deliver_now_with_pre_ready_peer(message, None).await
    }

    async fn deliver_now_with_pre_ready_peer(
        &self,
        message: Message,
        required_controller: Option<&crate::BusName>,
    ) -> Result<()> {
        if self.is_faulted() || self.admission_closed() {
            return Err(Error::ConnectionClosed);
        }
        let bytes = serde_json::to_vec(&message)?;
        if bytes.len() > MAX_FRAME_LEN {
            return Err(Error::protocol("module frame exceeds the size cap"));
        }

        loop {
            let notified = self.context.wake.notified();
            if self.is_faulted() || self.admission_closed() {
                return Err(Error::ConnectionClosed);
            }
            let module = *self.module.lock().expect("module vtable lock");
            let Some(module) = module else {
                return Err(Error::ConnectionClosed);
            };
            let lease = self.callbacks.enter()?;
            if required_controller.is_some_and(|required| {
                self.context
                    .trusted_host_binding
                    .as_ref()
                    .is_none_or(|binding| binding.peer() != required || !binding.is_live())
            }) {
                drop(lease);
                return Err(Error::ConnectionClosed);
            }
            // Stop closes this gate and drains reservations before teardown;
            // a caller disappearing cannot refund a running native callback.
            let code = unsafe { (module.deliver)(module.module_ctx, bytes.as_ptr(), bytes.len()) };
            drop(lease);
            match code {
                TB_OK => return Ok(()),
                TB_BACKPRESSURE => {
                    if self.admission_closed() {
                        return Err(Error::ConnectionClosed);
                    }
                    // A module that stops draining its queue and never calls
                    // `wake` must not park this delivery task — or the caller
                    // behind it — for the process lifetime. Expiry faults the
                    // module downstream so later callers get `ModuleUnavailable`
                    // instead of waiting out their own deadline.
                    if tokio::time::timeout(BACKPRESSURE_DEADLINE, notified)
                        .await
                        .is_err()
                    {
                        let _pending = self.pending.lock().expect("module pending lock");
                        if self.admission_closed() {
                            return Err(Error::ConnectionClosed);
                        }
                        self.publish_terminal_flags(true);
                        drop(_pending);
                        self.request_terminal(true);
                        self.context.ready_notify.notify_waiters();
                        self.context
                            .inbound
                            .lock()
                            .expect("host inbound lock")
                            .take();
                        return Err(Error::transport(
                            "module stopped draining its queue within the deadline",
                        ));
                    }
                }
                TB_CLOSED => return Err(Error::ConnectionClosed),
                TB_BAD_ARGUMENT => return Err(Error::protocol("module refused a valid frame")),
                _ => return Err(Error::transport("module delivery callback failed")),
            }
        }
    }

    async fn drain_pending(self: Arc<Self>) {
        self.wait_ready().await;
        loop {
            while !self.is_faulted() && !self.admission_closed() {
                let message = self
                    .pending
                    .lock()
                    .expect("module pending lock")
                    .pop_front();
                let Some(message) = message else {
                    break;
                };
                if let Err(error) = self.deliver_now(message.clone()).await {
                    self.fault_pending(message, error).await;
                    self.drain_started.store(false, Ordering::Release);
                    return;
                }
            }
            if self.is_faulted() || self.admission_closed() {
                self.drain_started.store(false, Ordering::Release);
                return;
            }
            // A `send` that pushed while the loop observed an empty queue saw
            // `drain_started == true` and skipped spawning its own drainer, so
            // it is this loop's job to pick the message up. Clear the flag only
            // while holding the queue lock, and return only once the queue is
            // genuinely empty; the lock makes the check-and-clear atomic with a
            // concurrent push.
            let restart = {
                let pending = self.pending.lock().expect("module pending lock");
                let empty = pending.is_empty();
                if empty {
                    self.drain_started.store(false, Ordering::Release);
                }
                !empty
            };
            if !restart {
                return;
            }
            // Fall back into the drain loop for the newly queued messages.
        }
    }

    /// Answer every still-queued call — and the one that just failed to
    /// deliver — with `ModuleUnavailable`, then fault the module, so callers
    /// learn the delivery failed instead of waiting out their deadline.
    async fn fault_pending(&self, failed: Message, error: Error) {
        let cleared = {
            let mut pending = self.pending.lock().expect("module pending lock");
            if self.admission_closed() && matches!(error, Error::ConnectionClosed) {
                pending.clear();
                return;
            }
            self.publish_terminal_flags(true);
            std::mem::take(&mut *pending)
        };
        // Each of these calls incremented `inflight` when it was queued. The
        // synthesized error replies below travel back through `recv`, which
        // refunds one slot per reply, so no manual adjustment is needed.
        let sender = self
            .context
            .inbound
            .lock()
            .expect("host inbound lock")
            .clone();
        if let Some(sender) = sender {
            for queued in cleared.iter().chain([&failed]) {
                if queued.header.kind == MessageKind::MethodCall {
                    let error = Error::ModuleUnavailable {
                        module: self.label.clone(),
                        state: "faulted".to_string(),
                        detail: "module stopped accepting calls".to_string(),
                    };
                    let reply = Message::error_reply(&queued.header, &error);
                    if let Ok(bytes) = serde_json::to_vec(&reply) {
                        let _ = sender.try_send(bytes);
                    }
                }
            }
        }
        self.request_terminal(true);
        self.context.ready_notify.notify_waiters();
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
    }

    pub(crate) fn shutdown_sync(&self, deadline: Duration) -> i32 {
        let module = *self.module.lock().expect("module vtable lock");
        match module {
            Some(module) => unsafe {
                (module.shutdown)(module.module_ctx, deadline.as_millis() as u64)
            },
            None => TB_CLOSED,
        }
    }

    pub(crate) fn stop_sync(&self, deadline: Duration) -> i32 {
        if self.is_stopped() {
            return TB_CLOSED;
        }
        let started = std::time::Instant::now();
        self.close_admission();
        // This is the retained physical worker, not the caller's timeout. Never
        // tear down code underneath an admitted callback or abort its lease.
        self.callbacks.wait_drained();
        let code = self.shutdown_sync(deadline.saturating_sub(started.elapsed()));
        if code == TB_OK {
            self.context.native_terminal_state.fetch_or(
                crate::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED,
                Ordering::AcqRel,
            );
        }
        self.finish_stop();
        self.context
            .inbound
            .lock()
            .expect("host inbound lock")
            .take();
        code
    }

    #[cfg(test)]
    pub(crate) fn native_terminal_state_for_test(&self) -> u32 {
        unsafe { host_native_terminal_state(std::ptr::from_ref(self.context).cast_mut().cast()) }
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_is_owned_for_test(&self) -> bool {
        self.lifecycle.try_lock().is_err()
    }

    pub(crate) fn stop_task_spawned(&self) -> bool {
        self.stop_task_spawned.load(Ordering::Acquire)
    }

    pub(crate) async fn stop(
        self: Arc<Self>,
        deadline: Duration,
    ) -> std::result::Result<i32, StopError> {
        if self.is_stopped() {
            return Err(StopError::NotStarted(Error::ConnectionClosed));
        }
        let started = std::time::Instant::now();
        let lifecycle = tokio::time::timeout(deadline, self.lifecycle.clone().lock_owned())
            .await
            .map_err(|_| {
                StopError::NotStarted(Error::failed(
                    "module lifecycle operation exceeded its deadline",
                ))
            })?;
        if self.is_stopped() {
            return Err(StopError::NotStarted(Error::ConnectionClosed));
        }
        let remaining = deadline.saturating_sub(started.elapsed());
        self.commit_terminal(false);
        let (_existing, job, mut acquired, ordinary_handoff) = {
            let mut state = self.context.physical.lock().expect("physical state lock");
            if state.cleanup == CleanupState::Idle {
                state.cleanup = CleanupState::Requested;
            }
            let existing = state.job.is_some();
            let mut acquired = Some(lifecycle);
            let job = if existing || state.cleanup != CleanupState::Requested {
                None
            } else {
                let job = Arc::new(PhysicalJob {
                    transport: self.clone(),
                    initializer: StdMutex::new(None),
                    guard: StdMutex::new(acquired.take()),
                    deadline: remaining,
                    fault_on_spawn_failure: false,
                    submitted: AtomicBool::new(false),
                    started: AtomicBool::new(false),
                });
                state.job = Some(job.clone());
                Some(job)
            };
            let ordinary_handoff = !self.is_faulted()
                && state.cleanup == CleanupState::Requested
                && (job.is_some() || (existing && state.init == InitState::Scheduled));
            if ordinary_handoff {
                self.stop_task_spawned.store(true, Ordering::Release);
            }
            (existing, job, acquired, ordinary_handoff)
        };
        // A Scheduled initializer must acquire the guard itself to prove no C entry.
        drop(acquired.take());
        if let Some(job) = job {
            Self::submit_job(job);
        }
        let classify = |error| {
            if ordinary_handoff {
                StopError::Started(error)
            } else {
                StopError::NotStarted(error)
            }
        };
        let wait = async {
            loop {
                let changed = self.context.physical_notify.notified();
                let outcome = {
                    let state = self.context.physical.lock().expect("physical state lock");
                    match state.cleanup {
                        CleanupState::Complete | CleanupState::NoCallbackRequired => {
                            Some(Ok(state.code.unwrap_or(TB_CLOSED)))
                        }
                        CleanupState::Uncertain => Some(match state.code {
                            Some(code) => Ok(code),
                            None => Err(classify(Error::transport("module cleanup is uncertain"))),
                        }),
                        _ => None,
                    }
                };
                if let Some(outcome) = outcome {
                    return outcome;
                }
                changed.await;
            }
        };
        tokio::time::timeout(remaining, wait)
            .await
            .map_err(|_| classify(Error::failed("module shutdown exceeded its deadline")))?
    }
}

impl PhysicalJob {
    fn run(self: Arc<Self>) {
        self.started.store(true, Ordering::Release);
        let transport = &self.transport;
        #[cfg(test)]
        {
            let hook = transport
                .before_worker_lock
                .lock()
                .expect("worker test hook lock")
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let initializer = self
            .initializer
            .lock()
            .expect("physical initializer lock")
            .take();
        if let Some(initializer) = initializer {
            let guard = transport.lifecycle.clone().blocking_lock_owned();
            let entered = {
                let mut state = transport
                    .context
                    .physical
                    .lock()
                    .expect("physical state lock");
                if transport.admission_closed() || transport.is_faulted() {
                    state.init = InitState::CanceledBeforeEntry;
                    state.cleanup = CleanupState::NoCallbackRequired;
                    state.result = Some(Err(InitFailure::Closed));
                    state.code = Some(TB_CLOSED);
                    state.job = None;
                    false
                } else {
                    state.init = InitState::Entered;
                    true
                }
            };
            if !entered {
                drop(initializer);
                transport.clear_config();
                transport.finish_stop();
                transport.notify_physical();
                return;
            }
            let output =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| initializer.run()));
            let result = match output {
                Ok(Ok(module)) => transport
                    .initialize(module.0)
                    .map_err(|_| InitFailure::Failed("module returned an invalid vtable".into())),
                Ok(Err(reason)) => Err(InitFailure::Failed(reason)),
                Err(_) => Err(InitFailure::Failed("module initialization panicked".into())),
            };
            transport.clear_config();
            #[cfg(test)]
            {
                let hook = transport
                    .before_init_publication
                    .lock()
                    .expect("publication test hook lock")
                    .take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            if result.is_err() {
                transport.commit_terminal(true);
            }
            let cleanup = {
                let mut state = transport
                    .context
                    .physical
                    .lock()
                    .expect("physical state lock");
                state.init = if result.is_ok() {
                    InitState::PublishedValid
                } else {
                    InitState::PublishedRejected
                };
                state.result.get_or_insert(result.clone());
                if result.is_err() {
                    state.cleanup = CleanupState::Uncertain;
                }
                if state.cleanup == CleanupState::Idle {
                    state.job = None;
                }
                state.cleanup == CleanupState::Requested
            };
            transport.notify_physical();
            drop(guard);
            if !cleanup {
                return;
            }
        } else {
            // Eager entry publishes before releasing its exclusion. Never hold
            // lifecycle while waiting for an initializer which needs that lock.
            let mut state = transport
                .context
                .physical
                .lock()
                .expect("physical state lock");
            while matches!(state.init, InitState::Scheduled | InitState::Entered)
                && state.result.is_none()
            {
                state = transport
                    .context
                    .physical_changed
                    .wait(state)
                    .expect("physical state lock");
            }
        }
        let mut acquired = self.guard.lock().expect("physical guard lock").take();
        if let Some(callbacks) = transport.host_callbacks.get() {
            // User futures may need lifecycle services. Retain this physical
            // owner, but release native exclusion before their actual drain.
            drop(acquired.take());
            callbacks.wait_drained();
        }
        let guard = acquired.unwrap_or_else(|| transport.lifecycle.clone().blocking_lock_owned());
        {
            let mut state = transport
                .context
                .physical
                .lock()
                .expect("physical state lock");
            if state.cleanup != CleanupState::Requested {
                return;
            }
            if state.init == InitState::Deferred || state.init == InitState::CanceledBeforeEntry {
                state.init = InitState::CanceledBeforeEntry;
                state.cleanup = CleanupState::NoCallbackRequired;
                state.result = Some(Err(InitFailure::Closed));
                state.code = Some(TB_CLOSED);
                state.job = None;
                drop(state);
                transport
                    .initializer
                    .lock()
                    .expect("module initializer lock")
                    .take();
                transport.clear_config();
                transport.finish_stop();
                transport.notify_physical();
                return;
            }
            if transport
                .module
                .lock()
                .expect("module vtable lock")
                .is_none()
            {
                state.cleanup = CleanupState::Uncertain;
                drop(state);
                transport.notify_physical();
                return;
            }
            state.cleanup = CleanupState::Executing;
        }
        let pending = NativeLifecyclePending::begin(&transport.native_lifecycle_pending);
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transport.stop_sync(self.deadline)
        }));
        // Completion is observable before this worker releases its lifecycle
        // guard. Clear native activity first so a completed stop cannot leave
        // enable/disable reporting that opaque work is still running.
        drop(pending);
        let mut state = transport
            .context
            .physical
            .lock()
            .expect("physical state lock");
        state.code = code.as_ref().ok().copied();
        state.cleanup = if matches!(code, Ok(TB_OK)) {
            CleanupState::Complete
        } else {
            CleanupState::Uncertain
        };
        if state.cleanup == CleanupState::Complete {
            state.job = None;
        }
        drop(state);
        transport.notify_physical();
        #[cfg(test)]
        {
            let hook = transport
                .after_cleanup_publication
                .lock()
                .expect("cleanup publication test hook lock")
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        drop(guard);
    }
}

unsafe extern "C" fn host_native_terminal_state(ctx: *mut c_void) -> u32 {
    unsafe { ctx.cast::<HostContext>().as_ref() }.map_or(
        crate::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT
            | crate::module::abi::TB_NATIVE_TERMINAL_FAULT,
        |context| context.native_terminal_state.load(Ordering::Acquire),
    )
}

unsafe extern "C" fn host_trusted_host_binding(
    ctx: *mut c_void,
    out: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32 {
    if out_len.is_null() {
        return TB_BAD_ARGUMENT;
    }
    unsafe { out_len.write(0) };
    if capacity > crate::name::MAX_NAME_LEN || (out.is_null() && capacity != 0) {
        return TB_BAD_ARGUMENT;
    }

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(context) = (unsafe { ctx.cast::<HostContext>().as_ref() }) else {
            return TB_CLOSED;
        };
        let Some(binding) = context.trusted_host_binding.as_ref() else {
            return TB_UNSUPPORTED;
        };
        let owner = context.owner.lock().ok().and_then(|owner| owner.upgrade());
        let Some(owner) = owner else {
            return TB_CLOSED;
        };
        if context.native_terminal_state.load(Ordering::Acquire) != TB_NATIVE_TERMINAL_LIVE
            || !owner.init_started()
            || owner.init_failed()
            || owner.is_stopped()
            || owner.is_faulted()
            || owner.admission_closed()
            || !binding.is_live()
        {
            return TB_CLOSED;
        }

        let peer = binding.peer().as_str().as_bytes();
        if peer.len() > crate::name::MAX_NAME_LEN {
            return TB_BAD_ARGUMENT;
        }
        if capacity < peer.len() {
            unsafe { out_len.write(peer.len()) };
            return TB_BUFFER_TOO_SMALL;
        }
        if !peer.is_empty() {
            unsafe { std::ptr::copy_nonoverlapping(peer.as_ptr(), out, peer.len()) };
        }
        unsafe { out_len.write(peer.len()) };
        TB_OK
    })) {
        Ok(code) => code,
        Err(_) => TB_PANICKED,
    }
}

#[derive(Debug)]
pub(crate) enum StopError {
    NotStarted(Error),
    Started(Error),
}

impl From<StopError> for Error {
    fn from(error: StopError) -> Self {
        match error {
            StopError::NotStarted(error) | StopError::Started(error) => error,
        }
    }
}

#[async_trait]
impl Transport for ModuleTransport {
    async fn send(&self, message: Message) -> Result<()> {
        if self.admission_closed() {
            return Err(Error::ConnectionClosed);
        }
        if let Err(error) = self.ensure_initialized().await {
            {
                let _pending = self.pending.lock().expect("module pending lock");
                if self.admission_closed() && matches!(error, Error::ConnectionClosed) {
                    return Err(Error::ConnectionClosed);
                }
                self.context.init_failed.store(true, Ordering::Release);
                self.publish_terminal_flags(true);
            }
            self.context.ready_notify.notify_waiters();
            if message.header.kind == MessageKind::MethodCall {
                let error = Error::ModuleUnavailable {
                    module: self.label.clone(),
                    state: "failed".to_string(),
                    detail: "module initialization failed".to_string(),
                };
                let reply = Message::error_reply(&message.header, &error);
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    let sender = self
                        .context
                        .inbound
                        .lock()
                        .expect("host inbound lock")
                        .take();
                    if let Some(sender) = sender {
                        let _ = sender.try_send(bytes);
                    }
                }
                self.request_terminal(true);
                return Ok(());
            }
            self.request_terminal(true);
            return Err(Error::ConnectionClosed);
        }
        let is_call = message.header.kind == MessageKind::MethodCall;
        let pre_ready_bypass_peer = if is_call {
            self.authorized_pre_ready_peer(&message)
                .filter(|_| !self.is_ready())
        } else {
            None
        };
        {
            let mut pending = self.pending.lock().expect("module pending lock");
            if self.admission_closed() {
                return Err(Error::ConnectionClosed);
            }
            if is_call {
                self.context.inflight.fetch_add(1, Ordering::AcqRel);
            }
            if is_call && !self.is_ready() && pre_ready_bypass_peer.is_none() {
                pending.push_back(message);
                if !self.drain_started.swap(true, Ordering::AcqRel) {
                    let transport = self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?;
                    tokio::spawn(transport.drain_pending());
                }
                return Ok(());
            }
        }
        let result = self
            .deliver_now_with_pre_ready_peer(message, pre_ready_bypass_peer.as_ref())
            .await;
        if result.is_err() && is_call {
            self.context.inflight.fetch_sub(1, Ordering::AcqRel);
        }
        result
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let bytes = self.inbound.lock().await.recv().await;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let message: Message = serde_json::from_slice(&bytes)?;
        if matches!(
            message.header.kind,
            MessageKind::MethodReturn | MessageKind::Error
        ) {
            // Saturating decrement via a CAS loop (avoids `fetch_update`, which
            // newer toolchains deprecate in favour of `try_update`).
            let inflight = &self.context.inflight;
            let mut current = inflight.load(Ordering::Acquire);
            while let Some(next) = current.checked_sub(1) {
                match inflight.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }
        Ok(Some(message))
    }

    async fn close(&self) -> Result<()> {
        // The module's `shutdown` callback may block up to its deadline. Run it
        // on a blocking thread so a wedged shutdown cannot stall the broker
        // task that is closing this transport.
        let transport = self.self_ref.upgrade().ok_or(Error::ConnectionClosed)?;
        transport.stop(Duration::from_secs(5)).await?;
        Ok(())
    }

    fn describe(&self) -> String {
        format!("module:{}", self.label)
    }
}

unsafe extern "C" fn host_send(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ctx.is_null() || ptr.is_null() || len > MAX_FRAME_LEN {
            return TB_BAD_ARGUMENT;
        }
        let context = unsafe { &*(ctx.cast::<HostContext>()) };
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        let sender = context.inbound.lock().expect("host inbound lock").clone();
        match sender {
            // This callback runs on a module-owned runtime thread. Blocking it
            // applies bounded backpressure only to that module and gives the
            // async SDK a reliable completion without adding a fifth callback
            // to the frozen v1 ABI.
            Some(sender) => match if tokio::runtime::Handle::try_current().is_ok() {
                // Linked modules share Tokio's thread-local runtime state with
                // the host. `blocking_send` directly on their worker panics.
                tokio::task::block_in_place(|| sender.blocking_send(bytes))
            } else {
                sender.blocking_send(bytes)
            } {
                Ok(()) => TB_OK,
                Err(_) => TB_CLOSED,
            },
            None => TB_CLOSED,
        }
    }))
    .unwrap_or(TB_BACKPRESSURE)
}

unsafe extern "C" fn host_wake(ctx: *mut c_void) {
    // A panic escaping a plain `extern "C"` frame aborts the process, so each
    // host callback that a module can reach must contain its own unwinds.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            context.wake.notify_one();
        }
    }));
}

unsafe extern "C" fn host_log(ctx: *mut c_void, level: u32, ptr: *const u8, len: usize) {
    // `host_log` is the most exposed callback: the `tracing` macros run
    // arbitrary subscriber code chosen by the embedding host, so a panicking
    // subscriber must not be able to abort the process through the module.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ctx.is_null() || ptr.is_null() {
            return;
        }
        let len = len.min(4096);
        let message = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(ptr, len) });
        match level {
            1 => tracing::error!(target: "tinybus_module", message = %message),
            2 => tracing::warn!(target: "tinybus_module", message = %message),
            3 => tracing::info!(target: "tinybus_module", message = %message),
            4 => tracing::debug!(target: "tinybus_module", message = %message),
            _ => tracing::trace!(target: "tinybus_module", message = %message),
        }
    }));
}

unsafe extern "C" fn host_fault(ctx: *mut c_void, _: *const u8, _: usize) {
    // `host_fault` exists so a misbehaving module is detached instead of
    // taking the host down; an abort inside it would defeat that goal.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            context.faulted.store(true, Ordering::Release);
            context.native_terminal_state.fetch_or(
                crate::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT
                    | crate::module::abi::TB_NATIVE_TERMINAL_FAULT,
                Ordering::AcqRel,
            );
            context.native_terminal_state.fetch_and(
                !crate::module::abi::TB_NATIVE_TERMINAL_LIVE,
                Ordering::AcqRel,
            );
            context.init_notify.notify_waiters();
            context.ready_notify.notify_waiters();
            context.inbound.lock().expect("host inbound lock").take();
            let owner = context.owner.lock().expect("physical owner lock").upgrade();
            if let Some(owner) = owner {
                owner.request_terminal(true);
            }
        }
    }));
}

unsafe extern "C" fn host_broker_routing(ctx: *mut c_void) -> u32 {
    // HostContext is leaked for the lifetime of the vtable; no module-owned
    // JSON or transport label can grant this provenance.
    unsafe { ctx.cast::<HostContext>().as_ref() }
        .is_some_and(|context| context.broker_routing.load(Ordering::Acquire)) as u32
}

unsafe extern "C" fn host_ready(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(context) = unsafe { ctx.cast::<HostContext>().as_ref() } {
            let _ready = context.ready_gate.lock().expect("module ready lock");
            if !context.admission_closed.load(Ordering::Acquire) {
                context.ready.store(true, Ordering::Release);
                context.ready_notify.notify_waiters();
            }
        }
    }));
}

#[cfg(test)]
#[path = "transport_tests.rs"]
pub(super) mod tests;
