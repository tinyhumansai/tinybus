use super::*;
use std::sync::atomic::AtomicU32;
use tinybus::module::abi::{TB_NATIVE_TERMINAL_STOP_INTENT, TbSlice};
static TEST_LOCK: StdMutex<()> = StdMutex::new(());
static FLAGS: AtomicU32 = AtomicU32::new(TB_NATIVE_TERMINAL_LIVE);
unsafe extern "C" fn query(_: *mut c_void) -> u32 {
    FLAGS.load(Ordering::Acquire)
}
unsafe extern "C" fn send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}
unsafe extern "C" fn notify(_: *mut c_void) {}
unsafe extern "C" fn fault(_: *mut c_void, _: *const u8, _: usize) {
    FLAGS.store(4, Ordering::Release);
}
unsafe extern "C" fn log(_: *mut c_void, _: u32, _: *const u8, _: usize) {}
fn host() -> HostCalls {
    HostCalls(TbHostVtable {
        size: size_of::<TbHostVtable>() as u32,
        _reserved: 0,
        host_ctx: std::ptr::null_mut(),
        send,
        wake: notify,
        log,
        ready: notify,
        fault,
        config: TbSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        broker_routing: None,
        native_terminal_state: Some(query),
        trusted_host_binding: None,
    })
}
#[test]
fn observed_native_terminal_never_reopens_a_retained_view() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let view = state.view();
    assert!(view.is_live());
    FLAGS.store(TB_NATIVE_TERMINAL_STOP_INTENT, Ordering::Release);
    assert!(!view.is_live());
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    assert!(
        !view.is_live(),
        "an observed terminal tombstone must stay closed"
    );
    state.publish();
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::PhysicallyStopped
    );
}

#[test]
fn retired_anchor_cannot_refund_permit_while_old_view_survives() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let mut views = Vec::new();
    for _ in 0..INSTANCE_LIMIT {
        let state = TerminalState::admit_in(&admission, host(), None).unwrap();
        views.push(state.view());
        state.empty_factory_failure();
    }
    assert!(TerminalState::admit_in(&admission, host(), None).is_none());
    views.pop();
    let replacement = TerminalState::admit_in(&admission, host(), None).unwrap();
    replacement.empty_factory_failure();
    drop(replacement);
    drop(views);
    assert!(
        admission
            .slots
            .lock()
            .unwrap()
            .iter()
            .all(|slot| !slot.reserved)
    );
}

struct AnchorRemovalRelease {
    gate: Arc<AnchorRemovalGate>,
    released: bool,
}
impl AnchorRemovalRelease {
    fn release(&mut self) {
        if !self.released {
            self.gate.release.wait();
            self.released = true;
        }
    }
}
impl Drop for AnchorRemovalRelease {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
fn physical_completion_is_published_only_after_anchor_retirement() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let view = state.view();
    let gate = Arc::new(AnchorRemovalGate {
        entered: std::sync::Barrier::new(2),
        release: std::sync::Barrier::new(2),
    });
    *state.before_anchor_removal.lock().unwrap() = Some(gate.clone());
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    state.signal(TerminalReason::Stop);

    gate.entered.wait();
    let mut release = AnchorRemovalRelease {
        gate,
        released: false,
    };
    assert!(TerminalState::lookup_in(&admission, state.context().addr()).is_some());
    assert_eq!(
        view.inner.wait(Duration::ZERO),
        TerminalCompletion::Uncertain
    );

    release.release();
    assert_eq!(
        view.inner.wait(Duration::from_secs(3)),
        TerminalCompletion::PhysicallyStopped
    );
    assert!(TerminalState::lookup_in(&admission, state.context().addr()).is_none());
    assert!(admission.slots.lock().unwrap()[state.context().addr() & 0xff].reserved);
    drop(state);
    assert!(
        admission.slots.lock().unwrap()[0].reserved,
        "the retained liveness view keeps the instance permit occupied"
    );
    drop(view);
}

#[test]
fn timeout_uncertainty_wins_over_a_prepared_physical_completion() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let gate = Arc::new(AnchorRemovalGate {
        entered: std::sync::Barrier::new(2),
        release: std::sync::Barrier::new(2),
    });
    *state.before_anchor_removal.lock().unwrap() = Some(gate.clone());
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    state.signal(TerminalReason::Stop);

    gate.entered.wait();
    let mut release = AnchorRemovalRelease {
        gate,
        released: false,
    };
    assert_eq!(
        state.wait(Duration::ZERO),
        TerminalCompletion::Uncertain,
        "the deadline expires before completion publication"
    );
    state.retain_uncertain_anchor();
    assert!(TerminalState::lookup_in(&admission, state.context().addr()).is_some());

    release.release();
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::Uncertain,
        "a prepared physical result cannot erase timeout uncertainty"
    );
    assert!(
        TerminalState::lookup_in(&admission, state.context().addr()).is_some(),
        "uncertain completion keeps its exact callback anchor"
    );
}

#[test]
fn uncertainty_after_physical_publication_downgrades_and_reanchors() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    state.signal(TerminalReason::Stop);

    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::PhysicallyStopped
    );
    assert!(TerminalState::lookup_in(&admission, state.context().addr()).is_none());

    state.retain_uncertain_anchor();

    assert_eq!(
        state.wait(Duration::ZERO),
        TerminalCompletion::Uncertain,
        "a later fault observation cannot leave stale physical success published"
    );
    assert!(TerminalState::lookup_in(&admission, state.context().addr()).is_some());
}

#[test]
fn request_primary_payload_is_retained_and_finish_still_attempted_once() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    use std::sync::atomic::AtomicUsize;
    struct PayloadDrop(Arc<AtomicUsize>);
    impl Drop for PayloadDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let (request_count, finish_count, payload) =
        (requests.clone(), finishes.clone(), dropped.clone());
    state.register(RuntimeHooks::new(
        move |_| {
            request_count.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(PayloadDrop(payload.clone()));
        },
        move || {
            finish_count.fetch_add(1, Ordering::SeqCst);
            TerminalCompletion::PhysicallyStopped
        },
    ));
    state.publish();
    state.signal(TerminalReason::Stop);
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::Uncertain
    );
    state.signal(TerminalReason::Fault);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(state.lock().request_state == RequestState::Panicked);
    assert!(
        admission.slots.lock().unwrap()[state.lease.index]
            .anchor
            .is_some()
    );
}

#[test]
fn physical_finish_waits_for_bounded_request_completion_and_startup_publication() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Barrier::new(2));
    let request_release = release.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let finish_seen = finished.clone();
    state.register(RuntimeHooks::new(
        move |_| {
            entered_tx.send(()).unwrap();
            request_release.wait();
        },
        move || {
            finish_seen.store(true, Ordering::Release);
            TerminalCompletion::PhysicallyStopped
        },
    ));
    let caller_state = state.clone();
    let caller = std::thread::spawn(move || caller_state.signal(TerminalReason::Stop));
    entered_rx.recv().unwrap();
    state.publish();
    assert!(!finished.load(Ordering::Acquire));
    release.wait();
    caller.join().unwrap();
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::PhysicallyStopped
    );
    assert!(finished.load(Ordering::Acquire));
}

#[test]
fn surviving_sdk_owned_transport_keeps_cleanup_uncertain_and_permit_anchored() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let sdk_owner = state.sdk_owner();
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    state.signal(TerminalReason::Stop);
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::Uncertain
    );
    drop(sdk_owner);
    assert!(
        admission.slots.lock().unwrap()[state.lease.index]
            .anchor
            .is_some()
    );
}

#[test]
fn stale_context_never_selects_replacement_and_unknown_contexts_are_closed() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let old = state.context().addr();
    state.empty_factory_failure();
    drop(state);
    let replacement = TerminalState::admit_in(&admission, host(), None).unwrap();
    assert_eq!(replacement.lease.index, old & 255);
    assert_ne!(replacement.context().addr(), old);
    assert!(TerminalState::lookup_in(&admission, old).is_none());
    for invalid in [0, 1, 255, usize::MAX] {
        assert!(TerminalState::lookup_in(&admission, invalid).is_none());
    }
    assert!(Arc::ptr_eq(
        &TerminalState::lookup_in(&admission, replacement.token).unwrap(),
        &replacement
    ));
    replacement.empty_factory_failure();
}
#[test]
fn exhausted_slot_generation_never_wraps_or_selects_old_context() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    {
        let mut slots = admission.slots.lock().unwrap();
        slots[0].generation = usize::MAX >> 8;
        for slot in &mut slots[1..] {
            slot.exhausted = true;
        }
    }
    assert!(TerminalState::admit_in(&admission, host(), None).is_none());
    assert!(admission.slots.lock().unwrap()[0].exhausted);
    assert!(TerminalState::lookup_in(&admission, (usize::MAX >> 8) << 8).is_none());
}
#[test]
fn callback_lookup_holds_exact_state_across_retirement_until_actual_last_drop() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let admitted = TerminalState::lookup_in(&admission, state.token).unwrap();
    let index = state.lease.index;
    state.empty_factory_failure();
    drop(state);
    assert!(admission.slots.lock().unwrap()[index].reserved);
    assert_eq!(admitted.lease.index, index);
    drop(admitted);
    assert!(!admission.slots.lock().unwrap()[index].reserved);
}

#[test]
fn first_cleanup_drop_panic_retains_all_unprocessed_resource_owners() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    struct FirstDrop;
    impl Drop for FirstDrop {
        fn drop(&mut self) {
            panic!("controlled first cleanup Drop");
        }
    }
    struct RemainingDrop(Arc<AtomicUsize>);
    impl Drop for RemainingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let admission = Admission::new();
    let state = TerminalState::admit_in(&admission, host(), None).unwrap();
    let remaining = Arc::new(AtomicUsize::new(0));
    {
        let mut resources = state.resources.lock().unwrap();
        let first = FirstDrop;
        resources.setup = Some(Box::new(move |_| {
            drop(first);
            Box::pin(async { Ok(()) })
        }));
        resources.retained_factory = Some(Box::new(RemainingDrop(remaining.clone())));
    }
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    state.signal(TerminalReason::Stop);
    let completion = state.wait(Duration::from_secs(3));
    let dropped = remaining.load(Ordering::SeqCst);
    assert_eq!(completion, TerminalCompletion::Uncertain);
    assert_eq!(
        dropped, 0,
        "first cleanup panic must leave later foreign owners attached"
    );
    assert!(state.resources.lock().unwrap().retained_factory.is_some());
}

#[cfg(feature = "__native-fixture-support")]
#[test]
fn retained_view_reports_exact_execution_owner_and_surviving_sdk_resources() {
    let _serial = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FLAGS.store(TB_NATIVE_TERMINAL_LIVE, Ordering::Release);
    let admission = Admission::new();
    let owner = ExecutionOwner::new(host());
    let state = TerminalState::admit_in(&admission, host(), Some(owner.clone())).unwrap();
    let view = state.view();
    assert!(!view.__fixture_owner_is_current());
    assert!(!view.__fixture_owner_is_stopped());
    {
        let _scope = owner.enter();
        assert!(view.__fixture_owner_is_current());
        let other = ExecutionOwner::new(host());
        {
            let _other_scope = other.enter();
            assert!(!view.__fixture_owner_is_current());
        }
        assert!(view.__fixture_owner_is_current());
    }
    assert!(!view.__fixture_owner_is_current());
    assert_eq!(view.__fixture_resources(), 0);
    let sdk_owner = state.sdk_owner();
    assert_eq!(view.__fixture_sdk_owners(), 1);
    assert!(!view.__fixture_request_done());
    drop(sdk_owner);
    assert_eq!(view.__fixture_sdk_owners(), 0);
    state.register(RuntimeHooks::new(
        |_| {},
        || TerminalCompletion::PhysicallyStopped,
    ));
    state.publish();
    view.request_fault();
    assert_eq!(
        state.wait(Duration::from_secs(3)),
        TerminalCompletion::PhysicallyStopped
    );
    assert!(view.__fixture_request_done());
    assert!(view.__fixture_owner_is_stopped());
    assert!(!view.is_live());
}
