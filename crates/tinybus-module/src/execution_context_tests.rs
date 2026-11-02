//! Execution-context ownership across callers, workers and blocking tasks.
use super::*;
use std::sync::atomic::AtomicUsize;

unsafe extern "C" fn log(ctx: *mut c_void, _: u32, _: *const u8, _: usize) {
    unsafe { &*ctx.cast::<AtomicUsize>() }.fetch_add(1, Ordering::SeqCst);
}
unsafe extern "C" fn send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}
unsafe extern "C" fn wake(_: *mut c_void) {}
unsafe extern "C" fn fault(_: *mut c_void, _: *const u8, _: usize) {}
unsafe extern "C" fn ready(_: *mut c_void) {}
fn owner() -> (ExecutionOwner, &'static AtomicUsize) {
    let count = Box::leak(Box::new(AtomicUsize::new(0)));
    let host = HostCalls(TbHostVtable {
        size: std::mem::size_of::<TbHostVtable>() as u32,
        _reserved: 0,
        host_ctx: std::ptr::from_mut(count).cast(),
        send,
        wake,
        log,
        fault,
        config: tinybus::module::abi::TbSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        ready,
        broker_routing: None,
        native_terminal_state: None,
        trusted_host_binding: None,
    });
    (ExecutionOwner::new(host), count)
}
#[test]
fn nested_caller_scopes_restore_owner_and_stopped_context_never_uses_replacement() {
    ExecutionOwner::install_panic_hook();
    let (first, first_count) = owner();
    let (second, second_count) = owner();
    let outer = first.enter();
    let parent = tracing::info_span!("parent");
    let child = tracing::info_span!("child", value = tracing::field::Empty);
    child.record("value", 42);
    child.follows_from(parent.id());
    {
        let _entered = child.enter();
    }
    tracing::info!("first caller");
    {
        let _inner = second.enter();
        tracing::info!("second caller");
        let _ = std::panic::catch_unwind(|| panic!("private payload"));
        assert_eq!(first_count.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.load(Ordering::SeqCst), 2);
        assert!(second.panic_location.lock().unwrap().is_some());
        assert!(first.panic_location.lock().unwrap().is_none());
        second.stop();
        tracing::info!("stopped caller");
        let _ = std::panic::catch_unwind(|| panic!("private late payload"));
        assert_eq!(second_count.load(Ordering::SeqCst), 2);
    }
    tracing::info!("first restored");
    assert_eq!(first_count.load(Ordering::SeqCst), 2);
    drop(outer);
    assert!(OWNER.with(|slot| slot.borrow().is_none()));
    tracing::info!("outside any module");
    assert_eq!(first_count.load(Ordering::SeqCst), 2);
    assert_eq!(second_count.load(Ordering::SeqCst), 2);
}
#[test]
fn worker_blocking_and_block_on_contexts_use_their_own_runtime_callbacks() {
    for _ in 0..2 {
        let (owner, count) = owner();
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(1).enable_all();
        owner.configure(&mut builder);
        let runtime = builder.build().unwrap();
        {
            let _caller = owner.enter();
            runtime.block_on(async {
                tracing::info!("block on caller");
                runtime
                    .spawn(async {
                        tracing::info!("worker");
                        tokio::task::spawn_blocking(|| tracing::info!("blocking worker"))
                            .await
                            .unwrap();
                    })
                    .await
                    .unwrap();
            });
        }
        assert_eq!(count.load(Ordering::SeqCst), 3);
        owner.stop();
        runtime.shutdown_timeout(Duration::from_secs(1));
        assert!(OWNER.with(|slot| slot.borrow().is_none()));
    }
}

#[test]
fn unowned_and_reentrant_panic_contexts_never_select_a_host_callback() {
    ExecutionOwner::install_panic_hook();
    let (owner, count) = owner();
    assert!(std::panic::catch_unwind(|| panic!("unowned secret payload")).is_err());
    {
        let _guard = owner.enter();
        OWNER.with(|slot| {
            let _borrowed = slot.borrow_mut();
            assert!(std::panic::catch_unwind(|| panic!("reentrant secret payload")).is_err());
        });
    }
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(owner.panic_location.lock().unwrap().is_none());
}

static CONFIG_DISPATCH: StdMutex<Option<tracing::Dispatch>> = StdMutex::new(None);
struct CapturedConfig;
impl<'de> serde::Deserialize<'de> for CapturedConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let _ = serde_json::Value::deserialize(deserializer)?;
        *CONFIG_DISPATCH.lock().unwrap() = Some(tracing::dispatcher::get_default(Clone::clone));
        Ok(Self)
    }
}

#[test]
fn shutdown_suppresses_the_exact_factory_and_configuration_caller_dispatch() {
    for factory in [true, false] {
        let (fixture, count) = owner();
        let mut host = fixture.host.0;
        host.config = tinybus::module::abi::TbSlice {
            ptr: b"{}".as_ptr(),
            len: 2,
        };
        let captured = Arc::new(StdMutex::new(None));
        let capture = captured.clone();
        let mut module = TbModuleVtable::default();
        let started = if factory {
            unsafe {
                start_reconfigurable_module_with_factory::<serde_json::Value, _, _>(
                    &host,
                    &mut module,
                    1,
                    false,
                    move || {
                        *capture.lock().unwrap() =
                            Some(tracing::dispatcher::get_default(Clone::clone));
                        |_, _| async { Ok(()) }
                    },
                )
            }
        } else {
            unsafe {
                start_module_with_config::<CapturedConfig, _, _>(
                    &host,
                    &mut module,
                    1,
                    false,
                    |_, _| async { Ok(()) },
                )
            }
        };
        let dispatch = if factory {
            captured.lock().unwrap().take()
        } else {
            CONFIG_DISPATCH.lock().unwrap().take()
        };
        let before = count.load(Ordering::SeqCst);
        if let Some(dispatch) = &dispatch {
            tracing::dispatcher::with_default(dispatch, || {
                tracing::info!("captured caller before stop")
            });
        }
        let live = count.load(Ordering::SeqCst);
        let stopped = if started == TB_OK {
            unsafe { (module.shutdown)(module.module_ctx, 1000) }
        } else {
            TB_CLOSED
        };
        let after_shutdown = count.load(Ordering::SeqCst);
        let (replacement_fixture, replacement_count) = owner();
        let mut replacement_host = replacement_fixture.host.0;
        replacement_host.config = host.config;
        let mut replacement = TbModuleVtable::default();
        let replaced = unsafe {
            start_module(&replacement_host, &mut replacement, 1, false, |_| async {
                Ok(())
            })
        };
        let replacement_before = replacement_count.load(Ordering::SeqCst);
        if let Some(dispatch) = &dispatch {
            tracing::dispatcher::with_default(dispatch, || {
                tracing::info!("captured caller after actual shutdown")
            });
        }
        let late = count.load(Ordering::SeqCst);
        let replacement_after = replacement_count.load(Ordering::SeqCst);
        let replacement_stopped = if replaced == TB_OK {
            unsafe { (replacement.shutdown)(replacement.module_ctx, 1000) }
        } else {
            TB_CLOSED
        };
        eprintln!(
            "factory={factory}: start={started}, stop={stopped}, before={before}, live={live}, after_shutdown={after_shutdown}, late={late}, replacement={replacement_before}/{replacement_after}, replacement_stop={replacement_stopped}"
        );
        assert_eq!(started, TB_OK);
        assert!(dispatch.is_some());
        assert!(
            live > before,
            "real caller dispatch forwards before shutdown"
        );
        assert_eq!(stopped, TB_OK);
        assert_eq!(replaced, TB_OK);
        assert_eq!(replacement_stopped, TB_OK);
        assert_eq!(
            late, after_shutdown,
            "stopped caller dispatch must not forward"
        );
        assert_eq!(
            replacement_after, replacement_before,
            "old dispatch cannot select replacement"
        );
    }
}

unsafe extern "C" fn tracing_native_query(_: *mut c_void) -> u32 {
    tracing::info!("terminal delivery query");
    tinybus::module::abi::TB_NATIVE_TERMINAL_LIVE
}
#[test]
fn terminal_deliver_enters_exact_instance_and_restores_nested_caller() {
    let (caller, caller_count) = owner();
    let (instance, instance_count) = owner();
    let host = HostCalls(TbHostVtable {
        native_terminal_state: Some(tracing_native_query),
        trusted_host_binding: None,
        ..instance.host.0
    });
    let state = super::super::terminal::TerminalState::admit(host, Some(instance)).unwrap();
    let _outer = caller.enter();
    let byte = 0;
    let code = unsafe { super::super::terminal_deliver(state.context(), &byte, 1) };
    tracing::info!("restored caller");
    state.empty_factory_failure();
    assert_eq!(code, TB_CLOSED); // No inbound queue was installed.
    assert_eq!(instance_count.load(Ordering::SeqCst), 1);
    assert_eq!(caller_count.load(Ordering::SeqCst), 1);
}
