use super::*;
use std::sync::atomic::AtomicUsize;

unsafe extern "C" fn send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}
unsafe extern "C" fn wake(_: *mut c_void) {}
unsafe extern "C" fn log(_: *mut c_void, _: u32, _: *const u8, _: usize) {}
unsafe extern "C" fn fault(_: *mut c_void, _: *const u8, _: usize) {}
unsafe extern "C" fn ready(_: *mut c_void) {}
fn host() -> TbHostVtable {
    TbHostVtable {
        size: size_of::<TbHostVtable>() as u32,
        _reserved: 0,
        host_ctx: std::ptr::null_mut(),
        send,
        wake,
        log,
        fault,
        config: tinybus::module::abi::TbSlice {
            ptr: b"{}".as_ptr(),
            len: 2,
        },
        ready,
        broker_routing: None,
        native_terminal_state: None,
        trusted_host_binding: None,
    }
}
struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn shutdown_releases_captured_callback_while_retaining_closed_tombstone() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let token = DropCounter(Arc::clone(&dropped));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut state = RuntimeState {
        owner: None,
        inbound: StdMutex::new(None),
        host: HostCalls(host()),
        runtime: StdMutex::new(Some(runtime)),
        reinitialize: Arc::new(StdMutex::new(Some(Box::new(move |_, _| {
            let _ = &token;
            TB_OK
        })))),
    };
    let pointer = std::ptr::from_mut(&mut state).cast();
    assert_eq!(unsafe { shutdown(pointer, 1000) }, TB_OK);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "ABI tombstone must not retain factory state"
    );
    assert_eq!(
        unsafe { reinitialize(pointer, std::ptr::null(), 0) },
        TB_CLOSED
    );
    assert_eq!(unsafe { deliver(pointer, b"{}".as_ptr(), 2) }, TB_CLOSED);
    assert_eq!(unsafe { shutdown(pointer, 1000) }, TB_CLOSED);
}
#[test]
fn invalid_output_or_workers_rejects_before_factory_side_effects() {
    let host = host();
    let mut observations = Vec::new();
    for (output_size, workers, null_output) in [
        (size_of::<TbModuleVtable>() as u32, 1, true),
        (0, 1, false),
        (size_of::<TbModuleVtable>() as u32, 0, false),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let mut output = std::mem::MaybeUninit::<TbModuleVtable>::zeroed();
        let pointer = if null_output {
            std::ptr::null_mut()
        } else {
            unsafe {
                output.as_mut_ptr().cast::<u32>().write(output_size);
            }
            output.as_mut_ptr()
        };
        let code = unsafe {
            start_linked_reconfigurable_module_with_factory::<serde_json::Value, _, _>(
                &host,
                pointer,
                workers,
                true,
                move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    |_: Connection, _: serde_json::Value| async { Ok(()) }
                },
            )
        };
        assert_eq!(code, TB_BAD_ARGUMENT);
        observations.push(calls.load(Ordering::SeqCst));
    }
    assert_eq!(
        observations,
        [0, 0, 0],
        "invalid ABI must not invoke factory"
    );
}

#[test]
fn shutdown_excludes_active_reinitializer_before_disposing_capture() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let token = DropCounter(Arc::clone(&dropped));
    let (entered, started) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let released = StdMutex::new(released);
    let state = Arc::new(RuntimeState {
        owner: None,
        inbound: StdMutex::new(None),
        host: HostCalls(host()),
        runtime: StdMutex::new(Some(tokio::runtime::Runtime::new().unwrap())),
        reinitialize: Arc::new(StdMutex::new(Some(Box::new(move |_, _| {
            let _ = &token;
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            TB_OK
        })))),
    });
    let active = Arc::clone(&state);
    let reinitializer = std::thread::spawn(move || unsafe {
        reinitialize(Arc::as_ptr(&active).cast_mut().cast(), std::ptr::null(), 0)
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        state.reinitialize.try_lock().is_err(),
        "active ABI callback must retain lifecycle exclusion"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    let stopping = Arc::clone(&state);
    let shutdown_thread = std::thread::spawn(move || unsafe {
        shutdown(Arc::as_ptr(&stopping).cast_mut().cast(), 1000)
    });
    release.send(()).unwrap();
    assert_eq!(reinitializer.join().unwrap(), TB_OK);
    assert_eq!(shutdown_thread.join().unwrap(), TB_OK);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        unsafe { reinitialize(Arc::as_ptr(&state).cast_mut().cast(), std::ptr::null(), 0) },
        TB_CLOSED
    );
}

#[test]
fn shutdown_disposes_callback_even_after_reinitializer_panics() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let token = DropCounter(Arc::clone(&dropped));
    let mut state = RuntimeState {
        owner: None,
        inbound: StdMutex::new(None),
        host: HostCalls(host()),
        runtime: StdMutex::new(None),
        reinitialize: Arc::new(StdMutex::new(Some(Box::new(move |_, _| {
            let _ = &token;
            panic!("fixture callback failed")
        })))),
    };
    let pointer = std::ptr::from_mut(&mut state).cast();
    assert_eq!(
        unsafe { reinitialize(pointer, std::ptr::null(), 0) },
        TB_PANICKED
    );
    assert_eq!(unsafe { shutdown(pointer, 1000) }, TB_CLOSED);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_setup_disposal_keeps_single_runtime_worker_available() {
    let callback: Arc<StdMutex<Option<Reinitialize>>> =
        Arc::new(StdMutex::new(Some(Box::new(|_, _| TB_OK))));
    let held = callback.lock().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let dispose = callback.clone();
    runtime.spawn(async move {
        entered_tx.send(()).unwrap();
        dispose_failed_setup_callback(dispose).await.unwrap();
        done_tx.send(()).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        progress_tx.send(()).unwrap();
    });
    let progressed = progress_rx.recv_timeout(Duration::from_secs(1)).is_ok();
    // Always release exclusion before asserting, including the RED path.
    drop(held);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(callback.lock().unwrap().is_none());
    runtime.shutdown_timeout(Duration::from_secs(1));
    assert!(
        progressed,
        "callback disposal must not occupy the sole timer worker"
    );
}
