//! Native SDK reload lifecycle regressions using the real compiled clock fixture.
use super::*;
use crate::{Connection, transport::memory::MemoryBus};

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_stopped_native_sdk_instance_can_be_replaced_after_all_names_are_released() {
    let artifact = std::env::var_os("TINYBUS_TEST_MODULE").expect("TINYBUS_TEST_MODULE");
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "ai.tinyhumans.openhuman.Clock",
            "/ai/tinyhumans/openhuman/Clock",
            "ai.tinyhumans.openhuman.Clock",
        )
        .unwrap();
    let first = ModuleHost::new(broker.clone());
    let info = first
        .load_file_with_config(&artifact, serde_json::json!({"prefix":"first:"}))
        .unwrap();
    let value: String = proxy.call("Now", ()).await.unwrap();
    assert!(value.starts_with("first:"));
    first.shutdown(Duration::from_secs(3)).await;
    assert!(matches!(first.list()[0].state, ModuleState::Stopped));
    // Prove broker detach has released BOTH the manifest name and setup alias.
    // A replacement cannot confuse this test with an intentional live collision.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let names = caller.list_names().await.unwrap();
            if !names.iter().any(|name| {
                name == &info.manifest.bus_name || name.as_str() == "ai.tinyhumans.openhuman.Clock"
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stopped module releases all names");
    drop(first);
    let second = ModuleHost::new(broker);
    second
        .load_file_with_config(&artifact, serde_json::json!({"prefix":"second:"}))
        .unwrap();
    let result = proxy.call::<String>("Now", ()).await;
    assert!(
        result
            .as_ref()
            .is_ok_and(|value| value.starts_with("second:")),
        "same native artifact must initialize a fresh instance after complete detach: {result:?}; state={:?}",
        second.list()
    );
    second.shutdown(Duration::from_secs(3)).await;
    broker_task.abort();
}

// The native entrypoint and broker are real; only its ABI logging callback is
// observed by the test, retaining the SDK's own immutable host context pointer.
static LOGS: OnceLock<Mutex<HashMap<usize, Vec<String>>>> = OnceLock::new();
unsafe extern "C" fn observe_log(ctx: *mut std::ffi::c_void, _: u32, ptr: *const u8, len: usize) {
    let line =
        String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(ptr, len) }).into_owned();
    record_log(LOGS.get(), ctx as usize, line);
}
fn record_log(storage: Option<&Mutex<HashMap<usize, Vec<String>>>>, key: usize, line: String) {
    if let Some(storage) = storage {
        storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key)
            .or_default()
            .push(line);
    }
}
fn recorded_lines(key: usize) -> Vec<String> {
    LOGS.get()
        .and_then(|logs| {
            logs.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&key)
                .cloned()
        })
        .unwrap_or_default()
}
fn observed_native(host: &ModuleHost, path: &Path, prefix: &str) -> usize {
    let artifact = loader::load(path, false).unwrap();
    let manifest = artifact.manifest.clone();
    let admitted = host
        .validate(path, &artifact.descriptor, &manifest)
        .unwrap();
    let config = serde_json::to_vec(&serde_json::json!({"prefix":prefix})).unwrap();
    let (transport, mut vtable) = ModuleTransport::new(admitted.name.clone(), config);
    let key = vtable.host_ctx as usize;
    LOGS.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key, Vec::new());
    vtable.log = observe_log;
    transport.defer_initialize(artifact.init, vtable);
    host.attach_transport(
        path,
        admitted,
        manifest,
        transport,
        Activation {
            lazy_init: true,
            lazy_load: false,
            descriptor_info: None,
            pinned_sha256: None,
            pinned_library_sha256: None,
        },
    )
    .unwrap();
    key
}
#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn native_replacement_logs_and_sanitized_panics_use_only_its_original_host_callback() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "ai.tinyhumans.openhuman.Clock",
            "/ai/tinyhumans/openhuman/Clock",
            "ai.tinyhumans.openhuman.Clock",
        )
        .unwrap();
    let first = ModuleHost::new(broker.clone());
    let first_key = observed_native(&first, &artifact, "first:");
    assert!(
        proxy
            .call::<String>("Now", ())
            .await
            .unwrap()
            .starts_with("first:")
    );
    let first_lines = recorded_lines(first_key);
    assert!(
        first_lines
            .iter()
            .any(|line| line.contains("clock fixture served"))
    );
    first.shutdown(Duration::from_secs(3)).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while caller
            .list_names()
            .await
            .unwrap()
            .iter()
            .any(|name| name.as_str() == "ai.tinyhumans.openhuman.Clock")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(first);
    let first_count = recorded_lines(first_key).len();
    let second = ModuleHost::new(broker);
    let second_key = observed_native(&second, &artifact, "second:");
    assert_ne!(first_key, second_key);
    assert!(
        proxy
            .call::<String>("Now", ())
            .await
            .unwrap()
            .starts_with("second:")
    );
    assert!(proxy.call::<serde_json::Value>("Panic", ()).await.is_err());
    let first_lines = recorded_lines(first_key);
    let second_lines = recorded_lines(second_key);
    second.shutdown(Duration::from_secs(3)).await;
    broker_task.abort();
    assert_eq!(
        first_lines.len(),
        first_count,
        "B must not call A's original logging callback"
    );
    assert!(
        second_lines
            .iter()
            .any(|line| line.contains("clock fixture served"))
    );
    assert!(
        second_lines
            .iter()
            .any(|line| line.contains("module panicked at"))
    );
    assert!(
        second_lines
            .iter()
            .all(|line| !line.contains("secret-token"))
    );
}

#[test]
fn native_log_fixture_recovers_poison_without_unwinding_across_abi() {
    let storage = Mutex::new(HashMap::new());
    let context = 0u8;
    let key = std::ptr::from_ref(&context) as usize;
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = storage.lock().unwrap();
        panic!("controlled Rust fixture poison");
    });
    // Exercise the callback's Rust body first, safely containing baseline RED.
    let recovered = std::panic::catch_unwind(|| record_log(Some(&storage), 1, "local".into()));
    let unavailable = std::panic::catch_unwind(|| record_log(None, 1, "unavailable".into()));
    if recovered.is_ok() {
        let logs = LOGS.get_or_init(|| Mutex::new(HashMap::new()));
        let _ = std::panic::catch_unwind(|| {
            let _guard = logs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            panic!("controlled Rust fixture poison");
        });
        let line = b"actual ABI callback after fixture poison";
        unsafe { observe_log(key as *mut std::ffi::c_void, 1, line.as_ptr(), line.len()) };
    }
    let recorded = LOGS.get().and_then(|logs| {
        logs.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned()
    });
    assert!(poisoned.is_err());
    assert!(
        unavailable.is_ok(),
        "unavailable fixture storage must not unwind"
    );
    assert!(
        recovered.is_ok(),
        "callback body must recover poisoned fixture storage"
    );
    assert!(recorded.is_some_and(|lines| {
        lines
            .iter()
            .any(|line| line.contains("actual ABI callback"))
    }));
}

#[cfg(unix)]
unsafe fn terminal_symbol<T: Copy>(path: &Path, name: &std::ffi::CStr) -> T {
    use std::os::unix::ffi::OsStrExt;
    #[cfg_attr(target_os = "linux", link(name = "dl"))]
    unsafe extern "C" {
        fn dlopen(path: *const std::ffi::c_char, flags: i32) -> *mut std::ffi::c_void;
        fn dlsym(
            handle: *mut std::ffi::c_void,
            name: *const std::ffi::c_char,
        ) -> *mut std::ffi::c_void;
        fn dlclose(handle: *mut std::ffi::c_void) -> i32;
    }
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let handle = unsafe { dlopen(path.as_ptr(), 2) };
    assert!(!handle.is_null());
    let raw = unsafe { dlsym(handle, name.as_ptr()) };
    unsafe {
        dlclose(handle);
    }
    assert!(!raw.is_null());
    assert_eq!(std::mem::size_of::<T>(), std::mem::size_of_val(&raw));
    unsafe { std::mem::transmute_copy(&raw) }
}
#[repr(C)]
#[derive(Clone, Copy)]
struct TerminalInitControls {
    ctx: *mut std::ffi::c_void,
    event: unsafe extern "C" fn(*mut std::ffi::c_void, u32, u32),
}
struct TerminalInitBarrier {
    entered: std::sync::mpsc::Sender<()>,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
}
unsafe extern "C" fn terminal_init_event(ctx: *mut std::ffi::c_void, event: u32, _: u32) {
    if event != 18 {
        return;
    }
    let control = unsafe { &*ctx.cast::<TerminalInitBarrier>() };
    let _ = control.entered.send(());
    let mut release = control.release.0.lock().unwrap();
    while !*release {
        release = control.release.1.wait(release).unwrap();
    }
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the privately pinned __native-fixture-support eight-artifact cohort"]
async fn compiled_native_host_stop_and_fault_deny_factory_after_entered_init_barrier() {
    use crate::module::abi::*;
    let path = PathBuf::from(std::env::var_os("TINYBUS_TEST_TERMINAL_MODULE").unwrap());
    // Real loader admission pins the image before any test symbol resolution.
    let artifact = loader::load(&path, false).unwrap();
    let configure: unsafe extern "C" fn(
        usize,
        *mut std::ffi::c_void,
        unsafe extern "C" fn(*mut std::ffi::c_void, u32, u32),
    ) = unsafe { terminal_symbol(&path, c"terminal_test_factory_mode") };
    let factories: extern "C" fn() -> usize =
        unsafe { terminal_symbol(&path, c"terminal_test_factories") };
    for fault in [false, true] {
        let broker = Broker::new();
        let broker_task = broker.clone().spawn(MemoryBus::new());
        let host = ModuleHost::new(broker);
        let admitted = host
            .validate(&path, &artifact.descriptor, &artifact.manifest)
            .unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let control = Box::into_raw(Box::new(TerminalInitBarrier {
            entered: sender,
            release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        }));
        let controls = TerminalInitControls {
            ctx: control.cast(),
            event: terminal_init_event,
        };
        let encoded = unsafe {
            std::slice::from_raw_parts(
                (&controls as *const TerminalInitControls).cast::<u8>(),
                std::mem::size_of::<TerminalInitControls>(),
            )
        };
        let config =
            serde_json::to_vec(&serde_json::json!({"mode": 0, "test_controls": encoded})).unwrap();
        let (transport, calls) = ModuleTransport::new(admitted.name.clone(), config);
        unsafe {
            configure(30, control.cast(), terminal_init_event);
        }
        let before = factories();
        transport.defer_initialize(artifact.init, calls);
        // Attach the genuine loaded artifact and transport to the real host,
        // matching the existing observed_native deferred-init fixture above.
        host.attach_transport(
            &path,
            admitted,
            artifact.manifest.clone(),
            transport.clone(),
            Activation {
                lazy_init: true,
                lazy_load: false,
                descriptor_info: None,
                pinned_sha256: None,
                pinned_library_sha256: None,
            },
        )
        .unwrap();
        let initialize = {
            let transport = transport.clone();
            tokio::spawn(async move { transport.ensure_initialized().await })
        };
        let entered = receiver.recv_timeout(Duration::from_secs(5)).is_ok();
        if fault {
            unsafe {
                (calls.fault)(calls.host_ctx, std::ptr::null(), 0);
            }
        } else {
            host.shutdown(Duration::from_millis(20)).await;
        }
        let flags = unsafe { calls.native_terminal_state.unwrap()(calls.host_ctx) };
        // Release and await the actual opaque init worker before assertions.
        unsafe {
            *(*control).release.0.lock().unwrap() = true;
            (*control).release.1.notify_all();
        }
        let initialized = tokio::time::timeout(Duration::from_secs(5), initialize)
            .await
            .unwrap();
        host.shutdown(Duration::from_secs(5)).await;
        drop(host);
        let final_flags = unsafe { calls.native_terminal_state.unwrap()(calls.host_ctx) };
        unsafe {
            drop(Box::from_raw(control));
        }
        broker_task.abort();
        let _ = broker_task.await;
        assert!(entered);
        assert!(initialized.is_ok_and(|result| result.is_err()));
        assert_eq!(
            factories(),
            before,
            "terminal authority must deny before the actual factory"
        );
        assert_ne!(
            flags
                & if fault {
                    TB_NATIVE_TERMINAL_FAULT
                } else {
                    TB_NATIVE_TERMINAL_STOP_INTENT
                },
            0
        );
        assert_eq!(final_flags & TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED, 0);
    }
}
