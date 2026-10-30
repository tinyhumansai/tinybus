use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct HostObjectValue(&'static str);

struct PanickingHostObjectName;

struct PanickingHostObjectDrop(Arc<AtomicBool>);

impl Drop for PanickingHostObjectDrop {
    fn drop(&mut self) {
        assert!(
            !self.0.load(Ordering::Acquire),
            "retired callback destructor panic"
        );
    }
}

#[async_trait::async_trait]
impl crate::Interface for HostObjectValue {
    fn name(&self) -> crate::InterfaceName {
        "org.example.ModuleRuntimeHostObject".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Value".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!(self.0))
    }
}

#[async_trait::async_trait]
impl crate::Interface for PanickingHostObjectName {
    fn name(&self) -> crate::InterfaceName {
        panic!("fixture interface name panic")
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Value".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }
}

#[async_trait::async_trait]
impl crate::Interface for PanickingHostObjectDrop {
    fn name(&self) -> crate::InterfaceName {
        "org.example.ModuleRuntimeHostObject".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Value".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!("old"))
    }
}

async fn wait_for_owner(connection: &Connection, name: &str, expected: Option<crate::BusName>) {
    for _ in 0..1_000 {
        if connection.name_owner(name).await.unwrap() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("bus name owner did not reach the expected state");
}

async fn wait_for_host_name_lease_cleanup(runtime: &ModuleRuntime, name: &crate::BusName) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if !runtime.host_objects.lock().await.names.contains_key(name) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host object guard cleanup did not finish");
}

#[tokio::test]
async fn setup_runs_before_the_runtime_is_handed_out() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let setup: RuntimeSetup = Box::new(move |_connection, host| {
        Box::pin(async move {
            // Setup runs before the runtime is handed out, but it must already
            // have access to the loader so it can prepare host services before
            // module initialization starts.
            assert!(host.list().is_empty());
            flag.store(true, Ordering::SeqCst);
            Ok(())
        })
    });
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), Some(setup)).unwrap();
    assert!(ran.load(Ordering::SeqCst));
    assert!(runtime.host().list().is_empty());
    assert!(runtime.connection().unique_name().is_some());
}

#[tokio::test]
async fn runtime_options_set_the_worker_stack_size_and_keep_the_worker_name() {
    let names = RuntimeNames {
        thread: "tinybus-options-runtime".into(),
        worker: "tinybus-options-worker".into(),
    };
    let runtime = ModuleRuntime::start_with_options(
        &names,
        None,
        RuntimeOptions {
            thread_stack_size: Some(4 * 1024 * 1024),
        },
    )
    .unwrap();

    let worker_name = runtime
        .spawn(async { std::thread::current().name().map(str::to_owned) })
        .await
        .unwrap();
    assert_eq!(worker_name.as_deref(), Some(names.worker.as_str()));
}

#[tokio::test]
async fn a_failing_setup_fails_the_start() {
    let ran = Arc::new(AtomicBool::new(false));
    let was_run = ran.clone();
    let setup: RuntimeSetup = Box::new(move |_connection, _host| {
        Box::pin(async move {
            was_run.store(true, Ordering::SeqCst);
            Err(crate::Error::Transport("setup".to_string()))
        })
    });
    let result = ModuleRuntime::start(&RuntimeNames::default(), Some(setup));
    assert!(result.is_err());
    assert!(ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn proxies_validate_names_and_unloaded_modules_do_not_answer() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let proxy = runtime
        .proxy("ai.tinyhumans.test.Nothing", "/ai/tinyhumans/test/Nothing")
        .unwrap();
    let result: crate::Result<serde_json::Value> = proxy.call("Anything", ()).await;
    assert!(result.is_err(), "an unloaded module must not answer");
    assert!(runtime.proxy("not a bus name", "/nope").is_err());
}

#[tokio::test]
async fn work_outlives_the_caller_runtime_and_blocking_reports_errors() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let value = runtime.spawn(async { 7 }).await.unwrap();
    assert_eq!(value, 7);
    assert_eq!(runtime.blocking(|| Ok(())).await, Ok(()));
    assert_eq!(
        runtime.blocking(|| Err("refused".to_string())).await,
        Err("refused".to_string())
    );
    let panicked = runtime.blocking(|| panic!("loader panic")).await;
    assert!(panicked.unwrap_err().contains("did not finish"));
}

#[tokio::test]
async fn host_object_guards_share_names_and_only_remove_their_owned_registration() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.RuntimeHostObject".parse().unwrap();
    let first_path: crate::ObjectPath = "/runtime/first".parse().unwrap();
    let second_path: crate::ObjectPath = "/runtime/second".parse().unwrap();
    let first = runtime
        .serve_host_object(name.clone(), first_path.clone(), HostObjectValue("old"))
        .await
        .unwrap();
    assert!(
        runtime
            .connection()
            .attestation(runtime.connection().unique_name().unwrap())
            .await
            .unwrap()
            .is_none(),
        "ordinary host objects do not attest their connection"
    );
    let second = runtime
        .serve_host_object(name.clone(), second_path.clone(), HostObjectValue("second"))
        .await
        .unwrap();
    let replacement = runtime
        .serve_host_object(name.clone(), first_path.clone(), HostObjectValue("new"))
        .await
        .unwrap();

    drop(first);
    let old_guard_proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            first_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let replaced: serde_json::Value = old_guard_proxy.call("Value", ()).await.unwrap();
    assert_eq!(replaced, serde_json::json!("new"));
    wait_for_owner(
        runtime.connection(),
        name.as_str(),
        runtime.connection().unique_name(),
    )
    .await;

    let remaining_proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            second_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let remaining: serde_json::Value = remaining_proxy.call("Value", ()).await.unwrap();
    assert_eq!(remaining, serde_json::json!("second"));

    runtime
        .connection()
        .serve_at(first_path.clone(), HostObjectValue("direct replacement"))
        .await
        .unwrap();
    drop(replacement);
    let direct_replacement = runtime
        .connection()
        .proxy(
            runtime.connection().unique_name().unwrap().as_str(),
            first_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let value: serde_json::Value = direct_replacement.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("direct replacement"));
    wait_for_owner(
        runtime.connection(),
        name.as_str(),
        runtime.connection().unique_name(),
    )
    .await;

    drop(second);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
    let removed: crate::Result<serde_json::Value> = remaining_proxy.call("Value", ()).await;
    assert!(removed.is_err());
    let direct: serde_json::Value = direct_replacement.call("Value", ()).await.unwrap();
    assert_eq!(direct, serde_json::json!("direct replacement"));
}

#[tokio::test]
async fn replacing_a_host_object_contains_the_retired_callback_drop_panic() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.RuntimeHostObjectDropPanic"
        .parse()
        .unwrap();
    let path: crate::ObjectPath = "/runtime/drop_panic_replacement".parse().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let first = runtime
        .serve_host_object(
            name.clone(),
            path.clone(),
            PanickingHostObjectDrop(armed.clone()),
        )
        .await
        .unwrap();
    armed.store(true, Ordering::Release);

    let replacement = runtime
        .serve_host_object(name.clone(), path.clone(), HostObjectValue("new"))
        .await
        .expect("retiring a panicking callback must preserve the new registration");
    let proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let value: serde_json::Value = proxy.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("new"));

    drop(first);
    let value: serde_json::Value = proxy.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("new"));
    drop(replacement);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
    assert!(proxy.call::<serde_json::Value>("Value", ()).await.is_err());
}

#[tokio::test]
async fn dropping_a_host_object_guard_outside_a_tokio_runtime_releases_its_name() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.DropOutsideRuntime".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/drop_outside".parse().unwrap();
    let guard = runtime
        .serve_host_object(name.clone(), path, HostObjectValue("value"))
        .await
        .unwrap();
    std::thread::spawn(move || drop(guard)).join().unwrap();
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
}

#[tokio::test]
async fn dropping_a_host_object_guard_preserves_a_name_owned_before_registration() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.PreownedHostObject".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/preowned".parse().unwrap();
    runtime
        .connection()
        .request_name(name.as_str())
        .await
        .unwrap();
    let owner = runtime.connection().unique_name();
    let guard = runtime
        .serve_host_object(name.clone(), path, HostObjectValue("value"))
        .await
        .unwrap();
    drop(guard);
    wait_for_owner(runtime.connection(), name.as_str(), owner).await;
}

#[tokio::test]
async fn a_name_collision_leaves_no_host_object_registration_behind() {
    let name: crate::BusName = "ai.tinyhumans.test.HostObjectCollision".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/collision".parse().unwrap();
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let manifest = crate::module::manifest::ModuleManifest {
        schema: crate::module::manifest::MANIFEST_SCHEMA,
        module: crate::module::manifest::ModuleIdentity {
            name: "host-object-collision".to_string(),
            version: crate::version::Version::new(0, 1, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: name.clone(),
        object_path: path.clone(),
        provides: Vec::new(),
        requires: Vec::new(),
        environment: Vec::new(),
        capabilities: Vec::new(),
        lazy_init: true,
        worker_threads: 1,
        on_panic: crate::module::manifest::PanicPolicy::Detach,
    };
    let descriptor = crate::module::abi::TbAbiDescriptor::current("host-object-collision", "0.1.0");
    runtime
        .host()
        .attach_linked_with_config(
            crate::module::LinkedModule {
                descriptor,
                manifest: manifest.clone(),
                init: collision_module_init,
                init_with_setup: None,
            },
            serde_json::Value::Null,
        )
        .unwrap();
    let owner = runtime
        .connection()
        .name_owner(name.as_str())
        .await
        .unwrap();
    assert!(owner.is_some());
    assert!(
        runtime
            .serve_host_object(name.clone(), path.clone(), HostObjectValue("must not leak"))
            .await
            .is_err()
    );
    wait_for_owner(runtime.connection(), name.as_str(), owner).await;
    let proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let leaked: crate::Result<serde_json::Value> = proxy.call("Value", ()).await;
    assert!(leaked.is_err());
}

#[tokio::test]
async fn a_name_claim_race_preserves_the_peer_that_wins_before_registration() {
    let runtime = Arc::new(ModuleRuntime::start(&RuntimeNames::default(), None).unwrap());
    let name: crate::BusName = "ai.tinyhumans.test.HostObjectNameRace".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/name_race".parse().unwrap();
    let barrier = HostObjectRegistrationBarrier {
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    runtime.host_objects.lock().await.before_name_request = Some(barrier.clone());

    let serving_runtime = runtime.clone();
    let serving_name = name.clone();
    let serving_path = path.clone();
    let serving = tokio::spawn(async move {
        serving_runtime
            .serve_host_object(
                serving_name,
                serving_path,
                HostObjectValue("must not register"),
            )
            .await
    });
    let entered =
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.entered.acquire())
            .await
            .expect("host registration did not reach the name-request race")
            .unwrap();

    let manifest = crate::module::manifest::ModuleManifest {
        schema: crate::module::manifest::MANIFEST_SCHEMA,
        module: crate::module::manifest::ModuleIdentity {
            name: "host-object-name-race".to_string(),
            version: crate::version::Version::new(0, 1, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: name.clone(),
        object_path: path.clone(),
        provides: Vec::new(),
        requires: Vec::new(),
        environment: Vec::new(),
        capabilities: Vec::new(),
        lazy_init: true,
        worker_threads: 1,
        on_panic: crate::module::manifest::PanicPolicy::Detach,
    };
    runtime
        .host()
        .attach_linked_with_config(
            crate::module::LinkedModule {
                descriptor: crate::module::abi::TbAbiDescriptor::current(
                    "host-object-name-race",
                    "0.1.0",
                ),
                manifest,
                init: collision_module_init,
                init_with_setup: None,
            },
            serde_json::Value::Null,
        )
        .unwrap();
    let winning_owner = runtime
        .connection()
        .name_owner(name.as_str())
        .await
        .unwrap()
        .expect("the linked peer should win the name claim");

    barrier.release.add_permits(1);
    drop(entered);
    assert!(serving.await.unwrap().is_err());
    runtime.host_objects.lock().await.before_name_request = None;

    assert_eq!(
        runtime
            .connection()
            .name_owner(name.as_str())
            .await
            .unwrap(),
        Some(winning_owner),
        "failed host registration must not release the peer's name"
    );
    assert!(!runtime.host_objects.lock().await.names.contains_key(&name));
    let proxy = runtime
        .connection()
        .proxy(
            runtime.connection().unique_name().unwrap().as_str(),
            path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let leaked: crate::Result<serde_json::Value> = proxy.call("Value", ()).await;
    assert!(
        leaked.is_err(),
        "the failed registration must not publish an object"
    );
}

#[tokio::test]
async fn a_same_connection_name_claim_that_wins_before_registration_is_preserved() {
    let runtime = Arc::new(ModuleRuntime::start(&RuntimeNames::default(), None).unwrap());
    let name: crate::BusName = "ai.tinyhumans.test.SamePeerNameRace".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/same_peer_name_race".parse().unwrap();
    let barrier = HostObjectRegistrationBarrier {
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    runtime.host_objects.lock().await.before_name_request = Some(barrier.clone());

    let serving_runtime = runtime.clone();
    let serving_name = name.clone();
    let serving_path = path.clone();
    let serving = tokio::spawn(async move {
        serving_runtime
            .serve_host_object(serving_name, serving_path, HostObjectValue("direct owner"))
            .await
    });
    let entered =
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.entered.acquire())
            .await
            .expect("host registration did not reach the name-request race")
            .unwrap();

    runtime
        .connection()
        .request_name(name.as_str())
        .await
        .unwrap();
    let owner = runtime.connection().unique_name();
    barrier.release.add_permits(1);
    drop(entered);
    let guard = serving
        .await
        .unwrap()
        .expect("helper registers the interface");
    runtime.host_objects.lock().await.before_name_request = None;

    let proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let value: serde_json::Value = proxy.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("direct owner"));
    drop(guard);
    wait_for_host_name_lease_cleanup(&runtime, &name).await;

    assert_eq!(
        runtime
            .connection()
            .name_owner(name.as_str())
            .await
            .unwrap(),
        owner,
        "dropping a helper guard must preserve an earlier same-peer claim"
    );
    let removed: crate::Result<serde_json::Value> = proxy.call("Value", ()).await;
    assert!(
        removed.is_err(),
        "dropping the guard still removes its object"
    );
    runtime
        .connection()
        .release_name(name.as_str())
        .await
        .unwrap();
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
}

#[tokio::test]
async fn interface_name_panic_rolls_back_a_new_host_object_name_claim() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.PanickingHostObjectName"
        .parse()
        .unwrap();
    let path: crate::ObjectPath = "/runtime/panicking_name".parse().unwrap();

    assert!(
        runtime
            .serve_host_object(name.clone(), path, PanickingHostObjectName)
            .await
            .is_err()
    );
    assert_eq!(
        runtime
            .connection()
            .name_owner(name.as_str())
            .await
            .unwrap(),
        None,
        "failed registration must release the name it just claimed"
    );
}

#[tokio::test]
async fn failed_shared_registration_preserves_the_existing_host_object_guard() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.SharedHostObjectRegistration"
        .parse()
        .unwrap();
    let retained_path: crate::ObjectPath = "/runtime/shared_retained".parse().unwrap();
    let failed_path: crate::ObjectPath = "/runtime/shared_failed".parse().unwrap();
    let owner = runtime.connection().unique_name();
    let retained = runtime
        .serve_host_object(
            name.clone(),
            retained_path.clone(),
            HostObjectValue("retained"),
        )
        .await
        .unwrap();

    assert!(
        runtime
            .serve_host_object(name.clone(), failed_path.clone(), PanickingHostObjectName)
            .await
            .is_err()
    );
    assert_eq!(
        runtime
            .connection()
            .name_owner(name.as_str())
            .await
            .unwrap(),
        owner,
        "failed shared registration must preserve the existing name lease"
    );
    let retained_proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            retained_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let value: serde_json::Value = retained_proxy.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("retained"));

    drop(retained);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
    let failed_proxy = runtime
        .connection()
        .proxy(
            runtime.connection().unique_name().unwrap().as_str(),
            failed_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let leaked: crate::Result<serde_json::Value> = failed_proxy.call("Value", ()).await;
    assert!(leaked.is_err());
}

#[tokio::test]
async fn failed_registration_preserves_a_name_owned_before_the_helper() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.PreownedHostObjectFailure"
        .parse()
        .unwrap();
    let path: crate::ObjectPath = "/runtime/preowned_failure".parse().unwrap();
    runtime
        .connection()
        .request_name(name.as_str())
        .await
        .unwrap();
    let owner = runtime.connection().unique_name();

    assert!(
        runtime
            .serve_host_object(name.clone(), path, PanickingHostObjectName)
            .await
            .is_err()
    );
    assert_eq!(
        runtime
            .connection()
            .name_owner(name.as_str())
            .await
            .unwrap(),
        owner,
        "failed registration must retain the name claimed before the helper"
    );
    assert!(!runtime.host_objects.lock().await.names.contains_key(&name));

    runtime
        .connection()
        .release_name(name.as_str())
        .await
        .unwrap();
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
}

#[tokio::test]
async fn a_closed_runtime_connection_refuses_host_object_registration() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.ClosedHostObjectConnection"
        .parse()
        .unwrap();
    let path: crate::ObjectPath = "/runtime/closed_connection".parse().unwrap();
    runtime.connection().close().await.unwrap();

    assert!(
        runtime
            .serve_host_object(name.clone(), path, HostObjectValue("unreachable"))
            .await
            .is_err()
    );
    assert!(!runtime.host_objects.lock().await.names.contains_key(&name));
}

#[tokio::test]
async fn canceling_after_name_claim_rolls_back_on_the_module_runtime() {
    let runtime = Arc::new(ModuleRuntime::start(&RuntimeNames::default(), None).unwrap());
    let name: crate::BusName = "ai.tinyhumans.test.CancelAfterClaim".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/cancel_after_claim".parse().unwrap();
    let barrier = HostObjectRegistrationBarrier {
        entered: Arc::new(tokio::sync::Semaphore::new(0)),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    {
        let mut state = runtime.host_objects.lock().await;
        state.after_name_claim = Some(barrier.clone());
    }

    let serving_runtime = runtime.clone();
    let serving_name = name.clone();
    let serving_path = path.clone();
    let serving = tokio::spawn(async move {
        serving_runtime
            .serve_host_object(serving_name, serving_path, HostObjectValue("value"))
            .await
    });
    let entered =
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.entered.acquire())
            .await
            .expect("name claim was not reached")
            .unwrap();
    let owner = runtime
        .connection()
        .name_owner(name.as_str())
        .await
        .unwrap();
    assert_eq!(owner, runtime.connection().unique_name());

    serving.abort();
    assert!(matches!(serving.await, Err(error) if error.is_cancelled()));
    drop(entered);
    barrier.release.add_permits(1);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
    runtime.host_objects.lock().await.after_name_claim = None;

    let guard = runtime
        .serve_host_object(name.clone(), path, HostObjectValue("reclaimed"))
        .await
        .unwrap();
    drop(guard);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
}

unsafe extern "C" fn collision_module_init(
    _: *const crate::module::abi::TbHostVtable,
    _: *mut crate::module::abi::TbModuleVtable,
) -> i32 {
    crate::module::abi::TB_BAD_ARGUMENT
}

#[tokio::test]
async fn configured_worker_stack_supports_deep_synchronous_work() {
    const CHILD: &str = "TINYBUS_CONFIGURED_STACK_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = ModuleRuntime::start_with_options(
            &RuntimeNames::default(),
            None,
            RuntimeOptions {
                thread_stack_size: Some(4 * 1024 * 1024),
            },
        )
        .unwrap();
        let result = runtime
            .spawn(async { consume_worker_stack(96) })
            .await
            .unwrap();
        assert_ne!(result, 0);
        return;
    }

    // A stack overflow aborts the process, so isolate the regression check in
    // a child test process. Without the configured 4 MiB Tokio worker stack,
    // this 3 MiB recursive workload overflows Tokio's default worker stack.
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "module::runtime::tests::configured_worker_stack_supports_deep_synchronous_work",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "configured-stack child exited with {status}"
    );
}

#[inline(never)]
fn consume_worker_stack(depth: usize) -> usize {
    const FRAME_BYTES: usize = 32 * 1024;
    let frame = [depth as u8; FRAME_BYTES];
    std::hint::black_box(&frame);
    let value = if depth == 0 {
        0
    } else {
        consume_worker_stack(depth - 1)
    };
    value + usize::from(frame[depth % FRAME_BYTES])
}
