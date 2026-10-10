use super::*;

#[test]
fn resolves_a_crate_name_to_the_current_platform_library_filename() {
    let path = artifact_path("target/debug", "tiny-docs-module");
    let expected = if cfg!(target_os = "windows") {
        "tiny_docs_module.dll"
    } else if cfg!(target_os = "macos") {
        "libtiny_docs_module.dylib"
    } else {
        "libtiny_docs_module.so"
    };
    assert_eq!(path, PathBuf::from("target/debug").join(expected));
}

#[test]
fn checks_the_adjacent_modules_toml_digest_before_loading() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = artifact_path(directory.path(), "clock-module");
    std::fs::write(&artifact, b"fixture bytes").unwrap();
    let digest = sha256_file(&artifact).unwrap();
    let file_name = artifact.file_name().unwrap().to_str().unwrap();
    let manifest = directory.path().join("modules.toml");

    std::fs::write(&manifest, format!("\"{file_name}\" = \"{digest}\"\n")).unwrap();
    verify_modules_pin(&artifact).unwrap();

    std::fs::write(
        &manifest,
        format!("\"{file_name}\" = \"{}\"\n", "0".repeat(64)),
    )
    .unwrap();
    assert!(verify_modules_pin(&artifact).is_err());
}

#[test]
fn rejects_a_missing_or_unpinned_module_artifact() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = artifact_path(directory.path(), "clock-module");
    std::fs::write(&artifact, b"fixture bytes").unwrap();

    assert!(verify_modules_pin(&artifact).is_err());
    let host = ModuleHost::new(Broker::new());
    let load_state = AtomicU8::new(MODULE_UNLOADED);
    assert!(admit_artifact(&host, &artifact, "clock-module", &load_state).is_err());
    assert!(LoadReservation::reserve(&load_state).is_ok());
    std::fs::write(
        directory.path().join("modules.toml"),
        "\"another.so\" = \"abc\"\n",
    )
    .unwrap();
    assert!(verify_modules_pin(&artifact).is_err());
}

#[test]
fn requires_a_dedicated_module_artifact_directory() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = artifact_path(directory.path(), "clock-module");
    std::fs::write(&artifact, b"fixture bytes").unwrap();
    assert!(verify_single_artifact(directory.path(), &artifact).is_ok());

    let other = artifact_path(directory.path(), "another-module");
    std::fs::write(&other, b"another fixture").unwrap();
    assert!(verify_single_artifact(directory.path(), &artifact).is_err());
    let file_name = artifact.file_name().unwrap().to_str().unwrap();
    let digest = sha256_file(&artifact).unwrap();
    std::fs::write(
        directory.path().join("modules.toml"),
        format!("\"{file_name}\" = \"{digest}\"\n"),
    )
    .unwrap();
    let load_state = AtomicU8::new(MODULE_UNLOADED);
    let host = ModuleHost::new(Broker::new());
    assert!(admit_artifact(&host, &artifact, "clock-module", &load_state).is_err());
    assert!(LoadReservation::reserve(&load_state).is_ok());
}

#[test]
fn a_rejected_library_consumes_the_loader_attempt() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = artifact_path(directory.path(), "not-a-module");
    std::fs::write(&artifact, b"not a dynamic library").unwrap();
    let file_name = artifact.file_name().unwrap().to_str().unwrap();
    let digest = sha256_file(&artifact).unwrap();
    std::fs::write(
        directory.path().join("modules.toml"),
        format!("\"{file_name}\" = \"{digest}\"\n"),
    )
    .unwrap();

    let host = ModuleHost::new(Broker::new());
    let load_state = AtomicU8::new(MODULE_UNLOADED);
    assert!(admit_artifact(&host, &artifact, "not-a-module", &load_state).is_err());
    assert_eq!(load_state.load(Ordering::Acquire), MODULE_LOAD_CONSUMED);
    assert!(admit_artifact(&host, &artifact, "not-a-module", &load_state).is_err());
}

#[tokio::test]
async fn waits_report_missing_module_and_timeout_errors() {
    let (host, client, broker_task) = start_bus().await.unwrap();
    assert!(
        wait_until_idle(&host, "missing-module", Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(
        wait_until_serving(&client, "missing.service", Duration::from_millis(1))
            .await
            .is_err()
    );
    broker_task.abort();
}

#[test]
fn missing_module_environment_variable_is_reported_before_reserving_the_load_slot() {
    let env_var = format!("TINYBUS_TEST_SUPPORT_MISSING_{}", std::process::id());
    // SAFETY: the pid-specific variable is private to this test and is not used
    // by any other test or process environment consumer.
    unsafe { std::env::remove_var(&env_var) };
    let host = ModuleHost::new(Broker::new());
    assert!(admit_module(&host, &env_var, "missing").is_err());
    assert_eq!(MODULE_LOAD_STATE.load(Ordering::Acquire), MODULE_UNLOADED);
}

#[test]
fn an_uncommitted_load_reservation_can_be_retried() {
    let state = AtomicU8::new(MODULE_UNLOADED);
    {
        let _reservation = LoadReservation::reserve(&state).unwrap();
        assert_eq!(state.load(Ordering::Acquire), MODULE_LOADING);
    }
    assert_eq!(state.load(Ordering::Acquire), MODULE_UNLOADED);

    let mut reservation = LoadReservation::reserve(&state).unwrap();
    reservation.commit();
    assert_eq!(state.load(Ordering::Acquire), MODULE_LOAD_CONSUMED);
    assert!(LoadReservation::reserve(&state).is_err());
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE_TWO to point at the built clock fixture"]
async fn the_shared_helper_loads_an_allowlisted_module_and_calls_it() {
    let (host, client, broker_task) = start_bus().await.unwrap();
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE_TWO").unwrap());
    let load_state = AtomicU8::new(MODULE_UNLOADED);
    assert!(admit_artifact(&host, &artifact, "unexpected-module-name", &load_state).is_err());
    assert_eq!(load_state.load(Ordering::Acquire), MODULE_LOAD_CONSUMED);
    assert!(
        host.list().iter().all(|module| {
            module.name != "module-clock-two"
                || matches!(module.state, ModuleState::Rejected { .. })
        }),
        "mismatched identity must not be registered"
    );
    let services = client.list_names().await.unwrap();
    assert!(
        !services
            .iter()
            .any(|name| name.as_str() == "ai.tinyhumans.openhuman.SecondClock"),
        "mismatched module service was exposed: {services:?}"
    );

    let load_state = AtomicU8::new(MODULE_UNLOADED);
    let module = admit_artifact(&host, &artifact, "module-clock-two", &load_state).unwrap();
    assert_eq!(module.manifest.module.name, "module-clock-two");

    let name = "ai.tinyhumans.openhuman.SecondClock";
    wait_until_serving(&client, name, Duration::from_secs(5))
        .await
        .unwrap();
    let proxy = client
        .proxy(name, "/ai/tinyhumans/openhuman/SecondClock", name)
        .unwrap();
    let identity: String = call(&proxy, "Identify", ()).await.unwrap();
    assert_eq!(identity, "second-clock");
    wait_until_idle(&host, "module-clock-two", Duration::from_secs(5))
        .await
        .unwrap();
    broker_task.abort();
}
