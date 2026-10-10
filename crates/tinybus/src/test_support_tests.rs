#![allow(clippy::expect_used, clippy::unwrap_used)]

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

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_MODULE_TWO to point at the built clock fixture"]
async fn the_shared_helper_loads_an_allowlisted_module_and_calls_it() {
    let (host, client, broker_task) = start_bus().await.unwrap();
    let module = admit_module(&host, "TINYBUS_TEST_MODULE_TWO", "module-clock-two").unwrap();
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
