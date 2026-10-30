use super::{ReleaseAsset, ReleasePlan, load_first_admitted};
use crate::broker::Broker;
use crate::module::{ModuleHost, artifact_dir};

const ASSET: ReleaseAsset<'static> = ReleaseAsset {
    host_key: "ubuntu-24.04-x86_64",
    archive: "demo-ubuntu-24.04-x86_64.tar.gz",
    sha256: "0000000000000000000000000000000000000000000000000000000000000000",
    library_sha256: None,
};

fn plan<'a>(
    assets: &'a [ReleaseAsset<'a>],
    install_root: &'a std::path::Path,
    bundled_root: Option<&'a std::path::Path>,
) -> ReleasePlan<'a> {
    ReleasePlan {
        id: "demo",
        version: "1.0.0",
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        assets,
        install_root,
        bundled_root,
        allow_download: false,
    }
}

#[test]
fn no_candidates_means_unavailable_on_this_platform() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let error = load_first_admitted(&host, &plan(&[], root.path(), None), &serde_json::json!({}))
        .unwrap_err();
    assert!(error.contains("not available for this platform"), "{error}");
}

#[test]
fn an_invalid_installer_bundle_is_reported_without_falling_back_to_the_cache() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(ASSET.archive), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("repairing the installation"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn an_absent_installer_bundle_uses_the_release_cache_path() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn an_unbuildable_cache_path_falls_through_then_reports_the_miss() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, root.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}

#[test]
fn an_unbuildable_bundle_path_is_skipped_and_the_release_cache_is_used() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn a_terminal_failure_with_downloads_enabled_says_to_restart() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let mut release = plan(&assets, root.path(), None);
    release.allow_download = true;
    let error = load_first_admitted(&host, &release, &serde_json::json!({})).unwrap_err();
    assert!(error.contains("could not be loaded"), "{error}");
    assert!(error.contains("restart the app"), "{error}");
}

#[test]
fn a_bundle_marker_that_does_not_match_the_pin_is_refused_without_falling_back() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", "f".repeat(64)),
    )
    .unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("does not match the pin"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn a_bundle_marker_is_never_honoured_from_the_download_cache() {
    let host = ModuleHost::new(Broker::new());
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(user_cache.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", ASSET.sha256),
    )
    .unwrap();
    std::fs::write(dir.join("libdemo_module.so"), b"").unwrap();
    std::fs::write(dir.join("libdemo_module.dylib"), b"").unwrap();
    std::fs::write(dir.join("demo_module.dll"), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}

#[derive(Clone)]
struct TestCallback(crate::BusName);

#[async_trait::async_trait]
impl crate::Interface for TestCallback {
    fn name(&self) -> crate::InterfaceName {
        "org.example.Callback".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Observe".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!(self.0))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_callback_resolution_uses_a_fresh_binding_for_each_rejected_candidate() {
    use super::load_first_admitted_with_host_callbacks;
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::{Arc, Mutex};

    let Some(fixture) =
        std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").map(std::path::PathBuf::from)
    else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(root.path()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "tinybus-callback-release-test".into(),
                worker: "tinybus-callback-release-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let record = Box::leak(Box::new(crate::module::ReleaseRecord {
        id: "tinybus",
        bus_name: "org.example.EagerCallback",
        object_path: "/org/example/EagerCallback",
        version: "0.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
        assets: &[],
    }));

    let mut assets = Vec::new();
    for (host_key, archive_name, bytes) in [
        ("candidate-one", "one.tar.gz", b"archive-one".as_slice()),
        ("candidate-two", "two.tar.gz", b"archive-two".as_slice()),
    ] {
        let dir = artifact_dir(&root, record.id, record.version, host_key).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(archive_name), bytes).unwrap();
        let digest =
            crate::module::hash::file_hex(std::fs::File::open(dir.join(archive_name)).unwrap())
                .unwrap();
        let module = dir.join(format!(
            "libcandidate.{}",
            crate::module::cache::library_extension()
        ));
        if host_key == "candidate-one" {
            std::fs::write(&module, b"not a loadable module").unwrap();
        } else {
            std::fs::copy(&fixture, &module).unwrap();
            let library_hash =
                crate::module::hash::file_hex(std::fs::File::open(&module).unwrap()).unwrap();
            std::fs::write(
                dir.join("modules.toml"),
                format!(
                    "{:?} = {library_hash:?}\n",
                    module.file_name().unwrap().to_str().unwrap()
                ),
            )
            .unwrap();
        }
        assets.push(ReleaseAsset {
            host_key: Box::leak(host_key.to_owned().into_boxed_str()),
            archive: Box::leak(archive_name.to_owned().into_boxed_str()),
            sha256: Box::leak(digest.into_boxed_str()),
            library_sha256: None,
        });
    }

    let plan = ReleasePlan {
        id: record.id,
        version: record.version,
        release_url: record.release_url,
        assets: &assets,
        install_root: &root,
        bundled_root: None,
        allow_download: false,
    };
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let factory_runtime = Arc::clone(&runtime);
    let factory_attempts = Arc::clone(&attempts);
    let prepare = move |_attempt: usize| {
        let runtime = Arc::clone(&factory_runtime);
        let attempts = Arc::clone(&factory_attempts);
        async move {
            let binding = Arc::new(
                runtime
                    .host()
                    .prepare_host_callbacks(vec![HostCallbackEndpoint {
                        path: "/callback".parse().unwrap(),
                        interface: "org.example.Callback".parse().unwrap(),
                        members: vec!["Observe".parse().unwrap()],
                    }])
                    .await
                    .map_err(|error| error.to_string())?,
            );
            binding
                .serve_at(
                    "/callback".parse().unwrap(),
                    TestCallback(binding.unique_name().clone()),
                )
                .await
                .map_err(|error| error.to_string())?;
            attempts.lock().unwrap().push(Arc::clone(&binding));
            let config = serde_json::json!({ "host": binding.unique_name().as_str() });
            Ok((binding, config))
        }
    };

    let host = runtime.host();
    let (_info, selected) = load_first_admitted_with_host_callbacks(
        host,
        &plan,
        prepare,
        record.bus_name,
        record.object_path,
    )
    .await
    .unwrap();
    let bindings = attempts.lock().unwrap();
    assert_eq!(bindings.len(), 2);
    assert!(!host.retains_host_callback_binding(record.bus_name, &bindings[0]));
    assert!(host.retains_host_callback_binding(record.bus_name, &selected));
    assert!(Arc::ptr_eq(&bindings[1], &selected));
}

#[tokio::test]
async fn callback_release_loader_reports_empty_candidates_without_preparing_authority() {
    let runtime = crate::module::ModuleRuntime::start(
        &crate::module::RuntimeNames {
            thread: "empty-callback-candidate-test".into(),
            worker: "empty-callback-candidate-worker".into(),
        },
        None,
    )
    .unwrap();
    let plan = ReleasePlan {
        assets: &[],
        ..plan(&[], std::path::Path::new("."), None)
    };
    let mut attempts = 0;
    let error = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        |_| {
            attempts += 1;
            async { Err("must not prepare an empty candidate".into()) }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    .err()
    .expect("the missing archive cannot be admitted");
    assert!(error.contains("not available for this platform"));
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn callback_release_loader_skips_unbuildable_bundle_and_cache_paths() {
    let runtime = crate::module::ModuleRuntime::start(
        &crate::module::RuntimeNames {
            thread: "callback-unbuildable-path-test".into(),
            worker: "callback-unbuildable-path-worker".into(),
        },
        None,
    )
    .unwrap();
    let bundle = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let plan = ReleasePlan {
        bundled_root: Some(bundle.path()),
        ..plan(&assets, cache.path(), Some(bundle.path()))
    };
    let mut attempts = 0;
    let error = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        |_| {
            attempts += 1;
            async { Err("no candidate should prepare authority".into()) }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    .err()
    .expect("unbuildable candidates should be rejected");

    assert!(error.contains("downloads are disabled"), "{error}");
    assert_eq!(attempts, 0);

    let mut downloadable = plan;
    downloadable.allow_download = true;
    let Err(error) = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &downloadable,
        |_| async { Err("unbuildable paths must skip the factory".into()) },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    else {
        panic!("invalid installer marker should be rejected");
    };
    assert!(error.contains("restart the app"), "{error}");
}

#[tokio::test]
async fn callback_preparation_failure_stops_before_trying_another_candidate() {
    let runtime = crate::module::ModuleRuntime::start(
        &crate::module::RuntimeNames {
            thread: "callback-prepare-failure-test".into(),
            worker: "callback-prepare-failure-worker".into(),
        },
        None,
    )
    .unwrap();
    let bundled = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let assets = [
        ReleaseAsset {
            host_key: "candidate-one",
            archive: "one.tar.gz",
            ..ASSET
        },
        ReleaseAsset {
            host_key: "candidate-two",
            archive: "two.tar.gz",
            ..ASSET
        },
    ];
    let first_dir = artifact_dir(bundled.path(), "demo", "1.0.0", assets[0].host_key).unwrap();
    std::fs::create_dir_all(&first_dir).unwrap();
    std::fs::write(first_dir.join(assets[0].archive), b"candidate archive").unwrap();
    let plan = ReleasePlan {
        bundled_root: Some(bundled.path()),
        ..plan(&assets, cache.path(), Some(bundled.path()))
    };
    let mut attempts = Vec::new();

    let Err(error) = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        |attempt| {
            attempts.push(attempt);
            async { Err("callback authority could not be prepared".to_owned()) }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    else {
        panic!("callback preparation failure must stop resolution");
    };

    assert_eq!(error, "callback authority could not be prepared");
    assert_eq!(attempts, [0]);
    assert!(!cache.path().join("demo").exists());
}

#[tokio::test]
async fn callback_release_loader_skips_bundle_candidates_without_an_archive_or_marker() {
    let runtime = crate::module::ModuleRuntime::start(
        &crate::module::RuntimeNames {
            thread: "callback-empty-bundle-candidate-test".into(),
            worker: "callback-empty-bundle-candidate-worker".into(),
        },
        None,
    )
    .unwrap();
    let bundle = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let assets = [ASSET];
    let plan = plan(&assets, cache.path(), Some(bundle.path()));
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let prepare_attempts = std::sync::Arc::clone(&attempts);
    let host = runtime.host();
    let Err(error) = super::load_first_admitted_with_host_callbacks(
        host,
        &plan,
        move |_| {
            let attempts = std::sync::Arc::clone(&prepare_attempts);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let binding = std::sync::Arc::new(
                    host.prepare_host_callbacks(vec![crate::module::HostCallbackEndpoint {
                        path: "/callback".parse().unwrap(),
                        interface: "org.example.Callback".parse().unwrap(),
                        members: vec!["Observe".parse().unwrap()],
                    }])
                    .await
                    .unwrap(),
                );
                Ok((binding, serde_json::json!({})))
            }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    else {
        panic!("an uncached candidate should not be admitted");
    };

    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn callback_release_loader_refuses_a_present_bundle_with_an_invalid_marker() {
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::Arc;

    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "callback-invalid-marker-test".into(),
                worker: "callback-invalid-marker-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let bundled = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let directory = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", "f".repeat(64)),
    )
    .unwrap();
    let assets = [ASSET];
    let plan = plan(&assets, cache.path(), Some(bundled.path()));
    let for_prepare = Arc::clone(&runtime);
    let Err(error) = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        move |_| {
            let runtime = Arc::clone(&for_prepare);
            async move {
                let binding = Arc::new(
                    runtime
                        .host()
                        .prepare_host_callbacks(vec![HostCallbackEndpoint {
                            path: "/callback".parse().unwrap(),
                            interface: "org.example.Callback".parse().unwrap(),
                            members: vec!["Observe".parse().unwrap()],
                        }])
                        .await
                        .map_err(|error| error.to_string())?,
                );
                Ok((binding, serde_json::json!({})))
            }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    else {
        panic!("invalid installer marker should be rejected");
    };

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("does not match the pin"), "{error}");
    assert!(!cache.path().join("demo").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_release_loader_admits_a_pinned_installer_module_and_retains_its_binding() {
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::Arc;

    let Some(fixture) =
        std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").map(std::path::PathBuf::from)
    else {
        return;
    };
    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "callback-bundled-admission-test".into(),
                worker: "callback-bundled-admission-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let root = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(root.path()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let asset = ReleaseAsset {
        host_key: ASSET.host_key,
        archive: "tinybus-callback.tar.gz",
        sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        library_sha256: None,
    };
    let directory = artifact_dir(&root, "tinybus", "0.0.0", asset.host_key).unwrap();
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(format!("{}.sha256", asset.archive)),
        format!("{}\n", asset.sha256),
    )
    .unwrap();
    let module = directory.join(format!(
        "libcallback.{}",
        crate::module::cache::library_extension()
    ));
    std::fs::copy(&fixture, &module).unwrap();
    let library_hash =
        crate::module::hash::file_hex(std::fs::File::open(&module).unwrap()).unwrap();
    std::fs::write(
        directory.join("modules.toml"),
        format!(
            "{:?} = {library_hash:?}\n",
            module.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();
    let assets = [asset];
    let plan = ReleasePlan {
        id: "tinybus",
        version: "0.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
        assets: &assets,
        install_root: &root,
        bundled_root: Some(&root),
        allow_download: false,
    };
    let factory_runtime = Arc::clone(&runtime);
    let (info, selected) = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        move |_| {
            let runtime = Arc::clone(&factory_runtime);
            async move {
                let binding = Arc::new(
                    runtime
                        .host()
                        .prepare_host_callbacks(vec![HostCallbackEndpoint {
                            path: "/callback".parse().unwrap(),
                            interface: "org.example.Callback".parse().unwrap(),
                            members: vec!["Observe".parse().unwrap()],
                        }])
                        .await
                        .map_err(|error| error.to_string())?,
                );
                binding
                    .serve_at(
                        "/callback".parse().unwrap(),
                        TestCallback(binding.unique_name().clone()),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let config = serde_json::json!({ "host": binding.unique_name().as_str() });
                Ok((binding, config))
            }
        },
        "org.example.EagerCallback",
        "/org/example/EagerCallback",
    )
    .await
    .unwrap();

    assert_eq!(info.manifest.bus_name.as_str(), "org.example.EagerCallback");
    assert!(
        runtime
            .host()
            .retains_host_callback_binding("org.example.EagerCallback", &selected)
    );
}

#[tokio::test]
async fn callback_release_loader_reports_uncached_candidate_when_downloads_are_disabled() {
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct Callback;
    #[async_trait::async_trait]
    impl crate::Interface for Callback {
        fn name(&self) -> crate::InterfaceName {
            "org.example.Callback".parse().unwrap()
        }
        fn members(&self) -> Vec<crate::MemberName> {
            vec!["Observe".parse().unwrap()]
        }
        async fn call(
            &self,
            _: &crate::MemberName,
            _: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "uncached-callback-candidate-test".into(),
                worker: "uncached-callback-candidate-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let root = tempfile::tempdir().unwrap();
    let assets = [ASSET];
    let plan = plan(&assets, root.path(), None);
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime_for_prepare = Arc::clone(&runtime);
    let attempts_for_prepare = Arc::clone(&attempts);
    let error = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        move |_| {
            let runtime = Arc::clone(&runtime_for_prepare);
            attempts_for_prepare.fetch_add(1, Ordering::SeqCst);
            async move {
                let binding = Arc::new(
                    runtime
                        .host()
                        .prepare_host_callbacks(vec![HostCallbackEndpoint {
                            path: "/callback".parse().unwrap(),
                            interface: "org.example.Callback".parse().unwrap(),
                            members: vec!["Observe".parse().unwrap()],
                        }])
                        .await
                        .map_err(|error| error.to_string())?,
                );
                binding
                    .serve_at("/callback".parse().unwrap(), Callback)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok((binding, serde_json::json!({})))
            }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    .err()
    .expect("a corrupt installer archive cannot be admitted");

    assert!(error.contains("downloads are disabled"), "{error}");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn callback_release_loader_rejects_a_present_invalid_bundle_without_using_user_cache() {
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::Arc;

    #[derive(Clone)]
    struct Callback;
    #[async_trait::async_trait]
    impl crate::Interface for Callback {
        fn name(&self) -> crate::InterfaceName {
            "org.example.Callback".parse().unwrap()
        }
        fn members(&self) -> Vec<crate::MemberName> {
            vec!["Observe".parse().unwrap()]
        }
        async fn call(
            &self,
            _: &crate::MemberName,
            _: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "invalid-bundle-callback-test".into(),
                worker: "invalid-bundle-callback-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(ASSET.archive), b"not a module archive").unwrap();
    let assets = [ASSET];
    let plan = plan(&assets, user_cache.path(), Some(bundled.path()));
    let runtime_for_prepare = Arc::clone(&runtime);
    let error = super::load_first_admitted_with_host_callbacks(
        runtime.host(),
        &plan,
        move |_| {
            let runtime = Arc::clone(&runtime_for_prepare);
            async move {
                let binding = Arc::new(
                    runtime
                        .host()
                        .prepare_host_callbacks(vec![HostCallbackEndpoint {
                            path: "/callback".parse().unwrap(),
                            interface: "org.example.Callback".parse().unwrap(),
                            members: vec!["Observe".parse().unwrap()],
                        }])
                        .await
                        .map_err(|error| error.to_string())?,
                );
                binding
                    .serve_at("/callback".parse().unwrap(), Callback)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok((binding, serde_json::json!({})))
            }
        },
        "org.example.Demo",
        "/org/example/Demo",
    )
    .await
    .err()
    .expect("a corrupt installer archive cannot be admitted");

    assert!(error.contains("installer bundle"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

/// A marker-style bundle entry whose library and allowlist agree, so nothing
/// about its bytes is refused before the loader reaches the directory gate.
#[cfg(unix)]
fn marked_bundle(bundled: &std::path::Path) -> std::path::PathBuf {
    let dir = artifact_dir(bundled, "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    use std::os::unix::fs::PermissionsExt;
    for path in dir.ancestors().take_while(|path| path.starts_with(bundled)) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", ASSET.sha256),
    )
    .unwrap();
    let library = format!(
        "libdemo_module.{}",
        crate::module::cache::library_extension()
    );
    std::fs::write(dir.join(&library), b"not a real library").unwrap();
    let sha = crate::module::sha256_file(dir.join(&library)).unwrap();
    std::fs::write(
        dir.join("modules.toml"),
        format!("\"{library}\" = \"{sha}\"\n"),
    )
    .unwrap();
    dir
}

#[cfg(unix)]
#[test]
fn a_bundle_refused_only_for_its_location_falls_back_to_the_release_cache() {
    use std::os::unix::fs::PermissionsExt;

    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = marked_bundle(bundled.path());
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    // The release cache was consulted: with downloads off, that is a miss.
    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[cfg(unix)]
#[test]
fn a_bundle_refused_for_its_content_is_still_authoritative() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    // A private directory: the gate admits the location, then the loader
    // refuses bytes that are not a library.
    marked_bundle(bundled.path());

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[cfg(unix)]
#[test]
fn the_release_cache_is_repaired_to_owner_only_write_before_it_is_used() {
    use std::os::unix::fs::PermissionsExt;

    let host = ModuleHost::new(Broker::new());
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(user_cache.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    // What an earlier launch under umask 002 left behind.
    for directory in [user_cache.path().to_path_buf(), dir.clone()] {
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o775)).unwrap();
    }

    let assets = [ASSET];
    let _ = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), None),
        &serde_json::json!({}),
    );

    for directory in [user_cache.path().to_path_buf(), dir] {
        let mode = std::fs::metadata(&directory).unwrap().permissions().mode();
        assert_eq!(mode & 0o022, 0, "{} is {mode:o}", directory.display());
    }
}
