use super::*;
use crate::module::manifest::{
    Dependency, MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy, ProvidedInterface,
};
use crate::{BusName, InterfaceName, InterfaceVersion, ObjectPath, Version};

#[tokio::test]
async fn resolver_awaits_async_module_configuration_only_for_the_requested_module() {
    use super::test_support::{Recording, record, shared_runtime};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let record = record("resolver_async_module_config");
    let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
    let async_calls = Arc::new(AtomicUsize::new(0));
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let async_counter = Arc::clone(&async_calls);
    let sync_counter = Arc::clone(&sync_calls);
    let async_module_config: AsyncModuleConfigProvider = Arc::new(move |id| {
        let counter = Arc::clone(&async_counter);
        let id = id.to_owned();
        Box::pin(async move {
            assert_eq!(id, "resolver_async_module_config");
            counter.fetch_add(1, Ordering::SeqCst);
            serde_json::json!({ "loaded_async": true })
        })
    });
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources,
        linked: Arc::new(LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![record],
        terminal_marker: String::new(),
        runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
        module_config: Arc::new(move |_| {
            sync_counter.fetch_add(1, Ordering::SeqCst);
            serde_json::json!({ "loaded_sync": true })
        }),
        async_module_config: Some(async_module_config),
        reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
    });

    let error = resolver.ensure_loaded(record.id).await.unwrap_err();
    assert!(error.contains("linked factory"), "{error}");
    assert_eq!(async_calls.load(Ordering::SeqCst), 1);
    assert_eq!(sync_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn reconfiguration_classifies_only_closed_lifecycle_wire_names() {
    let cases = [
        (
            crate::Error::ModuleUnavailable {
                module: "ignored".into(),
                state: "initializing".into(),
                detail: "arbitrary detail is ignored".into(),
            },
            crate::module::ReconfigureFailure::Initializing,
        ),
        (
            crate::Error::MethodFailed {
                name: crate::Error::MODULE_UNAVAILABLE.into(),
                message: "arbitrary remote prose is ignored".into(),
            },
            crate::module::ReconfigureFailure::Unavailable,
        ),
        (
            crate::Error::MethodFailed {
                name: "ai.tinyhumans.tinybus.Error.Other".into(),
                message: "module initializing".into(),
            },
            crate::module::ReconfigureFailure::Failed,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(access::reconfigure_failure(&error), expected);
    }
}

fn module(name: &str, provides: &[&str], requires: &[(&str, bool)]) -> ModuleManifest {
    let version = Version::new(1, 0, 0);
    ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: name.to_string(),
            version: version.clone(),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: BusName::new(format!("ai.tinyhumans.module.{name}")).unwrap(),
        object_path: ObjectPath::new(format!("/ai/tinyhumans/module/{name}")).unwrap(),
        provides: provides
            .iter()
            .map(|name| ProvidedInterface {
                version: InterfaceVersion::provided(
                    InterfaceName::new(*name).unwrap(),
                    version.clone(),
                ),
                methods: Vec::new(),
                signals: Vec::new(),
            })
            .collect(),
        requires: requires
            .iter()
            .map(|(name, optional)| Dependency {
                interface: InterfaceVersion::consumed(
                    InterfaceName::new(*name).unwrap(),
                    version.clone(),
                ),
                optional: *optional,
                reason: String::new(),
            })
            .collect(),
        environment: Vec::new(),
        capabilities: Vec::new(),
        lazy_init: false,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    }
}

#[test]
fn a_required_dependency_no_module_provides_leaves_the_module_unresolved_and_names_the_interface() {
    let manifests = [module(
        "Consumer",
        &[],
        &[("ai.tinyhumans.module.Missing", false)],
    )];
    let result = resolve(&manifests, &HashSet::new());
    assert!(result.order.is_empty());
    assert!(
        result.unresolved[0]
            .1
            .contains("ai.tinyhumans.module.Missing")
    );
}

#[test]
fn an_optional_dependency_that_is_missing_still_resolves() {
    let manifests = [module(
        "Consumer",
        &[],
        &[("ai.tinyhumans.module.Missing", true)],
    )];
    assert_eq!(resolve(&manifests, &HashSet::new()).order, [0]);
}

#[test]
fn a_dependency_cycle_is_reported_rather_than_loaded() {
    let manifests = [
        module(
            "One",
            &["ai.tinyhumans.module.One"],
            &[("ai.tinyhumans.module.Two", false)],
        ),
        module(
            "Two",
            &["ai.tinyhumans.module.Two"],
            &[("ai.tinyhumans.module.One", false)],
        ),
    ];
    let result = resolve(&manifests, &HashSet::new());
    assert!(result.order.is_empty());
    assert!(
        result
            .unresolved
            .iter()
            .all(|(_, reason)| reason == "module dependency cycle detected")
    );
}

#[test]
fn a_module_blocked_behind_an_unresolved_provider_is_not_reported_as_a_cycle() {
    let manifests = [
        module("Consumer", &[], &[("ai.tinyhumans.module.Provider", false)]),
        module(
            "Provider",
            &["ai.tinyhumans.module.Provider"],
            &[("ai.tinyhumans.module.Missing", false)],
        ),
    ];
    let result = resolve(&manifests, &HashSet::new());
    assert!(result.order.is_empty());
    assert!(
        result
            .unresolved
            .iter()
            .all(|(_, reason)| reason.contains("has no provider"))
    );
}

#[test]
fn a_module_is_initialized_after_every_module_it_depends_on() {
    let manifests = [
        module("Consumer", &[], &[("ai.tinyhumans.module.Provider", false)]),
        module("Provider", &["ai.tinyhumans.module.Provider"], &[]),
    ];
    assert_eq!(resolve(&manifests, &HashSet::new()).order, [1, 0]);
}

#[test]
fn two_modules_claiming_one_bus_name_are_both_reported_and_neither_is_initialized() {
    let first = module("One", &[], &[]);
    let mut second = module("Two", &[], &[]);
    second.bus_name = first.bus_name.clone();
    let result = resolve(&[first, second], &HashSet::new());
    assert!(result.order.is_empty());
    assert_eq!(result.unresolved.len(), 2);
}

mod resolver {
    use super::super::test_support::{Fixture, Recording, fixture, record, shared_runtime};

    struct ResolverCallback(crate::BusName);

    #[async_trait::async_trait]
    impl crate::Interface for ResolverCallback {
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

    async fn callback_fixture_lock() -> tokio::sync::MutexGuard<'static, ()> {
        static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        LOCK.lock().await
    }

    #[tokio::test]
    async fn callback_resolution_rejects_a_binding_from_another_host_without_claiming_the_slot() {
        use crate::module::HostCallbackEndpoint;
        use crate::module::runtime::{ModuleRuntime, RuntimeNames};

        let record = record("resolver_foreign_callback_binding");
        let runtime = shared_runtime();
        let foreign_runtime = ModuleRuntime::start(
            &RuntimeNames {
                thread: "tinybus-test-foreign-callback".to_owned(),
                worker: "tinybus-test-foreign-callback-worker".to_owned(),
            },
            None,
        )
        .unwrap();
        let binding = Arc::new(
            foreign_runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        let f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
        );

        let error = f
            .resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(error.contains("another host"), "{error}");
        assert_eq!(f.resolver.state_of(record.id), Availability::Available);

        // A rejected capability cannot reserve the process-global slot or
        // prevent the normal resolver path from deciding the module outcome.
        f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert_eq!(f.resolver.state_of(record.id), Availability::Failed);
        assert_eq!(
            runtime
                .host()
                .list()
                .iter()
                .filter(|info| { info.manifest.bus_name.as_str() == record.bus_name })
                .count(),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_local_callback_module_is_admitted_with_its_single_binding() {
        let _fixture_guard = callback_fixture_lock().await;
        let Some(source) =
            std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").map(std::path::PathBuf::from)
        else {
            return;
        };
        let working_dir = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        let artifact_dir = tempfile::tempdir_in(working_dir).unwrap();
        let artifact_dir = std::fs::canonicalize(artifact_dir.path()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&artifact_dir, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let artifact = artifact_dir.join(source.file_name().unwrap());
        std::fs::copy(&source, &artifact).unwrap();
        let digest =
            crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
        std::fs::write(
            artifact_dir.join("modules.toml"),
            format!(
                "{:?} = {digest:?}\n",
                artifact.file_name().unwrap().to_str().unwrap()
            ),
        )
        .unwrap();
        let record = Box::leak(Box::new(crate::module::ReleaseRecord {
            id: "resolver_single_callback_success",
            bus_name: "org.example.EagerCallback",
            object_path: "/org/example/EagerCallback",
            version: "0.0.0",
            release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
            assets: &[],
        }));
        let runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
            crate::module::ModuleRuntime::start(
                &crate::module::RuntimeNames {
                    thread: "single-callback-local-test".into(),
                    worker: "single-callback-local-worker".into(),
                },
                None,
            )
            .unwrap(),
        ));
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![crate::module::HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                ResolverCallback("org.example.Callback".parse().unwrap()),
            )
            .await
            .unwrap();
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: artifact.to_string_lossy().into_owned(),
                },
            )]),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
        });

        resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                Arc::clone(&binding),
                serde_json::json!({ "host": binding.unique_name().as_str() }),
            )
            .await
            .unwrap();
        // A repeated single-binding request is accepted only when the
        // resolver's completed result still retains this exact binding.
        resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                Arc::clone(&binding),
                serde_json::json!({ "host": binding.unique_name().as_str() }),
            )
            .await
            .unwrap();

        assert_eq!(resolver.state_of(record.id), Availability::Ready);
        assert!(
            runtime
                .host()
                .retains_host_callback_binding(record.bus_name, &binding)
        );
    }

    #[tokio::test]
    async fn failed_local_callback_load_does_not_retain_its_binding() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_failed_local_callback_load");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                ResolverCallback("org.example.Callback".parse().unwrap()),
            )
            .await
            .unwrap();
        let resolver = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/missing-callback-module.so".to_owned(),
                },
            )]),
            None,
        )
        .resolver;

        let error = resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                Arc::clone(&binding),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();

        assert!(error.contains("callback load failed"), "{error}");
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
        assert!(
            !runtime
                .host()
                .retains_host_callback_binding(record.bus_name, &binding)
        );
    }

    #[tokio::test]
    async fn callback_factory_failure_is_cached_without_retrying_or_loading() {
        let host_key = Box::leak(
            crate::module::platform::host_candidates()
                .into_iter()
                .next()
                .expect("this host has a release candidate")
                .into_boxed_str(),
        );
        let assets = Box::leak(
            vec![ReleaseAsset {
                host_key,
                archive: "callback-factory.tar.gz",
                sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                library_sha256: None,
            }]
            .into_boxed_slice(),
        );
        let record = Box::leak(Box::new(crate::module::ReleaseRecord {
            id: "resolver_callback_factory_failure",
            bus_name: "ai.tinyhumans.test.resolver_callback_factory_failure",
            object_path: "/ai/tinyhumans/test/Module",
            version: "0.0.0",
            release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
            assets,
        }));
        let install_dir = tempfile::tempdir().unwrap();
        let resolver = fixture(
            record,
            true,
            HashMap::new(),
            Some(install_dir.path().to_path_buf()),
        )
        .resolver;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let factory_attempts = Arc::clone(&attempts);

        let error = resolver
            .ensure_loaded_with_host_callback_factory(record.id, move |_| {
                let attempts = Arc::clone(&factory_attempts);
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err("callback setup was refused".to_owned())
                }
            })
            .await
            .err()
            .expect("the callback factory refusal should fail resolution");

        assert!(error.starts_with("callback setup was refused"), "{error}");
        assert!(error.contains("restart the app to try again"), "{error}");
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let cached_error = resolver
            .ensure_loaded_with_host_callback_factory(record.id, |_| async {
                panic!("a cached callback failure must not rerun its factory")
            })
            .await
            .err()
            .expect("the cached callback refusal should remain a failure");
        assert_eq!(cached_error, error);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            !shared_runtime()
                .host()
                .list()
                .iter()
                .any(|info| info.manifest.bus_name.as_str() == record.bus_name)
        );
    }

    #[tokio::test]
    async fn callback_apis_reject_linked_sources_before_preparing_authority() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_callback_linked_source");
        let resolver = fixture(
            record,
            true,
            HashMap::from([(record.id.to_string(), ModuleSource::Linked)]),
            None,
        )
        .resolver;
        let binding = Arc::new(
            shared_runtime()
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );

        let single_error = resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            single_error.contains("verified local-file source"),
            "{single_error}"
        );

        let factory_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&factory_calls);
        let factory_error = resolver
            .ensure_loaded_with_host_callback_factory(record.id, move |_| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Err("callback authority must not be prepared for a linked source".to_owned())
                }
            })
            .await
            .err()
            .expect("linked modules cannot carry resolver callback authority");
        assert!(
            factory_error.contains("callback source cannot preserve admitted identity"),
            "{factory_error}"
        );
        assert_eq!(factory_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(resolver.state_of(record.id), Availability::Available);
    }

    #[tokio::test]
    async fn callback_load_rejects_a_preloaded_module_without_its_binding() {
        let record = record("resolver_callback_preloaded_without_binding");
        let runtime = shared_runtime();
        let mut manifest = super::module(record.id, &[], &[]);
        manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
        manifest.object_path = crate::ObjectPath::new(record.object_path).unwrap();
        runtime
            .host()
            .attach_linked_with_config(
                crate::module::LinkedModule {
                    descriptor: crate::module::abi::TbAbiDescriptor::current(record.id, "1.0.0"),
                    manifest,
                    init: initialize_raced_module,
                    init_with_setup: None,
                },
                serde_json::Value::Null,
            )
            .unwrap();
        let f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/callback-module.so".to_owned(),
                },
            )]),
            None,
        );

        let error = f
            .resolver
            .ensure_loaded_with_host_callback_factory(record.id, |_| async {
                panic!("a preloaded module without a binding must reject the factory")
            })
            .await
            .err()
            .expect("the preloaded module must fail closed without its binding");

        assert!(
            error.contains("already loaded without a callback binding"),
            "{error}"
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Available);
    }

    #[tokio::test]
    async fn callback_load_fails_closed_when_admission_races_the_search_path() {
        let record = record("resolver_callback_search_race");
        let mut f = fixture(record, true, HashMap::new(), None);
        f.resolver.after_initial_host_check = Some(Arc::new(|runtime, record| {
            let mut manifest = super::module(record.id, &[], &[]);
            manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
            manifest.object_path = crate::ObjectPath::new(record.object_path).unwrap();
            runtime
                .host()
                .attach_linked_with_config(
                    crate::module::LinkedModule {
                        descriptor: crate::module::abi::TbAbiDescriptor::current(
                            record.id, "1.0.0",
                        ),
                        manifest,
                        init: initialize_raced_module,
                        init_with_setup: None,
                    },
                    serde_json::Value::Null,
                )
                .unwrap();
        }));
        let factory_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&factory_calls);

        let error = f
            .resolver
            .ensure_loaded_with_host_callback_factory(record.id, move |_| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err("callback factory should not run after admission".to_owned())
                }
            })
            .await
            .err()
            .expect("a concurrent unbound admission must fail closed");

        assert!(
            error.contains("was loaded without the requested callback binding"),
            "{error}"
        );
        assert_eq!(factory_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(f.resolver.state_of(record.id), Availability::Failed);
    }

    #[tokio::test]
    async fn cached_ready_callback_resolution_requires_its_retained_binding() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_cached_callback_without_binding");
        match resolution::global().claim(record.id) {
            Claim::Run { sender, .. } => {
                resolution::global().complete(record.id, Resolution::Ready, sender);
            }
            _ => panic!("test id unexpectedly had a resolution"),
        }
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                ResolverCallback("org.example.Callback".parse().unwrap()),
            )
            .await
            .unwrap();
        let resolver = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/cached-callback-module.so".to_owned(),
                },
            )]),
            None,
        )
        .resolver;

        let error = match resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::Value::Null)
            .await
        {
            Ok(()) => panic!("a cached ready slot cannot substitute for its callback binding"),
            Err(error) => error,
        };

        assert!(
            error.contains("was loaded without the requested callback binding"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn callback_apis_hide_runtime_startup_details_and_do_not_claim_resolution() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_callback_runtime_failure");
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| {
                Box::pin(async { Err(crate::Error::failed("private runtime detail")) })
            }),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
        });
        let binding = Arc::new(
            shared_runtime()
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );

        let single_error = resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            single_error.contains("the module bus could not start"),
            "{single_error}"
        );
        assert!(!single_error.contains("private runtime detail"));

        let factory_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&factory_calls);
        let factory_error = resolver
            .ensure_loaded_with_host_callback_factory(record.id, move |_| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Err("the factory must not run".to_owned()) }
            })
            .await
            .err()
            .expect("runtime startup failure should be reported");
        assert!(
            factory_error.contains("the module bus could not start"),
            "{factory_error}"
        );
        assert!(!factory_error.contains("private runtime detail"));
        assert_eq!(factory_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(resolver.state_of(record.id), Availability::Available);
    }

    #[tokio::test]
    async fn callback_resolution_does_not_join_a_generic_in_flight_load() {
        use crate::module::HostCallbackEndpoint;
        use std::sync::{Condvar, Mutex};

        let record = record("resolver_callback_does_not_join_generic_load");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                ResolverCallback("org.example.Callback".parse().unwrap()),
            )
            .await
            .unwrap();
        let f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
        );
        let entered = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_entered = entered.clone();
        let hook_release = release.clone();
        let mut resolver = f.resolver.clone();
        resolver.after_initial_host_check = Some(Arc::new(move |_, _| {
            let (lock, changed) = &*hook_entered;
            *lock.lock().unwrap() = true;
            changed.notify_one();
            let (lock, changed) = &*hook_release;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = changed.wait(ready).unwrap();
            }
        }));

        let generic_resolver = resolver.clone();
        let generic_load =
            tokio::spawn(async move { generic_resolver.ensure_loaded(record.id).await });
        let waiting = entered.clone();
        tokio::task::spawn_blocking(move || {
            let (lock, changed) = &*waiting;
            let mut entered = lock.lock().unwrap();
            while !*entered {
                entered = changed.wait(entered).unwrap();
            }
        })
        .await
        .unwrap();

        let error = resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            error.contains("already resolving without this callback binding"),
            "{error}"
        );
        assert_eq!(resolver.state_of(record.id), Availability::Loading);

        let (lock, changed) = &*release;
        *lock.lock().unwrap() = true;
        changed.notify_one();
        assert!(generic_load.await.unwrap().is_err());
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
    }

    #[tokio::test]
    async fn callback_resolution_rejects_a_module_already_loaded_without_its_binding() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_callback_rejects_preloaded_generic_module");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                ResolverCallback("org.example.Callback".parse().unwrap()),
            )
            .await
            .unwrap();
        let mut manifest = super::module(record.id, &[], &[]);
        manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
        manifest.object_path = crate::ObjectPath::new(record.object_path).unwrap();
        runtime
            .host()
            .attach_linked_with_config(
                crate::module::LinkedModule {
                    descriptor: crate::module::abi::TbAbiDescriptor::current(record.id, "1.0.0"),
                    manifest,
                    init: initialize_raced_module,
                    init_with_setup: None,
                },
                serde_json::Value::Null,
            )
            .unwrap();

        let f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
        );
        let error = f
            .resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            error.contains("already loaded without this callback binding"),
            "{error}"
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Available);
        assert_eq!(
            runtime
                .host()
                .list()
                .iter()
                .filter(|info| { info.manifest.bus_name.as_str() == record.bus_name })
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolver_retains_the_callback_binding_for_its_exact_admitted_module() {
        use crate::module::HostCallbackEndpoint;
        use std::path::PathBuf;

        let _fixture_guard = callback_fixture_lock().await;
        let Some(source) = std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").map(PathBuf::from)
        else {
            return;
        };
        // Keep the bundled cache below the checkout: macOS's system temp path
        // can include symlinked ancestors, which ModuleHost correctly refuses.
        let working_dir = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        let root = tempfile::tempdir_in(working_dir).unwrap();
        let root_path = std::fs::canonicalize(root.path()).unwrap();
        let host_key = Box::leak(
            crate::module::platform::host_candidates()
                .into_iter()
                .next()
                .expect("this host has a candidate")
                .into_boxed_str(),
        );
        let asset = ReleaseAsset {
            host_key,
            archive: "callback-module.tar.gz",
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            library_sha256: None,
        };
        let assets = Box::leak(vec![asset].into_boxed_slice());
        let module_dir =
            crate::module::artifact_dir(&root_path, "tinybus", "0.0.0", asset.host_key).unwrap();
        #[cfg(windows)]
        crate::module::windows_acl::create_private_dir_all(&module_dir).unwrap();
        #[cfg(not(windows))]
        std::fs::create_dir_all(&module_dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&module_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let artifact = module_dir.join(format!(
            "libcallback.{}",
            crate::module::cache::library_extension()
        ));
        std::fs::copy(&source, &artifact).unwrap();
        let library_hash =
            crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
        std::fs::write(
            module_dir.join(format!("{}.sha256", asset.archive)),
            format!("{}\n", asset.sha256),
        )
        .unwrap();
        std::fs::write(
            module_dir.join("modules.toml"),
            format!(
                "{:?} = {library_hash:?}\n",
                artifact.file_name().unwrap().to_str().unwrap()
            ),
        )
        .unwrap();
        let record = Box::leak(Box::new(ReleaseRecord {
            id: "tinybus",
            bus_name: "org.example.EagerCallback",
            object_path: "/org/example/EagerCallback",
            version: "0.0.0",
            release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
            assets,
        }));
        // The fixture module's bus name is fixed, and the shared runtime can
        // already hold it from the other callback tests, so this test owns its
        // runtime.
        let runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
            crate::module::ModuleRuntime::start(
                &crate::module::RuntimeNames {
                    thread: "retained-callback-binding-test".into(),
                    worker: "retained-callback-binding-worker".into(),
                },
                None,
            )
            .unwrap(),
        ));
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: Some(root_path.clone()),
            allow_download: false,
            bundled_dir: Some(root_path),
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(
                test_support::Recording::default(),
            ))),
        });
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let prepare_attempts = Arc::clone(&attempts);
        let binding = resolver
            .ensure_loaded_with_host_callback_factory(record.id, move |_| {
                let attempts = Arc::clone(&prepare_attempts);
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                            ResolverCallback("org.example.Callback".parse().unwrap()),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    let config = serde_json::json!({ "host": binding.unique_name() });
                    Ok((binding, config))
                }
            })
            .await
            .unwrap();
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let guard = binding.guard();
        let info = runtime
            .host()
            .list()
            .into_iter()
            .find(|info| info.manifest.bus_name.as_str() == record.bus_name)
            .expect("the requested registry identity should be loaded");
        assert_eq!(info.manifest.object_path.as_str(), record.object_path);
        assert!(
            runtime
                .host()
                .retains_host_callback_binding(record.bus_name, &binding)
        );
        let peer = guard
            .module_peer()
            .expect("the callback is bound to a peer");
        assert!(guard.authorizes(&crate::CallContext::brokered(Some(&peer), true)));

        // Repeated resolution with this same retained capability is idempotent;
        // a different or ordinary load cannot borrow its authority.
        let repeated = resolver
            .ensure_loaded_with_host_callback_factory(record.id, |_| async {
                panic!("an already loaded callback binding is reused")
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&binding, &repeated));

        // A resolution that is ready in the process-wide table but was loaded
        // into another runtime cannot lend this resolver a callback binding.
        let other_runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
            crate::module::ModuleRuntime::start(
                &crate::module::RuntimeNames {
                    thread: "retained-callback-other-test".into(),
                    worker: "retained-callback-other-worker".into(),
                },
                None,
            )
            .unwrap(),
        ));
        let elsewhere = fixture_with_runtime(
            record,
            true,
            HashMap::new(),
            None,
            Arc::new(move || Box::pin(async move { Ok(other_runtime) })),
        )
        .resolver;
        let error = elsewhere
            .ensure_loaded_with_host_callback_factory(record.id, |_| async {
                panic!("a ready resolution never prepares a new callback binding")
            })
            .await
            .err()
            .expect("a ready module in another runtime has no retained binding");
        assert!(
            error.contains("loaded without a callback binding"),
            "{error}"
        );

        let direct_resolver = fixture_with_runtime(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: source.to_string_lossy().into_owned(),
                },
            )]),
            None,
            Arc::new(move || Box::pin(async move { Ok(runtime) })),
        )
        .resolver;
        direct_resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                Arc::clone(&binding),
                serde_json::json!({}),
            )
            .await
            .expect("the direct callback API reuses the retained binding");
    }

    fn admitted_info(record: &ReleaseRecord, state: ModuleState) -> ModuleInfo {
        let mut manifest = super::module(record.id, &[], &[]);
        manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
        manifest.object_path = crate::ObjectPath::new(record.object_path).unwrap();
        ModuleInfo {
            name: manifest.module.name.clone(),
            version: manifest.module.version.to_string(),
            file: "module.dylib".into(),
            state,
            manifest,
            rustc_version: String::new(),
            rustc_mismatch: false,
            enabled: true,
        }
    }

    unsafe extern "C" fn accept_raced_message(
        _: *mut std::ffi::c_void,
        _: *const u8,
        _: usize,
    ) -> i32 {
        crate::module::abi::TB_OK
    }

    unsafe extern "C" fn stop_raced_module(ctx: *mut std::ffi::c_void, _: u64) -> i32 {
        if !ctx.is_null() {
            drop(unsafe { Box::from_raw(ctx.cast::<()>()) });
        }
        crate::module::abi::TB_OK
    }

    unsafe extern "C" fn initialize_raced_module(
        host: *const crate::module::abi::TbHostVtable,
        module: *mut crate::module::abi::TbModuleVtable,
    ) -> i32 {
        let host = unsafe { *host };
        unsafe {
            *module = crate::module::abi::TbModuleVtable {
                size: crate::module::abi::TB_MODULE_VTABLE_BASE_SIZE,
                _reserved: 0,
                module_ctx: Box::into_raw(Box::new(())).cast(),
                deliver: accept_raced_message,
                shutdown: stop_raced_module,
                reinitialize: None,
            }
        };
        unsafe { (host.ready)(host.host_ctx) };
        crate::module::abi::TB_OK
    }

    fn raced_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_raced_module", &[], &[]);
        manifest.bus_name = crate::BusName::new("ai.tinyhumans.test.resolver_raced_module")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_raced_module",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    fn raced_search_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_search_raced_module", &[], &[]);
        manifest.bus_name = crate::BusName::new("ai.tinyhumans.test.resolver_search_raced_module")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_search_raced_module",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    fn raced_cache_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_cache_raced_module", &[], &[]);
        manifest.bus_name = crate::BusName::new("ai.tinyhumans.test.resolver_cache_raced_module")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_cache_raced_module",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    fn linked_success_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_linked_success", &[], &[]);
        manifest.bus_name = crate::BusName::new("ai.tinyhumans.test.resolver_linked_success")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_linked_success",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    fn linked_conflicting_name_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_linked_attach_failure", &[], &[]);
        manifest.bus_name = crate::BusName::new("ai.tinyhumans.test.conflicting_module")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Conflict")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_linked_attach_failure",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    fn linked_attach_target_module() -> crate::Result<LinkedModule> {
        let mut manifest = super::module("resolver_linked_attach_failure", &[], &[]);
        manifest.bus_name =
            crate::BusName::new("ai.tinyhumans.test.resolver_linked_attach_failure")?;
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
        Ok(LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(
                "resolver_linked_attach_failure",
                "1.0.0",
            ),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        })
    }

    #[tokio::test]
    async fn a_loaded_module_with_the_registered_bus_but_wrong_object_path_does_not_satisfy_the_initial_fast_path()
     {
        let record = record("resolver_wrong_object_path_fast_path");
        let runtime = shared_runtime();
        let mut manifest = super::module(record.id, &[], &[]);
        manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
        manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/WrongObject").unwrap();
        runtime
            .host()
            .attach_linked_with_config(
                LinkedModule {
                    descriptor: crate::module::abi::TbAbiDescriptor::current(record.id, "1.0.0"),
                    manifest,
                    init: initialize_raced_module,
                    init_with_setup: None,
                },
                serde_json::Value::Null,
            )
            .unwrap();

        let preloaded = runtime
            .host()
            .list()
            .into_iter()
            .find(|info| info.manifest.bus_name.as_str() == record.bus_name)
            .expect("the wrong-path fixture should be admitted under the requested bus name");
        assert_eq!(
            preloaded.manifest.object_path.as_str(),
            "/ai/tinyhumans/test/WrongObject"
        );

        let f = fixture(record, true, HashMap::new(), None);
        let result = f.resolver.ensure_loaded(record.id).await;

        assert!(
            result.is_err(),
            "a same-bus module with another object path must not resolve the pinned record"
        );
        assert!(runtime.host().list().iter().any(|info| {
            info.manifest.bus_name.as_str() == record.bus_name
                && info.manifest.object_path.as_str() == "/ai/tinyhumans/test/WrongObject"
        }));
    }

    #[test]
    fn a_duplicate_admission_error_recovers_when_the_requested_module_is_initializing() {
        let record = record("resolver_raced_admission");
        // This snapshot is taken after a competing host path won admission but
        // before this resolver's load attempt returned its duplicate-name error.
        let modules = [admitted_info(record, ModuleState::Initializing)];
        let result = recover_admission(
            Err("module name is already loaded".into()),
            &modules,
            record,
        );
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn ensure_loaded_recovers_when_direct_host_admission_wins_after_its_initial_check() {
        let record = record("resolver_raced_module");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        let mut f = fixture(record, true, sources.clone(), None);
        let runtime = shared_runtime();
        f.resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources,
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".into(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(f.sink.clone())),
        });
        f.resolver.after_initial_host_check = Some(Arc::new(|runtime, _| {
            runtime
                .host()
                .attach_linked_with_config(raced_module().unwrap(), serde_json::json!({}))
                .unwrap();
        }));

        assert_eq!(f.resolver.ensure_loaded(record.id).await, Ok(()));
        assert!(runtime.host().list().iter().any(|info| {
            info.manifest.bus_name.as_str() == record.bus_name
                && info.manifest.object_path.as_str() == record.object_path
        }));
        assert_eq!(f.resolver.state_of(record.id), Availability::Ready);
    }

    #[tokio::test]
    async fn a_registered_linked_module_is_loaded_and_reported_ready() {
        let record = record("resolver_linked_success");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        let f = fixture(record, true, sources, None);
        f.resolver
            .inner
            .linked
            .register(record.id, linked_success_module);

        f.resolver.ensure_loaded(record.id).await.unwrap();

        assert_eq!(f.resolver.state_of(record.id), Availability::Ready);
        assert_eq!(f.resolver.status_of(record), (Availability::Ready, None));
        assert!(shared_runtime().host().list().iter().any(|info| {
            info.manifest.bus_name.as_str() == record.bus_name
                && info.manifest.object_path.as_str() == record.object_path
                && matches!(info.state, ModuleState::Ready | ModuleState::Serving)
        }));
    }

    #[tokio::test]
    async fn a_linked_attach_collision_returns_a_terminal_failure() {
        let record = record("resolver_linked_attach_failure");
        let runtime = shared_runtime();
        runtime
            .host()
            .attach_linked_with_config(
                linked_conflicting_name_module().unwrap(),
                serde_json::Value::Null,
            )
            .unwrap();
        let resolver = fixture(
            record,
            true,
            HashMap::from([(record.id.to_string(), ModuleSource::Linked)]),
            None,
        )
        .resolver;
        resolver
            .inner
            .linked
            .register(record.id, linked_attach_target_module);

        let error = resolver.ensure_loaded(record.id).await.unwrap_err();

        assert!(
            error.contains("could not be attached from the linked executable"),
            "{error}"
        );
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
    }

    #[tokio::test]
    async fn a_blocking_loader_runtime_failure_is_terminal_and_retryable_only_after_restart() {
        let record = record("resolver_blocking_runtime_failure");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let runtime_calls = Arc::clone(&calls);
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/blocked-module.so".to_owned(),
                },
            )]),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".to_owned(),
            runtime: Arc::new(move || {
                let calls = Arc::clone(&runtime_calls);
                Box::pin(async move {
                    if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        Ok(shared_runtime())
                    } else {
                        Err(crate::Error::failed("runtime temporarily unavailable"))
                    }
                })
            }),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
        });

        let error = resolver.ensure_loaded(record.id).await.unwrap_err();

        assert!(error.contains("the module bus could not start"), "{error}");
        assert!(error.contains("restart the app to try again"), "{error}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
    }

    #[tokio::test]
    async fn blocking_work_marks_runtime_startup_errors_as_terminal() {
        let record = record("resolver_blocking_runtime_provider_failure");
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".to_owned(),
            runtime: Arc::new(|| {
                Box::pin(async { Err(crate::Error::failed("runtime startup refused")) })
            }),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
        });

        let error = resolver
            .blocking(|| Ok(()))
            .await
            .expect_err("blocking work must not run without its runtime");

        assert!(
            error.contains("module bus could not start")
                && error.contains("runtime startup refused"),
            "{error}"
        );
        assert!(
            error.contains("This is terminal for the running process"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn ensure_loaded_rechecks_host_after_search_before_touching_the_release_cache() {
        let record = record("resolver_search_raced_module");
        let dir = tempfile::tempdir().unwrap();
        let mut f = fixture(record, true, HashMap::new(), Some(dir.path().to_path_buf()));
        let runtime = shared_runtime();
        f.resolver.after_initial_host_check = Some(Arc::new(|runtime, _| {
            runtime
                .host()
                .attach_linked_with_config(raced_search_module().unwrap(), serde_json::json!({}))
                .unwrap();
        }));

        assert_eq!(f.resolver.ensure_loaded(record.id).await, Ok(()));
        assert!(runtime.host().list().iter().any(|info| {
            info.manifest.bus_name.as_str() == record.bus_name
                && info.manifest.object_path.as_str() == record.object_path
        }));
        assert_eq!(
            f.config_calls.load(Ordering::SeqCst),
            0,
            "the release-cache rung ran after the module was admitted"
        );
    }

    #[tokio::test]
    async fn recovering_after_a_cache_error_does_not_prune_other_cached_versions() {
        let record = record("resolver_cache_raced_module");
        let install_dir = tempfile::tempdir().unwrap();
        let stale = install_dir.path().join(record.id).join("old-version");
        std::fs::create_dir_all(&stale).unwrap();
        let marker = stale.join("preserve-me");
        std::fs::write(&marker, "cached by the other host load").unwrap();
        let sink = Arc::new(Recording::default());
        let runtime = shared_runtime();
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: Some(install_dir.path().to_path_buf()),
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".into(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(move |_| {
                runtime
                    .host()
                    .attach_linked_with_config(raced_cache_module().unwrap(), serde_json::json!({}))
                    .unwrap();
                serde_json::json!({})
            }),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(sink)),
        });

        assert_eq!(resolver.ensure_loaded(record.id).await, Ok(()));
        assert!(runtime.host().list().iter().any(|info| {
            info.manifest.bus_name.as_str() == record.bus_name
                && info.manifest.object_path.as_str() == record.object_path
        }));
        assert!(
            marker.exists(),
            "a recovered cache failure pruned its sibling cache"
        );
    }

    #[test]
    fn admission_recovery_requires_the_exact_enabled_nonterminal_module() {
        let record = record("resolver_raced_admission_guards");
        let error = "module name is already loaded".to_string();
        let wrong_path = {
            let mut info = admitted_info(record, ModuleState::Ready);
            info.manifest.object_path = crate::ObjectPath::new("/other/module").unwrap();
            info
        };
        let wrong_bus = {
            let mut info = admitted_info(record, ModuleState::Ready);
            info.manifest.bus_name = crate::BusName::new("ai.tinyhumans.module.Other").unwrap();
            info
        };
        let disabled = {
            let mut info = admitted_info(record, ModuleState::Ready);
            info.enabled = false;
            info
        };
        for info in [
            wrong_path,
            wrong_bus,
            disabled,
            admitted_info(
                record,
                ModuleState::Failed {
                    reason: "init failed".into(),
                },
            ),
            admitted_info(
                record,
                ModuleState::Faulted {
                    reason: "faulted".into(),
                },
            ),
            admitted_info(record, ModuleState::Stopped),
            admitted_info(record, ModuleState::Disabled),
        ] {
            assert_eq!(
                recover_admission(Err(error.clone()), &[info], record),
                Err(error.clone())
            );
        }
    }

    use super::super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCallback(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl crate::Interface for CountingCallback {
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
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!("org.example.Callback"))
        }
    }

    fn fixture_with_runtime(
        record: &'static ReleaseRecord,
        enabled: bool,
        sources: HashMap<String, ModuleSource>,
        install_dir: Option<PathBuf>,
        runtime: RuntimeProvider,
    ) -> Fixture {
        let sink = Arc::new(Recording::default());
        let config_calls = Arc::new(AtomicUsize::new(0));
        let runtime_calls = Arc::new(AtomicUsize::new(0));
        let counted = config_calls.clone();
        let started = runtime_calls.clone();
        let measured_runtime = Arc::new(move || {
            started.fetch_add(1, Ordering::SeqCst);
            runtime()
        });
        let resolver = Resolver::new(ResolverInputs {
            enabled,
            install_dir,
            allow_download: false,
            bundled_dir: None,
            sources,
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".to_string(),
            runtime: measured_runtime,
            module_config: Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                serde_json::json!({})
            }),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(sink.clone())),
        });
        Fixture {
            sink,
            config_calls,
            runtime_calls,
            resolver,
        }
    }

    fn linked_module_for(record: &ReleaseRecord) -> LinkedModule {
        let mut manifest = super::module(record.id, &[], &[]);
        manifest.bus_name = crate::BusName::new(record.bus_name).unwrap();
        manifest.object_path = crate::ObjectPath::new(record.object_path).unwrap();
        LinkedModule {
            descriptor: crate::module::abi::TbAbiDescriptor::current(record.id, "1.0.0"),
            manifest,
            init: initialize_raced_module,
            init_with_setup: None,
        }
    }

    #[tokio::test]
    async fn a_foreign_callback_binding_does_not_poison_ordinary_resolution() {
        use crate::module::HostCallbackEndpoint;
        use crate::module::runtime::{ModuleRuntime, RuntimeNames};

        let record = record("resolver_callback_bad_binding");
        let _runtime = shared_runtime();
        let foreign_runtime = ModuleRuntime::start(
            &RuntimeNames {
                thread: "tinybus-test-foreign-callback".to_owned(),
                worker: "tinybus-test-foreign-callback-worker".to_owned(),
            },
            None,
        )
        .unwrap();
        let binding = Arc::new(
            foreign_runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(Arc::new(AtomicUsize::new(0))),
            )
            .await
            .unwrap();
        let mut f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
        );

        assert!(
            f.resolver
                .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
                .await
                .is_err()
        );
        assert_eq!(
            f.resolver.state_of(record.id),
            Availability::Available,
            "invalid callback binding poisoned the module resolution slot"
        );

        f.resolver.after_initial_host_check = Some(Arc::new(move |runtime, _| {
            runtime
                .host()
                .attach_linked_with_config(linked_module_for(record), serde_json::json!({}))
                .unwrap();
        }));
        f.resolver.ensure_loaded(record.id).await.unwrap();
        assert_eq!(f.resolver.state_of(record.id), Availability::Ready);
    }

    #[tokio::test]
    async fn a_direct_admission_race_keeps_callback_resolution_available() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver_callback_admission_race");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(Arc::new(AtomicUsize::new(0))),
            )
            .await
            .unwrap();
        let mut f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
        );
        f.resolver.after_initial_host_check = Some(Arc::new(|runtime, record| {
            runtime
                .host()
                .attach_linked_with_config(linked_module_for(record), serde_json::json!({}))
                .unwrap();
        }));

        assert!(
            f.resolver
                .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
                .await
                .is_err()
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Ready);
        f.resolver.ensure_loaded(record.id).await.unwrap();
    }

    #[tokio::test]
    async fn a_callback_request_does_not_claim_resolution_when_runtime_start_fails() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver-callback-runtime-unavailable");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(Arc::new(AtomicUsize::new(0))),
            )
            .await
            .unwrap();
        let resolver = fixture_with_runtime(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: "/unused/verified-module.so".to_owned(),
                },
            )]),
            None,
            Arc::new(|| Box::pin(async { Err(crate::Error::failed("runtime unavailable")) })),
        );

        let error = resolver
            .resolver
            .ensure_loaded_with_host_callbacks(record.id, binding, serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(error.contains("module bus could not start"), "{error}");
        assert_eq!(
            resolver.resolver.state_of(record.id),
            Availability::Available
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires TINYBUS_TEST_HOST_CALLBACK_MODULE to point at the built cdylib"]
    async fn resolver_loads_a_verified_module_with_only_its_prepared_host_callback() {
        use crate::module::HostCallbackEndpoint;

        let source = PathBuf::from(std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").unwrap());
        let hash = crate::module::hash::file_hex(std::fs::File::open(&source).unwrap()).unwrap();
        let staging_root = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let staging = tempfile::tempdir_in(staging_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(staging.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let path = staging.path().join(source.file_name().unwrap());
        std::fs::copy(&source, &path).unwrap();
        std::fs::write(
            staging.path().join("modules.toml"),
            format!(
                "{:?} = {hash:?}\n",
                source.file_name().unwrap().to_str().unwrap()
            ),
        )
        .unwrap();
        let record = Box::leak(Box::new(ReleaseRecord {
            id: "resolver-host-callback",
            bus_name: "org.example.EagerCallback",
            object_path: "/org/example/EagerCallback",
            version: "0.0.0",
            release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
            assets: &[],
        }));
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(calls.clone()),
            )
            .await
            .unwrap();
        let callback_host = binding.unique_name().to_string();
        let f = fixture(
            record,
            true,
            HashMap::from([(
                record.id.to_string(),
                ModuleSource::LocalFile {
                    path: path.to_string_lossy().into_owned(),
                },
            )]),
            None,
        );

        let loaded = f
            .resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                binding,
                serde_json::json!({ "host": callback_host }),
            )
            .await;
        assert!(
            loaded.is_ok(),
            "{loaded:?}; callbacks={}",
            calls.load(Ordering::SeqCst)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "resolver init did not use its exact prepared callback"
        );
        runtime
            .proxy("org.example.EagerCallback", "/org/example/EagerCallback")
            .unwrap()
            .call::<serde_json::Value>("Observe", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "resolver dropped the callback binding after module initialization"
        );

        let already_serving = Box::leak(Box::new(ReleaseRecord {
            id: "resolver-callback-already-serving-alias",
            bus_name: "org.example.EagerCallback",
            object_path: "/org/example/EagerCallback",
            version: "0.0.0",
            release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
            assets: &[],
        }));
        let alias = fixture(
            already_serving,
            true,
            HashMap::from([(
                already_serving.id.to_string(),
                ModuleSource::LocalFile {
                    path: path.to_string_lossy().into_owned(),
                },
            )]),
            None,
        );
        let refused_binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        refused_binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(calls.clone()),
            )
            .await
            .unwrap();
        assert!(
            alias
                .resolver
                .ensure_loaded_with_host_callbacks(
                    already_serving.id,
                    refused_binding,
                    serde_json::json!({}),
                )
                .await
                .is_err()
        );
        assert_eq!(
            alias.resolver.state_of(already_serving.id),
            Availability::Available,
            "already-serving callback rejection poisoned ordinary resolution"
        );
        alias
            .resolver
            .ensure_loaded(already_serving.id)
            .await
            .unwrap();
        assert_eq!(
            alias.resolver.state_of(already_serving.id),
            Availability::Ready
        );
        assert!(
            runtime
                .host()
                .list()
                .iter()
                .any(|module| module.manifest.bus_name.as_str() == "org.example.EagerCallback")
        );
    }

    #[tokio::test]
    async fn resolver_refuses_host_callbacks_for_pinned_sources_before_loading() {
        use crate::module::HostCallbackEndpoint;

        let record = record("resolver-callback-pinned-refusal");
        let runtime = shared_runtime();
        let binding = Arc::new(
            runtime
                .host()
                .prepare_host_callbacks(vec![HostCallbackEndpoint {
                    path: "/callback".parse().unwrap(),
                    interface: "org.example.Callback".parse().unwrap(),
                    members: vec!["Observe".parse().unwrap()],
                }])
                .await
                .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        binding
            .serve_at(
                "/callback".parse().unwrap(),
                CountingCallback(calls.clone()),
            )
            .await
            .unwrap();
        let f = fixture(
            record,
            true,
            HashMap::from([(record.id.to_string(), ModuleSource::Pinned)]),
            None,
        );

        let error = f
            .resolver
            .ensure_loaded_with_host_callbacks(
                record.id,
                binding,
                serde_json::json!({ "host": "untrusted-config-must-not-be-used" }),
            )
            .await
            .unwrap_err();
        assert!(error.contains("verified local-file source"), "{error}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "unsupported source reached a callback"
        );
        assert_eq!(
            f.config_calls.load(Ordering::SeqCst),
            0,
            "resolver's generic config ran"
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Available);
    }

    #[tokio::test]
    async fn a_faulted_module_is_never_retried_in_process() {
        let record = record("resolver-faulted");
        let dir = tempfile::tempdir().unwrap();
        let f = fixture(record, true, HashMap::new(), Some(dir.path().to_path_buf()));

        let first = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(first.contains("not available for this platform"), "{first}");
        assert!(first.contains("restart the app to try again"), "{first}");
        assert_eq!(f.resolver.state_of(record.id), Availability::Failed);
        assert_eq!(f.config_calls.load(Ordering::SeqCst), 1);

        // The cached outcome is returned verbatim, without another attempt.
        for _ in 0..3 {
            let again = f.resolver.ensure_loaded(record.id).await.unwrap_err();
            assert_eq!(again, first);
        }
        assert_eq!(
            f.config_calls.load(Ordering::SeqCst),
            1,
            "retried a faulted module"
        );
        // One terminal report, however many callers asked.
        assert_eq!(
            f.sink.0.lock().unwrap().as_slice(),
            &[(record.id.to_string(), Reason::ResolutionFailed)]
        );
    }

    #[tokio::test]
    async fn status_of_reports_the_failure_reason_and_unsupported_platforms() {
        let record = record("resolver-status");
        let dir = tempfile::tempdir().unwrap();
        let f = fixture(record, true, HashMap::new(), Some(dir.path().to_path_buf()));

        // No asset is published for this host.
        assert_eq!(
            f.resolver.status_of(record),
            (
                Availability::Unsupported,
                Some("no artifact is published for this platform".to_string())
            )
        );
        let _ = f.resolver.ensure_loaded(record.id).await;
        let (state, detail) = f.resolver.status_of(record);
        assert_eq!(state, Availability::Failed);
        assert!(detail.unwrap().contains("restart the app"));
    }

    #[test]
    fn status_of_distinguishes_a_resolution_that_is_still_loading() {
        let record = record("resolver-loading-status");
        let f = fixture(record, true, HashMap::new(), None);
        let Claim::Run { sender, .. } = resolution::global().claim(record.id) else {
            panic!("this test owns the process-global resolution id");
        };

        assert_eq!(f.resolver.status_of(record), (Availability::Loading, None));

        resolution::global().complete(
            record.id,
            Resolution::Failed("test cleanup".to_owned()),
            sender,
        );
    }

    #[test]
    fn invocation_failure_reports_are_not_deduplicated() {
        let record = record("resolver-invocation-reports");
        let f = fixture(record, true, HashMap::new(), None);

        f.resolver.report_invocation(record, Reason::ModuleFault);
        f.resolver.report_invocation(record, Reason::ModuleFault);

        assert_eq!(
            f.sink.0.lock().unwrap().as_slice(),
            &[
                (record.id.to_string(), Reason::ModuleFault),
                (record.id.to_string(), Reason::ModuleFault),
            ]
        );
    }

    #[test]
    fn still_loading_has_a_retry_message_for_non_bounded_callers() {
        assert_eq!(
            LoadError::StillLoading.into_message(),
            "the module is still loading; try again shortly"
        );
    }

    #[test]
    fn disabled_configuration_wins_over_a_loaded_module() {
        let record = record("resolver-disabled-status");
        let f = fixture(record, false, HashMap::new(), None);
        assert_eq!(
            f.resolver.status_of(record),
            (
                Availability::Unsupported,
                Some("modules are disabled in configuration".to_string())
            )
        );
    }

    #[tokio::test]
    async fn refusals_never_start_the_module_runtime() {
        let record = record("resolver-refusal");
        let f = fixture(record, false, HashMap::new(), None);
        let error = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(error.contains("modules are disabled"));
        let error = f.resolver.ensure_loaded("nobody").await.unwrap_err();
        // Disabled is checked before the registry, as it always was.
        assert!(error.contains("modules are disabled"));
        let enabled = fixture(record, true, HashMap::new(), None);
        let error = enabled.resolver.ensure_loaded("nobody").await.unwrap_err();
        assert_eq!(error, "unknown module 'nobody'");
        assert_eq!(enabled.runtime_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.runtime_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            f.sink.0.lock().unwrap().as_slice(),
            &[(record.id.to_string(), Reason::Disabled)]
        );
    }

    #[tokio::test]
    async fn a_missing_install_dir_is_terminal() {
        let record = record("resolver-no-install-dir");
        let f = fixture(record, true, HashMap::new(), None);
        let error = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(error.contains("no directory is available to install modules into"));
    }

    #[tokio::test]
    async fn a_local_override_is_tried_before_the_release_cache() {
        let record = record("resolver-override");
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent-module");
        let sources = HashMap::from([(
            record.id.to_string(),
            ModuleSource::LocalFile {
                path: missing.to_string_lossy().into_owned(),
            },
        )]);
        // No install dir: reaching the cache rung would say so instead.
        let f = fixture(record, true, sources, None);
        let error = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(
            error.contains("could not be loaded from its local artifact"),
            "{error}"
        );
        assert!(!error.contains("no directory is available"), "{error}");
        assert_eq!(
            error
                .matches("This is terminal for the running process")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_bounded_wait_reports_still_loading_without_cancelling_the_resolution() {
        let record = record("resolver-bounded");
        let dir = tempfile::tempdir().unwrap();
        let f = fixture(record, true, HashMap::new(), Some(dir.path().to_path_buf()));
        let table = resolution::global();
        let Claim::Run { sender, .. } = table.claim(record.id) else {
            panic!("first claim must run");
        };
        let waited = f
            .resolver
            .ensure_loaded_within(record.id, Some(Duration::from_millis(20)))
            .await;
        assert_eq!(waited, Err(LoadError::StillLoading));
        assert_eq!(f.resolver.state_of(record.id), Availability::Loading);
        table.complete(record.id, Resolution::Ready, sender);
        assert_eq!(f.resolver.ensure_loaded(record.id).await, Ok(()));
        assert_eq!(f.resolver.state_of(record.id), Availability::Ready);
    }

    #[tokio::test]
    async fn a_gated_module_is_refused_before_anything_starts_or_is_reported() {
        let record = record("resolver-gated");
        let mut f = fixture(record, true, HashMap::new(), None);
        let sink = Arc::new(Recording::default());
        let runtime_calls = Arc::new(AtomicUsize::new(0));
        let started = runtime_calls.clone();
        f.resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: Some(Arc::new(|id| {
                (id == "resolver-gated").then_some("test-switch")
            })),
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(move || {
                started.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(shared_runtime()) })
            }),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(sink.clone())),
        });
        let refused = f
            .resolver
            .ensure_loaded_within(record.id, None)
            .await
            .unwrap_err();
        assert_eq!(refused, LoadError::Disabled("test-switch"));
        assert!(refused.into_message().contains("test-switch"));
        assert_eq!(runtime_calls.load(Ordering::SeqCst), 0);
        assert!(
            sink.0.lock().unwrap().is_empty(),
            "a switch is not a failure"
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Available);
    }

    #[tokio::test]
    async fn a_linked_source_without_a_factory_is_terminal_and_never_retried() {
        let record = record("resolver-linked-missing");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        let f = fixture(record, true, sources, None);
        let first = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(first.contains("no linked factory is registered"), "{first}");
        assert!(!first.contains("no directory is available"), "{first}");
        assert_eq!(f.resolver.state_of(record.id), Availability::Failed);
        assert_eq!(
            f.resolver.ensure_loaded(record.id).await.unwrap_err(),
            first
        );
        assert_eq!(f.config_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_linked_factory_is_terminal_and_names_no_cause() {
        let record = record("resolver-linked-failing");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        let mut f = fixture(record, true, sources.clone(), None);
        f.resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources,
            linked: {
                fn failing_factory() -> crate::Result<LinkedModule> {
                    Err(crate::Error::Transport("secret path /home/x".into()))
                }
                let registry = Arc::new(LinkedRegistry::new());
                registry.register(record.id, failing_factory);
                registry
            },
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(f.sink.clone())),
        });
        let error = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(
            error.contains("its linked factory could not be started"),
            "{error}"
        );
        assert!(!error.contains("/home/x"), "{error}");
    }

    #[tokio::test]
    async fn the_resolved_hook_runs_once_before_waiters_are_released() {
        let record = record("resolver-hook");
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let recorded = seen.clone();
        let sink = Arc::new(Recording::default());
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: Some(Arc::new(move |id| recorded.lock().unwrap().push(id.into()))),
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(sink)),
        });
        let _ = resolver.ensure_loaded(record.id).await;
        let _ = resolver.ensure_loaded(record.id).await;
        assert_eq!(seen.lock().unwrap().as_slice(), &[record.id.to_string()]);
    }

    unsafe extern "C" fn never_initialised(
        _: *const crate::module::abi::TbHostVtable,
        _: *mut crate::module::abi::TbModuleVtable,
    ) -> i32 {
        panic!("a mismatched linked module must be refused before init");
    }

    #[tokio::test]
    async fn a_linked_module_that_is_not_the_requested_record_is_refused_before_attach() {
        fn impostor() -> crate::Result<LinkedModule> {
            Ok(LinkedModule {
                descriptor: crate::module::abi::TbAbiDescriptor::current("impostor", "1.0.0"),
                manifest: super::module("Impostor", &[], &[]),
                init: never_initialised,
                init_with_setup: None,
            })
        }
        let record = record("resolver-linked-impostor");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        let mut f = fixture(record, true, sources.clone(), None);
        let linked = Arc::new(LinkedRegistry::new());
        linked.register(record.id, impostor);
        f.resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources,
            linked,
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(f.sink.clone())),
        });
        let error = f.resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(
            error.contains("does not match the registry record"),
            "{error}"
        );
        assert_eq!(f.resolver.state_of(record.id), Availability::Failed);
    }

    #[test]
    fn status_reflects_the_gate_and_a_local_source_on_an_unsupported_host() {
        let record = record("resolver-status-policy");
        let sources = HashMap::from([(record.id.to_string(), ModuleSource::Linked)]);
        // No asset is published for this host, but a linked source can serve it.
        let f = fixture(record, true, sources, None);
        assert_eq!(
            f.resolver.status_of(record),
            (Availability::Available, None)
        );

        let gated = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: Some(Arc::new(|_| Some("test-switch"))),
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(f.sink.clone())),
        });
        let (state, detail) = gated.status_of(record);
        assert_eq!(state, Availability::Disabled);
        assert!(detail.unwrap().contains("test-switch"));
    }

    #[tokio::test]
    async fn a_panicking_resolved_hook_does_not_strand_waiters() {
        let record = record("resolver-hook-panics");
        let sink = Arc::new(Recording::default());
        let resolver = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: Some(Arc::new(|_| panic!("hook bug"))),
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(sink)),
        });
        let outcome = resolver
            .ensure_loaded_within(record.id, Some(Duration::from_secs(10)))
            .await;
        assert!(matches!(outcome, Err(LoadError::Failed(_))), "{outcome:?}");
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
    }

    #[tokio::test]
    async fn a_client_over_a_resolver_is_unavailable_and_reports_when_modules_are_disabled() {
        use crate::module::{ModuleCallError, ModuleClient};
        let record = record("resolver-client-disabled");
        let f = fixture(record, false, HashMap::new(), None);
        let client = ModuleClient::new(f.resolver.clone());
        let result = client.call::<()>(record.id, "Anything", ()).await;
        assert_eq!(result, Err(ModuleCallError::Unavailable));
        assert_eq!(
            f.sink.0.lock().unwrap().as_slice(),
            &[(record.id.to_string(), Reason::Disabled)]
        );
        assert_eq!(f.runtime_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_access_trait_delegates_to_the_resolver() {
        use crate::module::{ModuleAccess, ProxyFailure};
        let record = record("resolver-access");
        let f = fixture(record, true, HashMap::new(), None);
        let access: &dyn ModuleAccess = &f.resolver;
        assert_eq!(access.find(record.id), Some(record));
        assert!(access.find("nobody").is_none());
        assert!(access.enabled());
        access.reporter().report_unknown_module();
        // A valid bus address yields a proxy once the runtime is up.
        assert!(
            access
                .proxy("ai.tinyhumans.test.Access", "/ai/tinyhumans/test/Access")
                .await
                .is_ok()
        );
        // A malformed address is a contract failure, not a transport one.
        assert_eq!(
            access.proxy("not a bus name", "/nope").await.err(),
            Some(ProxyFailure::Contract)
        );
        // Loading goes through the ladder and fails here for lack of an artifact.
        assert!(access.ensure_loaded(record.id).await.is_err());

        let failing = Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::new(),
            linked: Arc::new(LinkedRegistry::new()),
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: String::new(),
            runtime: Arc::new(|| {
                Box::pin(async { Err(crate::Error::Transport("no runtime".to_string())) })
            }),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(f.sink.clone())),
        });
        assert_eq!(
            ModuleAccess::proxy(
                &failing,
                "ai.tinyhumans.test.Access",
                "/ai/tinyhumans/test/Access"
            )
            .await
            .err(),
            Some(ProxyFailure::Transport)
        );
    }

    #[test]
    fn mark_terminal_is_idempotent() {
        let record = record("resolver-marker");
        let f = fixture(record, true, HashMap::new(), None);
        let once = f.resolver.mark_terminal("boom".to_string());
        assert_eq!(
            once,
            "boom. This is terminal for the running process; restart the app to try again"
        );
        assert_eq!(f.resolver.mark_terminal(once.clone()), once);
    }

    /// What a module's `init` leaves behind for the test to drive its setup:
    /// the host's `fault`/`ready` entry points and context, as plain integers.
    struct SetupHandle {
        host_ctx: usize,
        fault: unsafe extern "C" fn(*mut std::ffi::c_void, *const u8, usize),
        ready: unsafe extern "C" fn(*mut std::ffi::c_void),
    }

    impl SetupHandle {
        fn fault(&self) {
            unsafe { (self.fault)(self.host_ctx as *mut _, std::ptr::null(), 0) };
        }

        fn ready(&self) {
            unsafe { (self.ready)(self.host_ctx as *mut _) };
        }
    }

    /// A module whose `init` returns at once while its setup is still running:
    /// it neither calls `ready` nor `fault` until the test says so.
    macro_rules! slow_setup_module {
        ($init:ident, $factory:ident, $handle:ident, $entered:ident, $id:literal) => {
            static $handle: Mutex<Option<SetupHandle>> = Mutex::new(None);
            static $entered: tokio::sync::Notify = tokio::sync::Notify::const_new();

            unsafe extern "C" fn $init(
                host: *const crate::module::abi::TbHostVtable,
                module: *mut crate::module::abi::TbModuleVtable,
            ) -> i32 {
                let host = unsafe { *host };
                unsafe {
                    *module = crate::module::abi::TbModuleVtable {
                        size: crate::module::abi::TB_MODULE_VTABLE_BASE_SIZE,
                        _reserved: 0,
                        module_ctx: Box::into_raw(Box::new(())).cast(),
                        deliver: accept_raced_message,
                        shutdown: stop_raced_module,
                        reinitialize: None,
                    }
                };
                *$handle.lock().unwrap() = Some(SetupHandle {
                    host_ctx: host.host_ctx as usize,
                    fault: host.fault,
                    ready: host.ready,
                });
                $entered.notify_one();
                crate::module::abi::TB_OK
            }

            fn $factory() -> crate::Result<LinkedModule> {
                let mut manifest = super::module($id, &[], &[]);
                manifest.bus_name = crate::BusName::new(concat!("ai.tinyhumans.test.", $id))?;
                manifest.object_path = crate::ObjectPath::new("/ai/tinyhumans/test/Module")?;
                Ok(LinkedModule {
                    descriptor: crate::module::abi::TbAbiDescriptor::current($id, "1.0.0"),
                    manifest,
                    init: $init,
                    init_with_setup: None,
                })
            }
        };
    }

    fn linked_resolver(record: &'static ReleaseRecord, factory: LinkedFactory) -> Resolver {
        Resolver::new(ResolverInputs {
            enabled: true,
            install_dir: None,
            allow_download: false,
            bundled_dir: None,
            sources: HashMap::from([(record.id.to_string(), ModuleSource::Linked)]),
            linked: {
                let registry = Arc::new(LinkedRegistry::new());
                registry.register(record.id, factory);
                registry
            },
            search_paths: None,
            search_skip: Vec::new(),
            gate: None,
            on_resolved: None,
            records: vec![record],
            terminal_marker: "This is terminal for the running process".into(),
            runtime: Arc::new(|| Box::pin(async { Ok(shared_runtime()) })),
            module_config: Arc::new(|_| serde_json::json!({})),
            async_module_config: None,
            reporter: Arc::new(FailureReporter::new(Arc::new(Recording::default()))),
        })
    }

    async fn wait_until_attached(record: &ReleaseRecord) {
        for _ in 0..2000 {
            if shared_runtime()
                .host()
                .list()
                .iter()
                .any(|info| info.manifest.bus_name.as_str() == record.bus_name)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("module never finished attaching");
    }

    slow_setup_module!(
        init_then_fault,
        faulting_factory,
        FAULTING_HANDLE,
        FAULTING_ENTERED,
        "resolver_setup_faults"
    );

    #[tokio::test]
    async fn a_module_whose_setup_faults_after_load_fails_the_load_with_its_reason() {
        let record = record("resolver_setup_faults");
        let resolver = linked_resolver(record, faulting_factory);
        let load = tokio::spawn({
            let resolver = resolver.clone();
            async move { resolver.ensure_loaded(record.id).await }
        });
        FAULTING_ENTERED.notified().await;
        // `init` signals from inside attachment; faulting before the host has
        // registered the module races admission instead of exercising a fault
        // raised during setup.
        wait_until_attached(record).await;
        FAULTING_HANDLE.lock().unwrap().as_ref().unwrap().fault();

        let error = load.await.unwrap().unwrap_err();
        assert!(
            error.contains("faulted during setup")
                && error.contains("module reported an unrecoverable fault"),
            "load must carry the module's reason, got: {error}"
        );
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
        let again = resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(again.contains("faulted during setup"), "{again}");
    }

    slow_setup_module!(
        init_then_ready,
        pending_factory,
        PENDING_HANDLE,
        PENDING_ENTERED,
        "resolver_setup_pending"
    );

    #[tokio::test]
    async fn a_module_is_not_ready_until_its_setup_signals_ready() {
        let record = record("resolver_setup_pending");
        let resolver = linked_resolver(record, pending_factory);
        let first = tokio::spawn({
            let resolver = resolver.clone();
            async move { resolver.ensure_loaded(record.id).await }
        });
        PENDING_ENTERED.notified().await;
        // A deadline, not a sleep: nothing signals ready, so a bounded wait
        // must run out while the module is still loading.
        assert_eq!(
            resolver
                .ensure_loaded_within(record.id, Some(Duration::from_millis(200)))
                .await,
            Err(LoadError::StillLoading),
            "the module was reported ready while its setup was still running"
        );
        assert_eq!(resolver.state_of(record.id), Availability::Loading);
        PENDING_HANDLE.lock().unwrap().as_ref().unwrap().ready();

        first.await.unwrap().unwrap();
        assert_eq!(resolver.state_of(record.id), Availability::Ready);
    }

    slow_setup_module!(
        init_ready_later_faults,
        late_fault_factory,
        LATE_HANDLE,
        LATE_ENTERED,
        "resolver_late_fault"
    );

    #[tokio::test]
    async fn a_cached_ready_is_not_served_once_the_module_has_faulted() {
        let record = record("resolver_late_fault");
        let resolver = linked_resolver(record, late_fault_factory);
        let load = tokio::spawn({
            let resolver = resolver.clone();
            async move { resolver.ensure_loaded(record.id).await }
        });
        LATE_ENTERED.notified().await;
        LATE_HANDLE.lock().unwrap().as_ref().unwrap().ready();
        load.await.unwrap().unwrap();
        assert_eq!(resolver.state_of(record.id), Availability::Ready);

        LATE_HANDLE.lock().unwrap().as_ref().unwrap().fault();
        let error = resolver.ensure_loaded(record.id).await.unwrap_err();
        assert!(error.contains("faulted during setup"), "{error}");
        assert_eq!(resolver.state_of(record.id), Availability::Failed);
    }

    #[test]
    fn wire_forms_of_source_and_availability_are_stable() {
        assert_eq!(
            serde_json::to_value(ModuleSource::Pinned).unwrap(),
            serde_json::json!({"kind": "pinned"})
        );
        assert_eq!(
            serde_json::to_value(ModuleSource::LocalFile { path: "/x".into() }).unwrap(),
            serde_json::json!({"kind": "local_file", "path": "/x"})
        );
        assert_eq!(
            serde_json::to_value(ModuleSource::Linked).unwrap(),
            serde_json::json!({"kind": "linked"})
        );
        for (state, wire) in [
            (Availability::Available, "available"),
            (Availability::Loading, "loading"),
            (Availability::Ready, "ready"),
            (Availability::Failed, "failed"),
            (Availability::Unsupported, "unsupported"),
            (Availability::Disabled, "disabled"),
        ] {
            assert_eq!(
                serde_json::to_value(state).unwrap(),
                serde_json::json!(wire)
            );
        }
    }
}

#[test]
fn bundled_directory_lookup_uses_registered_then_env_then_executable_sibling() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let registered = root.path().join("registered");
    let from_env = root.path().join("environment");
    let exe_dir = root.path().join("bin");
    let sibling = exe_dir.join("bundled-modules");
    for directory in [&registered, &from_env, &sibling] {
        std::fs::create_dir_all(directory).unwrap();
    }

    assert_eq!(
        resolve_bundled_dir_from(
            Some(registered.clone()),
            Some(from_env.clone()),
            Some(exe_dir.clone()),
        ),
        Some(registered)
    );
    assert_eq!(
        resolve_bundled_dir_from(None, Some(from_env.clone()), Some(exe_dir.clone())),
        Some(from_env)
    );
    assert_eq!(
        resolve_bundled_dir_from(None, None, Some(exe_dir)),
        Some(sibling)
    );
}

#[test]
fn bundled_directory_lookup_skips_missing_higher_precedence_candidates() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let missing_registered = root.path().join("not-installed");
    let valid_env = root.path().join("environment");
    std::fs::create_dir_all(&valid_env).unwrap();

    assert_eq!(
        resolve_bundled_dir_from(Some(missing_registered), Some(valid_env.clone()), None),
        Some(valid_env)
    );
}

#[test]
fn bundled_directory_lookup_reads_the_explicit_environment_variable_name() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let from_env = root.path().join("environment");
    std::fs::create_dir_all(&from_env).unwrap();
    let env_var = "TINYBUS_TEST_BUNDLED_RELEASES";
    unsafe { std::env::set_var(env_var, &from_env) };

    let resolved_from_env = resolve_bundled_dir(env_var);
    unsafe { std::env::remove_var(env_var) };
    assert_eq!(resolved_from_env, Some(from_env));

    let registered = root.path().join("registered");
    std::fs::create_dir_all(&registered).unwrap();
    assert!(set_bundled_releases_dir(registered.clone()).is_ok());
    assert_eq!(resolve_bundled_dir(env_var), Some(registered));
}

#[test]
fn linked_registry_replaces_factories_and_returns_the_last_registration() {
    fn first_factory() -> crate::Result<LinkedModule> {
        Err(crate::Error::failed("first factory"))
    }
    fn replacement_factory() -> crate::Result<LinkedModule> {
        Err(crate::Error::failed("replacement factory"))
    }

    let registry = LinkedRegistry::new();
    assert!(!registry.is_registered("registry-test"));
    assert!(registry.get("registry-test").is_none());

    registry.register("registry-test", first_factory);
    assert!(registry.is_registered("registry-test"));
    let first = (registry.get("registry-test").unwrap())();
    assert!(matches!(first, Err(error) if error.to_string().contains("first factory")));

    registry.register("registry-test", replacement_factory);
    let replacement = (registry.get("registry-test").unwrap())();
    assert!(matches!(replacement, Err(error) if error.to_string().contains("replacement factory")));

    let global = LinkedRegistry::global();
    let same_global = LinkedRegistry::global();
    global.register("global-registry-test", first_factory);
    assert!(same_global.is_registered("global-registry-test"));
    let global_result = (same_global.get("global-registry-test").unwrap())();
    assert!(matches!(global_result, Err(error) if error.to_string().contains("first factory")));

    let default_registry = LinkedRegistry::default();
    assert!(!default_registry.is_registered("registry-test"));
}

struct ResolverSearchSink;

impl crate::module::FailureSink for ResolverSearchSink {
    fn capture(&self, _: &crate::module::FailureReport<'_>) {}
}

#[tokio::test]
async fn resolver_inputs_configure_the_runtime_host_search_paths() {
    let root = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let search_dir = root.path().join("resolver-search");
    std::fs::create_dir(&search_dir).unwrap();
    let env_var = "TINYBUS_TEST_RESOLVER_SEARCH_PATH";
    unsafe { std::env::set_var(env_var, &search_dir) };
    let runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
        crate::module::ModuleRuntime::start(&crate::module::RuntimeNames::default(), None).unwrap(),
    ));
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::new(),
        linked: Arc::new(LinkedRegistry::new()),
        search_paths: Some(SearchPathConfig::new(env_var, "test-host/modules")),
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: Vec::new(),
        terminal_marker: String::new(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::Value::Null),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(Arc::new(ResolverSearchSink))),
    });

    let resolved_runtime = resolver.runtime().await.unwrap();
    let paths = resolved_runtime.host().search_paths();
    unsafe { std::env::remove_var(env_var) };

    assert_eq!(paths.first(), Some(&search_dir));
}

fn info_in(state: ModuleState) -> ModuleInfo {
    ModuleInfo {
        name: "probe".into(),
        version: "1.0.0".into(),
        file: "module.so".into(),
        state,
        manifest: module("probe", &[], &[]),
        rustc_version: String::new(),
        rustc_mismatch: false,
        enabled: true,
    }
}

#[test]
fn setup_outcome_names_why_a_module_is_not_serving() {
    let reason = || "because".to_string();
    let err = |state| setup_outcome(&info_in(state), "probe").unwrap_err();
    assert!(
        err(ModuleState::Faulted { reason: reason() }).contains("faulted during setup: because")
    );
    assert!(err(ModuleState::Failed { reason: reason() }).contains("faulted during setup"));
    assert!(err(ModuleState::Rejected { reason: reason() }).contains("was refused: because"));
    assert!(err(ModuleState::Unresolved { reason: reason() }).contains("was refused"));
    assert!(err(ModuleState::Stopped).contains("is not running"));
    assert!(err(ModuleState::Disabled).contains("is not running"));
    assert!(err(ModuleState::Initializing).contains("did not finish setup within 30 seconds"));
    assert!(setup_outcome(&info_in(ModuleState::Ready), "probe").is_ok());
}

#[test]
fn load_error_messages_are_fit_for_a_user() {
    assert_eq!(LoadError::Failed("x".into()).into_message(), "x");
    assert!(
        LoadError::StillLoading
            .into_message()
            .contains("still loading")
    );
    assert!(
        LoadError::Disabled("voice")
            .into_message()
            .contains("(voice)")
    );
}
