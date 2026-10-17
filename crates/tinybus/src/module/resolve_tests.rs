use super::*;
use crate::module::manifest::{
    Dependency, MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy, ProvidedInterface,
};
use crate::{BusName, InterfaceName, InterfaceVersion, ObjectPath, Version};

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
    use super::super::test_support::{Recording, fixture, record, shared_runtime};
    use super::super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        reporter: Arc::new(FailureReporter::new(Arc::new(ResolverSearchSink))),
    });

    let resolved_runtime = resolver.runtime().await.unwrap();
    let paths = resolved_runtime.host().search_paths();
    unsafe { std::env::remove_var(env_var) };

    assert_eq!(paths.first(), Some(&search_dir));
}
