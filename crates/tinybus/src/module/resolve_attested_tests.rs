use super::*;

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built module_clock cdylib"]
async fn a_local_artifact_is_loaded_and_reported_ready() {
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let runtime =
        crate::module::ModuleRuntime::start(&crate::module::RuntimeNames::default(), None).unwrap();

    Resolver::load_local(&runtime, &artifact, "clock", serde_json::Value::Null).unwrap();

    let loaded = runtime.host().list();
    assert!(
        loaded.iter().any(|info| {
            info.manifest.bus_name.as_str() == "ai.tinyhumans.openhuman.Clock"
                && info.manifest.object_path.as_str() == "/ai/tinyhumans/openhuman/Clock"
        }),
        "loaded modules: {loaded:#?}"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built module_clock cdylib"]
async fn the_first_strict_call_prepares_a_real_lazy_resolver_module_without_a_probe() {
    use crate::module::{ModuleAccess, ModuleCallError, ModuleClient, ProxyFailure};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let library_source = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let install = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let host_key = crate::module::platform::host_candidates()
        .into_iter()
        .next()
        .expect("test host has a published module platform key");
    let module_id: &'static str = Box::leak("resolver-attested-lazy".to_string().into_boxed_str());
    let archive_name: &'static str =
        Box::leak("clock-attested-test.tar.gz".to_string().into_boxed_str());
    let version = "0.1.7";
    let bus_name = "ai.tinyhumans.openhuman.Clock";
    let object_path = "/ai/tinyhumans/openhuman/Clock";
    let host_key: &'static str = Box::leak(host_key.into_boxed_str());
    let library_name = library_source
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap()
        .to_string();
    let cache_dir =
        crate::module::artifact_dir(install.path(), module_id, version, host_key).unwrap();
    let extraction = cache_dir.join("assets");
    std::fs::create_dir_all(&extraction).unwrap();
    let library_path = extraction.join(&library_name);
    std::fs::copy(&library_source, &library_path).unwrap();
    let library_sha256 = crate::module::sha256_file(&library_path).unwrap();
    std::fs::write(
        extraction.join("modules.toml"),
        format!("{library_name:?} = \"{library_sha256}\"\n"),
    )
    .unwrap();

    let archive_path = cache_dir.join(archive_name);
    let encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(&archive_path).unwrap(),
        flate2::Compression::default(),
    );
    let mut archive = tar::Builder::new(encoder);
    for (name, bytes) in [
        (
            format!("assets/{library_name}"),
            std::fs::read(&library_path).unwrap(),
        ),
        (
            "assets/modules.toml".to_string(),
            std::fs::read(extraction.join("modules.toml")).unwrap(),
        ),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append_data(&mut header, name, &bytes[..]).unwrap();
    }
    archive.into_inner().unwrap().finish().unwrap();
    let archive_sha256 = crate::module::sha256_file(&archive_path).unwrap();
    crate::module::cache::write_digest_marker(&cache_dir, archive_name, &archive_sha256).unwrap();

    let assets = Box::leak(
        vec![crate::module::ReleaseAsset {
            host_key,
            archive: archive_name,
            sha256: Box::leak(archive_sha256.into_boxed_str()),
            library_sha256: Some(Box::leak(library_sha256.into_boxed_str())),
        }]
        .into_boxed_slice(),
    );
    let record = Box::leak(Box::new(ReleaseRecord {
        id: module_id,
        bus_name,
        object_path,
        version,
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.1.7",
        assets,
    }));
    // Other ignored loader tests attach module_clock to a process-shared
    // runtime under the same well-known bus name. Give this strict-call test
    // its own host so parallel test execution cannot race that admission.
    let runtime: &'static crate::module::ModuleRuntime = Box::leak(Box::new(
        crate::module::ModuleRuntime::start(&crate::module::RuntimeNames::default(), None).unwrap(),
    ));
    let sink = Arc::new(super::test_support::Recording::default());
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: Some(install.path().to_path_buf()),
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::from([(module_id.to_string(), ModuleSource::Pinned)]),
        linked: Arc::new(LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![record],
        terminal_marker: "This is terminal for the running process".to_string(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::json!({})),
        async_module_config: None,
        reporter: Arc::new(crate::module::FailureReporter::new(sink)),
    });

    struct CountedResolver {
        resolver: Resolver,
        preparations: Arc<AtomicUsize>,
        preparation_error: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl ModuleAccess for CountedResolver {
        fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
            self.resolver.find(id)
        }

        fn enabled(&self) -> bool {
            self.resolver.enabled()
        }

        fn reporter(&self) -> &FailureReporter {
            self.resolver.reporter()
        }

        fn ensure_loaded<'a>(
            &'a self,
            id: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<(), String>> {
            Box::pin(self.resolver.ensure_loaded(id))
        }

        fn prepare_attested<'a>(
            &'a self,
            id: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<(), String>> {
            self.preparations.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let result = self.resolver.prepare_attested(id).await;
                if let Err(error) = &result {
                    *self
                        .preparation_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
                }
                result
            })
        }

        fn proxy<'a>(
            &'a self,
            bus_name: &'a str,
            object_path: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<crate::Proxy, ProxyFailure>> {
            Box::pin(async move {
                self.resolver
                    .runtime()
                    .await
                    .map_err(|_| ProxyFailure::Transport)?
                    .proxy(bus_name, object_path)
                    .map_err(|_| ProxyFailure::Contract)
            })
        }
    }

    let preparations = Arc::new(AtomicUsize::new(0));
    let preparation_error = Arc::new(std::sync::Mutex::new(None));
    let client = ModuleClient::new(CountedResolver {
        resolver: resolver.clone(),
        preparations: preparations.clone(),
        preparation_error: preparation_error.clone(),
    });
    let response: std::result::Result<String, ModuleCallError> =
        client.call_attested(record.id, "Now", ()).await;
    assert!(
        response.is_ok(),
        "{response:?}; prepare error: {:?}",
        *preparation_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.state_of(record.id), Availability::Ready);
}
