//! Two modules in one executable must keep distinct symbols and manifests.

mod first {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticFirst"],
        methods = ["First"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

mod second {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticSecond"],
        methods = ["Second"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

mod configured {
    #[derive(serde::Deserialize)]
    struct Config {}

    async fn setup(_: tinybus::Connection, _: Config) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        config = Config,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticConfigured"],
        methods = ["Configured"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

mod optional {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_optional_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.OptionalStatic"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[test]
fn linked_modules_retain_distinct_manifests() {
    fn manifest(slice: tinybus::module::abi::TbSlice) -> tinybus::module::manifest::ModuleManifest {
        let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
        serde_json::from_slice(bytes).expect("generated manifest")
    }

    let first_manifest = manifest(first::tinybus_module_manifest_v1());
    let second_manifest = manifest(second::tinybus_module_manifest_v1());
    let configured_manifest = manifest(configured::tinybus_module_manifest_v1());
    assert_eq!(
        first_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticFirst"
    );
    assert_eq!(
        second_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticSecond"
    );
    assert_eq!(
        configured_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticConfigured"
    );
    assert_eq!(first::linked_module().unwrap().manifest, first_manifest);
    assert_eq!(second::linked_module().unwrap().manifest, second_manifest);
    assert_eq!(
        configured::linked_module().unwrap().manifest,
        configured_manifest
    );
    let _entries = (
        &first::TINYBUS_MODULE_ABI_V1,
        first::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
        &second::TINYBUS_MODULE_ABI_V1,
        second::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
        &configured::TINYBUS_MODULE_ABI_V1,
        configured::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
    );
    assert!(!optional::tinybus_module_manifest_v1().ptr.is_null());
    #[cfg(feature = "static-link")]
    assert_eq!(
        optional::linked_module().unwrap().manifest.module.name,
        "tinybus-module"
    );
}

#[test]
fn invalid_linked_manifest_never_exposes_partial_bytes() {
    static BYTES: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    let slice = crate::manifest_slice_in(
        &BYTES,
        crate::ManifestDeclaration {
            name: "invalid",
            version: "not-semver",
            provides: &["ai.tinyhumans.tinybus.Invalid"],
            methods: &[],
            signals: &[],
            requires: &[],
            optional: &[],
            lazy: false,
            worker_threads: 1,
        },
    );
    assert!(slice.ptr.is_null());
    assert_eq!(slice.len, 0);
    assert!(BYTES.get().is_none());
}

#[test]
fn linked_helper_refuses_an_invalid_descriptor() {
    let mut descriptor = first::TINYBUS_MODULE_ABI_V1;
    descriptor.magic = 0;
    let error = unsafe {
        tinybus::module::LinkedModule::from_exports(
            &descriptor,
            first::tinybus_module_manifest_v1,
            first::tinybus_module_init_v1,
        )
    }
    .err()
    .expect("invalid descriptor must be rejected");
    assert!(error.to_string().contains("ABI magic does not match"));
}

#[tokio::test]
async fn linked_entries_attach_to_one_broker() {
    fn manifest(slice: tinybus::module::abi::TbSlice) -> tinybus::module::manifest::ModuleManifest {
        let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
        serde_json::from_slice(bytes).expect("generated manifest")
    }

    let bus = tinybus::transport::memory::MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let broker_task = broker.spawn(bus);
    let host = tinybus::module::ModuleHost::new(broker);
    let first_info = unsafe {
        host.attach_raw(
            "linked-first",
            first::TINYBUS_MODULE_ABI_V1,
            manifest(first::tinybus_module_manifest_v1()),
            first::tinybus_module_init_v1,
        )
    }
    .expect("first linked module attaches");
    let mut configured_descriptor = configured::TINYBUS_MODULE_ABI_V1;
    configured_descriptor.module_name = [0; 64];
    configured_descriptor.module_name[..10].copy_from_slice(b"configured");
    let mut configured_manifest = manifest(configured::tinybus_module_manifest_v1());
    configured_manifest.module.name = "configured".to_string();
    let second_info = unsafe {
        host.attach_raw_with_config(
            "linked-configured",
            configured_descriptor,
            configured_manifest,
            configured::tinybus_module_init_v1,
            serde_json::json!({}),
        )
    }
    .expect("configured linked module attaches");
    assert_ne!(first_info.manifest.bus_name, second_info.manifest.bus_name);
    assert_eq!(host.list().len(), 2);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while host
            .list()
            .iter()
            .any(|module| !matches!(module.state, tinybus::module::ModuleState::Ready))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("linked modules did not become ready: {:?}", host.list()));
    broker_task.abort();
}
