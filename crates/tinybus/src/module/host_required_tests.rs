//! Required manifest admission must precede the setup callback.
use super::*;
use crate::module::{ModuleRequirements, RequiredInterface};
use crate::name::{InterfaceName, MemberName};
use crate::version::VersionRange;
use std::sync::atomic::{AtomicUsize, Ordering};

static SETUPS: AtomicUsize = AtomicUsize::new(0);
static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

unsafe extern "C" fn counted_setup(
    _: *const crate::module::abi::TbHostVtable,
    _: *mut TbModuleVtable,
) -> i32 {
    SETUPS.fetch_add(1, Ordering::SeqCst);
    crate::module::abi::TB_BAD_ARGUMENT
}

fn required() -> ModuleRequirements {
    ModuleRequirements::new(
        "clock",
        BusName::new("ai.tinyhumans.module.Clock").unwrap(),
        ObjectPath::new("/ai/tinyhumans/module/Clock").unwrap(),
        vec![
            RequiredInterface::new(
                InterfaceName::new("ai.tinyhumans.module.Clock").unwrap(),
                VersionRange::new(Version::new(1, 15, 0), Some(Version::new(2, 0, 0))),
                vec![MemberName::new("RoutedRead").unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn an_unsupported_actual_manifest_is_refused_before_setup() {
    let _lock = LOCK.lock().unwrap();
    SETUPS.store(0, Ordering::SeqCst);
    let host = ModuleHost::new(Broker::new());
    let artifact = LoadedArtifact {
        descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
        manifest: super::tests::manifest(),
        init: counted_setup,
        library_sha256: None,
    };
    assert!(
        host.activate_required(
            Path::new("clock.so"),
            artifact,
            serde_json::json!({}),
            &required()
        )
        .is_err()
    );
    assert_eq!(
        SETUPS.load(Ordering::SeqCst),
        0,
        "incompatible setup must never run"
    );
}

#[tokio::test]
async fn a_provisional_lazy_sidecar_cannot_satisfy_activation_requirements() {
    let directory = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let filename = if cfg!(windows) {
        "clock.dll"
    } else if cfg!(target_os = "macos") {
        "clock.dylib"
    } else {
        "clock.so"
    };
    let path = directory.path().join(filename);
    std::fs::write(&path, b"not an actual module").unwrap();
    let mut manifest = super::tests::manifest();
    manifest.lazy_init = true;
    std::fs::write(
        path.with_file_name(format!("{filename}.manifest.json")),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let host = ModuleHost::new(Broker::new());
    assert!(
        host.load_file_with_requirements(&path, serde_json::json!({}), &required())
            .is_err(),
        "lazy metadata cannot authorize initialization"
    );
    assert!(
        host.list().is_empty(),
        "a refused artifact must not register for later calls"
    );
}

fn matching_manifest() -> ModuleManifest {
    let mut manifest = super::tests::manifest();
    manifest
        .provides
        .push(super::super::manifest::ProvidedInterface {
            version: crate::version::InterfaceVersion {
                interface: InterfaceName::new("ai.tinyhumans.module.Clock").unwrap(),
                speaks: Version::new(1, 15, 0),
                accepts: VersionRange::at_least(Version::new(1, 0, 0)),
            },
            methods: vec![MemberName::new("RoutedRead").unwrap()],
            signals: vec![],
        });
    manifest
}

#[test]
fn the_matching_loaded_artifact_reaches_its_original_initializer() {
    let _lock = LOCK.lock().unwrap();
    SETUPS.store(0, Ordering::SeqCst);
    let host = ModuleHost::new(Broker::new());
    let artifact = LoadedArtifact {
        descriptor: TbAbiDescriptor::current("clock", "0.1.0"),
        manifest: matching_manifest(),
        init: counted_setup,
        library_sha256: None,
    };
    let error = host
        .activate_required(
            Path::new("clock.so"),
            artifact,
            serde_json::json!({}),
            &required(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("initialization failed"));
    assert_eq!(SETUPS.load(Ordering::SeqCst), 1);
}

#[test]
fn requirements_compare_identity_address_versions_and_methods_on_one_interface() {
    let required = required();
    let original = matching_manifest();
    required.check(&original).unwrap();
    let mut changed = original.clone();
    changed.module.name = "different".into();
    assert!(required.check(&changed).is_err());
    let mut changed = original.clone();
    changed.bus_name = BusName::new("ai.tinyhumans.module.Other").unwrap();
    assert!(required.check(&changed).is_err());
    let mut changed = original.clone();
    changed.object_path = ObjectPath::new("/other").unwrap();
    assert!(required.check(&changed).is_err());
    let mut changed = original.clone();
    changed.provides[0].version.speaks = Version::new(1, 14, 0);
    assert!(required.check(&changed).is_err());
    let mut changed = original.clone();
    changed.provides[0].version.speaks = Version::new(2, 0, 0);
    assert!(required.check(&changed).is_err());
    let mut changed = original.clone();
    changed.provides[0].methods.clear();
    assert!(required.check(&changed).is_err());
    let mut changed = original;
    changed.provides[0].version.interface =
        InterfaceName::new("ai.tinyhumans.module.Other").unwrap();
    assert!(required.check(&changed).is_err());
}

#[test]
fn invalid_requirements_are_refused_without_echoing_input() {
    let bus = BusName::new("ai.tinyhumans.module.Clock").unwrap();
    let path = ObjectPath::new("/clock").unwrap();
    for module in ["", "secret\nvalue", &"a".repeat(257)] {
        let error = ModuleRequirements::new(module, bus.clone(), path.clone(), vec![]).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    assert!(
        ModuleRequirements::new("clock", BusName::new(":1.42").unwrap(), path, vec![]).is_err()
    );
    assert!(
        RequiredInterface::new(
            InterfaceName::new("ai.tinyhumans.module.Clock").unwrap(),
            VersionRange::new(Version::new(2, 0, 0), Some(Version::new(1, 0, 0))),
            vec![]
        )
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
async fn a_real_required_artifact_is_refused_then_admitted_and_serves_confidentially() {
    use crate::Connection;
    use crate::transport::memory::MemoryBus;
    let artifact = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    // Windows CI supplies an owner-only TEMP; the checkout can inherit group
    // write permissions. Canonicalize Unix TEMP to avoid macOS /var symlinks.
    let directory = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    let private_path = std::fs::canonicalize(directory.path()).unwrap();
    #[cfg(windows)]
    let private_path = directory.path().to_path_buf();
    let filename = artifact.file_name().unwrap().to_str().unwrap();
    let staged = private_path.join(filename);
    std::fs::copy(&artifact, &staged).unwrap();
    let hash = crate::module::sha256_file(&staged).unwrap();
    std::fs::write(
        private_path.join("modules.toml"),
        format!("\"{filename}\" = \"{hash}\"\n"),
    )
    .unwrap();
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let address = BusName::new("ai.tinyhumans.openhuman.Clock").unwrap();
    let path = ObjectPath::new("/ai/tinyhumans/openhuman/Clock").unwrap();
    let accepted_interface_versions = Version::parse(env!("CARGO_PKG_VERSION"))
        .expect("package version is a valid interface version")
        .compatible_series();
    let required_for = |method| {
        ModuleRequirements::new(
            "tinybus",
            address.clone(),
            path.clone(),
            vec![
                RequiredInterface::new(
                    InterfaceName::new(address.as_str()).unwrap(),
                    accepted_interface_versions.clone(),
                    vec![MemberName::new(method).unwrap()],
                )
                .unwrap(),
            ],
        )
        .unwrap()
    };
    assert!(
        host.load_file_with_requirements(&staged, serde_json::json!({}), &required_for("Missing"))
            .is_err()
    );
    assert!(host.list().is_empty());
    host.load_file_with_requirements(
        &staged,
        serde_json::json!({"prefix":"required:"}),
        &required_for("Now"),
    )
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(address.as_str(), path.as_str(), address.as_str())
        .unwrap();
    let reply: String = proxy.call_confidential("Now", ()).await.unwrap();
    assert!(reply.starts_with("required:"));
    assert_eq!(
        connection
            .attestation(address)
            .await
            .unwrap()
            .unwrap()
            .sha256,
        hash
    );
    broker_task.abort();
}
