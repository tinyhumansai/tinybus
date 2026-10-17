use super::*;
use crate::module::abi::{TB_BAD_ARGUMENT, TB_OK, TbAbiDescriptor, TbHostVtable, TbModuleVtable};
use crate::module::failure::{FailureReport, FailureSink};
use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy};
use crate::module::resolution::{self, Claim, Resolution, ResolutionState};
use crate::module::runtime::{ModuleRuntime, RuntimeNames};
use crate::module::{LinkedModule, ReleaseRecord, ResolverInputs};
use crate::name::{BusName, ObjectPath};
use crate::version::Version;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, OnceLock};

const ID: &str = "resolveraccessreinit";
const BUS: &str = "ai.tinyhumans.test.ResolverAccess";
const PATH: &str = "/ai/tinyhumans/test/ResolverAccess";
static RECORD: ReleaseRecord = ReleaseRecord {
    id: ID,
    bus_name: BUS,
    object_path: PATH,
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};

struct Quiet;
impl FailureSink for Quiet {
    fn capture(&self, _: &FailureReport<'_>) {}
}

fn runtime() -> &'static ModuleRuntime {
    static RUNTIME: OnceLock<ModuleRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| ModuleRuntime::start(&RuntimeNames::default(), None).unwrap())
}

fn resolver(runtime_ready: bool) -> Resolver {
    let runtime: crate::module::RuntimeProvider = if runtime_ready {
        Arc::new(|| Box::pin(async { Ok(runtime()) }))
    } else {
        Arc::new(|| Box::pin(async { Err(crate::Error::transport("runtime unavailable")) }))
    };
    Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::new(),
        linked: Arc::new(crate::module::LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RECORD],
        terminal_marker: String::new(),
        runtime,
        module_config: Arc::new(|_| serde_json::Value::Null),
        reporter: Arc::new(FailureReporter::new(Arc::new(Quiet))),
    })
}

fn manifest() -> ModuleManifest {
    ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: ID.to_owned(),
            version: Version::new(0, 0, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: BusName::new(BUS).unwrap(),
        object_path: ObjectPath::new(PATH).unwrap(),
        provides: vec![],
        requires: vec![],
        environment: vec![],
        capabilities: vec![],
        lazy_init: false,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    }
}

unsafe extern "C" fn deliver(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}

unsafe extern "C" fn shutdown(_: *mut c_void, _: u64) -> i32 {
    TB_OK
}

unsafe extern "C" fn reinitialize(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}

unsafe extern "C" fn initialize(host: *const TbHostVtable, out: *mut TbModuleVtable) -> i32 {
    if host.is_null() || out.is_null() {
        return TB_BAD_ARGUMENT;
    }
    let host = unsafe { *host };
    let module_ctx = Box::into_raw(Box::new(0_u8)).cast();
    unsafe { (host.ready)(host.host_ctx) };
    unsafe {
        *out = TbModuleVtable {
            size: std::mem::size_of::<TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx,
            deliver,
            shutdown,
            reinitialize: Some(reinitialize),
        };
    }
    TB_OK
}

#[tokio::test]
async fn resolver_access_forwards_runtime_proxy_lifecycle_and_fault_operations() {
    let resolver = resolver(true);
    let access: &dyn ModuleAccess = &resolver;
    assert_eq!(access.find(ID).map(|record| record.id), Some(ID));
    assert!(access.enabled());
    assert!(std::ptr::eq(access.reporter(), resolver.reporter()));
    assert!(access.ensure_loaded("not-in-registry").await.is_err());
    assert!(access.proxy(BUS, PATH).await.is_ok());
    assert_eq!(
        access.proxy("not a bus name", PATH).await.unwrap_err(),
        ProxyFailure::Contract
    );

    let runtime = runtime();
    runtime
        .host()
        .attach_linked_with_config(
            LinkedModule {
                descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
                manifest: manifest(),
                init: initialize,
                init_with_setup: None,
            },
            serde_json::Value::Null,
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if runtime.host().list().iter().any(|info| {
                info.name == ID
                    && matches!(
                        &info.state,
                        crate::module::ModuleState::Ready | crate::module::ModuleState::Serving
                    )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    access
        .reinitialize(ID, serde_json::json!({ "route": "replacement" }))
        .await
        .unwrap();
    assert_eq!(
        access
            .reinitialize("missingnotloaded", serde_json::Value::Null)
            .await,
        Err(ReconfigureFailure::Failed)
    );

    let faulted_id = "resolveraccessfaulted";
    let Claim::Run { sender, .. } = resolution::global().claim(faulted_id) else {
        panic!("new test id must claim the unresolved slot");
    };
    resolution::global().complete(faulted_id, Resolution::Ready, sender);
    access.mark_faulted(faulted_id);
    assert_eq!(
        resolution::global().peek(faulted_id),
        ResolutionState::Failed("module reconfiguration failed".to_owned())
    );
}

#[tokio::test]
async fn runtime_failures_map_to_proxy_transport_and_reconfigure_unavailable() {
    let resolver = resolver(false);
    let access: &dyn ModuleAccess = &resolver;
    assert_eq!(
        access.proxy(BUS, PATH).await.unwrap_err(),
        ProxyFailure::Transport
    );
    assert_eq!(
        access.reinitialize(ID, serde_json::Value::Null).await,
        Err(ReconfigureFailure::Unavailable)
    );
}

#[test]
fn resolver_access_reconfigure_failure_uses_only_closed_error_state() {
    assert_eq!(
        reconfigure_failure(&crate::Error::ModuleUnavailable {
            module: ID.to_owned(),
            state: "faulted".to_owned(),
            detail: "terminal".to_owned(),
        }),
        ReconfigureFailure::Unavailable
    );
    assert_eq!(
        reconfigure_failure(&crate::Error::MethodFailed {
            name: "ai.tinyhumans.tinybus.Error.Other".to_owned(),
            message: "module initializing".to_owned(),
        }),
        ReconfigureFailure::Failed
    );
}
