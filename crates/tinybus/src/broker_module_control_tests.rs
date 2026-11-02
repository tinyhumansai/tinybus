use super::*;
use crate::Connection;
use crate::module::host::{ModuleControl, ModuleInfo, ModuleState, ModuleTransition};
use crate::transport::memory::MemoryTransport;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DENIED: &str = "ai.tinyhumans.tinybus.Error.ModuleControlDenied";
const SLOW_MODULE: &str = "slow.so";
const SLOW_LOAD: Duration = Duration::from_millis(1500);

#[derive(Default)]
struct RecordingControl(Mutex<Vec<&'static str>>);

impl RecordingControl {
    fn changed(&self, operation: &'static str) -> ModuleInfo {
        self.0.lock().unwrap().push(operation);
        ModuleInfo {
            name: "fixture".into(),
            version: "1.0.0".into(),
            file: "fixture.so".into(),
            state: ModuleState::Ready,
            manifest: serde_json::from_value(serde_json::json!({
                "tinybus_manifest": 1,
                "module": {"name": "fixture", "version": "1.0.0"},
                "bus_name": "ai.tinyhumans.Fixture",
                "object_path": "/ai/tinyhumans/Fixture"
            }))
            .unwrap(),
            rustc_version: String::new(),
            rustc_mismatch: false,
            enabled: true,
        }
    }
}

#[async_trait::async_trait]
impl ModuleControl for RecordingControl {
    fn list(&self) -> Vec<ModuleInfo> {
        Vec::new()
    }
    fn load(
        self: Arc<Self>,
        path: PathBuf,
        _: Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        if path == Path::new(SLOW_MODULE) {
            // Mapping a real library, or a GitHub load's download, blocks.
            std::thread::sleep(SLOW_LOAD);
        }
        Ok((self.changed("LoadModule"), None))
    }
    fn load_github(
        self: Arc<Self>,
        _: String,
        _: String,
        _: String,
        _: Value,
    ) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        Ok((self.changed("LoadGithubModule"), None))
    }
    async fn stop(&self, _: &str, _: Duration) -> Result<ModuleInfo> {
        Ok(self.changed("StopModule"))
    }
    async fn stop_after_lifecycle(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        self.stop(name, deadline).await
    }
    async fn stop_inner(&self, name: &str, deadline: Duration) -> Result<ModuleInfo> {
        self.stop(name, deadline).await
    }
    async fn reinitialize(&self, _: &str, _: Value) -> Result<ModuleInfo> {
        Ok(self.changed("ReinitializeModule"))
    }
    fn enable(&self, _: &str, _: bool) -> Result<(ModuleInfo, Option<ModuleTransition>)> {
        Ok((self.changed("EnableModule"), None))
    }
    fn rescan(
        self: Arc<Self>,
        _: Vec<PathBuf>,
        _: bool,
    ) -> Result<(Vec<ModuleInfo>, Vec<ModuleTransition>)> {
        self.changed("RescanModules");
        Ok((Vec::new(), Vec::new()))
    }
    fn peer_detached(&self, _: &BusName) -> Option<ModuleTransition> {
        None
    }
    fn unavailable_for(&self, _: &BusName) -> Option<Error> {
        None
    }
}

async fn peer(broker: &Broker, controller: bool) -> Connection {
    let (server, client) = MemoryTransport::pair();
    if controller {
        broker.attach_module_controller(Arc::new(server)).unwrap();
    } else {
        broker.attach(Arc::new(server));
    }
    Connection::connect(Box::new(client)).await.unwrap()
}

fn operations() -> Vec<(&'static str, Value)> {
    vec![
        (
            "LoadModule",
            serde_json::json!(["fixture.so", {"secret": "private"}]),
        ),
        (
            "LoadGithubModule",
            serde_json::json!(["url", "asset", "digest", {}]),
        ),
        (
            "ReinitializeModule",
            serde_json::json!(["fixture", {"secret": "private"}]),
        ),
        ("StopModule", serde_json::json!(["fixture", 1000])),
        ("EnableModule", serde_json::json!(["fixture", false])),
        ("RescanModules", serde_json::json!([[], false])),
    ]
}

async fn invoke(
    client: &Connection,
    member: &str,
    args: Value,
    forged: Option<BusName>,
) -> Result<Value> {
    let mut message = Message::method_call(
        BusName::new(crate::BUS_NAME).unwrap(),
        ObjectPath::new(crate::BUS_PATH).unwrap(),
        InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
        MemberName::new(member).unwrap(),
        args,
    );
    message.header.sender = forged;
    client.call_raw(message, Duration::from_secs(2)).await
}

fn install_control(broker: &Broker) -> Arc<RecordingControl> {
    let control = Arc::new(RecordingControl::default());
    let erased: Arc<dyn ModuleControl> = control.clone();
    broker.set_module_control(Arc::downgrade(&erased));
    control
}

async fn refuse_noncontroller(index: usize) {
    let broker = Broker::new_restricted_module_control();
    let control = install_control(&broker);
    let owner = peer(&broker, true).await;
    let stranger = peer(&broker, false).await;
    for (member, args) in operations().into_iter().skip(index).take(1) {
        let result = invoke(&stranger, member, args, Some(owner.unique_name().unwrap())).await;
        assert!(
            result.is_err(),
            "unauthorized {member} reached control: {result:?}"
        );
        assert_eq!(result.unwrap_err().wire_name(), DENIED);
        assert!(control.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn restricted_load_refuses_noncontroller_before_callback() {
    refuse_noncontroller(0).await;
}

#[tokio::test]
async fn restricted_github_load_refuses_noncontroller_before_callback() {
    refuse_noncontroller(1).await;
}

#[tokio::test]
async fn restricted_reinitialize_refuses_noncontroller_before_callback() {
    refuse_noncontroller(2).await;
}

#[tokio::test]
async fn restricted_stop_refuses_noncontroller_before_callback() {
    refuse_noncontroller(3).await;
}

#[tokio::test]
async fn restricted_enable_refuses_noncontroller_before_callback() {
    refuse_noncontroller(4).await;
}

#[tokio::test]
async fn restricted_rescan_refuses_noncontroller_before_callback() {
    refuse_noncontroller(5).await;
}

#[tokio::test]
async fn selected_controller_can_mutate_and_other_peers_can_inspect() {
    let broker = Broker::new_restricted_module_control();
    let control = install_control(&broker);
    let owner = peer(&broker, true).await;
    for (member, args) in operations() {
        invoke(&owner, member, args, None).await.unwrap();
    }
    assert_eq!(
        *control.0.lock().unwrap(),
        operations()
            .iter()
            .map(|(member, _)| *member)
            .collect::<Vec<_>>()
    );
    let stranger = peer(&broker, false).await;
    let listed = invoke(&stranger, "ListModules", serde_json::json!([]), None)
        .await
        .unwrap();
    assert_eq!(listed, serde_json::json!([]));
}

#[tokio::test]
async fn restriction_starts_closed_and_denies_before_argument_parsing() {
    let broker = Broker::new_restricted_module_control();
    let control = install_control(&broker);
    let stranger = peer(&broker, false).await;
    for (member, _) in operations() {
        let error = invoke(
            &stranger,
            member,
            serde_json::json!({"secret": "private"}),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.wire_name(), DENIED);
        assert!(!error.to_string().contains("private"));
    }
    assert!(control.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn controller_cannot_be_rebound_through_a_clone_or_after_disconnect() {
    let broker = Broker::new_restricted_module_control();
    let control = install_control(&broker);
    let owner = peer(&broker, true).await;
    let (server, _client) = MemoryTransport::pair();
    assert!(
        broker
            .clone()
            .attach_module_controller(Arc::new(server))
            .is_err()
    );
    owner.close().await.unwrap();
    let stranger = peer(&broker, false).await;
    let (server, _client) = MemoryTransport::pair();
    assert!(broker.attach_module_controller(Arc::new(server)).is_err());
    let error = invoke(
        &stranger,
        "StopModule",
        serde_json::json!(["fixture", 1]),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.wire_name(), DENIED);
    assert!(control.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn permissive_broker_retains_legacy_control_calls_and_cannot_claim_restricted_binding() {
    let broker = Broker::new();
    let control = install_control(&broker);
    let client = peer(&broker, false).await;
    for (member, args) in operations() {
        invoke(&client, member, args, None).await.unwrap();
    }
    assert_eq!(control.0.lock().unwrap().len(), 6);
    let (server, _client) = MemoryTransport::pair();
    assert!(broker.attach_module_controller(Arc::new(server)).is_err());
}

#[tokio::test]
async fn closed_admission_precedes_missing_host_lookup_and_no_wire_method_selects_controller() {
    let broker = Broker::new_restricted_module_control();
    let stranger = peer(&broker, false).await;
    for (member, args) in operations() {
        let error = invoke(&stranger, member, args, None).await.unwrap_err();
        assert_eq!(error.wire_name(), DENIED);
    }
    let error = invoke(
        &stranger,
        "AttachModuleController",
        serde_json::json!([stranger.unique_name().unwrap()]),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownMethod"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_host_selection_attaches_exactly_one_controller() {
    let broker = Broker::new_restricted_module_control();
    let control = install_control(&broker);
    let start = Arc::new(tokio::sync::Barrier::new(2));
    let mut selectors = Vec::new();
    for _ in 0..2 {
        let broker = broker.clone();
        let start = start.clone();
        selectors.push(tokio::spawn(async move {
            let (server, client) = MemoryTransport::pair();
            start.wait().await;
            (broker.attach_module_controller(Arc::new(server)), client)
        }));
    }
    let mut winner = None;
    let mut refused = 0;
    for selector in selectors {
        let (result, client) = selector.await.unwrap();
        match result {
            Ok(_) => winner = Some(Connection::connect(Box::new(client)).await.unwrap()),
            Err(error) => {
                assert_eq!(error.wire_name(), DENIED);
                refused += 1;
            }
        }
    }
    assert_eq!(refused, 1);
    invoke(
        &winner.unwrap(),
        "StopModule",
        serde_json::json!(["fixture", 1]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(*control.0.lock().unwrap(), vec!["StopModule"]);
}

#[tokio::test]
async fn replacing_module_host_does_not_relax_broker_controller_admission() {
    let broker = Broker::new_restricted_module_control();
    let owner = peer(&broker, true).await;
    let stranger = peer(&broker, false).await;
    let host = crate::module::host::ModuleHost::new(broker.clone());
    let error = invoke(
        &stranger,
        "ReinitializeModule",
        serde_json::json!(["fixture", {}]),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.wire_name(), DENIED);
    drop(host);
    let replacement = install_control(&broker);
    let error = invoke(
        &stranger,
        "LoadModule",
        serde_json::json!(["fixture.so", {}]),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.wire_name(), DENIED);
    invoke(
        &owner,
        "LoadModule",
        serde_json::json!(["fixture.so", {}]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(*replacement.0.lock().unwrap(), vec!["LoadModule"]);
}

#[tokio::test]
async fn a_module_binding_snapshot_is_fixed_to_a_live_selected_controller() {
    let permissive = Broker::new();
    assert!(permissive.restricted_module_controller_binding().is_none());

    let broker = Broker::new_restricted_module_control();
    assert!(broker.restricted_module_controller_binding().is_none());
    let (server, client) = MemoryTransport::pair();
    let selected = broker.attach_module_controller(Arc::new(server)).unwrap();
    let controller = Connection::connect(Box::new(client)).await.unwrap();
    assert_eq!(controller.unique_name().as_ref(), Some(&selected));

    let binding = broker
        .restricted_module_controller_binding()
        .expect("live selected controller is snapshotted");
    assert_eq!(binding.peer(), &selected);
    assert!(binding.is_live());

    controller.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while binding.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect removes the selected unique peer");
    assert!(broker.restricted_module_controller_binding().is_none());
    let (replacement_server, _replacement_client) = MemoryTransport::pair();
    assert!(
        broker
            .attach_module_controller(Arc::new(replacement_server))
            .is_err(),
        "a dead selection cannot be rebound on the same broker"
    );
}

/// The default test runtime has one thread: a load that blocks it would hold
/// every other connection's call until the load returned.
#[tokio::test]
async fn a_slow_module_load_does_not_stall_other_bus_calls() {
    let broker = Broker::new();
    let _control = install_control(&broker);
    let loader = peer(&broker, false).await;
    let other = peer(&broker, false).await;
    let started = std::time::Instant::now();
    let load = tokio::spawn(async move {
        invoke(
            &loader,
            "LoadModule",
            serde_json::json!([SLOW_MODULE]),
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    invoke(&other, "ListModules", serde_json::json!([]), None)
        .await
        .unwrap();
    let waited = started.elapsed();
    assert!(
        waited < SLOW_LOAD / 2,
        "ListModules waited {waited:?} behind another peer's module load"
    );
    load.await.unwrap().unwrap();
}
