use super::*;
use crate::message::Message;
use crate::router::{MatchRule, Router};
use std::sync::{Arc, atomic::AtomicUsize};

async fn active_stream_binding() -> (
    Arc<ModuleHost>,
    tokio::task::JoinHandle<Result<()>>,
    HostCallbackBinding,
    crate::Connection,
    crate::transport::memory::MemoryBus,
) {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    let binding = host
        .prepare_host_callbacks_with_stream_endpoints(vec![endpoint()], vec![stream_endpoint()])
        .await
        .unwrap();
    let module = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    {
        let mut lifetime = binding.state.lifetime.lock().unwrap();
        lifetime.active = true;
        lifetime.used = true;
        lifetime.module = Some(module_peer);
    }
    (host, broker_task, binding, module, bus)
}

fn endpoint() -> HostCallbackEndpoint {
    HostCallbackEndpoint {
        path: "/callback".parse().unwrap(),
        interface: "org.example.Callback".parse().unwrap(),
        members: vec!["Observe".parse().unwrap()],
    }
}
fn stream_endpoint() -> HostStreamCallbackEndpoint {
    HostStreamCallbackEndpoint {
        path: "/callback".parse().unwrap(),
        interface: "org.example.Callback".parse().unwrap(),
        members: vec!["Transfer".parse().unwrap()],
    }
}
fn signal_declaration() -> HostCallbackSignal {
    HostCallbackSignal {
        path: "/callback".parse().unwrap(),
        interface: "org.example.Callback".parse().unwrap(),
        members: vec!["WatchChanged".parse().unwrap()],
    }
}

#[tokio::test]
async fn bundled_callback_loader_rejects_missing_or_wrong_digest_marker() {
    let host = ModuleHost::new(Broker::new());
    let binding = Arc::new(host.prepare_host_callbacks(vec![endpoint()]).await.unwrap());
    let directory = tempfile::tempdir().unwrap();

    let error = host
        .load_bundled_marked_with_host_callbacks(
            directory.path(),
            "demo.tar.gz",
            &"a".repeat(64),
            serde_json::json!({}),
            &binding,
            ("org.example.Demo", "/org/example/Demo"),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("digest marker"), "{error}");
}

#[derive(Clone)]
struct StreamCallback;

#[async_trait]
impl Interface for StreamCallback {
    fn name(&self) -> InterfaceName {
        "org.example.Callback".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec!["Transfer".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &MemberName,
        args: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(args)
    }
}

#[test]
fn required_endpoint_count_deduplicates_a_shared_confidential_and_stream_interface() {
    let mut state = state(
        "org.example.Module".parse().unwrap(),
        "org.example.Host".parse().unwrap(),
    );
    Arc::get_mut(&mut state).unwrap().stream_endpoints = vec![stream_endpoint()];
    assert_eq!(state.required_endpoint_count(), 1);
}

#[test]
fn directed_signal_requires_a_live_module_recipient() {
    let state = state(
        "org.example.Module".parse().unwrap(),
        "org.example.Host".parse().unwrap(),
    );
    state.lifetime.lock().unwrap().module = None;
    assert!(
        state
            .signal_destination(
                &"/callback".parse().unwrap(),
                &"org.example.Callback".parse().unwrap(),
                &"WatchChanged".parse().unwrap(),
            )
            .is_err()
    );
}
fn state(source: BusName, destination: BusName) -> Arc<CallbackState> {
    Arc::new(CallbackState {
        destination,
        endpoints: vec![endpoint()],
        stream_endpoints: vec![],
        signals: vec![signal_declaration()],
        module_transport: Mutex::new(None),
        _parent: None,
        children: Mutex::new(Vec::new()),
        lifetime: Mutex::new(Lifetime {
            active: true,
            used: true,
            revoked: false,
            module: Some(source),
            running: 0,
            served: vec![],
            registering: vec![],
        }),
        drained: Notify::new(),
        physical_drained: std::sync::Condvar::new(),
        physical_wait_started: Notify::new(),
    })
}

#[tokio::test]
async fn reset_requests_receive_distinct_revocable_host_callback_bindings() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let parent = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let module = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    let (module_transport, _) =
        crate::module::transport::ModuleTransport::new("reset-test".into(), vec![]);
    *parent.state.module_transport.lock().unwrap() = Some(Arc::downgrade(&module_transport));
    {
        let mut lifetime = parent.state.lifetime.lock().unwrap();
        lifetime.active = true;
        lifetime.module = Some(module_peer.clone());
    }
    broker.attest_module(
        &module_peer,
        crate::attest::Attestation {
            name: "org.example.ResetTest".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let context = CallContext::brokered(Some(&module_peer), true);

    let first_reset = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    let second_reset = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    let grandchild = first_reset
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();

    assert_ne!(first_reset.unique_name(), second_reset.unique_name());
    assert!(HostCallbackGuard(first_reset.state.clone()).authorizes(&context));
    assert!(HostCallbackGuard(second_reset.state.clone()).authorizes(&context));
    assert!(HostCallbackGuard(grandchild.state.clone()).authorizes(&context));
    parent.revoke();
    assert!(!HostCallbackGuard(first_reset.state.clone()).authorizes(&context));
    assert!(!HostCallbackGuard(second_reset.state.clone()).authorizes(&context));
    assert!(!HostCallbackGuard(grandchild.state.clone()).authorizes(&context));

    broker_task.abort();
}

#[tokio::test]
async fn draining_a_binding_waits_for_running_grandchild_callbacks() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let parent = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let module = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    broker.attest_module(
        &module_peer,
        crate::attest::Attestation {
            name: "org.example.NestedDrainTest".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let (module_transport, _) =
        crate::module::transport::ModuleTransport::new("nested-drain-test".into(), vec![]);
    *parent.state.module_transport.lock().unwrap() = Some(Arc::downgrade(&module_transport));
    {
        let mut lifetime = parent.state.lifetime.lock().unwrap();
        lifetime.active = true;
        lifetime.module = Some(module_peer.clone());
    }
    let context = CallContext::brokered(Some(&module_peer), true);
    let child = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    let grandchild = child
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    grandchild.state.lifetime.lock().unwrap().running = 1;

    let mut drain = Box::pin(parent.state.drain(Duration::from_secs(1)));
    let first_poll =
        std::future::poll_fn(|context| std::task::Poll::Ready(drain.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "drain must include live grandchildren"
    );

    grandchild.state.lifetime.lock().unwrap().running = 0;
    grandchild.state.drained.notify_waiters();
    drain.await.unwrap();

    broker_task.abort();
}

#[tokio::test]
async fn root_drain_retains_ancestry_for_running_grandchild_after_child_drop() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let parent = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let module = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    broker.attest_module(
        &module_peer,
        crate::attest::Attestation {
            name: "org.example.DescendantAncestryTest".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let (module_transport, _) =
        crate::module::transport::ModuleTransport::new("descendant-ancestry-test".into(), vec![]);
    *parent.state.module_transport.lock().unwrap() = Some(Arc::downgrade(&module_transport));
    {
        let mut lifetime = parent.state.lifetime.lock().unwrap();
        lifetime.active = true;
        lifetime.module = Some(module_peer.clone());
    }
    let context = CallContext::brokered(Some(&module_peer), true);
    let child = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    let grandchild = child
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    grandchild.state.lifetime.lock().unwrap().running = 1;
    let running = Running(grandchild.state.clone());

    drop(grandchild);
    drop(child);

    let mut drain = Box::pin(parent.state.drain(Duration::from_secs(1)));
    let first_poll =
        std::future::poll_fn(|context| std::task::Poll::Ready(drain.as_mut().poll(context))).await;
    let waited_for_running_grandchild = first_poll.is_pending();

    drop(running);
    if waited_for_running_grandchild {
        drain.await.unwrap();
    }
    broker_task.abort();

    assert!(
        waited_for_running_grandchild,
        "root drain must retain the path to a running grandchild after its child binding is dropped"
    );
}

#[tokio::test]
async fn child_creation_prunes_expired_parent_entries() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker.clone());
    let parent = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let module = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    broker.attest_module(
        &module_peer,
        crate::attest::Attestation {
            name: "org.example.ExpiredChildTest".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let (module_transport, _) =
        crate::module::transport::ModuleTransport::new("expired-child-test".into(), vec![]);
    *parent.state.module_transport.lock().unwrap() = Some(Arc::downgrade(&module_transport));
    {
        let mut lifetime = parent.state.lifetime.lock().unwrap();
        lifetime.active = true;
        lifetime.module = Some(module_peer.clone());
    }
    let context = CallContext::brokered(Some(&module_peer), true);
    let expired = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    drop(expired);

    let _replacement = parent
        .create_child_binding(&context, vec![endpoint()])
        .await
        .unwrap();
    assert!(
        parent
            .state
            .children
            .lock()
            .unwrap()
            .iter()
            .all(|child| child.strong_count() != 0)
    );

    broker_task.abort();
}

struct MixedCallback;
#[async_trait]
impl Interface for MixedCallback {
    fn name(&self) -> InterfaceName {
        endpoint().interface
    }
    fn members(&self) -> Vec<MemberName> {
        vec![
            endpoint().members[0].clone(),
            stream_endpoint().members[0].clone(),
        ]
    }
    async fn call(&self, member: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Ok(serde_json::Value::String(member.to_string()))
    }
}

#[tokio::test]
async fn mixed_callback_dispatch_keeps_confidential_and_stream_members_separate() {
    let module_peer: BusName = ":1.8".parse().unwrap();
    let callback_peer: BusName = ":1.9".parse().unwrap();
    let mut state = state(module_peer.clone(), callback_peer);
    Arc::get_mut(&mut state)
        .unwrap()
        .stream_endpoints
        .push(stream_endpoint());
    let guarded = GuardedInterface {
        interface: MixedCallback,
        confidential_endpoint: Some(endpoint()),
        stream_endpoint: Some(stream_endpoint()),
        state: state.clone(),
    };
    let confidential = CallContext::brokered(Some(&module_peer), true);
    let ordinary = CallContext::brokered(Some(&module_peer), false);
    let wrong_sender: BusName = ":1.10".parse().unwrap();
    let wrong_ordinary = CallContext::brokered(Some(&wrong_sender), false);
    let secret_member = &endpoint().members[0];
    let stream_member = &stream_endpoint().members[0];

    assert!(
        guarded
            .call_with_context(secret_member, serde_json::json!([]), &confidential)
            .await
            .is_ok()
    );
    assert!(
        guarded
            .call_with_context(secret_member, serde_json::json!([]), &ordinary)
            .await
            .is_err()
    );
    assert!(
        guarded
            .call_with_context(stream_member, serde_json::json!([]), &ordinary)
            .await
            .is_ok()
    );
    assert!(
        guarded
            .call_with_context(stream_member, serde_json::json!([]), &confidential)
            .await
            .is_err()
    );
    assert!(
        guarded
            .call_with_context(stream_member, serde_json::json!([]), &wrong_ordinary)
            .await
            .is_err()
    );

    state.revoke();
    assert!(
        guarded
            .call_with_context(stream_member, serde_json::json!([]), &ordinary)
            .await
            .is_err()
    );
}

#[test]
fn directed_signals_target_only_the_live_bound_module_and_declared_member() {
    let mut router = Router::default();
    let (binding_id, binding_peer) = router.attach(tokio::sync::mpsc::channel(8).0);
    let (target_sender, mut target_receiver) = tokio::sync::mpsc::channel(8);
    let (_, module_peer) = router.attach(target_sender);
    let (outsider_sender, mut outsider_receiver) = tokio::sync::mpsc::channel(8);
    let (outsider_id, outsider_peer) = router.attach(outsider_sender);
    router.add_match(outsider_id, MatchRule::new().directed_signals());
    router.set_attestation_for_unique(
        &module_peer,
        crate::attest::Attestation {
            name: "org.example.Module".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let capability = state(module_peer.clone(), binding_peer.clone());
    router.add_host_callbacks(capability.clone()).unwrap();

    let path: crate::ObjectPath = "/callback".parse().unwrap();
    let interface: crate::InterfaceName = "org.example.Callback".parse().unwrap();
    let member: crate::MemberName = "WatchChanged".parse().unwrap();
    let mut message = Message::directed_signal(
        module_peer.clone(),
        path.clone(),
        interface.clone(),
        member.clone(),
        serde_json::json!([{"watch_id":"opaque","notice":{"kind":"invalidated"}}]),
    );
    message.header.sender = Some(binding_peer.clone());
    let target = router
        .directed_signal_target(&binding_peer, &module_peer, &message.header)
        .unwrap();
    target.try_send(message.clone()).unwrap();
    assert_eq!(target_receiver.try_recv().unwrap(), message);
    assert!(outsider_receiver.try_recv().is_err());

    let mut forged = message.clone();
    forged.header.destination = Some(outsider_peer.clone());
    assert!(
        router
            .directed_signal_target(&binding_peer, &outsider_peer, &forged.header)
            .is_err()
    );
    assert!(
        router
            .directed_signal_target(&outsider_peer, &module_peer, &message.header)
            .is_err()
    );
    let mut wrong_member = message.clone();
    wrong_member.header.member = Some("Other".parse().unwrap());
    assert!(
        router
            .directed_signal_target(&binding_peer, &module_peer, &wrong_member.header)
            .is_err()
    );

    router.revoke_host_callbacks(&capability);
    assert!(
        router
            .directed_signal_target(&binding_peer, &module_peer, &message.header)
            .is_err()
    );
    router.detach(binding_id);
    assert!(router.owner_of(&module_peer).is_some());
}
fn call(state: &CallbackState) -> Message {
    let endpoint = endpoint();
    let mut message = Message::method_call(
        state.destination.clone(),
        endpoint.path,
        endpoint.interface,
        endpoint.members[0].clone(),
        serde_json::json!([]),
    );
    message.header.sender = state.module_peer();
    message.header.confidential = true;
    message
}
fn kernel_fixture() -> (Router, Arc<CallbackState>, u64, u64) {
    // Private router behavior tests only; actual artifact admission is proved by
    // host_tests with the hash-verified cdylib, never by this table fixture.
    let mut router = Router::default();
    let (source_id, source) = router.attach(tokio::sync::mpsc::channel(8).0);
    let (destination_id, destination) = router.attach(tokio::sync::mpsc::channel(8).0);
    router.set_attestation_for_unique(
        &source,
        crate::attest::Attestation {
            name: "org.example.Module".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let state = state(source, destination);
    router.add_host_callbacks(state.clone()).unwrap();
    (router, state, source_id, destination_id)
}
fn resolves(router: &Router, message: &Message) -> Result<tokio::sync::mpsc::Sender<Message>> {
    router.resolve_call(
        message.header.destination.as_ref().unwrap(),
        message.header.expected_responder.as_ref(),
        true,
        &message.header,
    )
}
#[test]
fn scoped_routes_match_every_field_and_never_attest_the_host() {
    let (router, state, _, _) = kernel_fixture();
    let message = call(&state);
    assert!(resolves(&router, &message).is_ok());
    assert!(router.attestation_of(&state.destination).is_none());
    assert!(router.resolve_attested(&state.destination).is_err());
    for field in 0..5 {
        let mut wrong = message.clone();
        match field {
            0 => wrong.header.sender = Some(":1.999".parse().unwrap()),
            1 => wrong.header.path = Some("/other".parse().unwrap()),
            2 => wrong.header.interface = Some("org.example.Other".parse().unwrap()),
            3 => wrong.header.member = Some("Other".parse().unwrap()),
            _ => wrong.header.expected_responder = Some(":1.999".parse().unwrap()),
        }
        assert!(resolves(&router, &wrong).is_err());
    }
}
#[test]
fn a_host_alias_and_a_second_verified_module_inherit_no_authority() {
    let (mut router, state, _, destination_id) = kernel_fixture();
    let (_, other) = router.attach(tokio::sync::mpsc::channel(8).0);
    router.set_attestation_for_unique(
        &other,
        crate::attest::Attestation {
            name: "org.example.Other".parse().unwrap(),
            sha256: "b".repeat(64),
            library_sha256: None,
        },
    );
    let mut message = call(&state);
    message.header.sender = Some(other);
    assert!(resolves(&router, &message).is_err());
    let alias: BusName = "org.example.Host".parse().unwrap();
    router.request_name(destination_id, alias.clone()).unwrap();
    message = call(&state);
    message.header.destination = Some(alias);
    assert!(resolves(&router, &message).is_err());
}
#[test]
fn detaching_either_peer_revokes_stale_guard_and_replacements() {
    for detach_source in [false, true] {
        let (mut router, state, source_id, destination_id) = kernel_fixture();
        router.detach(if detach_source {
            source_id
        } else {
            destination_id
        });
        assert!(!state.matches(&call(&state).header));
        assert_eq!(
            router
                .add_host_callbacks(state.clone())
                .unwrap_err()
                .wire_name(),
            "ai.tinyhumans.tinybus.Error.Failed"
        );
        let (_, replacement) = router.attach(tokio::sync::mpsc::channel(8).0);
        assert_ne!(Some(replacement), state.module_peer());
    }
}
#[test]
fn unverified_sources_cannot_be_enrolled_and_scopes_are_bounded() {
    let mut router = Router::default();
    let (_, source) = router.attach(tokio::sync::mpsc::channel(8).0);
    let (_, destination) = router.attach(tokio::sync::mpsc::channel(8).0);
    let state = state(source.clone(), destination);
    assert!(router.add_host_callbacks(state.clone()).is_err());
    router.set_attestation_for_unique(
        &source,
        crate::attest::Attestation {
            name: "org.example.Module".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    router.add_host_callbacks(state.clone()).unwrap();
    assert!(router.add_host_callbacks(state).is_err());
    for _ in 1..128 {
        let (_, source) = router.attach(tokio::sync::mpsc::channel(8).0);
        let (_, destination) = router.attach(tokio::sync::mpsc::channel(8).0);
        router.set_attestation_for_unique(
            &source,
            crate::attest::Attestation {
                name: "org.example.Module".parse().unwrap(),
                sha256: "a".repeat(64),
                library_sha256: None,
            },
        );
        router
            .add_host_callbacks(super::tests::state(source, destination))
            .unwrap();
    }
    let (_, source) = router.attach(tokio::sync::mpsc::channel(8).0);
    let (_, destination) = router.attach(tokio::sync::mpsc::channel(8).0);
    router.set_attestation_for_unique(
        &source,
        crate::attest::Attestation {
            name: "org.example.Module".parse().unwrap(),
            sha256: "a".repeat(64),
            library_sha256: None,
        },
    );
    let extra = super::tests::state(source, destination);
    assert!(router.add_host_callbacks(extra).is_err());
}

struct BlockingCallback {
    entered: tokio::sync::mpsc::Sender<()>,
    release: Arc<tokio::sync::Semaphore>,
    published: Arc<AtomicUsize>,
    guard: HostCallbackGuard,
}
#[async_trait]
impl Interface for BlockingCallback {
    fn name(&self) -> InterfaceName {
        endpoint().interface
    }
    fn members(&self) -> Vec<MemberName> {
        endpoint().members
    }
    async fn call(&self, _: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Err(Error::failed("unverified"))
    }
    async fn call_with_context(
        &self,
        _: &MemberName,
        _: serde_json::Value,
        context: &CallContext,
    ) -> Result<serde_json::Value> {
        self.entered.send(()).await.unwrap();
        let _permit = self.release.acquire().await.unwrap();
        if !self.guard.authorizes(context) {
            return Err(Error::failed("revoked before publication"));
        }
        self.published.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::Value::Null)
    }
}
#[tokio::test]
async fn revoke_after_route_selection_blocks_dispatch_and_direct_unverified_dispatch() {
    let (mut router, state, _, _) = kernel_fixture();
    let message = call(&state);
    assert!(resolves(&router, &message).is_ok());
    let (entered, mut receiver) = tokio::sync::mpsc::channel(1);
    let guarded = GuardedInterface {
        confidential_endpoint: Some(endpoint()),
        stream_endpoint: None,
        state: state.clone(),
        interface: BlockingCallback {
            entered,
            release: Arc::new(tokio::sync::Semaphore::new(1)),
            published: Arc::new(AtomicUsize::new(0)),
            guard: HostCallbackGuard(state.clone()),
        },
    };
    assert!(
        guarded
            .call(&endpoint().members[0], serde_json::json!([]))
            .await
            .is_err()
    );
    assert!(
        guarded
            .call_with_context(
                &endpoint().members[0],
                serde_json::json!([]),
                &CallContext::brokered(state.module_peer().as_ref(), false)
            )
            .await
            .is_err()
    );
    assert!(
        guarded
            .call_with_context(
                &endpoint().members[0],
                serde_json::json!([]),
                &CallContext::default()
            )
            .await
            .is_err()
    );
    router.revoke_host_callbacks(&state);
    assert!(
        guarded
            .call_with_context(
                &endpoint().members[0],
                serde_json::json!([]),
                &CallContext::brokered(state.module_peer().as_ref(), true)
            )
            .await
            .is_err()
    );
    assert!(receiver.try_recv().is_err());
    assert_eq!(HostCallbackGuard(state).running(), 0);
}
#[tokio::test]
async fn revoke_during_await_preserves_running_charge_until_actual_completion() {
    let (mut router, state, _, _) = kernel_fixture();
    let guard = HostCallbackGuard(state.clone());
    let (entered, mut receiver) = tokio::sync::mpsc::channel(1);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let guarded = GuardedInterface {
        confidential_endpoint: Some(endpoint()),
        stream_endpoint: None,
        state: state.clone(),
        interface: BlockingCallback {
            entered,
            release: release.clone(),
            published: published.clone(),
            guard: guard.clone(),
        },
    };
    let context = CallContext::brokered(state.module_peer().as_ref(), true);
    let task = tokio::spawn(async move {
        guarded
            .call_with_context(&endpoint().members[0], serde_json::json!([]), &context)
            .await
    });
    receiver.recv().await.unwrap();
    assert_eq!(guard.running(), 1);
    router.revoke_host_callbacks(&state);
    assert_eq!(guard.running(), 1);
    let mut drain = Box::pin(guard.drained());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx).is_pending()))
            .await
    );
    release.add_permits(1);
    assert!(task.await.unwrap().is_err());
    guard.drained().await;
    assert_eq!(guard.running(), 0);
    assert_eq!(published.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn endpoint_declarations_registration_and_broker_ownership_are_checked() {
    let host = ModuleHost::new(Broker::new());
    assert!(host.prepare_host_callbacks(vec![]).await.is_err());
    assert!(
        host.prepare_host_callbacks(vec![endpoint(); 33])
            .await
            .is_err()
    );
    assert!(
        host.prepare_host_callbacks(vec![endpoint(), endpoint()])
            .await
            .is_err()
    );
    assert!(
        host.prepare_host_callbacks_with_stream_endpoints(
            vec![endpoint()],
            vec![HostStreamCallbackEndpoint {
                path: endpoint().path,
                interface: endpoint().interface,
                members: endpoint().members,
            }],
        )
        .await
        .is_err()
    );
    let mut duplicate = endpoint();
    duplicate.members.push(duplicate.members[0].clone());
    assert!(host.prepare_host_callbacks(vec![duplicate]).await.is_err());
    let mut excessive = endpoint();
    excessive.members = (0..65).map(|i| format!("M{i}").parse().unwrap()).collect();
    assert!(host.prepare_host_callbacks(vec![excessive]).await.is_err());
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    assert!(
        host.load_file_with_host_callbacks("unused", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    let other_owner = ModuleHost::new(host.inner.broker.clone());
    assert!(
        other_owner
            .load_file_with_host_callbacks("unused", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    let other = ModuleHost::new(Broker::new());
    assert!(
        other
            .load_file_with_host_callbacks("unused", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    binding.revoke();
    binding.revoke();
    assert!(
        host.load_file_with_host_callbacks("unused", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    binding.close().await.unwrap();
}

struct EmptyCallback;
#[async_trait]
impl Interface for EmptyCallback {
    fn name(&self) -> InterfaceName {
        endpoint().interface
    }
    fn members(&self) -> Vec<MemberName> {
        vec![]
    }
    async fn call(&self, _: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }
}
#[tokio::test]
async fn registration_is_exact_single_use_and_failed_load_detaches_the_local_peer() {
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    assert!(
        binding
            .serve_at("/other".parse().unwrap(), EmptyCallback)
            .await
            .is_err()
    );
    assert!(
        binding
            .serve_at(endpoint().path, EmptyCallback)
            .await
            .is_err()
    );
    let (entered, _) = tokio::sync::mpsc::channel(1);
    let callback = || BlockingCallback {
        entered: entered.clone(),
        release: Arc::new(tokio::sync::Semaphore::new(1)),
        published: Arc::new(AtomicUsize::new(0)),
        guard: binding.guard(),
    };
    binding.serve_at(endpoint().path, callback()).await.unwrap();
    assert!(binding.serve_at(endpoint().path, callback()).await.is_err());
    assert_eq!(
        binding.unique_name(),
        &broker
            .router
            .lock()
            .unwrap()
            .owner_of(binding.unique_name())
            .unwrap()
    );
    assert!(
        host.load_file_with_host_callbacks("missing-fixture", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    assert!(
        broker
            .router
            .lock()
            .unwrap()
            .owner_of(binding.unique_name())
            .is_none()
    );
    assert!(binding.serve_at(endpoint().path, callback()).await.is_err());
    assert!(
        host.load_file_with_host_callbacks("missing-fixture", serde_json::json!({}), &binding)
            .await
            .is_err()
    );
    let peer = binding.unique_name().clone();
    drop(binding);
    assert!(broker.router.lock().unwrap().owner_of(&peer).is_none());
}

#[tokio::test]
async fn a_module_fault_revokes_dispatch_authority_before_peer_detach() {
    let (router, state, _, _) = kernel_fixture();
    let (transport, host) = ModuleTransport::new("fault-fixture".into(), Vec::new());
    *state.module_transport.lock().unwrap() = Some(Arc::downgrade(&transport));
    assert!(resolves(&router, &call(&state)).is_ok());
    unsafe {
        (host.fault)(host.host_ctx, std::ptr::null(), 0);
    }
    assert!(!state.matches(&call(&state).header));
    assert!(
        !HostCallbackGuard(state.clone())
            .authorizes(&CallContext::brokered(state.module_peer().as_ref(), true))
    );
    assert!(resolves(&router, &call(&state)).is_err());
}

#[tokio::test]
async fn restricted_outgoing_handle_preserves_typed_calls_and_metadata() {
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let outgoing = binding
        .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_NAME)
        .unwrap()
        .with_timeout(Duration::from_secs(1));
    assert_eq!(outgoing.destination().as_str(), crate::BUS_NAME);
    assert_eq!(outgoing.path().as_str(), crate::BUS_PATH);
    assert_eq!(outgoing.interface().as_str(), crate::BUS_NAME);
    let names: Vec<BusName> = outgoing.clone().call("ListNames", ()).await.unwrap();
    assert!(names.contains(binding.unique_name()));
    let streamed: Vec<BusName> = outgoing.call_streaming("ListNames", ()).await.unwrap();
    assert_eq!(names, streamed);
    assert!(
        outgoing
            .call_confidential::<serde_json::Value>("ListNames", ())
            .await
            .is_err()
    );
    assert!(
        outgoing
            .call_sensitive::<serde_json::Value>("ListNames", ())
            .await
            .is_err()
    );
    assert!(outgoing.attestation().await.unwrap().is_none());
    assert!(!outgoing.is_available().await.unwrap());
    let _signals = outgoing.receive_signal("NameOwnerChanged").await.unwrap();
    assert!(format!("{outgoing:?}").contains("HostCallbackProxy"));
    binding.close().await.unwrap();
}

#[tokio::test]
async fn a_bound_callback_can_receive_and_send_streams_without_exposing_its_connection() {
    let (_host, broker_task, binding, module, _) = active_stream_binding().await;
    let module_peer = module.unique_name().unwrap();
    let context = CallContext::brokered(Some(&module_peer), false);

    let mut module_writer = module
        .open_stream(binding.unique_name(), StreamDescriptor::with_len(5))
        .await
        .unwrap();
    let mut host_reader = binding
        .accept_stream_from_module(&context, &module_writer.stream_ref())
        .unwrap();
    let module_send = tokio::spawn(async move {
        module_writer.write(b"hello").await.unwrap();
        module_writer.finish().await.unwrap();
    });
    assert_eq!(host_reader.read_to_end_capped(5).await.unwrap(), b"hello");
    module_send.await.unwrap();

    let mut host_writer = binding
        .open_stream_to_module(
            &context,
            StreamDescriptor::with_len(5),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let mut module_reader = module
        .accept_stream_from(&host_writer.stream_ref(), binding.unique_name())
        .unwrap();
    host_writer.write(b"world").await.unwrap();
    host_writer.finish().await.unwrap();
    assert_eq!(module_reader.read_to_end_capped(5).await.unwrap(), b"world");

    binding.close().await.unwrap();
    broker_task.abort();
}

#[tokio::test]
async fn callback_streams_reject_other_peers_nonconfidential_calls_and_revoked_bindings() {
    let (_host, broker_task, binding, module, bus) = active_stream_binding().await;
    let module_peer = module.unique_name().unwrap();
    let context = CallContext::brokered(Some(&module_peer), false);
    let outsider = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let outsider_peer = outsider.unique_name().unwrap();
    let wrong_peer = CallContext::brokered(Some(&outsider_peer), false);
    let confidential_call = CallContext::brokered(Some(&module_peer), true);
    let guard = binding.guard();
    assert!(!guard.authorizes(&context));
    assert!(guard.authorizes_stream(&context));
    assert!(!guard.authorizes_stream(&wrong_peer));
    assert!(!guard.authorizes_stream(&confidential_call));
    let stream = StreamRef {
        id: "not-opened".to_owned(),
        content_type: None,
        len: Some(1),
    };

    assert!(
        binding
            .accept_stream_from_module(&wrong_peer, &stream)
            .is_err()
    );
    assert!(
        binding
            .open_stream_to_module(
                &wrong_peer,
                StreamDescriptor::with_len(1),
                Duration::from_secs(1),
            )
            .await
            .is_err()
    );
    assert!(
        binding
            .accept_stream_from_module(&confidential_call, &stream)
            .is_err()
    );
    assert!(
        binding
            .open_stream_to_module(
                &confidential_call,
                StreamDescriptor::with_len(1),
                Duration::from_secs(1),
            )
            .await
            .is_err()
    );

    binding.revoke();
    assert!(!guard.authorizes_stream(&context));
    assert!(
        binding
            .accept_stream_from_module(&context, &stream)
            .is_err()
    );
    assert!(
        binding
            .open_stream_to_module(
                &context,
                StreamDescriptor::with_len(1),
                Duration::from_secs(1),
            )
            .await
            .is_err()
    );
    binding.close().await.unwrap();
    broker_task.abort();
}

#[tokio::test]
async fn callback_stream_acceptance_rejects_a_handle_opened_by_another_peer() {
    let (_host, broker_task, binding, module, bus) = active_stream_binding().await;
    let module_peer = module.unique_name().unwrap();
    let context = CallContext::brokered(Some(&module_peer), false);
    let outsider = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut outsider_writer = outsider
        .open_stream(binding.unique_name(), StreamDescriptor::with_len(1))
        .await
        .unwrap();

    assert!(
        binding
            .accept_stream_from_module(&context, &outsider_writer.stream_ref())
            .is_err()
    );

    outsider_writer.write(b"x").await.unwrap();
    outsider_writer.finish().await.unwrap();
    binding.close().await.unwrap();
    broker_task.abort();
}

struct ActualStreamCallback {
    binding: Arc<HostCallbackBinding>,
    observe_entered: Arc<Notify>,
    observe_release: Arc<Notify>,
    transfer_dispatches: Arc<AtomicUsize>,
    undeclared_dispatches: Arc<AtomicUsize>,
}

#[async_trait]
impl Interface for ActualStreamCallback {
    fn name(&self) -> InterfaceName {
        endpoint().interface
    }
    fn members(&self) -> Vec<MemberName> {
        vec![
            endpoint().members[0].clone(),
            stream_endpoint().members[0].clone(),
            "Undeclared".parse().unwrap(),
        ]
    }
    async fn call(&self, _: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Err(Error::failed("callback context is required"))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: serde_json::Value,
        context: &CallContext,
    ) -> Result<serde_json::Value> {
        if member == &endpoint().members[0] {
            self.observe_entered.notify_one();
            self.observe_release.notified().await;
            return Ok(serde_json::Value::Null);
        }
        if member.as_str() == "Undeclared" {
            self.undeclared_dispatches.fetch_add(1, Ordering::SeqCst);
            return Ok(serde_json::Value::Null);
        }
        self.transfer_dispatches.fetch_add(1, Ordering::SeqCst);
        if member != &stream_endpoint().members[0] {
            return Err(Error::failed("callback member is not declared"));
        }

        let (stream,): (StreamRef,) =
            serde_json::from_value(args).map_err(|_| Error::failed("invalid stream request"))?;
        let mut reader = self.binding.accept_stream_from_module(context, &stream)?;
        let received = reader.read_to_end_capped(12).await?;
        if received != b"module->host" {
            return Err(Error::failed(
                "module callback stream payload did not match",
            ));
        }
        let stream_result = self
            .binding
            .open_stream_to_module(
                context,
                StreamDescriptor::with_len(12),
                Duration::from_secs(2),
            )
            .await;
        let mut writer = stream_result?;
        let reply = writer.stream_ref();
        tokio::spawn(async move {
            let _ = writer.write(b"host->module").await;
            let _ = writer.finish().await;
        });
        serde_json::to_value(reply).map_err(|_| Error::failed("stream reply encoding failed"))
    }
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_HOST_STREAM_CALLBACK_MODULE built cdylib"]
async fn admitted_module_uses_only_declared_stream_callbacks_and_revoke_blocks_reentry() {
    let artifact = std::path::PathBuf::from(
        std::env::var_os("TINYBUS_TEST_HOST_STREAM_CALLBACK_MODULE").unwrap(),
    );
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_directory, artifact) = super::super::tests::staged_module(&artifact, &hash);
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    let binding = Arc::new(
        host.prepare_host_callbacks_with_stream_endpoints(
            vec![endpoint()],
            vec![stream_endpoint()],
        )
        .await
        .unwrap(),
    );
    let observe_entered = Arc::new(Notify::new());
    let observe_release = Arc::new(Notify::new());
    let transfer_dispatches = Arc::new(AtomicUsize::new(0));
    let undeclared_dispatches = Arc::new(AtomicUsize::new(0));
    binding
        .serve_at(
            endpoint().path,
            ActualStreamCallback {
                binding: binding.clone(),
                observe_entered: observe_entered.clone(),
                observe_release: observe_release.clone(),
                transfer_dispatches: transfer_dispatches.clone(),
                undeclared_dispatches: undeclared_dispatches.clone(),
            },
        )
        .await
        .unwrap();

    let loaded = host
        .load_file_with_host_callbacks(
            artifact,
            serde_json::json!({"host": binding.unique_name()}),
            &binding,
        )
        .await
        .unwrap();
    let client = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module = client
        .proxy(
            "org.example.EagerCallback",
            "/module",
            "org.example.EagerCallback",
        )
        .unwrap();
    let round_trip =
        tokio::spawn(async move { module.call::<serde_json::Value>("RoundTrip", ()).await });
    if tokio::time::timeout(Duration::from_secs(10), observe_entered.notified())
        .await
        .is_err()
    {
        panic!("the admitted module did not complete its stream round trip");
    }
    binding.revoke();
    observe_release.notify_one();
    let round_trip_result = tokio::time::timeout(Duration::from_secs(10), round_trip)
        .await
        .expect("module should observe revocation")
        .unwrap();
    let result = round_trip_result.unwrap();
    assert!(result.is_null());
    assert_eq!(transfer_dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(undeclared_dispatches.load(Ordering::SeqCst), 0);
    host.inner
        .stop(&loaded.name, Duration::from_secs(2))
        .await
        .unwrap();
    binding.close().await.unwrap();
    broker_task.abort();
}

#[tokio::test]
async fn rollback_after_attach_releases_the_actual_initialization_observer() {
    let broker = Broker::new();
    let host = Arc::new(ModuleHost::new(broker));
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let manifest = super::super::tests::manifest();
    let path = Path::new("observer-fixture.so");
    let mut admitted = host
        .validate(path, &TbAbiDescriptor::current("clock", "0.1.0"), &manifest)
        .unwrap();
    admitted.state = ModuleState::Resolved;
    let (transport, _) = ModuleTransport::new(admitted.name.clone(), vec![]);
    transport.pause_initialization();
    let weak = Arc::downgrade(&transport);
    host.attach_transport(
        path,
        admitted,
        manifest,
        transport.clone(),
        Activation {
            lazy_init: true,
            lazy_load: false,
            descriptor_info: None,
            pinned_sha256: None,
            pinned_library_sha256: None,
        },
    )
    .unwrap();
    tokio::task::yield_now().await;
    drop(LoadRollback {
        binding: &binding,
        host: &host,
        transport: Some(transport.clone()),
        committed: false,
        cause: AdmissionTermination::Canceled,
    });
    assert!(!transport.init_started());
    assert!(host.list().is_empty());
    drop(transport);
    tokio::time::timeout(Duration::from_millis(100), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn configuration_fingerprints_preserve_json_equality_without_normalizing_strings() {
    fn fingerprint(value: &serde_json::Value) -> [u8; 32] {
        let bytes = crate::Secret::new(serde_json::to_vec(value).unwrap());
        callback_config_fingerprint(bytes.expose_secret())
    }
    let negative = serde_json::json!({
        "z": [-0.0, {"number": -0.0, "text": "\"-0.0\"\\-0.0"}],
        "credential": "private-test-secret",
    });
    // Construct the same map in reverse insertion order.
    let mut positive = serde_json::Map::new();
    positive.insert(
        "credential".into(),
        serde_json::json!("private-test-secret"),
    );
    positive.insert(
        "z".into(),
        serde_json::json!([
            0.0, {"number": 0.0, "text": "\"-0.0\"\\-0.0"}
        ]),
    );
    let positive = serde_json::Value::Object(positive);
    assert_eq!(negative, positive);
    assert_eq!(fingerprint(&negative), fingerprint(&positive));
    for differing in [
        serde_json::json!({"z": [0, {"number": 0.0, "text": "\"-0.0\"\\-0.0"}], "credential": "private-test-secret"}),
        serde_json::json!({"z": [0.0, {"number": 0.0, "text": "\"0.0\"\\-0.0"}], "credential": "private-test-secret"}),
        serde_json::json!({"z": [0.0, {"number": 0.0, "text": "\"-0.0\"\\-0.0"}], "credential": "different-private-test-secret"}),
    ] {
        assert_ne!(negative, differing);
        assert_ne!(fingerprint(&negative), fingerprint(&differing));
    }
    assert_eq!(
        fingerprint(&serde_json::json!(-0.0)),
        fingerprint(&serde_json::json!(0.0))
    );
    assert_ne!(
        fingerprint(&serde_json::json!(-0.01)),
        fingerprint(&serde_json::json!(0.01))
    );
}

// Private fixture-only release ownership. Drop never joins or asserts.
struct ControlledRelease {
    barrier: Arc<super::super::super::transport::tests::NativeBarrier>,
    finish: Option<std::sync::mpsc::Sender<()>>,
    resume: Option<tokio::sync::oneshot::Sender<()>>,
    transport: Option<Arc<ModuleTransport>>,
}
impl ControlledRelease {
    fn new(barrier: Arc<super::super::super::transport::tests::NativeBarrier>) -> Self {
        Self {
            barrier,
            finish: None,
            resume: None,
            transport: None,
        }
    }
}
impl ControlledRelease {
    fn release(&mut self) {
        self.barrier
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1 = true;
        self.barrier.changed.notify_all();
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(());
        }
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
    }
}
impl Drop for ControlledRelease {
    fn drop(&mut self) {
        self.release();
        // Commit and schedule synchronously, before any Tokio cleanup observer
        // could be canceled. request_terminal anchors PhysicalJob in the actual
        // context and submits its OS thread; neither depends on this runtime.
        // Only fixture-local locks can be poisoned by test assertions; release
        // above recovers those. No SDK internal lock is held by this guard.
        if let Some(transport) = self.transport.take() {
            transport.request_terminal(false);
        }
    }
}

#[test]
fn controlled_release_wakes_entry_and_releases_channels_on_early_unwind() {
    use super::super::super::transport::tests::NativeBarrier;
    let barrier = Arc::new(NativeBarrier {
        state: Mutex::new((None, false, false)),
        changed: std::sync::Condvar::new(),
        entered: Notify::new(),
        joined: Notify::new(),
        finish_gate: Mutex::new(None),
    });
    let (finish, receive) = std::sync::mpsc::channel();
    let (resume, decision) = tokio::sync::oneshot::channel();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut cleanup = ControlledRelease::new(barrier.clone());
        cleanup.finish = Some(finish);
        cleanup.resume = Some(resume);
        panic!("fixture assertion before controlled release");
    }));
    let finish_result = receive.try_recv();
    let mut decision = decision;
    let decision_result = decision.try_recv();
    assert!(result.is_err());
    assert!(
        barrier.state.lock().unwrap().1,
        "early unwind must wake native entry"
    );
    assert!(
        finish_result.is_ok(),
        "finish must be released, not disconnected"
    );
    assert!(
        decision_result.is_ok(),
        "decision must be released, not disconnected"
    );
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_early_unwind_releases_entry_and_reclaims_after_callback_return() {
    use super::super::super::transport::tests::native_fixture;
    let (transport, host, init, barrier) = native_fixture();
    let weak = Arc::downgrade(&barrier);
    let mut cleanup = ControlledRelease::new(barrier.clone());
    cleanup.transport = Some(transport.clone());
    let (finish, receive) = std::sync::mpsc::channel();
    cleanup.finish = Some(finish);
    *barrier.finish_gate.lock().unwrap() = Some(receive);
    transport.defer_initialize(init, host);
    let initializing = tokio::spawn({
        let transport = transport.clone();
        async move { transport.ensure_initialized().await }
    });
    let entered = tokio::time::timeout(Duration::from_secs(2), barrier.entered.notified()).await;
    // Deliberate ordinary Rust unwind exercises the guard while actual C entry
    // is blocked on its condition variable. No panic crosses a C boundary.
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _cleanup = cleanup;
        panic!("early controlled fixture failure");
    }));
    let initialized = initializing.await;
    let stopped = transport.clone().stop(Duration::from_secs(2)).await;
    let callback_entered_after_worker_join = barrier.state.lock().unwrap().2;
    drop(barrier);
    drop(transport);
    let reclaimed = tokio::time::timeout(Duration::from_secs(2), async {
        while weak.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Stop completion, rather than callback entry or abort, proves actual return.
    assert!(entered.is_ok());
    assert!(unwind.is_err());
    assert!(initialized.is_ok());
    assert!(matches!(
        stopped,
        Ok(_)
            | Err(super::super::super::transport::StopError::NotStarted(
                Error::ConnectionClosed
            ))
    ));
    assert!(callback_entered_after_worker_join);
    assert!(reclaimed.is_ok());
}

#[test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
fn actual_native_release_owner_survives_canceled_waiter_and_runtime_drop() {
    use super::super::super::transport::tests::native_fixture;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (transport, host, init, barrier) = native_fixture();
    barrier.state.lock().unwrap().1 = true;
    transport.defer_initialize(init, host);
    let initialized = runtime.block_on(transport.ensure_initialized());
    let weak = Arc::downgrade(&barrier);
    let (finish, receive) = std::sync::mpsc::channel();
    *barrier.finish_gate.lock().unwrap() = Some(receive);
    let mut cleanup = ControlledRelease::new(barrier.clone());
    cleanup.transport = Some(transport.clone());
    // This test's external finish control stays held to inspect physical owner
    // retention. It is released before any assertions, including failure paths.
    drop(cleanup);
    let waiter = runtime.spawn({
        let transport = transport.clone();
        async move { transport.stop(Duration::from_secs(2)).await }
    });
    waiter.abort(); // intentionally cancel without relying on a first poll
    let canceled = runtime.block_on(waiter);
    let joined_worker = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), barrier.joined.notified()).await
    });
    drop(runtime);
    drop(barrier);
    drop(transport);
    let retained_during_callback = weak.strong_count() != 0;
    let released = finish.send(());
    // The physical thread is independent of the dropped Tokio runtime.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while weak.strong_count() != 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let reclaimed_after_return = weak.strong_count() == 0;
    assert!(initialized.is_ok());
    assert!(canceled.unwrap_err().is_cancelled());
    assert!(joined_worker.is_ok());
    assert!(retained_during_callback);
    assert!(released.is_ok());
    assert!(reclaimed_after_return);
}

// Manually polling the public future lets ordinary shutdown commit while the
// loader is suspended; no scheduler race decides which cancellation is tested.
async fn native_callback_load_after_ordinary_stop(drop_load: bool, reject_gate: bool) {
    use super::super::super::transport::tests::{native_fixture, native_fixture_json};
    use std::future::Future;
    let (unused, _, _, barrier) = native_fixture();
    let config = native_fixture_json(&unused);
    drop(unused);
    let artifact =
        std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTROLLED_INIT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = super::super::tests::staged_module(&artifact, &hash);
    let host = ModuleHost::new(Broker::new());
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    *binding.fixture_controller.lock().unwrap() = Some(barrier.clone());
    let (entered, _) = tokio::sync::mpsc::channel(1);
    binding
        .serve_at(
            endpoint().path,
            BlockingCallback {
                entered,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                published: Arc::new(AtomicUsize::new(0)),
                guard: binding.guard(),
            },
        )
        .await
        .unwrap();
    let mut cleanup = ControlledRelease::new(barrier.clone());
    let mut load = Some(Box::pin(
        host.load_file_with_host_callbacks(&artifact, config, &binding),
    ));
    let first =
        std::future::poll_fn(|cx| std::task::Poll::Ready(load.as_mut().unwrap().as_mut().poll(cx)))
            .await;
    if let std::task::Poll::Ready(completion) = first {
        cleanup.release();
        drop(completion);
        drop(load.take()); // already completed, no pending owner is discarded
        panic!("native fixture completed before C entry");
    }
    let entered = tokio::time::timeout(Duration::from_secs(2), barrier.entered.notified()).await;
    if entered.is_err() {
        cleanup.release();
        let completion = load.take().unwrap().await;
        drop(completion);
        panic!("native fixture entry watchdog expired; loader completion consumed");
    }
    let transport = host.inner.loaded.lock().unwrap()[0].transport.clone();
    cleanup.transport = Some(transport.clone());
    let view = barrier.state.lock().unwrap().0.unwrap();
    barrier.state.lock().unwrap().1 = true;
    barrier.changed.notify_all();
    let initialized = transport.ensure_initialized().await;
    // Advance the public future to its real withheld-Ready observation.
    let withheld =
        std::future::poll_fn(|cx| std::task::Poll::Ready(load.as_mut().unwrap().as_mut().poll(cx)))
            .await;
    let was_withheld = withheld.is_pending();
    let (finish, receive) = std::sync::mpsc::channel();
    *barrier.finish_gate.lock().unwrap() = Some(receive);
    cleanup.finish = Some(finish);
    let stopping = transport.clone();
    let stop = tokio::spawn(async move { stopping.stop(Duration::from_secs(2)).await });
    tokio::time::timeout(Duration::from_secs(2), barrier.joined.notified())
        .await
        .unwrap();
    let stopped_flags = unsafe { view.native_terminal_state.unwrap()(view.host_ctx) };
    let load_result = if let std::task::Poll::Ready(result) = withheld {
        Some(result)
    } else if reject_gate {
        transport.release_initialization(false);
        drop(load.take());
        None
    } else if drop_load {
        drop(load.take());
        None
    } else {
        // Real not-ready branch awaits close, whose native return is held.
        std::future::poll_fn(|cx| match load.as_mut().unwrap().as_mut().poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(None),
            std::task::Poll::Ready(result) => std::task::Poll::Ready(Some(result)),
        })
        .await
    };
    let held_flags = unsafe { view.native_terminal_state.unwrap()(view.host_ctx) };
    let held_fault = transport.is_faulted();
    cleanup.release();
    let stopped = stop.await;
    let loaded = if let Some(result) = load_result {
        drop(load.take());
        Some(result)
    } else if let Some(load) = load {
        Some(load.await)
    } else {
        None
    };
    cleanup.transport = None;
    assert!(initialized.is_ok());
    assert!(was_withheld, "fixture withholds Ready");
    assert!(stopped.unwrap().is_ok());
    if let Some(result) = loaded {
        assert!(result.is_err());
    }
    assert_eq!(stopped_flags, 2);
    assert_eq!(held_flags, 2, "ordinary cancellation cannot invent Fault");
    assert!(!held_fault);
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        10
    );
    assert!(barrier.state.lock().unwrap().2);
}

#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_callback_notready_after_ordinary_stop_preserves_cause() {
    native_callback_load_after_ordinary_stop(false, false).await;
}
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_callback_load_drop_after_ordinary_stop_preserves_cause() {
    native_callback_load_after_ordinary_stop(true, false).await;
}
#[tokio::test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_callback_gate_rejection_after_ordinary_stop_preserves_cause() {
    native_callback_load_after_ordinary_stop(false, true).await;
}

async fn served_native_binding(host: &ModuleHost) -> HostCallbackBinding {
    let binding = host.prepare_host_callbacks(vec![endpoint()]).await.unwrap();
    let (entered, _) = tokio::sync::mpsc::channel(1);
    binding
        .serve_at(
            endpoint().path,
            BlockingCallback {
                entered,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                published: Arc::new(AtomicUsize::new(0)),
                guard: binding.guard(),
            },
        )
        .await
        .unwrap();
    binding
}

async fn native_callback_decision_race(ready: bool, native_fault: bool) {
    use super::super::super::transport::tests::{native_fixture, native_fixture_json};
    let (unused, _, _, barrier) = native_fixture();
    let mut config = native_fixture_json(&unused);
    config["test_ready"] = serde_json::json!(ready);
    drop(unused);
    let artifact =
        std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTROLLED_INIT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = super::super::tests::staged_module(&artifact, &hash);
    let host = Arc::new(ModuleHost::new(Broker::new()));
    let binding = Arc::new(served_native_binding(&host).await);
    *binding.fixture_controller.lock().unwrap() = Some(barrier.clone());
    let decision = Arc::new(Notify::new());
    let termination = Arc::new(Notify::new());
    *binding.after_load_termination.lock().unwrap() = Some(termination.clone());
    let (resume, release) = tokio::sync::oneshot::channel();
    *binding.before_load_decision.lock().unwrap() = Some((decision.clone(), release));
    let mut cleanup = ControlledRelease::new(barrier.clone());
    cleanup.resume = Some(resume);
    let loading_host = host.clone();
    let loading_binding = binding.clone();
    let load = tokio::spawn(async move {
        loading_host
            .load_file_with_host_callbacks(artifact, config, &loading_binding)
            .await
    });
    // Keep the paused runtime runnable until actual C entry/publication. This
    // prevents virtual time from auto-advancing the initializer's deadline.
    while barrier.state.lock().unwrap().0.is_none() {
        tokio::task::yield_now().await;
    }
    let transport = host.inner.loaded.lock().unwrap()[0].transport.clone();
    cleanup.transport = Some(transport.clone());
    let view = barrier.state.lock().unwrap().0.unwrap();
    barrier.state.lock().unwrap().1 = true;
    barrier.changed.notify_all();
    // Poll until native initialization is genuinely published, without an idle
    // paused timer deciding the deadline before the native owner can run.
    while !transport.init_started() {
        tokio::task::yield_now().await;
    }
    // init_started precedes actual publication; the observer result is the proof.
    let initialized = transport.ensure_initialized();
    tokio::pin!(initialized);
    let initialization =
        std::future::poll_fn(
            |cx| match std::future::Future::poll(initialized.as_mut(), cx) {
                std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                std::task::Poll::Pending => {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            },
        )
        .await;
    if !ready {
        // Real wait_ready deadline, not a fake readiness flag. Let the public
        // future install its timer before advancing past the actual bound.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
    }
    decision.notified().await;
    let observed_ready = transport.is_ready();
    let (finish, receive) = std::sync::mpsc::channel();
    *barrier.finish_gate.lock().unwrap() = Some(receive);
    cleanup.finish = Some(finish);
    let stopping = transport.clone();
    let stop = tokio::spawn(async move { stopping.stop(Duration::from_secs(2)).await });
    while !barrier.state.lock().unwrap().2 {
        tokio::task::yield_now().await;
    }
    let stopped_flags = unsafe { view.native_terminal_state.unwrap()(view.host_ctx) };
    let mut fault_flags = None;
    if native_fault {
        unsafe {
            (view.fault)(view.host_ctx, std::ptr::null(), 0);
        }
        fault_flags = Some(unsafe { view.native_terminal_state.unwrap()(view.host_ctx) });
    }
    let resumed = cleanup.resume.take().unwrap().send(());
    // The physical worker can revoke the account independently. Observe the
    // loader's actual failure commit instead of treating revocation as proof.
    let terminated = if !ready {
        Some(tokio::time::timeout(Duration::from_secs(2), termination.notified()).await)
    } else {
        None
    };
    let held = unsafe { view.native_terminal_state.unwrap()(view.host_ctx) };
    cleanup.release();
    // Keep virtual time runnable through the actual opaque return.
    while !stop.is_finished() {
        tokio::task::yield_now().await;
    }
    let stopped = stop.await;
    let loaded = load.await;
    cleanup.transport = None;
    assert!(initialization.is_ok());
    assert!(resumed.is_ok());
    assert!(terminated.is_none_or(|result| result.is_ok()));
    assert!(stopped.unwrap().is_ok());
    assert!(loaded.unwrap().is_err());
    assert_eq!(observed_ready, ready);
    assert_eq!(stopped_flags, 2);
    if let Some(flags) = fault_flags {
        assert_eq!(flags, 6);
    }
    assert_eq!(held, if ready && !native_fault { 2 } else { 6 });
    assert_eq!(
        unsafe { view.native_terminal_state.unwrap()(view.host_ctx) },
        if ready && !native_fault { 10 } else { 14 }
    );
    assert_eq!(transport.is_faulted(), !ready || native_fault);
}

#[tokio::test(start_paused = true)]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_ready_commit_cancellation_preserves_ordinary_cause() {
    native_callback_decision_race(true, false).await;
}
#[tokio::test(start_paused = true)]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_readiness_deadline_still_faults_after_ordinary_stop() {
    native_callback_decision_race(false, false).await;
}
#[tokio::test(start_paused = true)]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_native_fault_at_ready_commit_dominates_ordinary_stop() {
    native_callback_decision_race(true, true).await;
}

#[derive(Debug, PartialEq, Eq)]
enum AttachOutcome {
    Attached,
    LoadSucceeded,
    LoadRefused,
    LoadConnectionClosed,
    LoadFailed,
    LoadPanicked,
    LoadCanceled,
    WatchdogExpired,
}

async fn await_native_attach<T>(
    attached: &Notify,
    load: &mut tokio::task::JoinHandle<Result<T>>,
    completed: &mut Option<std::result::Result<Result<T>, tokio::task::JoinError>>,
) -> AttachOutcome {
    // A fixture precondition watchdog, not a product security/readiness TTL.
    // Abort requests cooperative cancellation; it cannot interrupt synchronous
    // admission or prove physical native quiescence. The caller owns cleanup.
    tokio::select! {
        biased;
        _ = attached.notified() => AttachOutcome::Attached,
        result = &mut *load => {
            let category = match &result {
                Ok(Ok(_)) => AttachOutcome::LoadSucceeded,
                Ok(Err(Error::ModuleRefused { .. })) => AttachOutcome::LoadRefused,
                Ok(Err(Error::ConnectionClosed)) => AttachOutcome::LoadConnectionClosed,
                Ok(Err(_)) => AttachOutcome::LoadFailed,
                Err(error) if error.is_cancelled() => AttachOutcome::LoadCanceled,
                Err(_) => AttachOutcome::LoadPanicked,
            };
            *completed = Some(result);
            category
        },
        _ = tokio::time::sleep(Duration::from_secs(60)) => {
            load.abort();
            AttachOutcome::WatchdogExpired
        }
    }
}

#[tokio::test(start_paused = true)]
async fn setup_attachment_wait_allows_preload_to_outlive_product_deadlines() {
    let attached = Arc::new(Notify::new());
    let observed = attached.clone();
    let (release, preload) = tokio::sync::oneshot::channel::<()>();
    let mut load = tokio::spawn(async move {
        let _ = preload.await;
        std::future::pending::<Result<()>>().await
    });
    let wait = tokio::spawn(async move {
        let mut completed = None;
        let outcome = await_native_attach(&observed, &mut load, &mut completed).await;
        load.abort();
        let result = match completed {
            Some(result) => result,
            None => load.await,
        };
        (outcome, result)
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    let waiting = !wait.is_finished();
    let released = release.send(());
    attached.notify_one();
    let joined = wait.await;
    assert!(waiting, "valid preload was mistaken for setup failure");
    assert!(released.is_ok());
    let (outcome, result) = joined.unwrap();
    assert_eq!(outcome, AttachOutcome::Attached);
    assert!(result.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn setup_attachment_wait_reports_an_early_load_failure() {
    let attached = Notify::new();
    let mut load = tokio::spawn(async { Err::<(), _>(Error::ConnectionClosed) });
    let mut completed = None;
    let outcome = await_native_attach(&attached, &mut load, &mut completed).await;
    let result = completed.unwrap();
    assert_eq!(outcome, AttachOutcome::LoadConnectionClosed);
    assert!(matches!(result, Ok(Err(Error::ConnectionClosed))));
}

#[tokio::test]
async fn setup_attachment_wait_preserves_actual_admission_refusal_before_c_entry() {
    let host = Arc::new(ModuleHost::new(Broker::new()));
    let binding = Arc::new(served_native_binding(&host).await);
    let attached = Notify::new();
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("absent-module.so");
    let mut load = tokio::spawn({
        let host = host.clone();
        let binding = binding.clone();
        async move {
            host.load_file_with_host_callbacks(missing, serde_json::json!({}), &binding)
                .await
        }
    });
    let mut completed = None;
    let outcome = await_native_attach(&attached, &mut load, &mut completed).await;
    let completion = completed.unwrap();
    assert_eq!(outcome, AttachOutcome::LoadRefused);
    assert!(matches!(completion, Ok(Err(Error::ModuleRefused { .. }))));
    assert!(
        host.list().is_empty(),
        "refusal cannot attach or publish Ready"
    );
}

#[tokio::test(start_paused = true)]
async fn setup_attachment_watchdog_cancels_a_stuck_preload() {
    let attached = Notify::new();
    let mut load = tokio::spawn(std::future::pending::<Result<()>>());
    let wait = tokio::spawn(async move {
        let mut completed = None;
        let outcome = await_native_attach(&attached, &mut load, &mut completed).await;
        // Actual task completion is consumed. This synthetic task is cooperative;
        // this is not evidence of synchronous/native cancellation or C return.
        let result = match completed {
            Some(result) => result,
            None => load.await,
        };
        (outcome, result)
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(61)).await;
    let (outcome, result) = wait.await.unwrap();
    assert_eq!(outcome, AttachOutcome::WatchdogExpired);
    assert!(result.unwrap_err().is_cancelled());
}

async fn native_callback_setup_failure_race(panic_setup: bool) {
    use super::super::super::transport::tests::{native_fixture, native_fixture_json};
    let (unused, _, _, barrier) = native_fixture();
    let config = native_fixture_json(&unused);
    drop(unused);
    let artifact =
        std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_CONTROLLED_INIT_MODULE").unwrap());
    let hash = crate::module::hash::file_hex(std::fs::File::open(&artifact).unwrap()).unwrap();
    let (_dir, artifact) = super::super::tests::staged_module(&artifact, &hash);
    let host = Arc::new(ModuleHost::new(Broker::new()));
    let binding = Arc::new(served_native_binding(&host).await);
    *binding.fixture_controller.lock().unwrap() = Some(barrier.clone());
    let attached = Arc::new(Notify::new());
    let seen = attached.clone();
    let (resume, release) = std::sync::mpsc::channel();
    let mut cleanup = ControlledRelease::new(barrier.clone());
    cleanup.finish = Some(resume);
    *binding.after_attach.lock().unwrap() = Some(Box::new(move || {
        seen.notify_one();
        let _ = release.recv();
        assert!(
            !panic_setup,
            "actual Rust unwind during host callback setup"
        );
    }));
    let loading_host = host.clone();
    let loading_binding = binding.clone();
    let mut load = tokio::spawn(async move {
        loading_host
            .load_file_with_host_callbacks(artifact, config, &loading_binding)
            .await
    });
    let mut completed = None;
    let attachment = await_native_attach(&attached, &mut load, &mut completed).await;
    if attachment != AttachOutcome::Attached {
        cleanup.release();
        // Report the original finite cause before cleanup: synchronous admission
        // cannot be interrupted by abort. Do not turn it into a readiness failure.
        eprintln!("native fixture attachment: {attachment:?}");
        if let Some(completion) = completed {
            drop(completion);
        } else if tokio::time::timeout(Duration::from_secs(2), &mut load)
            .await
            .is_err()
        {
            // Pending ownership is explicit, not a quiescence claim. The task
            // consumes eventual completion and keeps the real staging, binding,
            // host and controller alive; transport's physical anchor also remains.
            tokio::spawn(async move {
                let _owners = (_dir, host, binding, barrier);
                let _ = load.await;
            });
            panic!("native fixture attachment: {attachment:?}; cleanup pending");
        }
        panic!("native fixture attachment: {attachment:?}; loader completion consumed");
    }
    let transport = host.inner.loaded.lock().unwrap()[0].transport.clone();
    cleanup.transport = Some(transport.clone());
    // Actual ordinary lifecycle ownership, before the callback initialization
    // gate opens. This instance proves C never entered, rather than any join.
    let stopped = transport.clone().stop(Duration::from_secs(2)).await;
    let held_fault = transport.is_faulted();
    if !panic_setup {
        // Public revocation makes real setup return its concurrently-used error.
        binding.revoke();
    }
    cleanup.release();
    let result = load.await;
    cleanup.transport = None;
    assert!(stopped.is_ok());
    assert!(!held_fault);
    if panic_setup {
        assert!(result.unwrap_err().is_panic());
    } else {
        assert!(result.unwrap().is_err());
    }
    assert!(
        transport.is_faulted(),
        "genuine setup failure upgrades ordinary stop"
    );
    assert!(
        barrier.state.lock().unwrap().0.is_none(),
        "C initializer never entered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_callback_setup_error_still_faults_after_ordinary_stop() {
    native_callback_setup_failure_race(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
async fn actual_callback_setup_unwind_still_faults_after_ordinary_stop() {
    native_callback_setup_failure_race(true).await;
}

#[test]
fn a_transport_rejects_replacement_of_its_exact_callback_account() {
    let first = state(":1.1".parse().unwrap(), ":1.2".parse().unwrap());
    let second = state(":1.3".parse().unwrap(), ":1.4".parse().unwrap());
    let original = Arc::downgrade(&first);
    let replacement = Arc::downgrade(&second);
    let (transport, _) = ModuleTransport::new("one-account".into(), vec![]);
    transport.bind_host_callbacks(first.clone()).unwrap();
    let rejected = transport.bind_host_callbacks(second.clone());
    drop(first);
    drop(second);
    assert!(rejected.is_err());
    assert!(original.upgrade().is_some());
    assert!(replacement.upgrade().is_none());
}

struct DestructionObserved {
    guard: HostCallbackGuard,
    observed: Arc<AtomicUsize>,
}
impl Drop for DestructionObserved {
    fn drop(&mut self) {
        self.observed.store(self.guard.running(), Ordering::Release);
    }
}
struct PendingUntilDestroyed {
    complete: bool,
    entered: Arc<Notify>,
    guard: HostCallbackGuard,
    observed: Arc<AtomicUsize>,
}
#[async_trait]
impl Interface for PendingUntilDestroyed {
    fn name(&self) -> InterfaceName {
        endpoint().interface
    }
    fn members(&self) -> Vec<MemberName> {
        endpoint().members
    }
    async fn call(&self, _: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Err(Error::failed("context required"))
    }
    async fn call_with_context(
        &self,
        _: &MemberName,
        _: serde_json::Value,
        _: &CallContext,
    ) -> Result<serde_json::Value> {
        let _destruction = DestructionObserved {
            guard: self.guard.clone(),
            observed: self.observed.clone(),
        };
        self.entered.notify_one();
        if self.complete {
            Ok(serde_json::Value::Null)
        } else {
            std::future::pending().await
        }
    }
}

#[test]
#[ignore = "requires TINYBUS_TEST_CONTROLLED_INIT_MODULE built cdylib"]
fn runtime_drop_destroys_provider_future_before_offruntime_native_cleanup() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let account = state(":1.1".parse().unwrap(), ":1.2".parse().unwrap());
    let observed = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let (transport, view, barrier, user_entered, wait_entered, replacement_rejected) = runtime
        .block_on(async {
            let (transport, view, init, barrier) =
                crate::module::transport::tests::native_fixture();
            barrier.state.lock().unwrap().1 = true;
            transport.bind_host_callbacks(account.clone()).unwrap();
            transport.defer_initialize(init, view);
            transport.ensure_initialized().await.unwrap();
            barrier.entered.notified().await;
            let guarded = GuardedInterface {
                interface: PendingUntilDestroyed {
                    complete: false,
                    entered: entered.clone(),
                    guard: HostCallbackGuard(account.clone()),
                    observed: observed.clone(),
                },
                confidential_endpoint: Some(endpoint()),
                stream_endpoint: None,
                state: account.clone(),
            };
            let context = CallContext::brokered(account.module_peer().as_ref(), true);
            tokio::spawn(async move {
                guarded
                    .call_with_context(&endpoint().members[0], serde_json::json!([]), &context)
                    .await
            });
            let user_entered = tokio::time::timeout(Duration::from_secs(2), entered.notified())
                .await
                .is_ok();
            let replacement = state(":1.3".parse().unwrap(), ":1.4".parse().unwrap());
            let replacement_rejected = transport.bind_host_callbacks(replacement).is_err();
            transport.request_terminal(false);
            let wait_entered = tokio::time::timeout(
                Duration::from_secs(2),
                account.physical_wait_started.notified(),
            )
            .await
            .is_ok();
            (
                transport,
                view,
                barrier,
                user_entered,
                wait_entered,
                replacement_rejected,
            )
        });
    let count_before_drop = HostCallbackGuard(account.clone()).running();
    let terminal_before_drop = unsafe { view.native_terminal_state.unwrap()(view.host_ctx) };
    let lifecycle_free = !transport.lifecycle_is_owned_for_test();
    // This drops the actual GuardedInterface/user future, not a caller observer.
    drop(runtime);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let completed = loop {
        if unsafe { view.native_terminal_state.unwrap()(view.host_ctx) }
            & crate::module::abi::TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED
            != 0
        {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    assert!(completed, "actual native callback return remains pending");
    assert!(user_entered && wait_entered && replacement_rejected);
    assert_eq!(count_before_drop, 1);
    assert_eq!(
        terminal_before_drop,
        crate::module::abi::TB_NATIVE_TERMINAL_STOP_INTENT
    );
    assert!(lifecycle_free);
    assert_eq!(
        observed.load(Ordering::Acquire),
        1,
        "user future destruction precedes counter release"
    );
    assert_eq!(HostCallbackGuard(account).running(), 0);
    assert!(barrier.state.lock().unwrap().2);
    assert!(!transport.is_faulted());
}

#[tokio::test]
async fn completing_provider_future_is_destroyed_before_its_running_account() {
    let account = state(":1.1".parse().unwrap(), ":1.2".parse().unwrap());
    let observed = Arc::new(AtomicUsize::new(0));
    let guarded = GuardedInterface {
        interface: PendingUntilDestroyed {
            complete: true,
            entered: Arc::new(Notify::new()),
            guard: HostCallbackGuard(account.clone()),
            observed: observed.clone(),
        },
        confidential_endpoint: Some(endpoint()),
        stream_endpoint: None,
        state: account.clone(),
    };
    let context = CallContext::brokered(account.module_peer().as_ref(), true);
    let result = guarded
        .call_with_context(&endpoint().members[0], serde_json::json!([]), &context)
        .await;
    assert!(result.is_ok());
    assert_eq!(observed.load(Ordering::Acquire), 1);
    assert_eq!(HostCallbackGuard(account).running(), 0);
}

#[tokio::test]
async fn host_stream_callbacks_are_bound_to_the_declared_module_peer() {
    let broker = Broker::new();
    let bus = crate::transport::memory::MemoryBus::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let binding = host
        .prepare_host_callbacks_with_stream_endpoints(vec![], vec![stream_endpoint()])
        .await
        .unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap();
    binding
        .serve_at("/callback".parse().unwrap(), StreamCallback)
        .await
        .unwrap();
    {
        let mut state = binding.state.lifetime.lock().unwrap();
        state.active = true;
        state.module = Some(module_peer.clone());
    }
    let context = CallContext::brokered(Some(&module_peer), false);
    assert!(
        binding
            .authorized_stream_module(&CallContext::brokered(Some(&module_peer), true))
            .is_err()
    );
    assert!(
        binding
            .authorized_stream_module(&CallContext::brokered(
                Some(&"org.example.Outsider".parse().unwrap()),
                false,
            ))
            .is_err()
    );
    let proxy = module
        .proxy(binding.unique_name(), "/callback", "org.example.Callback")
        .unwrap();
    assert_eq!(
        proxy
            .call::<serde_json::Value>("Transfer", serde_json::json!({ "ok": true }))
            .await
            .unwrap(),
        serde_json::json!([{ "ok": true }])
    );

    let mut module_to_host = module
        .open_stream(binding.unique_name(), StreamDescriptor::with_len(5))
        .await
        .unwrap();
    let mut host_reader = binding
        .accept_stream_from_module(&context, &module_to_host.stream_ref())
        .unwrap();
    let send = tokio::spawn(async move {
        module_to_host.write(b"hello").await.unwrap();
        module_to_host.finish().await.unwrap();
    });
    assert_eq!(host_reader.read_to_end_capped(5).await.unwrap(), b"hello");
    send.await.unwrap();

    let mut host_to_module = binding
        .open_stream_to_module(
            &context,
            StreamDescriptor::with_len(5),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let mut module_reader = module
        .accept_stream_from(&host_to_module.stream_ref(), binding.unique_name())
        .unwrap();
    host_to_module.write(b"world").await.unwrap();
    host_to_module.finish().await.unwrap();
    assert_eq!(module_reader.read_to_end_capped(5).await.unwrap(), b"world");

    binding.close().await.unwrap();
    broker_task.abort();
}
