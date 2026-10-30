use super::*;
use crate::CallContext;
use crate::broker::Broker;
use crate::transport::memory::{MemoryBus, MemoryTransport};
use async_trait::async_trait;

/// A service that answers `Echo` and fails `Boom`.
struct Echo;

#[async_trait]
impl Interface for Echo {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.Test").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![
            MemberName::new("Echo").unwrap(),
            MemberName::new("Boom").unwrap(),
            MemberName::new("Panic").unwrap(),
            MemberName::new("Hang").unwrap(),
            MemberName::new("Large").unwrap(),
        ]
    }

    async fn call(&self, member: &MemberName, args: Value) -> Result<Value> {
        match member.as_str() {
            "Echo" => Ok(args),
            "Boom" => Err(Error::failed("as requested")),
            "Panic" => panic!("secret panic payload"),
            "Hang" => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(Value::Null)
            }
            "Large" => Ok(Value::String(
                "x".repeat(crate::message::codec::MAX_FRAME_LEN + 1),
            )),
            other => Err(Error::failed(format!("unreachable: {other}"))),
        }
    }
}

struct ReplacementEcho;

#[async_trait]
impl Interface for ReplacementEcho {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.Test").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Ok(Value::String("replacement".to_owned()))
    }
}

fn path() -> ObjectPath {
    ObjectPath::new("/ai/tinyhumans/Test").unwrap()
}

#[tokio::test]
async fn try_serve_at_refuses_a_collision_without_replacing_the_live_interface() {
    let (client, service) = brokered_pair().await;
    let error = service
        .try_serve_at(path(), ReplacementEcho)
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::FAILED);

    let result: Value = client
        .proxy("ai.tinyhumans.Test", path(), "ai.tinyhumans.Test")
        .unwrap()
        .call("Echo", ("original",))
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!(["original"]));
}

#[tokio::test]
async fn serve_at_keeps_its_existing_replacement_behavior() {
    let (client, service) = brokered_pair().await;
    service.serve_at(path(), ReplacementEcho).await.unwrap();

    let result: Value = client
        .proxy("ai.tinyhumans.Test", path(), "ai.tinyhumans.Test")
        .unwrap()
        .call("Echo", ())
        .await
        .unwrap();
    assert_eq!(result, Value::String("replacement".to_string()));
}

#[tokio::test]
async fn concurrent_try_serve_at_calls_have_exactly_one_winner() {
    let (client, service) = brokered_pair().await;
    let concurrent_path = ObjectPath::new("/ai/tinyhumans/TestConcurrent").unwrap();
    let (first, second) = tokio::join!(
        service.try_serve_at(concurrent_path.clone(), ReplacementEcho),
        service.try_serve_at(concurrent_path.clone(), Echo),
    );
    assert_ne!(first.is_ok(), second.is_ok());

    let result: Value = client
        .proxy("ai.tinyhumans.Test", concurrent_path, "ai.tinyhumans.Test")
        .unwrap()
        .call("Echo", ("original",))
        .await
        .unwrap();
    if first.is_ok() {
        assert_eq!(result, Value::String("replacement".to_string()));
    } else {
        assert_eq!(result, serde_json::json!(["original"]));
    }
}

struct DifferentInterface;

#[async_trait]
impl Interface for DifferentInterface {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.Other").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }

    async fn call(&self, _member: &MemberName, _args: Value) -> Result<Value> {
        Ok(Value::String("other".to_string()))
    }
}

#[tokio::test]
async fn try_serve_at_refuses_any_occupied_object_path() {
    let (client, service) = brokered_pair().await;
    service.serve_at(path(), DifferentInterface).await.unwrap();
    assert!(service.try_serve_at(path(), ReplacementEcho).await.is_err());

    let result: Value = client
        .proxy("ai.tinyhumans.Test", path(), "ai.tinyhumans.Other")
        .unwrap()
        .call("Echo", ())
        .await
        .unwrap();
    assert_eq!(result, Value::String("other".to_string()));
}

fn call(member: &str, body: Value) -> Message {
    Message::method_call(
        BusName::new("ai.tinyhumans.Test").unwrap(),
        path(),
        InterfaceName::new("ai.tinyhumans.Test").unwrap(),
        MemberName::new(member).unwrap(),
        body,
    )
}

/// Two connections wired directly to each other, with no broker in the
/// middle: enough to exercise dispatch, replies, serials and timeouts.
async fn pair() -> (Connection, Connection) {
    let (a, b) = MemoryTransport::pair();
    let client = Connection::attach(Arc::new(a));
    let service = Connection::attach(Arc::new(b));
    service.serve_at(path(), Echo).await.unwrap();
    (client, service)
}

async fn brokered_pair() -> (Connection, Connection) {
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let service = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name("ai.tinyhumans.Test").await.unwrap();
    service.serve_at(path(), Echo).await.unwrap();
    let client = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    (client, service)
}

#[tokio::test]
async fn try_serve_at_keeps_the_original_interface_after_a_collision() {
    let (client, service) = pair().await;
    assert!(service.try_serve_at(path(), ReplacementEcho).await.is_err());

    let result: Value = client
        .proxy("ai.tinyhumans.Test", path(), "ai.tinyhumans.Test")
        .unwrap()
        .call("Echo", ("original",))
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!(["original"]));
}

struct CaptureCallContext(std::sync::Mutex<Option<tokio::sync::oneshot::Sender<CallContext>>>);

#[async_trait]
impl Interface for CaptureCallContext {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("org.example.ContextCapture").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Capture").unwrap()]
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("context-aware dispatch required"))
    }

    async fn call_with_context(
        &self,
        _: &MemberName,
        _: Value,
        context: &CallContext,
    ) -> Result<Value> {
        self.0
            .lock()
            .unwrap()
            .take()
            .expect("one capture call")
            .send(context.clone())
            .map_err(|_| Error::failed("context receiver closed"))?;
        Ok(Value::Null)
    }
}

struct CapturedContext {
    context: CallContext,
    receiver: Connection,
    caller: Connection,
    _broker_task: tokio::task::JoinHandle<Result<()>>,
}

async fn capture_context_from_fresh_broker() -> CapturedContext {
    let bus = MemoryBus::new();
    let broker_task = Broker::new().spawn(bus.clone());
    let receiver = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let (capture, captured_rx) = tokio::sync::oneshot::channel();
    receiver
        .serve_at(
            "/capture".parse().unwrap(),
            CaptureCallContext(std::sync::Mutex::new(Some(capture))),
        )
        .await
        .unwrap();
    receiver
        .request_name("org.example.ContextCapture")
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(
            "org.example.ContextCapture",
            "/capture",
            "org.example.ContextCapture",
        )
        .unwrap();
    let _: Value = proxy.call("Capture", ()).await.unwrap();
    CapturedContext {
        context: captured_rx.await.unwrap(),
        receiver,
        caller,
        _broker_task: broker_task,
    }
}

#[tokio::test]
async fn captured_contexts_from_distinct_brokers_do_not_match_same_named_receivers() {
    let first = capture_context_from_fresh_broker().await;
    let second = capture_context_from_fresh_broker().await;
    assert_eq!(
        first.caller.unique_name(),
        second.caller.unique_name(),
        "fresh brokers deliberately assign the same sender name"
    );
    assert!(first.context.is_for_connection(&first.receiver.downgrade()));
    assert!(
        second
            .context
            .is_for_connection(&second.receiver.downgrade())
    );
    assert!(
        !first
            .context
            .is_for_connection(&second.receiver.downgrade())
    );
    assert!(
        !second
            .context
            .is_for_connection(&first.receiver.downgrade())
    );
}

struct MutatesObjectTree {
    service: Connection,
    extra_path: ObjectPath,
}

#[async_trait]
impl Interface for MutatesObjectTree {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.Test").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![
            MemberName::new("Serve").unwrap(),
            MemberName::new("Unserve").unwrap(),
        ]
    }

    async fn call(&self, member: &MemberName, _args: Value) -> Result<Value> {
        match member.as_str() {
            "Serve" => {
                self.service.serve_at(self.extra_path.clone(), Echo).await?;
                Ok(Value::Bool(true))
            }
            "Unserve" => Ok(Value::Bool(self.service.unserve(&self.extra_path).await)),
            _ => unreachable!("members() lists only the supported operations"),
        }
    }
}

#[tokio::test]
async fn a_service_can_export_and_remove_objects_during_a_method_call() {
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let service = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name("ai.tinyhumans.Test").await.unwrap();
    let client = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let extra_path = ObjectPath::new("/ai/tinyhumans/TestExtra").unwrap();
    service
        .serve_at(
            path(),
            MutatesObjectTree {
                service: service.clone(),
                extra_path: extra_path.clone(),
            },
        )
        .await
        .unwrap();
    let proxy = client
        .proxy("ai.tinyhumans.Test", path().as_str(), "ai.tinyhumans.Test")
        .unwrap()
        .with_timeout(Duration::from_millis(250));

    assert!(proxy.call::<bool>("Serve", ()).await.unwrap());
    let extra = client
        .proxy(
            "ai.tinyhumans.Test",
            extra_path.as_str(),
            "ai.tinyhumans.Test",
        )
        .unwrap();
    assert_eq!(
        extra.call::<Value>("Echo", ("served",)).await.unwrap(),
        serde_json::json!(["served"])
    );

    assert!(proxy.call::<bool>("Unserve", ()).await.unwrap());
    let removed_call = extra.call::<Value>("Echo", ("removed",)).await;
    assert!(
        matches!(
            removed_call,
            Err(Error::MethodFailed { ref name, .. })
                if name == "ai.tinyhumans.tinybus.Error.UnknownObject"
        ),
        "expected removal to reject the object, got {removed_call:?}"
    );
}

#[tokio::test]
async fn a_call_reaches_the_service_and_the_reply_comes_back() {
    let (client, _service) = pair().await;
    let reply = client
        .call_raw(call("Echo", serde_json::json!(["hi"])), DEFAULT_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(reply, serde_json::json!(["hi"]));
}

#[tokio::test]
async fn a_reply_larger_than_one_frame_arrives_through_a_bounded_stream() {
    let (client, _service) = brokered_pair().await;
    let proxy = client
        .proxy("ai.tinyhumans.Test", path().as_str(), "ai.tinyhumans.Test")
        .unwrap();
    let reply: String = proxy.call_streaming("Large", ()).await.unwrap();
    assert_eq!(reply.len(), crate::message::codec::MAX_FRAME_LEN + 1);
    assert!(reply.bytes().all(|byte| byte == b'x'));
}

struct PausedMethodTransport {
    inner: Box<dyn crate::ports::Transport>,
    paused_member: &'static str,
    blocked: tokio::sync::mpsc::UnboundedSender<()>,
    resume: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait]
impl crate::ports::Transport for PausedMethodTransport {
    async fn send(&self, message: Message) -> Result<()> {
        if message.header.member.as_ref().map(MemberName::as_str) == Some(self.paused_member) {
            self.blocked.send(()).unwrap();
            self.resume.notified().await;
        }
        self.inner.send(message).await
    }

    async fn recv(&self) -> Result<Option<Message>> {
        self.inner.recv().await
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

struct ReadsUploadedStream {
    connection: Connection,
    received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

#[async_trait]
impl Interface for ReadsUploadedStream {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.StreamHandover").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Upload").unwrap()]
    }

    async fn call(&self, _member: &MemberName, args: Value) -> Result<Value> {
        let (stream,): (crate::stream::StreamRef,) = serde_json::from_value(args)?;
        let bytes = self.connection.read_stream(&stream).await?;
        self.received.send(bytes.clone()).unwrap();
        Ok(serde_json::to_value(bytes.len())?)
    }
}

struct ReplacementUpload {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl Interface for ReplacementUpload {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.StreamHandover").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Upload").unwrap()]
    }

    async fn call(&self, _member: &MemberName, _args: Value) -> Result<Value> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn a_stream_method_stays_with_the_owner_that_opened_its_stream_after_name_handover() {
    const NAME: &str = "ai.tinyhumans.StreamHandover";
    const PATH: &str = "/ai/tinyhumans/StreamHandover";

    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let owner = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    owner.request_name(NAME).await.unwrap();
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    owner
        .serve_at(
            ObjectPath::new(PATH).unwrap(),
            ReadsUploadedStream {
                connection: owner.clone(),
                received: received_tx,
            },
        )
        .await
        .unwrap();

    let replacement = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let replacement_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    replacement
        .serve_at(
            ObjectPath::new(PATH).unwrap(),
            ReplacementUpload {
                calls: replacement_calls.clone(),
            },
        )
        .await
        .unwrap();

    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let (blocked_tx, mut blocked_rx) = tokio::sync::mpsc::unbounded_channel();
    let client_transport = PausedMethodTransport {
        inner: bus.connect().await.unwrap(),
        paused_member: "Upload",
        blocked: blocked_tx,
        resume: resume.clone(),
    };
    let client = Connection::connect(Box::new(client_transport))
        .await
        .unwrap();
    let upload = tokio::spawn(async move {
        client
            .call_with_stream::<usize>(
                BusName::new(NAME).unwrap(),
                ObjectPath::new(PATH).unwrap(),
                InterfaceName::new(NAME).unwrap(),
                MemberName::new("Upload").unwrap(),
                |stream| serde_json::json!([stream]),
                b"stream opened by the original owner",
            )
            .await
    });

    // The Open reply has arrived before the transport pauses the following
    // method call. Move the public name while leaving the original stream peer
    // alive, then allow the method frame to route.
    tokio::time::timeout(Duration::from_secs(10), blocked_rx.recv())
        .await
        .expect("the post-Open method call reached the transport barrier")
        .expect("the transport barrier remained connected");
    owner.release_name(NAME).await.unwrap();
    replacement.request_name(NAME).await.unwrap();
    resume.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(10), upload)
        .await
        .expect("the streamed method completed after name handover")
        .unwrap();
    assert_eq!(
        result.unwrap(),
        b"stream opened by the original owner".len()
    );
    assert_eq!(
        replacement_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the replacement owner must not receive the method"
    );
    assert_eq!(
        received_rx.recv().await.unwrap(),
        b"stream opened by the original owner"
    );
}

#[tokio::test]
async fn a_streaming_caller_accepts_an_ordinary_legacy_reply() {
    let (client, _service) = pair().await;
    let reply: Vec<String> = client
        .decode_streaming_reply(serde_json::json!(["legacy"]), None)
        .await
        .unwrap();
    assert_eq!(reply, vec!["legacy"]);
}

#[tokio::test]
async fn a_streamed_reply_can_keep_progressing_past_the_reply_deadline() {
    let (client, service) = pair().await;
    let destination = BusName::new("ai.tinyhumans.Test").unwrap();
    let mut writer = service
        .open_stream(&destination, StreamDescriptor::with_len(6))
        .await
        .unwrap();
    let stream = writer.stream_ref();
    let send = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        writer.write(b"\"slow\"").await.unwrap();
        writer.finish().await.unwrap();
    });
    let reply = tokio::time::timeout(
        Duration::from_secs(1),
        client.decode_streaming_reply::<String>(
            serde_json::json!({ "$tinybus_stream_reply": stream }),
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.await.unwrap();
    assert_eq!(reply, "slow");
}

#[tokio::test]
async fn a_streamed_reply_that_stops_making_progress_times_out() {
    let (client, service) = pair().await;
    client.set_stream_limits(StreamLimits {
        idle_timeout: Duration::from_millis(20),
        ..StreamLimits::default()
    });
    let destination = BusName::new("ai.tinyhumans.Test").unwrap();
    let writer = service
        .open_stream(&destination, StreamDescriptor::with_len(6))
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        client.decode_streaming_reply::<String>(
            serde_json::json!({ "$tinybus_stream_reply": writer.stream_ref() }),
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("went idle"), "{error}");
}

#[tokio::test]
async fn a_streaming_call_keeps_errors_as_ordinary_replies() {
    let (client, _service) = brokered_pair().await;
    let proxy = client
        .proxy("ai.tinyhumans.Test", path().as_str(), "ai.tinyhumans.Test")
        .unwrap();
    let error = proxy.call_streaming::<Value>("Boom", ()).await.unwrap_err();
    assert_eq!(error.wire_name(), Error::FAILED);
}

#[tokio::test]
async fn a_confidential_call_refuses_streamed_reply_semantics_before_send() {
    let (client, _service) = pair().await;
    let mut message = Message::streaming_call(
        BusName::new("ai.tinyhumans.Test").unwrap(),
        path(),
        InterfaceName::new("ai.tinyhumans.Test").unwrap(),
        MemberName::new("Echo").unwrap(),
        serde_json::json!(["secret"]),
    );
    message.header.confidential = true;
    let error = client.call_raw(message, DEFAULT_TIMEOUT).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot request a streamed reply")
    );
}

#[tokio::test]
async fn a_failing_method_arrives_as_an_error_with_its_name_intact() {
    let (client, _service) = pair().await;
    let err = client
        .call_raw(call("Boom", serde_json::json!([])), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(err.wire_name(), Error::FAILED);
    assert!(err.to_string().contains("as requested"), "{err}");
}

#[tokio::test]
async fn a_panicking_module_method_becomes_an_error_reply_rather_than_an_abort() {
    let (client, service) = pair().await;
    service.__set_panic_handler(Arc::new(|| Error::MethodFailed {
        name: "ai.tinyhumans.tinybus.Error.ModulePanicked".to_string(),
        message: "module panicked at fixture.rs:12:3".to_string(),
    }));
    let error = client
        .call_raw(call("Panic", serde_json::json!([])), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModulePanicked"
    );
    let reply = client
        .call_raw(call("Echo", serde_json::json!(["alive"])), DEFAULT_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(reply, serde_json::json!(["alive"]));
}

#[tokio::test]
async fn a_panic_reply_carries_the_location_but_never_the_payload() {
    let (client, service) = pair().await;
    service.__set_panic_handler(Arc::new(|| Error::MethodFailed {
        name: "ai.tinyhumans.tinybus.Error.ModulePanicked".to_string(),
        message: "module panicked at fixture.rs:12:3".to_string(),
    }));
    let error = client
        .call_raw(call("Panic", serde_json::json!([])), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fixture.rs:12:3"));
    assert!(!error.to_string().contains("secret panic payload"));
}

#[tokio::test]
async fn an_unknown_member_is_reported_as_such_rather_than_hanging() {
    let (client, _service) = pair().await;
    let err = client
        .call_raw(call("Nope", serde_json::json!([])), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(err.wire_name(), Error::UNKNOWN_METHOD);
}

#[tokio::test]
async fn a_wedged_method_times_out_the_caller_and_not_the_connection() {
    let (client, _service) = pair().await;
    let err = client
        .call_raw(
            call("Hang", serde_json::json!([])),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Timeout { .. }), "{err}");

    // The point of the exercise: the connection still works afterwards.
    let reply = client
        .call_raw(
            call("Echo", serde_json::json!(["still here"])),
            DEFAULT_TIMEOUT,
        )
        .await
        .unwrap();
    assert_eq!(reply, serde_json::json!(["still here"]));
}

#[tokio::test]
async fn a_timed_out_call_does_not_leak_its_pending_slot() {
    let (client, _service) = pair().await;
    let _ = client
        .call_raw(
            call("Hang", serde_json::json!([])),
            Duration::from_millis(20),
        )
        .await;
    assert!(client.inner.pending.lock().await.is_empty());
}

#[tokio::test]
async fn concurrent_calls_are_matched_by_serial_not_by_order() {
    let (client, _service) = pair().await;
    let slow = client.call_raw(call("Echo", serde_json::json!(["first"])), DEFAULT_TIMEOUT);
    let fast = client.call_raw(call("Echo", serde_json::json!(["second"])), DEFAULT_TIMEOUT);
    let (a, b) = tokio::join!(slow, fast);
    assert_eq!(a.unwrap(), serde_json::json!(["first"]));
    assert_eq!(b.unwrap(), serde_json::json!(["second"]));
}

#[tokio::test]
async fn a_dropped_peer_wakes_waiters_instead_of_making_them_wait_out_the_clock() {
    let (a, b) = MemoryTransport::pair();
    let client = Connection::attach(Arc::new(a));
    drop(b);
    let err = client
        .call_raw(
            call("Echo", serde_json::json!([])),
            Duration::from_secs(300),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConnectionClosed | Error::Transport(_)),
        "{err}"
    );
}

#[test]
fn a_scalar_argument_is_wrapped_into_the_positional_array() {
    assert_eq!(to_body(&"a").unwrap(), serde_json::json!(["a"]));
    assert_eq!(to_body(&("a", 1)).unwrap(), serde_json::json!(["a", 1]));
    assert_eq!(to_body(&Vec::<u8>::new()).unwrap(), serde_json::json!([]));
    assert_eq!(to_body(&Value::Null).unwrap(), serde_json::json!([]));
}

#[test]
fn a_rule_round_trips_through_its_wire_form() {
    let rule = MatchRule::parse(
        "type=signal,interface=ai.tinyhumans.Mail,member=Received,path_namespace=/ai/Mail",
    )
    .unwrap();
    assert_eq!(MatchRule::parse(&rule_to_wire(&rule)).unwrap(), rule);
}

#[tokio::test]
async fn broker_introspection_and_name_lifecycle_use_the_typed_helpers() {
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let connection = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let unique = connection.unique_name().unwrap();

    assert!(connection.list_names().await.unwrap().contains(&unique));
    assert_eq!(
        connection.name_owner(&unique).await.unwrap(),
        Some(unique.clone())
    );
    connection
        .request_name("ai.tinyhumans.TestService")
        .await
        .unwrap();
    assert_eq!(
        connection
            .name_owner("ai.tinyhumans.TestService")
            .await
            .unwrap(),
        Some(unique.clone())
    );
    connection
        .release_name("ai.tinyhumans.TestService")
        .await
        .unwrap();
    assert!(
        connection
            .name_owner("ai.tinyhumans.TestService")
            .await
            .unwrap()
            .is_none()
    );

    let manifest = PeerManifest::new("connection-test");
    connection.announce(&manifest).await.unwrap();
    assert_eq!(
        connection.manifest_of(&unique).await.unwrap(),
        Some(manifest)
    );
    assert_eq!(connection.peers().await.unwrap().len(), 1);
}

// The module-management methods they call exist only when the `modules`
// feature is compiled in; the portability jobs build without it.
#[cfg(feature = "modules")]
#[tokio::test]
async fn module_management_helpers_forward_their_requests_to_the_broker() {
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let connection = Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();

    assert!(connection.list_modules().await.is_err());
    assert!(connection.module("missing").await.is_err());
    assert!(connection.module_manifest("missing").await.is_err());
    assert!(connection.rescan_modules().await.is_err());
    assert!(
        connection
            .scan_modules([std::path::Path::new("/definitely/not/a/module")], true)
            .await
            .is_err()
    );
    assert!(
        connection
            .load_module("/definitely/not/a/module", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(
        connection
            .reinitialize_module("missing", serde_json::json!({ "secret": "redacted" }))
            .await
            .is_err()
    );
    assert!(
        connection
            .stop_module("missing", Duration::from_millis(1))
            .await
            .is_err()
    );
    assert!(connection.enable_module("missing", true).await.is_err());
}

struct ContextEcho;
#[async_trait]
impl Interface for ContextEcho {
    fn name(&self) -> InterfaceName {
        Echo.name()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }
    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("context override was lost"))
    }
    async fn call_with_context(
        &self,
        _: &MemberName,
        args: Value,
        context: &crate::CallContext,
    ) -> Result<Value> {
        Ok(serde_json::json!([context.authenticated_sender(), args]))
    }
}

#[tokio::test]
async fn brokered_context_authenticates_distinct_senders_and_overwrites_forgery() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name("ai.tinyhumans.Test").await.unwrap();
    service
        .serve_at(path(), Arc::new(ContextEcho))
        .await
        .unwrap();
    let first = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let second = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert_ne!(first.unique_name(), second.unique_name());
    for caller in [&first, &second] {
        let mut request = call("Echo", serde_json::json!([42]));
        request.header.sender = first.unique_name();
        let result = caller.call_raw(request, DEFAULT_TIMEOUT).await.unwrap();
        assert_eq!(result, serde_json::json!([caller.unique_name(), [42]]));
    }
}

#[tokio::test]
async fn direct_header_sender_is_not_authenticated() {
    let (a, b) = MemoryTransport::pair();
    let service = Connection::attach(Arc::new(b));
    service.serve_at(path(), ContextEcho).await.unwrap();
    let client = Connection::attach(Arc::new(a));
    let mut request = call("Echo", serde_json::json!([]));
    request.header.sender = Some(BusName::new(":1.42").unwrap());
    let result = client.call_raw(request, DEFAULT_TIMEOUT).await.unwrap();
    assert_eq!(result, serde_json::json!([null, []]));
}

struct RegisterDuringCall(Connection);
#[async_trait]
impl Interface for RegisterDuringCall {
    fn name(&self) -> InterfaceName {
        Echo.name()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }
    async fn call(&self, _: &MemberName, args: Value) -> Result<Value> {
        self.0
            .serve_at(ObjectPath::new("/replacement").unwrap(), Echo)
            .await?;
        Ok(args)
    }
}

#[tokio::test]
async fn inbound_callback_can_register_an_interface_without_deadlock() {
    let (client, service) = pair().await;
    service
        .serve_at(path(), RegisterDuringCall(service.clone()))
        .await
        .unwrap();
    let result = client
        .call_raw(
            call("Echo", serde_json::json!([1])),
            Duration::from_millis(500),
        )
        .await;
    assert_eq!(result.unwrap(), serde_json::json!([1]));
    service.unserve(&path()).await;
}

struct CaptureContext(mpsc::UnboundedSender<Option<BusName>>);
#[async_trait]
impl Interface for CaptureContext {
    fn name(&self) -> InterfaceName {
        Echo.name()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }
    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        unreachable!()
    }
    async fn call_with_context(
        &self,
        _: &MemberName,
        _: Value,
        context: &crate::CallContext,
    ) -> Result<Value> {
        self.0
            .send(context.authenticated_sender().cloned())
            .unwrap();
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn retaining_a_broker_endpoint_does_not_allow_authenticated_injection() {
    let (peer, broker_end) = MemoryTransport::pair();
    let broker_end = Arc::new(broker_end);
    Broker::new().attach(broker_end.clone());
    let service = Connection::connect(Box::new(peer)).await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    service.serve_at(path(), CaptureContext(tx)).await.unwrap();
    let mut forged = call("Echo", serde_json::json!([]));
    forged.header.sender = Some(BusName::new(":1.42").unwrap());
    broker_end.send(forged).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap(),
        None
    );
}

struct LyingTransport {
    inner: MemoryTransport,
}
impl LyingTransport {
    // This is exactly why provenance does not use a user-overridable as_any.
    fn as_any(&self) -> &dyn std::any::Any {
        &self.inner
    }
}
#[async_trait]
impl Transport for LyingTransport {
    async fn send(&self, message: Message) -> Result<()> {
        self.inner.send(message).await
    }
    async fn recv(&self) -> Result<Option<Message>> {
        let mut message = self.inner.recv().await?;
        if let Some(message) = message
            .as_mut()
            .filter(|message| message.header.kind == MessageKind::MethodCall)
        {
            message.header.sender = Some(BusName::new(":1.42").unwrap());
        }
        Ok(message)
    }
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
    fn describe(&self) -> String {
        "memory".into()
    }
}

#[tokio::test]
async fn a_custom_transport_cannot_borrow_its_inner_broker_provenance() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus.clone());
    let (peer, broker_end) = MemoryTransport::pair();
    broker.attach(Arc::new(broker_end));
    let wrapper = LyingTransport { inner: peer };
    assert!(wrapper.as_any().is::<MemoryTransport>());
    let service = Connection::connect(Box::new(wrapper)).await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    service.serve_at(path(), CaptureContext(tx)).await.unwrap();
    service.request_name("ai.tinyhumans.Test").await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    client
        .send(call("Echo", serde_json::json!([])))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn replacing_a_well_known_name_does_not_inherit_the_old_sender_identity() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name("ai.tinyhumans.Test").await.unwrap();
    service.serve_at(path(), ContextEcho).await.unwrap();
    let first = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    first.request_name("ai.tinyhumans.Operator").await.unwrap();
    let old = first.unique_name().unwrap();
    first.release_name("ai.tinyhumans.Operator").await.unwrap();
    first.close().await.unwrap();
    let replacement = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    replacement
        .request_name("ai.tinyhumans.Operator")
        .await
        .unwrap();
    let current = replacement.unique_name().unwrap();
    assert_ne!(old, current);
    assert_eq!(
        service.name_owner("ai.tinyhumans.Operator").await.unwrap(),
        Some(current.clone())
    );
    let mut request = call("Echo", serde_json::json!([]));
    request.header.sender = Some(old);
    let result = replacement
        .call_raw(request, DEFAULT_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!([current, []]));
}

struct ParkedSnapshot {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Interface for ParkedSnapshot {
    fn name(&self) -> InterfaceName {
        Echo.name()
    }
    fn members(&self) -> Vec<MemberName> {
        vec!["Echo".parse().unwrap()]
    }
    async fn call(&self, _: &MemberName, args: Value) -> Result<Value> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(args)
    }
}

#[tokio::test]
async fn unserve_stops_new_admission_but_preserves_an_already_admitted_call() {
    let (client, service) = pair().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    service
        .serve_at(
            path(),
            ParkedSnapshot {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
    let active_client = client.clone();
    let active = tokio::spawn(async move {
        active_client
            .call_raw(call("Echo", serde_json::json!([1])), DEFAULT_TIMEOUT)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), service.unserve(&path()))
            .await
            .unwrap()
    );
    let rejected = client
        .call_raw(call("Echo", serde_json::json!([2])), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(
        rejected.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownObject"
    );
    release.notify_one();
    assert_eq!(active.await.unwrap().unwrap(), serde_json::json!([1]));
}

/// A served object needs its connection to issue callbacks, but must not own it.
struct CallbackConnection {
    connection: WeakConnection,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for CallbackConnection {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl Interface for CallbackConnection {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.test.Callback").unwrap()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Owner").unwrap()]
    }
    async fn call(&self, _member: &MemberName, _args: Value) -> Result<Value> {
        Ok(serde_json::json!(
            self.connection
                .upgrade()
                .ok_or(Error::ConnectionClosed)?
                .unique_name()
                .map(|name| name.to_string())
        ))
    }
}

#[tokio::test]
async fn callback_connection_does_not_keep_its_served_object_and_bus_name_alive() -> Result<()> {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await?).await?;
    let client = Connection::connect(bus.connect().await?).await?;
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    service
        .serve_at(
            ObjectPath::new("/test/Callback")?,
            CallbackConnection {
                connection: service.downgrade(),
                dropped: dropped.clone(),
            },
        )
        .await?;
    service.request_name("ai.test.Callback").await?;
    let owner: Option<String> = client
        .proxy("ai.test.Callback", "/test/Callback", "ai.test.Callback")?
        .call("Owner", ())
        .await?;
    assert_eq!(owner, service.unique_name().map(|name| name.to_string()));
    drop(service);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if dropped.load(std::sync::atomic::Ordering::SeqCst)
                && !client
                    .list_names()
                    .await?
                    .iter()
                    .any(|name| name.as_str() == "ai.test.Callback")
            {
                return Result::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::failed("callback connection retained its object/name"))??;
    Ok(())
}

#[tokio::test]
async fn weak_references_cannot_resurrect_dispatch_state_after_the_last_owner_drops() -> Result<()>
{
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await?).await?;
    let weak = connection.downgrade();
    let weak_clone = weak.clone();
    let upgraded = weak.upgrade().ok_or(Error::ConnectionClosed)?;
    assert_eq!(upgraded.unique_name(), connection.unique_name());
    drop(connection);
    assert!(weak_clone.upgrade().is_some());
    drop(upgraded);
    // The dispatch task still has its own Arc<Inner> until it sees closure.
    // That is not permission to recreate the lifetime guard.
    assert!(weak.upgrade().is_none());
    assert!(weak_clone.upgrade().is_none());
    assert!(format!("{weak:?}").contains("has_strong_owner: false"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opted_in_method_fault_fences_before_primary_payload_drop() {
    struct Payload {
        fenced: Arc<AtomicBool>,
        observed: Arc<AtomicBool>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.observed
                .store(self.fenced.load(Ordering::Acquire), Ordering::Release);
        }
    }
    struct PanicService(Arc<AtomicBool>, Arc<AtomicBool>);
    #[async_trait]
    impl Interface for PanicService {
        fn name(&self) -> InterfaceName {
            "ai.tinyhumans.Test".parse().unwrap()
        }
        fn members(&self) -> Vec<MemberName> {
            vec!["Panic".parse().unwrap()]
        }
        async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
            std::panic::panic_any(Payload {
                fenced: self.0.clone(),
                observed: self.1.clone(),
            });
        }
    }
    let fenced = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let (client, service) = pair().await;
    service
        .serve_at(path(), PanicService(fenced.clone(), observed.clone()))
        .await
        .unwrap();
    let fence = fenced.clone();
    service.__set_catch_observer(Arc::new(move || {
        fence.store(true, Ordering::Release);
    }));
    service.__set_panic_handler(Arc::new(|| Error::MethodFailed {
        name: "ai.tinyhumans.tinybus.Error.ModulePanicked".into(),
        message: "a module method panicked".into(),
    }));
    let error = client
        .call_raw(call("Panic", Value::Null), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.ModulePanicked"
    );
    assert!(
        observed.load(Ordering::Acquire),
        "primary payload dropped before opted-in fault observer"
    );
}
