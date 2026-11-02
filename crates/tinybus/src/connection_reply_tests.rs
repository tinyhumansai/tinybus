//! Reply authorization regressions over broker-stamped memory deliveries.

use super::*;
use crate::broker::Broker;
use crate::transport::memory::MemoryBus;

#[tokio::test]
async fn another_broker_peer_cannot_complete_a_waiter_with_a_forged_reply() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let responder = bus.connect().await.unwrap();
    let mut hello = Message::method_call(
        BusName::new(crate::BUS_NAME).unwrap(),
        ObjectPath::new(crate::BUS_PATH).unwrap(),
        InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
        MemberName::new("Hello").unwrap(),
        serde_json::json!([]),
    );
    hello.header.serial = 1;
    responder.send(hello).await.unwrap();
    let responder_name = BusName::new(
        responder
            .recv()
            .await
            .unwrap()
            .unwrap()
            .body
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let requester = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let forger = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut barriers = requester
        .add_match(MatchRule::new().signals())
        .await
        .unwrap();
    let request = Message::method_call(
        responder_name.clone(),
        ObjectPath::new("/fixture").unwrap(),
        InterfaceName::new("ai.tinyhumans.Fixture").unwrap(),
        MemberName::new("Await").unwrap(),
        serde_json::json!([]),
    );
    let pending = tokio::spawn({
        let requester = requester.clone();
        async move { requester.call_raw(request, DEFAULT_TIMEOUT).await }
    });
    let delivered = responder.recv().await.unwrap().unwrap();
    assert_eq!(delivered.header.sender, requester.unique_name());
    let mut forged = Message::method_return(&delivered.header, serde_json::json!("forged"));
    // Even a claimed B sender is overwritten to the actual peer C.
    forged.header.sender = Some(responder_name.clone());
    forger.send(forged).await.unwrap();
    forger
        .emit(
            ObjectPath::new("/fixture").unwrap(),
            InterfaceName::new("ai.tinyhumans.Fixture").unwrap(),
            MemberName::new("Barrier").unwrap(),
            (),
        )
        .await
        .unwrap();
    let barrier = tokio::time::timeout(DEFAULT_TIMEOUT, barriers.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(barrier.header.sender, forger.unique_name());
    // That connection's ordered signal arrived after its forged response, so
    // requester dispatch has processed the forgery before B is allowed to reply.
    let mut genuine = Message::method_return(&delivered.header, serde_json::json!("genuine"));
    genuine.header.serial = 2;
    responder.send(genuine).await.unwrap();
    assert_eq!(
        pending.await.unwrap().unwrap(),
        serde_json::json!("genuine")
    );
}

fn fixture_call(destination: BusName) -> Message {
    Message::method_call(
        destination,
        ObjectPath::new("/fixture").unwrap(),
        InterfaceName::new("ai.tinyhumans.Fixture").unwrap(),
        MemberName::new("Await").unwrap(),
        serde_json::json!([]),
    )
}

async fn raw_control(peer: &dyn Transport, serial: u64, member: &str, body: Value) -> Message {
    let mut call = Message::method_call(
        BusName::new(crate::BUS_NAME).unwrap(),
        ObjectPath::new(crate::BUS_PATH).unwrap(),
        InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
        MemberName::new(member).unwrap(),
        body,
    );
    call.header.serial = serial;
    peer.send(call).await.unwrap();
    loop {
        let reply = peer.recv().await.unwrap().unwrap();
        if reply.header.reply_serial == Some(serial) {
            return reply;
        }
    }
}

async fn reject_forged_replies(named: bool, confidential: bool, genuine_error: bool) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let responder = bus.connect().await.unwrap();
    let hello = raw_control(responder.as_ref(), 1, "Hello", serde_json::json!([])).await;
    let responder_name = BusName::new(hello.body.as_str().unwrap()).unwrap();
    let alias = BusName::new("ai.tinyhumans.Fixture").unwrap();
    if named {
        let reply = raw_control(
            responder.as_ref(),
            2,
            "RequestName",
            serde_json::json!([alias]),
        )
        .await;
        assert_eq!(reply.header.kind, MessageKind::MethodReturn);
    }
    #[cfg(feature = "modules")]
    if confidential {
        broker.attest_module(
            &responder_name,
            crate::attest::Attestation {
                name: alias.clone(),
                sha256: "a".repeat(64),
                library_sha256: None,
            },
        );
    }
    let requester = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let forger = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut barriers = requester
        .add_match(MatchRule::new().signals())
        .await
        .unwrap();
    let mut request = fixture_call(if named { alias } else { responder_name.clone() });
    request.header.confidential = confidential;
    let pending = tokio::spawn({
        let requester = requester.clone();
        async move { requester.call_raw(request, DEFAULT_TIMEOUT).await }
    });
    let delivered = responder.recv().await.unwrap().unwrap();
    if named {
        assert_eq!(
            delivered.header.expected_responder,
            Some(responder_name.clone())
        );
    }
    for error in [false, true] {
        for private in [false, true] {
            let mut forged = if error {
                Message::error_reply(&delivered.header, &Error::ConnectionClosed)
            } else {
                Message::method_return(&delivered.header, serde_json::json!("forged"))
            };
            forged.header.confidential = private;
            forged.header.sensitive = false;
            forged.header.sender = Some(BusName::new(crate::BUS_NAME).unwrap());
            forger.send(forged).await.unwrap();
        }
    }
    forger
        .emit(
            ObjectPath::new("/fixture").unwrap(),
            InterfaceName::new("ai.tinyhumans.Fixture").unwrap(),
            MemberName::new("Barrier").unwrap(),
            (),
        )
        .await
        .unwrap();
    let barrier = tokio::time::timeout(DEFAULT_TIMEOUT, barriers.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(barrier.header.sender, forger.unique_name());
    let mut genuine = if genuine_error {
        Message::error_reply(
            &delivered.header,
            &Error::UnknownMethod {
                interface: InterfaceName::new("ai.tinyhumans.Fixture").unwrap(),
                member: MemberName::new("Await").unwrap(),
            },
        )
    } else {
        Message::method_return(&delivered.header, serde_json::json!("genuine"))
    };
    genuine.header.serial = 3;
    responder.send(genuine).await.unwrap();
    let result = pending.await.unwrap();
    if genuine_error {
        let error = result.unwrap_err();
        assert!(matches!(error, Error::MethodFailed { ref name, .. }
            if name == Error::UnknownMethod { interface: InterfaceName::new("ai.tinyhumans.Fixture").unwrap(), member: MemberName::new("Await").unwrap() }.wire_name()));
    } else {
        assert_eq!(result.unwrap(), serde_json::json!("genuine"));
    }
}

#[tokio::test]
async fn named_reply_rejects_forged_success_and_errors_with_any_private_flags() {
    reject_forged_replies(true, false, false).await;
}

#[tokio::test]
async fn unique_reply_preserves_genuine_error_after_rejecting_forged_errors() {
    reject_forged_replies(false, false, true).await;
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn confidential_named_reply_binds_the_attested_owner() {
    reject_forged_replies(true, true, false).await;
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn confidential_unique_reply_preserves_genuine_sanitized_error() {
    reject_forged_replies(false, true, true).await;
}

async fn alias_takeover_is_refused(confidential: bool) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let original = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let replacement = bus.connect().await.unwrap();
    let requester = bus.connect().await.unwrap();
    raw_control(replacement.as_ref(), 1, "Hello", serde_json::json!([])).await;
    raw_control(requester.as_ref(), 1, "Hello", serde_json::json!([])).await;
    let alias = BusName::new("ai.tinyhumans.Fixture").unwrap();
    original.request_name(alias.clone()).await.unwrap();
    #[cfg(feature = "modules")]
    if confidential {
        broker.attest_module(
            &original.unique_name().unwrap(),
            crate::attest::Attestation {
                name: alias.clone(),
                sha256: "a".repeat(64),
                library_sha256: None,
            },
        );
    }
    let captured_reply = raw_control(
        requester.as_ref(),
        2,
        "ResolveCallTarget",
        serde_json::json!([alias]),
    )
    .await;
    let captured: BusName = serde_json::from_value(captured_reply.body).unwrap();
    original.release_name(alias.clone()).await.unwrap();
    raw_control(
        replacement.as_ref(),
        2,
        "RequestName",
        serde_json::json!([alias]),
    )
    .await;
    let mut call = fixture_call(alias.clone());
    call.header.expected_responder = Some(captured.clone());
    call.header.confidential = confidential;
    call.header.serial = 3;
    requester.send(call).await.unwrap();
    let refusal = requester.recv().await.unwrap().unwrap();
    assert_eq!(refusal.header.reply_serial, Some(3));
    assert_eq!(refusal.header.kind, MessageKind::Error);
    assert_eq!(
        refusal.header.sender.as_ref().unwrap().as_str(),
        crate::BUS_NAME
    );
    assert_eq!(
        refusal.header.error_name.as_deref(),
        Some(Error::NameHasNoOwner(alias.clone()).wire_name())
    );
    // An ordered broker control reply to the replacement is a barrier after
    // the refused call: an incorrectly routed call would arrive before it.
    let mut ping = fixture_call(BusName::new(crate::BUS_NAME).unwrap());
    ping.header.path = Some(ObjectPath::new(crate::BUS_PATH).unwrap());
    ping.header.interface = Some(InterfaceName::new(crate::BUS_INTERFACE).unwrap());
    ping.header.member = Some(MemberName::new("Ping").unwrap());
    ping.header.serial = 3;
    replacement.send(ping).await.unwrap();
    let first = replacement.recv().await.unwrap().unwrap();
    assert_eq!(first.header.kind, MessageKind::MethodReturn);
    assert_eq!(first.header.reply_serial, Some(3));
    let sdk = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut stale = fixture_call(alias.clone());
    stale.header.expected_responder = Some(captured);
    assert!(matches!(sdk.call_raw(stale, DEFAULT_TIMEOUT).await,
        Err(Error::NameHasNoOwner(name)) if name == alias));
}

#[tokio::test]
async fn raw_direct_reply_compatibility_does_not_claim_broker_identity() {
    let (a, b) = crate::transport::memory::MemoryTransport::pair();
    let requester = Connection::attach(Arc::new(a));
    let responder = Arc::new(b);
    let pending = tokio::spawn({
        let requester = requester.clone();
        async move {
            requester
                .call_raw(fixture_call(BusName::new(":1.2").unwrap()), DEFAULT_TIMEOUT)
                .await
        }
    });
    let delivered = responder.recv().await.unwrap().unwrap();
    let mut response = Message::method_return(&delivered.header, serde_json::json!("raw"));
    response.header.serial = 1;
    responder.send(response).await.unwrap();
    assert_eq!(pending.await.unwrap().unwrap(), serde_json::json!("raw"));
}

#[test]
fn reply_authority_rejects_other_peers_and_requires_private_success() {
    let expected = BusName::new(":1.2").unwrap();
    let caller = BusName::new(":1.1").unwrap();
    let (sender, _) = oneshot::channel();
    let waiter = PendingReply {
        sender,
        expected: Some(expected.clone()),
        bus_control: false,
        confidential: true,
        unverified_direct: false,
    };
    let mut call = fixture_call(expected.clone());
    call.header.serial = 8;
    call.header.sender = Some(caller);
    for kind in [MessageKind::MethodReturn, MessageKind::Error] {
        for private in [false, true] {
            for sensitive in [false, true] {
                let mut reply = Message::method_return(&call.header, Value::Null);
                reply.header.kind = kind;
                reply.header.sender = Some(BusName::new(":1.3").unwrap());
                reply.header.confidential = private;
                reply.header.sensitive = sensitive;
                assert!(!waiter.accepts(&reply, true));
                reply.header.sender = Some(expected.clone());
                assert_eq!(
                    waiter.accepts(&reply, true),
                    kind == MessageKind::Error || private
                );
                reply.header.sender = Some(BusName::new(crate::BUS_NAME).unwrap());
                assert_eq!(waiter.accepts(&reply, true), kind == MessageKind::Error);
            }
        }
    }
    let (sender, _) = oneshot::channel();
    let control = PendingReply {
        sender,
        expected: None,
        bus_control: true,
        confidential: false,
        unverified_direct: false,
    };
    let mut reply = Message::method_return(&call.header, Value::Null);
    reply.header.sender = Some(expected);
    assert!(!control.accepts(&reply, true));
    reply.header.sender = None;
    assert!(control.accepts(&reply, true));
}

#[tokio::test]
async fn legacy_alias_calls_remain_routable_without_a_conditional_header() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let responder = bus.connect().await.unwrap();
    raw_control(responder.as_ref(), 1, "Hello", serde_json::json!([])).await;
    let alias = BusName::new("ai.tinyhumans.Fixture").unwrap();
    raw_control(
        responder.as_ref(),
        2,
        "RequestName",
        serde_json::json!([alias]),
    )
    .await;
    let requester = bus.connect().await.unwrap();
    raw_control(requester.as_ref(), 1, "Hello", serde_json::json!([])).await;
    let mut call = fixture_call(alias);
    call.header.serial = 2;
    assert!(call.header.expected_responder.is_none());
    requester.send(call).await.unwrap();
    let delivered = responder.recv().await.unwrap().unwrap();
    assert_eq!(delivered.header.kind, MessageKind::MethodCall);
    let mut reply = Message::method_return(&delivered.header, serde_json::json!("legacy"));
    reply.header.serial = 3;
    responder.send(reply).await.unwrap();
    assert_eq!(
        requester.recv().await.unwrap().unwrap().body,
        serde_json::json!("legacy")
    );
}

#[tokio::test]
async fn unsupported_owner_guard_negotiation_fails_before_alias_delivery() {
    let (a, b) = crate::transport::memory::MemoryTransport::pair();
    let server = tokio::spawn(async move {
        let hello = b.recv().await.unwrap().unwrap();
        let mut hello_reply = Message::method_return(&hello.header, serde_json::json!(":1.1"));
        hello_reply.header.serial = 1;
        b.send(hello_reply).await.unwrap();
        let lookup = b.recv().await.unwrap().unwrap();
        assert_eq!(
            lookup.header.member.as_ref().unwrap().as_str(),
            "ResolveCallTarget"
        );
        let mut refusal = Message::error_reply(
            &lookup.header,
            &Error::UnknownMethod {
                interface: InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
                member: MemberName::new("ResolveCallTarget").unwrap(),
            },
        );
        refusal.header.serial = 2;
        b.send(refusal).await.unwrap();
    });
    let requester = Connection::connect(Box::new(a)).await.unwrap();
    let error = requester
        .call_raw(
            fixture_call(BusName::new("ai.tinyhumans.Fixture").unwrap()),
            DEFAULT_TIMEOUT,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::MethodFailed { ref name, .. } if name == Error::UNKNOWN_METHOD));
    server.await.unwrap();
}

#[test]
fn conditional_owner_header_requires_a_unique_method_call_target() {
    let mut call = fixture_call(BusName::new("ai.tinyhumans.Fixture").unwrap());
    call.header.serial = 1;
    call.header.expected_responder = Some(BusName::new("ai.tinyhumans.Other").unwrap());
    assert!(call.validate().is_err());
    call.header.expected_responder = Some(BusName::new(":1.2").unwrap());
    assert!(call.validate().is_ok());
    let mut reply = Message::method_return(&call.header, Value::Null);
    reply.header.serial = 2;
    reply.header.expected_responder = call.header.expected_responder;
    assert!(reply.validate().is_err());
}

#[tokio::test]
async fn captured_alias_owner_is_checked_atomically_before_dispatch() {
    alias_takeover_is_refused(false).await;
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn confidential_alias_takeover_is_refused_before_dispatch() {
    alias_takeover_is_refused(true).await;
}

#[tokio::test]
async fn genuine_broker_routing_and_control_errors_retain_native_wire_names() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let requester = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let missing = BusName::new(":1.9999").unwrap();
    let error = requester
        .call_raw(fixture_call(missing.clone()), DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::MethodFailed { ref name, .. }
        if name == Error::NameHasNoOwner(missing).wire_name()));
    let mut private = fixture_call(BusName::new(crate::BUS_NAME).unwrap());
    private.header.confidential = true;
    let error = requester
        .call_raw(private, DEFAULT_TIMEOUT)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::MethodFailed { ref name, .. }
        if name == Error::not_attested(BusName::new(crate::BUS_NAME).unwrap(), "fixture").wire_name()));
}

#[test]
fn absent_conditional_header_round_trips_legacy_wire_frames() {
    let mut call = fixture_call(BusName::new("ai.tinyhumans.Fixture").unwrap());
    call.header.serial = 1;
    let encoded = serde_json::to_value(&call).unwrap();
    assert!(encoded["header"].get("expected_responder").is_none());
    let decoded: Message = serde_json::from_value(encoded).unwrap();
    assert!(decoded.header.expected_responder.is_none());
    call.header.expected_responder = Some(BusName::new(":1.2").unwrap());
    let decoded: Message = serde_json::from_value(serde_json::to_value(call).unwrap()).unwrap();
    assert_eq!(
        decoded.header.expected_responder.as_ref().unwrap().as_str(),
        ":1.2"
    );
}

async fn negotiated_owner_reply(owner: &str) -> Result<Value> {
    let owner = owner.to_owned();
    let (a, b) = crate::transport::memory::MemoryTransport::pair();
    let server = tokio::spawn(async move {
        let hello = b.recv().await.unwrap().unwrap();
        let mut reply = Message::method_return(&hello.header, serde_json::json!(":1.1"));
        reply.header.serial = 1;
        b.send(reply).await.unwrap();
        let lookup = b.recv().await.unwrap().unwrap();
        let mut reply = Message::method_return(&lookup.header, serde_json::json!(owner));
        reply.header.serial = 2;
        b.send(reply).await.unwrap();
        if owner.starts_with(':') {
            let call = b.recv().await.unwrap().unwrap();
            assert!(call.header.serial > lookup.header.serial);
            assert_eq!(
                call.header.expected_responder.as_ref().unwrap().as_str(),
                owner
            );
            let mut reply = Message::method_return(&call.header, serde_json::json!("bound"));
            reply.header.sender = Some(BusName::new(owner).unwrap());
            reply.header.serial = 3;
            b.send(reply).await.unwrap();
        }
    });
    let requester = Connection::connect(Box::new(a)).await.unwrap();
    let result = requester
        .call_raw(
            fixture_call(BusName::new("ai.tinyhumans.Fixture").unwrap()),
            DEFAULT_TIMEOUT,
        )
        .await;
    server.await.unwrap();
    result
}

#[tokio::test]
async fn guarded_call_serial_follows_its_owner_negotiation() {
    assert_eq!(
        negotiated_owner_reply(":1.2").await.unwrap(),
        serde_json::json!("bound")
    );
}

#[tokio::test]
async fn owner_negotiation_rejects_non_unique_responder_before_delivery() {
    assert!(matches!(
        negotiated_owner_reply("ai.tinyhumans.Fixture").await,
        Err(Error::Protocol(_))
    ));
}

#[tokio::test]
async fn stream_chunks_and_close_stay_with_the_peer_that_answered_open() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let original = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let replacement = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let requester = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let alias = BusName::new("ai.tinyhumans.Fixture").unwrap();
    original.request_name(alias.clone()).await.unwrap();
    let mut writer = requester
        .open_stream(&alias, StreamDescriptor::with_len(7))
        .await
        .unwrap();
    let mut reader = original.accept_stream(&writer.stream_ref()).unwrap();
    original.release_name(alias.clone()).await.unwrap();
    replacement.request_name(alias).await.unwrap();
    let received = tokio::spawn(async move { reader.read_to_end_capped(16).await });
    writer.write(b"payload").await.unwrap();
    writer.finish().await.unwrap();
    assert_eq!(received.await.unwrap().unwrap(), b"payload");
}

#[tokio::test]
async fn stream_abort_stays_with_the_peer_that_answered_open() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let original = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let replacement = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let requester = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let alias = BusName::new("ai.tinyhumans.Fixture").unwrap();
    original.request_name(alias.clone()).await.unwrap();
    let writer = requester
        .open_stream(&alias, StreamDescriptor::with_len(7))
        .await
        .unwrap();
    original.release_name(alias.clone()).await.unwrap();
    replacement.request_name(alias).await.unwrap();
    writer.abort().await.unwrap();
}
