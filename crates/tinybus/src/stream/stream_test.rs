//! Streams end to end, through a real broker.
//!
//! Through the broker rather than over a bare transport pair because the
//! ownership check on every chunk reads the `sender` the *broker* stamps —
//! testing it on a direct pair would test a code path where every peer looks
//! identical, which is exactly the case the check exists to rule out.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::broker::Broker;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::stream::{
    MAX_CHUNK_LEN, STREAM_INTERFACE, STREAM_PATH, StreamDescriptor, StreamLimits, StreamRef,
};
use crate::transport::memory::MemoryBus;

const SINK: &str = "ai.tinyhumans.Sink";
const SINK_PATH: &str = "/ai/tinyhumans/Sink";

/// A service that accepts a payload as a stream and reports what it received.
struct Sink {
    connection: std::sync::Mutex<Option<Connection>>,
}

impl Sink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connection: std::sync::Mutex::new(None),
        })
    }

    fn connection(&self) -> Connection {
        self.connection
            .lock()
            .unwrap()
            .clone()
            .expect("the sink is wired to its connection before it is called")
    }
}

#[async_trait]
impl crate::service::Interface for Arc<Sink> {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(SINK).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![
            MemberName::new("Absorb").unwrap(),
            MemberName::new("Digest").unwrap(),
            MemberName::new("Ignore").unwrap(),
        ]
    }

    async fn call(&self, member: &MemberName, args: Value) -> Result<Value> {
        let (stream,): (StreamRef,) = serde_json::from_value(args)?;
        let connection = self.connection();
        match member.as_str() {
            // Buffer it and report the length and a checksum, so a transposed
            // or dropped chunk cannot pass as a correct transfer.
            "Absorb" => {
                let bytes = connection.read_stream(&stream).await?;
                Ok(serde_json::json!([bytes.len(), checksum(&bytes)]))
            }
            // Read chunk by chunk, holding one at a time.
            "Digest" => {
                let mut reader = connection.accept_stream(&stream)?;
                let (mut len, mut sum) = (0usize, 0u64);
                while let Some(chunk) = reader.next_chunk().await? {
                    len += chunk.len();
                    sum = sum.wrapping_add(checksum(&chunk));
                }
                Ok(serde_json::json!([len, sum]))
            }
            // Never touch the stream at all.
            _ => Ok(serde_json::json!([0, 0])),
        }
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(1469598103934665603u64, |hash, byte| {
        (hash ^ *byte as u64).wrapping_mul(1099511628211)
    })
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A broker, a sink service that owns [`SINK`], and a client.
async fn bus() -> (Connection, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let sink = Sink::new();
    *sink.connection.lock().unwrap() = Some(service.clone());
    service
        .serve_at(ObjectPath::new(SINK_PATH).unwrap(), sink)
        .await
        .unwrap();
    service.request_name(SINK).await.unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (client, service)
}

async fn absorb(client: &Connection, member: &str, bytes: &[u8]) -> Result<(usize, u64)> {
    client
        .call_with_stream(
            BusName::new(SINK).unwrap(),
            ObjectPath::new(SINK_PATH).unwrap(),
            InterfaceName::new(SINK).unwrap(),
            MemberName::new(member).unwrap(),
            |stream| serde_json::json!([stream]),
            bytes,
        )
        .await
}

#[tokio::test]
async fn a_payload_larger_than_one_frame_arrives_whole_and_in_order() {
    let (client, _service) = bus().await;
    // Comfortably past `MAX_FRAME_LEN`, so this payload could not have crossed
    // as a single body no matter how it was encoded.
    let bytes = payload(20 * 1024 * 1024);
    let (len, sum) = absorb(&client, "Absorb", &bytes).await.unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(sum, checksum(&bytes));
}

#[tokio::test]
async fn a_payload_read_chunk_by_chunk_never_needs_the_whole_thing_in_memory() {
    let (client, _service) = bus().await;
    let bytes = payload(MAX_CHUNK_LEN * 3 + 17);
    let expected = bytes
        .chunks(MAX_CHUNK_LEN)
        .fold(0u64, |sum, chunk| sum.wrapping_add(checksum(chunk)));
    let (len, sum) = absorb(&client, "Digest", &bytes).await.unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(sum, expected);
}

#[tokio::test]
async fn an_empty_payload_is_a_complete_stream_rather_than_an_error() {
    let (client, _service) = bus().await;
    let (len, sum) = absorb(&client, "Absorb", b"").await.unwrap();
    assert_eq!(len, 0);
    assert_eq!(sum, checksum(b""));
}

#[tokio::test]
async fn a_payload_that_fits_the_window_can_finish_before_the_reader_attaches() {
    let (client, _service) = bus().await;
    // Written and closed with no reader in sight: the receiving method is only
    // dispatched afterwards, and must still find the stream.
    let mut writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::with_len(64))
        .await
        .unwrap();
    let stream = writer.stream_ref();
    writer.write(&payload(64)).await.unwrap();
    writer.finish().await.unwrap();

    let (len, _): (usize, u64) = client
        .call(
            BusName::new(SINK).unwrap(),
            ObjectPath::new(SINK_PATH).unwrap(),
            InterfaceName::new(SINK).unwrap(),
            MemberName::new("Absorb").unwrap(),
            serde_json::json!([stream]),
        )
        .await
        .unwrap();
    assert_eq!(len, 64);
}

#[tokio::test]
async fn a_peer_cannot_write_into_a_stream_another_peer_opened() {
    let (client, service) = bus().await;
    let bus_again = client.clone();
    let mut writer = bus_again
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::with_len(1024),
        )
        .await
        .unwrap();
    let stream = writer.stream_ref();
    writer.write(&payload(16)).await.unwrap();

    // The service is a third peer on the same bus with its own unique name. It
    // knows the id — it is in this test's scope — and must still be refused,
    // because the broker stamped a different sender on its call.
    let forged = service
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Write",
            serde_json::json!([stream.id, 1, super::base64::encode(b"intruder")]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(
        forged.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownStream",
        "{forged}"
    );

    // And the legitimate owner's stream is untouched by the attempt.
    writer.write(&payload(16)).await.unwrap();
    assert_eq!(writer.sent(), 32);
}

#[tokio::test]
async fn a_chunk_out_of_order_aborts_the_stream_rather_than_transposing_it() {
    let (client, _service) = bus().await;
    let writer = client
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::with_len(1024),
        )
        .await
        .unwrap();
    let id = writer.stream_ref().id;

    let skipped = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Write",
            serde_json::json!([id, 4, super::base64::encode(b"late")]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(skipped.wire_name(), "ai.tinyhumans.tinybus.Error.Protocol");

    // Aborted, not merely rejected: the stream is gone.
    let after = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Write",
            serde_json::json!([id, 0, super::base64::encode(b"first")]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(
        after.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownStream"
    );
}

#[tokio::test]
async fn a_stream_longer_than_it_declared_is_cut_off_at_the_declaration() {
    let (client, _service) = bus().await;
    let mut writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::with_len(8))
        .await
        .unwrap();
    let error = writer.write(&payload(9)).await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamTooLarge",
        "{error}"
    );
}

#[tokio::test]
async fn a_declared_length_over_the_receivers_cap_is_refused_before_a_byte_moves() {
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_stream_len: 1024,
        ..StreamLimits::default()
    });
    let error = client
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::with_len(4096),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamTooLarge"
    );
}

#[tokio::test]
async fn an_undeclared_stream_is_still_stopped_at_the_receivers_cap() {
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_stream_len: 1024,
        ..StreamLimits::default()
    });
    let mut writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::default())
        .await
        .unwrap();
    writer.write_chunk(&payload(1024)).await.unwrap();
    let error = writer.write_chunk(b"one too many").await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamTooLarge"
    );
}

#[tokio::test]
async fn a_peer_holding_open_more_streams_than_its_share_is_refused_a_new_one() {
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_streams_per_peer: 2,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();
    let _first = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let _second = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let error = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.TooManyStreams"
    );
}

#[tokio::test]
async fn closing_short_of_the_declared_length_is_an_error_not_a_short_read() {
    let (client, _service) = bus().await;
    let mut writer = client
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::with_len(100),
        )
        .await
        .unwrap();
    writer.write(&payload(10)).await.unwrap();
    let id = writer.stream_ref().id;
    // Claim a length the receiver did not get. A truncated payload accepted as
    // a whole one is the failure this check exists to prevent.
    let error = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Close",
            serde_json::json!([id, 100]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), "ai.tinyhumans.tinybus.Error.Protocol");
}

#[tokio::test]
async fn a_reader_sees_an_error_rather_than_an_eof_when_the_sender_aborts() {
    let (client, service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let writer = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let stream = writer.stream_ref();
    let mut reader = service.accept_stream(&stream).unwrap();
    writer.abort().await.unwrap();

    let error = reader.next_chunk().await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted",
        "{error}"
    );
}

#[tokio::test]
async fn a_dropped_writer_aborts_the_stream_instead_of_leaving_it_open() {
    let (client, service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let stream = {
        let writer = client
            .open_stream(&destination, StreamDescriptor::default())
            .await
            .unwrap();
        writer.stream_ref()
    };
    let mut reader = service.accept_stream(&stream).unwrap();
    // The abort is spawned by `Drop`, so awaiting the reader is what waits for
    // it — a deadline, not a sleep.
    let error = tokio::time::timeout(Duration::from_secs(5), reader.next_chunk())
        .await
        .expect("the dropped writer should abort promptly")
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted"
    );
}

#[tokio::test]
async fn writing_to_a_stream_nobody_will_read_fails_instead_of_hanging_forever() {
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        window_chunks: 1,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();
    let mut writer = client
        .open_stream_with_timeout(
            &destination,
            StreamDescriptor::default(),
            Duration::from_millis(250),
        )
        .await
        .unwrap();

    // The window takes one chunk; the second has nowhere to go, and the write's
    // own deadline is what turns that into an error rather than a hang.
    writer.write_chunk(b"first").await.unwrap();
    let error = writer.write_chunk(b"second").await.unwrap_err();
    assert_eq!(error.wire_name(), "ai.tinyhumans.tinybus.Error.Timeout");
}

#[tokio::test]
async fn a_stream_the_receiver_never_reads_does_not_stall_the_rest_of_the_bus() {
    let (client, _service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let mut writer = client
        .open_stream_with_timeout(
            &destination,
            StreamDescriptor::default(),
            Duration::from_millis(250),
        )
        .await
        .unwrap();
    // `Ignore` never touches the stream, so the window fills and stays full.
    let _ = client
        .call::<(usize, u64)>(
            destination.clone(),
            ObjectPath::new(SINK_PATH).unwrap(),
            InterfaceName::new(SINK).unwrap(),
            MemberName::new("Ignore").unwrap(),
            serde_json::json!([writer.stream_ref()]),
        )
        .await
        .unwrap();
    for _ in 0..StreamLimits::default().window_chunks {
        let _ = writer.write_chunk(b"filler").await;
    }

    // An ordinary call on the same connection still goes through: the stalled
    // stream is the sender's problem and nobody else's.
    let names = tokio::time::timeout(Duration::from_secs(5), client.list_names())
        .await
        .expect("the wedged stream must not stall an unrelated call")
        .unwrap();
    assert!(names.iter().any(|name| name.as_str() == SINK));
}

#[tokio::test]
async fn a_stream_can_only_be_read_once() {
    let (client, service) = bus().await;
    let writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::default())
        .await
        .unwrap();
    let stream = writer.stream_ref();
    let _reader = service.accept_stream(&stream).unwrap();
    let error = service.accept_stream(&stream).unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted"
    );
}

#[tokio::test]
async fn a_handle_that_names_no_stream_is_an_error_rather_than_an_empty_payload() {
    let (_client, service) = bus().await;
    let error = service
        .accept_stream(&StreamRef {
            id: "s404".to_string(),
            content_type: None,
            len: None,
        })
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownStream"
    );
}

#[tokio::test]
async fn an_unknown_member_on_the_stream_interface_is_an_unknown_method() {
    let (client, _service) = bus().await;
    let error = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Rewind",
            serde_json::json!([]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::UNKNOWN_METHOD);
}

#[tokio::test]
async fn a_service_gets_the_stream_interface_without_exporting_anything() {
    // A connection that has exported no objects at all still answers `Open`:
    // bulk transfer is plumbing, not something each service opts into.
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let bare = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    bare.request_name("ai.tinyhumans.Bare").await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let mut writer = client
        .open_stream(
            &BusName::new("ai.tinyhumans.Bare").unwrap(),
            StreamDescriptor::default().content_type("application/pdf"),
        )
        .await
        .unwrap();
    let stream = writer.stream_ref();
    assert_eq!(stream.content_type.as_deref(), Some("application/pdf"));

    let mut reader = bare.accept_stream(&stream).unwrap();
    writer.write(b"%PDF-1.7").await.unwrap();
    writer.finish().await.unwrap();
    assert_eq!(reader.next_chunk().await.unwrap().unwrap(), b"%PDF-1.7");
    assert_eq!(reader.next_chunk().await.unwrap(), None);
}

#[tokio::test]
async fn a_chunk_over_the_cap_is_refused_by_the_sender_before_it_reaches_the_wire() {
    let (client, _service) = bus().await;
    let mut writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::default())
        .await
        .unwrap();
    let error = writer
        .write_chunk(&payload(MAX_CHUNK_LEN + 1))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), "ai.tinyhumans.tinybus.Error.Protocol");
}

#[tokio::test]
async fn a_chunk_over_the_cap_is_refused_by_the_receiver_too() {
    // The sender-side check is a courtesy; the receiver's is the one that
    // counts, because a peer is free not to run our sender.
    let (client, _service) = bus().await;
    let writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::default())
        .await
        .unwrap();
    let id = writer.stream_ref().id;
    let oversize = super::base64::encode(&payload(MAX_CHUNK_LEN + 1));
    let error = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Write",
            serde_json::json!([id, 0, oversize]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), "ai.tinyhumans.tinybus.Error.Protocol");
}

#[tokio::test]
async fn the_stream_interface_cannot_be_shadowed_by_a_service_exporting_it() {
    // A service that exports its own interface at the stream address must not
    // be able to intercept chunks: the connection answers streams before the
    // object tree is consulted.
    struct Impostor;

    #[async_trait]
    impl crate::service::Interface for Impostor {
        fn name(&self) -> InterfaceName {
            InterfaceName::new(STREAM_INTERFACE).unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Open").unwrap()]
        }

        async fn call(&self, _member: &MemberName, _args: Value) -> Result<Value> {
            Ok(Value::String("hijacked".to_string()))
        }
    }

    let (client, service) = bus().await;
    service
        .serve_at(ObjectPath::new(STREAM_PATH).unwrap(), Impostor)
        .await
        .unwrap();
    let writer = client
        .open_stream(&BusName::new(SINK).unwrap(), StreamDescriptor::default())
        .await
        .unwrap();
    assert_ne!(writer.stream_ref().id, "hijacked");
}

#[tokio::test]
async fn a_malformed_open_is_a_bad_arguments_error_that_does_not_quote_the_body() {
    let (client, _service) = bus().await;
    let error = client
        .call_stream_member(
            &BusName::new(SINK).unwrap(),
            "Open",
            serde_json::json!(["ceremonial-secret"]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.BadArguments"
    );
    assert!(!error.to_string().contains("ceremonial-secret"), "{error}");
}

#[tokio::test]
async fn a_stream_call_with_no_member_is_rejected_rather_than_dispatched() {
    let (client, _service) = bus().await;
    let mut message = Message::method_call(
        BusName::new(SINK).unwrap(),
        ObjectPath::new(STREAM_PATH).unwrap(),
        InterfaceName::new(STREAM_INTERFACE).unwrap(),
        MemberName::new("Open").unwrap(),
        serde_json::json!([StreamDescriptor::default()]),
    );
    message.header.member = None;
    let error = client
        .call_raw(message, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), "ai.tinyhumans.tinybus.Error.Protocol");
}

#[tokio::test]
async fn a_stream_that_has_gone_idle_is_reaped_and_stops_holding_its_window() {
    let (client, service) = bus().await;
    // A zero idle timeout makes every existing stream idle by the time the next
    // `Open` sweeps — the reaper's condition, expressed without waiting for a
    // clock.
    service.set_stream_limits(StreamLimits {
        idle_timeout: Duration::ZERO,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();
    let abandoned = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let stream = abandoned.stream_ref();
    let mut reader = service.accept_stream(&stream).unwrap();

    // The sweep runs on the next `Open`, so that is what collects the first.
    let _next = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();

    let error = reader.next_chunk().await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted",
        "{error}"
    );
    // And the sender is told, rather than writing into a stream that is gone.
    let mut abandoned = abandoned;
    assert!(abandoned.write_chunk(b"too late").await.is_err());
}

#[tokio::test]
async fn a_closed_stream_nobody_collects_is_evicted_oldest_first() {
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_streams_per_peer: 2,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();

    // Three payloads written and closed, none ever read. A closed stream still
    // holds its window, so the receiver must not accumulate them without bound.
    let mut handles = Vec::new();
    for _ in 0..3 {
        let mut writer = client
            .open_stream(&destination, StreamDescriptor::with_len(4))
            .await
            .unwrap();
        handles.push(writer.stream_ref());
        writer.write(b"data").await.unwrap();
        writer.finish().await.unwrap();
    }

    // The oldest is gone; the newest — the one a call is most likely still
    // waiting on — survives.
    let error = service.accept_stream(&handles[0]).unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.UnknownStream",
        "{error}"
    );
    let mut kept = service.accept_stream(&handles[2]).unwrap();
    assert_eq!(kept.next_chunk().await.unwrap().unwrap(), b"data");
}

#[tokio::test]
async fn a_closed_stream_still_counts_for_nothing_against_the_live_limit() {
    // Closing frees the slot: a peer that finishes its transfers can keep
    // opening new ones, which is the whole difference between the live cap and
    // the uncollected cap.
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_streams_per_peer: 1,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();
    for _ in 0..3 {
        let mut writer = client
            .open_stream(&destination, StreamDescriptor::with_len(2))
            .await
            .expect("a finished transfer must not hold its slot");
        let stream = writer.stream_ref();
        writer.write(b"hi").await.unwrap();
        writer.finish().await.unwrap();
        service.accept_stream(&stream).unwrap();
    }
}

#[tokio::test]
async fn writing_after_close_is_refused_rather_than_appended() {
    let (client, _service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let mut writer = client
        .open_stream(&destination, StreamDescriptor::with_len(4))
        .await
        .unwrap();
    let id = writer.stream_ref().id;
    writer.write(b"data").await.unwrap();
    writer.finish().await.unwrap();

    // A payload the receiver has already been told is complete must not grow.
    let error = client
        .call_stream_member(
            &destination,
            "Write",
            serde_json::json!([id, 1, super::base64::encode(b"more")]),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted",
        "{error}"
    );
}

#[tokio::test]
async fn a_sender_is_told_when_the_receiver_drops_the_reader_mid_transfer() {
    let (client, service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let mut writer = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let stream = writer.stream_ref();
    let reader = service.accept_stream(&stream).unwrap();
    writer.write_chunk(b"first").await.unwrap();

    // Nobody is going to look at the rest, so pushing it is wasted work on both
    // sides — the sender learns immediately instead of at its deadline.
    drop(reader);
    let error = writer.write_chunk(b"second").await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted",
        "{error}"
    );
}

#[tokio::test]
async fn a_reader_reports_what_the_sender_declared_about_the_payload() {
    let (client, service) = bus().await;
    let writer = client
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::with_len(9).content_type("audio/wav"),
        )
        .await
        .unwrap();
    let reader = service.accept_stream(&writer.stream_ref()).unwrap();
    assert_eq!(reader.content_type(), Some("audio/wav"));
    assert_eq!(reader.declared_len(), Some(9));
}

#[tokio::test]
async fn reading_past_the_callers_own_limit_is_an_error_not_a_silent_truncation() {
    let (client, service) = bus().await;
    let destination = BusName::new(SINK).unwrap();
    let mut writer = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    let mut reader = service.accept_stream(&writer.stream_ref()).unwrap();
    writer.write_chunk(&payload(64)).await.unwrap();

    let error = reader.read_to_end_capped(16).await.unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamTooLarge",
        "{error}"
    );
}

#[tokio::test]
async fn a_reader_whose_connection_went_away_reports_it_rather_than_a_clean_eof() {
    // No outcome is ever recorded when the receiving side simply disappears, and
    // an unfinished payload must not be mistaken for a finished one. A bare
    // receiver, because the shared fixture's service holds its own connection —
    // a legitimate thing for a service to do, and a cycle that keeps the
    // receiving side alive past the point this test needs it gone.
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let receiver = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    receiver
        .request_name("ai.tinyhumans.Vanishing")
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let writer = client
        .open_stream(
            &BusName::new("ai.tinyhumans.Vanishing").unwrap(),
            StreamDescriptor::default(),
        )
        .await
        .unwrap();
    let mut reader = receiver.accept_stream(&writer.stream_ref()).unwrap();
    // Leaked rather than dropped: a dropped writer aborts, which would record an
    // outcome and test the wrong path.
    std::mem::forget(writer);
    drop(receiver);

    let error = tokio::time::timeout(Duration::from_secs(5), reader.next_chunk())
        .await
        .expect("a vanished connection must not leave the reader parked")
        .unwrap_err();
    assert_eq!(
        error.wire_name(),
        "ai.tinyhumans.tinybus.Error.StreamAborted",
        "{error}"
    );
}

#[tokio::test]
async fn neither_end_of_a_stream_prints_the_payload_when_debugged() {
    // Both halves get printed in error paths, and what flows through them is
    // the caller's data.
    let (client, service) = bus().await;
    let mut writer = client
        .open_stream(
            &BusName::new(SINK).unwrap(),
            StreamDescriptor::default().content_type("text/plain"),
        )
        .await
        .unwrap();
    let reader = service.accept_stream(&writer.stream_ref()).unwrap();
    writer.write_chunk(b"recovery-phrase").await.unwrap();

    let printed = format!("{writer:?} {reader:?}");
    assert!(!printed.contains("recovery-phrase"), "{printed}");
    assert!(printed.contains("StreamWriter"), "{printed}");
    assert!(printed.contains("StreamReader"), "{printed}");
}

#[tokio::test]
async fn one_peers_open_streams_do_not_consume_another_peers_slots() {
    // The cap is per peer for the same reason every other queue on the bus is:
    // a busy peer must not be able to starve a quiet one.
    let (client, service) = bus().await;
    service.set_stream_limits(StreamLimits {
        max_streams_per_peer: 1,
        ..StreamLimits::default()
    });
    let destination = BusName::new(SINK).unwrap();

    let _hog = client
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .unwrap();
    // A second stream from the same peer is refused…
    assert!(
        client
            .open_stream(&destination, StreamDescriptor::default())
            .await
            .is_err()
    );

    // …while a different peer is unaffected by the first one's spending.
    let other = service
        .open_stream(&destination, StreamDescriptor::default())
        .await
        .expect("another peer's slots are its own");
    assert!(!other.stream_ref().id.is_empty());
}
