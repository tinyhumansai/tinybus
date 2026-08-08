//! End-to-end coverage for what `#[tinybus::interface]` generates.
//!
//! The expansion is the part of tinybus most likely to break silently: a
//! mistake in argument decoding or member naming produces code that compiles
//! and then fails at the first call, in a *different* process, as an opaque
//! error. So these tests exercise it the way a real integration does — over a
//! broker, through a proxy — rather than by asserting on tokens.

#![cfg(all(test, feature = "macros"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::broker::Broker;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::name::{InterfaceName, MemberName, ObjectPath};
use crate::service::Interface;
use crate::transport::memory::MemoryBus;

const NAME: &str = "ai.tinyhumans.openhuman.Voice";
const PATH: &str = "/ai/tinyhumans/openhuman/Voice";

/// A stand-in for the integration this whole project exists to extract: the
/// speech stack, which in-kernel drags in `whisper-rs`, `cpal` and a model
/// runtime, and out-of-kernel is this.
struct Voice {
    calls: AtomicU32,
}

#[tinybus::interface(name = "ai.tinyhumans.openhuman.Voice")]
impl Voice {
    /// A one-argument method. `transcribe` becomes `Transcribe` on the wire.
    async fn transcribe(&self, path: String) -> Result<String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(format!("transcript of {path}"))
    }

    /// Several arguments, deserialized positionally.
    async fn transcribe_range(&self, path: String, start: u32, end: u32) -> Result<String> {
        Ok(format!("{path}[{start}..{end}]"))
    }

    /// No arguments: the caller sends `[]`, which is not the same JSON as the
    /// `null` that a unit tuple would decode from.
    async fn languages(&self) -> Result<Vec<String>> {
        Ok(vec!["en".to_string(), "sv".to_string()])
    }

    /// A failure, to prove it becomes an error reply with its name intact.
    async fn fail(&self) -> Result<()> {
        Err(Error::MethodFailed {
            name: "ai.tinyhumans.openhuman.Voice.Error.NoDevice".into(),
            message: "no capture device".into(),
        })
    }

    /// An explicit member name, for matching a contract that already exists.
    #[tinybus(name = "GetModelID")]
    async fn model_id(&self) -> Result<String> {
        Ok("whisper-large-v3".to_string())
    }

    /// Not on the bus at all. Never called from Rust either — the point is
    /// that it is absent from the generated dispatch, which the test below
    /// asserts by name.
    #[allow(dead_code)]
    #[tinybus(skip)]
    async fn internal_warmup(&self) -> Result<()> {
        Ok(())
    }

    /// An associated function: no receiver, so nothing to dispatch against.
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            calls: AtomicU32::new(0),
        }
    }
}

async fn bus() -> (MemoryBus, Connection, Connection, Arc<Voice>) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let voice = Arc::new(Voice {
        calls: AtomicU32::new(0),
    });
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service
        .serve_at(ObjectPath::new(PATH).unwrap(), voice.clone())
        .await
        .unwrap();
    service.request_name(NAME).await.unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (bus, service, client, voice)
}

#[tokio::test]
async fn a_snake_case_method_is_callable_as_its_pascal_case_member() {
    let (_bus, _service, client, voice) = bus().await;
    let proxy = client.proxy(NAME, PATH, NAME).unwrap();
    let transcript: String = proxy.call("Transcribe", ("/tmp/clip.wav",)).await.unwrap();
    assert_eq!(transcript, "transcript of /tmp/clip.wav");
    assert_eq!(voice.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn arguments_are_decoded_positionally() {
    let (_bus, _service, client, _) = bus().await;
    let proxy = client.proxy(NAME, PATH, NAME).unwrap();
    let out: String = proxy
        .call("TranscribeRange", ("/tmp/clip.wav", 10, 20))
        .await
        .unwrap();
    assert_eq!(out, "/tmp/clip.wav[10..20]");
}

#[tokio::test]
async fn a_zero_argument_method_accepts_an_empty_array() {
    let (_bus, _service, client, _) = bus().await;
    let proxy = client.proxy(NAME, PATH, NAME).unwrap();
    let languages: Vec<String> = proxy.call("Languages", ()).await.unwrap();
    assert_eq!(languages, vec!["en", "sv"]);
}

#[tokio::test]
async fn wrong_argument_types_fail_at_the_service_without_naming_the_arguments() {
    let (_bus, _service, client, _) = bus().await;
    let proxy = client
        .proxy(NAME, PATH, NAME)
        .unwrap()
        .with_timeout(Duration::from_secs(5));
    let err = proxy.call::<String>("Transcribe", (42,)).await.unwrap_err();
    assert!(err.to_string().contains("bad arguments"), "{err}");
    // The value must not appear: bodies routinely carry credentials, and this
    // string ends up in the caller's logs.
    assert!(!err.to_string().contains("42"), "{err}");
}

#[tokio::test]
async fn a_failing_method_keeps_its_own_error_name_across_the_bus() {
    let (_bus, _service, client, _) = bus().await;
    let proxy = client
        .proxy(NAME, PATH, NAME)
        .unwrap()
        .with_timeout(Duration::from_secs(5));
    let err = proxy.call::<()>("Fail", ()).await.unwrap_err();
    assert_eq!(
        err.wire_name(),
        "ai.tinyhumans.openhuman.Voice.Error.NoDevice"
    );
}

#[tokio::test]
async fn an_explicit_member_name_overrides_the_derived_one() {
    let (_bus, _service, client, _) = bus().await;
    let proxy = client.proxy(NAME, PATH, NAME).unwrap();
    let id: String = proxy.call("GetModelID", ()).await.unwrap();
    assert_eq!(id, "whisper-large-v3");
}

#[tokio::test]
async fn skipped_and_receiverless_methods_are_not_on_the_bus() {
    let (_bus, _service, client, voice) = bus().await;
    let members: Vec<String> = voice.members().iter().map(|m| m.to_string()).collect();
    assert!(!members.iter().any(|m| m == "InternalWarmup"));
    assert!(!members.iter().any(|m| m == "New"));

    let proxy = client
        .proxy(NAME, PATH, NAME)
        .unwrap()
        .with_timeout(Duration::from_secs(5));
    let err = proxy.call::<()>("InternalWarmup", ()).await.unwrap_err();
    assert_eq!(err.wire_name(), Error::UNKNOWN_METHOD);
}

#[test]
fn the_generated_interface_reports_its_name_and_members() {
    let voice = Voice {
        calls: AtomicU32::new(0),
    };
    assert_eq!(voice.name(), InterfaceName::new(NAME).unwrap());
    let members = voice.members();
    for expected in ["Transcribe", "TranscribeRange", "Languages", "GetModelID"] {
        assert!(
            members.contains(&MemberName::new(expected).unwrap()),
            "missing {expected} in {members:?}"
        );
    }
}
