//! The in-process transport: a pair of bounded channels, no file descriptors.
//!
//! This is not a test double bolted on beside the real thing — it is a first
//! class transport, and it earns its place twice over.
//!
//! 1. **The test suite.** A broker, several services and a client run inside
//!    one `#[tokio::test]`, so a test can assert on exact message ordering
//!    without a socket, a temp dir, or a sleep.
//! 2. **The slim kernel build.** An OpenHuman build that compiles an
//!    integration in-process (because it is cheap, or because the platform has
//!    no sockets) uses the *same* bus, the same interfaces and the same proxy
//!    code as the out-of-process one. Moving an integration across that line is
//!    a deployment decision, not a rewrite.
//!
//! Channels are **bounded**. An unbounded channel would turn a slow service
//! into unbounded kernel memory growth — the failure mode we are trying to
//! delete, not relocate. When a peer's queue fills, the sender waits, which is
//! backpressure a caller can observe and time out on.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use crate::error::{Error, Result};
use crate::message::Message;
use crate::ports::{Listener, Transport};

/// How many messages may sit in a peer's queue before senders wait.
///
/// Sized for burst absorption, not for buffering: a queue this deep already
/// means the consumer is not keeping up, and the right answer then is
/// backpressure rather than a longer queue.
pub const CHANNEL_CAPACITY: usize = 256;

/// One end of an in-process link.
///
/// The outbound sender lives in an `Option` behind a *std* mutex so that
/// [`Transport::close`] can drop it: dropping the sender is what makes the far
/// end's `recv` return `None`, and without that a closed in-process link would
/// look, to the peer, exactly like an idle one. The lock is a std mutex rather
/// than a tokio one precisely so that it is never held across the send await.
pub struct MemoryTransport {
    outbound: std::sync::Mutex<Option<mpsc::Sender<Message>>>,
    inbound: Mutex<mpsc::Receiver<Message>>,
    label: String,
}

impl MemoryTransport {
    /// Build a connected pair. Whatever one end sends, the other receives.
    pub fn pair() -> (Self, Self) {
        let (a_tx, a_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (b_tx, b_rx) = mpsc::channel(CHANNEL_CAPACITY);
        (Self::new(a_tx, b_rx), Self::new(b_tx, a_rx))
    }

    fn new(outbound: mpsc::Sender<Message>, inbound: mpsc::Receiver<Message>) -> Self {
        Self {
            outbound: std::sync::Mutex::new(Some(outbound)),
            inbound: Mutex::new(inbound),
            label: "memory".to_string(),
        }
    }

    /// Take a clone of the sender without holding the lock across an await.
    fn sender(&self) -> Result<mpsc::Sender<Message>> {
        self.outbound
            .lock()
            .expect("the outbound lock is never held across a panic point")
            .clone()
            .ok_or_else(|| Error::transport("this end of the in-process link is closed"))
    }
}

#[async_trait]
impl Transport for MemoryTransport {
    async fn send(&self, message: Message) -> Result<()> {
        self.sender()?
            .send(message)
            .await
            .map_err(|_| Error::transport("the peer end of the in-process link was dropped"))
    }

    async fn recv(&self) -> Result<Option<Message>> {
        // Single-reader contract (see the port docs) makes holding this lock
        // across the await safe: there is never a second task to block.
        let mut inbound = self.inbound.lock().await;
        Ok(inbound.recv().await)
    }

    async fn close(&self) -> Result<()> {
        // Dropping the sender is the hangup the far end sees as `Ok(None)`.
        // Idempotent: `take` on an already-empty slot is not an error, because
        // a shutdown race closing twice is normal.
        self.outbound
            .lock()
            .expect("the outbound lock is never held across a panic point")
            .take();
        Ok(())
    }

    fn describe(&self) -> String {
        self.label.clone()
    }
}

/// An in-process bus: a listener plus the connect side that feeds it.
///
/// Clone it and hand copies to as many would-be peers as you like; every
/// [`MemoryBus::connect`] produces a transport whose other end lands in the
/// broker's accept queue.
#[derive(Clone)]
pub struct MemoryBus {
    connect_tx: mpsc::Sender<Box<dyn Transport>>,
    accept_rx: Arc<Mutex<mpsc::Receiver<Box<dyn Transport>>>>,
}

impl MemoryBus {
    /// Create an in-process bus with nothing attached to it yet.
    pub fn new() -> Self {
        let (connect_tx, accept_rx) = mpsc::channel(CHANNEL_CAPACITY);
        Self {
            connect_tx,
            accept_rx: Arc::new(Mutex::new(accept_rx)),
        }
    }

    /// Open a new peer link and hand the far end to the listener.
    pub async fn connect(&self) -> Result<Box<dyn Transport>> {
        let (peer, broker_side) = MemoryTransport::pair();
        self.connect_tx
            .send(Box::new(broker_side))
            .await
            .map_err(|_| Error::transport("the in-process broker is not accepting connections"))?;
        Ok(Box::new(peer))
    }
}

impl Default for MemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Listener for MemoryBus {
    async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
        let mut rx = self.accept_rx.lock().await;
        Ok(rx.recv().await)
    }

    fn describe(&self) -> String {
        "memory".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};

    fn message(member: &str) -> Message {
        Message::method_call(
            BusName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            ObjectPath::new("/ai/tinyhumans/openhuman/Voice").unwrap(),
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            MemberName::new(member).unwrap(),
            serde_json::Value::Null,
        )
    }

    #[tokio::test]
    async fn a_pair_delivers_in_order_in_both_directions() {
        let (a, b) = MemoryTransport::pair();
        a.send(message("First")).await.unwrap();
        a.send(message("Second")).await.unwrap();
        b.send(message("Back")).await.unwrap();

        assert_eq!(
            b.recv()
                .await
                .unwrap()
                .unwrap()
                .header
                .member
                .unwrap()
                .as_str(),
            "First"
        );
        assert_eq!(
            b.recv()
                .await
                .unwrap()
                .unwrap()
                .header
                .member
                .unwrap()
                .as_str(),
            "Second"
        );
        assert_eq!(
            a.recv()
                .await
                .unwrap()
                .unwrap()
                .header
                .member
                .unwrap()
                .as_str(),
            "Back"
        );
    }

    #[tokio::test]
    async fn a_dropped_peer_reads_as_clean_shutdown_not_as_an_error() {
        let (a, b) = MemoryTransport::pair();
        drop(b);
        assert!(a.recv().await.unwrap().is_none());
        // ...but writing to a hung-up peer is a real failure the caller must see.
        assert!(a.send(message("Late")).await.is_err());
    }

    #[tokio::test]
    async fn connecting_hands_the_far_end_to_the_listener() {
        let bus = MemoryBus::new();
        let peer = bus.connect().await.unwrap();
        let accepted = bus.accept().await.unwrap().expect("a peer");
        peer.send(message("Hello")).await.unwrap();
        assert_eq!(
            accepted
                .recv()
                .await
                .unwrap()
                .unwrap()
                .header
                .member
                .unwrap()
                .as_str(),
            "Hello"
        );
    }
}
