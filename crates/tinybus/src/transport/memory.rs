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

struct Delivery {
    message: Message,
    brokered: bool,
}

/// One end of an in-process link.
///
/// The outbound sender lives in an `Option` behind a *std* mutex so that
/// [`Transport::close`] can drop it: dropping the sender is what makes the far
/// end's `recv` return `None`, and without that a closed in-process link would
/// look, to the peer, exactly like an idle one. The lock is a std mutex rather
/// than a tokio one precisely so that it is never held across the send await.
pub struct MemoryTransport {
    outbound: std::sync::Mutex<Option<mpsc::Sender<Delivery>>>,
    inbound: Mutex<mpsc::Receiver<Delivery>>,
    label: String,
}

impl MemoryTransport {
    /// Build a connected pair. Whatever one end sends, the other receives.
    pub fn pair() -> (Self, Self) {
        let (a_tx, a_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (b_tx, b_rx) = mpsc::channel(CHANNEL_CAPACITY);
        (Self::new(a_tx, b_rx), Self::new(b_tx, a_rx))
    }

    fn new(outbound: mpsc::Sender<Delivery>, inbound: mpsc::Receiver<Delivery>) -> Self {
        Self {
            outbound: std::sync::Mutex::new(Some(outbound)),
            inbound: Mutex::new(inbound),
            label: "memory".to_string(),
        }
    }

    // Only the actual broker writer can mint this out-of-band provenance.
    pub(crate) async fn send_brokered(&self, message: Message) -> Result<()> {
        self.send_delivery(message, true).await
    }

    async fn send_delivery(&self, message: Message, brokered: bool) -> Result<()> {
        self.sender()?
            .send(Delivery { message, brokered })
            .await
            .map_err(|_| Error::transport("the peer end of the in-process link was dropped"))
    }

    pub(crate) async fn recv_with_provenance(&self) -> Result<Option<(Message, bool)>> {
        Ok(self
            .inbound
            .lock()
            .await
            .recv()
            .await
            .map(|delivery| (delivery.message, delivery.brokered)))
    }

    /// Take a clone of the sender without holding the lock across an await.
    fn sender(&self) -> Result<mpsc::Sender<Delivery>> {
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
        self.send_delivery(message, false).await
    }

    async fn recv(&self) -> Result<Option<Message>> {
        // Public receive discards provenance; wrapping/replaying a frame cannot
        // carry the private broker delivery proof into a custom transport.
        Ok(self
            .recv_with_provenance()
            .await?
            .map(|(message, _)| message))
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
#[path = "memory_tests.rs"]
mod tests;
