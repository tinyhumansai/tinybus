//! Typed pub/sub over the bus: publish a domain event, subscribe with a filter.
//!
//! This is OpenHuman's `core::event_bus` broadcast surface, ported onto tinybus
//! and made generic over the event type. The API is deliberately the same shape
//! — [`EventHandler`] with a `domains()` filter, an RAII
//! [`SubscriptionHandle`], a synchronous fire-and-forget `publish` — because
//! several hundred call sites depend on that shape, and because the shape was
//! right. What changed underneath is the transport: events used to reach only
//! subscribers inside one process, and now reach every peer on the bus.
//!
//! # Why generic over `E`
//!
//! The event catalog is the *host's* vocabulary. A bus crate that knew what
//! `AgentTurnCompleted` meant would invert the dependency it exists to remove —
//! and would have to be rebuilt every time a domain gains an event. So the host
//! defines its enum, implements [`Event`], and keeps the catalog where the
//! domains that emit it live.
//!
//! # How an event maps onto the wire
//!
//! | Event concept | Bus concept |
//! | --- | --- |
//! | the catalog | one interface, e.g. `ai.tinyhumans.openhuman.Events` |
//! | `event.domain()` | the last element of the object path |
//! | the event itself | the signal body |
//!
//! Putting the domain in the *path* rather than in the body is what lets the
//! broker do the filtering: a peer that only cares about `cron` subscribes to
//! `…/events/cron` and is never woken for anything else. Filtering in the
//! subscriber — which is what the in-process bus had to do — means every
//! process pays to deserialize and discard every event any domain emits.
//!
//! Subscribers still filter client-side as well, because every subscriber on
//! one connection shares that connection's signal stream and has to demultiplex
//! it. The two filters are not redundant: the broker-side one decides whether
//! this *process* wakes up, the client-side one decides which handler runs.

mod subscriber;

use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::name::{InterfaceName, MemberName, ObjectPath};
use crate::router::MatchRule;

pub use crate::events::subscriber::{EventHandler, SubscriptionHandle};

/// The member every published event travels under.
///
/// One member for the whole catalog, rather than one per variant: a match rule
/// selects by *domain*, which is the path, and a peer that wanted variant-level
/// filtering would be describing a domain that is too coarse.
pub const PUBLISHED: &str = "Published";

/// An event that can travel on the bus.
///
/// `domain()` is the routing key. It must be a legal object-path element —
/// ASCII alphanumerics and `_` — because it becomes one; [`EventBus::publish`]
/// reports a clear error rather than silently dropping an event whose domain
/// cannot be addressed.
pub trait Event: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {
    /// Which domain this event belongs to: `agent`, `cron`, `voice`, ….
    fn domain(&self) -> &str;
}

/// Where a catalog lives on the bus.
#[derive(Debug, Clone)]
pub struct EventBusConfig {
    /// The object path every event hangs beneath. `event.domain()` is appended.
    pub root: ObjectPath,
    /// The interface the catalog is published under.
    pub interface: InterfaceName,
}

impl EventBusConfig {
    /// Build a config from string literals, validating both.
    pub fn new(root: &str, interface: &str) -> Result<Self> {
        Ok(Self {
            root: ObjectPath::new(root)?,
            interface: InterfaceName::new(interface)?,
        })
    }
}

/// A typed pub/sub handle over one connection.
///
/// Cheap to clone; clones share the connection.
#[derive(Clone)]
pub struct EventBus<E> {
    connection: Connection,
    config: EventBusConfig,
    member: MemberName,
    _event: PhantomData<fn() -> E>,
}

impl<E: Event> EventBus<E> {
    /// Attach a catalog to `connection` and start receiving for it.
    ///
    /// The match rule is added **once, here**, rather than per subscriber. That
    /// is what keeps [`EventBus::subscribe`] synchronous: adding a rule is a
    /// call to the broker, and a synchronous subscriber cannot await one. The
    /// rule covers the whole catalog, so a subscriber added later is already
    /// receiving by the time it exists — there is no window in which an event
    /// is emitted, matches nothing, and is lost.
    pub async fn attach(connection: Connection, config: EventBusConfig) -> Result<Self> {
        connection
            .add_match(
                MatchRule::new()
                    .signals()
                    .interface(config.interface.clone())
                    .path_namespace(config.root.clone()),
            )
            .await?;
        Ok(Self::without_match(connection, config))
    }

    /// Build a handle without registering a match rule.
    ///
    /// For a peer that only ever publishes. Skipping the rule means the broker
    /// never forwards this catalog's events to it, which is the difference
    /// between a write-only publisher costing nothing and costing a wakeup per
    /// event on the bus.
    pub fn without_match(connection: Connection, config: EventBusConfig) -> Self {
        Self {
            connection,
            config,
            member: MemberName::new(PUBLISHED).expect("the PUBLISHED constant is a valid member"),
            _event: PhantomData,
        }
    }

    /// The object path a given domain's events are published at.
    pub fn path_for(&self, domain: &str) -> Result<ObjectPath> {
        let root = self.config.root.as_str();
        let joined = if root == "/" {
            format!("/{domain}")
        } else {
            format!("{root}/{domain}")
        };
        ObjectPath::new(joined).map_err(|e| {
            // A domain that is not a legal path element is a programming error
            // in the host's catalog, and it must not be reported as "the event
            // vanished". Name it.
            Error::invalid_domain(domain, e)
        })
    }

    /// Publish an event. Fire-and-forget, synchronous, never blocks.
    ///
    /// Synchronous because the callers are: a domain announcing "a message
    /// arrived" from a plain `fn` should not have to be `async`, and making it
    /// so would push `async` up through hundreds of call sites that want
    /// nothing else from it.
    ///
    /// Returns `Err` for a malformed domain or a full outbox. Most callers use
    /// [`EventBus::publish`], which logs and drops — see its note on why that
    /// is the right default for a notification.
    pub fn try_publish(&self, event: E) -> Result<()> {
        let message = self.signal_for(&event)?;
        // Local first, then the wire. Local delivery cannot fail and does not
        // depend on the broker being reachable, so a subscriber in the
        // publisher's own process keeps working even when the bus is wedged —
        // which is exactly the behaviour the in-process bus had.
        self.connection.deliver_local(message.clone());
        self.connection.try_send(message)
    }

    /// Build the signal one event travels as.
    fn signal_for(&self, event: &E) -> Result<Message> {
        let path = self.path_for(event.domain())?;
        Ok(Message::signal(
            path,
            self.config.interface.clone(),
            self.member.clone(),
            serde_json::to_value((event,))?,
        ))
    }

    /// Publish an event, logging and dropping on failure.
    ///
    /// This is the direct replacement for a fire-and-forget `publish_global`,
    /// and it swallows errors on purpose. A notification that cannot be
    /// delivered must not become an error the emitting domain has to handle:
    /// the alternative is every publish site growing a `let _ =` or an
    /// `.expect()`, which is the same drop with more noise. Failures are logged
    /// at `warn`, so a bus that is genuinely wedged is visible rather than
    /// silent.
    pub fn publish(&self, event: E) {
        if let Err(e) = self.try_publish(event) {
            // Local subscribers have already been fed by this point — the
            // failure is the wire leg — so the wording says what was actually
            // lost rather than implying the event went nowhere.
            tracing::warn!(error = %e, "[tinybus] event not delivered beyond this process");
        }
    }

    /// Publish, waiting if the outbox is full.
    ///
    /// For the rare emitter that would rather be slowed down than lose an
    /// event — an audit trail, say.
    pub async fn publish_awaited(&self, event: E) -> Result<()> {
        let message = self.signal_for(&event)?;
        self.connection.deliver_local(message.clone());
        self.connection.send(message).await
    }

    /// Subscribe a handler. The returned handle cancels it when dropped.
    ///
    /// Synchronous, for the same reason `publish` is: subscribers are
    /// registered during startup, from code that is not always async.
    pub fn subscribe(&self, handler: Arc<dyn EventHandler<E>>) -> SubscriptionHandle {
        subscriber::spawn(self.connection.signals(), self.config.clone(), handler)
    }

    /// Subscribe with a closure instead of an [`EventHandler`] implementation.
    ///
    /// Receives every event in the catalog — there is no domain filter, because
    /// a closure small enough to be worth this shortcut is small enough to
    /// `match` on what it cares about.
    pub fn on<F, Fut>(&self, name: &str, handler: F) -> SubscriptionHandle
    where
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.subscribe(Arc::new(subscriber::FnSubscriber {
            name: name.to_string(),
            handler,
            _event: PhantomData,
        }))
    }

    /// A raw receiver of decoded events, for a consumer that wants to drive its
    /// own loop rather than implement [`EventHandler`].
    pub fn receiver(&self) -> EventReceiver<E> {
        EventReceiver {
            signals: self.connection.signals(),
            config: self.config.clone(),
            _event: PhantomData,
        }
    }

    /// The connection underneath.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Where this catalog lives.
    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }
}

/// A receiver that decodes bus signals back into events.
///
/// The replacement for the old bus's `raw_receiver()`. It yields only events
/// belonging to its catalog; anything else on the connection is skipped rather
/// than surfaced as an error, because a connection carrying another interface's
/// signals is normal, not a fault.
pub struct EventReceiver<E> {
    signals: tokio::sync::broadcast::Receiver<Message>,
    config: EventBusConfig,
    _event: PhantomData<fn() -> E>,
}

/// Why a non-blocking [`EventReceiver::try_recv`] produced nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing is waiting right now.
    Empty,
    /// The receiver fell behind and `n` messages were dropped.
    ///
    /// Surfaced rather than hidden: a consumer draining for "did anything
    /// change" must treat a lag as "yes, probably" and re-scan, because the
    /// event it needed may have been one of the dropped ones.
    Lagged(u64),
    /// The connection is gone; nothing further will arrive.
    Closed,
}

impl<E: Event> EventReceiver<E> {
    /// Take the next event without waiting.
    ///
    /// For a consumer that drains on its own schedule — an agent turn checking
    /// "did the installed tools change since last time" — rather than parking
    /// a task on the stream. Skips messages belonging to other catalogs, so
    /// `Empty` means "nothing *for you*", not "nothing at all".
    pub fn try_recv(&mut self) -> std::result::Result<E, TryRecvError> {
        use tokio::sync::broadcast::error::TryRecvError as Raw;
        loop {
            match self.signals.try_recv() {
                Ok(message) => {
                    if let Some(event) = decode(&self.config, &message) {
                        return Ok(event);
                    }
                }
                Err(Raw::Empty) => return Err(TryRecvError::Empty),
                Err(Raw::Lagged(n)) => return Err(TryRecvError::Lagged(n)),
                Err(Raw::Closed) => return Err(TryRecvError::Closed),
            }
        }
    }
}

impl<E: Event> EventReceiver<E> {
    /// Wait for the next event in this catalog.
    ///
    /// Returns `None` once the connection is gone. A lagged receiver logs and
    /// keeps going rather than terminating: dropping a consumer because it fell
    /// behind turns a transient burst into a permanently dead subscriber.
    pub async fn recv(&mut self) -> Option<E> {
        loop {
            match self.signals.recv().await {
                Ok(message) => {
                    if let Some(event) = decode(&self.config, &message) {
                        return Some(event);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "[tinybus] event receiver lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

/// Decode a signal into an event, or `None` if it is not one of ours.
pub(crate) fn decode<E: Event>(config: &EventBusConfig, message: &Message) -> Option<E> {
    let header = &message.header;
    if header.interface.as_ref() != Some(&config.interface) {
        return None;
    }
    if header.member.as_ref().map(|m| m.as_str()) != Some(PUBLISHED) {
        return None;
    }
    let path = header.path.as_ref()?;
    if !path.starts_with(&config.root) {
        return None;
    }
    // The body is the positional array every signal carries, so the event is
    // its first element.
    let value = message.body.get(0)?;
    match serde_json::from_value::<E>(value.clone()) {
        Ok(event) => Some(event),
        Err(e) => {
            // A peer publishing a catalog we cannot parse is a version skew
            // between two independently built processes — exactly the case the
            // interface-renaming rule exists to make loud. Log it and skip; one
            // unparseable event must not kill a subscriber.
            tracing::warn!(error = %e, "[tinybus] could not decode an event; skipping");
            None
        }
    }
}

#[cfg(test)]
mod tests;
