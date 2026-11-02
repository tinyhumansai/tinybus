//! Process-wide singletons: one bus, one native registry, initialised once.
//!
//! A host application has exactly one bus, and every domain in it needs to
//! reach that bus from code that was not handed a reference — a `publish` deep
//! inside a scheduler, a handler registered from a `Once::call_once`. That is
//! what this provides, and it is why OpenHuman's bus was a singleton before any
//! of this existed.
//!
//! # Why the host declares the static, not tinybus
//!
//! The bus is generic over the host's event type, so tinybus cannot own a
//! `static` of it — there is no single type to name. Instead the host writes:
//!
//! ```
//! use serde::{Deserialize, Serialize};
//! use tinybus::events::Event;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct DomainEvent;
//!
//! impl Event for DomainEvent {
//!     fn domain(&self) -> &str {
//!         "example"
//!     }
//! }
//!
//! static BUS: tinybus::global::OnceBus<DomainEvent> = tinybus::global::OnceBus::new();
//! assert!(!BUS.is_initialised());
//! ```
//!
//! …and gets the whole surface off it. `OnceBus::new` is `const`, so this costs
//! nothing until something initialises it.
//!
//! # Before initialisation
//!
//! Every accessor is safe to call before `init`. Publishing goes nowhere and
//! logs at `trace`; subscribing returns `None`. This is deliberate and carried
//! over from the bus being replaced: a domain that publishes during early
//! startup, or inside a unit test that never stood a bus up, must not panic.
//! The cost is that a genuinely missing `init` is quiet — which is what
//! [`OnceBus::is_initialised`] and the startup log line are for.

use std::sync::Arc;
use std::sync::OnceLock;

use crate::broker::Broker;
use crate::connection::Connection;
use crate::error::Result;
use crate::events::{Event, EventBus, EventBusConfig, EventHandler, SubscriptionHandle};
use crate::native::NativeRegistry;
use crate::ports::Transport;
use crate::transport::memory::MemoryBus;
use crate::version::PeerManifest;

/// A lazily-initialised, process-wide [`EventBus`].
pub struct OnceBus<E: Event> {
    bus: OnceLock<EventBus<E>>,
    native: OnceLock<NativeRegistry>,
}

impl<E: Event> Default for OnceBus<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Event> OnceBus<E> {
    /// An uninitialised singleton. `const`, so it can be a `static`.
    pub const fn new() -> Self {
        Self {
            bus: OnceLock::new(),
            native: OnceLock::new(),
        }
    }

    /// Initialise with an in-process broker: no sockets, no external services.
    ///
    /// The default for a host that has not extracted anything yet. It is a
    /// real broker over a real transport, so the wiring, the serialisation and
    /// the match rules are all exercised exactly as they will be in
    /// production — moving an integration out of the process later is then a
    /// deployment change rather than a different code path that has never run.
    pub async fn init_in_process(&self, config: EventBusConfig) -> Result<&EventBus<E>> {
        let transport = MemoryBus::new();
        Broker::new().spawn(transport.clone());
        let connection = Connection::connect(transport.connect().await?).await?;
        self.init_with(connection, config).await
    }

    /// Initialise over an existing transport — a Unix socket to a shared broker.
    pub async fn init_over(
        &self,
        transport: Box<dyn Transport>,
        config: EventBusConfig,
    ) -> Result<&EventBus<E>> {
        let connection = Connection::connect(transport).await?;
        self.init_with(connection, config).await
    }

    /// Initialise on a connection the caller already has.
    ///
    /// Repeat calls return the existing bus and do **not** replace it, matching
    /// `OnceLock` semantics: two subsystems both calling `init` at startup is
    /// normal, and the second one silently winning would be a race nobody could
    /// debug.
    pub async fn init_with(
        &self,
        connection: Connection,
        config: EventBusConfig,
    ) -> Result<&EventBus<E>> {
        if let Some(existing) = self.bus.get() {
            return Ok(existing);
        }
        let bus = EventBus::attach(connection, config).await?;
        // A lost race here means another thread initialised first; its bus is
        // the one everyone gets, and ours is dropped.
        Ok(self.bus.get_or_init(|| bus))
    }

    /// The bus, if initialised.
    pub fn get(&self) -> Option<&EventBus<E>> {
        self.bus.get()
    }

    /// Whether [`OnceBus::init_in_process`] or a sibling has run.
    pub fn is_initialised(&self) -> bool {
        self.bus.get().is_some()
    }

    /// The native registry. Available before the bus is initialised, because
    /// handler registration happens during startup from sync contexts that run
    /// before any runtime exists.
    pub fn native(&self) -> &NativeRegistry {
        self.native.get_or_init(NativeRegistry::new)
    }

    /// Publish an event. A no-op before initialisation.
    pub fn publish(&self, event: E) {
        match self.bus.get() {
            Some(bus) => bus.publish(event),
            None => tracing::trace!("[tinybus] bus not initialised; dropping event"),
        }
    }

    /// Subscribe a handler. `None` before initialisation.
    pub fn subscribe(&self, handler: Arc<dyn EventHandler<E>>) -> Option<SubscriptionHandle> {
        self.bus.get().map(|bus| bus.subscribe(handler))
    }

    /// Announce this process's manifest to the broker.
    pub async fn announce(&self, manifest: &PeerManifest) -> Result<()> {
        match self.bus.get() {
            Some(bus) => bus.connection().announce(manifest).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "global_tests.rs"]
mod tests;
