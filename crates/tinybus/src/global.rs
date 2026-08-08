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
//! ```ignore
//! static BUS: tinybus::global::OnceBus<DomainEvent> = tinybus::global::OnceBus::new();
//! ```
//!
//! …and gets the whole surface off it. `OnceBus::new` is `const`, so this costs
//! nothing until something initialises it.
//!
//! # Runtime ownership
//!
//! [`OnceBus::init_in_process`] and [`OnceBus::init_over`] put the bus's tasks
//! — the broker, and this peer's reader/writer loops — on a **dedicated
//! runtime that the singleton owns**, rather than on whichever runtime happened
//! to call `init` first.
//!
//! That is not a detail. A process-wide bus outlives any one runtime: under
//! `cargo test` every `#[tokio::test]` builds and tears down its own, so the
//! first test to win the `OnceLock` would otherwise leave every later test
//! holding a bus attached to a dead reactor — publishes going nowhere,
//! subscribers never waking, and no error anywhere to say why. Owning the
//! runtime makes the bus's lifetime match the `static`'s, which is what
//! everything reaching for a global bus already assumes.
//!
//! [`OnceBus::init_with`] is the exception: the caller supplied that connection
//! and owns its tasks, so it is left where it was built.
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
use crate::error::{Error, Result};
use crate::events::{Event, EventBus, EventBusConfig, EventHandler, SubscriptionHandle};
use crate::native::NativeRegistry;
use crate::ports::Transport;
use crate::transport::memory::MemoryBus;
use crate::version::PeerManifest;

/// A lazily-initialised, process-wide [`EventBus`].
pub struct OnceBus<E: Event> {
    bus: OnceLock<EventBus<E>>,
    native: OnceLock<NativeRegistry>,
    /// Owns the bus's tasks so they outlive the caller's runtime. Never
    /// dropped — this lives in a `static`, and dropping a runtime from inside
    /// an async context panics.
    runtime: OnceLock<tokio::runtime::Runtime>,
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
            runtime: OnceLock::new(),
        }
    }

    /// The runtime this bus's tasks live on, built on first use.
    ///
    /// One worker thread: the broker routes messages and the connection loops
    /// shuffle frames, none of which is CPU-bound. A second thread would buy
    /// nothing and cost a context switch per hop.
    fn runtime(&self) -> Result<&tokio::runtime::Runtime> {
        if let Some(existing) = self.runtime.get() {
            return Ok(existing);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("tinybus")
            .enable_all()
            .build()
            .map_err(|e| Error::transport(format!("could not build the bus runtime: {e}")))?;
        Ok(self.runtime.get_or_init(|| runtime))
    }

    /// Initialise with an in-process broker: no sockets, no external services.
    ///
    /// The default for a host that has not extracted anything yet. It is a
    /// real broker over a real transport, so the wiring, the serialisation and
    /// the match rules are all exercised exactly as they will be in
    /// production — moving an integration out of the process later is then a
    /// deployment change rather than a different code path that has never run.
    pub async fn init_in_process(&self, config: EventBusConfig) -> Result<&EventBus<E>> {
        if let Some(existing) = self.bus.get() {
            return Ok(existing);
        }
        let transport = MemoryBus::new();
        let peer = transport.connect().await?;

        // Everything that spawns happens under the guard; nothing is awaited
        // under it, because an `EnterGuard` is `!Send` and holding one across
        // an await would make this future `!Send` for every caller.
        let connection = {
            let _guard = self.runtime()?.enter();
            Broker::new().spawn(transport);
            Connection::attach(peer.into())
        };
        connection.handshake().await?;

        self.init_with(connection, config).await
    }

    /// Initialise over an existing transport — a Unix socket to a shared broker.
    pub async fn init_over(
        &self,
        transport: Box<dyn Transport>,
        config: EventBusConfig,
    ) -> Result<&EventBus<E>> {
        if let Some(existing) = self.bus.get() {
            return Ok(existing);
        }
        // Same runtime ownership as `init_in_process`: only the broker differs,
        // and it is somebody else's process here.
        let connection = {
            let _guard = self.runtime()?.enter();
            Connection::attach(transport.into())
        };
        connection.handshake().await?;
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
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Tick(u32);

    impl Event for Tick {
        fn domain(&self) -> &str {
            "test"
        }
    }

    fn config() -> EventBusConfig {
        EventBusConfig::new("/ai/tinyhumans/test/events", "ai.tinyhumans.test.Events").unwrap()
    }

    struct Capture(Arc<Mutex<Vec<Tick>>>);

    #[async_trait]
    impl EventHandler<Tick> for Capture {
        fn name(&self) -> &str {
            "test::capture"
        }
        async fn handle(&self, event: &Tick) {
            self.0.lock().await.push(event.clone());
        }
    }

    #[tokio::test]
    async fn publishing_before_init_is_a_no_op_rather_than_a_panic() {
        // Early-startup publishes and bus-less unit tests both depend on this.
        let bus: OnceBus<Tick> = OnceBus::new();
        assert!(!bus.is_initialised());
        bus.publish(Tick(1));
        assert!(
            bus.subscribe(Arc::new(Capture(Arc::new(Mutex::new(Vec::new())))))
                .is_none()
        );
    }

    #[tokio::test]
    async fn an_in_process_bus_delivers_end_to_end() {
        let bus: OnceBus<Tick> = OnceBus::new();
        bus.init_in_process(config()).await.unwrap();
        assert!(bus.is_initialised());

        let seen = Arc::new(Mutex::new(Vec::new()));
        let _handle = bus.subscribe(Arc::new(Capture(seen.clone()))).unwrap();
        bus.publish(Tick(7));

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while seen.lock().await.is_empty() {
            assert!(tokio::time::Instant::now() < deadline, "no delivery");
            tokio::task::yield_now().await;
        }
        assert_eq!(seen.lock().await[0], Tick(7));
    }

    #[tokio::test]
    async fn a_second_init_returns_the_first_bus() {
        // Two subsystems both initialising at startup is normal; the second
        // silently replacing the first would strand every existing subscriber.
        let bus: OnceBus<Tick> = OnceBus::new();
        let first = bus.init_in_process(config()).await.unwrap() as *const _;
        let second = bus.init_in_process(config()).await.unwrap() as *const _;
        assert_eq!(first, second);
    }

    #[test]
    fn the_native_registry_works_without_a_runtime_or_an_initialised_bus() {
        // Startup registers handlers from sync contexts, before anything async
        // exists. This is a `#[test]`, so it has no runtime at all.
        let bus: OnceBus<Tick> = OnceBus::new();
        bus.native()
            .register::<u32, u32, _, _>("test.double", |n| async move { Ok(n * 2) });
        assert!(bus.native().is_registered("test.double"));
        assert!(!bus.is_initialised());
    }

    /// Collect events off a bus until `n` arrive, or fail on a deadline.
    async fn drain(bus: &OnceBus<Tick>, n: usize) -> Vec<Tick> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let _handle = bus
            .subscribe(Arc::new(Capture(seen.clone())))
            .expect("the bus is initialised");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            {
                let guard = seen.lock().await;
                if guard.len() >= n {
                    return guard.clone();
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the bus delivered nothing"
            );
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn a_bus_outlives_the_runtime_that_initialised_it() {
        // The exact shape of the bug this owns a runtime to prevent: under
        // `cargo test` the first `#[tokio::test]` to touch a global bus
        // initialises it and then tears its own runtime down. If the bus's
        // tasks lived on that runtime, every later test would inherit a bus
        // attached to a dead reactor — publishing into silence, with nothing
        // anywhere reporting an error.
        static BUS: OnceBus<Tick> = OnceBus::new();

        let first = tokio::runtime::Runtime::new().unwrap();
        first.block_on(async {
            BUS.init_in_process(config()).await.unwrap();
            // Delivery works while the initialising runtime is still alive.
            BUS.publish(Tick(1));
            assert_eq!(drain(&BUS, 1).await, vec![Tick(1)]);
        });
        drop(first);

        // A later, unrelated runtime — a second test, in the real case.
        let second = tokio::runtime::Runtime::new().unwrap();
        second.block_on(async {
            BUS.publish(Tick(2));
            assert_eq!(drain(&BUS, 1).await, vec![Tick(2)]);
        });
    }

    #[test]
    fn the_bus_runtime_is_built_once_and_shared() {
        static BUS: OnceBus<Tick> = OnceBus::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            BUS.init_in_process(config()).await.unwrap();
            BUS.init_in_process(config()).await.unwrap();
        });
        // One runtime, however many times `init` is called.
        assert!(BUS.runtime.get().is_some());
    }

    #[test]
    fn a_once_bus_can_be_a_static() {
        // `new()` being `const` is what lets the host declare the singleton.
        static BUS: OnceBus<Tick> = OnceBus::new();
        assert!(!BUS.is_initialised());
    }
}
