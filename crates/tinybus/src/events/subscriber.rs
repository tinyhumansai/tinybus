//! The subscriber side: the [`EventHandler`] trait and its RAII handle.
//!
//! Ported from OpenHuman's `core::event_bus::subscriber`, with the handler
//! generic over the event type and the dispatch loop reading bus signals rather
//! than a `tokio::sync::broadcast` of Rust values. Three behaviours are carried
//! over deliberately, because each one exists to stop a specific failure:
//!
//! - **Domain filtering before dispatch**, so a handler that asked for `cron`
//!   is not woken by every agent turn.
//! - **Panic isolation**, so one handler panicking does not kill the loop and
//!   silently unsubscribe every *other* handler on that connection.
//! - **Lag is survivable**, so a subscriber that falls behind during a burst
//!   logs and continues rather than terminating for good.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::events::{Event, EventBusConfig};
use crate::message::Message;

/// A typed event handler. Implement this to react to events.
#[async_trait]
pub trait EventHandler<E: Event>: Send + Sync + 'static {
    /// Human-readable name, for logging and diagnostics.
    fn name(&self) -> &str;

    /// Optional domain filter. `None` receives everything in the catalog;
    /// `Some(&["agent", "cron"])` receives only those domains.
    fn domains(&self) -> Option<&[&str]> {
        None
    }

    /// Handle one event. Must not block the runtime.
    async fn handle(&self, event: &E);
}

/// A running subscriber. Dropping it aborts the subscriber's task.
///
/// RAII rather than an explicit unsubscribe because the common bug it prevents
/// is a subscriber outliving the thing it updates: a handler holding an `Arc`
/// to state its owner has torn down keeps reacting to events forever.
pub struct SubscriptionHandle {
    task: JoinHandle<()>,
    name: String,
}

impl SubscriptionHandle {
    pub(crate) fn new(name: String, task: JoinHandle<()>) -> Self {
        Self { task, name }
    }

    /// The subscriber's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Cancel the subscriber explicitly.
    pub fn cancel(self) {
        tracing::debug!(subscriber = self.name, "[tinybus] cancelling subscriber");
        self.task.abort();
    }

    /// Leak the subscriber, so it runs for the life of the process.
    ///
    /// For subscribers registered at startup that genuinely should never stop.
    /// Named `forget` rather than being the default because an accidentally
    /// dropped handle is a subscriber that silently stops working, and that
    /// should be a visible choice.
    pub fn forget(self) {
        std::mem::forget(self);
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if !self.task.is_finished() {
            tracing::debug!(
                subscriber = self.name,
                "[tinybus] subscriber dropped, aborting task"
            );
            self.task.abort();
        }
    }
}

/// A closure-based handler, for a subscriber too small to justify a type.
///
/// Carried over from the bus this replaces, where it existed for exactly the
/// same reason: a test or a one-line bridge should not have to declare a struct
/// and an `impl` to react to an event.
pub(crate) struct FnSubscriber<E, F> {
    pub(crate) name: String,
    pub(crate) handler: F,
    pub(crate) _event: std::marker::PhantomData<fn() -> E>,
}

#[async_trait]
impl<E, F, Fut> EventHandler<E> for FnSubscriber<E, F>
where
    E: Event,
    F: Fn(E) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn handle(&self, event: &E) {
        // The event is cloned rather than borrowed so the closure's future can
        // be `'static` — which is what lets callers write an `async move` block
        // instead of fighting a borrow that outlives the call.
        (self.handler)(event.clone()).await;
    }
}

/// Spawn the dispatch loop for one handler.
pub(crate) fn spawn<E: Event>(
    mut signals: broadcast::Receiver<Message>,
    config: EventBusConfig,
    handler: Arc<dyn EventHandler<E>>,
) -> SubscriptionHandle {
    let name = handler.name().to_string();
    // Snapshot the filter as owned strings: `domains()` borrows from the
    // handler, and the loop outlives the borrow.
    let domains: Option<Vec<String>> = handler
        .domains()
        .map(|d| d.iter().map(|s| s.to_string()).collect());

    tracing::debug!(
        subscriber = name,
        domains = ?domains,
        "[tinybus] registering subscriber"
    );

    let task_name = name.clone();
    let task = tokio::spawn(async move {
        loop {
            let message = match signals.recv().await {
                Ok(message) => message,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        handler = task_name,
                        skipped = n,
                        "[tinybus] subscriber lagged, skipped events"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!(
                        handler = task_name,
                        "[tinybus] connection closed, subscriber exiting"
                    );
                    break;
                }
            };

            let Some(event) = crate::events::decode::<E>(&config, &message) else {
                continue;
            };

            if let Some(allowed) = &domains
                && !allowed.iter().any(|d| d == event.domain())
            {
                continue;
            }

            // A panicking handler must not take this loop with it: losing the
            // loop silently unsubscribes the handler, so it stops reacting and
            // nothing says so.
            //
            // The isolation is a spawned task rather than `catch_unwind`,
            // because tokio already turns a panicking task into an `Err` on its
            // `JoinHandle` — and doing it this way costs no dependency, in a
            // crate whose entire purpose is dependency reduction. The event is
            // cloned into the task, which is why [`Event`] requires `Clone`.
            let dispatch = {
                let handler = handler.clone();
                let event = event.clone();
                tokio::spawn(async move { handler.handle(&event).await })
            };
            if let Err(join) = dispatch.await {
                tracing::error!(
                    handler = task_name,
                    domain = event.domain(),
                    panicked = join.is_panic(),
                    "[tinybus] handler failed, continuing"
                );
            }
        }
    });

    SubscriptionHandle::new(name, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct TestEvent;

    impl Event for TestEvent {
        fn domain(&self) -> &str {
            "test"
        }
    }

    struct Handler;

    #[async_trait]
    impl EventHandler<TestEvent> for Handler {
        fn name(&self) -> &str {
            "subscriber::test"
        }

        async fn handle(&self, _: &TestEvent) {}
    }

    #[test]
    fn a_handler_without_a_filter_accepts_every_domain() {
        assert!(Handler.domains().is_none());
    }

    #[tokio::test]
    async fn handlers_and_closures_can_be_called_through_the_trait() {
        Handler.handle(&TestEvent).await;
        let closure = FnSubscriber {
            name: "closure".to_string(),
            handler: |_| async {},
            _event: std::marker::PhantomData::<fn() -> TestEvent>,
        };
        assert_eq!(closure.name(), "closure");
        closure.handle(&TestEvent).await;
    }

    #[tokio::test]
    async fn a_handle_exposes_its_name_and_cancels_its_task() {
        let task = tokio::spawn(std::future::pending::<()>());
        let handle = SubscriptionHandle::new("test::pending".to_string(), task);
        assert_eq!(handle.name(), "test::pending");
        handle.cancel();
    }

    #[tokio::test]
    async fn forgetting_a_completed_handle_leaves_its_task_alone() {
        let task = tokio::spawn(async {});
        tokio::task::yield_now().await;
        SubscriptionHandle::new("test::permanent".to_string(), task).forget();
    }

    #[tokio::test]
    async fn a_closed_signal_stream_ends_the_dispatch_loop() {
        let (sender, receiver) = broadcast::channel(1);
        drop(sender);
        let config = EventBusConfig::new("/events", "ai.tinyhumans.Events").unwrap();
        let handle = spawn(receiver, config, Arc::new(Handler));
        assert_eq!(handle.name(), "subscriber::test");
        tokio::task::yield_now().await;
        drop(handle);
    }

    #[tokio::test]
    async fn malformed_and_lagged_messages_do_not_end_the_dispatch_loop() {
        let (sender, receiver) = broadcast::channel(1);
        let config = EventBusConfig::new("/events", "ai.tinyhumans.Events").unwrap();
        let message = Message::signal(
            crate::ObjectPath::new("/events/test").unwrap(),
            crate::InterfaceName::new("ai.tinyhumans.Other").unwrap(),
            crate::MemberName::new("Published").unwrap(),
            serde_json::json!([TestEvent]),
        );
        sender.send(message.clone()).unwrap();
        sender.send(message).unwrap();
        let handle = spawn(receiver, config, Arc::new(Handler));
        tokio::task::yield_now().await;
        assert_eq!(handle.name(), "subscriber::test");
        drop(sender);
        drop(handle);
    }
}
