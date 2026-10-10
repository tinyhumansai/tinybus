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
use std::task::Poll;

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

            if domains
                .as_ref()
                .is_some_and(|allowed| !allowed.iter().any(|d| d == event.domain()))
            {
                continue;
            }

            // Catch each poll inline. This preserves panic isolation without a
            // task allocation and scheduling hop per event; `poll_fn` keeps the
            // future pinned, while `catch_unwind` covers panics from any poll.
            let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.handle(&event)
            })) {
                Ok(future) => catch_handler_panic(future).await,
                Err(payload) => Err(payload),
            };
            if outcome.is_err() {
                tracing::error!(
                    handler = task_name,
                    domain = event.domain(),
                    panicked = true,
                    "[tinybus] handler failed, continuing"
                );
            }
        }
    });

    SubscriptionHandle::new(name, task)
}

async fn catch_handler_panic<F: std::future::Future>(future: F) -> std::thread::Result<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|context| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().poll(context)
        })) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

#[cfg(test)]
#[path = "subscriber_tests.rs"]
mod tests;
