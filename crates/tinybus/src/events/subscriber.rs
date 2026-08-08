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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
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

            // A panicking handler must not take the loop with it: the loop is
            // shared by every subscriber's *sibling* tasks only in spirit, but
            // losing this one silently is still the worst outcome — the
            // subscriber stops reacting and nothing says so.
            let outcome = AssertUnwindSafe(handler.handle(&event)).catch_unwind().await;
            if let Err(panic) = outcome {
                let message = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("unknown panic");
                tracing::error!(
                    handler = task_name,
                    domain = event.domain(),
                    panic = message,
                    "[tinybus] handler panicked, continuing"
                );
            }
        }
    });

    SubscriptionHandle::new(name, task)
}
