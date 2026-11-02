//! Serialized lifecycle for one process-wide module-backed service.
//!
//! The host supplies the service key and its open, close, and operation
//! callbacks. TinyBus owns only the keyed slot and its lifecycle ordering; it
//! does not depend on a host shutdown registry or service policy.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};

type CloseFuture<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>;
type StoredCloser<S, E> = Box<dyn FnOnce(S) -> CloseFuture<E> + Send + 'static>;

/// A host-compatible asynchronous shutdown hook accepted by [`ModuleServiceSlot::on_shutdown`].
pub type ShutdownHook =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static>;

struct Entry<K, S, E> {
    key: K,
    service: S,
    close: StoredCloser<S, E>,
}

/// Own one keyed module service and serialize operations against replacement
/// and close.
///
/// A matching key reuses the installed service. A changed key closes and
/// removes the old service before attempting to open its replacement. An
/// explicit close removes the entry before awaiting the host callback, so a
/// failed or cancelled callback cannot leave the slot logically occupied.
pub struct ModuleServiceSlot<K, S, E = crate::Error> {
    entry: tokio::sync::Mutex<Option<Entry<K, S, E>>>,
    shutdown_registered: AtomicBool,
}

impl<K, S, E> ModuleServiceSlot<K, S, E> {
    /// Create an empty service slot.
    pub fn new() -> Self {
        Self {
            entry: tokio::sync::Mutex::new(None),
            shutdown_registered: AtomicBool::new(false),
        }
    }

    /// Open or reuse the service for `key`, then run one operation while the
    /// slot lock remains held.
    ///
    /// The installed entry keeps the close callback supplied when it was
    /// opened. If the key changes, that stored callback is invoked and its
    /// error is discarded; opening the replacement determines the result.
    /// The operation receives mutable access so hosts can keep service-scoped
    /// caches alongside the module handle.
    pub async fn with<R, Open, OpenFuture, Close, CloseFutureType, Operation>(
        &self,
        key: K,
        open: Open,
        close: Close,
        operation: Operation,
    ) -> Result<R, E>
    where
        K: PartialEq + Send,
        S: Send,
        Open: FnOnce() -> OpenFuture + Send,
        OpenFuture: Future<Output = Result<S, E>> + Send,
        Close: FnOnce(S) -> CloseFutureType + Send + 'static,
        CloseFutureType: Future<Output = Result<(), E>> + Send + 'static,
        Operation: for<'service> FnOnce(
                &'service mut S,
            )
                -> Pin<Box<dyn Future<Output = Result<R, E>> + Send + 'service>>
            + Send,
    {
        let mut entry = self.entry.lock().await;
        if entry.as_ref().is_some_and(|current| current.key != key) {
            if let Some(old) = entry.take() {
                let _ = (old.close)(old.service).await;
            }
        }

        if entry.is_none() {
            let service = open().await?;
            let stored_close: StoredCloser<S, E> = Box::new(move |service| {
                let future: CloseFuture<E> = Box::pin(close(service));
                future
            });
            *entry = Some(Entry {
                key,
                service,
                close: stored_close,
            });
        }

        let current = entry
            .as_mut()
            .expect("a successful open installs the requested service");
        operation(&mut current.service).await
    }

    /// Close the installed service, if any.
    ///
    /// The entry is removed before awaiting the callback. Its error is returned
    /// to the caller, but does not restore the entry. Closing an empty slot is
    /// a successful no-op.
    pub async fn close(&self) -> Result<(), E> {
        let mut entry = self.entry.lock().await;
        let Some(old) = entry.take() else {
            return Ok(());
        };
        (old.close)(old.service).await
    }

    /// Run a host-owned explicit close against the installed service while
    /// serializing it with operations, replacement, and other closes.
    ///
    /// An empty slot is a successful no-op and does not call `close`. If the
    /// callback returns an error or this future is cancelled, the entry and
    /// its stored closer remain installed. Cancellation does not establish
    /// whether a remote module acknowledged the close, so the host owns the
    /// decision about whether another attempt is safe. Once the callback
    /// returns success, the entry is retired without invoking its stored
    /// closer; the callback owns the close operation and its acknowledgement.
    ///
    /// Unlike [`Self::close`], this method borrows the service so the host can
    /// perform policy-specific cleanup before the slot drops it. The callback
    /// future may borrow the service for its duration.
    pub async fn try_close_with<Close>(&self, close: Close) -> Result<(), E>
    where
        K: Send,
        S: Send,
        Close: for<'service> FnOnce(
                &'service mut S,
            )
                -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'service>>
            + Send,
    {
        let mut entry = self.entry.lock().await;
        let Some(current) = entry.as_mut() else {
            return Ok(());
        };

        close(&mut current.service).await?;
        let _retired = entry.take();
        Ok(())
    }

    /// Register one host-provided shutdown callback for this slot.
    ///
    /// The host supplies its own registrar, such as its process shutdown hook
    /// registry. The registered callback closes the slot and ignores a close
    /// error because the shutdown hook has no error return. Registration may be
    /// repeated after the callback runs, including when cleanup returns an
    /// error.
    pub fn on_shutdown(&'static self, register: impl FnOnce(ShutdownHook))
    where
        K: Send + 'static,
        S: Send + 'static,
        E: 'static,
    {
        if self
            .shutdown_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        register(Box::new(move || {
            Box::pin(async move {
                let _reset = ShutdownRegistrationReset(&self.shutdown_registered);
                let _ = self.close().await;
            })
        }));
    }
}

impl<K, S, E> Default for ModuleServiceSlot<K, S, E> {
    fn default() -> Self {
        Self::new()
    }
}

struct ShutdownRegistrationReset<'a>(&'a AtomicBool);

impl Drop for ShutdownRegistrationReset<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
#[path = "service_slot_tests.rs"]
mod tests;
