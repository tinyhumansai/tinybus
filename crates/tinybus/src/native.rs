//! In-process typed request/response, with no serialization at all.
//!
//! This is OpenHuman's `core::event_bus::native_request`, ported unchanged in
//! behaviour. It lives in the bus crate for one reason: without it, a host that
//! moved to tinybus would still need its own second bus for the calls that
//! cannot be serialized, and "which bus does this go on" would be a question
//! every domain had to answer twice.
//!
//! # Why this exists next to a perfectly good wire protocol
//!
//! The rest of tinybus turns a call into JSON and puts it on a socket. That is
//! the right trade for an integration in another process, and the wrong one for
//! two modules in the same address space passing things that have no
//! serialized form at all:
//!
//! ```text
//! AgentTurnRequest {
//!     parent_tools: Arc<Vec<Box<dyn Tool>>>,   // trait objects
//!     on_progress:  Option<Sender<Progress>>,  // a live channel
//!     run_queue:    Option<Arc<RunQueue>>,     // shared mutable state
//! }
//! ```
//!
//! There is no encoding of `Arc<dyn Tool>` that survives a process boundary,
//! and inventing one would mean the receiver getting a *copy* of something
//! whose whole purpose is being shared. So this surface stays in-process,
//! permanently and by design — it is not a milestone that has not landed yet.
//!
//! The rule for choosing, stated once:
//!
//! | Need | Surface |
//! | --- | --- |
//! | notify anyone who cares | [`crate::events`] |
//! | call another *process* | [`crate::Proxy`] |
//! | call another module with a non-serializable payload | this |
//!
//! # Sync vs async
//!
//! Registration is **sync** — it is a `HashMap::insert` under a std lock, so
//! startup code in a `Once::call_once` or a plain `fn main` can register
//! without a runtime. Dispatch is **async**, and takes care to clone the
//! handler's `Arc` and drop the lock *before* awaiting, so a slow handler never
//! blocks an unrelated dispatch.
//!
//! A dynamically loaded module cannot use this registry to cross the `dlopen`
//! boundary. It links its own copy of this static and Rust does not promise
//! `TypeId` identity across separately loaded artifacts. Module-to-host and
//! module-to-module calls therefore go through the framed module transport;
//! no `Any`, `TypeId`, or native-registry pointer appears in the module ABI.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

/// Errors raised by the native request surface.
///
/// Separate from [`crate::Error`] on purpose: nothing here can cross the wire,
/// so none of it has — or should have — a dotted wire name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeRequestError {
    /// No handler is registered for the method.
    UnregisteredHandler {
        /// The method that was called.
        method: String,
    },
    /// Caller and handler disagree on the request or response type.
    ///
    /// This is the failure the `TypeId` check exists to turn into an error
    /// rather than a transmute: two modules compiled against different versions
    /// of a shared request struct would otherwise reinterpret each other's
    /// memory.
    TypeMismatch {
        /// The method that was called.
        method: String,
        /// The type the handler registered.
        expected: &'static str,
        /// The type the caller supplied.
        actual: &'static str,
    },
    /// The handler ran and returned an error.
    HandlerFailed {
        /// The method that was called.
        method: String,
        /// What the handler said.
        message: String,
    },
}

impl std::fmt::Display for NativeRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnregisteredHandler { method } => {
                write!(f, "no native handler registered for method '{method}'")
            }
            Self::TypeMismatch {
                method,
                expected,
                actual,
            } => write!(
                f,
                "native handler type mismatch for '{method}': expected {expected}, got {actual}"
            ),
            Self::HandlerFailed { method, message } => {
                write!(f, "native handler '{method}' failed: {message}")
            }
        }
    }
}

impl std::error::Error for NativeRequestError {}

type BoxedAny = Box<dyn Any + Send>;
type HandlerFuture = Pin<Box<dyn Future<Output = Result<BoxedAny, String>> + Send>>;
type BoxedHandler = Arc<dyn Fn(BoxedAny) -> HandlerFuture + Send + Sync>;

struct HandlerEntry {
    handler: BoxedHandler,
    req_type: TypeId,
    resp_type: TypeId,
    req_name: &'static str,
    resp_name: &'static str,
}

/// A registry of in-process, Rust-typed request handlers, keyed by method name.
#[derive(Clone, Default)]
pub struct NativeRegistry {
    handlers: Arc<RwLock<HashMap<String, HandlerEntry>>>,
}

impl std::fmt::Debug for NativeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A non-blocking read: a `Debug` that can deadlock is a `Debug` that
        // makes a hung process impossible to inspect.
        match self.handlers.try_read() {
            Ok(guard) => f
                .debug_struct("NativeRegistry")
                .field("methods", &guard.keys().collect::<Vec<_>>())
                .finish(),
            Err(_) => f
                .debug_struct("NativeRegistry")
                .field("methods", &"<locked>")
                .finish(),
        }
    }
}

/// Recover from lock poisoning by taking the inner guard.
///
/// The registry holds a plain `HashMap`; a panic elsewhere while holding the
/// lock cannot have left it in a state that matters. Propagating the poison
/// would turn one unrelated panic into every subsequent dispatch failing.
fn unpoison<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

impl NativeRegistry {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler for `method`, replacing any existing one.
    ///
    /// Replacement is deliberate and is what lets a test stub out a production
    /// handler by registering over it.
    pub fn register<Req, Resp, F, Fut>(&self, method: &str, handler: F)
    where
        Req: Send + 'static,
        Resp: Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, String>> + Send + 'static,
    {
        let erased: BoxedHandler = Arc::new(move |boxed: BoxedAny| {
            // Infallible: `request` compares `TypeId`s before ever calling this.
            let req = *boxed
                .downcast::<Req>()
                .expect("native: dispatch passed the wrong request type despite the TypeId check");
            let fut = handler(req);
            Box::pin(async move { fut.await.map(|resp| Box::new(resp) as BoxedAny) })
        });

        let entry = HandlerEntry {
            handler: erased,
            req_type: TypeId::of::<Req>(),
            resp_type: TypeId::of::<Resp>(),
            req_name: std::any::type_name::<Req>(),
            resp_name: std::any::type_name::<Resp>(),
        };

        let replaced = unpoison(self.handlers.write())
            .insert(method.to_string(), entry)
            .is_some();
        tracing::debug!(
            method,
            req_type = std::any::type_name::<Req>(),
            resp_type = std::any::type_name::<Resp>(),
            replaced,
            "[tinybus::native] registered handler"
        );
    }

    /// Dispatch a typed request.
    pub async fn request<Req, Resp>(
        &self,
        method: &str,
        req: Req,
    ) -> Result<Resp, NativeRequestError>
    where
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        // Clone what is needed and drop the lock before awaiting. Holding a
        // std lock across an await would both risk deadlock and serialise every
        // dispatch behind the slowest handler.
        let (handler, req_type, resp_type, req_name, resp_name) = {
            let guard = unpoison(self.handlers.read());
            let entry =
                guard
                    .get(method)
                    .ok_or_else(|| NativeRequestError::UnregisteredHandler {
                        method: method.to_string(),
                    })?;
            (
                Arc::clone(&entry.handler),
                entry.req_type,
                entry.resp_type,
                entry.req_name,
                entry.resp_name,
            )
        };

        if TypeId::of::<Req>() != req_type {
            return Err(NativeRequestError::TypeMismatch {
                method: method.to_string(),
                expected: req_name,
                actual: std::any::type_name::<Req>(),
            });
        }
        if TypeId::of::<Resp>() != resp_type {
            return Err(NativeRequestError::TypeMismatch {
                method: method.to_string(),
                expected: resp_name,
                actual: std::any::type_name::<Resp>(),
            });
        }

        match handler(Box::new(req)).await {
            Ok(boxed) => Ok(*boxed.downcast::<Resp>().expect(
                "native: handler returned the wrong response type despite the TypeId check",
            )),
            Err(message) => Err(NativeRequestError::HandlerFailed {
                method: method.to_string(),
                message,
            }),
        }
    }

    /// Whether a handler is registered for `method`.
    pub fn is_registered(&self, method: &str) -> bool {
        unpoison(self.handlers.read()).contains_key(method)
    }

    /// Every registered method name, sorted.
    pub fn methods(&self) -> Vec<String> {
        let mut methods: Vec<String> = unpoison(self.handlers.read()).keys().cloned().collect();
        methods.sort();
        methods
    }

    /// How many handlers are registered.
    pub fn len(&self) -> usize {
        unpoison(self.handlers.read()).len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        unpoison(self.handlers.read()).is_empty()
    }

    /// Drop every handler. For tests.
    pub fn clear(&self) {
        unpoison(self.handlers.write()).clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    #[derive(Debug, PartialEq)]
    struct Req(u32);
    #[derive(Debug, PartialEq)]
    struct Resp(String);

    #[tokio::test]
    async fn a_registered_handler_round_trips_a_typed_payload() {
        let registry = NativeRegistry::new();
        registry.register::<Req, Resp, _, _>("demo.double", |req| async move {
            Ok(Resp(format!("{}", req.0 * 2)))
        });

        let resp: Resp = registry.request("demo.double", Req(21)).await.unwrap();
        assert_eq!(resp, Resp("42".to_string()));
    }

    #[tokio::test]
    async fn a_non_serializable_payload_passes_through_untouched() {
        // The property that justifies this surface existing at all: a live
        // channel and a trait object arrive on the far side still usable.
        trait Tool: Send + Sync {
            fn name(&self) -> &str;
        }
        struct Hammer;
        impl Tool for Hammer {
            fn name(&self) -> &str {
                "hammer"
            }
        }

        struct Request {
            tools: Arc<Vec<Box<dyn Tool>>>,
            progress: mpsc::Sender<String>,
        }

        let registry = NativeRegistry::new();
        registry.register::<Request, (), _, _>("demo.run", |req| async move {
            let name = req.tools[0].name().to_string();
            req.progress.send(name).await.map_err(|e| e.to_string())?;
            Ok(())
        });

        let (tx, mut rx) = mpsc::channel(1);
        registry
            .request::<Request, ()>(
                "demo.run",
                Request {
                    tools: Arc::new(vec![Box::new(Hammer)]),
                    progress: tx,
                },
            )
            .await
            .unwrap();

        assert_eq!(rx.recv().await.unwrap(), "hammer");
    }

    #[tokio::test]
    async fn an_unregistered_method_names_itself() {
        let registry = NativeRegistry::new();
        let err = registry
            .request::<Req, Resp>("demo.missing", Req(1))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            NativeRequestError::UnregisteredHandler {
                method: "demo.missing".into()
            }
        );
    }

    #[tokio::test]
    async fn a_type_mismatch_is_an_error_rather_than_a_transmute() {
        let registry = NativeRegistry::new();
        registry.register::<Req, Resp, _, _>("demo.typed", |_| async { Ok(Resp("x".into())) });

        // Right request type, wrong response type.
        let err = registry
            .request::<Req, u64>("demo.typed", Req(1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, NativeRequestError::TypeMismatch { .. }),
            "{err}"
        );

        // Wrong request type.
        let err = registry
            .request::<String, Resp>("demo.typed", "nope".to_string())
            .await
            .unwrap_err();
        assert!(
            matches!(err, NativeRequestError::TypeMismatch { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_handler_error_reaches_the_caller_with_its_message() {
        let registry = NativeRegistry::new();
        registry
            .register::<Req, Resp, _, _>("demo.fail", |_| async { Err("no device".to_string()) });

        let err = registry
            .request::<Req, Resp>("demo.fail", Req(1))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            NativeRequestError::HandlerFailed {
                method: "demo.fail".into(),
                message: "no device".into()
            }
        );
    }

    #[tokio::test]
    async fn re_registering_replaces_so_a_test_can_stub_production() {
        let registry = NativeRegistry::new();
        registry.register::<Req, Resp, _, _>("demo.m", |_| async { Ok(Resp("real".into())) });
        registry.register::<Req, Resp, _, _>("demo.m", |_| async { Ok(Resp("stub".into())) });

        let resp: Resp = registry.request("demo.m", Req(0)).await.unwrap();
        assert_eq!(resp, Resp("stub".to_string()));
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn a_slow_handler_does_not_block_an_unrelated_dispatch() {
        // The reason the lock is dropped before the await. If it were held,
        // the fast call could not complete until the slow one did.
        let registry = NativeRegistry::new();
        let (release_tx, mut release_rx) = mpsc::channel::<()>(1);
        let release = Arc::new(tokio::sync::Mutex::new(release_rx.recv()));
        drop(release);

        registry.register::<Req, Resp, _, _>("demo.slow", |_| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(Resp("slow".into()))
        });
        registry.register::<Req, Resp, _, _>("demo.fast", |_| async { Ok(Resp("fast".into())) });

        let slow = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.request::<Req, Resp>("demo.slow", Req(0)).await })
        };
        // Completes immediately despite the slow dispatch being in flight.
        let fast: Resp = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            registry.request("demo.fast", Req(0)),
        )
        .await
        .expect("the fast dispatch is not blocked")
        .unwrap();
        assert_eq!(fast, Resp("fast".to_string()));
        assert_eq!(slow.await.unwrap().unwrap(), Resp("slow".to_string()));
        let _ = release_tx;
    }

    #[tokio::test]
    async fn a_poisoned_lock_does_not_disable_the_registry() {
        let registry = NativeRegistry::new();
        registry.register::<Req, Resp, _, _>("demo.m", |_| async { Ok(Resp("ok".into())) });

        // Poison the lock from another thread.
        let poisoner = registry.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.handlers.write().unwrap();
            panic!("poisoning the registry lock");
        })
        .join();

        // One unrelated panic must not take every later dispatch with it.
        let resp: Resp = registry.request("demo.m", Req(0)).await.unwrap();
        assert_eq!(resp, Resp("ok".to_string()));
    }

    #[test]
    fn methods_are_listed_sorted_for_introspection() {
        let registry = NativeRegistry::new();
        registry.register::<Req, Resp, _, _>("b.method", |_| async { Ok(Resp(String::new())) });
        registry.register::<Req, Resp, _, _>("a.method", |_| async { Ok(Resp(String::new())) });
        assert_eq!(registry.methods(), vec!["a.method", "b.method"]);
        assert!(!registry.is_empty());
        registry.clear();
        assert!(registry.is_empty());
    }

    #[test]
    fn registration_needs_no_async_runtime() {
        // Startup code registers from `Once::call_once` and plain `fn main`,
        // neither of which has a runtime. A `#[test]` has none either, so this
        // compiling and running *is* the assertion.
        let registry = NativeRegistry::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        registry.register::<Req, Resp, _, _>("demo.m", move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Resp(String::new()))
            }
        });
        assert!(registry.is_registered("demo.m"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
