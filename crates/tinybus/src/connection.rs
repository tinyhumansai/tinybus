//! A peer's link to the broker: outgoing calls, incoming dispatch, signals.
//!
//! One [`Connection`] serves both roles a peer can have. The kernel uses the
//! client half ([`Connection::call`], [`Connection::add_match`]); an
//! integration additionally uses the service half ([`Connection::serve_at`],
//! [`Connection::emit`]). They are not separate types because a service that
//! calls another service is normal — the transcription service asking the
//! notification service to say it is done should not need a second socket.
//!
//! # The dispatch loop
//!
//! Exactly one task reads the transport. It never awaits user code inline: a
//! method call is handed to a spawned task, so a slow `Transcribe` cannot stop
//! the connection from noticing that a reply to an earlier call has arrived.
//! That is the whole reason calls carry serials rather than relying on order.
//!
//! # Timeouts
//!
//! Every call has a deadline, defaulting to [`DEFAULT_TIMEOUT`]. This is not
//! optional and cannot be disabled, because the failure it prevents is the one
//! that motivated the project: an integration wedged inside a third-party
//! library used to wedge the kernel with it. A timeout does *not* cancel the
//! remote work — tinybus cannot — it stops waiting and frees the caller.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};

use crate::error::{Error, Result};
use crate::message::{Header, Message, MessageKind};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::ports::Transport;
use crate::proxy::Proxy;
use crate::router::MatchRule;
use crate::service::{Interface, ObjectTree};
use crate::version::{Compatibility, PeerManifest, PeerRecord};

/// How long a call waits before giving up.
///
/// Thirty seconds is long enough for a model round-trip or a cold service
/// start, and short enough that a wedged integration surfaces as an error
/// inside one user interaction rather than as a hang.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How many signals may queue for a subscriber before the oldest are dropped.
///
/// Dropping rather than blocking is the right trade for a broadcast: one slow
/// subscriber must not stall every other subscriber, and a lagging receiver is
/// told it lagged (`RecvError::Lagged`) rather than being silently starved.
pub const SIGNAL_BUFFER: usize = 256;

/// How many outbound messages may queue before a sender waits.
///
/// The queue is what makes [`Connection::try_send`] — and therefore a
/// synchronous, fire-and-forget `publish` — possible at all: a sync caller
/// cannot await a socket write, but it can hand a message to a writer task.
/// Bounded, because an unbounded outbox turns a slow broker into unbounded
/// growth in the process doing the publishing.
pub const OUTBOX_CAPACITY: usize = 1024;

struct Inner {
    transport: Arc<dyn Transport>,
    /// Everything outbound goes through here and out via the writer task.
    /// Serialising writes through one task is also what lets `send` be called
    /// concurrently without interleaving two frames on the wire.
    outbox: mpsc::Sender<Message>,
    serial: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Message>>>,
    objects: RwLock<ObjectTree>,
    /// A *std* lock, not a tokio one, so a synchronous publisher can stamp
    /// the sender on a locally looped-back signal without an await.
    unique_name: std::sync::RwLock<Option<BusName>>,
    signals: broadcast::Sender<Message>,
    panic_handler: std::sync::RwLock<Option<Arc<dyn Fn() -> Error + Send + Sync>>>,
}

/// Closes the transport when the last [`Connection`] handle goes away.
///
/// Needed because the dispatch task holds its own `Arc<Inner>`, so the
/// refcount on `Inner` never reaches zero while that task lives — and the task
/// only exits when the transport closes. Without this guard, dropping the last
/// `Connection` would leak a task, and, worse, the *broker* would never see the
/// hangup: a service that exited would keep its well-known name until the
/// process died. `NameOwnerChanged` firing promptly is the whole reason the
/// kernel can react to an integration dying instead of timing out on it.
struct CloseOnDrop(Arc<Inner>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        let inner = self.0.clone();
        // Spawned, because `close` is async and `Drop` is not. If there is no
        // runtime left to spawn on the process is going away anyway, which
        // closes the transport by closing its file descriptors.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let _ = inner.transport.close().await;
            });
        }
    }
}

/// One peer's link to the bus. Cheap to clone; every clone shares one
/// transport, one serial counter and one object tree.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
    /// Shared, so the hangup happens when the *last* handle drops.
    _close: Arc<CloseOnDrop>,
}

impl Connection {
    /// Attach to the bus over `transport` and complete the `Hello` handshake.
    ///
    /// Returns once the broker has assigned a unique name, so
    /// [`Connection::unique_name`] is populated for the whole life of the
    /// connection and callers never have to handle "not yet named".
    pub async fn connect(transport: Box<dyn Transport>) -> Result<Self> {
        let conn = Self::attach(transport.into());
        conn.handshake().await?;
        Ok(conn)
    }

    /// Complete the `Hello` handshake on an already-attached connection.
    ///
    /// Split out from [`Connection::connect`] for callers that must control
    /// *which runtime* the connection's tasks are spawned on. [`Connection::attach`]
    /// is where the spawning happens and is synchronous, so such a caller can
    /// hold a runtime guard across `attach` — which is `!Send` and therefore
    /// cannot be held across an await — and then handshake afterwards, off the
    /// guard. See [`crate::global::OnceBus::init_in_process`].
    ///
    /// Idempotent in the only sense that matters: calling it twice would
    /// request a second unique name, so don't.
    pub async fn handshake(&self) -> Result<()> {
        let name: String = self
            .call_bus("Hello", serde_json::json!([]))
            .await
            .and_then(|v| Ok(serde_json::from_value(v)?))?;
        *self
            .inner
            .unique_name
            .write()
            .expect("the unique-name lock is never held across a panic point") =
            Some(BusName::new(name)?);
        Ok(())
    }

    /// Wire up a connection without handshaking.
    ///
    /// Used by the broker for its own side of a peer link, and by tests that
    /// drive the protocol by hand. Ordinary callers want
    /// [`Connection::connect`].
    pub fn attach(transport: Arc<dyn Transport>) -> Self {
        let (signals, _) = broadcast::channel(SIGNAL_BUFFER);
        let (outbox, outbound) = mpsc::channel(OUTBOX_CAPACITY);
        let inner = Arc::new(Inner {
            transport,
            outbox,
            // Serials start at 1: zero is the "unassigned" value a freshly
            // built `Message` carries, so it must never be a live serial.
            serial: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            objects: RwLock::new(ObjectTree::new()),
            unique_name: std::sync::RwLock::new(None),
            signals,
            panic_handler: std::sync::RwLock::new(None),
        });
        tokio::spawn(writer_loop(inner.transport.clone(), outbound));
        tokio::spawn(dispatch_loop(inner.clone()));
        Self {
            _close: Arc::new(CloseOnDrop(inner.clone())),
            inner,
        }
    }

    /// This peer's broker-assigned unique name, once handshaken.
    pub fn unique_name(&self) -> Option<BusName> {
        self.inner
            .unique_name
            .read()
            .expect("the unique-name lock is never held across a panic point")
            .clone()
    }

    /// Deliver a signal to *this* connection's own subscribers, without the
    /// broker.
    ///
    /// The broker never echoes a signal back to the peer that sent it — that is
    /// what stops a service which both emits and subscribes on one interface
    /// from looping. But a host whose publishers and subscribers live in the
    /// same process still expects its own subscribers to see what it published,
    /// which was free when the bus was a `tokio::sync::broadcast` inside that
    /// process.
    ///
    /// This closes that gap explicitly: the publisher loops the signal back
    /// locally and the broker fans it out to everyone else, so every subscriber
    /// anywhere sees it exactly once. It is a separate method rather than
    /// behaviour folded into [`Connection::emit`] precisely because the
    /// loop-prevention it steps around is load-bearing for services; the caller
    /// has to mean it.
    pub fn deliver_local(&self, mut message: Message) {
        message.header.sender = self.unique_name();
        // Fails only when nothing on this connection is subscribed, which is
        // the normal case for a write-only publisher.
        let _ = self.inner.signals.send(message);
    }

    /// Export `interface` at `path`.
    pub async fn serve_at(&self, path: ObjectPath, interface: impl Interface) -> Result<()> {
        self.inner
            .objects
            .write()
            .await
            .insert(path, Arc::new(interface));
        Ok(())
    }

    /// Stop exporting everything at `path`.
    pub async fn unserve(&self, path: &ObjectPath) -> bool {
        self.inner.objects.write().await.remove(path)
    }

    /// Claim a well-known name. Fails if another live peer holds it.
    pub async fn request_name(&self, name: impl AsRef<str>) -> Result<()> {
        let name = BusName::new(name.as_ref())?;
        self.call_bus("RequestName", serde_json::json!([name]))
            .await
            .map(|_| ())
    }

    /// Give up a well-known name.
    pub async fn release_name(&self, name: impl AsRef<str>) -> Result<()> {
        let name = BusName::new(name.as_ref())?;
        self.call_bus("ReleaseName", serde_json::json!([name]))
            .await
            .map(|_| ())
    }

    /// Every name currently owned on the bus, unique names included.
    pub async fn list_names(&self) -> Result<Vec<BusName>> {
        let value = self.call_bus("ListNames", serde_json::json!([])).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Which peer, if any, owns `name`.
    pub async fn name_owner(&self, name: impl AsRef<str>) -> Result<Option<BusName>> {
        let name = BusName::new(name.as_ref())?;
        let value = self
            .call_bus("GetNameOwner", serde_json::json!([name]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Tell the broker what this peer speaks and accepts.
    ///
    /// Announcing is optional and additive: a peer that never calls this stays
    /// fully routable, so manifests can be adopted one service at a time. What
    /// it buys is that *other* peers can check compatibility before calling,
    /// and get a verdict naming versions rather than a decode error naming
    /// JSON.
    pub async fn announce(&self, manifest: &PeerManifest) -> Result<()> {
        self.call_bus("Announce", serde_json::json!([manifest]))
            .await
            .map(|_| ())
    }

    /// What `name` announced, if anything.
    pub async fn manifest_of(&self, name: impl AsRef<str>) -> Result<Option<PeerManifest>> {
        let name = BusName::new(name.as_ref())?;
        let value = self
            .call_bus("GetManifest", serde_json::json!([name]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Every peer that has announced, with the names it owns.
    pub async fn peers(&self) -> Result<Vec<PeerRecord>> {
        let value = self.call_bus("ListPeers", serde_json::json!([])).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Every module known to the embedded host.
    #[cfg(feature = "modules")]
    pub async fn list_modules(&self) -> Result<Vec<crate::module::ModuleInfo>> {
        let value = self.call_bus("ListModules", serde_json::json!([])).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Inspect one module by stable module name.
    #[cfg(feature = "modules")]
    pub async fn module(&self, name: impl AsRef<str>) -> Result<Option<crate::module::ModuleInfo>> {
        let value = self
            .call_bus("GetModule", serde_json::json!([name.as_ref()]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Read one module's declared manifest without initializing it.
    #[cfg(feature = "modules")]
    pub async fn module_manifest(
        &self,
        name: impl AsRef<str>,
    ) -> Result<Option<crate::module::manifest::ModuleManifest>> {
        let value = self
            .call_bus("GetModuleManifest", serde_json::json!([name.as_ref()]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Dynamically install one module, passing JSON setup configuration.
    #[cfg(feature = "modules")]
    pub async fn load_module(
        &self,
        path: impl AsRef<std::path::Path>,
        config: serde_json::Value,
    ) -> Result<crate::module::ModuleInfo> {
        let value = self
            .call_bus(
                "LoadModule",
                serde_json::json!([path.as_ref().to_string_lossy(), config]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Stop one module. Its library remains mapped until process exit.
    #[cfg(feature = "modules")]
    pub async fn stop_module(
        &self,
        name: impl AsRef<str>,
        deadline: Duration,
    ) -> Result<crate::module::ModuleInfo> {
        let value = self
            .call_bus(
                "StopModule",
                serde_json::json!([name.as_ref(), deadline.as_millis() as u64]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Enable or disable a known module for subsequent scans.
    #[cfg(feature = "modules")]
    pub async fn enable_module(
        &self,
        name: impl AsRef<str>,
        enabled: bool,
    ) -> Result<crate::module::ModuleInfo> {
        let value = self
            .call_bus("EnableModule", serde_json::json!([name.as_ref(), enabled]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Rescan the host's configured module directories.
    #[cfg(feature = "modules")]
    pub async fn rescan_modules(&self) -> Result<Vec<crate::module::ModuleInfo>> {
        self.scan_modules(std::iter::empty::<&std::path::Path>(), false)
            .await
    }

    /// Scan explicit module directories, optionally without initializing any
    /// admitted artifact.
    #[cfg(feature = "modules")]
    pub async fn scan_modules<P: AsRef<std::path::Path>>(
        &self,
        paths: impl IntoIterator<Item = P>,
        dry_run: bool,
    ) -> Result<Vec<crate::module::ModuleInfo>> {
        let paths = paths
            .into_iter()
            .map(|path| path.as_ref().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let value = self
            .call_bus("RescanModules", serde_json::json!([paths, dry_run]))
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Check whether this peer can call `interface` on `destination`.
    ///
    /// `local` is this peer's own manifest. Returns the verdict rather than an
    /// error so a caller can decide what an incompatibility means for it — a
    /// missing optional integration is a degraded feature, not a startup
    /// failure. Use [`Connection::require`] when it *is* a startup failure.
    pub async fn compatibility(
        &self,
        destination: impl AsRef<str>,
        interface: impl AsRef<str>,
        local: &PeerManifest,
    ) -> Result<Compatibility> {
        let interface = InterfaceName::new(interface.as_ref())?;
        let Some(remote) = self.manifest_of(destination.as_ref()).await? else {
            // A peer that has not announced is treated as compatible, for the
            // same reason the broker does not enforce: adoption is incremental.
            return Ok(Compatibility::Compatible {
                provider_speaks: crate::version::Version::new(0, 0, 0),
                consumer_speaks: crate::version::Version::new(0, 0, 0),
            });
        };
        Ok(crate::version::check(&remote, local, &interface))
    }

    /// Like [`Connection::compatibility`], but turn an incompatibility into an
    /// error naming both versions.
    ///
    /// For a dependency the caller cannot run without. Failing here — at
    /// startup, with both versions in the message — is the entire point of the
    /// exercise: the alternative is the same failure hours later, as a
    /// deserialize error in a log line that mentions neither peer.
    pub async fn require(
        &self,
        destination: impl AsRef<str>,
        interface: impl AsRef<str>,
        local: &PeerManifest,
    ) -> Result<()> {
        let verdict = self
            .compatibility(destination.as_ref(), interface.as_ref(), local)
            .await?;
        if verdict.is_compatible() {
            return Ok(());
        }
        Err(Error::IncompatibleVersion {
            peer: destination.as_ref().to_string(),
            interface: interface.as_ref().to_string(),
            detail: verdict.to_string(),
        })
    }

    /// Subscribe to signals matching `rule`.
    ///
    /// Returns a receiver rather than taking a callback: a callback would have
    /// to run on the dispatch task, and anything it awaited would delay every
    /// other message on the connection.
    pub async fn add_match(&self, rule: MatchRule) -> Result<broadcast::Receiver<Message>> {
        // Subscribe *before* telling the broker, so a signal that fires between
        // the two cannot slip through the gap.
        let receiver = self.inner.signals.subscribe();
        self.call_bus("AddMatch", serde_json::json!([rule_to_wire(&rule)]))
            .await?;
        Ok(receiver)
    }

    /// A receiver for every signal already subscribed to on this connection.
    pub fn signals(&self) -> broadcast::Receiver<Message> {
        self.inner.signals.subscribe()
    }

    /// Broadcast a signal. Returns as soon as the broker has it; there is no
    /// delivery confirmation, by design — a signal nobody subscribed to is not
    /// a failure.
    pub async fn emit(
        &self,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: impl Serialize,
    ) -> Result<()> {
        let message = Message::signal(path, interface, member, to_body(&body)?);
        self.send(message).await
    }

    /// A typed handle to one interface on one remote object.
    pub fn proxy(
        &self,
        destination: impl AsRef<str>,
        path: impl AsRef<str>,
        interface: impl AsRef<str>,
    ) -> Result<Proxy> {
        Proxy::new(
            self.clone(),
            BusName::new(destination.as_ref())?,
            ObjectPath::new(path.as_ref())?,
            InterfaceName::new(interface.as_ref())?,
        )
    }

    /// Call a method and deserialize the reply, using [`DEFAULT_TIMEOUT`].
    pub async fn call<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl Serialize,
    ) -> Result<R> {
        self.call_with_timeout(destination, path, interface, member, args, DEFAULT_TIMEOUT)
            .await
    }

    /// Call a method with an explicit deadline.
    pub async fn call_with_timeout<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl Serialize,
        timeout: Duration,
    ) -> Result<R> {
        let message = Message::method_call(
            destination,
            path,
            interface,
            member.clone(),
            to_body(&args)?,
        );
        let reply = self.call_raw(message, timeout).await?;
        Ok(serde_json::from_value(reply)?)
    }

    /// Send a call and wait for its reply, without typing either end.
    ///
    /// The plumbing under every typed call, and what `tinybus call` uses.
    pub async fn call_raw(&self, mut message: Message, timeout: Duration) -> Result<Value> {
        let serial = self.inner.serial.fetch_add(1, Ordering::Relaxed);
        message.header.serial = serial;
        message.validate()?;
        let member = message.member_or_unknown();

        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().await.insert(serial, tx);

        if let Err(e) = self.enqueue(message).await {
            self.inner.pending.lock().await.remove(&serial);
            return Err(e);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(reply)) => match reply.header.kind {
                MessageKind::Error => Err(reply.into_error()),
                _ => Ok(reply.body),
            },
            // The dispatch loop dropped the sender: the transport is gone.
            Ok(Err(_)) => Err(Error::ConnectionClosed),
            Err(_) => {
                // Reclaim the slot, or a timed-out call leaks one entry per
                // occurrence for the life of the connection.
                self.inner.pending.lock().await.remove(&serial);
                Err(Error::Timeout {
                    member,
                    timeout_ms: timeout.as_millis() as u64,
                })
            }
        }
    }

    /// Send a message with no reply expected.
    pub async fn send(&self, mut message: Message) -> Result<()> {
        message.header.serial = self.inner.serial.fetch_add(1, Ordering::Relaxed);
        message.validate()?;
        self.enqueue(message).await
    }

    /// Send a message without awaiting, failing rather than blocking when the
    /// outbox is full.
    ///
    /// This is the seam a synchronous, fire-and-forget publisher needs. A
    /// domain that wants to announce "a message arrived" from a plain `fn`
    /// cannot await a socket write, and making it await would push `async` up
    /// through hundreds of call sites that have no other reason to have it.
    ///
    /// A full outbox means the process is producing faster than the broker is
    /// draining. Returning [`Error::Backpressure`] rather than blocking is the
    /// deliberate choice: a notification is worth less than the latency of the
    /// caller that would be stalled to deliver it, and a caller that *does*
    /// care can use [`Connection::send`].
    pub fn try_send(&self, mut message: Message) -> Result<()> {
        message.header.serial = self.inner.serial.fetch_add(1, Ordering::Relaxed);
        message.validate()?;
        self.inner.outbox.try_send(message).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Backpressure,
            mpsc::error::TrySendError::Closed(_) => Error::ConnectionClosed,
        })
    }

    /// Broadcast a signal without awaiting. The sync twin of
    /// [`Connection::emit`]; see [`Connection::try_send`] for when to reach for
    /// it.
    pub fn try_emit(
        &self,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        body: impl Serialize,
    ) -> Result<()> {
        let message = Message::signal(path, interface, member, to_body(&body)?);
        self.try_send(message)
    }

    /// Hand a message to the writer task, waiting if the outbox is full.
    ///
    /// Waiting here is backpressure a caller asked for by using an async send:
    /// it bounds how far ahead of the broker this process can run.
    async fn enqueue(&self, message: Message) -> Result<()> {
        self.inner
            .outbox
            .send(message)
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    /// Close the link.
    pub async fn close(&self) -> Result<()> {
        self.inner.transport.close().await
    }

    /// Install module-boundary panic conversion for dispatched methods.
    ///
    /// Hidden because ordinary process peers intentionally keep the existing
    /// policy that a panic is fatal to the service. The module SDK uses this to
    /// turn an unwind into a redacted error reply before applying its manifest
    /// panic policy.
    #[doc(hidden)]
    pub fn __set_panic_handler(&self, handler: Arc<dyn Fn() -> Error + Send + Sync>) {
        *self
            .inner
            .panic_handler
            .write()
            .expect("panic handler lock") = Some(handler);
    }

    /// Call a method on the broker's own interface.
    async fn call_bus(&self, member: &str, args: Value) -> Result<Value> {
        let message = Message::method_call(
            BusName::new(crate::BUS_NAME)?,
            ObjectPath::new(crate::BUS_PATH)?,
            InterfaceName::new(crate::BUS_INTERFACE)?,
            MemberName::new(member)?,
            args,
        );
        self.call_raw(message, DEFAULT_TIMEOUT).await
    }
}

/// Serialize a call body, normalising it to the positional array the protocol
/// specifies.
///
/// A caller writing `("/tmp/a.wav",)` and a caller writing
/// `["/tmp/a.wav"]` mean the same thing; a caller writing a bare `"/tmp/a.wav"`
/// almost certainly also does. Wrapping a scalar rather than rejecting it makes
/// the one-argument case — by far the most common — pleasant to write.
fn to_body(value: &impl Serialize) -> Result<Value> {
    let value = serde_json::to_value(value)?;
    Ok(match value {
        Value::Array(_) => value,
        Value::Null => Value::Array(Vec::new()),
        other => Value::Array(vec![other]),
    })
}

/// Render a rule back into its wire form for `AddMatch`.
fn rule_to_wire(rule: &MatchRule) -> String {
    let mut clauses: Vec<String> = Vec::new();
    if let Some(kind) = rule.kind {
        let name = match kind {
            MessageKind::Signal => "signal",
            MessageKind::MethodCall => "method_call",
            MessageKind::MethodReturn => "method_return",
            MessageKind::Error => "error",
        };
        clauses.push(format!("type={name}"));
    }
    if let Some(v) = &rule.sender {
        clauses.push(format!("sender={v}"));
    }
    if let Some(v) = &rule.interface {
        clauses.push(format!("interface={v}"));
    }
    if let Some(v) = &rule.member {
        clauses.push(format!("member={v}"));
    }
    if let Some(v) = &rule.path {
        clauses.push(format!("path={v}"));
    }
    if let Some(v) = &rule.path_namespace {
        clauses.push(format!("path_namespace={v}"));
    }
    clauses.join(",")
}

/// The single writer task. Owns `send`; drains the outbox onto the transport.
///
/// One task rather than "everyone writes concurrently" for two reasons: a
/// synchronous publisher needs somewhere to hand a message to, and two
/// concurrent writers on one transport risk interleaving frames.
async fn writer_loop(transport: Arc<dyn Transport>, mut outbound: mpsc::Receiver<Message>) {
    while let Some(message) = outbound.recv().await {
        if let Err(e) = transport.send(message).await {
            tracing::debug!(error = %e, "connection write failed; closing");
            break;
        }
    }
}

/// The single reader task. Owns `recv`; never awaits user code inline.
async fn dispatch_loop(inner: Arc<Inner>) {
    loop {
        let message = match inner.transport.recv().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "connection read failed; closing");
                break;
            }
        };

        match message.header.kind {
            MessageKind::MethodReturn | MessageKind::Error => {
                let Some(serial) = message.header.reply_serial else {
                    tracing::debug!("dropped a reply with no reply_serial");
                    continue;
                };
                // An unmatched reply is a timed-out call arriving late. Normal,
                // and nothing to do but drop it.
                if let Some(waiter) = inner.pending.lock().await.remove(&serial) {
                    let _ = waiter.send(message);
                }
            }
            MessageKind::Signal => {
                // `send` fails only when nobody is subscribed, which is the
                // common case for a service that emits but never listens.
                let _ = inner.signals.send(message);
            }
            MessageKind::MethodCall => {
                // Spawned, so a long-running method does not block replies to
                // calls this connection has outstanding.
                tokio::spawn(handle_call(inner.clone(), message));
            }
        }
    }

    // Wake everyone still waiting rather than leaving them to time out one by
    // one: the link is gone and no reply is ever coming.
    inner.pending.lock().await.clear();
}

/// Run one inbound method call and send its reply.
async fn handle_call(inner: Arc<Inner>, message: Message) {
    let header = message.header.clone();
    let panic_handler = inner
        .panic_handler
        .read()
        .expect("panic handler lock")
        .clone();
    let result = if let Some(panic_handler) = panic_handler {
        match CatchUnwind::new(dispatch(&inner, &header, message.body)).await {
            Ok(result) => result,
            Err(()) => Err(panic_handler()),
        }
    } else {
        dispatch(&inner, &header, message.body).await
    };

    let mut reply = match result {
        Ok(value) => Message::method_return(&header, value),
        Err(e) => Message::error_reply(&header, &e),
    };
    reply.header.serial = inner.serial.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = inner.outbox.send(reply).await {
        tracing::debug!(error = %e, "could not reply; the caller will time out");
    }
}

struct CatchUnwind<F> {
    future: std::panic::AssertUnwindSafe<F>,
}

impl<F> CatchUnwind<F> {
    fn new(future: F) -> Self {
        Self {
            future: std::panic::AssertUnwindSafe(future),
        }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::result::Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // `future` is structurally pinned with its wrapper and never moved.
        let future = unsafe { self.map_unchecked_mut(|this| &mut this.future.0) };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

async fn dispatch(inner: &Inner, header: &Header, body: Value) -> Result<Value> {
    let (Some(path), Some(interface), Some(member)) =
        (&header.path, &header.interface, &header.member)
    else {
        return Err(Error::protocol("method call is missing an address"));
    };
    let objects = inner.objects.read().await;
    objects.dispatch(path, interface, member, body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::memory::MemoryTransport;
    use async_trait::async_trait;

    /// A service that answers `Echo` and fails `Boom`.
    struct Echo;

    #[async_trait]
    impl Interface for Echo {
        fn name(&self) -> InterfaceName {
            InterfaceName::new("ai.tinyhumans.Test").unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![
                MemberName::new("Echo").unwrap(),
                MemberName::new("Boom").unwrap(),
                MemberName::new("Panic").unwrap(),
                MemberName::new("Hang").unwrap(),
            ]
        }

        async fn call(&self, member: &MemberName, args: Value) -> Result<Value> {
            match member.as_str() {
                "Echo" => Ok(args),
                "Boom" => Err(Error::failed("as requested")),
                "Panic" => panic!("secret panic payload"),
                "Hang" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(Value::Null)
                }
                other => Err(Error::failed(format!("unreachable: {other}"))),
            }
        }
    }

    fn path() -> ObjectPath {
        ObjectPath::new("/ai/tinyhumans/Test").unwrap()
    }

    fn call(member: &str, body: Value) -> Message {
        Message::method_call(
            BusName::new("ai.tinyhumans.Test").unwrap(),
            path(),
            InterfaceName::new("ai.tinyhumans.Test").unwrap(),
            MemberName::new(member).unwrap(),
            body,
        )
    }

    /// Two connections wired directly to each other, with no broker in the
    /// middle: enough to exercise dispatch, replies, serials and timeouts.
    async fn pair() -> (Connection, Connection) {
        let (a, b) = MemoryTransport::pair();
        let client = Connection::attach(Arc::new(a));
        let service = Connection::attach(Arc::new(b));
        service.serve_at(path(), Echo).await.unwrap();
        (client, service)
    }

    #[tokio::test]
    async fn a_call_reaches_the_service_and_the_reply_comes_back() {
        let (client, _service) = pair().await;
        let reply = client
            .call_raw(call("Echo", serde_json::json!(["hi"])), DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(reply, serde_json::json!(["hi"]));
    }

    #[tokio::test]
    async fn a_failing_method_arrives_as_an_error_with_its_name_intact() {
        let (client, _service) = pair().await;
        let err = client
            .call_raw(call("Boom", serde_json::json!([])), DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.wire_name(), Error::FAILED);
        assert!(err.to_string().contains("as requested"), "{err}");
    }

    #[tokio::test]
    async fn a_panicking_module_method_becomes_an_error_reply_rather_than_an_abort() {
        let (client, service) = pair().await;
        service.__set_panic_handler(Arc::new(|| Error::MethodFailed {
            name: "ai.tinyhumans.tinybus.Error.ModulePanicked".to_string(),
            message: "module panicked at fixture.rs:12:3".to_string(),
        }));
        let error = client
            .call_raw(call("Panic", serde_json::json!([])), DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(
            error.wire_name(),
            "ai.tinyhumans.tinybus.Error.ModulePanicked"
        );
        let reply = client
            .call_raw(call("Echo", serde_json::json!(["alive"])), DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(reply, serde_json::json!(["alive"]));
    }

    #[tokio::test]
    async fn a_panic_reply_carries_the_location_but_never_the_payload() {
        let (client, service) = pair().await;
        service.__set_panic_handler(Arc::new(|| Error::MethodFailed {
            name: "ai.tinyhumans.tinybus.Error.ModulePanicked".to_string(),
            message: "module panicked at fixture.rs:12:3".to_string(),
        }));
        let error = client
            .call_raw(call("Panic", serde_json::json!([])), DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fixture.rs:12:3"));
        assert!(!error.to_string().contains("secret panic payload"));
    }

    #[tokio::test]
    async fn an_unknown_member_is_reported_as_such_rather_than_hanging() {
        let (client, _service) = pair().await;
        let err = client
            .call_raw(call("Nope", serde_json::json!([])), DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.wire_name(), Error::UNKNOWN_METHOD);
    }

    #[tokio::test]
    async fn a_wedged_method_times_out_the_caller_and_not_the_connection() {
        let (client, _service) = pair().await;
        let err = client
            .call_raw(
                call("Hang", serde_json::json!([])),
                Duration::from_millis(50),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }), "{err}");

        // The point of the exercise: the connection still works afterwards.
        let reply = client
            .call_raw(
                call("Echo", serde_json::json!(["still here"])),
                DEFAULT_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(reply, serde_json::json!(["still here"]));
    }

    #[tokio::test]
    async fn a_timed_out_call_does_not_leak_its_pending_slot() {
        let (client, _service) = pair().await;
        let _ = client
            .call_raw(
                call("Hang", serde_json::json!([])),
                Duration::from_millis(20),
            )
            .await;
        assert!(client.inner.pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn concurrent_calls_are_matched_by_serial_not_by_order() {
        let (client, _service) = pair().await;
        let slow = client.call_raw(call("Echo", serde_json::json!(["first"])), DEFAULT_TIMEOUT);
        let fast = client.call_raw(call("Echo", serde_json::json!(["second"])), DEFAULT_TIMEOUT);
        let (a, b) = tokio::join!(slow, fast);
        assert_eq!(a.unwrap(), serde_json::json!(["first"]));
        assert_eq!(b.unwrap(), serde_json::json!(["second"]));
    }

    #[tokio::test]
    async fn a_dropped_peer_wakes_waiters_instead_of_making_them_wait_out_the_clock() {
        let (a, b) = MemoryTransport::pair();
        let client = Connection::attach(Arc::new(a));
        drop(b);
        let err = client
            .call_raw(
                call("Echo", serde_json::json!([])),
                Duration::from_secs(300),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::ConnectionClosed | Error::Transport(_)),
            "{err}"
        );
    }

    #[test]
    fn a_scalar_argument_is_wrapped_into_the_positional_array() {
        assert_eq!(to_body(&"a").unwrap(), serde_json::json!(["a"]));
        assert_eq!(to_body(&("a", 1)).unwrap(), serde_json::json!(["a", 1]));
        assert_eq!(to_body(&Vec::<u8>::new()).unwrap(), serde_json::json!([]));
        assert_eq!(to_body(&Value::Null).unwrap(), serde_json::json!([]));
    }

    #[test]
    fn a_rule_round_trips_through_its_wire_form() {
        let rule = MatchRule::parse(
            "type=signal,interface=ai.tinyhumans.Mail,member=Received,path_namespace=/ai/Mail",
        )
        .unwrap();
        assert_eq!(MatchRule::parse(&rule_to_wire(&rule)).unwrap(), rule);
    }
}
