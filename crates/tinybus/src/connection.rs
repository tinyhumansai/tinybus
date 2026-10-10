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
use std::io::{self, Write};
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
use crate::stream::{
    STREAM_INTERFACE, STREAM_PATH, StreamDescriptor, StreamLimits, StreamReader, StreamRef,
    StreamRegistry, StreamWriter,
};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamReplyEnvelope {
    #[serde(rename = "$tinybus_stream_reply")]
    stream: StreamRef,
}

const STREAM_REPLY_ENCODE_CHUNK: usize = 16 * 1024;

struct CountingWriter(u64);

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("streamed reply length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct StreamReplyEncoder {
    sender: mpsc::Sender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl StreamReplyEncoder {
    fn new(sender: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(STREAM_REPLY_ENCODE_CHUNK),
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buffer);
        self.sender
            .blocking_send(chunk)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "stream receiver closed"))
    }
}

impl Write for StreamReplyEncoder {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        while !bytes.is_empty() {
            let available = STREAM_REPLY_ENCODE_CHUNK - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == STREAM_REPLY_ENCODE_CHUNK {
                self.send_buffer()?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffer()
    }
}
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
    brokered_native: bool,
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
    /// Bulk streams being received. On the connection rather than in the object
    /// tree because a chunk has to be checked against the header's stamped
    /// `sender`; interfaces receive only the verified subset in CallContext.
    streams: StreamRegistry,
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
struct CloseOnDrop(Option<Arc<Inner>>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        let Some(inner) = self.0.clone() else {
            return;
        };
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
        Self::attach_inner(transport, false)
    }

    /// Attach the module SDK's broker-proven C transport bridge.
    ///
    /// # Safety
    /// The caller must guarantee every received frame with a sender comes
    /// exclusively from a real TinyBus broker which overwrites it on ingress.
    /// The SDK captures routing at delivery and removes sender on unverified
    /// frames, including frames queued before an eager module's admission. A successful
    /// Hello, artifact attestation, or arbitrary C callback is insufficient.
    /// This boundary is an unsafe trusted-host contract, not cryptographic
    /// authentication against a malicious native host.
    #[doc(hidden)]
    pub unsafe fn __attach_brokered_module(transport: Arc<dyn Transport>) -> Self {
        Self::attach_inner(transport, true)
    }

    fn attach_inner(transport: Arc<dyn Transport>, brokered_native: bool) -> Self {
        let (signals, _) = broadcast::channel(SIGNAL_BUFFER);
        let (outbox, outbound) = mpsc::channel(OUTBOX_CAPACITY);
        let inner = Arc::new(Inner {
            transport,
            brokered_native,
            outbox,
            // Serials start at 1: zero is the "unassigned" value a freshly
            // built `Message` carries, so it must never be a live serial.
            serial: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            objects: RwLock::new(ObjectTree::new()),
            unique_name: std::sync::RwLock::new(None),
            signals,
            panic_handler: std::sync::RwLock::new(None),
            streams: StreamRegistry::new(),
        });
        tokio::spawn(writer_loop(inner.transport.clone(), outbound));
        tokio::spawn(dispatch_loop(inner.clone()));
        Self {
            _close: Arc::new(CloseOnDrop(Some(inner.clone()))),
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

    /// What the broker has verified about whoever owns `name`.
    ///
    /// The check a sender makes before it commits to handing over a secret.
    /// `None` means the bus will refuse a confidential message to that name —
    /// nothing owns it, or the operator never allowlisted an artifact for it.
    pub async fn attestation(&self, name: BusName) -> Result<Option<crate::attest::Attestation>> {
        let value = self
            .call_bus("GetAttestation", serde_json::json!([name]))
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
            .call_bus_sensitive(
                "LoadModule",
                serde_json::json!([path.as_ref().to_string_lossy(), config]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Download and load a verified GitHub release module through the host.
    #[cfg(feature = "modules")]
    pub async fn load_github_module(
        &self,
        release_url: impl AsRef<str>,
        asset_name: impl AsRef<str>,
        sha256: impl AsRef<str>,
        config: serde_json::Value,
    ) -> Result<crate::module::ModuleInfo> {
        let value = self
            .call_bus_sensitive(
                "LoadGithubModule",
                serde_json::json!([
                    release_url.as_ref(),
                    asset_name.as_ref(),
                    sha256.as_ref(),
                    config
                ]),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Apply replacement JSON configuration to a running module.
    ///
    /// Configuration is always carried as a sensitive control call and is
    /// never eligible for signal fan-out or monitor output.
    #[cfg(feature = "modules")]
    pub async fn reinitialize_module(
        &self,
        name: impl AsRef<str>,
        config: serde_json::Value,
    ) -> Result<crate::module::ModuleInfo> {
        let value = self
            .call_bus_sensitive(
                "ReinitializeModule",
                serde_json::json!([name.as_ref(), config]),
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
    pub async fn call_raw(&self, message: Message, timeout: Duration) -> Result<Value> {
        Ok(self.call_raw_message(message, timeout).await?.body)
    }

    async fn call_raw_message(&self, mut message: Message, timeout: Duration) -> Result<Message> {
        let serial = self.inner.serial.fetch_add(1, Ordering::Relaxed);
        message.header.serial = serial;
        message.validate()?;
        if message.header.confidential && message.header.stream_reply {
            return Err(Error::protocol(
                "a confidential call cannot request a streamed reply",
            ));
        }
        // Refused here, in the sender's own process, rather than at the broker.
        // A stream's bytes travel as their own unflagged `Write` calls, so a
        // handle in a confidential body protects the handle and not the payload
        // — and the broker cannot catch that for us, because seeing the handle
        // would mean reading a confidential body. Checked only when the flag is
        // set, so ordinary traffic pays nothing.
        if (message.header.confidential || message.header.sensitive)
            && crate::stream::body_contains_stream_ref(&message.body)
        {
            return Err(Error::protocol(
                "a private call cannot carry a stream handle: the stream's bytes \
                 travel as separate unattested writes, so the payload would not be \
                 private even though the handle was",
            ));
        }
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
                _ => Ok(reply),
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

    /// What this connection will accept from peers sending it bulk streams.
    pub fn stream_limits(&self) -> StreamLimits {
        self.inner.streams.limits()
    }

    /// Change what this connection accepts from peers sending it bulk streams.
    ///
    /// Takes effect on the next `Open`; streams already running keep the window
    /// they were opened with, because shrinking a window under a sender that is
    /// mid-transfer would abort a transfer that was within the rules when it
    /// started.
    pub fn set_stream_limits(&self, limits: StreamLimits) {
        self.inner.streams.set_limits(limits);
    }

    /// Open a bulk stream to `destination` and get the writer for it.
    ///
    /// The usual shape is: open, put [`StreamWriter::stream_ref`] in a method
    /// call, issue the call, and write the payload *while the call is
    /// outstanding*. The receiver's window is a few megabytes, so writing a
    /// large payload before the receiving method has been dispatched stalls
    /// against a reader that does not exist yet.
    /// [`Connection::call_with_stream`] does the interleaving for the common
    /// case.
    pub async fn open_stream(
        &self,
        destination: &BusName,
        descriptor: StreamDescriptor,
    ) -> Result<StreamWriter> {
        self.open_stream_with_timeout(destination, descriptor, DEFAULT_TIMEOUT)
            .await
    }

    /// [`Connection::open_stream`] with an explicit deadline for every chunk.
    ///
    /// The deadline applies per chunk, not to the transfer: it is how long this
    /// peer will wait for the receiver to take *one* chunk. A slow consumer of
    /// a large payload is normal; a consumer that has stopped consuming is not.
    pub async fn open_stream_with_timeout(
        &self,
        destination: &BusName,
        descriptor: StreamDescriptor,
        timeout: Duration,
    ) -> Result<StreamWriter> {
        let id: String = serde_json::from_value(
            self.call_stream_member(
                destination,
                "Open",
                serde_json::json!([descriptor]),
                timeout,
            )
            .await?,
        )?;
        Ok(StreamWriter::new(
            self.clone(),
            destination.clone(),
            id,
            descriptor,
            timeout,
        ))
    }

    /// Call a method whose payload is too big for a frame, streaming `bytes`
    /// alongside it.
    ///
    /// `args` is built from the [`StreamRef`] the receiver should read, so the
    /// caller decides where in its own argument list the handle goes. The call
    /// and the payload are in flight together, which is what keeps a sender
    /// from stalling against its own receiver.
    pub async fn call_with_stream<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl FnOnce(&StreamRef) -> Value,
        bytes: &[u8],
    ) -> Result<R> {
        self.call_with_stream_timeout(
            destination,
            path,
            interface,
            member,
            args,
            bytes,
            DEFAULT_TIMEOUT,
        )
        .await
    }

    /// [`Connection::call_with_stream`] with an explicit deadline.
    ///
    /// `timeout` bounds two different waits: how long the callee has to answer,
    /// and how long the receiver has to take any one chunk. Both are "the peer
    /// has stopped making progress" deadlines rather than a budget for the
    /// whole transfer, which is why one value fits both — but the call half is
    /// the one worth thinking about, because the callee cannot reply until it
    /// has read the payload. A large upload to a slow-but-healthy consumer
    /// needs more than [`DEFAULT_TIMEOUT`] here, or it fails a call that was
    /// still making progress.
    #[allow(clippy::too_many_arguments)]
    pub async fn call_with_stream_timeout<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl FnOnce(&StreamRef) -> Value,
        bytes: &[u8],
        timeout: Duration,
    ) -> Result<R> {
        let mut writer = self
            .open_stream_with_timeout(
                &destination,
                StreamDescriptor::with_len(bytes.len() as u64),
                timeout,
            )
            .await?;
        let message = Message::method_call(
            destination,
            path,
            interface,
            member,
            to_body(&args(&writer.stream_ref()))?,
        );

        // Both halves at once, and the first failure wins: the callee is
        // reading the stream while it answers, so waiting for either one before
        // starting the other is a deadlock, not a slow path.
        let (reply, ()) = tokio::try_join!(self.call_raw(message, timeout), async {
            writer.write(bytes).await?;
            writer.finish().await.map(|_| ())
        })?;
        Ok(serde_json::from_value(reply)?)
    }

    /// Call a method and receive its successful serialized result through the
    /// bounded stream transport.
    ///
    /// An older peer that ignores the additive request flag may return an
    /// ordinary result; that remains accepted for rolling upgrades. Errors are
    /// always ordinary replies because they never contain the rejected value.
    pub async fn call_streaming<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl Serialize,
    ) -> Result<R> {
        self.call_streaming_with_timeout(
            destination,
            path,
            interface,
            member,
            args,
            DEFAULT_TIMEOUT,
        )
        .await
    }

    /// [`Connection::call_streaming`] with an explicit deadline for receiving
    /// the reply handle. The stream itself retains its bounded per-chunk and
    /// idle deadlines.
    pub async fn call_streaming_with_timeout<R: DeserializeOwned>(
        &self,
        destination: BusName,
        path: ObjectPath,
        interface: InterfaceName,
        member: MemberName,
        args: impl Serialize,
        timeout: Duration,
    ) -> Result<R> {
        let message =
            Message::streaming_call(destination, path, interface, member, to_body(&args)?);
        let reply = self.call_raw_message(message, timeout).await?;
        self.decode_streaming_reply(reply.body, reply.header.sender)
            .await
    }

    async fn decode_streaming_reply<R: DeserializeOwned>(
        &self,
        reply: Value,
        sender: Option<BusName>,
    ) -> Result<R> {
        let is_stream_reply = reply
            .as_object()
            .is_some_and(|object| object.contains_key("$tinybus_stream_reply"));
        let Ok(envelope) = serde_json::from_value::<StreamReplyEnvelope>(reply.clone()) else {
            if is_stream_reply {
                return Ok(serde_json::from_value(reply)?);
            }
            return Ok(serde_json::from_value(reply)?);
        };
        let limit = envelope
            .stream
            .len
            .ok_or_else(|| Error::protocol("a streamed reply must declare its length"))?;
        let mut reader = self
            .inner
            .streams
            .take_reader_from(&envelope.stream.id, &sender)?;
        let bytes = reader.read_to_end_capped(limit).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Take the reader for a stream a peer opened on this connection.
    ///
    /// Once only: a stream has one consumer, because two consumers would each
    /// get an arbitrary half of the payload.
    pub fn accept_stream(&self, stream: &StreamRef) -> Result<StreamReader> {
        self.inner.streams.take_reader(&stream.id)
    }

    /// Read a whole stream into memory, refusing to exceed
    /// [`StreamLimits::max_stream_len`].
    ///
    /// For a payload that is too big for a frame but not too big for memory.
    /// Anything else wants [`Connection::accept_stream`] and a loop over
    /// [`StreamReader::next_chunk`], which never holds more than one chunk.
    pub async fn read_stream(&self, stream: &StreamRef) -> Result<Vec<u8>> {
        let limit = self.stream_limits().max_stream_len;
        self.accept_stream(stream)?.read_to_end_capped(limit).await
    }

    /// Call one member of a peer's built-in stream interface.
    pub(crate) async fn call_stream_member(
        &self,
        destination: &BusName,
        member: &str,
        args: Value,
        timeout: Duration,
    ) -> Result<Value> {
        let message = Message::method_call(
            destination.clone(),
            ObjectPath::new(STREAM_PATH)?,
            InterfaceName::new(STREAM_INTERFACE)?,
            MemberName::new(member)?,
            args,
        );
        self.call_raw(message, timeout).await
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

    #[cfg(feature = "modules")]
    async fn call_bus_sensitive(&self, member: &str, args: Value) -> Result<Value> {
        let message = Message::sensitive_control_call(
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
pub(crate) fn to_body(value: &impl Serialize) -> Result<Value> {
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
        let delivery = if let Some(memory) = crate::transport::downcast::<
            crate::transport::memory::MemoryTransport,
        >(inner.transport.as_ref())
        {
            memory.recv_with_provenance().await
        } else {
            inner
                .transport
                .recv()
                .await
                .map(|message| message.map(|message| (message, inner.brokered_native)))
        };
        let (message, brokered) = match delivery {
            Ok(Some(delivery)) => delivery,
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
                tokio::spawn(handle_call(inner.clone(), message, brokered));
            }
        }
    }

    // Wake everyone still waiting rather than leaving them to time out one by
    // one: the link is gone and no reply is ever coming.
    inner.pending.lock().await.clear();
}

/// Run one inbound method call and send its reply.
async fn handle_call(inner: Arc<Inner>, message: Message, brokered: bool) {
    let header = message.header.clone();
    let panic_handler = inner
        .panic_handler
        .read()
        .expect("panic handler lock")
        .clone();
    let result = if header.stream_reply && header.confidential {
        Err(Error::protocol(
            "a confidential call cannot request a streamed reply",
        ))
    } else if let Some(panic_handler) = panic_handler {
        match CatchUnwind::new(dispatch(&inner, &header, message.body, brokered)).await {
            Ok(result) => result,
            Err(()) => Err(panic_handler()),
        }
    } else {
        dispatch(&inner, &header, message.body, brokered).await
    };

    let result = match result {
        Ok(value) if header.stream_reply => {
            if let Err(error) = send_streamed_reply(inner.clone(), &header, value).await {
                let _ = send_reply(&inner, Message::error_reply(&header, &error)).await;
            }
            return;
        }
        result => result,
    };

    let reply = match result {
        Ok(value) => Message::method_return(&header, value),
        Err(e) => Message::error_reply(&header, &e),
    };
    let _ = send_reply(&inner, reply).await;
}

async fn send_streamed_reply(inner: Arc<Inner>, header: &Header, value: Value) -> Result<()> {
    // Direct transports do not have a broker to stamp `sender`; they route all
    // frames to their sole peer, so the request destination is sufficient.
    let destination = header
        .sender
        .clone()
        .or_else(|| header.destination.clone())
        .ok_or_else(|| Error::protocol("a streamed reply needs a caller"))?;
    // Count before opening so the wire-level length is exact without retaining
    // a second result-sized JSON allocation in the service process.
    let mut counter = CountingWriter(0);
    serde_json::to_writer(&mut counter, &value)?;
    let connection = Connection {
        inner: inner.clone(),
        // This temporary handle borrows the live service connection. Dropping
        // it after the reply stream finishes must not close that connection.
        _close: Arc::new(CloseOnDrop(None)),
    };
    let mut writer = connection
        .open_stream_with_timeout(
            &destination,
            StreamDescriptor::with_len(counter.0).content_type("application/json"),
            DEFAULT_TIMEOUT,
        )
        .await?;
    let envelope = serde_json::to_value(StreamReplyEnvelope {
        stream: writer.stream_ref(),
    })?;
    if !send_reply(&inner, Message::method_return(header, envelope)).await {
        return Ok(());
    }
    let (sender, mut chunks) = mpsc::channel(1);
    let encode = tokio::task::spawn_blocking(move || -> io::Result<()> {
        let mut encoder = StreamReplyEncoder::new(sender);
        serde_json::to_writer(&mut encoder, &value)?;
        encoder.flush()
    });
    while let Some(chunk) = chunks.recv().await {
        if let Err(error) = writer.write(&chunk).await {
            tracing::debug!(error = %error, "streamed reply failed after its handle was sent");
            return Ok(());
        }
    }
    encode
        .await
        .map_err(|_| Error::transport("streamed reply encoder panicked"))??;
    if let Err(error) = writer.finish().await {
        tracing::debug!(error = %error, "streamed reply could not close cleanly");
    }
    Ok(())
}

async fn send_reply(inner: &Inner, mut reply: Message) -> bool {
    reply.header.serial = inner.serial.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = inner.outbox.send(reply).await {
        tracing::debug!(error = %e, "could not reply; the caller will time out");
        return false;
    }
    true
}

struct CatchUnwind<F> {
    future: Pin<Box<F>>,
}

impl<F> CatchUnwind<F> {
    fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::result::Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.get_mut().future.as_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

async fn dispatch(inner: &Inner, header: &Header, body: Value, brokered: bool) -> Result<Value> {
    // Streams are answered before the object tree is consulted, and without the
    // service having exported anything: bulk transfer is bus plumbing, and a
    // service that forgot to export it would be a service you cannot send a
    // file to. It also means a peer cannot shadow the stream interface by
    // exporting its own at that address.
    if StreamRegistry::handles(header) {
        return inner.streams.dispatch(header, body).await;
    }

    let (Some(path), Some(interface), Some(member)) =
        (&header.path, &header.interface, &header.member)
    else {
        return Err(Error::protocol("method call is missing an address"));
    };
    let context = if brokered {
        crate::CallContext::brokered(header.sender.as_ref())
    } else {
        crate::CallContext::default()
    };
    // Snapshot admission under the lock, then release it before user code:
    // callbacks can register/unserve interfaces while another call is parked.
    let target = inner.objects.read().await.lookup(path, interface)?;
    ObjectTree::invoke(
        target,
        interface,
        member,
        body,
        header.confidential,
        &context,
    )
    .await
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
