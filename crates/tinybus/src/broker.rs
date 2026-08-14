//! The broker: accepts peers, owns the routing table, answers the bus's own
//! interface.
//!
//! # Shape
//!
//! One accept loop, and per peer: a reader task and a writer task with a
//! bounded queue between them. The queue is what makes a slow peer *its own*
//! problem — a service that stops reading fills its queue, and senders to it
//! block or fail, but no other peer's traffic is delayed. A single shared
//! outbound path would let one wedged integration stall the bus, which is
//! precisely the failure mode we are moving integrations out of the kernel to
//! avoid.
//!
//! # What the broker does not do
//!
//! It does not start services, restart them, or know what any of them are for.
//! Activation — "call the wallet, and if nothing owns that name, launch it" —
//! is a real feature and a deliberate omission at this milestone; see
//! `ROADMAP.md`. The broker also does not inspect bodies. It reads the header,
//! routes, and forwards; a body is opaque bytes to it, which is what lets a
//! service pass credentials over the bus without them being logged, cached, or
//! parsed by a process that has no business seeing them.

#[cfg(feature = "modules")]
use std::sync::Weak;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::message::{Message, MessageKind};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::ports::{Listener, Transport};
use crate::router::{MatchRule, NameChange, Router};
use crate::version::PeerManifest;

/// How many messages may queue for one peer before senders wait.
pub const PEER_QUEUE_CAPACITY: usize = 256;

/// The bus itself.
///
/// Cheap to clone; every clone shares one routing table.
#[derive(Clone)]
pub struct Broker {
    router: Arc<Mutex<Router>>,
    id: String,
    // `Weak`, not `Arc`: the module host owns this broker, so a strong
    // reference back would form a cycle and leak both. Callers tolerate a
    // failed upgrade by falling back to the ordinary routing error.
    #[cfg(feature = "modules")]
    modules: Arc<Mutex<Option<Weak<dyn crate::module::host::ModuleControl>>>>,
}

impl Broker {
    /// Build a broker with an empty routing table.
    pub fn new() -> Self {
        Self {
            router: Arc::new(Mutex::new(Router::default())),
            #[cfg(feature = "modules")]
            modules: Arc::new(Mutex::new(None)),
            // The id changes per broker *process*, so a peer that reconnects
            // can tell "the bus restarted" (every name is gone, re-register)
            // from "my socket blipped" (state is intact).
            id: format!("tinybus-{}-{}", crate::VERSION, std::process::id()),
        }
    }

    /// This broker's id, as reported by the bus's `GetId`.
    pub fn id(&self) -> &str {
        &self.id
    }

    #[cfg(feature = "modules")]
    pub(crate) fn set_module_control(&self, control: Weak<dyn crate::module::host::ModuleControl>) {
        *self.modules.lock().expect("module control lock") = Some(control);
    }

    /// Serve until the listener stops accepting.
    ///
    /// A per-peer failure never ends this loop; only the listener itself
    /// failing does. One local process must not be able to take the bus down by
    /// misbehaving.
    pub async fn serve(&self, listener: impl Listener) -> Result<()> {
        tracing::info!(listener = %listener.describe(), id = %self.id, "tinybus is listening");
        loop {
            match listener.accept().await {
                Ok(Some(transport)) => {
                    self.attach(transport.into());
                }
                Ok(None) => {
                    tracing::info!("listener closed; broker stopping");
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(error = %e, "accept failed; broker stopping");
                    return Err(e);
                }
            }
        }
    }

    /// Spawn [`Broker::serve`] on the current runtime and return its handle.
    ///
    /// The handle is what an embedding process keeps to shut the bus down; the
    /// test suite uses it to run a whole bus inside one `#[tokio::test]`.
    pub fn spawn(&self, listener: impl Listener) -> tokio::task::JoinHandle<Result<()>> {
        let broker = self.clone();
        tokio::spawn(async move { broker.serve(listener).await })
    }

    /// Register one peer and start its reader and writer tasks.
    pub fn attach(&self, transport: Arc<dyn Transport>) -> BusName {
        let (outbox, inbox) = mpsc::channel(PEER_QUEUE_CAPACITY);
        let (id, unique) = self
            .router
            .lock()
            .expect("router lock is never held across a panic point")
            .attach(outbox);

        tracing::debug!(peer = %unique, transport = %transport.describe(), "peer attached");
        tokio::spawn(writer_task(transport.clone(), inbox));
        tokio::spawn(reader_task(self.clone(), transport, id, unique.clone()));
        unique
    }

    #[cfg(feature = "modules")]
    pub(crate) fn reserve_module_name(
        &self,
        unique: &BusName,
        name: BusName,
    ) -> Result<NameChange> {
        self.router
            .lock()
            .expect("router lock")
            .request_name_for_unique(unique, name)
    }

    /// Record that a loaded module's artifact matched the module allowlist.
    ///
    /// This is the *only* way a peer becomes eligible to receive a confidential
    /// message: the artifact was hashed against `modules.toml` before `dlopen`,
    /// and nothing reached across a transport to establish it.
    #[cfg(feature = "modules")]
    pub(crate) fn attest_module(&self, unique: &BusName, attestation: crate::attest::Attestation) {
        self.router
            .lock()
            .expect("router lock")
            .set_attestation_for_unique(unique, attestation);
    }

    /// Route one inbound message from peer `id`.
    async fn route(&self, from: u64, from_name: &BusName, mut message: Message) -> Result<()> {
        message.validate()?;
        // Overwrite rather than trust: a peer that could set `sender` could
        // impersonate the kernel to every service on the bus.
        message.header.sender = Some(from_name.clone());

        match message.header.kind {
            MessageKind::Signal => {
                let targets = self
                    .router
                    .lock()
                    .expect("router lock")
                    .subscribers(&message, from);
                for target in targets {
                    // A full or closed queue is that subscriber's problem;
                    // signals are best-effort and must not block the sender's
                    // reader task behind a peer that stopped reading.
                    let _ = target.try_send(message.clone());
                }
                Ok(())
            }
            _ => {
                let destination = message
                    .header
                    .destination
                    .clone()
                    .ok_or_else(|| Error::protocol("message has no destination"))?;

                if destination.as_str() == crate::BUS_NAME {
                    if message.header.confidential && message.header.kind == MessageKind::MethodCall
                    {
                        // The bus's own service is not a loaded, hash-verified
                        // module and can never be an attested recipient.
                        // `handle_bus_call` ends in `bus_method`, which
                        // deserializes the body — reaching that with a
                        // confidential payload would both break "the broker
                        // never parses a body" and risk a `BadArguments` built
                        // from secret material. Refuse before dispatch, not
                        // after.
                        return Err(Error::not_attested(
                            destination.clone(),
                            "the bus itself is never an attested recipient",
                        ));
                    }
                    return self.handle_bus_call(from, from_name, message).await;
                }

                // A confidential *call* may only go to a well-known name the
                // broker has verified an artifact for. A confidential *reply*
                // goes back to the unique name the broker itself minted for the
                // peer that made the call — that peer already chose to take
                // part in the exchange, and unique names are never reused, so
                // there is no one else the reply could reach.
                let confidential_call =
                    message.header.confidential && message.header.kind == MessageKind::MethodCall;
                let target = if confidential_call {
                    if destination.is_unique() {
                        // The broker knows *which connection* a unique name is,
                        // but not what binary is behind it. A sender that needs
                        // that answer has to address the well-known name.
                        Err(Error::not_attested(
                            destination.clone(),
                            "a confidential call must address a well-known name",
                        ))
                    } else {
                        self.router
                            .lock()
                            .expect("router lock")
                            .resolve_attested(&destination)
                    }
                } else {
                    self.router
                        .lock()
                        .expect("router lock")
                        .resolve(&destination)
                };
                #[cfg(feature = "modules")]
                let target = target.map_err(|error| {
                    // A refused attestation is the more specific answer and
                    // must survive: rewriting it as "the module is unavailable"
                    // would send an operator to fix the wrong thing.
                    if matches!(error, Error::NotAttested { .. }) {
                        return error;
                    }
                    let control = self
                        .modules
                        .lock()
                        .expect("module control lock")
                        .as_ref()
                        .and_then(Weak::upgrade);
                    control
                        .and_then(|control| control.unavailable_for(&destination))
                        .unwrap_or(error)
                });
                let target = target?;
                target
                    .send(message)
                    .await
                    .map_err(|_| Error::NameHasNoOwner(destination))
            }
        }
    }

    /// Answer a call addressed at the bus's own service.
    async fn handle_bus_call(
        &self,
        from: u64,
        from_name: &BusName,
        message: Message,
    ) -> Result<()> {
        let header = message.header.clone();
        let member = header
            .member
            .clone()
            .ok_or_else(|| Error::protocol("bus call has no member"))?;

        let (result, changes, module_states) = self
            .bus_method(from, from_name, &member, message.body)
            .await;

        let reply = match result {
            Ok(value) => Message::method_return(&header, value),
            Err(e) => Message::error_reply(&header, &e),
        };

        // Reply before announcing, so a service that requested a name is
        // registered by the time anyone reacts to hearing that it is.
        let outbox = self
            .router
            .lock()
            .expect("router lock")
            .resolve(from_name)?;
        let _ = outbox.send(reply).await;

        for change in changes {
            if change.old_owner != change.new_owner {
                self.announce_name_change(change).await;
            }
        }
        #[cfg(feature = "modules")]
        for module_state in module_states {
            self.announce_module_state(module_state).await;
        }
        #[cfg(not(feature = "modules"))]
        let _ = module_states;
        Ok(())
    }

    /// The bus's own interface. Module stop may await a blocking callback; the
    /// ordinary table still holds no lock across an await.
    async fn bus_method(
        &self,
        from: u64,
        from_name: &BusName,
        member: &MemberName,
        body: Value,
    ) -> (Result<Value>, Vec<NameChange>, Vec<Value>) {
        #[cfg(feature = "modules")]
        if let Some((result, module_states)) = self.module_method(member, body.clone()).await {
            return (result, Vec::new(), module_states);
        }

        let mut changes = Vec::new();
        let mut router = self.router.lock().expect("router lock");

        let result = (|| -> Result<Value> {
            match member.as_str() {
                "Hello" => Ok(serde_json::to_value(from_name)?),
                "GetId" => Ok(Value::String(self.id.clone())),
                "Ping" => Ok(Value::Null),
                "RequestName" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    changes.push(router.request_name(from, name)?);
                    Ok(Value::Bool(true))
                }
                "ReleaseName" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    changes.push(router.release_name(from, &name)?);
                    Ok(Value::Bool(true))
                }
                "ListNames" => Ok(serde_json::to_value(router.list_names())?),
                // Version negotiation. The broker stores and serves manifests
                // but never *enforces* them: refusing to route between two
                // peers the broker thinks are incompatible would make the bus
                // the arbiter of every contract on it, and would break the
                // moment a peer's declaration was merely stale. The peers
                // decide; the broker only makes the facts available.
                "Announce" => {
                    let (manifest,): (PeerManifest,) = parse_args(member, body)?;
                    router.set_manifest(from, manifest);
                    Ok(Value::Bool(true))
                }
                "GetManifest" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    Ok(serde_json::to_value(router.manifest_of(&name))?)
                }
                "ListPeers" => Ok(serde_json::to_value(router.peer_records())?),
                // What the broker verified about a prospective recipient, so a
                // sender can find out *before* it builds a message around a
                // secret rather than after the refusal. Returns null for an
                // unattested name; the answer is deliberately not a bare bool,
                // because an operator debugging this needs the hash that
                // matched.
                "GetAttestation" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    Ok(serde_json::to_value(router.attestation_of(&name))?)
                }
                "GetNameOwner" => {
                    let (name,): (BusName,) = parse_args(member, body)?;
                    Ok(serde_json::to_value(router.owner_of(&name))?)
                }
                "AddMatch" => {
                    let (rule,): (String,) = parse_args(member, body)?;
                    router.add_match(from, MatchRule::parse(&rule)?);
                    Ok(Value::Null)
                }
                "RemoveMatch" => {
                    let (rule,): (String,) = parse_args(member, body)?;
                    router.remove_match(from, &MatchRule::parse(&rule)?);
                    Ok(Value::Null)
                }
                other => Err(Error::UnknownMethod {
                    interface: InterfaceName::new(crate::BUS_INTERFACE)
                        .expect("the bus interface constant is valid"),
                    member: MemberName::new(other).unwrap_or_else(|_| member.clone()),
                }),
            }
        })();

        (result, changes, Vec::new())
    }

    #[cfg(feature = "modules")]
    async fn module_method(
        &self,
        member: &MemberName,
        body: Value,
    ) -> Option<(Result<Value>, Vec<Value>)> {
        use std::path::PathBuf;
        use std::time::Duration;

        enum ModuleMember {
            List,
            Get,
            GetManifest,
            Load,
            LoadGithub,
            Stop,
            Enable,
            Rescan,
        }
        let operation = match member.as_str() {
            "ListModules" => ModuleMember::List,
            "GetModule" => ModuleMember::Get,
            "GetModuleManifest" => ModuleMember::GetManifest,
            "LoadModule" => ModuleMember::Load,
            "LoadGithubModule" => ModuleMember::LoadGithub,
            "StopModule" => ModuleMember::Stop,
            "EnableModule" => ModuleMember::Enable,
            "RescanModules" => ModuleMember::Rescan,
            _ => return None,
        };
        let control = self
            .modules
            .lock()
            .expect("module control lock")
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::failed("module host is not installed"));
        let control = match control {
            Ok(control) => control,
            Err(error) => return Some((Err(error), Vec::new())),
        };
        if matches!(operation, ModuleMember::Stop) {
            let parsed = parse_args::<(String, u64)>(member, body);
            let result = match parsed {
                Ok((name, deadline_ms)) => control
                    .stop(&name, Duration::from_millis(deadline_ms))
                    .await
                    .and_then(|info| serde_json::to_value(info).map_err(Error::from)),
                Err(error) => Err(error),
            };
            return Some((result, Vec::new()));
        }

        let outcome = (|| -> Result<(Value, Vec<Value>)> {
            match operation {
                ModuleMember::List => Ok((serde_json::to_value(control.list())?, Vec::new())),
                ModuleMember::Get => {
                    let (name,): (String,) = parse_args(member, body)?;
                    Ok((
                        serde_json::to_value(
                            control
                                .list()
                                .into_iter()
                                .find(|module| module.name == name),
                        )?,
                        Vec::new(),
                    ))
                }
                ModuleMember::GetManifest => {
                    let (name,): (String,) = parse_args(member, body)?;
                    Ok((
                        serde_json::to_value(
                            control
                                .list()
                                .into_iter()
                                .find(|module| module.name == name)
                                .map(|module| module.manifest),
                        )?,
                        Vec::new(),
                    ))
                }
                ModuleMember::Load => {
                    let arguments = body.as_array().ok_or_else(|| {
                        Error::bad_arguments(member.clone(), "expected a positional array")
                    })?;
                    let path: String = arguments
                        .first()
                        .cloned()
                        .ok_or_else(|| Error::bad_arguments(member.clone(), "missing path"))
                        .and_then(|value| {
                            serde_json::from_value(value)
                                .map_err(|error| Error::bad_arguments(member.clone(), error))
                        })?;
                    let config = arguments
                        .get(1)
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    if arguments.len() > 2 {
                        return Err(Error::bad_arguments(
                            member.clone(),
                            "expected path and optional configuration",
                        ));
                    }
                    let (info, transition) = control.load(PathBuf::from(path), config)?;
                    Ok((
                        serde_json::to_value(info)?,
                        module_state_body(transition).into_iter().collect(),
                    ))
                }
                ModuleMember::LoadGithub => {
                    let (url, asset, sha256, config): (String, String, String, Value) =
                        parse_args(member, body)?;
                    let (info, transition) = control.load_github(url, asset, sha256, config)?;
                    Ok((
                        serde_json::to_value(info)?,
                        module_state_body(transition).into_iter().collect(),
                    ))
                }
                ModuleMember::Stop => Err(Error::failed("module stop dispatch failed")),
                ModuleMember::Enable => {
                    let (name, enabled): (String, bool) = parse_args(member, body)?;
                    let (info, transition) = control.enable(&name, enabled)?;
                    Ok((
                        serde_json::to_value(info)?,
                        module_state_body(transition).into_iter().collect(),
                    ))
                }
                ModuleMember::Rescan => {
                    let arguments = body.as_array().ok_or_else(|| {
                        Error::bad_arguments(member.clone(), "expected a positional array")
                    })?;
                    let paths = arguments
                        .first()
                        .cloned()
                        .map(serde_json::from_value::<Vec<PathBuf>>)
                        .transpose()
                        .map_err(|error| Error::bad_arguments(member.clone(), error))?
                        .unwrap_or_default();
                    let dry_run = arguments.get(1).and_then(Value::as_bool).unwrap_or(false);
                    if arguments.len() > 2 {
                        return Err(Error::bad_arguments(
                            member.clone(),
                            "expected optional paths and dry-run flag",
                        ));
                    }
                    let (infos, transitions) = control.rescan(paths, dry_run)?;
                    Ok((
                        serde_json::to_value(infos)?,
                        transitions
                            .into_iter()
                            .filter_map(|transition| module_state_body(Some(transition)))
                            .collect(),
                    ))
                }
            }
        })();
        Some(match outcome {
            Ok((value, states)) => (Ok(value), states),
            Err(error) => (Err(error), Vec::new()),
        })
    }

    /// Broadcast `NameOwnerChanged`.
    ///
    /// This is how the kernel learns an integration died — without it, the only
    /// signal would be a call timing out thirty seconds later, by which point a
    /// user has been staring at a spinner. Subscribers still have to have asked
    /// for it; the broker does not push it at peers that did not.
    pub(crate) async fn announce_name_change(&self, change: NameChange) {
        self.broadcast_bus_signal(
            "NameOwnerChanged",
            serde_json::json!([change.name, change.old_owner, change.new_owner]),
        )
        .await;
    }

    #[cfg(feature = "modules")]
    pub(crate) async fn announce_module_state(&self, body: Value) {
        self.broadcast_bus_signal("ModuleStateChanged", body).await;
    }

    async fn broadcast_bus_signal(&self, member: &str, body: Value) {
        let signal = Message {
            header: crate::message::Header {
                kind: MessageKind::Signal,
                serial: 0,
                reply_serial: None,
                sender: Some(
                    BusName::new(crate::BUS_NAME).expect("the bus name constant is valid"),
                ),
                destination: None,
                path: Some(
                    ObjectPath::new(crate::BUS_PATH).expect("the bus path constant is valid"),
                ),
                interface: Some(
                    InterfaceName::new(crate::BUS_INTERFACE)
                        .expect("the bus interface constant is valid"),
                ),
                member: Some(MemberName::new(member).expect("literal is a valid member")),
                error_name: None,
                // The bus's own announcements are broadcasts by construction.
                confidential: false,
            },
            body,
        };
        let targets = self
            .router
            .lock()
            .expect("router lock")
            .broadcast_targets(&signal);
        for target in targets {
            let _ = target.try_send(signal.clone());
        }
    }
}

#[cfg(feature = "modules")]
fn module_state_body(transition: Option<crate::module::host::ModuleTransition>) -> Option<Value> {
    use crate::module::host::{state_detail, state_name};

    transition.map(|(module, old, new)| {
        serde_json::json!([
            module,
            state_name(&old),
            state_name(&new),
            state_detail(&new)
        ])
    })
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

/// Deserialize a positional argument array, naming the member on failure.
fn parse_args<T: serde::de::DeserializeOwned>(member: &MemberName, body: Value) -> Result<T> {
    serde_json::from_value(body).map_err(|e| Error::bad_arguments(member.clone(), e))
}

/// Drain one peer's queue onto its transport.
async fn writer_task(transport: Arc<dyn Transport>, mut inbox: mpsc::Receiver<Message>) {
    while let Some(message) = inbox.recv().await {
        if let Err(e) = transport.send(message).await {
            tracing::debug!(error = %e, "peer write failed; dropping the peer");
            break;
        }
    }
}

/// Read one peer's messages until it disconnects, then release its names.
async fn reader_task(broker: Broker, transport: Arc<dyn Transport>, id: u64, name: BusName) {
    loop {
        match transport.recv().await {
            Ok(Some(message)) => {
                let header = message.header.clone();
                if let Err(e) = broker.route(id, &name, message).await {
                    tracing::debug!(peer = %name, error = %e, "routing failed");
                    // A call that cannot be routed must produce an error reply,
                    // or the caller waits out its full timeout for a message
                    // that was never going anywhere.
                    if header.kind == MessageKind::MethodCall {
                        let mut reply = Message::error_reply(&header, &e);
                        reply.header.sender =
                            Some(BusName::new(crate::BUS_NAME).expect("bus name is valid"));
                        let _ = transport.send(reply).await;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(peer = %name, error = %e, "peer read failed");
                break;
            }
        }
    }

    tracing::debug!(peer = %name, "peer detached");
    let changes = broker.router.lock().expect("router lock").detach(id);
    for change in changes {
        broker.announce_name_change(change).await;
    }
    #[cfg(feature = "modules")]
    {
        let control = broker
            .modules
            .lock()
            .expect("module control lock")
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some((module, old, new)) = control.and_then(|control| control.peer_detached(&name)) {
            broker
                .announce_module_state(serde_json::json!([
                    module,
                    crate::module::host::state_name(&old),
                    crate::module::host::state_name(&new),
                    crate::module::host::state_detail(&new)
                ]))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::ports::Listener;
    use crate::service::Interface;
    use crate::transport::memory::MemoryBus;
    use async_trait::async_trait;
    use std::time::Duration;

    struct Voice;

    #[async_trait]
    impl Interface for Voice {
        fn name(&self) -> InterfaceName {
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Transcribe").unwrap()]
        }

        async fn call(&self, _member: &MemberName, args: Value) -> Result<Value> {
            let (path,): (String,) = serde_json::from_value(args)?;
            Ok(Value::String(format!("transcript of {path}")))
        }
    }

    const VOICE_NAME: &str = "ai.tinyhumans.openhuman.Voice";
    const VOICE_PATH: &str = "/ai/tinyhumans/openhuman/Voice";

    /// A broker, a registered service, and a client — all in one process.
    async fn bus() -> (MemoryBus, Connection, Connection) {
        let bus = MemoryBus::new();
        Broker::new().spawn(bus.clone());

        let service = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        service
            .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
            .await
            .unwrap();
        service.request_name(VOICE_NAME).await.unwrap();

        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        (bus, service, client)
    }

    #[tokio::test]
    async fn a_client_calls_a_service_by_its_well_known_name() {
        let (_bus, _service, client) = bus().await;
        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
        let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await.unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }

    #[tokio::test]
    async fn hello_assigns_distinct_unique_names() {
        let (_bus, service, client) = bus().await;
        let a = service.unique_name().unwrap();
        let b = client.unique_name().unwrap();
        assert!(a.is_unique() && b.is_unique());
        assert_ne!(a, b);
    }

    struct ClosedListener;

    #[async_trait]
    impl Listener for ClosedListener {
        async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
            Ok(None)
        }

        fn describe(&self) -> String {
            "closed-test-listener".into()
        }
    }

    struct FailingListener;

    #[async_trait]
    impl Listener for FailingListener {
        async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
            Err(Error::transport("accept failed"))
        }
    }

    #[tokio::test]
    async fn serving_stops_cleanly_on_listener_shutdown_and_reports_listener_errors() {
        let broker = Broker::default();
        assert!(broker.id().starts_with("tinybus-"));
        broker.serve(ClosedListener).await.unwrap();
        assert!(broker.serve(FailingListener).await.is_err());
    }

    #[tokio::test]
    async fn malformed_bus_arguments_are_replied_to_without_hanging() {
        let (_bus, _service, client) = bus().await;
        let invalid = Message::method_call(
            BusName::new(crate::BUS_NAME).unwrap(),
            ObjectPath::new(crate::BUS_PATH).unwrap(),
            InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
            MemberName::new("AddMatch").unwrap(),
            serde_json::json!([42]),
        );
        let error = client
            .call_raw(invalid, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(
            error.wire_name(),
            "ai.tinyhumans.tinybus.Error.BadArguments"
        );
    }

    #[tokio::test]
    async fn calling_an_integration_that_is_not_running_fails_fast_and_names_it() {
        let (_bus, _service, client) = bus().await;
        let absent = client
            .proxy(
                "ai.tinyhumans.openhuman.Wallet",
                "/ai/tinyhumans/openhuman/Wallet",
                "ai.tinyhumans.openhuman.Wallet",
            )
            .unwrap()
            .with_timeout(Duration::from_secs(5));

        assert!(!absent.is_available().await.unwrap());
        let err = absent.call::<Value>("Sign", ("0xdead",)).await.unwrap_err();
        // Fast, and specific: not a timeout, and it names the missing service.
        assert!(err.to_string().contains("Wallet"), "{err}");
        assert!(!matches!(err, Error::Timeout { .. }), "{err}");
    }

    #[tokio::test]
    async fn a_second_claimant_for_a_name_is_refused() {
        let (bus, _service, _client) = bus().await;
        let impostor = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let err = impostor.request_name(VOICE_NAME).await.unwrap_err();
        assert!(err.to_string().contains("already owned"), "{err}");
    }

    #[tokio::test]
    async fn the_bus_reserves_its_own_name() {
        let (bus, _service, _client) = bus().await;
        let peer = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        assert!(peer.request_name(crate::BUS_NAME).await.is_err());
    }

    #[tokio::test]
    async fn list_names_shows_the_well_known_name_and_the_unique_ones() {
        let (_bus, _service, client) = bus().await;
        let names = client.list_names().await.unwrap();
        assert!(names.iter().any(|n| n.as_str() == VOICE_NAME));
        assert!(names.iter().filter(|n| n.is_unique()).count() >= 2);
    }

    #[tokio::test]
    async fn a_signal_reaches_a_subscriber_and_skips_a_non_subscriber() {
        let (bus, service, client) = bus().await;
        let mut subscribed = client
            .add_match(
                MatchRule::new()
                    .signals()
                    .interface(InterfaceName::new(VOICE_NAME).unwrap()),
            )
            .await
            .unwrap();
        let bystander = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let mut ignored = bystander.signals();

        service
            .emit(
                ObjectPath::new(VOICE_PATH).unwrap(),
                InterfaceName::new(VOICE_NAME).unwrap(),
                MemberName::new("TranscriptReady").unwrap(),
                ("clip-1",),
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), subscribed.recv())
            .await
            .expect("the subscriber is woken")
            .unwrap();
        assert_eq!(received.header.member.unwrap().as_str(), "TranscriptReady");
        // The bystander added no match, so the broker never woke it.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ignored.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn the_sender_is_stamped_by_the_broker_and_cannot_be_forged() {
        let (_bus, service, client) = bus().await;
        let mut signals = client.add_match(MatchRule::new().signals()).await.unwrap();

        // Claim to be the bus itself.
        let mut forged = Message::signal(
            ObjectPath::new(VOICE_PATH).unwrap(),
            InterfaceName::new(VOICE_NAME).unwrap(),
            MemberName::new("TranscriptReady").unwrap(),
            Value::Null,
        );
        forged.header.sender = Some(BusName::new(crate::BUS_NAME).unwrap());
        service.send(forged).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .expect("delivered")
            .unwrap();
        assert_eq!(received.header.sender, service.unique_name());
    }

    #[tokio::test]
    async fn a_service_dying_releases_its_name_and_announces_it() {
        let (bus, service, client) = bus().await;
        let mut signals = client
            .add_match(
                MatchRule::new()
                    .signals()
                    .member(MemberName::new("NameOwnerChanged").unwrap()),
            )
            .await
            .unwrap();

        drop(service);

        let announcement = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .expect("the kernel is told")
            .unwrap();
        let (name, _old, new): (BusName, Option<BusName>, Option<BusName>) =
            serde_json::from_value(announcement.body).unwrap();
        assert_eq!(name.as_str(), VOICE_NAME);
        assert!(new.is_none(), "the name has no owner now");

        // And the name is genuinely free again — a restarted service can claim it.
        let restarted = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        restarted.request_name(VOICE_NAME).await.unwrap();
    }

    #[tokio::test]
    async fn an_unknown_bus_method_is_an_error_reply_rather_than_a_hang() {
        let (_bus, _service, client) = bus().await;
        let bus_proxy = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        let err = bus_proxy.call::<Value>("Enumerate", ()).await.unwrap_err();
        assert_eq!(err.wire_name(), Error::UNKNOWN_METHOD);
    }

    #[tokio::test]
    async fn a_peer_announces_and_another_reads_the_manifest_back() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        let manifest = PeerManifest::new("voice-service")
            .version(Version::new(0, 4, 2))
            .provides(InterfaceVersion::provided(
                InterfaceName::new(VOICE_NAME).unwrap(),
                Version::new(2, 3, 0),
            ));
        service.announce(&manifest).await.unwrap();

        // Readable by well-known name, which is what a caller actually holds.
        let seen = client
            .manifest_of(VOICE_NAME)
            .await
            .unwrap()
            .expect("announced");
        assert_eq!(seen, manifest);

        let peers = client.peers().await.unwrap();
        assert_eq!(peers.len(), 1, "only the peer that announced");
        assert_eq!(peers[0].names, vec![BusName::new(VOICE_NAME).unwrap()]);
    }

    #[tokio::test]
    async fn require_passes_on_a_compatible_peer_and_names_both_versions_otherwise() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        let interface = InterfaceName::new(VOICE_NAME).unwrap();
        service
            .announce(
                &PeerManifest::new("voice-service").provides(InterfaceVersion::provided(
                    interface.clone(),
                    Version::new(2, 3, 0),
                )),
            )
            .await
            .unwrap();

        // A caller written against 2.1 is served by a 2.3 provider.
        let ok = PeerManifest::new("openhuman").consumes(InterfaceVersion::consumed(
            interface.clone(),
            Version::new(2, 1, 0),
        ));
        client.require(VOICE_NAME, VOICE_NAME, &ok).await.unwrap();

        // A caller that needs 3.x is not, and the error says so with numbers.
        let stale = PeerManifest::new("openhuman")
            .consumes(InterfaceVersion::consumed(interface, Version::new(3, 0, 0)));
        let err = client
            .require(VOICE_NAME, VOICE_NAME, &stale)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IncompatibleVersion { .. }), "{err}");
        assert!(err.to_string().contains("2.3.0"), "{err}");
        assert!(err.to_string().contains("3.0.0"), "{err}");
    }

    #[tokio::test]
    async fn a_peer_that_never_announced_is_still_callable() {
        // Manifests roll out service by service; a peer without one must not
        // be locked off the bus by peers that have adopted them.
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, _service, client) = bus().await;
        let local = PeerManifest::new("openhuman").consumes(InterfaceVersion::consumed(
            InterfaceName::new(VOICE_NAME).unwrap(),
            Version::new(9, 0, 0),
        ));
        client
            .require(VOICE_NAME, VOICE_NAME, &local)
            .await
            .unwrap();

        let transcript: String = client
            .proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME)
            .unwrap()
            .call("Transcribe", ("/tmp/clip.wav",))
            .await
            .unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }

    #[tokio::test]
    async fn a_dead_peers_manifest_goes_with_it() {
        use crate::version::{InterfaceVersion, PeerManifest, Version};

        let (_bus, service, client) = bus().await;
        service
            .announce(
                &PeerManifest::new("voice-service").provides(InterfaceVersion::provided(
                    InterfaceName::new(VOICE_NAME).unwrap(),
                    Version::new(2, 3, 0),
                )),
            )
            .await
            .unwrap();
        assert!(client.manifest_of(VOICE_NAME).await.unwrap().is_some());

        drop(service);
        // Wait for the detach to land, then the name — and its manifest — are gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while client.manifest_of(VOICE_NAME).await.unwrap().is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "manifest outlived its peer"
            );
            tokio::task::yield_now().await;
        }
        assert!(client.peers().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_bus_answers_ping_and_reports_a_stable_id() {
        let (_bus, _service, client) = bus().await;
        let bus_proxy = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap();
        bus_proxy.call::<Value>("Ping", ()).await.unwrap();
        let first: String = bus_proxy.call("GetId", ()).await.unwrap();
        let second: String = bus_proxy.call("GetId", ()).await.unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with("tinybus-"), "{first}");
    }

    #[tokio::test]
    async fn one_wedged_peer_does_not_stall_another_peers_traffic() {
        // The load-bearing property of the whole design: a service that stops
        // reading is isolated behind its own bounded queue.
        let (bus, _service, client) = bus().await;

        // A peer that subscribes to everything and then never reads again —
        // an integration blocked inside a third-party library, in other words.
        // Driven at the transport level because a `Connection` would read the
        // replies, which is the thing this peer is refusing to do.
        let sulker = bus.connect().await.unwrap();
        let mut subscribe = Message::method_call(
            BusName::new(crate::BUS_NAME).unwrap(),
            ObjectPath::new(crate::BUS_PATH).unwrap(),
            InterfaceName::new(crate::BUS_INTERFACE).unwrap(),
            MemberName::new("AddMatch").unwrap(),
            serde_json::json!(["type=signal"]),
        );
        subscribe.header.serial = 1;
        sulker.send(subscribe).await.unwrap();

        for _ in 0..(PEER_QUEUE_CAPACITY * 2) {
            client
                .emit(
                    ObjectPath::new(VOICE_PATH).unwrap(),
                    InterfaceName::new(VOICE_NAME).unwrap(),
                    MemberName::new("Noise").unwrap(),
                    (),
                )
                .await
                .unwrap();
        }

        let voice = client
            .proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME)
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await.unwrap();
        assert_eq!(transcript, "transcript of /tmp/clip.wav");
    }

    /// A bus with a service that the host has attested, as a module load would.
    ///
    /// `attest_module` is the same call the module host makes after hashing an
    /// artifact against `modules.toml`; driving it directly keeps the test on
    /// the in-memory transport instead of requiring a built `cdylib` on disk.
    #[cfg(feature = "modules")]
    async fn attested_bus() -> (MemoryBus, Broker, Connection, Connection) {
        let bus = MemoryBus::new();
        let broker = Broker::new();
        broker.spawn(bus.clone());

        let service = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        service
            .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
            .await
            .unwrap();
        service.request_name(VOICE_NAME).await.unwrap();
        broker.attest_module(
            &service.unique_name().unwrap(),
            crate::attest::Attestation {
                name: BusName::new(VOICE_NAME).unwrap(),
                sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
            },
        );

        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        (bus, broker, service, client)
    }

    #[tokio::test]
    async fn a_confidential_call_to_an_unattested_recipient_is_refused() {
        // The default bus has no trust store, so nothing is attested — and the
        // service is reachable by an ordinary call, which is what makes the
        // refusal meaningful rather than incidental.
        let (_bus, _service, client) = bus().await;
        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
        assert!(
            voice
                .call::<String>("Transcribe", ("/tmp/a.wav",))
                .await
                .is_ok()
        );

        let error = voice
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err();
        assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
        assert_eq!(voice.attestation().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_confidential_call_may_not_address_a_unique_name() {
        // The broker knows which connection `:1.n` is, but not what binary is
        // behind it, so it cannot answer the question the sender is asking.
        let (_bus, service, client) = bus().await;
        let unique = service.unique_name().unwrap();
        let proxy = client
            .proxy(unique.as_str(), VOICE_PATH, VOICE_NAME)
            .unwrap();
        let error = proxy
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err();
        assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
    }

    #[tokio::test]
    async fn a_confidential_call_to_the_bus_itself_is_refused_before_its_body_is_parsed() {
        // The bus's own service is never a loaded, hash-verified module, so it
        // can never be an attested recipient. Before the fix this dispatch
        // reached `handle_bus_call` -> `bus_method` -> `parse_args`, which
        // deserializes the body — breaking "the broker never parses a body"
        // for exactly the messages that must never be parsed. An ordinary
        // (non-confidential) bus call must keep working.
        let (_bus, _service, client) = bus().await;
        let bus_proxy = client
            .proxy(crate::BUS_NAME, crate::BUS_PATH, crate::BUS_INTERFACE)
            .unwrap();
        let error = bus_proxy
            .call_confidential::<String>("GetId", ())
            .await
            .unwrap_err();
        assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
        let id: String = bus_proxy.call("GetId", ()).await.unwrap();
        assert!(id.starts_with("tinybus-"), "{id}");
    }

    #[cfg(feature = "modules")]
    #[tokio::test]
    async fn a_confidential_call_reaches_a_module_the_host_verified() {
        let (_bus, _broker, _service, client) = attested_bus().await;
        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();

        let attestation = voice.attestation().await.unwrap().expect("attested");
        assert_eq!(attestation.name.as_str(), VOICE_NAME);

        let transcript: String = voice
            .call_confidential("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap();
        assert_eq!(transcript, "transcript of /tmp/secret.wav");
    }

    #[cfg(feature = "modules")]
    #[tokio::test]
    async fn a_name_handed_on_to_another_peer_does_not_hand_on_its_attestation() {
        // The property under test is whether trust is attached to the *name* or
        // to the *peer*. If it were the name, any process that grabbed it after
        // the real module released it would inherit the right to be handed
        // secrets without a single byte having been hashed.
        let (bus, _broker, service, client) = attested_bus().await;
        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
        assert!(voice.attestation().await.unwrap().is_some());

        service.release_name(VOICE_NAME).await.unwrap();
        let impostor = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        impostor
            .serve_at(ObjectPath::new(VOICE_PATH).unwrap(), Voice)
            .await
            .unwrap();
        impostor.request_name(VOICE_NAME).await.unwrap();

        // The impostor owns the name and answers ordinary calls...
        assert!(
            voice
                .call::<String>("Transcribe", ("/tmp/a.wav",))
                .await
                .is_ok()
        );
        // ...and is refused the secret, because nothing verified its artifact.
        assert_eq!(voice.attestation().await.unwrap(), None);
        let error = voice
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await
            .unwrap_err();
        assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
    }

    #[tokio::test]
    async fn a_confidential_body_is_never_fanned_out_to_a_monitor() {
        let (_bus, _service, client) = bus().await;
        let watcher = Connection::connect(_bus.connect().await.unwrap())
            .await
            .unwrap();
        // The broadest possible subscription: if anything could see a secret,
        // this would.
        let mut seen = watcher.add_match(MatchRule::new()).await.unwrap();

        let voice = client.proxy(VOICE_NAME, VOICE_PATH, VOICE_NAME).unwrap();
        let _ = voice
            .call_confidential::<String>("Transcribe", ("/tmp/secret.wav",))
            .await;

        // Nothing arrives at all, rather than something arriving redacted.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), seen.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn get_attestation_answers_for_a_name_nobody_owns() {
        let (_bus, _service, client) = bus().await;
        let missing = BusName::new("ai.tinyhumans.openhuman.Absent").unwrap();
        assert_eq!(client.attestation(missing).await.unwrap(), None);
    }

    #[tokio::test]
    async fn the_stream_interface_gets_no_exemption_from_attestation() {
        // Bulk payloads travel as `Stream.Write` calls rather than in a body,
        // which makes the stream interface the one place a second delivery path
        // could have grown. It did not: a chunk is an ordinary method call and
        // `route` reaches it through the same check as everything else. Pinned
        // as a test because the cost of the stream path ever being special-cased
        // is every secret on the bus, and nothing else would notice.
        let (_bus, service, client) = bus().await;
        let stream = client
            .proxy(
                VOICE_NAME,
                crate::stream::STREAM_PATH,
                crate::stream::STREAM_INTERFACE,
            )
            .unwrap();

        // The peer is reachable on the stream interface by an ordinary call —
        // it answers `UnknownStream`, not `NotAttested` — so the refusal below
        // is the attestation check firing and not the name failing to resolve.
        let ordinary = stream
            .call::<Value>("Abort", ("no-such-stream",))
            .await
            .unwrap_err();
        assert_eq!(
            ordinary.wire_name(),
            "ai.tinyhumans.tinybus.Error.UnknownStream"
        );

        let refused = stream
            .call_confidential::<Value>("Abort", ("no-such-stream",))
            .await
            .unwrap_err();
        assert_eq!(refused.wire_name(), Error::NOT_ATTESTED);
        drop(service);
    }
}
