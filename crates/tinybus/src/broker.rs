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

        #[cfg(feature = "modules")]
        if let Some(module) = crate::transport::downcast::<crate::module::transport::ModuleTransport>(
            transport.as_ref(),
        ) {
            module.admit_broker();
        }
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
                        return Err(Error::not_attested(
                            destination.clone(),
                            "the bus itself is never an attested recipient",
                        ));
                    }
                    if message.header.sensitive
                        && !message.header.member.as_ref().is_some_and(|member| {
                            matches!(
                                member.as_str(),
                                "LoadModule" | "LoadGithubModule" | "ReinitializeModule"
                            )
                        })
                    {
                        return Err(Error::protocol(
                            "sensitive control bodies are reserved for module configuration",
                        ));
                    }
                    return self.handle_bus_call(from, from_name, message).await;
                }

                if message.header.sensitive {
                    return Err(Error::protocol(
                        "sensitive control calls must address the bus module host",
                    ));
                }

                // Both destination forms resolve and check admission under
                // one router lock. Unique names pin the admitted artifact;
                // well-known names require an exact attested identity.
                // Confidential replies still return to the caller's unique
                // connection, which chose to participate in the exchange.
                let confidential_call =
                    message.header.confidential && message.header.kind == MessageKind::MethodCall;
                let target = if confidential_call {
                    self.router
                        .lock()
                        .expect("router lock")
                        .resolve_attested(&destination)
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
                // A stopped, disabled, failed or faulted module can keep its
                // unique connection and well-known name briefly after the host
                // settles its state: the peer detaches only once its reader sees the
                // closed transport. A call routed into that window would sit
                // in a queue nobody drains and the caller would wait out its
                // whole deadline, so answer from the host's state instead.
                #[cfg(feature = "modules")]
                if message.header.kind == MessageKind::MethodCall {
                    let control = self
                        .modules
                        .lock()
                        .expect("module control lock")
                        .as_ref()
                        .and_then(Weak::upgrade);
                    if let Some(error) =
                        control.and_then(|control| control.unavailable_for(&destination))
                    {
                        tracing::debug!(
                            destination = %destination,
                            "refusing call to a module that is not accepting calls"
                        );
                        return Err(error);
                    }
                }
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
            Reinitialize,
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
            "ReinitializeModule" => ModuleMember::Reinitialize,
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
        if matches!(operation, ModuleMember::Reinitialize) {
            let parsed = parse_args::<(String, Value)>(member, body);
            let result = match parsed {
                Ok((name, config)) => control
                    .reinitialize(&name, config)
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
                ModuleMember::Reinitialize => {
                    Err(Error::failed("module reinitialize dispatch failed"))
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
                sensitive: false,
                stream_reply: false,
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
        let result = if let Some(memory) = crate::transport::downcast::<
            crate::transport::memory::MemoryTransport,
        >(transport.as_ref())
        {
            memory.send_brokered(message).await
        } else {
            transport.send(message).await
        };
        if let Err(e) = result {
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
#[path = "broker_tests.rs"]
mod tests;
