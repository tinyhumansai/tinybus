//! The routing table and the match rules that drive signal delivery.
//!
//! The router is deliberately dumb and deliberately synchronous. It answers two
//! questions — "which peer owns this name" and "which peers want this signal" —
//! and it answers them without awaiting anything, so the lock it lives under is
//! never held across a suspend point. Callers take the answer (a set of cloned
//! channel senders), drop the lock, and only then do the sending. Getting this
//! wrong is the classic broker deadlock: peer A's slow queue holds the routing
//! lock while peer B is trying to disconnect.
//!
//! # Match rules
//!
//! A signal has no destination, so a peer states what it wants with a rule:
//!
//! ```text
//! type=signal,interface=ai.tinyhumans.openhuman.Mail,member=Received
//! type=signal,path_namespace=/ai/tinyhumans/openhuman/Mail
//! sender=ai.tinyhumans.tinybus.Bus,member=NameOwnerChanged
//! ```
//!
//! Unset fields match anything; set fields must all match. Filtering happens at
//! the *broker*, not at the client, because the whole point of the exercise is
//! that the kernel does not pay for integrations it is not using — waking it up
//! to discard a signal it never asked for is exactly that cost in miniature.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::attest::Attestation;
use crate::error::{Error, Result};
use crate::message::{Message, MessageKind};
use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
use crate::version::{PeerManifest, PeerRecord};

/// A subscription filter. Every set field must match; unset fields match all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MatchRule {
    /// Restrict to one message kind. Almost always [`MessageKind::Signal`].
    pub kind: Option<MessageKind>,
    /// Restrict to one sender, by unique or well-known name.
    pub sender: Option<BusName>,
    /// Restrict to one interface.
    pub interface: Option<InterfaceName>,
    /// Restrict to one member.
    pub member: Option<MemberName>,
    /// Restrict to one exact object path.
    pub path: Option<ObjectPath>,
    /// Restrict to a path and everything beneath it.
    ///
    /// Separate from `path` rather than a flag on it, because "this mailbox"
    /// and "every mailbox" are different subscriptions and conflating them is
    /// how a client ends up quietly receiving another account's traffic.
    pub path_namespace: Option<ObjectPath>,
}

impl MatchRule {
    /// An empty rule, which matches every signal. Build it up with the setters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict to signals.
    pub fn signals(mut self) -> Self {
        self.kind = Some(MessageKind::Signal);
        self
    }

    /// Restrict to one interface.
    pub fn interface(mut self, interface: InterfaceName) -> Self {
        self.interface = Some(interface);
        self
    }

    /// Restrict to one member.
    pub fn member(mut self, member: MemberName) -> Self {
        self.member = Some(member);
        self
    }

    /// Restrict to one sender.
    pub fn sender(mut self, sender: BusName) -> Self {
        self.sender = Some(sender);
        self
    }

    /// Restrict to a path and its subtree.
    pub fn path_namespace(mut self, path: ObjectPath) -> Self {
        self.path_namespace = Some(path);
        self
    }

    /// Whether `message` satisfies every set field.
    pub fn matches(&self, message: &Message) -> bool {
        let h = &message.header;
        if self.kind.is_some_and(|kind| kind != h.kind) {
            return false;
        }
        if self
            .sender
            .as_ref()
            .is_some_and(|sender| h.sender.as_ref() != Some(sender))
        {
            return false;
        }
        if self
            .interface
            .as_ref()
            .is_some_and(|interface| h.interface.as_ref() != Some(interface))
        {
            return false;
        }
        if self
            .member
            .as_ref()
            .is_some_and(|member| h.member.as_ref() != Some(member))
        {
            return false;
        }
        if self
            .path
            .as_ref()
            .is_some_and(|path| h.path.as_ref() != Some(path))
        {
            return false;
        }
        if let Some(namespace) = &self.path_namespace {
            match &h.path {
                Some(path) if path.starts_with(namespace) => {}
                _ => return false,
            }
        }
        true
    }

    /// Parse the comma-separated `key=value` wire form.
    ///
    /// Values are unquoted and may not contain a comma. That is a real
    /// restriction, and it is fine: every field is a name, and no name grammar
    /// in [`crate::name`] admits a comma.
    pub fn parse(input: &str) -> Result<Self> {
        let mut rule = Self::new();
        for clause in input.split(',').filter(|c| !c.trim().is_empty()) {
            let (key, value) = clause
                .split_once('=')
                .ok_or_else(|| Error::protocol(format!("match clause `{clause}` has no `=`")))?;
            let value = value.trim();
            match key.trim() {
                "type" => {
                    rule.kind = Some(match value {
                        "signal" => MessageKind::Signal,
                        "method_call" => MessageKind::MethodCall,
                        "method_return" => MessageKind::MethodReturn,
                        "error" => MessageKind::Error,
                        other => {
                            return Err(Error::protocol(format!("unknown message type `{other}`")));
                        }
                    });
                }
                "sender" => rule.sender = Some(BusName::new(value)?),
                "interface" => rule.interface = Some(InterfaceName::new(value)?),
                "member" => rule.member = Some(MemberName::new(value)?),
                "path" => rule.path = Some(ObjectPath::new(value)?),
                "path_namespace" => rule.path_namespace = Some(ObjectPath::new(value)?),
                other => {
                    return Err(Error::protocol(format!("unknown match key `{other}`")));
                }
            }
        }
        Ok(rule)
    }
}

/// One attached peer, from the broker's point of view.
struct Peer {
    unique: BusName,
    /// The writer task's inbox. Cloned out of the table and sent to *after*
    /// the routing lock is released.
    outbox: mpsc::Sender<Message>,
    matches: Vec<MatchRule>,
    /// What this peer says it speaks and accepts. `None` until it announces —
    /// and a peer that never announces stays routable, so manifests can be
    /// adopted one service at a time rather than as a flag day.
    manifest: Option<PeerManifest>,
    /// What the host verified about this peer, per name it owns.
    ///
    /// Keyed by name rather than one per peer because a peer may hold several
    /// well-known names and the operator allowlists an artifact *for a name*.
    /// Empty for every peer until a module load actually verifies one — the
    /// absence of an entry is what refuses a confidential delivery, so an
    /// ordinary out-of-process peer is ineligible by construction.
    attestations: HashMap<BusName, Attestation>,
    /// The artifact admitted for this exact connection. Its original manifest
    /// identity survives alias release; unrelated aliases never inherit it.
    admitted_artifact: Option<Attestation>,
}

/// Who is attached, what they are called, and what they want to hear.
///
/// Not `Sync` by itself — the broker wraps it in a plain `std::sync::Mutex`,
/// which is only sound because no method here awaits.
#[derive(Default)]
pub(crate) struct Router {
    peers: HashMap<u64, Peer>,
    names: HashMap<BusName, u64>,
    next_id: u64,
}

/// What changed when a name's owner changed, so the broker can announce it.
#[derive(Debug)]
pub(crate) struct NameChange {
    pub name: BusName,
    pub old_owner: Option<BusName>,
    pub new_owner: Option<BusName>,
}

impl Router {
    /// Attach a peer and mint its unique name.
    pub fn attach(&mut self, outbox: mpsc::Sender<Message>) -> (u64, BusName) {
        // Ids start at 1 and are never reused, so a stale reply addressed to a
        // dead `:1.4` can never be delivered to its replacement.
        self.next_id += 1;
        let id = self.next_id;
        let unique = BusName::unique(id);
        self.peers.insert(
            id,
            Peer {
                unique: unique.clone(),
                outbox,
                matches: Vec::new(),
                manifest: None,
                attestations: HashMap::new(),
                admitted_artifact: None,
            },
        );
        self.names.insert(unique.clone(), id);
        (id, unique)
    }

    /// Detach a peer, releasing every name it owned.
    ///
    /// Returns one [`NameChange`] per released well-known name so the broker
    /// can emit `NameOwnerChanged`. That signal is how the kernel learns an
    /// integration died without polling it.
    pub fn detach(&mut self, id: u64) -> Vec<NameChange> {
        let Some(peer) = self.peers.remove(&id) else {
            return Vec::new();
        };
        let owned: Vec<BusName> = self
            .names
            .iter()
            .filter(|(_, owner)| **owner == id)
            .map(|(name, _)| name.clone())
            .collect();
        let mut changes = Vec::new();
        for name in owned {
            self.names.remove(&name);
            if !name.is_unique() {
                changes.push(NameChange {
                    name,
                    old_owner: Some(peer.unique.clone()),
                    new_owner: None,
                });
            }
        }
        changes
    }

    /// Claim a well-known name for `id`.
    ///
    /// First writer wins and there is no queue: a second claimant is told the
    /// name is taken rather than being parked. Queued ownership is a D-Bus
    /// feature we are consciously not copying — two live processes both able to
    /// answer as the wallet is a worse outcome than a clear startup failure.
    pub fn request_name(&mut self, id: u64, name: BusName) -> Result<NameChange> {
        if name.is_unique() {
            return Err(Error::protocol("a unique name cannot be requested"));
        }
        if name.as_str() == crate::BUS_NAME {
            return Err(Error::protocol("the bus's own name is reserved"));
        }
        match self.names.get(&name) {
            Some(owner) if *owner == id => Ok(NameChange {
                name: name.clone(),
                old_owner: Some(self.unique_of(id)?),
                new_owner: Some(self.unique_of(id)?),
            }),
            Some(owner) => Err(Error::NameTaken {
                name,
                owner: self.unique_of(*owner)?,
            }),
            None => {
                self.names.insert(name.clone(), id);
                Ok(NameChange {
                    name,
                    old_owner: None,
                    new_owner: Some(self.unique_of(id)?),
                })
            }
        }
    }

    /// Claim a well-known name for an attached peer identified by its unique
    /// name. Module activation uses this to make a lazy module routable before
    /// its SDK handshake has run.
    #[cfg(feature = "modules")]
    pub(crate) fn request_name_for_unique(
        &mut self,
        unique: &BusName,
        name: BusName,
    ) -> Result<NameChange> {
        let id = *self
            .names
            .get(unique)
            .ok_or_else(|| Error::NameHasNoOwner(unique.clone()))?;
        self.request_name(id, name)
    }

    /// Give up a well-known name. Releasing a name you do not own is an error,
    /// not a no-op — it always means the caller's model of the bus is wrong.
    pub fn release_name(&mut self, id: u64, name: &BusName) -> Result<NameChange> {
        match self.names.get(name) {
            Some(owner) if *owner == id => {
                self.names.remove(name);
                Ok(NameChange {
                    name: name.clone(),
                    old_owner: Some(self.unique_of(id)?),
                    new_owner: None,
                })
            }
            Some(owner) => Err(Error::NameTaken {
                name: name.clone(),
                owner: self.unique_of(*owner)?,
            }),
            None => Err(Error::NameHasNoOwner(name.clone())),
        }
    }

    /// Record what a peer says about itself.
    pub fn set_manifest(&mut self, id: u64, manifest: PeerManifest) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.manifest = Some(manifest);
        }
    }

    /// Every peer that has announced a manifest, with the names it owns.
    pub fn peer_records(&self) -> Vec<PeerRecord> {
        let mut records: Vec<PeerRecord> = self
            .peers
            .iter()
            .filter_map(|(id, peer)| {
                let manifest = peer.manifest.clone()?;
                let mut names: Vec<BusName> = self
                    .names
                    .iter()
                    .filter(|(name, owner)| *owner == id && !name.is_unique())
                    .map(|(name, _)| name.clone())
                    .collect();
                names.sort();
                Some(PeerRecord {
                    peer: peer.unique.clone(),
                    names,
                    manifest,
                })
            })
            .collect();
        records.sort_by(|a, b| a.peer.cmp(&b.peer));
        records
    }

    /// The manifest of whoever owns `name`, by unique or well-known name.
    pub fn manifest_of(&self, name: &BusName) -> Option<PeerManifest> {
        let id = self.names.get(name)?;
        self.peers.get(id)?.manifest.clone()
    }

    /// Register a subscription for `id`.
    pub fn add_match(&mut self, id: u64, rule: MatchRule) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.matches.push(rule);
        }
    }

    /// Drop a subscription. Silently does nothing if it was never added — a
    /// client tearing down twice is not an error worth propagating.
    pub fn remove_match(&mut self, id: u64, rule: &MatchRule) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.matches.retain(|r| r != rule);
        }
    }

    /// Every name currently owned, unique names included.
    pub fn list_names(&self) -> Vec<BusName> {
        let mut names: Vec<BusName> = self.names.keys().cloned().collect();
        names.sort();
        names
    }

    /// The unique name of whoever owns `name`.
    pub fn owner_of(&self, name: &BusName) -> Option<BusName> {
        let id = self.names.get(name)?;
        self.peers.get(id).map(|p| p.unique.clone())
    }

    /// Record what the host verified about the peer owning `name`.
    ///
    /// Gated with module loading, because that is the only thing that can
    /// produce an attestation. Without it nothing is ever attested and every
    /// confidential delivery is refused, which is the correct behaviour for a
    /// build that cannot load a module in the first place.
    #[cfg(feature = "modules")]
    ///
    /// Stored against the peer, so it dies with the peer: a service that exits
    /// takes its attestation with it, and the next process to claim the name
    /// has to earn its own. Nothing here is ever copied forward on a name
    /// handover, which is what stops a released name carrying its predecessor's
    /// trust to whoever grabs it next.
    pub fn set_attestation(&mut self, id: u64, attestation: Attestation) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.admitted_artifact = Some(attestation.clone());
            peer.attestations
                .insert(attestation.name.clone(), attestation);
        }
    }

    /// [`Router::set_attestation`] addressed by the peer's unique name, for the
    /// module host, which holds that rather than the internal peer id.
    #[cfg(feature = "modules")]
    pub(crate) fn set_attestation_for_unique(
        &mut self,
        unique: &BusName,
        attestation: Attestation,
    ) {
        if let Some(id) = self.names.get(unique).copied() {
            self.set_attestation(id, attestation);
        }
    }

    /// What the broker verified about whoever owns `name`, if anything.
    pub fn attestation_of(&self, name: &BusName) -> Option<Attestation> {
        let id = self.names.get(name)?;
        let peer = self.peers.get(id)?;
        if name.is_unique() {
            peer.admitted_artifact.clone()
        } else {
            peer.attestations.get(name).cloned()
        }
    }

    /// The outbox of whoever owns `destination`, but only if the host has
    /// verified that peer's artifact for that well-known identity, or admitted
    /// the artifact on that exact unique connection.
    ///
    /// The lookup and the check are one operation on purpose. Resolving first
    /// and checking after would leave a window in which a caller could hold a
    /// sender for an unattested peer, and every such window eventually becomes
    /// a delivery.
    pub fn resolve_attested(&self, destination: &BusName) -> Result<mpsc::Sender<Message>> {
        let id = self
            .names
            .get(destination)
            .ok_or_else(|| Error::NameHasNoOwner(destination.clone()))?;
        let peer = self
            .peers
            .get(id)
            .ok_or_else(|| Error::NameHasNoOwner(destination.clone()))?;
        let attested = if destination.is_unique() {
            peer.admitted_artifact.is_some()
        } else {
            peer.attestations.contains_key(destination)
        };
        if !attested {
            return Err(Error::not_attested(
                destination.clone(),
                "only a loaded module with a verified artifact may receive a secret",
            ));
        }
        Ok(peer.outbox.clone())
    }

    /// The outbox of whoever owns `destination`.
    pub fn resolve(&self, destination: &BusName) -> Result<mpsc::Sender<Message>> {
        let id = self
            .names
            .get(destination)
            .ok_or_else(|| Error::NameHasNoOwner(destination.clone()))?;
        self.peers
            .get(id)
            .map(|p| p.outbox.clone())
            .ok_or_else(|| Error::NameHasNoOwner(destination.clone()))
    }

    /// The outboxes of every peer subscribed to `signal`, excluding the sender.
    ///
    /// Excluding the sender is not an optimisation: a service that both emits
    /// and subscribes on the same interface would otherwise hear its own
    /// signal and, if it re-emits in response, loop.
    /// A confidential message has no subscribers, whatever anyone matched.
    /// `validate` already refuses confidential signals on ingress, so this can
    /// only fire if some future path builds one internally — and the cost of
    /// being wrong here is a secret delivered to every peer holding a match
    /// rule, so it is checked twice rather than reasoned about once.
    pub fn subscribers(&self, signal: &Message, from: u64) -> Vec<mpsc::Sender<Message>> {
        if signal.header.confidential {
            return Vec::new();
        }
        self.peers
            .iter()
            .filter(|(id, _)| **id != from)
            .filter(|(_, peer)| peer.matches.iter().any(|rule| rule.matches(signal)))
            .map(|(_, peer)| peer.outbox.clone())
            .collect()
    }

    /// Every attached peer's outbox. Used for bus-generated announcements that
    /// still go through match filtering at the call site.
    pub fn broadcast_targets(&self, signal: &Message) -> Vec<mpsc::Sender<Message>> {
        if signal.header.confidential {
            return Vec::new();
        }
        self.peers
            .values()
            .filter(|peer| peer.matches.iter().any(|rule| rule.matches(signal)))
            .map(|peer| peer.outbox.clone())
            .collect()
    }

    fn unique_of(&self, id: u64) -> Result<BusName> {
        self.peers
            .get(&id)
            .map(|p| p.unique.clone())
            .ok_or_else(|| Error::transport("peer detached mid-operation"))
    }
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
