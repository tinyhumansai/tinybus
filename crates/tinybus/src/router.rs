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
        if let Some(kind) = self.kind
            && kind != h.kind
        {
            return false;
        }
        if let Some(sender) = &self.sender
            && h.sender.as_ref() != Some(sender)
        {
            return false;
        }
        if let Some(interface) = &self.interface
            && h.interface.as_ref() != Some(interface)
        {
            return false;
        }
        if let Some(member) = &self.member
            && h.member.as_ref() != Some(member)
        {
            return false;
        }
        if let Some(path) = &self.path
            && h.path.as_ref() != Some(path)
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
    pub fn subscribers(&self, signal: &Message, from: u64) -> Vec<mpsc::Sender<Message>> {
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
mod tests {
    use super::*;

    fn signal(interface: &str, member: &str, path: &str) -> Message {
        let mut m = Message::signal(
            ObjectPath::new(path).unwrap(),
            InterfaceName::new(interface).unwrap(),
            MemberName::new(member).unwrap(),
            serde_json::Value::Null,
        );
        m.header.sender = Some(BusName::new(":1.1").unwrap());
        m
    }

    fn outbox() -> mpsc::Sender<Message> {
        mpsc::channel(8).0
    }

    #[test]
    fn an_empty_rule_matches_everything() {
        assert!(MatchRule::new().matches(&signal("ai.tinyhumans.Mail", "Received", "/ai/Mail")));
    }

    #[test]
    fn every_set_field_must_match() {
        let rule =
            MatchRule::parse("type=signal,interface=ai.tinyhumans.Mail,member=Received").unwrap();
        assert!(rule.matches(&signal("ai.tinyhumans.Mail", "Received", "/ai/Mail")));
        assert!(!rule.matches(&signal("ai.tinyhumans.Mail", "Sent", "/ai/Mail")));
        assert!(!rule.matches(&signal("ai.tinyhumans.Voice", "Received", "/ai/Mail")));
    }

    #[test]
    fn a_namespace_rule_covers_the_subtree_but_not_a_sibling() {
        let rule = MatchRule::parse("path_namespace=/ai/Mail").unwrap();
        assert!(rule.matches(&signal("ai.tinyhumans.Mail", "Received", "/ai/Mail/work")));
        assert!(!rule.matches(&signal("ai.tinyhumans.Mail", "Received", "/ai/Mailbox")));
    }

    #[test]
    fn parsing_rejects_unknown_keys_rather_than_ignoring_them() {
        // Silently dropping an unrecognised clause would widen the
        // subscription — the client asked to hear less and would hear more.
        assert!(MatchRule::parse("interfce=ai.tinyhumans.Mail").is_err());
        assert!(MatchRule::parse("type=telegram").is_err());
        assert!(MatchRule::parse("interface").is_err());
    }

    #[test]
    fn unique_names_are_minted_in_order_and_never_reused() {
        let mut router = Router::default();
        let (a, a_name) = router.attach(outbox());
        let (_, b_name) = router.attach(outbox());
        assert_eq!(a_name.as_str(), ":1.1");
        assert_eq!(b_name.as_str(), ":1.2");
        router.detach(a);
        let (_, c_name) = router.attach(outbox());
        assert_eq!(c_name.as_str(), ":1.3");
    }

    #[test]
    fn a_well_known_name_has_one_owner_and_the_loser_is_told_who_won() {
        let mut router = Router::default();
        let (a, a_unique) = router.attach(outbox());
        let (b, _) = router.attach(outbox());
        let name = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();

        router.request_name(a, name.clone()).unwrap();
        let err = router.request_name(b, name.clone()).unwrap_err();
        match err {
            Error::NameTaken { owner, .. } => assert_eq!(owner, a_unique),
            other => panic!("expected NameTaken, got {other}"),
        }
        // Re-requesting a name you already hold is idempotent, so a service
        // that reconnects its own registration does not fail on restart.
        router.request_name(a, name).unwrap();
    }

    #[test]
    fn detaching_frees_the_names_and_reports_the_change() {
        let mut router = Router::default();
        let (a, a_unique) = router.attach(outbox());
        let name = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
        router.request_name(a, name.clone()).unwrap();

        let changes = router.detach(a);
        assert_eq!(changes.len(), 1, "only the well-known name is announced");
        assert_eq!(changes[0].name, name);
        assert_eq!(changes[0].old_owner, Some(a_unique));
        assert!(changes[0].new_owner.is_none());
        assert!(router.list_names().is_empty());

        // ...and a call to the dead integration now names it, rather than
        // hanging or reporting a generic failure.
        let err = router.resolve(&name).unwrap_err();
        assert!(err.to_string().contains("no peer owns"), "{err}");
    }

    #[test]
    fn the_bus_name_and_unique_names_cannot_be_claimed() {
        let mut router = Router::default();
        let (a, _) = router.attach(outbox());
        assert!(
            router
                .request_name(a, BusName::new(crate::BUS_NAME).unwrap())
                .is_err()
        );
        assert!(
            router
                .request_name(a, BusName::new(":1.99").unwrap())
                .is_err()
        );
    }

    #[test]
    fn a_sender_never_receives_its_own_signal() {
        let mut router = Router::default();
        let (a, _) = router.attach(outbox());
        let (b, _) = router.attach(outbox());
        router.add_match(a, MatchRule::new().signals());
        router.add_match(b, MatchRule::new().signals());

        let sig = signal("ai.tinyhumans.Mail", "Received", "/ai/Mail");
        assert_eq!(router.subscribers(&sig, a).len(), 1);
        assert_eq!(router.subscribers(&sig, b).len(), 1);
    }

    #[test]
    fn an_unsubscribed_peer_is_not_woken() {
        let mut router = Router::default();
        let (a, _) = router.attach(outbox());
        let (b, _) = router.attach(outbox());
        router.add_match(
            b,
            MatchRule::new()
                .signals()
                .interface(InterfaceName::new("ai.tinyhumans.Voice").unwrap()),
        );
        let sig = signal("ai.tinyhumans.Mail", "Received", "/ai/Mail");
        assert!(router.subscribers(&sig, a).is_empty());
    }

    #[test]
    fn removing_a_match_stops_delivery() {
        let mut router = Router::default();
        let (a, _) = router.attach(outbox());
        let (b, _) = router.attach(outbox());
        let rule = MatchRule::new().signals();
        router.add_match(b, rule.clone());
        let sig = signal("ai.tinyhumans.Mail", "Received", "/ai/Mail");
        assert_eq!(router.subscribers(&sig, a).len(), 1);
        router.remove_match(b, &rule);
        assert!(router.subscribers(&sig, a).is_empty());
    }
}
