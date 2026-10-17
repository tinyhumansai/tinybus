//! The object tree: what this connection exports, and where.
//!
//! A flat map from [`ObjectPath`] to a list of interfaces. Flat rather than an
//! actual tree because nothing needs the hierarchy at dispatch time — the only
//! consumer of path structure is `path_namespace` matching, which is a string
//! comparison on the sender's side. A real tree would buy nothing and would
//! make "list every object" — the operation introspection actually performs —
//! a traversal instead of an iteration.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::name::{InterfaceName, MemberName, ObjectPath};
use crate::service::{CallContext, Interface};

/// Everything one connection exports.
#[derive(Default)]
pub struct ObjectTree {
    objects: HashMap<ObjectPath, Vec<ObjectRegistration>>,
    next_registration: u64,
}

struct ObjectRegistration {
    #[cfg_attr(not(feature = "modules"), allow(dead_code))]
    id: u64,
    interface: Arc<dyn Interface>,
}

impl ObjectTree {
    /// An empty tree.
    pub fn new() -> Self {
        Self::default()
    }

    /// Export `interface` at `path`.
    ///
    /// Registering a second interface with a name already present at that path
    /// **replaces** it. Replace rather than reject because hot-reloading an
    /// implementation is a legitimate thing for a long-lived service to do, and
    /// two implementations of one contract at one address would make dispatch
    /// order-dependent.
    pub fn insert(&mut self, path: ObjectPath, interface: Arc<dyn Interface>) {
        self.insert_owned(path, interface);
    }

    /// Export `interface` and return the identity of this exact registration.
    /// A later replacement at the same path and interface gets a different
    /// identity, so an older owner cannot unregister the newer implementation.
    pub(crate) fn insert_owned(&mut self, path: ObjectPath, interface: Arc<dyn Interface>) -> u64 {
        self.next_registration = self
            .next_registration
            .checked_add(1)
            .expect("object registration identity exhausted");
        let id = self.next_registration;
        let name = interface.name();
        let entry = self.objects.entry(path).or_default();
        entry.retain(|existing| existing.interface.name() != name);
        entry.push(ObjectRegistration { id, interface });
        id
    }

    /// Remove one interface only if it is still the same registration.
    #[cfg(feature = "modules")]
    pub(crate) fn remove_if_owned(
        &mut self,
        path: &ObjectPath,
        interface: &InterfaceName,
        id: u64,
    ) -> bool {
        let Some(entries) = self.objects.get_mut(path) else {
            return false;
        };
        let Some(index) = entries
            .iter()
            .position(|entry| entry.id == id && entry.interface.name() == *interface)
        else {
            return false;
        };
        entries.remove(index);
        if entries.is_empty() {
            self.objects.remove(path);
        }
        true
    }

    /// Stop exporting everything at `path`. Returns whether anything went away.
    pub fn remove(&mut self, path: &ObjectPath) -> bool {
        self.objects.remove(path).is_some()
    }

    /// Every exported path, sorted, for introspection.
    pub fn paths(&self) -> Vec<ObjectPath> {
        let mut paths: Vec<ObjectPath> = self.objects.keys().cloned().collect();
        paths.sort();
        paths
    }

    /// The interfaces exported at `path`.
    pub fn interfaces_at(&self, path: &ObjectPath) -> Vec<InterfaceName> {
        self.objects
            .get(path)
            .map(|list| list.iter().map(|entry| entry.interface.name()).collect())
            .unwrap_or_default()
    }

    /// Look up one interface, distinguishing "no such object" from
    /// "object exists, wrong contract".
    pub fn lookup(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
    ) -> Result<Arc<dyn Interface>> {
        let list = self
            .objects
            .get(path)
            .ok_or_else(|| Error::UnknownObject { path: path.clone() })?;
        list.iter()
            .find(|entry| &entry.interface.name() == interface)
            .map(|entry| entry.interface.clone())
            .ok_or_else(|| Error::UnknownInterface {
                path: path.clone(),
                interface: interface.clone(),
            })
    }

    /// Resolve and invoke an ordinary call with an unverified default context.
    pub async fn dispatch(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
        args: Value,
    ) -> Result<Value> {
        self.dispatch_with_confidential(path, interface, member, args, false)
            .await
    }

    /// Resolve and invoke one call while enforcing the delivery's
    /// confidentiality flag before argument decoding reaches user code.
    ///
    /// Uses an unverified default context even when `confidential` is true;
    /// the flag does not authenticate a caller.
    pub async fn dispatch_with_confidential(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
        args: Value,
        confidential: bool,
    ) -> Result<Value> {
        self.dispatch_with_context(
            path,
            interface,
            member,
            args,
            confidential,
            &CallContext::default(),
        )
        .await
    }

    /// Resolve and invoke with out-of-band incoming context and delivery flags.
    ///
    /// Checks the member and its confidentiality requirement before calling
    /// [`Interface::call_with_context`]. Passes `context` through unchanged;
    /// this method does not derive caller identity from `args` or the flag.
    pub async fn dispatch_with_context(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
        args: Value,
        confidential: bool,
        context: &CallContext,
    ) -> Result<Value> {
        let target = self.lookup(path, interface)?;
        Self::invoke(target, interface, member, args, confidential, context).await
    }

    /// Invoke a resolved snapshot without holding the connection's tree lock.
    pub(crate) async fn invoke(
        target: Arc<dyn Interface>,
        interface: &InterfaceName,
        member: &MemberName,
        args: Value,
        confidential: bool,
        context: &CallContext,
    ) -> Result<Value> {
        if !target.members().contains(member) {
            return Err(Error::UnknownMethod {
                interface: interface.clone(),
                member: member.clone(),
            });
        }
        if target.requires_confidential(member) && !confidential {
            return Err(Error::ConfidentialityRequired {
                interface: interface.clone(),
                member: member.clone(),
            });
        }
        target.call_with_context(member, args, context).await
    }
}

#[cfg(test)]
#[path = "tree_tests.rs"]
mod tests;
