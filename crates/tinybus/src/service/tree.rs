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
use crate::service::Interface;

/// Everything one connection exports.
#[derive(Default)]
pub struct ObjectTree {
    objects: HashMap<ObjectPath, Vec<Arc<dyn Interface>>>,
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
        let name = interface.name();
        let entry = self.objects.entry(path).or_default();
        entry.retain(|existing| existing.name() != name);
        entry.push(interface);
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
            .map(|list| list.iter().map(|i| i.name()).collect())
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
            .find(|i| &i.name() == interface)
            .cloned()
            .ok_or_else(|| Error::UnknownInterface {
                path: path.clone(),
                interface: interface.clone(),
            })
    }

    /// Resolve and invoke in one step.
    pub async fn dispatch(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
        args: Value,
    ) -> Result<Value> {
        let target = self.lookup(path, interface)?;
        if !target.members().contains(member) {
            return Err(Error::UnknownMethod {
                interface: interface.clone(),
                member: member.clone(),
            });
        }
        target.call(member, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct Echo {
        name: &'static str,
        tag: &'static str,
    }

    #[async_trait]
    impl Interface for Echo {
        fn name(&self) -> InterfaceName {
            InterfaceName::new(self.name).unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Echo").unwrap()]
        }

        async fn call(&self, _member: &MemberName, _args: Value) -> Result<Value> {
            Ok(Value::String(self.tag.to_string()))
        }
    }

    fn path() -> ObjectPath {
        ObjectPath::new("/ai/tinyhumans/openhuman/Voice").unwrap()
    }

    fn iface(name: &str) -> InterfaceName {
        InterfaceName::new(name).unwrap()
    }

    #[tokio::test]
    async fn dispatch_finds_the_registered_interface() {
        let mut tree = ObjectTree::new();
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Voice",
                tag: "first",
            }),
        );
        let out = tree
            .dispatch(
                &path(),
                &iface("ai.tinyhumans.Voice"),
                &MemberName::new("Echo").unwrap(),
                Value::Null,
            )
            .await
            .unwrap();
        assert_eq!(out, Value::String("first".into()));
    }

    #[tokio::test]
    async fn re_registering_a_contract_replaces_it_rather_than_shadowing_it() {
        let mut tree = ObjectTree::new();
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Voice",
                tag: "old",
            }),
        );
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Voice",
                tag: "new",
            }),
        );
        assert_eq!(tree.interfaces_at(&path()).len(), 1);
        let out = tree
            .dispatch(
                &path(),
                &iface("ai.tinyhumans.Voice"),
                &MemberName::new("Echo").unwrap(),
                Value::Null,
            )
            .await
            .unwrap();
        assert_eq!(out, Value::String("new".into()));
    }

    #[tokio::test]
    async fn the_three_failure_modes_are_distinguishable() {
        let mut tree = ObjectTree::new();
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Voice",
                tag: "x",
            }),
        );

        let missing_object = tree
            .dispatch(
                &ObjectPath::new("/nope").unwrap(),
                &iface("ai.tinyhumans.Voice"),
                &MemberName::new("Echo").unwrap(),
                Value::Null,
            )
            .await
            .unwrap_err();
        assert!(matches!(missing_object, Error::UnknownObject { .. }));

        let missing_interface = tree
            .dispatch(
                &path(),
                &iface("ai.tinyhumans.Mail"),
                &MemberName::new("Echo").unwrap(),
                Value::Null,
            )
            .await
            .unwrap_err();
        assert!(matches!(missing_interface, Error::UnknownInterface { .. }));

        let missing_member = tree
            .dispatch(
                &path(),
                &iface("ai.tinyhumans.Voice"),
                &MemberName::new("Nope").unwrap(),
                Value::Null,
            )
            .await
            .unwrap_err();
        assert!(matches!(missing_member, Error::UnknownMethod { .. }));
    }

    #[test]
    fn one_object_can_carry_several_contracts() {
        let mut tree = ObjectTree::new();
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Voice",
                tag: "a",
            }),
        );
        tree.insert(
            path(),
            Arc::new(Echo {
                name: "ai.tinyhumans.Peer",
                tag: "b",
            }),
        );
        assert_eq!(tree.interfaces_at(&path()).len(), 2);
        assert_eq!(tree.paths(), vec![path()]);
        assert!(tree.remove(&path()));
        assert!(!tree.remove(&path()));
    }
}
