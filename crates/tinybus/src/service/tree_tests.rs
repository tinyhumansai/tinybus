use super::*;
use async_trait::async_trait;

struct Echo {
    name: &'static str,
    tag: &'static str,
    confidential: bool,
}

#[async_trait]
impl Interface for Echo {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(self.name).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Echo").unwrap()]
    }

    fn requires_confidential(&self, _member: &MemberName) -> bool {
        self.confidential
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
            confidential: false,
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
            confidential: false,
        }),
    );
    tree.insert(
        path(),
        Arc::new(Echo {
            name: "ai.tinyhumans.Voice",
            tag: "new",
            confidential: false,
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
async fn a_confidential_member_rejects_an_ordinary_dispatch() {
    let mut tree = ObjectTree::new();
    tree.insert(
        path(),
        Arc::new(Echo {
            name: "ai.tinyhumans.Voice",
            tag: "secret",
            confidential: true,
        }),
    );
    let interface = iface("ai.tinyhumans.Voice");
    let member = MemberName::new("Echo").unwrap();

    let error = tree
        .dispatch(&path(), &interface, &member, Value::String("key".into()))
        .await
        .unwrap_err();
    assert_eq!(error.wire_name(), Error::CONFIDENTIALITY_REQUIRED);

    let value = tree
        .dispatch_with_confidential(
            &path(),
            &interface,
            &member,
            Value::String("key".into()),
            true,
        )
        .await
        .unwrap();
    assert_eq!(value, Value::String("secret".into()));
}

#[tokio::test]
async fn the_three_failure_modes_are_distinguishable() {
    let mut tree = ObjectTree::new();
    tree.insert(
        path(),
        Arc::new(Echo {
            name: "ai.tinyhumans.Voice",
            tag: "x",
            confidential: false,
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
            confidential: false,
        }),
    );
    tree.insert(
        path(),
        Arc::new(Echo {
            name: "ai.tinyhumans.Peer",
            tag: "b",
            confidential: false,
        }),
    );
    assert_eq!(tree.interfaces_at(&path()).len(), 2);
    assert_eq!(tree.paths(), vec![path()]);
    assert!(tree.remove(&path()));
    assert!(!tree.remove(&path()));
}

#[tokio::test]
async fn direct_context_dispatch_defaults_to_unverified_and_keeps_legacy_arguments() {
    let mut tree = ObjectTree::new();
    tree.insert(
        path(),
        Arc::new(Echo {
            name: "ai.tinyhumans.Voice",
            tag: "legacy",
            confidential: false,
        }),
    );
    let member = MemberName::new("Echo").unwrap();
    let context = CallContext::default();
    assert_eq!(context.authenticated_sender(), None);
    assert_eq!(
        tree.dispatch_with_context(
            &path(),
            &iface("ai.tinyhumans.Voice"),
            &member,
            serde_json::json!([1, 2]),
            false,
            &context
        )
        .await
        .unwrap(),
        "legacy"
    );
}
