use super::*;

#[test]
fn call_context_debug_exposes_only_its_sender_and_routing_facts() {
    let empty = CallContext::default();
    let empty_debug = format!("{empty:?}");
    assert!(empty_debug.contains("sender: None"));
    assert!(empty_debug.contains("confidential: false"));
    assert!(empty_debug.contains("has_receive_origin: false"));

    let sender = BusName::new(":1.42").unwrap();
    let authenticated = CallContext::brokered(Some(&sender), true);
    let authenticated_debug = format!("{authenticated:?}");
    assert!(authenticated_debug.contains(":1.42"));
    assert!(authenticated_debug.contains("confidential: true"));
    assert!(authenticated_debug.contains("has_receive_origin: false"));
}

struct ArcForwardingInterface;

#[async_trait::async_trait]
impl Interface for ArcForwardingInterface {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("org.example.ArcForwarding").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Inspect").unwrap()]
    }

    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }

    async fn call(&self, member: &MemberName, _: Value) -> Result<Value> {
        Ok(Value::String(member.as_str().to_owned()))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        _: Value,
        context: &CallContext,
    ) -> Result<Value> {
        Ok(serde_json::json!([
            member.as_str(),
            context.is_confidential()
        ]))
    }
}

#[tokio::test]
async fn arc_interface_forwards_legacy_and_contextual_dispatch() {
    let interface = std::sync::Arc::new(ArcForwardingInterface);
    let member = MemberName::new("Inspect").unwrap();
    assert_eq!(
        <std::sync::Arc<ArcForwardingInterface> as Interface>::name(&interface),
        InterfaceName::new("org.example.ArcForwarding").unwrap()
    );
    assert_eq!(
        <std::sync::Arc<ArcForwardingInterface> as Interface>::members(&interface),
        vec![member.clone()]
    );
    assert!(
        <std::sync::Arc<ArcForwardingInterface> as Interface>::requires_confidential(
            &interface, &member
        )
    );
    assert_eq!(
        <std::sync::Arc<ArcForwardingInterface> as Interface>::call(
            &interface,
            &member,
            Value::Null
        )
        .await
        .unwrap(),
        Value::String("Inspect".to_owned())
    );

    let sender = BusName::new(":1.42").unwrap();
    let context = CallContext::brokered(Some(&sender), true);
    assert_eq!(
        <std::sync::Arc<ArcForwardingInterface> as Interface>::call_with_context(
            &interface,
            &member,
            Value::Null,
            &context
        )
        .await
        .unwrap(),
        serde_json::json!(["Inspect", true])
    );
}

#[test]
fn only_a_brokered_unique_sender_is_authenticated() {
    assert!(CallContext::default().authenticated_sender().is_none());
    assert!(
        CallContext::brokered(None, false)
            .authenticated_sender()
            .is_none()
    );
    let well_known = BusName::new("org.example.Host").unwrap();
    assert!(
        CallContext::brokered(Some(&well_known), false)
            .authenticated_sender()
            .is_none()
    );
    let unique = BusName::new(":1.42").unwrap();
    assert_eq!(
        CallContext::brokered(Some(&unique), false).authenticated_sender(),
        Some(&unique)
    );
}
