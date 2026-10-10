use super::*;

#[test]
fn only_a_brokered_unique_sender_is_authenticated() {
    assert!(CallContext::default().authenticated_sender().is_none());
    assert!(CallContext::brokered(None).authenticated_sender().is_none());
    let well_known = BusName::new("org.example.Host").unwrap();
    assert!(
        CallContext::brokered(Some(&well_known))
            .authenticated_sender()
            .is_none()
    );
    let unique = BusName::new(":1.42").unwrap();
    assert_eq!(
        CallContext::brokered(Some(&unique)).authenticated_sender(),
        Some(&unique)
    );
}
