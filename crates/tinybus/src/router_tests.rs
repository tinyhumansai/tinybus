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

#[cfg(feature = "modules")]
fn attestation(name: &str) -> Attestation {
    Attestation {
        name: BusName::new(name).unwrap(),
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
    }
}

#[test]
fn a_confidential_message_reaches_no_subscriber_however_broad_the_rule() {
    let mut router = Router::default();
    let (a, _) = router.attach(outbox());
    router.attach(outbox());
    // An empty rule matches everything, which is the worst case: if any
    // rule could pull in a secret, this one would.
    router.add_match(a, MatchRule::new());

    let mut sig = signal("ai.tinyhumans.Test", "Tick", "/");
    assert_eq!(router.subscribers(&sig, 99).len(), 1);
    assert_eq!(router.broadcast_targets(&sig).len(), 1);

    sig.header.confidential = true;
    assert!(router.subscribers(&sig, 99).is_empty());
    assert!(router.broadcast_targets(&sig).is_empty());
}

#[test]
fn an_unattested_owner_routes_normally_but_never_confidentially() {
    let mut router = Router::default();
    let (id, _) = router.attach(outbox());
    let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    router.request_name(id, name.clone()).unwrap();

    assert!(router.resolve(&name).is_ok());
    assert_eq!(router.attestation_of(&name), None);
    let error = router.resolve_attested(&name).unwrap_err();
    assert_eq!(error.wire_name(), Error::NOT_ATTESTED);
}

#[cfg(feature = "modules")]
#[test]
fn an_attested_owner_can_receive_a_confidential_message() {
    let mut router = Router::default();
    let (id, _) = router.attach(outbox());
    let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    router.request_name(id, name.clone()).unwrap();
    router.set_attestation(id, attestation(name.as_str()));

    assert!(router.resolve_attested(&name).is_ok());
    assert_eq!(
        router.attestation_of(&name),
        Some(attestation(name.as_str()))
    );
}

#[cfg(feature = "modules")]
#[test]
fn an_attestation_is_bound_to_the_name_it_was_verified_for() {
    // Holding two names must not let trust earned for one carry to the
    // other: the operator allowlisted an artifact *as the wallet*, not as
    // everything that process might also answer to.
    let mut router = Router::default();
    let (id, _) = router.attach(outbox());
    let wallet = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    let voice = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
    router.request_name(id, wallet.clone()).unwrap();
    router.request_name(id, voice.clone()).unwrap();
    router.set_attestation(id, attestation(wallet.as_str()));

    assert!(router.resolve_attested(&wallet).is_ok());
    assert!(router.resolve_attested(&voice).is_err());
}

#[cfg(feature = "modules")]
#[test]
fn a_dead_peers_attestation_does_not_survive_it() {
    let mut router = Router::default();
    let (id, _) = router.attach(outbox());
    let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    router.request_name(id, name.clone()).unwrap();
    router.set_attestation(id, attestation(name.as_str()));
    router.detach(id);

    // Whoever claims the name next inherits nothing and must earn its own.
    let (next, _) = router.attach(outbox());
    router.request_name(next, name.clone()).unwrap();
    assert_eq!(router.attestation_of(&name), None);
    assert!(router.resolve_attested(&name).is_err());
}

#[cfg(feature = "modules")]
#[test]
fn a_name_attestation_does_not_admit_the_unique_connection() {
    let mut router = Router::default();
    let (id, unique) = router.attach(outbox());
    let wallet = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    let voice = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
    router.request_name(id, wallet.clone()).unwrap();
    router.request_name(id, voice.clone()).unwrap();
    let verified = attestation(wallet.as_str());
    router.set_attestation(id, verified.clone());

    assert_eq!(router.attestation_of(&wallet), Some(verified));
    assert!(router.resolve_attested(&wallet).is_ok());
    assert_eq!(router.attestation_of(&voice), None);
    assert_eq!(
        router.resolve_attested(&voice).unwrap_err().wire_name(),
        Error::NOT_ATTESTED
    );
    assert_eq!(router.attestation_of(&unique), None);
    assert_eq!(
        router.resolve_attested(&unique).unwrap_err().wire_name(),
        Error::NOT_ATTESTED
    );
}

#[cfg(feature = "modules")]
#[test]
fn a_name_attestation_does_not_replace_an_explicitly_admitted_artifact() {
    let mut router = Router::default();
    let (id, unique) = router.attach(outbox());
    let wallet = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    let voice = BusName::new("ai.tinyhumans.openhuman.Voice").unwrap();
    router.request_name(id, wallet.clone()).unwrap();
    router.request_name(id, voice.clone()).unwrap();
    let admitted = attestation(wallet.as_str());
    router.set_attestation_for_unique(&unique, admitted.clone());
    let mut named = attestation(voice.as_str());
    named.sha256 = "b".repeat(64);
    router.set_attestation(id, named.clone());

    assert_eq!(router.attestation_of(&unique), Some(admitted.clone()));
    assert!(router.resolve_attested(&unique).is_ok());
    assert_eq!(router.attestation_of(&wallet), Some(admitted));
    assert!(router.resolve_attested(&wallet).is_ok());
    assert_eq!(router.attestation_of(&voice), Some(named));
    assert!(router.resolve_attested(&voice).is_ok());
}

#[cfg(feature = "modules")]
#[test]
fn a_fixed_unique_peer_retains_its_admitted_artifact_across_alias_handover_until_detach() {
    let mut router = Router::default();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let (id, unique) = router.attach(tx);
    let name = BusName::new("ai.tinyhumans.openhuman.Wallet").unwrap();
    let alias = BusName::new("ai.tinyhumans.openhuman.Extra").unwrap();
    router.request_name(id, name.clone()).unwrap();
    let admitted = attestation(name.as_str());
    router.set_attestation_for_unique(&unique, admitted.clone());
    assert_eq!(router.attestation_of(&unique), Some(admitted.clone()));
    let target = router.resolve_attested(&unique).unwrap();
    target
        .try_send(signal("ai.tinyhumans.Test", "Tick", "/"))
        .unwrap();
    assert_eq!(
        rx.try_recv().unwrap().header.member.unwrap().as_str(),
        "Tick"
    );
    router.request_name(id, alias.clone()).unwrap();
    assert_eq!(router.attestation_of(&alias), None);
    assert!(router.resolve_attested(&alias).is_err());
    router.release_name(id, &name).unwrap();
    let (successor, successor_unique) = router.attach(outbox());
    router.request_name(successor, name.clone()).unwrap();
    assert_eq!(router.attestation_of(&name), None);
    assert_eq!(router.attestation_of(&successor_unique), None);
    assert!(router.resolve_attested(&name).is_err());
    assert!(router.resolve_attested(&successor_unique).is_err());
    assert_eq!(router.attestation_of(&unique), Some(admitted));
    assert!(router.resolve_attested(&unique).is_ok());
    router.detach(id);
    assert_eq!(router.attestation_of(&unique), None);
    assert!(matches!(
        router.resolve_attested(&unique),
        Err(Error::NameHasNoOwner(_))
    ));
}
