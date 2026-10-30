use tinybus::module::Availability;

#[test]
fn public_availability_serialization_is_available_without_the_loader() {
    let cases = [
        (Availability::Available, "available"),
        (Availability::Loading, "loading"),
        (Availability::Ready, "ready"),
        (Availability::Failed, "failed"),
        (Availability::Unsupported, "unsupported"),
        (Availability::Disabled, "disabled"),
    ];

    for (availability, wire_name) in cases {
        let json = serde_json::to_string(&availability).unwrap();
        assert_eq!(json, format!("\"{wire_name}\""));
        assert_eq!(
            serde_json::from_str::<Availability>(&json).unwrap(),
            availability
        );
    }
}
