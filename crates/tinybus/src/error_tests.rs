use super::*;

#[test]
fn feature_disabled_names_the_flag_to_rebuild_with() {
    let err = Error::FeatureDisabled("serving over a socket", "uds");
    assert_eq!(
        err.to_string(),
        "serving over a socket requires the `uds` feature; rebuild with --features uds"
    );
}

#[test]
fn a_method_failure_travels_under_its_own_name() {
    let err = Error::MethodFailed {
        name: "ai.tinyhumans.openhuman.Voice.Error.NoDevice".into(),
        message: "no capture device".into(),
    };
    assert_eq!(
        err.wire_name(),
        "ai.tinyhumans.openhuman.Voice.Error.NoDevice"
    );
}

#[test]
fn bad_arguments_keeps_the_diagnosis_and_drops_the_value() {
    // serde's own phrasing for a type mismatch, which quotes the value.
    let err = Error::bad_arguments(
        MemberName::new("Sign").unwrap(),
        "invalid type: string \"seed phrase here\", expected u64 at `0xdeadbeef`",
    );
    let text = err.to_string();
    assert!(text.contains("expected u64"), "{text}");
    assert!(!text.contains("0xdeadbeef"), "{text}");
    // The double-quoted half is the one serde uses for a rejected *string*,
    // which is the shape a token or a recovery phrase arrives in.
    assert!(!text.contains("seed phrase here"), "{text}");
}

#[test]
fn redaction_survives_an_unclosed_quote() {
    // A truncated message must not leak the tail just because its closing
    // backtick never arrived.
    assert_eq!(redact_values("bad token `abc"), "bad token `…");
    assert_eq!(redact_values("bad token \"abc"), "bad token \"…");
    assert_eq!(redact_values("no quotes here"), "no quotes here");
}

#[test]
fn a_backtick_inside_a_quoted_value_does_not_end_the_redaction_early() {
    // Otherwise a value chosen to contain a backtick would close the span
    // and put its own tail back into the message.
    assert_eq!(
        redact_values("invalid: \"a`b`c\", expected u64"),
        "invalid: \"…\", expected u64"
    );
}

#[test]
fn a_generic_failure_falls_back_to_the_failed_name() {
    assert_eq!(Error::failed("boom").wire_name(), Error::FAILED);
}

#[test]
fn missing_owner_names_the_integration_that_is_not_running() {
    let err = Error::NameHasNoOwner(BusName::try_from("ai.tinyhumans.openhuman.Voice").unwrap());
    assert_eq!(
        err.to_string(),
        "no peer owns the name `ai.tinyhumans.openhuman.Voice`"
    );
}

#[test]
fn module_refusal_sanitizes_an_untrusted_reason() {
    let error = Error::module_refused(
        std::path::Path::new("module.so"),
        "loader exposed /secret/path and spaces",
    );
    let Error::ModuleRefused { reason, .. } = error else {
        panic!("expected module refusal");
    };
    assert_eq!(reason, "loader exposed secretpath and spaces");
}

#[test]
fn module_refusal_uses_a_safe_filename_fallback() {
    let error = Error::module_refused(std::path::Path::new("/"), "refused");
    let Error::ModuleRefused { file, .. } = error else {
        panic!("expected module refusal");
    };
    assert_eq!(file, "module");
}

#[test]
fn every_structured_error_has_a_stable_wire_name() {
    let bus = BusName::new("ai.tinyhumans.Example").unwrap();
    let path = ObjectPath::new("/ai/tinyhumans/Example").unwrap();
    let interface = InterfaceName::new("ai.tinyhumans.Example").unwrap();
    let member = MemberName::new("Call").unwrap();
    let errors = [
        Error::InvalidName {
            kind: "name",
            input: "bad".into(),
            reason: "bad".into(),
        },
        Error::protocol("bad"),
        Error::transport("bad"),
        Error::ConnectionClosed,
        Error::Backpressure,
        Error::NameHasNoOwner(bus.clone()),
        Error::NameTaken {
            name: bus.clone(),
            owner: bus,
        },
        Error::UnknownObject { path: path.clone() },
        Error::UnknownInterface {
            path,
            interface: interface.clone(),
        },
        Error::UnknownMethod {
            interface,
            member: member.clone(),
        },
        Error::bad_arguments(member.clone(), "bad"),
        Error::invalid_domain("bad", "bad"),
        Error::Timeout {
            member,
            timeout_ms: 1,
        },
        Error::IncompatibleVersion {
            peer: "peer".into(),
            interface: "interface".into(),
            detail: "bad".into(),
        },
        Error::module_refused(std::path::Path::new("module.so"), "bad"),
        Error::ModuleUnavailable {
            module: "module".into(),
            state: "refused".into(),
            detail: "bad".into(),
        },
        Error::path("path", "bad"),
        Error::FeatureDisabled("thing", "uds"),
        Error::UnknownStream { id: "s1".into() },
        Error::StreamAborted {
            reason: "aborted".into(),
        },
        Error::StreamTooLarge { limit: 1 },
        Error::TooManyStreams { limit: 1 },
        Error::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
        Error::not_attested(BusName::new("ai.tinyhumans.Example").unwrap(), "bad"),
    ];
    for error in errors {
        assert!(error.wire_name().starts_with("ai.tinyhumans."));
    }
}

#[test]
fn module_unavailable_wire_name_remains_stable_across_lifecycle_states() {
    let unavailable = Error::ModuleUnavailable {
        module: "module".into(),
        state: "faulted".into(),
        detail: "safe".into(),
    };
    let initializing = Error::ModuleUnavailable {
        module: "module".into(),
        state: "initializing".into(),
        detail: "safe".into(),
    };
    assert_eq!(unavailable.wire_name(), Error::MODULE_UNAVAILABLE);
    assert_eq!(initializing.wire_name(), Error::MODULE_UNAVAILABLE);
}
