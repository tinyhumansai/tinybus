use super::*;

#[test]
fn an_allowlist_reads_entries_and_ignores_comments_and_sections() {
    let source = "# a comment\n[section]\n\"clock.so\" = \"AABB\" # trailing\n\n";
    let entries: Vec<_> = parse_allowlist(source).collect();
    assert_eq!(entries, vec![("clock.so".to_string(), "aabb".to_string())]);
}

#[test]
fn an_allowlist_line_without_an_assignment_is_skipped_rather_than_guessed_at() {
    assert_eq!(parse_allowlist("garbage\n").count(), 0);
}

#[test]
fn only_a_full_length_hex_digest_counts_as_a_hash() {
    assert!(is_hex_sha256(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    ));
    // Too short, and a plausible-looking typo that must not be accepted as
    // a digest — the allowlist is the only thing standing between an
    // arbitrary artifact and a private key.
    assert!(!is_hex_sha256("e3b0c442"));
    assert!(!is_hex_sha256(&"z".repeat(64)));
}

#[test]
fn an_attestation_without_a_library_digest_remains_compatible() {
    let value = serde_json::json!({
        "name": "ai.tinyhumans.test.Legacy",
        "sha256": "a".repeat(64),
    });
    let attestation: Attestation = serde_json::from_value(value).unwrap();
    assert_eq!(attestation.library_sha256, None);
}
