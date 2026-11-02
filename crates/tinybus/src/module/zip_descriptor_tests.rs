use super::*;

fn descriptor(zip64: bool, signature: bool, crc: u32, compressed: u64, expanded: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    if signature {
        bytes.extend_from_slice(b"PK\x07\x08");
    }
    bytes.extend_from_slice(&crc.to_le_bytes());
    if zip64 {
        bytes.extend_from_slice(&compressed.to_le_bytes());
        bytes.extend_from_slice(&expanded.to_le_bytes());
    } else {
        bytes.extend_from_slice(&(compressed as u32).to_le_bytes());
        bytes.extend_from_slice(&(expanded as u32).to_le_bytes());
    }
    bytes
}

fn validate(bytes: &[u8], zip64: bool, crc: u32, compressed: u64, expanded: u64) -> Result<bool> {
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("descriptor.zip");
    std::fs::write(&archive, bytes).unwrap();
    validate_data_descriptor(
        &archive,
        0,
        bytes.len() as u64,
        zip64,
        crc,
        compressed,
        expanded,
    )
}

#[test]
fn matches_signed_and_unsigned_32_bit_descriptors() {
    for signed in [true, false] {
        let bytes = descriptor(false, signed, 0x1234, 17, 29);
        assert!(validate(&bytes, false, 0x1234, 17, 29).unwrap());
    }
}

#[test]
fn matches_signed_and_unsigned_zip64_descriptors() {
    for signed in [true, false] {
        let bytes = descriptor(true, signed, 0x1234, 1 << 33, 1 << 34);
        assert!(validate(&bytes, true, 0x1234, 1 << 33, 1 << 34).unwrap());
    }
}

#[test]
fn rejects_wrong_fields_truncation_and_out_of_range_offsets() {
    let valid = descriptor(true, true, 0x1234, 17, 29);
    assert!(!validate(&valid, true, 0x5678, 17, 29).unwrap());
    assert!(!validate(&valid[..10], true, 0x1234, 17, 29).unwrap());

    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("descriptor.zip");
    std::fs::write(&archive, &valid).unwrap();
    assert!(validate_data_descriptor(&archive, 1, 0, true, 0x1234, 17, 29,).is_err());
}
