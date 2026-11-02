//! Strict validation of ZIP streaming descriptors.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::Result;
use crate::module::refused;

/// Match a descriptor against central-directory claims and stay inside the
/// validated compressed-data-to-central-directory range.
pub(super) fn validate_data_descriptor(
    archive_path: &Path,
    offset: u64,
    directory_offset: u64,
    zip64: bool,
    expected_crc: u32,
    expected_compressed: u64,
    expected_expanded: u64,
) -> Result<bool> {
    const SIGNATURE: [u8; 4] = *b"PK\x07\x08";
    let max_size = if zip64 { 24 } else { 16 };
    let available = directory_offset
        .checked_sub(offset)
        .ok_or_else(|| refused("release archive has an invalid data descriptor"))?;
    if available < (if zip64 { 20u64 } else { 12u64 }) {
        return Ok(false);
    }
    let read_size = available.min(max_size) as usize;
    let mut bytes = [0u8; 24];
    let mut file = File::open(archive_path)
        .map_err(|_| refused("release archive has an invalid data descriptor"))?;
    file.seek(SeekFrom::Start(offset))
        .and_then(|_| file.read_exact(&mut bytes[..read_size]))
        .map_err(|_| refused("release archive has an invalid data descriptor"))?;

    for prefix in [0usize, 4] {
        if prefix == 4 && bytes[..4] != SIGNATURE {
            continue;
        }
        let required = prefix + if zip64 { 20 } else { 12 };
        if read_size < required {
            continue;
        }
        let crc = read_u32(&bytes[prefix..prefix + 4]);
        let (compressed, expanded) = if zip64 {
            (
                read_u64(&bytes[prefix + 4..prefix + 12]),
                read_u64(&bytes[prefix + 12..prefix + 20]),
            )
        } else {
            (
                u64::from(read_u32(&bytes[prefix + 4..prefix + 8])),
                u64::from(read_u32(&bytes[prefix + 8..prefix + 12])),
            )
        };
        if crc == expected_crc && compressed == expected_compressed && expanded == expected_expanded
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("fixed-width ZIP integer"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed-width ZIP integer"))
}

#[cfg(test)]
#[path = "zip_descriptor_tests.rs"]
mod tests;
