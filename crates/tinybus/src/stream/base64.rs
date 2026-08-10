//! Standard base64 (RFC 4648, padded), hand-rolled.
//!
//! Hand-rolled rather than depended upon because this crate's whole argument is
//! that the kernel's dependency graph is a liability, and a chunk codec is
//! forty lines. The decoder is strict — it rejects any byte outside the
//! alphabet, wrong padding, and a trailing group that carries bits which
//! encode nothing — because a lenient decoder makes two peers disagree about
//! what a chunk contained, and the length accounting on either side of a
//! stream has to match exactly.

use crate::error::{Error, Result};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode `bytes` as padded standard base64.
pub fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let b0 = group[0] as u32;
        let b1 = *group.get(1).unwrap_or(&0) as u32;
        let b2 = *group.get(2).unwrap_or(&0) as u32;
        let packed = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(packed >> 18) as usize & 63] as char);
        out.push(ALPHABET[(packed >> 12) as usize & 63] as char);
        out.push(if group.len() > 1 {
            ALPHABET[(packed >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if group.len() > 2 {
            ALPHABET[packed as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Decode padded standard base64.
///
/// The error never quotes the offending input: a chunk is user data by
/// definition, and this message travels back to the peer as an error reply.
pub fn decode(text: &str) -> Result<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(Error::protocol(
            "base64 chunk length is not a multiple of four",
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for (index, group) in bytes.chunks(4).enumerate() {
        let last = index == bytes.len() / 4 - 1;
        let mut packed = 0u32;
        let mut kept = 3;
        for (position, &byte) in group.iter().enumerate() {
            let value = match byte {
                b'=' => {
                    // Padding is only ever the last one or two symbols of the
                    // final group; anywhere else it is a corrupt chunk, not a
                    // shorter one.
                    if !last || position < 2 {
                        return Err(Error::protocol("base64 chunk has misplaced padding"));
                    }
                    // Only the *first* pad fixes the length; the second is more
                    // of the same padding, not a shorter group again.
                    kept = kept.min(position - 1);
                    0
                }
                _ => decode_symbol(byte)?,
            };
            packed = (packed << 6) | value as u32;
        }
        // A padded group must not carry bits below the bytes it encodes, or two
        // distinct texts would decode to one chunk.
        let slack = (3 - kept) * 8;
        if slack > 0 && packed & ((1 << slack) - 1) != 0 {
            return Err(Error::protocol("base64 chunk has non-canonical padding"));
        }
        for shift in (0..kept).map(|i| 16 - i * 8) {
            out.push((packed >> shift) as u8);
        }
    }
    Ok(out)
}

fn decode_symbol(byte: u8) -> Result<u8> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(Error::protocol("base64 chunk has a symbol outside the alphabet")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_length_modulo_three_round_trips() {
        for len in 0..=64usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            let text = encode(&bytes);
            assert_eq!(decode(&text).unwrap(), bytes, "len {len}");
        }
    }

    #[test]
    fn the_encoding_matches_the_rfc_test_vectors() {
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"f"), "Zg==");
        assert_eq!(encode(b"fo"), "Zm8=");
        assert_eq!(encode(b"foo"), "Zm9v");
        assert_eq!(encode(b"foob"), "Zm9vYg==");
        assert_eq!(encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn the_full_byte_range_survives_a_round_trip() {
        let bytes: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&bytes)).unwrap(), bytes);
    }

    #[test]
    fn a_truncated_group_is_rejected_rather_than_padded_silently() {
        assert!(decode("Zm9").is_err());
    }

    #[test]
    fn a_symbol_outside_the_alphabet_is_rejected() {
        assert!(decode("Zm9*").is_err());
        assert!(decode("Zm9 ").is_err());
    }

    #[test]
    fn padding_in_the_middle_of_a_chunk_is_rejected() {
        assert!(decode("Zg==Zg==").is_err());
        assert!(decode("=g==").is_err());
    }

    #[test]
    fn a_padded_group_carrying_bits_it_does_not_encode_is_rejected() {
        // "Zh==" decodes the same byte as "Zg==" under a lenient decoder.
        assert!(decode("Zh==").is_err());
        assert!(decode("Zm9=").is_err());
    }

    #[test]
    fn a_decode_failure_never_quotes_the_chunk_it_rejected() {
        let secret = "recovery-phrase!";
        let error = decode(secret).unwrap_err().to_string();
        assert!(!error.contains(secret), "{error}");
    }
}
