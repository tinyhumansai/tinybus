//! Framing: a 4-byte big-endian length, then that many bytes of JSON.
//!
//! Newline-delimited JSON would have been shorter to write and is what most
//! JSON-RPC-over-socket code does. It is the wrong choice here for one reason:
//! a body can legitimately contain a newline (a transcript, a mail body, an
//! error backtrace), and the encoder would then have to escape it, meaning the
//! frame boundary depends on the *content* of the payload. A length prefix
//! makes reading a frame a fixed-cost operation that cannot be confused by
//! anything a caller puts in the body, and it lets the reader reject an
//! oversized frame before allocating for it.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// The largest frame the reader will allocate for.
///
/// A hard cap, not a tunable. The frame length arrives from the wire *before*
/// the bytes do, so without this the first four bytes of a hostile or corrupt
/// stream are a 4 GiB allocation. 16 MiB is far above any legitimate control
/// message; a payload that does not fit goes through [`crate::stream`], which
/// splits it into chunks that do, rather than through a larger cap here.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// The length prefix's width, in bytes.
pub const LENGTH_PREFIX_LEN: usize = 4;

/// Encode `value` into a length-prefixed frame.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)?;
    if payload.len() > MAX_FRAME_LEN {
        return Err(Error::protocol(format!(
            "frame of {} bytes exceeds the {MAX_FRAME_LEN}-byte cap",
            payload.len()
        )));
    }
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_LEN + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Read the payload length out of a length prefix, rejecting oversized frames.
pub fn decode_length(prefix: [u8; LENGTH_PREFIX_LEN]) -> Result<usize> {
    let len = u32::from_be_bytes(prefix) as usize;
    if len > MAX_FRAME_LEN {
        return Err(Error::protocol(format!(
            "peer announced a {len}-byte frame, over the {MAX_FRAME_LEN}-byte cap"
        )));
    }
    Ok(len)
}

/// Decode a payload that has already been read in full.
pub fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};

    fn sample() -> Message {
        Message::method_call(
            BusName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            ObjectPath::new("/ai/tinyhumans/openhuman/Voice").unwrap(),
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            MemberName::new("Transcribe").unwrap(),
            // A body containing the delimiter a newline-framed codec would
            // have choked on.
            serde_json::json!(["line one\nline two"]),
        )
    }

    #[test]
    fn a_frame_round_trips_with_its_newlines_intact() {
        let frame = encode(&sample()).unwrap();
        let len = decode_length(frame[..4].try_into().unwrap()).unwrap();
        assert_eq!(len, frame.len() - LENGTH_PREFIX_LEN);
        let decoded: Message = decode(&frame[LENGTH_PREFIX_LEN..]).unwrap();
        assert_eq!(decoded, sample());
    }

    #[test]
    fn an_announced_oversize_frame_is_rejected_before_allocating() {
        let err = decode_length(u32::MAX.to_be_bytes()).unwrap_err();
        assert!(err.to_string().contains("over the"), "{err}");
    }

    #[test]
    fn an_oversize_payload_is_refused_at_encode_time() {
        let huge = "x".repeat(MAX_FRAME_LEN + 1);
        let err = encode(&serde_json::json!(huge)).unwrap_err();
        assert!(err.to_string().contains("exceeds the"), "{err}");
    }
}
