//! Bounded output copying and strict raw-DEFLATE stream validation.

use std::io::{self, Read, Write};

use crate::error::Result;
use crate::module::refused;

pub(super) fn copy_bounded(
    input: &mut impl Read,
    output: &mut impl Write,
    remaining: u64,
    actual: &mut u64,
) -> Result<()> {
    let mut limited = input.take(remaining.saturating_add(1));
    *actual =
        io::copy(&mut limited, output).map_err(|_| refused("release archive extraction failed"))?;
    if *actual > remaining {
        return Err(refused("release archive exceeds the extraction limit"));
    }
    Ok(())
}

/// Requires a real DEFLATE end marker and exact input consumption.
/// The high-level flate2 reader can report EOF for an incomplete stream.
pub(super) struct StrictDeflate<R> {
    input: R,
    decoder: flate2::Decompress,
    buffer: [u8; 8192],
    position: usize,
    length: usize,
    expected_input: u64,
    ended: bool,
}

impl<R> StrictDeflate<R> {
    pub(super) fn new(input: R, expected_input: u64) -> Self {
        Self {
            input,
            decoder: flate2::Decompress::new(false),
            buffer: [0; 8192],
            position: 0,
            length: 0,
            expected_input,
            ended: false,
        }
    }
}

impl<R: Read> Read for StrictDeflate<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.ended {
            return Ok(0);
        }
        loop {
            if self.position == self.length {
                self.length = self.input.read(&mut self.buffer)?;
                self.position = 0;
            }
            let eof = self.length == 0;
            let input = &self.buffer[self.position..self.length];
            let before_in = self.decoder.total_in();
            let before_out = self.decoder.total_out();
            let status = self
                .decoder
                .decompress(
                    input,
                    output,
                    if eof {
                        flate2::FlushDecompress::Finish
                    } else {
                        flate2::FlushDecompress::None
                    },
                )
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid DEFLATE stream")
                })?;
            let consumed = (self.decoder.total_in() - before_in) as usize;
            let produced = (self.decoder.total_out() - before_out) as usize;
            self.position += consumed;

            if status == flate2::Status::StreamEnd {
                if self.decoder.total_in() != self.expected_input {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DEFLATE stream did not consume its complete entry",
                    ));
                }
                self.ended = true;
                return Ok(produced);
            }
            if produced != 0 {
                return Ok(produced);
            }
            if eof || (consumed == 0 && self.position < self.length) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated DEFLATE stream",
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "zip_deflate_tests.rs"]
mod tests;
