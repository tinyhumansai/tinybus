use super::*;

fn encoded(bytes: &[u8]) -> Vec<u8> {
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn reads_a_complete_stream_and_stops_at_its_end_marker() {
    let original = b"the whole deflate stream";
    let compressed = encoded(original);
    let mut decoder = StrictDeflate::new(compressed.as_slice(), compressed.len() as u64);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(decoder.read(&mut [0; 1]).unwrap(), 0);
}

#[test]
fn refuses_truncated_and_trailing_deflate_bytes() {
    let mut truncated = encoded(b"payload whose final DEFLATE marker matters");
    truncated.pop();
    let mut decoder = StrictDeflate::new(truncated.as_slice(), truncated.len() as u64);
    assert!(decoder.read_to_end(&mut Vec::new()).is_err());

    let compressed = encoded(b"payload");
    let mut trailing = compressed.clone();
    trailing.push(0);
    let mut decoder = StrictDeflate::new(trailing.as_slice(), trailing.len() as u64);
    assert!(decoder.read_to_end(&mut Vec::new()).is_err());
}

#[test]
fn output_copy_allows_exact_limit_but_refuses_one_byte_over() {
    let input = b"1234";
    let mut output = Vec::new();
    let mut actual = 0;
    copy_bounded(&mut &input[..], &mut output, 4, &mut actual).unwrap();
    assert_eq!(output, input);
    assert_eq!(actual, 4);

    let mut output = Vec::new();
    assert!(copy_bounded(&mut &input[..], &mut output, 3, &mut actual).is_err());
    assert_eq!(actual, 4);
}
