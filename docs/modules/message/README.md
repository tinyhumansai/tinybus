# `message`

The wire format: one `Message` type, four kinds, JSON bodies, length-prefixed
frames.

## Why JSON

The integrations on this bus are "transcribe a file" and "sign a transaction".
The call rate is human-scale and the body is dwarfed by the work it triggers. In
exchange: `tinybus monitor` is readable, a service can be written in any
language in an afternoon, and `serde` derives are the entire client binding.

Where this is the wrong trade is bulk binary payloads. That is what the
file-descriptor milestone is for; until then large payloads travel as paths, not
as base64.

## Why the header is flat

Routing reads `destination` and nothing else. A flat struct of `Option`s rather
than an enum per kind means the broker routes a message without knowing what
kind it is — so a future kind passes through an old broker instead of being
dropped.

## Why a length prefix

A body can legitimately contain a newline. With newline framing, the frame
boundary would depend on payload content and the encoder would have to escape
it. A length prefix makes reading a frame a fixed-cost operation nothing in the
body can confuse, and it lets the reader reject an oversized frame *before*
allocating for it — the length arrives before the bytes do.

## Invariants

- `validate()` runs once on ingress; every later stage indexes the header
  without re-checking.
- Absent header fields are omitted, not written as `null`. Signals outnumber
  everything on a busy bus and seven `null`s each is most of the framing cost.
- An error body is the message *without* the dotted name, which lives in
  `error_name`. Including it would make an error accrete a prefix each time it
  crossed the bus.
