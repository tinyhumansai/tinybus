# `name`

The four validated newtypes every address is made of: `BusName`, `ObjectPath`,
`InterfaceName`, `MemberName`.

## Why newtypes

Addresses are parsed once, at construction, and are infallible thereafter. The
router never asks whether a destination is well-formed — a `BusName` that exists
is a `BusName` that is valid. Discovering a malformed address halfway through a
dispatch table is how messages get delivered to the wrong peer.

The only place an unvalidated name can enter is the wire, and `serde`'s
`try_from = "String"` closes that: a malformed name in a frame fails to
deserialize rather than becoming a routing-table key.

## Why the grammars are narrower than D-Bus's

ASCII only, no leading digit per element, 255-byte cap. A name is therefore
always a legal identifier in Rust, a legal filename, and something a human can
grep for in a log. The cap is not style: names arrive from the wire and become
hash-map keys, so an unbounded name is an unbounded allocation keyed by
attacker-controlled input.

`MemberName` bars `.` so a match rule stays unambiguous.

## `starts_with`

`ObjectPath::starts_with` is subtree containment, not string prefix:
`/ai/Mailbox` does not start with `/ai/Mail`. Getting this wrong makes a client
that subscribed to one account's signals quietly receive another's.

## See also

- `docs/protocol.md` for the grammars as a specification
