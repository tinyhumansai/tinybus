# The tinybus wire protocol

Version 1. This document is the specification a non-Rust service is written
against; the Rust types in `crates/tinybus/src/message/` are one implementation
of it.

## Framing

Every message is a 4-byte big-endian unsigned length followed by that many bytes
of UTF-8 JSON.

```
+--------+--------+--------+--------+---------------------------+
|            length (u32, BE)       |  JSON payload (length B)  |
+--------+--------+--------+--------+---------------------------+
```

A reader **must** reject an announced length above 16 777 216 (16 MiB) without
allocating for it, and close the connection. The length arrives before the
bytes, so trusting it is a remote-triggered allocation.

Newline framing is deliberately not used: bodies legitimately contain newlines,
and a frame boundary must not depend on payload content.

## Message

```json
{
  "header": {
    "kind": "method_call",
    "serial": 7,
    "reply_serial": null,
    "sender": ":1.4",
    "destination": "ai.tinyhumans.openhuman.Voice",
    "path": "/ai/tinyhumans/openhuman/Voice",
    "interface": "ai.tinyhumans.openhuman.Voice",
    "member": "Transcribe",
    "error_name": null
  },
  "body": ["/tmp/clip.wav"]
}
```

Absent fields are **omitted**, not written as `null`. A reader must treat an
omitted field and a `null` field identically.

| Field | Type | Present on |
| --- | --- | --- |
| `kind` | `method_call` \| `method_return` \| `error` \| `signal` | always |
| `serial` | u64, ≥ 1, per-sender monotonic | always |
| `reply_serial` | u64 | `method_return`, `error` |
| `sender` | bus name | stamped by the broker on everything it forwards |
| `destination` | bus name | `method_call`, `method_return`, `error` |
| `path` | object path | `method_call`, `signal` |
| `interface` | interface name | `method_call`, `signal` |
| `member` | member name | `method_call`, `signal` |
| `error_name` | dotted string | `error` |

`body` is a positional JSON array for calls and signals, and a single JSON value
for returns. `error` bodies are a string: the human-readable message, without
the error name, which travels in `error_name`.

A peer **may** set `sender`; the broker overwrites it unconditionally. Nothing
downstream may trust a `sender` that did not come from the broker.

## Names

| Kind | Grammar |
| --- | --- |
| Unique bus name | `:` then dot-separated runs of digits — `:1.7` |
| Well-known bus name | ≥ 2 dot-separated elements, each `[A-Za-z_][A-Za-z0-9_-]*` |
| Interface name | as well-known bus name |
| Object path | `/`, or `/` then `/`-separated elements of `[A-Za-z0-9_]+` |
| Member name | `[A-Za-z_][A-Za-z0-9_]*` |

Every component is capped at 255 bytes. Members admit no `.` so that a match
rule is unambiguous.

## The handshake

1. Connect.
2. Send `Hello` (see below). Send nothing else first.
3. The broker replies with your unique name as a JSON string.

A peer that calls anything before `Hello` has no unique name for the broker to
address a reply to.

## The bus's own interface

Destination `ai.tinyhumans.tinybus.Bus`, path `/ai/tinyhumans/tinybus/Bus`,
interface `ai.tinyhumans.tinybus.Bus`.

| Member | Body | Returns |
| --- | --- | --- |
| `Hello` | `[]` | your unique name |
| `GetId` | `[]` | the broker's id, stable for its process lifetime |
| `Ping` | `[]` | `null` |
| `RequestName` | `[name]` | `true`, or an error if taken |
| `ReleaseName` | `[name]` | `true` |
| `ListNames` | `[]` | every owned name, unique names included |
| `Announce` | `[manifest]` | `true`; records this peer's interface versions |
| `GetManifest` | `[name]` | that peer's manifest, or `null` |
| `ListPeers` | `[]` | unique names, owned names, and peer manifests |
| `GetNameOwner` | `[name]` | the owner's unique name, or `null` |
| `AddMatch` | `[rule]` | `null` |
| `RemoveMatch` | `[rule]` | `null` |
| `ListModules` | `[]` | every module known to the embedded host |
| `GetModule` | `[name]` | module identity, ABI facts and state, or `null` |
| `GetModuleManifest` | `[name]` | the declared module manifest, or `null` |
| `LoadModule` | `[path]` | the newly loaded module record |
| `StopModule` | `[name, deadline_ms]` | the stopped module record |
| `EnableModule` | `[name, on]` | the updated module record |
| `RescanModules` | `[]` | modules loaded from configured search paths |

`ai.tinyhumans.tinybus.Bus` is reserved; `RequestName` for it always fails. So
does `RequestName` for a unique name.

The bus always emits `NameOwnerChanged`, with body
`[name, old_owner, new_owner]`, either owner being `null`. This is how a peer
learns a service died without polling it.

An embedded module host also exposes the additive module members above. A
broker without one returns `UnknownMethod`; the wire protocol version remains
1 because old peers can still parse every message. Module state changes are
observable through name ownership changes when a module attaches or stops.

## Match rules

Comma-separated `key=value`. Unset keys match anything; every set key must
match. Values are unquoted and cannot contain a comma — no name grammar admits
one.

```
type=signal,interface=ai.tinyhumans.openhuman.Mail,member=Received
type=signal,path_namespace=/ai/tinyhumans/openhuman/Mail
sender=ai.tinyhumans.tinybus.Bus,member=NameOwnerChanged
```

Keys: `type`, `sender`, `interface`, `member`, `path`, `path_namespace`. An
unknown key is an error, not an ignored clause — silently dropping one would
*widen* a subscription the peer asked to narrow.

A signal is never delivered back to the peer that emitted it.

## Errors

An `error` reply carries `error_name` and a string body. Callers match on
`error_name`, which is stable; the body is prose and may be reworded.

Bus-generated names:

| Name | Meaning |
| --- | --- |
| `…Error.NameHasNoOwner` | nothing owns the destination — the service is not running |
| `…Error.NameTaken` | `RequestName` lost |
| `…Error.UnknownObject` | the peer exports no object at that path |
| `…Error.UnknownInterface` | the object does not implement that interface |
| `…Error.UnknownMethod` | the interface has no such member |
| `…Error.BadArguments` | the body did not match the member's signature |
| `…Error.Failed` | a method failed with no more specific mapping |

(`…` is `ai.tinyhumans.tinybus`.) A service should define its own dotted names
under its own interface — `ai.tinyhumans.openhuman.Voice.Error.NoDevice` — for
anything a caller might reasonably branch on.

An error message must never contain the value that caused it. Bodies carry
credentials.
