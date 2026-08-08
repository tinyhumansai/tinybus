# `proxy`

A typed handle to one interface on one remote object. This is the entire surface
the OpenHuman kernel is meant to depend on.

```rust
let voice = connection.proxy(NAME, PATH, INTERFACE)?;
let transcript: Transcript = voice.call("Transcribe", ("/tmp/clip.wav",)).await?;
```

The type parameters do the work the deleted dependency used to: `R` is checked
against what actually came back, so a service that changes its return shape
fails at the caller with a deserialize error naming the mismatch, rather than
silently producing a default.

## Per-proxy timeouts

The natural unit is the integration, not the call: a wallet signature and a PDF
render have different reasonable waits, and every call to one shares its wait.

## `is_available`

Worth calling before a first call in a startup path. "The integration is not
installed" and "the call failed" are different things to tell a user, and only
this distinguishes them.

## `receive_signal`

Narrower than `Connection::add_match` on purpose: a proxy knows its own address,
so a subscription built from it cannot accidentally match another account's
traffic.

`Debug` is hand-written so the connection — which holds the transport, which may
name a socket path — never lands in a log line.
