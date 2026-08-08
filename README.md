# tinybus

A zbus-style message bus that keeps external integrations out of the OpenHuman
kernel's dependency graph.

## The problem

OpenHuman's dependency graph grew by absorption. Its `Cargo.toml` currently
carries, among others:

| Feature | What it drags in |
| --- | --- |
| Dictation | `whisper-rs`, `cpal`, `hound` |
| Documents | `pdf-extract`, `ppt-rs`, `docx-rs` |
| Wallets | `ethers-*`, `bitcoin`, `ed25519-dalek`, `curve25519-dalek`, `coins-bip39` |
| Browser control | `fantoccini` |
| Desktop control | `enigo`, `rdev`, `arboard` |
| Archives | `tar`, `xz2`, `zip`, `flate2` |
| Mail | `lettre` |

None of it is kernel logic. All of it is kernel build time, kernel binary size,
kernel CVE surface — and a panic anywhere in it is a kernel panic. Worse, none
of it can be *removed* by a user who does not want the feature: the closure is
linked whether or not the code path is ever taken.

## The shape of the fix

An integration becomes a **service**: its own process, its own `Cargo.toml`, its
own crash domain, announcing a well-known name on a bus. The kernel keeps a
proxy.

```rust
// The kernel's entire dependency on the speech stack, after the extraction.
let voice = connection.proxy(
    "ai.tinyhumans.openhuman.Voice",
    "/ai/tinyhumans/openhuman/Voice",
    "ai.tinyhumans.openhuman.Voice",
)?;
let transcript: Transcript = voice.call("Transcribe", ("/tmp/clip.wav",)).await?;
```

```rust
// The service's side.
#[tinybus::interface(name = "ai.tinyhumans.openhuman.Voice")]
impl Voice {
    async fn transcribe(&self, path: String) -> tinybus::Result<Transcript> { … }
    async fn languages(&self) -> tinybus::Result<Vec<String>> { … }
}
```

The model is D-Bus's, because the model is right and everyone already knows it:
unique names (`:1.7`) and well-known names (`ai.tinyhumans.openhuman.Voice`),
objects at paths, interfaces on objects, methods you call and signals you
subscribe to with match rules.

## What you get that a plain RPC crate does not

- **A wedged integration cannot wedge the kernel.** Every call has a deadline,
  and every peer sits behind its own bounded queue.
- **Death is an event, not a timeout.** When a service exits, the broker
  releases its name and emits `NameOwnerChanged`; the kernel finds out in
  milliseconds instead of thirty seconds later.
- **One integration, many consumers.** Services call each other over the same
  bus; there is no hub-and-spoke through the kernel.
- **You can watch it.** `tinybus monitor` prints traffic live.
- **The same code runs in-process.** The in-memory transport is a first-class
  transport, so a slim build can host an integration inside the kernel without
  changing a line of either side.

## Quick start

```sh
cargo run --bin tinybus -- serve                        # terminal 1: the broker
cargo run --example voice_service --all-features        # terminal 2: a service
cargo run --example kernel_client --all-features        # terminal 3: the kernel side

tinybus list                                            # who is on the bus
tinybus doctor                                          # is the bus healthy
tinybus monitor 'type=signal'                           # watch traffic
tinybus call ai.tinyhumans.openhuman.Voice \
    /ai/tinyhumans/openhuman/Voice \
    ai.tinyhumans.openhuman.Voice Transcribe '["/tmp/clip.wav"]'
```

## Layout

```
crates/tinybus/          the bus: protocol, broker, connection, proxy
  src/message/           the wire format and its framing
  src/name/              validated addresses (bus name, path, interface, member)
  src/ports/             the two seams: Transport and Listener
  src/transport/         in-memory (always) and Unix socket (feature `uds`)
  src/router.rs          the routing table and match rules
  src/broker.rs          the daemon
  src/connection.rs      a peer's link: calls out, dispatch in
  src/proxy.rs           the client handle — the kernel's whole API surface
  src/service/           the Interface trait and the object tree
  src/bin/tinybus.rs     the CLI
crates/tinybus-macros/   #[interface]
docs/modules/            one document per module
```

## Features

| Feature | Default | What it adds |
| --- | --- | --- |
| `uds` | yes | Unix-socket transport — the production one |
| `macros` | yes | `#[tinybus::interface]` |
| `cli` | yes | the `tinybus` binary |

`--no-default-features` leaves the protocol, the router, the broker and the
in-memory transport: no sockets, no proc-macro build step, no CLI. That is the
configuration a slim kernel build embeds.

## Status

Milestone 1. The core protocol, broker, connection, proxy, macro, Unix
transport and CLI are implemented and tested. Service activation, file-descriptor
passing, per-peer authorisation policy and the Windows named-pipe backend are
not — see `ROADMAP.md`.

## Licence

GPL-3.0-only.
