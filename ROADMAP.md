# Roadmap

Milestones, in the order they unblock things. Each one is shippable on its own;
nothing here is a prerequisite for the kernel starting to move integrations out,
which is the point of ordering it this way.

## M1 — the bus (done)

The protocol, the broker, the connection, the proxy, `#[interface]`, the Unix
transport, the CLI. Enough that an integration can be extracted today and the
kernel can call it, watch it, and notice it dying.

- [x] Validated addresses, framed JSON messages, positional bodies
- [x] Broker: unique names, well-known names, routing, match rules,
      `NameOwnerChanged`
- [x] Connection: concurrent calls by serial, per-call deadlines, inbound
      dispatch, signal subscription
- [x] `#[tinybus::interface]` with `skip` and `name` overrides
- [x] In-memory and Unix transports behind one port
- [x] `tinybus serve | call | emit | list | monitor | doctor`

## M2 — moving the first integrations

The bus is a means; this is the end. Each extraction is a separate repository
with its own graph, and the kernel drops the corresponding dependencies in the
same pull request — an extraction that leaves the dependency behind has
achieved nothing.

Ordered by how much they cost the kernel today, and by how self-contained they
are:

1. **Documents** — `pdf-extract`, `ppt-rs`, `docx-rs`. Pure transformation, no
   state, no credentials. The easiest possible first cut.
2. **Archives** — `tar`, `xz2`, `zip`, `flate2`. Same shape, and the Node
   runtime bootstrap is its only caller.
3. **Voice** — `whisper-rs`, `cpal`, `hound`, plus the macOS/Metal `#[cfg]`
   fork. Also removes a platform-conditional dependency, which is worth more
   than its byte count.
4. **Browser control** — `fantoccini`. Already talks to an external process; the
   bus just moves the boundary one hop.
5. **Wallets** — `ethers-*`, `bitcoin`, `*-dalek`, `coins-bip39`. Highest value
   and last, because it is the one where the security boundary has to be right
   before the extraction, not after (see M4).

Deliberately **not** on this list: `tinyagents`, `tinycortex`, `tinyflows`,
`tinyjuice`, `tinychannels`. Those are the kernel's own logic, split across
crates. Moving them behind a bus would add latency and a serialisation boundary
to the hot path and remove nothing.

## M3 — trusted in-process modules

OpenHuman may embed the broker and load selected integrations as `cdylib`
modules. Each module is an ordinary peer connected at the `Transport` seam,
with its own runtime and bounded queues. The ABI gate rejects incompatible
artifacts before calling their initialization entrypoint.

This is deployment convenience, not process isolation. Loading a library runs
arbitrary code with the host's privileges and puts it inside the host's memory
and crash boundary. Integrations that hold especially sensitive material or
need fault containment remain separate processes.

- [x] Versioned, C-shaped descriptor and manifest ABI
- [x] Module-side runtime and export macro
- [x] Host transport bridge, admission gate, dependency ordering and loader
- [x] Linux, macOS and Windows loader implementations
- [x] Bus and CLI module inspection/control surface

## M4 — the security boundary

The bus is a capability handle: anything that can connect can ask the wallet to
sign. Filesystem permissions on the socket are the whole story today, which is
adequate for one user's own processes and inadequate the moment a service is
meant to be reachable by some peers and not others.

- [ ] Peer credentials from `SO_PEERCRED`, stamped alongside `sender`
- [ ] A policy file: which peer may call which interface on which name
- [ ] Per-interface, not per-name: "may read mail" and "may send mail" are
      different capabilities on one service
- [ ] An audit signal, so a denied call is visible rather than silent

## M5 — bulk payloads

Bodies are JSON, and a transcript or a rendered PDF should not be base64 in a
JSON string. Two options, and the choice is not obvious yet:

- [ ] File-descriptor passing over `SCM_RIGHTS`, which is zero-copy and Unix-only
- [ ] A side-channel content store the bus hands out handles to, which works
      everywhere and costs a write

Until this lands, the convention is that large payloads travel as paths and the
sender is responsible for the file's lifetime.

## M6 — other platforms and other languages

- [ ] Windows named-pipe transport, as a sibling of `transport::unix`
- [ ] A published wire-protocol document, so a service can be written in
      something other than Rust
- [ ] A TypeScript client, because some integrations genuinely want the Node
      ecosystem and shelling out to it is worse than speaking the protocol

## Not planned

- **A network transport.** The bus is local by construction, and every
  authorisation argument in M4 assumes it. Remote access belongs behind a
  service that speaks the bus locally and something authenticated remotely.
- **Queued name ownership.** D-Bus lets a second claimant wait in line behind
  the current owner. Two live processes both able to answer as the wallet is a
  worse failure than a clear startup error.
- **Broker-side body inspection.** Routing reads the header. A broker that
  parsed bodies would be a process that has seen every credential on the bus.
