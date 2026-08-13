# Repository Guidelines

## Project Structure & Module Organization

- `crates/tinybus/` — the bus. One responsibility per module, core types in a
  module-local `types.rs` where a module grows one, and every port in
  `src/ports/` is one trait in one file.
- `crates/tinybus-macros/` — `#[interface]`, and nothing else. A proc-macro
  crate compiles for the host, so anything that does not have to live there
  does not.
- `crates/tinybus/src/bin/tinybus.rs` — the CLI. Every subcommand is declared
  even when its milestone has not landed, so scripts and runbooks can be written
  against a stable surface.
- `crates/tinybus/examples/` — declared explicitly in `Cargo.toml` with
  `required-features`, so an example can never be built by a job that has no
  transport compiled in.
- `docs/modules/<module>/README.md` — one document per `src/` module.

**Integrations are not members of this workspace.** An integration is a separate
process in its own repository with its own dependency graph — that is the entire
point. A crate added here that pulls in a media codec has recreated the problem
tinybus exists to solve.

## Build, Test, and Development Commands

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo check --locked --no-default-features   # the slim kernel build
```

The default build links a Unix socket and a proc-macro. The
`--no-default-features` build links neither, and it must keep compiling: it is
what an embedded OpenHuman build uses, and it is the configuration that proves
the ports are real seams rather than decoration.

## Coding Style & Naming Conventions

- rustfmt output, Rust 2024 idioms. `snake_case` modules and files,
  `PascalCase` types.
- Return `Result<T>` using the crate error type from `src/error.rs`.
- Every file opens with a `//!` module doc describing its role and any feature
  gating.
- Comments explain the *decision*, not the code. When an ordering, a bound, or a
  lock choice is load-bearing, say so and say why. "Bounded so a slow service
  cannot grow kernel memory" is a comment; "bounded channel" is not.
- Public items carry doc comments. Clap fields use `///` — it becomes the help
  text.

## Testing Guidelines

- Tests live **in-crate**: a `#[cfg(test)] mod tests` block at the bottom of the
  module, moving to a sibling `test.rs` or `<name>_test.rs` when they grow.
  There is no `tests/` directory.
- Tests run on the in-memory transport. A test that needs a socket is testing
  the socket, and belongs in `transport::unix`.
- Concurrency behaviour is asserted, not assumed: the timeout, the wedged peer,
  the dropped service and the forged sender each have a named test, because each
  is a property the kernel is relying on rather than an implementation detail.
- Test names state the property, not the function under test.
  `a_service_dying_releases_its_name_and_announces_it`, not `test_detach`.
- No `sleep` as synchronisation. Use the in-memory transport and await the thing
  you are waiting for; a `tokio::time::timeout` around a receive is a deadline,
  not a sleep.
- Maintain at least 80% coverage for meaningful library behaviour.

## Documentation Expectations

Keep every Markdown file, including this one, at 500 lines or fewer. When a
topic grows past that limit, split it into focused files and link them from the
module's `README.md`.

## Protocol Compatibility

The wire format is a contract between processes built at different times, from
different repositories, by different people. Treat it accordingly:

- Adding an optional header field or a struct field is compatible. Removing one,
  renaming one, or changing its type is not.
- A breaking change to an interface ships as a **new interface name**
  (`Voice2`), never as a redefinition. A kernel and a service built months apart
  must either speak or fail loudly at the first call.
- `PROTOCOL_VERSION` is bumped only when an older peer cannot parse the new
  format at all.

## Security Boundary

These are invariants, not preferences. Changing any of them needs an explicit
discussion in the pull request:

- **The broker never parses a body.** It reads the header, routes, and forwards.
  A broker that inspected bodies would be a process that has seen every
  credential, mail body and recovery phrase on the bus.
- **`sender` is stamped by the broker and overwritten on ingress.** A peer that
  could set it could impersonate the kernel to every service on the bus. Every
  authorisation decision anywhere depends on this one line.
- **Errors never carry the value that caused them.** `Error::bad_arguments`
  redacts backtick-quoted spans out of serde's messages for exactly this
  reason; new error paths must not reintroduce the leak.
- **The socket lives under the user's runtime directory**, never `/tmp`, and the
  bus never binds a TCP port. Filesystem permissions are the current access
  control story in full.
- **Every call has a deadline.** It cannot be disabled. A call with no deadline
  reintroduces the hang that motivated the project.
- **A confidential message goes to a verified recipient or to nobody.** The
  broker refuses to deliver one unless it has itself hashed the destination's
  artifact and matched it against the operator's trust store — never against
  anything the peer claimed. It is never fanned out to a subscriber, never
  printed by `monitor`, and never carried by a signal. The flag is the one
  header field the broker does not overwrite on ingress, because it can only
  ever restrict the sender's own traffic. See `docs/modules/attest/README.md`.
- **A misbehaving peer must not affect another peer.** Bounded per-peer queues,
  best-effort signal delivery, and an accept loop that survives a bad client are
  all this invariant. Any change that lets one peer's slowness reach another's
  latency is a bug, not a tuning question.
- **In-process modules are inside the trust boundary, not outside it.** The
  misbehaving-peer invariant applies to peers reached across a real transport
  boundary. A module loaded with `dlopen` shares the host address space and is
  trusted exactly as much as the host's own code. Bounded queues, deadlines and
  caught panics bound misbehaviour; they cannot contain a segfault, abort, heap
  corruption, OOM, or deliberate memory access. An integration whose crash or
  compromise must not reach the kernel belongs in a separate process.
