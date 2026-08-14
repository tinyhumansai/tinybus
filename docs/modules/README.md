# Module documentation

One design reference per module in `crates/tinybus/src/`: a focused README when
the rationale exceeds source rustdoc, otherwise the module-level rustdoc itself.
Each records why the module exists, what it deliberately does not do, and which
behaviours other modules rely on.

| Module | Document |
| --- | --- |
| `name` | [name/README.md](name/README.md) |
| `message` | [message/README.md](message/README.md) |
| `ports` | [ports/README.md](ports/README.md) |
| `transport` | [transport/README.md](transport/README.md) |
| `router` | [router/README.md](router/README.md) |
| `broker` | [broker/README.md](broker/README.md) |
| `attest` | [attest/README.md](attest/README.md) |
| `connection` | [connection/README.md](connection/README.md) |
| `proxy` | [proxy/README.md](proxy/README.md) |
| `service` | [service/README.md](service/README.md) |
| `stream` | [stream/README.md](stream/README.md) |
| `secret` | [secret/README.md](secret/README.md) |
| `events` | source rustdoc (bounded domain-event fan-out) |
| `global` | source rustdoc (one-time process-wide bus) |
| `native` | source rustdoc (typed in-process request registry) |
| `version` | source rustdoc (interface compatibility records) |
| `module` | [module/README.md](module/README.md) |

See also [protocol.md](../protocol.md) for the wire format as a specification
rather than as Rust.
