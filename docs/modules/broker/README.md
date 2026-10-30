# `broker`

Accepts peers, owns the routing table, answers the bus's own interface.

## Shape

One accept loop, and per peer a reader task and a writer task with a bounded
queue between them. The queue is what makes a slow peer *its own* problem: a
service that stops reading fills its queue and senders to it drop or block, but
no other peer's traffic is delayed. A single shared outbound path would let one
wedged integration stall the bus — precisely the failure mode integrations are
being moved out of the kernel to avoid. There is a test for exactly this
(`one_wedged_peer_does_not_stall_another_peers_traffic`).

## What it does not do

- **It does not start services.** Activation is M3; see `ROADMAP.md`.
- **It does not parse bodies.** It reads the header, routes, forwards. A broker
  that inspected bodies would be a process that has seen every credential on the
  bus.
- **It does not trust `sender`.** The field is overwritten on ingress. A peer
  that could set it could impersonate the kernel to every service attached.

## Error replies

A call that cannot be routed gets an error reply from the broker itself.
Without that, the caller waits out its full timeout for a message that was
never going anywhere — and "the integration is not running" would be
indistinguishable from "the integration is slow".

## `NameOwnerChanged`

Emitted when a well-known name is claimed or released, and when a peer detaches
for its unique name. The unique-name transition identifies peer death even
when that peer never claimed an alias. This is how the kernel learns an
integration is gone in milliseconds rather than at the next timeout.
Subscribers still have to ask for it; the broker does not push it at peers
that did not.

## Restricted module control (`modules` feature)

`Broker::new()` keeps legacy permissive module-control behavior. Embeddings
that pass tenant credentials or actor grants to modules should choose
`Broker::new_restricted_module_control()` instead. It starts closed. The trusted
host calls `attach_module_controller` with the concrete server transport end;
only its paired client connection can mutate module lifecycle/configuration.
No wire method or caller-selected name can select a controller.

```rust,no_run
# #[cfg(feature = "modules")]
# async fn example() -> tinybus::Result<()> {
use std::sync::Arc;
use tinybus::{Connection, broker::Broker, module::host::ModuleHost};
use tinybus::transport::memory::MemoryTransport;

let broker = Broker::new_restricted_module_control();
let (server, client) = MemoryTransport::pair();
let _controller_name = broker.attach_module_controller(Arc::new(server))?;
let controller = Connection::connect(Box::new(client)).await?;
let host = ModuleHost::new(broker.clone());
// Keep `controller` and `host` alive in trusted host code. Other connections
// use ordinary `broker.attach` or a listener; they never receive control.
# let _ = (controller, host);
# Ok(())
# }
```

Admission is checked against the broker's actual ingress connection before
argument parsing or module-host lookup. Unauthorized LoadModule,
LoadGithubModule, ReinitializeModule, StopModule, EnableModule and RescanModules
return `ai.tinyhumans.tinybus.Error.ModuleControlDenied` without callbacks or
state mutation. ListModules, GetModule and GetModuleManifest remain available
for inspection. Module inspection is not a confidentiality boundary.

Selection is atomic and exactly once across broker clones. Replacing ModuleHost
cannot relax it. Disconnect does not transfer control to a replacement peer or
well-known-name owner; construct a new restricted broker to select a new
controller. Already admitted work may finish after disconnect. There is no
public JSON capability or controller reassignment method.

This restricts **wire callers**, not trusted Rust code or loaded native code.
Direct ModuleHost methods remain trusted host APIs. Keep the controller's
client endpoint away from untrusted tasks, and do not proxy arbitrary user
requests through it. A loaded module shares the host address space; this API
does not sandbox it. Incoming module-method actor authorization still uses
`CallContext` and is independent of module-control admission.
