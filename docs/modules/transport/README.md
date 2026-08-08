# `transport`

Adapters for the two ports. `memory` is always compiled; `unix` sits behind the
`uds` feature.

## `memory` is not a test double

It is a first-class transport that earns its place twice:

1. **Tests.** A broker, several services and a client inside one
   `#[tokio::test]`.
2. **The slim kernel build.** An OpenHuman build that hosts an integration
   in-process uses the same bus, the same interfaces and the same proxy code as
   the out-of-process one. Moving an integration across that line is a
   deployment decision, not a rewrite.

Channels are bounded. An unbounded channel would turn a slow service into
unbounded kernel memory growth — relocating the failure mode rather than
deleting it.

The outbound sender lives in an `Option` behind a std mutex so `close` can drop
it: dropping the sender is what the far end sees as `Ok(None)`. Without that, a
closed in-process link would look exactly like an idle one, and a service that
exited would keep its name until its process died.

## `unix`, and why not TCP

The bus is a capability handle: anything that can connect can ask the wallet to
sign. A Unix socket inherits filesystem permissions, so `0700` on the containing
directory is the whole access-control story. A TCP port has no such story and
would need authentication invented on top of it. The socket lives under the
user's runtime directory rather than `/tmp`, which is world-writable.

Two locks per transport, not one: an idle read must not block a write.

The listener owns the socket file and unlinks it on drop, because a stale socket
from a crashed broker makes the next start fail on a path that is not, in any
meaningful sense, in use. Binding does *not* clobber unconditionally — a live
broker's socket is left alone and the bind fails, so two brokers cannot steal
the bus from each other.
