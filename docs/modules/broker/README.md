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

Emitted when a well-known name is claimed or released, including by a peer
dying. This is how the kernel learns an integration is gone in milliseconds
rather than at the next timeout. Subscribers still have to ask for it; the
broker does not push it at peers that did not.
