# `service`

The service side: the `Interface` trait, and the object tree a call is dispatched
through.

## Why the trait is dynamically typed

`Interface` must be object-safe — one connection holds a heterogeneous list of
interfaces behind `dyn` — so `call` takes and returns `serde_json::Value`. Typed
dispatch is what `#[tinybus::interface]` *generates* on top of it: handwritten
code stays typed, and only the seam is dynamic.

Writing that dispatch by hand means a string `match`, a positional deserialize,
a serialize, and a member list for introspection: four places per method for a
typo to hide, and no compiler check that they agree. The macro derives all four
from one signature.

## The object tree

Flat, not an actual tree. Nothing needs the hierarchy at dispatch time — the
only consumer of path structure is `path_namespace` matching, which is a string
comparison on the sender's side. A real tree would make "list every object", the
operation introspection actually performs, a traversal instead of an iteration.

A service exports one object per addressable *thing*, not per interface: a mail
integration with three accounts exports three paths carrying the same `Mailbox`
interface, which is what keeps the kernel's proxy code account-agnostic.

Re-registering an interface name at a path **replaces** it. Hot-reloading an
implementation is legitimate; two implementations of one contract at one address
would make dispatch order-dependent.

## Three distinct failures

`UnknownObject`, `UnknownInterface`, `UnknownMethod` are separate errors because
they mean different things to whoever is debugging: not running, running an
older contract, or a typo.

## Panics

A panic in a method body is not caught. A service that has panicked has an
unknown internal state, and answering the next call from it is worse than being
restarted.
