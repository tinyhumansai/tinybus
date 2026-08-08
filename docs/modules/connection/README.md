# `connection`

A peer's link to the broker: outgoing calls, incoming dispatch, signals.

## One type, both roles

The kernel uses the client half (`call`, `add_match`); an integration also uses
the service half (`serve_at`, `emit`). They are not separate types because a
service calling another service is normal — the transcription service asking the
notification service to announce it should not need a second socket.

## The dispatch loop

Exactly one task reads the transport, and it never awaits user code inline: an
inbound method call is handed to a spawned task, so a slow `Transcribe` cannot
stop the connection from noticing a reply to an earlier call. That is why calls
carry serials instead of relying on order.

On shutdown the loop wakes every pending caller rather than leaving them to time
out one at a time.

## Timeouts are not optional

Every call has a deadline, defaulting to 30 seconds. The failure it prevents is
the one that motivated the project: an integration wedged inside a third-party
library used to wedge the kernel with it. A timeout does not cancel remote work
— tinybus cannot — it stops waiting and frees the caller, and it reclaims the
pending slot so a timed-out call does not leak one per occurrence.

## `CloseOnDrop`

The dispatch task holds its own `Arc<Inner>`, so the refcount never reaches zero
while the task lives, and the task only exits when the transport closes. A guard
on the outer handle closes the transport when the last `Connection` drops.
Without it, an exiting service would keep its well-known name until its process
died, and `NameOwnerChanged` — the whole reason the kernel can react to a death
— would not fire.

## Argument normalisation

`to_body` wraps a scalar into the positional array the protocol specifies, so
the one-argument case (by far the most common) reads naturally at the call site.
