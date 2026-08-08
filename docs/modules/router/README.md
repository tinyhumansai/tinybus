# `router`

The routing table and the match rules that drive signal delivery.

## Dumb and synchronous, on purpose

The router answers two questions — who owns this name, who wants this signal —
and answers them without awaiting anything. Callers take the answer (cloned
channel senders), drop the lock, and only then send. That is why it can live
under a plain `std::sync::Mutex`.

Getting this wrong is the classic broker deadlock: peer A's full queue holds the
routing lock while peer B is trying to disconnect.

## Name ownership

- Unique ids start at 1 and are never reused, so a stale reply addressed to a
  dead `:1.4` cannot reach its replacement.
- First writer wins a well-known name; a second claimant is refused with the
  current owner named, not queued. D-Bus's ownership queue is deliberately not
  copied — two live processes both able to answer as the wallet is a worse
  outcome than a clear startup failure.
- Re-requesting a name you already hold is idempotent, so a service that
  re-registers on reconnect does not fail.
- Detaching releases every name and reports each change, which is what the
  broker turns into `NameOwnerChanged`.

## Match rules

Filtering happens at the broker, not the client. The entire point of the project
is that the kernel does not pay for integrations it is not using; waking it to
discard a signal it never asked for is that cost in miniature.

An unknown match key is an error rather than an ignored clause: silently
dropping one *widens* a subscription the peer asked to narrow.

A sender never receives its own signal. Not an optimisation — a service that
both emits and subscribes on one interface would otherwise hear itself and, if
it re-emits in response, loop.
