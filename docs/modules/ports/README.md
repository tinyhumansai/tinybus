# `ports`

Two traits: `Transport` (a peer's bidirectional framed stream) and `Listener`
(the broker's accept side). That is the whole seam.

Everything above them — the router, the connection, the proxy, generated
dispatch — has never heard of a socket. Consequences that are worth the
indirection:

- The test suite runs a whole bus in one process with no file descriptors, so a
  test can assert exact message ordering without a temp dir or a sleep.
- A Windows named-pipe backend is a new file, not a new code path.
- The kernel can embed a broker with `--no-default-features` and link no
  networking at all.

## The contract worth restating

- `recv` is called from **exactly one task** per transport. Both the connection
  and the broker's peer loop own a single reader, so an implementation may hold
  a lock across the await in `recv`.
- `send` has no such restriction: every proxy on a connection shares one
  transport and sends concurrently.
- `Ok(None)` from `recv` is a clean shutdown, not an error. A service exiting
  normally must not produce a stack of transport errors on the kernel side.
- `close` is idempotent. Shutdown races are normal.
- A `Listener` treats a per-connection failure as something to skip. One bad
  client must never take the accept loop down.
