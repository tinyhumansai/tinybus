# `stream` — bulk payloads larger than one frame

A frame is capped at 16 MiB and that cap does not move: the length arrives from
the wire before the bytes, so a reader that trusted it would allocate whatever a
peer announced. `stream` is how a payload bigger than a frame crosses the bus
anyway — the sender opens a stream on the receiver, writes it as a sequence of
bounded chunks, and closes it. The method call carries a `StreamRef`, a handle a
few dozen bytes long; the bytes travel beside it.

## Why it is a peer interface, not a broker feature

Every chunk is an ordinary method call addressed to the receiving peer. The
broker reads the header, routes it, forwards it. It never assembles a stream and
never sees a chunk as anything but traffic — a broker that buffered payloads
would be a process holding every mail body and every recovery phrase on the bus,
which is the one thing the security boundary says it must never be.

The other consequence: the receiver decides its own limits. Nothing about a
stream is negotiated, because a limit a sender can talk you out of is not a
limit.

## The shape of a transfer

```rust
let mut writer = conn.open_stream(&dest, StreamDescriptor::with_len(len)).await?;
let handle = writer.stream_ref();          // goes in the method body
// …issue the call, then feed the stream while the call is outstanding…
writer.write(&bytes).await?;
writer.finish().await?;
```

On the receiving side, inside the method:

```rust
let mut reader = conn.accept_stream(&handle)?;
while let Some(chunk) = reader.next_chunk().await? {
    file.write_all(&chunk).await?;         // one chunk in memory, never more
}
```

`Connection::call_with_stream` does the interleaving for the common case, and
`Connection::read_stream` buffers a whole payload for when it is too big for a
frame but not too big for memory.

**Order matters.** Send the call and *then* feed the stream. The receiver's
window is a few megabytes, so a sender that writes an entire payload before
making the call stalls against a reader that does not exist yet. This is not a
wart to be fixed with a bigger buffer — the bounded window is the flow control.

## Flow control, and what a misbehaving peer costs

`Write` is a call, so it has a reply and a deadline. The receiver does not reply
until the chunk has room in the reader's window, so a sender runs exactly as
fast as the receiver drains. There is no unbounded buffer anywhere in the path.

That is the misbehaving-peer invariant applied to bulk transfer. Walk the cases:

| A peer that… | Costs it | Costs anyone else |
| --- | --- | --- |
| never reads a stream sent to it | its sender's write deadline | nothing |
| opens streams and abandons them | its own per-peer slots | one window each, reaped after `idle_timeout` |
| writes past the length it declared | the stream, aborted | nothing |
| writes chunks out of order | the stream, aborted | nothing |
| exits mid-transfer | the transfer | one window, until the reaper |

A closed-but-uncollected stream is capped separately from a live one, because
the two are different failures: too many live streams is a sender running ahead
of itself, too many closed ones is a receiver not collecting what it was sent.

## Ownership

Chunks are authorised by the `sender` the broker stamps, and only by that. The
peer that called `Open` is the only peer whose `Write`, `Close` or `Abort` that
stream will answer; every other peer gets `UnknownStream` — the same error as a
handle that names nothing, because distinguishing the two would let a peer probe
for transfers running between two others.

This is the whole authorisation story for streams, and it rests entirely on the
broker overwriting `sender` on ingress.

## Ordering

Chunks carry a sequence number and the receiver requires the next one exactly.
The transport is already ordered, so this is not about a reordering network: it
catches a sender that pipelines. Two chunks in flight at once are dispatched
into two tasks on the receiver and could land either way round, and silently
transposing two megabytes of a PDF is worse than an error.

## What this costs, and what replaces it later

A chunk is base64 inside a JSON body, so a transfer pays about a third of its
size in overhead plus a round trip per 512 KiB. That is the price of bulk data
travelling the same wire as everything else, and it is why `ROADMAP.md` still
wants `SCM_RIGHTS`. When fd passing lands it slots in under this same API,
because callers hold a `StreamRef` rather than a byte array — the fast path can
change without the interface changing.

For a payload that is genuinely huge, the pre-existing convention still applies
and is still cheaper: pass a path, and own the file's lifetime.

## Limits

`StreamLimits`, per receiving connection, changed with
`Connection::set_stream_limits`:

| Field | Default | Bounds |
| --- | --- | --- |
| `max_stream_len` | 256 MiB | one transfer |
| `max_streams_per_peer` | 4 | concurrent transfers from one peer |
| `window_chunks` | 8 (≈4 MiB) | bytes in flight per transfer |
| `idle_timeout` | 60 s | how long an abandoned transfer holds its window |

New limits apply from the next `Open`. A stream already running keeps the window
it was opened with: shrinking a window under a sender mid-transfer would abort a
transfer that was within the rules when it started.

See [protocol.md](../../protocol.md#bulk-streams) for the wire members.
