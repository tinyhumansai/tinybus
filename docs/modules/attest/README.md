# `attest`

Confidential messages, and the recipient check that has to hold before one is
delivered.

## The problem

A method call already reaches exactly one peer: the broker resolves the
destination and forwards to that peer's queue, and no match rule can pull a call
into anyone else's stream. That is a *routing* property, not a security one. It
says the message goes to whoever owns the name; it says nothing about who that
is. For a transcription request that is fine. For a private key it is the whole
question — a process that claimed `…Wallet` before the real wallet started would
be handed the key by a bus doing exactly what it was designed to do.

So the guarantee is split in two, and both halves are needed:

1. **Confidentiality of the path.** A message marked `confidential` is delivered
   to its one destination or to nobody. It cannot be a signal, it is never
   fanned out to a subscriber, and `tinybus monitor` will not print its body.
2. **Identity of the recipient.** The broker refuses to deliver it at all unless
   it has itself verified what binary is answering to that name.

## What the broker actually checks

Nothing the peer says. A peer asked to describe itself can only lie, which is
why the check does not live in the handshake:

- **Out-of-process peers.** `SO_PEERCRED` gives the broker the peer's pid from
  the kernel. `/proc/<pid>/exe` is a link fixed at `execve`, so the broker reads
  the executable itself, hashes it with SHA-256, and compares against the
  operator's trust store. Linux only — elsewhere the pid yields no executable
  and every such delivery is refused rather than assumed.
- **In-process modules.** The module host already hashes a `cdylib` against
  `modules.toml` before `dlopen`. That is the same check written down in a
  different file, so a module that passed it becomes an attested recipient with
  `source: module`.

An attestation is bound to **one name**, held against **one peer**, and dies
with that peer. A service that exits takes its attestation with it; the next
process to claim the name earns its own or gets nothing. Two names on one peer
do not share trust: the operator allowlisted an artifact *as the wallet*, not as
everything that process also answers to.

## The trust store

```toml
# peers.toml — passed as `tinybus serve --trust-store peers.toml`
"ai.tinyhumans.openhuman.Wallet" = "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
```

Loaded once, at broker construction, and never re-read. A store that reloaded
itself would let whoever can write the file redirect the next secret. A missing
file is a startup error, not an empty store: silently starting a bus on which
every confidential send fails is a worse way to find the typo.

The default is an empty store. Nothing is attested, every confidential message
is refused, and the failure direction is the one that cannot leak.

## What this is not

**It is not encryption.** The body travels in plaintext and the broker sees it.
The threat this addresses is *the wrong recipient*, not *a compromised broker* —
a broker that is compromised has already seen every mail body and OAuth token on
the bus, and no routing rule fixes that. End-to-end sealing is a separate layer
and would slot in above this one.

**It is not a signature.** The trust store is a list of hashes an operator put
on disk, so an attestation means "this is the artifact the operator
allowlisted", not "a release key vouched for it". Signed release manifests are
the natural next layer: verification would produce the same `Attestation` record
and slot in behind `TrustStore::verify` without touching the wire format.

**It does not attest the sender.** Anyone may ask for confidentiality; the flag
only ever causes the broker to apply *more* restrictions, so a peer that sets it
on its own traffic restricts itself and nobody else. That is why `confidential`
is the one header field the broker does not overwrite on ingress, unlike
`sender`.

## Using it

```rust,ignore
let wallet = connection.proxy(WALLET, WALLET_PATH, WALLET)?;

// Ask what the bus verified before assembling the secret. `None` means the
// send will be refused: nothing owns the name, or nothing was allowlisted.
if wallet.attestation().await?.is_none() {
    return Err(Error::failed("wallet is not an attested recipient"));
}

let stored: bool = wallet.call_confidential("StoreKey", (key,)).await?;
```

A refusal arrives as `Error::NotAttested`, whose dotted name is
`ai.tinyhumans.tinybus.Error.NotAttested`. It is deliberately distinct from
`NameHasNoOwner`: "not installed" and "not trusted" are different problems with
different fixes, and an operator should not have to guess which one they have.

## Rules that are load-bearing

- A confidential **signal** is refused on ingress. A broadcast has no single
  recipient to attest, so there is nothing the flag could mean.
- A confidential **call** must address a well-known name. The broker knows which
  connection `:1.7` is, but not what binary is behind it.
- A confidential **reply** inherits the flag and goes back to the caller's
  unique name. A key derivation answers with a key, and a reply that quietly
  lost the flag would leak on the way back what the call protected on the way
  out.
- An **error reply** never inherits it. Errors carry no value, and a
  confidential error to a peer that just failed attestation would swallow the
  reason it failed.
