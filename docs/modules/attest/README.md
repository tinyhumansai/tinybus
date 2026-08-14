# `attest`

Confidential messages, and the recipient check that has to hold before one is
delivered.

## Who receives a secret

A **module loaded into the host's address space**, and nothing else.

That is the whole rule, and it is a deliberate narrowing rather than a
limitation. A secret handed to a loaded module never crosses a transport, never
reaches a separate process, and never touches a socket — so there is no peer to
identify, no process credential to read, and no per-OS code to keep working.

Services in their own processes, CLI clients and monitors do not receive
secrets. A confidential message addressed to one is refused.

## What the check establishes

Before `dlopen` runs, the module host hashes the artifact with SHA-256 and
compares it against the operator's `modules.toml`. A module that fails is not
loaded at all. A module that passes becomes an **attested recipient**: the host
records that this well-known name is owned by code whose bytes hashed to this
digest, and that the operator listed that digest as acceptable.

The hash is computed by the host over bytes the host read itself. Nothing a
module claims about itself participates.

```toml
# modules.toml, beside the artifact — the same file the loader already uses
"libwallet.so" = "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
```

## What it does not establish, and this matters

An in-process module shares the host's address space. It can read host memory
directly, so a *malicious loaded module* is not contained by any routing rule —
it never needed the bus to reach a secret in the first place. This is the
invariant CLAUDE.md already states: in-process modules are inside the trust
boundary.

So what attestation buys is **admission control, not isolation**. Only code
whose hash an operator allowlisted is loaded at all, and only such code is handed
a secret through the bus. The bus's job is to refuse to be the delivery
mechanism for anything else. An integration whose compromise must not reach the
kernel's secrets belongs in a separate process — where it is, by this design,
ineligible to receive them.

A second thing it does not cover: **bulk streams**. A stream's bytes move as
their own `Stream.Write` calls, which carry no `confidential` flag and so are
routed without this check. Putting a `StreamRef` in a confidential call would
attest the recipient of the *handle*, not of the payload.

Rather than leave that as a trap, it is **refused**. A confidential call whose
body carries a stream handle fails before it is sent:

```text
a confidential call cannot carry a stream handle: the stream's bytes travel as
separate unattested writes, so the payload would not be confidential even
though the handle was
```

The refusal happens in the **sending peer's own process**, not at the broker,
and that placement is forced: spotting a handle means reading the body, and a
broker that read a confidential body would be the very thing confidentiality
exists to prevent. The sender already owns the body it just built, so it is the
only party that can look without breaking the rule.

Matching is structural — an object whose keys are exactly a `StreamRef`'s, with
`id` present — and never looks inside `id`, which is documented as opaque. The
cost is that a bare `{"id": "…"}` in a confidential body is refused even when it
was never a handle. That is the direction to be wrong in: the failure is loud,
local, and fixed by restructuring the call, whereas the alternative failure is a
secret leaving unattested and nobody finding out.

A secret large enough to want a stream therefore still has no attested way to
travel. Confidential bulk transfer is its own piece of work; what this closes is
the silent version of the gap. See
[the protocol's `confidential` section](../../protocol.md#confidential).

A third case worth naming explicitly: a module loaded from a GitHub release
extracts into a fresh temporary directory that holds no `modules.toml`, so
`allowlisted_hash` finds nothing to compare against and the module is never
attested — this is the fail-closed default working as intended, not a bug, but
it means a GitHub-loaded module can never be a confidential recipient until the
operator also places its digest in the local allowlist beside it.

## Not a signature, yet

`modules.toml` is a list of hashes an operator put on disk, so an attestation
means "this is the artifact the operator allowlisted", not "a release key
vouched for it". Signed release manifests — an org key in CI signing each
release's checksums — are the natural next layer: verification would produce
this same `Attestation` record and needs no wire-format change.

## Using it

```rust,ignore
let wallet = connection.proxy(WALLET, WALLET_PATH, WALLET)?;

// Ask what the host verified before assembling the secret. `None` means the
// send will be refused: nothing owns the name, or it is not a verified module.
if wallet.attestation().await?.is_none() {
    return Err(Error::failed("wallet is not an attested recipient"));
}

let stored: bool = wallet.call_confidential("StoreKey", (key,)).await?;
```

A refusal arrives as `Error::NotAttested`, dotted name
`ai.tinyhumans.tinybus.Error.NotAttested`. It is deliberately distinct from
`NameHasNoOwner`: "not installed" and "not eligible for secrets" are different
problems with different fixes.

## Rules that are load-bearing

- **Attestation is bound to one name and held against one peer.** It dies with
  the peer. A module that exits takes its attestation with it, and the next
  process to claim the name inherits nothing — it answers ordinary calls and is
  refused secrets.
- **Two names on one peer do not share trust.** The operator allowlisted an
  artifact *as the wallet*, not as everything that module also answers to.
- A confidential **signal** is refused on ingress. A broadcast has no single
  recipient to attest, so there is nothing the flag could mean.
- A confidential **call** must address a well-known name. A unique name
  identifies a connection, not an artifact.
- A confidential **reply** inherits the flag and goes back to the caller. A key
  derivation answers with a key, and a reply that quietly lost the flag would
  leak on the way back what the call protected on the way out.
- An **error reply** never inherits it. Errors carry no value, and a
  confidential error to a peer that just failed attestation would swallow the
  reason it failed.
- The broker never fans a confidential message out to a match rule, and
  `tinybus monitor` prints `<confidential>` rather than the body.

## Tests

The enforcement rules are covered on the in-memory transport in `router.rs` and
`broker.rs`. The load-time seam — a real artifact, hashed off disk, becoming
attested — is covered by two opt-in tests that need a built `cdylib`:

```sh
cargo build --example module_clock --all-features
TINYBUS_TEST_MODULE="$PWD/target/debug/examples/libmodule_clock.so" \
  cargo test --all-features -- --ignored
```

The artifact name above is Linux's (`libmodule_clock.so`); on macOS Cargo
builds `libmodule_clock.dylib` instead, so point `TINYBUS_TEST_MODULE` at that
file when running the same command there.
