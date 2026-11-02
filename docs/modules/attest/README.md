# `attest`

Confidential messages, and the recipient check that has to hold before one is
delivered.

## Who receives a secret

A **verified module loaded into the host's address space**, or an explicitly
scoped local host callback created by `ModuleHost`. The callback exception binds
one verified loaded sender to one SDK-created local connection and exact
path/interface/member endpoints. It never produces an artifact attestation.
See [scoped local host callbacks](#scoped-local-host-callbacks).

Neither recipient crosses a process boundary or a socket. Ordinary connections,
remote peers and well-known host aliases cannot acquire this callback permission.

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

An admitted module can receive confidential calls at its original well-known
name or its fixed unique peer. The unique lookup returns the same attestation,
including the original manifest name and verified digest. Extra aliases remain
unattested. Releasing the manifest alias does not retarget a pinned unique peer;
a successor inherits no attestation, and detach removes the old peer's record.
Terminal module states refuse unique calls even before transport detach.

Recipient attestation does not authenticate the caller or grant human authority.
Incoming caller identity still requires broker-proven `CallContext` and a policy
that binds the trusted peer to its permitted operations.

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

## Two ways to vouch for an artifact

An operator asserts "these bytes are the ones I meant" in one of two places, and
attestation accepts either.

**A digest on disk.** `modules.toml` beside the library, which the host re-reads
at load time rather than carrying the value down from the admission gate, so an
artifact that changed underneath the check no longer matches and does not become
attested.

**A digest compiled into the host**, passed as `expected_sha256` to
`load_github_release`. Before extracting anything, `acquire` fetches the
release's own `checksum.toml`, refuses a disagreement between it and the caller's
value, downloads the archive, and hashes the bytes it actually received. Only
then is the library extracted and loaded. The attestation keeps that **archive**
digest in `sha256` — the artifact the operator named — and may carry a separately
pinned `library_sha256` for the extracted library.

Release checksum manifests may publish both values under the same archive key:

```toml
[sha256]
"clock-linux.tar.gz" = "<archive digest>"

[library_sha256]
"clock-linux.tar.gz" = "<library digest>"
```

`tinybus modules checksum` writes both values when an archive contains one
module library. A host registry pins the pair in `ReleaseAsset`. Cache hits
re-hash the extracted library against that pin, and the native loader checks it
again at the actual mapping boundary. The checked library digest is bound to the
process-lifetime native handle; the same handle cannot later acquire a different
digest because its path now names different bytes. `ModuleClient::call_attested`
requires the original manifest name and both digests, then sends to the unique
peer whose attestation it checked. Its host readiness hook initializes a lazy
module before the attestation query and sends no probe call.

The pre-map path check relies on the module directory protections enforced by
host admission. It does not claim to prove code mapped outside TinyBus, survive a
compromised host process, or contain an in-process module that can already read
host memory. Such modules remain inside the host trust boundary.

A pinned digest is the stronger of the two statements, because it cannot be
edited on the machine that runs it.

Omitting `expected_sha256` leaves the release's own checksum manifest as the only
claim about the bytes, which is a publisher vouching for itself rather than an
operator vouching for the publisher. Such a module loads and is refused secrets.
Loading from a directory with no allowlist behaves the same way: admissible, and
ineligible.

> Earlier revisions of this document described the release path as never
> attestable — correct when written, and the reason a host could pin a digest
> with great care and still have every confidential call refused. The pin was
> verified twice and then discarded. It is now carried through.

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
- A confidential **call** must address the attested well-known name or the
  unique peer on which the host admitted that artifact. Ordinary unique peers
  remain ineligible.
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


## Scoped local host callbacks

`ModuleHost::prepare_host_callbacks` creates its own local MemoryTransport
connection. It accepts at most 32 exact path/interface declarations, each with
one to 64 distinct members. `HostCallbackBinding::serve_at` wraps those methods
in authenticated, confidential, live dispatch checks; it exposes no raw
connection for bypassing that guard. An outgoing `proxy` uses this same private
host peer, so initialization and callback reception share one fixed identity.

`load_file_with_host_callbacks` requires an actual hash-verified local artifact
and all endpoints served before admission. It reserves the exact loaded module
peer and binds the exact sender, unique host destination, path, interface and
member before releasing its initialization gate. The read-only `guard()` exposes
`module_peer()` before eager setup callbacks run. The loader awaits actual
READY/fault with a deadline; ABI initialization success alone is insufficient.
Failures or a dropped load future revoke routes and detach reserved peers. A
binding is single-use. Reinitialization requires its unchanged configuration.

This intentional recipient exception is **not artifact attestation**. There is
no remote enrollment method, self-attestation, wire trust flag, existing
connection registration, alias inheritance or generic-load callback grant. The
router admits at most 128 active scopes and continues to apply ordinary module
artifact admission and expected-owner checks.

`revoke()` is immediate and idempotent for new route and user-dispatch admission.
Drop, peer detach and host-owner drop also revoke. A callback selected earlier
must recheck liveness before user code. Code already running remains counted by
`guard().running()` until its future completes or drops. `drained()` reports
that count only; caller timeouts and connection close do not imply that provider
writes have ceased. Detached provider tasks require their own completion fence.
Domain callbacks must check `guard().authorizes(context)` after awaits and before
publication and preserve uncertainty about writes already attempted. No buffer
copy wiping or whole-boundary zeroization guarantee is made.

`module_host_callback` is an actual cdylib fixture: hash-verified bytes perform
an eager confidential callback during setup before READY. The native-loader CI
job runs it on Linux, macOS and Windows. `module_context` separately proves
confidential runtime callbacks and refusal of ordinary, forged and revoked calls.
