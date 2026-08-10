# tinybus instructions for agents

This file is the short operational guide for agents changing this repository.
Read it before editing. The repository's source of truth is the code and tests;
do not broaden a task because a future roadmap item looks useful.

## Repository shape

- `crates/tinybus/` is the bus, broker, protocol, transports, CLI, and trusted
  in-process module host.
- `crates/tinybus-macros/` contains only the `#[interface]` proc macro.
- `crates/tinybus-module/` is the module-side runtime and ABI export macro.
- `crates/tinybus/examples/` contains bus and dynamic-module examples. The
  separate root `examples/` project is an upstream multi-process walkthrough;
  it is intentionally not a workspace member.
- `docs/modules/<module>/README.md` documents each source module.

Integrations remain separate processes and repositories. Do not add a codec,
provider SDK, or other integration dependency to this workspace.

## Starting work

Execute clear tasks directly. For new implementation or audit work, create an
isolated worktree before editing:

```sh
worktree <short-slug>
cd worktrees/<short-slug>
```

Do not edit `main`. Preserve unrelated user changes. The repository may have an
automatic checkpoint commit hook; do not reset, revert, or investigate commits
it creates. If a deliberate commit is needed, use:

```sh
atomic-commit "scoped message" -- <files...>
```

Never use destructive broad commands such as `git reset --hard`, recursive
deletion of a workspace, or checkout-based overwrites without explicit approval.

## Rust commands

Run the relevant checks before handoff; for normal changes run the full set:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo check --locked --no-default-features
```

The CI matrix also checks `modules` without sockets and builds the examples.
Do not export `CARGO_TARGET_DIR`, and do not send build output to `/tmp` or a
temporary target directory. Shared target configuration and sccache are
intentional. If an extra target directory is unavoidable, keep it under the
checkout's `target/`.

Use `rg`/`rg --files` for searches. Use `apply_patch` for source edits. Every
Rust source file begins with a `//!` module-level description, public items
have docs, and formatting follows Rust 2024/rustfmt.

Tests belong in the crate, normally in a `#[cfg(test)]` module at the bottom of
the implementation. Test the property and name it accordingly. Never use
`sleep` for synchronization: await the event or use a timeout as a deadline.

## Protocol and security invariants

The wire format is a compatibility contract. Add optional fields rather than
renaming/removing/changing existing ones. A breaking interface gets a new
interface name (`Voice2`); bump `PROTOCOL_VERSION` only when old peers cannot
parse the format at all.

Do not weaken these invariants:

- The broker reads headers, routes, and forwards; it never parses bodies.
- The broker overwrites/stamps `sender` on ingress.
- Errors redact values that caused them; never put untrusted payloads in error
  messages.
- Unix sockets live under the user's runtime directory, never `/tmp`; there is
  no TCP listener.
- Every call has a deadline and cannot be made unbounded.
- Per-peer queues are bounded; a slow, wedged, or malformed peer cannot stall
  another peer.
- In-process modules share the host address space. Deadlines, bounded queues,
  and caught panics limit behavior but cannot contain aborts, segfaults, heap
  corruption, OOM, or deliberate memory access. Use a separate process when
  crash or compromise isolation is required.

## Dynamic modules

The `modules` feature is off by default. A module is trusted native code and
must pass directory/file checks, ABI/target/version gates, manifest checks, and
dependency resolution before initialization.

GitHub release loading uses `ModuleHost::load_github_release` or the
`LoadGithubModule` bus method. The URL must be an HTTPS GitHub tag URL; callers
provide the exact archive asset name and expected SHA-256. The release must
publish `checksum.toml` or `checksum.json`, and both the host digest and
release-manifest digest must agree before extraction/loading. The archive must
contain exactly one platform library. Keep extracted release directories alive
for the lifetime of mapped modules.

The checksum TOML shape is:

```toml
[sha256]
"module-linux-x86_64.tar.gz" = "<64 hexadecimal characters>"
```

Use `crates/tinybus/examples/create-release-assets.sh` for the example Linux
packaging convention and `github_module_host` for a minimal loader invocation.

## Documentation

Keep Markdown files at 500 lines or fewer. Update the relevant module README
when behavior, protocol, security, or operator commands change. Explain
load-bearing decisions in comments, not obvious mechanics.

## GitHub handoff

Raise PRs against the canonical `tinyhumansai/*` repository, never a personal
fork. Push the feature branch to the appropriate upstream remote. PRs should
be ready for review, not drafts, unless the user explicitly asks for a draft
or the work is genuinely incomplete. Include the change summary, security or
compatibility impact, and validation commands in the PR body.

For review feedback, address the code and reply to each actionable thread with
the repository's `pr-comments`/`pr-reply`/`pr-review-resolve` workflow. Do not
silently resolve valid feedback or dismiss a changes-requested review instead
of fixing it.
