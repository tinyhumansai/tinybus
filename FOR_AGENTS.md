# tinybus module integration guide

This guide is for an agent building or integrating a dynamic tinybus module.
It intentionally covers module integration only; repository-wide contribution
and PR workflow belongs elsewhere.

## Choose the boundary first

A `cdylib` module is trusted native code inside the host process. It shares the
host address space, privileges, allocator, and crash domain. Deadlines,
bounded queues, and caught panics limit ordinary misbehavior but cannot contain
a segfault, abort, heap corruption, OOM, or deliberate memory access.

Use a separate service process when the integration must be crash- or
compromise-isolated. Do not put a codec, provider SDK, or other integration
dependency into the tinybus workspace merely to avoid that process boundary.

## Module crate setup

The module crate should depend on the ABI/runtime crate and be built as a
dynamic library:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
tinybus-module = "0.1"
tinybus = { version = "0.1", default-features = false, features = ["modules"] }
```

The exact dependency versions must match the tinybus ABI compatibility policy.
Build with unwinding enabled; a panic-abort module is rejected because its
panic would terminate the host.

## Export the module

Use `module_export!` once. The exported descriptor, manifest, and initializer
are the stable admission surface:

```rust
use tinybus::{Connection, Result};
use tinybus_module::module_export;

async fn setup(connection: Connection) -> Result<()> {
    // Serve objects and request the well-known name here.
    let _ = connection;
    Ok(())
}

module_export! {
    setup = setup,
    worker_threads = 1,
    provides = ["ai.example.Clock"],
    methods = [],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}
```

Follow the macro's current examples for the complete service-tree shape. Public
interfaces and methods must use the normal tinybus interface machinery; the
broker routes headers and never interprets method bodies.

If setup needs typed configuration, declare it with `config = MyConfig` and
use the configured setup form. Configuration is JSON supplied by the host; the
SDK copies it during initialization and the module must not retain borrowed
host memory.

## Manifest and admission

The manifest is checked before initialization. Keep these identities exactly
aligned:

- exported descriptor module name and manifest module name;
- exported descriptor version and manifest module version;
- manifest schema and tinybus ABI series;
- target triple, pointer width, endianness, feature bits, and panic strategy.

Declare required and optional interfaces accurately. Required dependencies must
be provided by an already-loaded module or the module remains unresolved.
Duplicate bus names, dependency cycles, incompatible ABI revisions, invalid
descriptors, and incompatible tinybus versions are refusals, not retryable
startup errors.

## Host loading APIs

For a local artifact, the embedding host can use:

```rust
let info = module_host.load_file_with_config(
    "/path/to/module.so",
    serde_json::json!({ "prefix": "configured:" }),
)?;
```

For a GitHub release, provide the HTTPS tag URL, exact archive asset name, and
the host's expected SHA-256:

```rust
let info = module_host.load_github_release(
    "https://github.com/example/module/releases/tag/v0.1.2",
    "module-linux-x86_64.tar.gz",
    Some("<64 hexadecimal SHA-256 characters>"),
    serde_json::json!({}),
)?;
```

The same operation is available across the bus as `LoadGithubModule` and in
the CLI as:

```sh
tinybus modules load-github \
  https://github.com/example/module/releases/tag/v0.1.2 \
  module-linux-x86_64.tar.gz \
  <sha256>
```

The URL must identify a tag, not an arbitrary host or GitHub page. The asset
must be a release archive; `.tar.gz` is the normal format. The archive must
contain exactly one platform library (`.so`, `.dylib`, or `.dll`).

## Release checksums

Every GitHub release consumed by the loader must publish `checksum.toml` or
`checksum.json` as a release asset. TOML uses:

```toml
[sha256]
"module-linux-x86_64.tar.gz" = "<64 hexadecimal characters>"
```

JSON may use either the equivalent top-level map or:

```json
{
  "sha256": {
    "module-linux-x86_64.tar.gz": "<64 hexadecimal characters>"
  }
}
```

The host digest must agree with the release manifest. tinybus then downloads
the archive, hashes the bytes, compares them with the manifest, extracts only
after verification, and loads the one discovered platform library. A missing,
malformed, or mismatched checksum refuses the module.

For the repository's Linux example modules, use
`crates/tinybus/examples/create-release-assets.sh` after a release build. The
`github_module_host` example demonstrates the minimal host call. Release
assets should be reproducible and named with the target/platform so a host
never has to guess which binary to load.

## Module safety rules

- Treat every module as host-trusted code; use a process for untrusted code.
- Never bypass the ABI descriptor, manifest, dependency, or checksum gates.
- Do not make initialization or calls unbounded; every call has a deadline.
- Keep per-peer/module work bounded so one slow module cannot affect another.
- Do not put credentials, payload values, or absolute paths into module errors.
- Do not rely on the module setting `sender`; the broker stamps it.
- Do not change an existing interface in place. Publish a new interface name
  for a breaking contract.
- Stop is terminal for the current process; the library remains mapped until
  process exit and is intentionally never unloaded.

## Validation

From the tinybus repository, validate module-related changes with:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo check --locked --no-default-features --features modules
cargo build --locked --example module_clock --no-default-features \
  --features modules,macros
```

Tests should use the in-memory transport where possible. Real dynamic-loader
tests belong with the module loader and must assert refusal, timeout, panic,
dependency, and lifecycle properties rather than relying on sleeps.
