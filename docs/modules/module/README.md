# Dynamic modules

The `module` package is the host half of tinybus's trusted in-process module
boundary. A module is a Rust `cdylib` loaded at runtime and attached to the
broker as an ordinary peer. The broker still stamps senders, routes only on
headers, applies per-peer queue bounds, and releases names when the bridge
closes.

Loading is behind the non-default `modules` feature. The ABI types and manifest
are always available so a module SDK can compile with no socket or loader.

## Trust model

`dlopen`, `LoadLibraryExW`, and their platform equivalents execute code before
the host can inspect an exported symbol. The ABI gate prevents incompatible
modules from receiving the host vtable; it does not make an artifact safe.
Every loaded module can read and write the host address space and a native
fault terminates the process. Install directories must therefore be private,
and modules must be treated as first-party host code.

The loader never unloads a library. A stopped module releases its bus transport
and names, but code, TLS, panic metadata and callback addresses remain mapped
until process exit. Replacing or removing an already loaded artifact requires a
restart.

## Loading sequence

1. Check every directory component's ownership/mode, require a regular
   platform library file no larger than 512 MiB, and enforce `modules.toml`
   when present.
2. Read an adjacent lazy manifest when one is present; otherwise load eagerly
   and locally (`RTLD_NOW | RTLD_LOCAL` on Unix).
3. Resolve `TINYBUS_MODULE_ABI_V1` against that specific handle.
4. Read and validate only the frozen 16-byte descriptor prefix.
5. Validate the full descriptor, then parse the manifest.
6. Resolve dependencies and reject missing providers, cycles and name clashes.
7. Call `tinybus_module_init_v1`, receive its vtable, and attach the transport.
   A manifest with `lazy_init = true` defers this step until its first method
   call; racing first calls share one initialization and retain their order.

## Lazy loading

To keep a library entirely out of the host address space until it is called,
install a JSON copy of its embedded manifest next to the artifact. For an
artifact named `wallet.so`, the sidecar is `wallet.so.manifest.json` (and the
same suffix rule applies to `.dylib` and `.dll`). The manifest must set
`lazy_init` to `true`.

At discovery, TinyBus validates the artifact and the sidecar, resolves
dependencies, reserves the declared bus name, and attaches a dormant bounded
transport without calling `dlopen` or `LoadLibraryExW`. The first method call
loads the library on a blocking worker, applies the normal ABI gate, requires
the embedded manifest to exactly match the sidecar, and runs setup. Concurrent
first calls share that one attempt and remain queued in arrival order. A load
or setup failure is terminal for the process and all callers receive
`ModuleUnavailable` rather than waiting for their individual deadlines.

`ModuleHost::register_lazy_file` provides the same behavior when an embedding
host already has the trusted manifest in memory. Modules without a sidecar keep
the existing behavior: their library is mapped during discovery, while
`lazy_init = true` still defers setup.

The host vtable also carries borrowed JSON configuration. The SDK copies and
deserializes it during initialization; the module never retains a pointer into
host memory. `module_export!` accepts `config = MyConfig` for an async setup
function shaped `setup(Connection, MyConfig)`, while the original
`setup(Connection)` form ignores configuration. Operators can pass the value
with `tinybus modules load <path> --config '{...}'`.
For directory discovery, embedding hosts call `ModuleHost::set_config(name,
value)` (or the builder-form `with_config`) before `load_dir`; the value is
selected by the admitted manifest name.

When present, `modules.toml` is authoritative for its directory. Keys are
artifact file names (or stems) and values are lowercase SHA-256 hashes. An
artifact absent from the file or with a mismatched hash is refused. For remote
loads, the host supplies the expected SHA-256 alongside the artifact URL;
hashing happens before the platform loader opens the artifact. Search
precedence is `OPENHUMAN_MODULE_PATH`, the platform user data directory, then
the platform system directory. `tinybus modules scan --path <dir> --dry-run`
performs admission and dependency checks without initializing or attaching.

GitHub releases are loaded with `ModuleHost::load_github_release`. The release
URL must identify a tag, the selected asset must be a `.tar.gz` or `.zip` archive, and
the release must publish `checksum.toml` or `checksum.json`. The manifest uses
this shape:

```toml
[sha256]
"module-linux-x86_64.tar.gz" = "<64 lowercase hexadecimal characters>"
```

The host-provided digest is checked against the release manifest, then the
downloaded archive is checked before extraction. The extracted archive must
contain exactly one platform library matching the host (`.so`, `.dylib`, or
`.dll`). `examples/create-release-assets.sh` and
`examples/create-release-assets.ps1` show the Linux/macOS and Windows
packaging conventions used by the example module.

The CLI can generate the manifest consumed by the loader without external
hashing tools:

```sh
tinybus modules checksum \
  --path module-linux-x86_64.tar.gz \
  --path module-macos-arm64.tar.gz \
  --output checksum.toml
```

Refusing one artifact does not prevent the host from admitting other artifacts
in the same directory. The refused artifact's error contains only a sanitized
basename and fixed reason.

Lifecycle states are `discovered`, `rejected`, `unresolved`, `resolved`,
`initializing`, `ready`, `serving`, `faulted`, `failed`, `stopped`, and
`disabled`. Rejected, faulted, failed, stopped, and disabled modules answer a
call immediately with `ModuleUnavailable`; they are never retried in the same
process. `ready` and `serving` reflect whether calls are in flight and do not
emit per-call state signals. `StopModule` emits no signal through its bus-call
path; the stopped transition is announced when the module peer subsequently
detaches. Other lifecycle edges emit `ModuleStateChanged` after any
corresponding `NameOwnerChanged` announcement.

See [abi.md](abi.md) for the binary contract and
[the protocol](../../protocol.md) for the bus control members.
