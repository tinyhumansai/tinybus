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
2. Load eagerly and locally (`RTLD_NOW | RTLD_LOCAL` on Unix).
3. Resolve `TINYBUS_MODULE_ABI_V1` against that specific handle.
4. Read and validate only the frozen 16-byte descriptor prefix.
5. Validate the full descriptor, then parse the manifest.
6. Resolve dependencies and reject missing providers, cycles and name clashes.
7. Call `tinybus_module_init_v1`, receive its vtable, and attach the transport.
   A manifest with `lazy_init = true` defers this step until its first method
   call; racing first calls share one initialization and retain their order.

The host vtable also carries borrowed JSON configuration. The SDK copies and
deserializes it during initialization; the module never retains a pointer into
host memory. `module_export!` accepts `config = MyConfig` for an async setup
function shaped `setup(Connection, MyConfig)`, while the original
`setup(Connection)` form ignores configuration. Operators can pass the value
with `tinybus modules load <path> --config '{...}'`.
For directory discovery, embedding hosts call `ModuleHost::set_config(name,
value)` (or the builder-form `with_config`) before `load_dir`; the value is
selected by the admitted manifest name.

The optional `modules.toml` file is authoritative for its directory. Keys are
artifact file names (or stems) and values are lowercase SHA-256 hashes. An
artifact absent from the file or with a mismatched hash is refused. Search
precedence is `OPENHUMAN_MODULE_PATH`, the platform user data directory, then
the platform system directory. `tinybus modules scan --path <dir> --dry-run`
performs admission and dependency checks without initializing or attaching.

One refusal is returned independently and does not stop other artifacts in a
directory. Errors contain only a sanitized basename and a fixed reason.

See [abi.md](abi.md) for the binary contract and
[the protocol](../../protocol.md) for the bus control members.
