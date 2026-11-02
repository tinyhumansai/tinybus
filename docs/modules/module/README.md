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

## SDK instance execution and replacement

Stopping an instance and loading the same unchanged artifact can start a fresh
instance after the broker releases the previous peer's names. A live duplicate
still refuses; changing the mapped artifact's code still requires process restart.

The native SDK scopes its tracing subscriber to each instance's initialization
caller, reinitialization caller, Tokio workers and blocking pool. Nested caller
scopes restore the previous subscriber and owner. Shutdown suppresses subsequent
log and panic callbacks from that instance; this does not cancel accepted bus RPC
or prove completion of module-created background work. Runtime worker teardown
removes its scoped context, without selecting a replacement owner.

The native panic hook resolves the current execution owner and reports location
only, never the panic payload. Module-created `std::thread` threads are outside
SDK runtime ownership: they receive no host logging callback, and an unowned panic
prints only its sanitized location to stderr. The SDK never falls back to the
last initialized instance. Linked modules keep the embedding host's tracing and
panic hook. The existing instance-wide last panic location is diagnostic state,
not a proof of exact attribution among concurrently panicking methods.

## Host runtime leases

`RuntimeLeaseDriver` is the protocol-neutral contract for a host that reserves
module-side resources and exports one callback through `ModuleRuntime::lease`.
The host validates the opaque reservation, creates its object, supplies an
owned callback factory, then starts the module. The callback factory runs only
after validation and object creation. The returned cloneable `RuntimeLease`
provides `object()` and `close()`; the last user clone starts the same cleanup
worker as explicit close, including when dropped outside Tokio.

The driver returns `StopOutcome::Acknowledged`, `RetryableFailure`, or
`Unacknowledged`. A retryable failure is safe only after the driver proves the
operation settled. Lost replies, timeouts, malformed responses and faults are
uncertain unless the driver has other authoritative evidence. The SDK retains
the active STOP future across its per-attempt deadline, never overlaps STOP
attempts, and removes the exact callback registration only after ACK. If the
operation remains uncertain or finite retries end, the dedicated runtime parks
the reservation and callback guard without another retry. Caller close waiters
may time out independently; that does not cancel cleanup.

`ModuleClient::bind` is the ordinary counterpart for owned module operations.
It waits for the module's normal lazy readiness, resolves the current serving
peer, and keeps that peer for root calls, streamed uploads and held object paths.
It supports linked modules and explicit fixtures without claiming archive or
library proof. A peer disconnect makes the held calls fail; they do not move to
a new owner of the public bus name. The proxy's deadline is preserved for owner
lookup and later calls, unless the client supplies an explicit timeout override.
Use this for ordinary Docs and Voice operations; use strict attested calls when
the operation requires native artifact proof or confidential delivery.

`ModuleClient::bind_attested` applies the same identity rule to module calls. It
returns a `BoundModuleClient` only after the loaded peer proves the registered
archive and library pair. Later ordinary, detailed, confidential, and held
object calls use that same unique peer even after its public bus name changes
owners. If the peer disconnects, those calls fail instead of rebinding.

The lifecycle contract, retry policy, typed error accessor and content-free
report records compile without the `modules` feature. The runtime lease and its
host-object registration require `modules`.

## Opted-in terminal factories

Configured modules can opt in with `module_export!(terminal_setup_factory =
factory, config = Config, ...)`; the static export macro accepts the same
arguments. The factory receives one `NativeInstanceLiveness` and returns its
setup callback plus `RuntimeHooks`. An unsupported or non-live native query
rejects initialization before the factory runs. Linked modules require the
same explicit host query and retain the embedding process's panic hook. The
factory owns cleanup of partial construction until it returns its hooks. It
must defer independent worker launch until setup, after hook registration;
reinitialization reuses the same controller and never replaces the hooks.

A retained liveness view refers to that exact instance. Only a fresh, exactly
Live native query admits work; any observed terminal intent permanently closes
the view. `request_fault()` first reports genuine native fault. The view grants
no publication lease and does not turn local cancellation into native success.
Under its physical phase gate, the trusted controller must check immutable
instance/receiver binding and local terminal flags, sample the view, then
recheck local flags. A cached Live result cannot authorize a later phase;
subsequent intent cannot undo an operation already admitted.

The request hook runs at most once and must only close controller admission and
perform bounded cancel/wake. It must not join, do I/O, wait for a gate, or reenter
the SDK. Finish runs independently at most once on the physical cleanup thread,
after startup ownership is published, request has returned, the sole initial
setup task has settled, and any admitted reinitializer has returned. Finish must
fence and join the controller's actual workers. `PhysicallyStopped` asserts that
work; a timeout or host shutdown return does not prove it.

The SDK retains setup futures across cancellation, destroys them on the
physical cleanup thread, and drops callback captures outside resource locks.
Reinitialization polls its result before terminal cancellation when both are
ready. Repeated successful reinitialization retires its previous future through
one bounded cell on that same physical worker; the next setup starts only after
the worker confirms disposal. Callback admission uses bookkeeping exclusion,
with foreign invocation, request and destruction outside every resource lock.
The runtime is destroyed on the physical thread. Surviving SDK resources,
spawn failure, a hook panic, or unproved cleanup keep the owner uncertain and
anchored. Seven separate primary panic cells retain request, finish, initial
setup, reinitialize, factory, startup, and cleanup payloads. Native fault and
request completion precede any payload disposal. After the first cleanup
destructor panic, later resource owners remain attached rather than being
destroyed during cleanup unwinding. Once actual setup execution has settled,
the controller finish still runs once; retained or panicking SDK teardown keeps
the result uncertain even if that controller finish succeeds. A counted observer
retains the entire Connection through its last field and all cloned method
observers. A destructor unwind explicitly reports native Fault even after
diagnostic callbacks have been suppressed; a transport count or runtime timeout
cannot certify whole-Connection destruction.

Each SDK image has 256 instance slots. The existing opaque `module_ctx` contains
a slot/generation address token, never a dereferenced pointer or extra ABI
allocation. Callback lookup acquires the exact State before releasing registry
exclusion. Successful teardown retires its registry anchor before publishing
physical completion; only actual State last-Drop releases the slot, so old views
and surviving SDK resources count against the limit.
Generations never wrap: exhaustion permanently denies that
slot. Null, stale or unknown context tokens return `TB_CLOSED` for otherwise
valid terminal callbacks and cannot select a replacement. Legacy shutdown's
null-context behavior remains unchanged.
Tokens are private bookkeeping, not native authentication.

The SDK's `__native-fixture-support` feature is private and disabled by default.
It exposes only compiled-fixture instrumentation, not production authority.

This metadata bound does not bound existing host-context or mapped-library
retention. Libraries remain mapped until process exit, including a rejected
initialization whose independent SDK finish job is still running. A rejected
initialization without an accepted vtable cannot earn host PhysicalStopped
evidence. The native query remains authoritative after local Stop and before
any successful shutdown return or context retirement. Legacy exports retain
their existing lifecycle behavior.

## Loading sequence

1. Check every directory component's ownership/mode, require a regular
   platform library file no larger than 512 MiB, and enforce `modules.toml`
   when present. On Unix a component must be owned by the user or root and
   not writable by anyone else: world write is refused unless the sticky bit
   is set, and group write is refused unless the group grants nobody else
   (the user's private group, on Linux only, or `wheel`/`admin`
   on a root-owned macOS directory). A private group is the user's primary
   group, named after the user, with no other member and no other account using it as its primary group. It is established only from `/etc/passwd` and `/etc/group` on a host whose `nsswitch.conf` names no directory service (`files`, or `files systemd` with no systemd user records), and never for a directory with a POSIX ACL; otherwise group write stays refused.
2. Read an adjacent lazy manifest when one is present; otherwise load eagerly
   and locally (`RTLD_NOW | RTLD_LOCAL` on Unix). On Windows, search the
   module's directory first for its dependencies, then System32. An artifact
   load refusal includes the Win32 error code so a missing dependency can be
   diagnosed.
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
`lazy_init` to `true`. The sidecar must be a regular file, must not be a
symlink, and must be no larger than 1 MiB. A sidecar that fails admission
refuses the artifact instead of falling back to eager loading.

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

The host vtable also carries borrowed JSON configuration. Module load and
reinitialization control calls are always marked sensitive; callers cannot opt
out, and monitors never receive their bodies. This host-control marker is
deliberately distinct from an attested confidential call: the broker and its
in-process module host must read configuration in order to initialize the
module. The SDK copies and deserializes the configuration during
initialization, and the module never retains a pointer into host memory.
`module_export!` accepts `config = MyConfig` for an async setup function shaped
`setup(Connection, MyConfig)`, while the original `setup(Connection)` form
ignores configuration. Operators pass the value from a private file with
`tinybus modules load <path> --config-file <file>`, or use `--config-file -` to
avoid putting a secret in argv or on disk.
For directory discovery, embedding hosts call `ModuleHost::set_config(name,
value)` (or the builder-form `with_config`) before `load_dir`; the value is
selected by the admitted manifest name.

Configured modules can apply replacement values without unloading their
library or dropping their bus connection. Call
`Connection::reinitialize_module`, `ModuleHost::reinitialize`, or
`tinybus modules reinitialize <name> --config-file <file>`. The SDK invokes the
same typed setup function again. Setup should validate before mutating shared
state: a returned error keeps the runtime alive, but cannot roll back changes
the module already made. Modules built before the optional ABI callback was
added continue to load and report that reinitialization is unsupported.
Reinitialization has a mandatory four-second module-side deadline and is
serialized with both other reinitializations and shutdown. A host timeout never
permits a later lifecycle callback to overlap code that is still returning.

Hosts that choose configuration from each caller can use
`module::ReconfigurableModule` to hash the selected JSON configuration and
reinitialize only when it changes. The coordinator stores a digest, not the
configuration. Its default strategy uses lifecycle reinitialization; a
`Reconfigure::Member("Configure")` strategy calls a module-owned member with
the configuration as its sole argument over a confidential call, so the
recipient must be attested. `retry_while_initializing` retries only a typed
initializing result, within the configured attempt and delay bounds.

`proxy(config)` serializes configuration transitions but releases its lock
before the caller uses the returned proxy. When a bus call must run under the
selected configuration, hold `exclusive()` through that call and obtain the
proxy with `guard.proxy(config)`. All users of the coordinator must follow this
contract; calling `proxy(config)` while holding its guard waits on the same
lock.

Stopping a module loaded with `HostCallbackBinding` revokes new callback
admission and waits for already admitted user futures before starting native
shutdown. Both phases share the stop deadline. A drain timeout refuses shutdown;
it does not report unfinished provider work as stopped. Accounting stays attached
to the loaded record even after the routing capability is revoked or dropped.
Detached provider tasks need their own completion fence.

Canceling or timing out a public lifecycle waiter does not end a running native
callback. Enable/disable remains unavailable until that callback actually
returns. Queued stop and reinitialize operations retain their existing mutex
serialization. See [callback lifecycle validation](callback-lifecycle-validation.md)
for regression evidence and the limits of this guarantee.

When present, `modules.toml` is authoritative for its directory. Keys are
artifact file names (or stems) and values are lowercase SHA-256 hashes. An
artifact absent from the file or with a mismatched hash is refused. For remote
loads, the host supplies the expected SHA-256 alongside the artifact URL;
hashing happens before the platform loader opens the artifact. Search paths are
host-configured through `SearchPathConfig::new(env_var, app_dir)`. When set,
path-list entries from `env_var` come first, followed by the app-specific user
and system data directories for the platform. A `ModuleHost` without this
configuration has no implicit search paths. `tinybus modules scan --path <dir>
--dry-run` performs admission and dependency checks without initializing or
attaching.

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

## Release cache

`ModuleHost::load_github_release_cached` is `load_github_release` with a
directory that outlives the process. The host names it — one per module
version — and the first load downloads into a staging directory beside it,
verifies the archive against the release's `checksum.toml` and the host's pin,
extracts, records the manifest digest beside the archive, and commits with one
rename. Every later load re-hashes the archive on disk against the pin, checks
the extracted library against the release's own `modules.toml`, and maps it
without touching the network. A directory that fails either check is a miss,
not a refusal: the next download replaces it.

Asset URLs are built from the tag rather than looked up through the REST API,
whose unauthenticated budget is shared by everyone behind one address; the API
is only the fallback for a release whose direct path answers 404. Every
request carries resolve, connect and response budgets, so an address that
drops packets costs seconds rather than the operating system's SYN timeout.
Set `allow_download` to `false` to make a miss a refusal, for a host whose
operator has disabled downloads.

## Installer bundle

`load_first_admitted` consults a read-only, installer-shipped directory
(`ReleasePlan::bundled_root`) before the release cache, with the same
`<id>/<version>/<host_key>/` layout. A bundle entry is the archive plus its
extraction, checked exactly like a cache hit, or, where the archive cannot
ship, the extraction plus `<archive>.sha256` holding the host's pin. A
notarized macOS app is that case: Apple's notary service unpacks nested
archives and rejects the unsigned Mach-O inside, and signing it would change
the pinned bytes.

A marker entry is admitted only when the archive is absent, the marker equals
the compiled pin, and a `modules.toml` beside the extraction names the
library with its current hash. That allowlist is required on this path,
because nothing else on it hashes the file that is mapped. On either path,
the library must resolve inside its own directory: a symlink to a file
elsewhere is refused.
The marker is a claim made by the installer, which verified the archive
against the same pin at build time. Integrity of the extraction then rests on
the installer's own protection; on macOS that is the app's code-signature
seal. The marker is honoured only from the bundle, never from the
user-writable release cache, where a text file vouching for itself would void
the pin.

The extracted library, the file that is actually mapped, is still held to the
release's `modules.toml` on the lookup and again at the allowlist gate on
every load, the same as when the archive is present. An installer that
rewrites the library must re-pin its `modules.toml` entry under the same seal,
or the bundle is refused. The macOS signer does this, because codesigning
changes the file.

A marker entry is never attested with the pin, because nothing on that path
hashed bytes matching it. Its attestation, and so its eligibility for
confidential messages, comes only from the required `modules.toml`, which
activation re-hashes against the mapped file.

A bundle with any candidate entry is authoritative. A refused entry is
reported, not replaced by a download.

Refusing one artifact does not prevent the host from admitting other artifacts
in the same directory. The refused artifact's error contains only a sanitized
basename and fixed reason; a Unix directory refusal ends by naming the one
ancestor and its mode (`... at opt mode 0777`). Only a fixed system or XDG
location (`/usr`, `.cache`) is named; the filesystem root, the module directory and
the home directory are named as such, and any other component is reported as
`an ancestor directory`, so no account name or path reaches the error.

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

### Per-instance setup state

Configured exports can use `setup_factory = factory` instead of `setup = setup`.
The safe, no-argument factory returns a setup callback with the same
`Fn(Connection, Config) -> Future<Output = Result<()>>` signature. TinyBus calls
it once after parsing valid initial configuration, then retains its captured
state for that instance's initial setup and every reinitialization. The callback
is shared by reference; its Clone implementation cannot detach that state. Separate
ABI instances get fresh state; no global registry is needed.

This permits a module to capture an immutable admission or mode in an Arc and
reject later configuration that would change it before replacing its served
object. The module owns that admission logic. Existing callback serialization,
configuration validation, deadlines and error codes remain authoritative.
Factory panics return the native initialization panic code. The factory form
is available in module_export, module_export_static and
module_export_optional_static; legacy setup callbacks remain supported.

When setup also needs the loader-issued host binding, use
`setup_factory_with_context = factory, config = Config` in either export macro.
Its factory returns `Fn(ModuleSetupContext, Config) -> Future<Result<()>>` and
runs once per ABI instance after host validation and initial config parsing.
The retained callback receives a new context for each initial setup or
reinitialization, so its connection matches the active module attachment while
its captured per-instance mode remains stable. A generic module can see
`context.trusted_host() == None`; a migration service must require the binding
before registering itself or starting ordinary storage access.

```rust,ignore
fn setup_factory() -> impl Fn(ModuleSetupContext, Config) -> SetupFuture {
    let admitted_mode = Arc::new(Mutex::new(None));
    move |context, config| {
        // Validate the mode against this factory's retained state, then use
        // context.connection() and context.trusted_host() for this attachment.
        Box::pin(setup_with_context(context, config, Arc::clone(&admitted_mode)))
    }
}

tinybus_module::module_export! {
    setup_factory_with_context = setup_factory,
    config = Config,
    worker_threads = 1,
    provides = ["org.example.Service"],
    methods = [], signals = [], requires = [], optional = [], lazy = false,
}
```

## Controller admission

`ModuleHost::new` uses its supplied broker's immutable module-control policy.
Legacy `Broker::new` permits peer module-control calls. Select
`Broker::new_restricted_module_control` and attach one trusted controller
transport when configuration carries credentials or scoped actor authority.
See [restricted module control](../broker/README.md#restricted-module-control-modules-feature)
for endpoint ownership, denial semantics and controller lifetime.

## Host-required activation

Use ModuleHost::load_file_with_requirements when setup must not run unless the
module exposes a particular contract. Construct ModuleRequirements from the
exact package identity, validated bus name and object path, plus
RequiredInterface entries containing interface names, accepted version ranges
and mandatory method names. Versions describe the provided interface's
implemented version; all required methods must belong to that same interface.

The loader checks the actual loaded artifact before activation and retains its
manifest and initializer together. It refuses provisional .manifest.json
sidecars on this path. An actual embedded lazy-init declaration remains
supported: requirements are checked before the initializer is registered.
A mismatch cannot run setup or receive its configuration. Existing admission,
dependency, ABI, digest and confidential-delivery rules still apply.

This is admission for trusted native code. Platform loading may execute library
static constructors before the manifest can be inspected; it is not a process
sandbox. Directory and artifact permission checks retain their existing trust
boundary. After initialization, the host must still verify any instance-specific
immutable mode through the module's public contract before sending credentials.
