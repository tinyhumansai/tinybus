# Module ABI v1

Rust has no stable ABI. The module boundary therefore carries only `repr(C)`
plain data, opaque pointers, borrowed byte slices and `extern "C"` callbacks.
No Rust allocation, trait object, future, Tokio type, panic payload or
`repr(Rust)` value crosses it.

## Symbols

Every module exports exactly these revisioned symbols:

- `TINYBUS_MODULE_ABI_V1`: a data descriptor;
- `tinybus_module_manifest_v1()`: borrowed JSON manifest bytes;
- `tinybus_module_init_v1(host, out)`: initialization after admission.

Putting the revision in each name makes an old module fail symbol lookup before
the host interprets a changed layout.

## Descriptor gate

The first 16 bytes are frozen: magic, revision, and descriptor size. The host
rejects a wrong prefix, sizes outside 16–4096 bytes, and descriptors smaller
than the v1 structure before reading more. Larger descriptors are accepted and
their tail ignored.

Admission then requires matching pointer width, endianness, target triple, a
compatible tinybus series, and module feature bits that are a subset of the
host's. Modules built with `panic=abort` are refused. A rustc mismatch warns in
permissive mode and refuses in strict mode.

Fixed byte arrays hold target, toolchain and identity fields. Displayed values
keep only ASCII letters, digits, `.`, `_`, `+`, and `-`, capped at 32
characters.

## Frames and runtimes

Both directions pass one borrowed JSON `Message` document without the socket
transport's four-byte length prefix. The 16 MiB frame cap applies at both ends.
The receiver copies the bytes before returning from the callback.

Each module creates its own Tokio runtime. A statically linked library has a
separate copy of Tokio's thread-locals, so it cannot borrow the host runtime.
The host-to-module `deliver` callback only performs a bounded `try_send` and
never blocks a host thread. Queue capacity is reported as backpressure and the
host retries after the module's `wake` callback.

The SDK installs a panic hook that forwards only the source location, never the
payload, and faults the transport so the broker releases the module's names.
Native faults remain outside what an in-process boundary can contain.
