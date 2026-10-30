# Rust 1.85 host-client proof

This isolated consumer compiles the public TinyBus host/core crate and its
default macros at the declared Rust 1.85 MSRV, then sends a real request through
the in-memory broker and receives the reply through `Proxy`. It intentionally
does not activate the module loader or link the separate `tinybus-module`
runtime; compiled module fixtures continue to run at their declared Rust 1.88
MSRV in the main workspace.
