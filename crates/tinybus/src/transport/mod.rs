//! Adapters for the [`Transport`](crate::ports::Transport) and
//! [`Listener`](crate::ports::Listener) ports.
//!
//! [`memory`] is always compiled and is what the test suite runs on. [`unix`]
//! is the production transport and sits behind the `uds` feature, so a kernel
//! that only wants an in-process bus never links `tokio/net`.

pub mod memory;

#[cfg(all(feature = "uds", unix))]
pub mod unix;
