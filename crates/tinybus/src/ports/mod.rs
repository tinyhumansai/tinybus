//! The bus's dependency-inverted seams. One port is one trait in one file.
//!
//! There are only two, and that is the point. Everything above them — the
//! router, the connection, the proxy, the `#[interface]` dispatch — is written
//! against [`Transport`] and [`Listener`] and has never heard of a socket. That
//! is why the entire test suite runs in-process, why a Windows named-pipe
//! backend is a new file rather than a new code path, and why the kernel can
//! embed a broker with `--no-default-features` and link no networking at all.

pub mod listener;
pub mod transport;

pub use crate::ports::listener::Listener;
pub use crate::ports::transport::Transport;
