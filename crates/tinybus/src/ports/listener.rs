//! The listener port: the broker's side of "a peer showed up".
//!
//! Separate from [`crate::ports::Transport`] because the broker is the only
//! thing that accepts, while everything on the bus transports. Keeping them
//! apart means a service links the accept path out entirely.

use async_trait::async_trait;

use crate::error::Result;
use crate::ports::Transport;

/// A source of inbound peer connections.
#[async_trait]
pub trait Listener: Send + Sync + 'static {
    /// Wait for the next peer, or `Ok(None)` when the listener is shut down.
    ///
    /// An implementation must treat a *per-connection* failure (a peer that
    /// dies mid-handshake) as something to skip, not as a reason to return an
    /// error: one bad client must never take the broker's accept loop down.
    async fn accept(&self) -> Result<Option<Box<dyn Transport>>>;

    /// A short label for logs, e.g. `unix:/run/user/1000/tinybus`.
    fn describe(&self) -> String {
        "listener".to_string()
    }
}
