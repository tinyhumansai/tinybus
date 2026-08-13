//! The transport port: a bidirectional, ordered, framed message stream.
//!
//! # The contract
//!
//! - **Ordered.** Messages arrive in send order. The router assumes it; signal
//!   ordering relative to the reply that caused them is observable behaviour.
//! - **Framed.** `recv` yields whole [`Message`]s or nothing. Partial frames
//!   are the implementation's problem, never the caller's.
//! - **`recv` is called from exactly one task.** Both the connection and the
//!   broker's peer loop own a single reader task per transport, so an
//!   implementation may hold a lock across the await in `recv` without
//!   deadlocking. `send` has no such restriction and must be callable
//!   concurrently — every proxy on a connection shares one transport.
//! - **`Ok(None)` from `recv` means clean shutdown**, and is not an error. A
//!   service exiting normally must not log a stack of transport errors on the
//!   kernel side.

use async_trait::async_trait;

use crate::error::Result;
use crate::message::Message;

/// One peer's bidirectional link, framed at the message level.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Write one message. Must be safe to call from many tasks at once.
    async fn send(&self, message: Message) -> Result<()>;

    /// Read the next message, or `Ok(None)` once the peer has hung up cleanly.
    ///
    /// Called from a single task per transport; see the module docs.
    async fn recv(&self) -> Result<Option<Message>>;

    /// Close the link. Idempotent: shutdown races are normal, and a second
    /// close must not be an error.
    async fn close(&self) -> Result<()>;

    /// A short label for logs and `tinybus monitor`, e.g. `memory` or
    /// `unix:/run/user/1000/tinybus`. Never a credential.
    fn describe(&self) -> String {
        "transport".to_string()
    }
}
