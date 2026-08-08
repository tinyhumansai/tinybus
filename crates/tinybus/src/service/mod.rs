//! The service side: what an integration exports, and how a call finds it.
//!
//! An integration implements [`Interface`] — usually by writing a normal `impl`
//! block and putting `#[tinybus::interface]` on it — and registers instances at
//! [`ObjectPath`](crate::name::ObjectPath)s on its connection. Dispatch is then
//! `path → interface → member`, in that order, with a distinct error at each
//! step so a caller can tell "that integration isn't running" from "that
//! integration is running an older contract".
//!
//! # Why the trait takes and returns [`Value`]
//!
//! Typed dispatch is what the macro *generates*; the trait underneath has to be
//! object-safe, because one connection holds a heterogeneous list of interfaces
//! behind `dyn`. The macro turns a typed Rust signature into an argument
//! deserialize, a call, and a return serialize — so the handwritten code is
//! typed and only the seam is dynamic.

pub mod tree;

#[cfg(test)]
mod macro_test;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::name::{InterfaceName, MemberName};

pub use crate::service::tree::ObjectTree;

/// One contract a service implements.
///
/// Implement this by hand only for dynamic interfaces — a bridge that proxies
/// an interface it does not know at compile time. Everything else should use
/// `#[tinybus::interface]`.
#[async_trait]
pub trait Interface: Send + Sync + 'static {
    /// The interface's name, e.g. `ai.tinyhumans.openhuman.Voice`.
    fn name(&self) -> InterfaceName;

    /// Every member this interface dispatches, for introspection.
    ///
    /// `tinybus call` uses it to fail fast with the available members listed,
    /// which is the difference between a typo taking ten seconds and ten
    /// minutes.
    fn members(&self) -> Vec<MemberName>;

    /// Run `member` against `args`, a positional JSON array.
    ///
    /// Returning `Err` is normal and expected: it becomes an error reply, and
    /// the caller sees [`crate::Error::MethodFailed`] carrying
    /// [`crate::Error::wire_name`]. A panic, by contrast, takes the whole
    /// service down — [`crate::connection::Connection`] does not catch unwinds,
    /// because a service that has panicked has an unknown internal state and
    /// answering the next call from it is worse than being restarted.
    async fn call(&self, member: &MemberName, args: Value) -> Result<Value>;
}

#[async_trait]
impl<T: Interface + ?Sized> Interface for std::sync::Arc<T> {
    fn name(&self) -> InterfaceName {
        (**self).name()
    }

    fn members(&self) -> Vec<MemberName> {
        (**self).members()
    }

    async fn call(&self, member: &MemberName, args: Value) -> Result<Value> {
        (**self).call(member, args).await
    }
}
