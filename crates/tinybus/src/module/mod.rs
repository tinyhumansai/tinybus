//! ABI shared by module authors and hosts, plus the optional host loader.
//!
//! The ABI and manifest types are always compiled so a module can depend on
//! tinybus with no default features. Loading is behind `modules`: in-process
//! modules are trusted code with the host's full address-space privileges.

pub mod abi;
pub mod manifest;

#[cfg(feature = "modules")]
pub(crate) mod host;
#[cfg(feature = "modules")]
mod loader;
#[cfg(feature = "modules")]
mod transport;

#[cfg(feature = "modules")]
pub use host::{ModuleHost, ModuleInfo, ModuleState};
