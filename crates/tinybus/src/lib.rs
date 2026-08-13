//! tinybus — a zbus-style message bus for everything the OpenHuman kernel talks
//! to that is not the OpenHuman kernel.
//!
//! # Why this exists
//!
//! The kernel's dependency graph grew by absorption. A speech feature pulled in
//! `whisper-rs` and `cpal`; a document tool pulled in `pdf-extract`, `ppt-rs`
//! and `docx-rs`; a wallet pulled in `ethers`, `bitcoin` and
//! `curve25519-dalek`; a browser tool pulled in `fantoccini`. None of that is
//! kernel logic, but all of it is kernel *build time*, kernel binary size, and
//! kernel CVE surface — and a crash in any of it is a crash in the kernel.
//!
//! tinybus moves those integrations behind a wire. An integration becomes a
//! **service**: its own process, its own Cargo graph, its own crash domain,
//! announcing a **well-known name** on the bus. The kernel keeps a
//! [`Proxy`] and a `serde` derive, and nothing else.
//!
//! # The model, in one paragraph
//!
//! Same shape as D-Bus, because the shape is right and everyone already knows
//! it. A [`Connection`] is one peer's link to the
//! [`Broker`](broker::Broker). Every peer gets a unique name (`:1.7`) at
//! connect time and may request well-known names
//! (`ai.tinyhumans.openhuman.Voice`). A peer exports **objects** at
//! [`ObjectPath`]s; each object implements one or more
//! [`Interface`]s; an interface has **methods** you call
//! and **signals** it emits. Calls are addressed
//! `destination + path + interface + member`; signals are broadcast and
//! delivered to whoever added a matching [`MatchRule`].
//!
//! # The shape of the crate
//!
//! Ports & adapters, following `tinysweeper`. [`ports`] holds one trait per
//! file, every port has an always-compiled offline implementation, and only the
//! socket-backed adapter sits behind a Cargo feature. That is what makes the
//! test suite hermetic: [`transport::memory`] runs a broker, three services and
//! a client inside one process with no file descriptors, so `cargo test` can
//! assert on exact message ordering.
//!
//! ```no_run
//! # async fn example() -> tinybus::Result<()> {
//! use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};
//!
//! // A broker, a service and a client — all in one process, no sockets.
//! let bus = MemoryBus::new();
//! Broker::new().spawn(bus.clone());
//!
//! let client = Connection::connect(bus.connect().await?).await?;
//! let voice = client.proxy(
//!     "ai.tinyhumans.openhuman.Voice",
//!     "/ai/tinyhumans/openhuman/Voice",
//!     "ai.tinyhumans.openhuman.Voice",
//! )?;
//! let transcript: String = voice.call("Transcribe", ("/tmp/clip.wav",)).await?;
//! # let _ = transcript;
//! # Ok(())
//! # }
//! ```
//!
//! Modules land milestone by milestone; see `ROADMAP.md`.

// `#[tinybus::interface]` expands to `::tinybus::…` paths, which do not resolve
// inside this crate. The alias lets the macro be exercised by this crate's own
// tests rather than only by a downstream one — the expansion is the part most
// likely to break silently, so it has to be covered here.
extern crate self as tinybus;

pub mod attest;
pub mod broker;
pub mod build_info;
pub mod connection;
pub mod error;
pub mod events;
pub mod global;
pub mod message;
pub mod module;
pub mod name;
pub mod native;
pub mod ports;
pub mod proxy;
pub mod router;
pub mod service;
pub mod transport;
pub mod version;

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;

pub use crate::attest::Attestation;
pub use crate::connection::Connection;
pub use crate::error::{Error, Result};
pub use crate::events::{
    Event, EventBus, EventBusConfig, EventHandler, EventReceiver, SubscriptionHandle, TryRecvError,
};
pub use crate::global::OnceBus;
pub use crate::message::{Header, Message, MessageKind};
pub use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};
pub use crate::native::{NativeRegistry, NativeRequestError};
pub use crate::ports::{Listener, Transport};
pub use crate::proxy::Proxy;
pub use crate::router::MatchRule;
pub use crate::service::Interface;
pub use crate::version::{
    Compatibility, InterfaceVersion, PeerManifest, PeerRecord, Version, VersionRange,
};

#[cfg(feature = "macros")]
pub use tinybus_macros::interface;

/// The crate version, reported by `tinybus --version` and by the bus's own
/// `GetId` method so a peer can tell which broker it is attached to.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The wire-format revision, reserved for a change an older peer cannot parse.
///
/// Interface additions do not bump this value. It is published for manifests
/// and diagnostics; v1 does not put it in the `Hello` handshake.
pub const PROTOCOL_VERSION: u32 = 1;

/// The well-known name of the broker's own service.
///
/// Reserved: [`broker::Broker`] refuses any `RequestName` for it.
pub const BUS_NAME: &str = "ai.tinyhumans.tinybus.Bus";

/// The object path the broker's own service is exported at.
pub const BUS_PATH: &str = "/ai/tinyhumans/tinybus/Bus";

/// The broker's own interface: `Hello`, `RequestName`, `ListNames`, `AddMatch`…
pub const BUS_INTERFACE: &str = "ai.tinyhumans.tinybus.Bus";

/// The default socket path, overridable with `TINYBUS_ADDRESS`.
///
/// A path under the user's runtime dir, not `/tmp`: the socket is a capability
/// handle to every integration the kernel owns, and `/tmp` is world-writable.
pub const DEFAULT_SOCKET_ENV: &str = "TINYBUS_ADDRESS";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_the_package_version() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn bus_constants_parse_as_the_names_they_claim_to_be() {
        BusName::try_from(BUS_NAME).expect("bus name");
        ObjectPath::try_from(BUS_PATH).expect("bus path");
        InterfaceName::try_from(BUS_INTERFACE).expect("bus interface");
    }
}
