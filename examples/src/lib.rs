//! Shared addresses and connection setup for the complete tinybus example.

use std::path::PathBuf;

use tinybus::Result;
use tinybus::connection::Connection;
use tinybus::transport::unix::UnixTransport;

pub const CATALOG_NAME: &str = "com.example.Shop.Catalog";
pub const CATALOG_PATH: &str = "/com/example/Shop/Catalog";
pub const CATALOG_INTERFACE: &str = "com.example.Shop.Catalog";

/// Resolve the socket in the same way as the tinybus CLI.
pub fn socket_path() -> PathBuf {
    if let Ok(address) = std::env::var(tinybus::DEFAULT_SOCKET_ENV) {
        return PathBuf::from(address);
    }
    PathBuf::from(std::env::var("XDG_RUNTIME_DIR").expect(
        "set XDG_RUNTIME_DIR or TINYBUS_ADDRESS; tinybus sockets must live in a runtime directory",
    ))
    .join("tinybus")
    .join("bus")
}

/// Connect a process to the Unix-socket broker.
pub async fn connect() -> Result<Connection> {
    Connection::connect(Box::new(UnixTransport::connect(socket_path()).await?)).await
}
