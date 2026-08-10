//! The production transport: length-prefixed JSON over a Unix domain socket.
//!
//! A socket, not TCP, and that is a security decision rather than a performance
//! one. The bus is a capability handle: anything that can connect can ask the
//! wallet service to sign, or the mail service to read. A Unix socket inherits
//! filesystem permissions, so `0700` on the containing directory is the whole
//! access-control story; a TCP port has no such story and would need
//! authentication invented on top. That is also why the socket lives under the
//! user's runtime directory and not `/tmp`, which is world-writable.
//!
//! Windows gets a named-pipe backend as a sibling module, not as `#[cfg]`
//! sprinkled through this one. See `ROADMAP.md`.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::message::codec::{self, LENGTH_PREFIX_LEN};
use crate::ports::{Listener, Transport};

/// One peer's socket, split into an independently locked reader and writer.
///
/// Two locks rather than one: `send` is called concurrently by every proxy on
/// the connection while the single reader task is parked in `recv`. Sharing one
/// lock would make an idle read block every write.
pub struct UnixTransport {
    reader: Mutex<tokio::net::unix::OwnedReadHalf>,
    writer: Mutex<tokio::net::unix::OwnedWriteHalf>,
    label: String,
}

impl UnixTransport {
    /// Wrap an already-connected stream.
    pub fn new(stream: UnixStream, label: impl Into<String>) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            label: label.into(),
        }
    }

    /// Dial the broker at `path`.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let stream = UnixStream::connect(path)
            .await
            .map_err(|e| Error::path(path, format!("could not connect to the bus: {e}")))?;
        Ok(Self::new(stream, format!("unix:{}", path.display())))
    }
}

#[async_trait]
impl Transport for UnixTransport {
    async fn send(&self, message: Message) -> Result<()> {
        let frame = codec::encode(&message)?;
        let mut writer = self.writer.lock().await;
        // One `write_all` for the whole frame: a length prefix written
        // separately from its payload can interleave with a concurrent sender
        // and desynchronise the stream permanently.
        writer.write_all(&frame).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn recv(&self) -> Result<Option<Message>> {
        let mut reader = self.reader.lock().await;
        let mut prefix = [0u8; LENGTH_PREFIX_LEN];
        match reader.read_exact(&mut prefix).await {
            Ok(_) => {}
            // A clean hangup on a frame boundary is a peer exiting normally.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = codec::decode_length(prefix)?;
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).await.map_err(|e| {
            // Mid-frame EOF is not a clean shutdown: the peer died holding a
            // half-written message, and the caller waiting on a reply needs to
            // learn that rather than see a tidy `None`.
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::protocol("peer disappeared mid-frame")
            } else {
                Error::Io(e)
            }
        })?;
        Ok(Some(codec::decode(&payload)?))
    }

    async fn close(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        // `shutdown` on an already-closed socket is a normal shutdown race, so
        // its error is swallowed rather than surfaced — see the port contract.
        let _ = writer.shutdown().await;
        Ok(())
    }

    fn describe(&self) -> String {
        self.label.clone()
    }
}

/// The broker's accept side.
///
/// Owns the socket file: [`Drop`] unlinks it, because a stale socket left
/// behind by a crashed broker makes the next start fail with `EADDRINUSE` on a
/// path that is not, in any meaningful sense, in use.
#[derive(Debug)]
pub struct UnixListenerAdapter {
    listener: UnixListener,
    path: PathBuf,
}

impl UnixListenerAdapter {
    /// Bind at `path`, replacing a socket left behind by a previous run.
    ///
    /// The replacement is not unconditional: a *live* broker's socket is left
    /// alone and the bind fails, so two brokers cannot silently steal the bus
    /// from each other.
    pub async fn bind(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if path.exists() {
            if UnixStream::connect(&path).await.is_ok() {
                return Err(Error::path(
                    &path,
                    "a tinybus broker is already listening here",
                ));
            }
            std::fs::remove_file(&path)
                .map_err(|e| Error::path(&path, format!("could not clear a stale socket: {e}")))?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::path(parent, format!("could not create: {e}")))?;
        }
        let listener = UnixListener::bind(&path)
            .map_err(|e| Error::path(&path, format!("could not bind: {e}")))?;
        Ok(Self { listener, path })
    }

    /// The path this listener is bound to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for UnixListenerAdapter {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[async_trait]
impl Listener for UnixListenerAdapter {
    async fn accept(&self) -> Result<Option<Box<dyn Transport>>> {
        loop {
            match self.listener.accept().await {
                Ok((stream, _addr)) => {
                    return Ok(Some(Box::new(UnixTransport::new(
                        stream,
                        format!("unix:{}", self.path.display()),
                    ))));
                }
                // Per-connection failures are the client's problem. Taking the
                // broker down because one peer aborted its handshake would let
                // any local process kill the bus.
                Err(e) if is_per_connection(&e) => {
                    tracing::debug!(error = %e, "dropped a peer during accept");
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn describe(&self) -> String {
        format!("unix:{}", self.path.display())
    }
}

/// Whether an accept error concerns one client rather than the listener.
fn is_per_connection(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionAborted | ConnectionReset | Interrupted | WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name::{BusName, InterfaceName, MemberName, ObjectPath};

    fn message(member: &str, body: serde_json::Value) -> Message {
        Message::method_call(
            BusName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            ObjectPath::new("/ai/tinyhumans/openhuman/Voice").unwrap(),
            InterfaceName::new("ai.tinyhumans.openhuman.Voice").unwrap(),
            MemberName::new(member).unwrap(),
            body,
        )
    }

    #[tokio::test]
    async fn a_message_survives_the_socket_with_newlines_intact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bus");
        let listener = UnixListenerAdapter::bind(&path).await.unwrap();

        let client = UnixTransport::connect(&path).await.unwrap();
        let server = listener.accept().await.unwrap().expect("a peer");

        let sent = message("Transcribe", serde_json::json!(["a\nb\nc"]));
        client.send(sent.clone()).await.unwrap();
        assert_eq!(server.recv().await.unwrap().unwrap(), sent);
    }

    #[tokio::test]
    async fn a_clean_hangup_is_none_rather_than_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bus");
        let listener = UnixListenerAdapter::bind(&path).await.unwrap();
        let client = UnixTransport::connect(&path).await.unwrap();
        let server = listener.accept().await.unwrap().expect("a peer");
        drop(client);
        assert!(server.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn binding_over_a_stale_socket_succeeds_but_over_a_live_one_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bus");

        let first = UnixListenerAdapter::bind(&path).await.unwrap();
        let err = UnixListenerAdapter::bind(&path).await.unwrap_err();
        assert!(err.to_string().contains("already listening"), "{err}");

        // Dropping unlinks; recreate the file to stand in for a crashed broker
        // that never got to clean up after itself.
        drop(first);
        std::fs::write(&path, b"").unwrap();
        UnixListenerAdapter::bind(&path)
            .await
            .expect("stale socket cleared");
    }

    #[tokio::test]
    async fn a_mid_frame_hangup_is_a_protocol_error_and_close_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bus");
        let listener = UnixListenerAdapter::bind(&path).await.unwrap();
        let mut raw = UnixStream::connect(&path).await.unwrap();
        let server = listener.accept().await.unwrap().expect("a peer");

        raw.write_all(&10u32.to_be_bytes()).await.unwrap();
        raw.write_all(b"short").await.unwrap();
        drop(raw);
        assert!(server.recv().await.unwrap_err().to_string().contains("mid-frame"));
        server.close().await.unwrap();
        server.close().await.unwrap();
    }

    #[tokio::test]
    async fn socket_labels_and_failed_dials_are_operator_useful() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing");
        assert!(UnixTransport::connect(&path).await.unwrap_err().to_string().contains("could not connect"));

        let listener = UnixListenerAdapter::bind(&path).await.unwrap();
        let client = UnixTransport::connect(&path).await.unwrap();
        assert_eq!(client.describe(), format!("unix:{}", path.display()));
        assert_eq!(listener.describe(), format!("unix:{}", path.display()));
        assert_eq!(listener.path(), path.as_path());
    }

    #[test]
    fn transient_accept_errors_are_classified_without_hiding_fatal_errors() {
        assert!(is_per_connection(&std::io::Error::from(std::io::ErrorKind::ConnectionReset)));
        assert!(is_per_connection(&std::io::Error::from(std::io::ErrorKind::Interrupted)));
        assert!(!is_per_connection(&std::io::Error::from(std::io::ErrorKind::PermissionDenied)));
    }
}
