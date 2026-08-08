//! A worked example of the extraction this project exists for: the speech
//! integration, as its own process.
//!
//! In-kernel, this feature drags `whisper-rs`, `cpal` and a model runtime into
//! every OpenHuman build — on every platform, for every user, whether or not
//! they ever dictate anything. Out here it is a binary that owns those
//! dependencies alone, and the kernel's side of it is a `Proxy` and a `serde`
//! derive.
//!
//! ```sh
//! # terminal 1
//! cargo run --bin tinybus --features cli -- serve
//! # terminal 2
//! cargo run --example voice_service --features "uds macros"
//! # terminal 3
//! tinybus call ai.tinyhumans.openhuman.Voice /ai/tinyhumans/openhuman/Voice \
//!     ai.tinyhumans.openhuman.Voice Transcribe '["/tmp/clip.wav"]'
//! ```
//!
//! The transcription here is a stub. Everything around it — the name, the
//! object, the interface, the signal, the shutdown behaviour — is exactly what
//! a real integration does.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tinybus::connection::Connection;
use tinybus::transport::unix::UnixTransport;
use tinybus::{Error, Result};

const NAME: &str = "ai.tinyhumans.openhuman.Voice";
const PATH: &str = "/ai/tinyhumans/openhuman/Voice";

/// A structured return value. Structs cross the bus as JSON objects, so
/// adding a field is backward-compatible and removing one is not — which is
/// the same contract discipline as any other API, now enforced at a process
/// boundary instead of by review.
#[derive(Debug, Serialize, Deserialize)]
struct Transcript {
    text: String,
    language: String,
    duration_seconds: f32,
}

struct Voice {
    /// Stands in for the loaded model — the several hundred megabytes of
    /// dependency and weights that used to live in the kernel's address space.
    model: String,
}

#[tinybus::interface(name = "ai.tinyhumans.openhuman.Voice")]
impl Voice {
    /// Transcribe a file. Errors carry a service-specific dotted name so the
    /// kernel can distinguish "no such file" from "no model loaded" without
    /// parsing prose.
    async fn transcribe(&self, path: String) -> Result<Transcript> {
        let file = PathBuf::from(&path);
        if !file.exists() {
            return Err(Error::MethodFailed {
                name: format!("{NAME}.Error.NoSuchFile"),
                message: format!("{path} does not exist"),
            });
        }
        Ok(Transcript {
            text: format!("[{}] a transcript of {path}", self.model),
            language: "en".to_string(),
            duration_seconds: 12.5,
        })
    }

    /// What this build can transcribe.
    async fn languages(&self) -> Result<Vec<String>> {
        Ok(vec!["en".to_string(), "sv".to_string(), "de".to_string()])
    }

    /// Which model is loaded, so the kernel can report it without knowing what
    /// a model is.
    async fn model(&self) -> Result<String> {
        Ok(self.model.clone())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let address = std::env::var(tinybus::DEFAULT_SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_address());

    let connection = Connection::connect(Box::new(UnixTransport::connect(&address).await?)).await?;

    // Export first, claim the name second. The other order has a window in
    // which the kernel can see the name, call it, and get "no object at that
    // path" from a service that was moments away from being ready.
    connection
        .serve_at(
            PATH.try_into()?,
            Voice {
                model: "whisper-large-v3".to_string(),
            },
        )
        .await?;
    connection.request_name(NAME).await?;

    println!(
        "voice service is up as {NAME} ({})",
        connection
            .unique_name()
            .map(|n| n.to_string())
            .unwrap_or_default()
    );

    // A heartbeat signal, to show the emit side. A real integration would emit
    // on something happening — a transcript finishing, a device disappearing.
    let mut ticker = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                connection
                    .emit(PATH.try_into()?, NAME.try_into()?, "Heartbeat".try_into()?, ())
                    .await?;
            }
            _ = tokio::signal::ctrl_c() => {
                // Dropping the connection releases the name and makes the
                // broker announce it, so the kernel learns immediately rather
                // than on its next timeout.
                println!("shutting down");
                return Ok(());
            }
        }
    }
}

fn default_address() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|dir| PathBuf::from(dir).join("tinybus").join("bus"))
        .unwrap_or_else(|_| PathBuf::from("/tmp/tinybus/bus"))
}
