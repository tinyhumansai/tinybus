//! The other half of `voice_service`: what the kernel's side looks like after
//! the extraction.
//!
//! Read this next to `voice_service.rs`. The whole point is how little there is
//! here — three strings, a `serde` struct, and a call. No model, no audio
//! stack, no platform `#[cfg]`, and no way for a panic inside a third-party
//! decoder to take this process with it.
//!
//! ```sh
//! cargo run --example kernel_client --features "uds macros" -- /tmp/clip.wav
//! ```

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use tinybus::connection::Connection;
use tinybus::router::MatchRule;
use tinybus::transport::unix::UnixTransport;
use tinybus::{MemberName, Result};

const NAME: &str = "ai.tinyhumans.openhuman.Voice";
const PATH: &str = "/ai/tinyhumans/openhuman/Voice";

/// The kernel's view of the contract. It does not have to match the service's
/// struct field for field — only the fields it actually reads.
#[derive(Debug, Deserialize)]
struct Transcript {
    text: String,
    language: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let clip = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/clip.wav".into());

    let address = std::env::var(tinybus::DEFAULT_SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("XDG_RUNTIME_DIR")
                .map(|dir| PathBuf::from(dir).join("tinybus").join("bus"))
                .unwrap_or_else(|_| PathBuf::from("/tmp/tinybus/bus"))
        });

    let connection = Connection::connect(Box::new(UnixTransport::connect(&address).await?)).await?;

    // Learn about the integration going away, rather than finding out when a
    // call times out thirty seconds later.
    let mut lifecycle = connection
        .add_match(
            MatchRule::new()
                .signals()
                .member(MemberName::new("NameOwnerChanged")?),
        )
        .await?;
    tokio::spawn(async move {
        while let Ok(message) = lifecycle.recv().await {
            eprintln!("bus lifecycle: {}", message.body);
        }
    });

    let voice = connection
        .proxy(NAME, PATH, NAME)?
        // Transcription is slow and bounded; a wallet signature would not get
        // anything like this long.
        .with_timeout(Duration::from_secs(120));

    if !voice.is_available().await? {
        // The distinction a user can act on: the feature is not installed or
        // not running, as opposed to the call having failed.
        eprintln!("the voice integration is not running; start it and try again");
        return Ok(());
    }

    let model: String = voice.call("Model", ()).await?;
    let languages: Vec<String> = voice.call("Languages", ()).await?;
    println!("using {model}, speaking {}", languages.join(", "));

    match voice
        .call::<Transcript>("Transcribe", (clip.as_str(),))
        .await
    {
        Ok(transcript) => println!("[{}] {}", transcript.language, transcript.text),
        // Matching on the dotted name, not on the prose: the message is for a
        // human and may be reworded, the name is the contract.
        Err(e) if e.wire_name().ends_with(".Error.NoSuchFile") => {
            eprintln!("{clip}: no such file");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
