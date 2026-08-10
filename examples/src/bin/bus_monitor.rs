//! A consumer specialised for observability: subscribes to matching signals.

use tinybus::router::MatchRule;
use tinybus::{MemberName, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let connection = tinybus_examples::connect().await?;
    let mut signals = connection
        .add_match(
            MatchRule::new()
                .signals()
                .interface(tinybus_examples::CATALOG_INTERFACE.try_into()?)
                .member(MemberName::new("StockChanged")?),
        )
        .await?;
    println!("monitoring catalog signals; press Ctrl-C to stop");

    loop {
        tokio::select! {
            message = signals.recv() => match message {
                Ok(message) => println!("from {:?}: {:?} {}", message.header.sender, message.header.member, message.body),
                Err(error) => eprintln!("monitor lagged or closed: {error}"),
            },
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}
