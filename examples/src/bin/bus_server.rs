//! The broker/server process: accepts peers and routes calls and signals.

use tinybus::Result;
use tinybus::broker::Broker;
use tinybus::transport::unix::UnixListenerAdapter;

#[tokio::main]
async fn main() -> Result<()> {
    let address = tinybus_examples::socket_path();
    let listener = UnixListenerAdapter::bind(&address).await?;
    println!("tinybus broker listening at {}", address.display());

    let broker = Broker::new();
    tokio::select! {
        result = broker.serve(listener) => result,
        _ = tokio::signal::ctrl_c() => {
            println!("broker stopping");
            Ok(())
        }
    }
}
