//! A consumer process: discovers a service, makes typed calls, and listens for signals.

use std::time::Duration;

use serde::Deserialize;
use tinybus::Result;

#[derive(Debug, Deserialize)]
struct Product {
    id: String,
    name: String,
    cents: u64,
    stock: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let connection = tinybus_examples::connect().await?;
    let catalog = connection
        .proxy(
            tinybus_examples::CATALOG_NAME,
            tinybus_examples::CATALOG_PATH,
            tinybus_examples::CATALOG_INTERFACE,
        )?
        .with_timeout(Duration::from_secs(5));

    if !catalog.is_available().await? {
        eprintln!("catalog is not running; start catalog_service first");
        return Ok(());
    }

    let mut stock_changes = catalog.receive_signal("StockChanged").await?;
    let products: Vec<Product> = catalog.call("Products", ()).await?;
    for product in &products {
        println!(
            "{}: {} (${:.2}, {} in stock)",
            product.id,
            product.name,
            product.cents as f64 / 100.0,
            product.stock
        );
    }

    if let Some(product) = products.first() {
        match catalog.call::<Product>("Buy", (&product.id, 1_u32)).await {
            Ok(bought) => println!("bought one {} ({} remaining)", bought.name, bought.stock),
            Err(error) if error.wire_name().ends_with(".Error.OutOfStock") => {
                eprintln!("{} is out of stock", product.name);
            }
            Err(error) => return Err(error),
        }
    }

    println!("watching catalog signals for 6 seconds");
    let deadline = tokio::time::sleep(Duration::from_secs(6));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            result = stock_changes.recv() => match result {
                Ok(message) => println!("stock update: {}", message.body),
                Err(error) => {
                    eprintln!("signal stream ended: {error}");
                    break;
                }
            },
            _ = &mut deadline => break,
        }
    }
    Ok(())
}
