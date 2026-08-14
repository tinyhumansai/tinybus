//! A stateful service process: exports an interface, claims a name, and emits signals.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tinybus::{Error, Result};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Product {
    pub id: String,
    pub name: String,
    pub cents: u64,
    pub stock: u32,
}

struct Catalog {
    products: Arc<RwLock<Vec<Product>>>,
}

#[tinybus::interface(name = "com.example.Shop.Catalog")]
impl Catalog {
    async fn products(&self) -> Result<Vec<Product>> {
        Ok(self.products.read().await.clone())
    }

    async fn buy(&self, id: String, quantity: u32) -> Result<Product> {
        let mut products = self.products.write().await;
        let product = products
            .iter_mut()
            .find(|product| product.id == id)
            .ok_or_else(|| Error::MethodFailed {
                name: "com.example.Shop.Catalog.Error.NotFound".to_string(),
                message: "the requested product was not found".to_string(),
            })?;
        if quantity == 0 || product.stock < quantity {
            return Err(Error::MethodFailed {
                name: "com.example.Shop.Catalog.Error.OutOfStock".to_string(),
                message: "the requested quantity is not available".to_string(),
            });
        }
        product.stock -= quantity;
        Ok(product.clone())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let connection = tinybus_examples::connect().await?;
    let products = Arc::new(RwLock::new(vec![
        Product {
            id: "coffee".into(),
            name: "Coffee beans".into(),
            cents: 1299,
            stock: 8,
        },
        Product {
            id: "tea".into(),
            name: "Green tea".into(),
            cents: 899,
            stock: 12,
        },
    ]));

    connection
        .serve_at(
            tinybus_examples::CATALOG_PATH.try_into()?,
            Catalog {
                products: products.clone(),
            },
        )
        .await?;
    connection
        .request_name(tinybus_examples::CATALOG_NAME)
        .await?;
    println!(
        "catalog service ready as {}",
        tinybus_examples::CATALOG_NAME
    );

    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let snapshot = products.read().await.clone();
                connection.emit(
                    tinybus_examples::CATALOG_PATH.try_into()?,
                    tinybus_examples::CATALOG_INTERFACE.try_into()?,
                    "StockChanged".try_into()?,
                    snapshot,
                ).await?;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("catalog service stopping");
                return Ok(());
            }
        }
    }
}
