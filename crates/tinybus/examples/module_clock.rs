//! A complete dynamically loaded module used by documentation and loader CI.

use tinybus::{Connection, Result};

#[derive(serde::Deserialize)]
struct ClockConfig {
    #[serde(default)]
    prefix: String,
}

struct Clock {
    prefix: String,
}

#[tinybus::interface(name = "ai.tinyhumans.openhuman.Clock")]
impl Clock {
    async fn now(&self) -> Result<String> {
        Ok(format!("{}{:?}", self.prefix, std::time::SystemTime::now()))
    }

    async fn panic(&self) -> Result<()> {
        panic!("panic payload must never cross the module boundary: secret-token")
    }
}

async fn setup(connection: Connection, config: ClockConfig) -> Result<()> {
    connection
        .serve_at(
            "/ai/tinyhumans/openhuman/Clock".try_into()?,
            Clock {
                prefix: config.prefix,
            },
        )
        .await?;
    connection
        .request_name("ai.tinyhumans.openhuman.Clock")
        .await?;
    Ok(())
}

tinybus_module::module_export! {
    setup = setup,
    config = ClockConfig,
    worker_threads = 1,
    provides = ["ai.tinyhumans.openhuman.Clock"],
    requires = [],
    optional = [],
    lazy = false,
}
