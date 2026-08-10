//! A complete dynamically loaded module used by documentation and loader CI.

use tinybus::{Connection, Result};

struct Clock;

#[tinybus::interface(name = "ai.tinyhumans.openhuman.Clock")]
impl Clock {
    async fn now(&self) -> Result<String> {
        Ok(format!("{:?}", std::time::SystemTime::now()))
    }
}

async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at("/ai/tinyhumans/openhuman/Clock".try_into()?, Clock)
        .await?;
    connection
        .request_name("ai.tinyhumans.openhuman.Clock")
        .await?;
    Ok(())
}

tinybus_module::module_export! {
    setup = setup,
    worker_threads = 1,
    provides = ["ai.tinyhumans.openhuman.Clock"],
    requires = [],
    optional = [],
    lazy = false,
}
