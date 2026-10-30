//! Actual eager native fixture calling its fixed host during setup before READY.
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tinybus::{BusName, Connection, Error, Interface, InterfaceName, MemberName, Result};

#[derive(Deserialize)]
struct Config {
    host: tinybus::BusName,
    #[serde(default)]
    fail: bool,
    #[serde(default = "confidential_by_default")]
    confidential: bool,
}

fn confidential_by_default() -> bool {
    true
}
struct CallbackModule(Connection, BusName);

#[async_trait]
impl Interface for CallbackModule {
    fn name(&self) -> InterfaceName {
        "org.example.EagerCallback".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec!["Observe".parse().unwrap()]
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        let reply: String = self
            .0
            .proxy(self.1.as_str(), "/callback", "org.example.Callback")?
            .call_confidential("Observe", ())
            .await?;
        Ok(serde_json::json!(reply))
    }
}

async fn setup(connection: Connection, config: Config) -> Result<()> {
    async {
        if config.fail {
            return Err(Error::failed("fixture initialization rejected"));
        }
        let proxy = connection.proxy(config.host.as_str(), "/callback", "org.example.Callback")?;
        let _: tinybus::BusName = if config.confidential {
            proxy.call_confidential("Observe", ()).await?
        } else {
            proxy.call("Observe", ()).await?
        };
        connection
            .serve_at(
                "/org/example/EagerCallback".parse()?,
                CallbackModule(connection.clone(), config.host.clone()),
            )
            .await?;
        connection.request_name("org.example.EagerCallback").await
    }
    .await
}

tinybus_module::module_export! {
    setup = setup, config = Config,
    worker_threads = 1,
    provides = ["org.example.EagerCallback"],
    methods = ["Observe"], signals = [], requires = [], optional = [], lazy = false,
}
