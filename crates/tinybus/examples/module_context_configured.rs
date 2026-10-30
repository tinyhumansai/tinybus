//! Native fixture exercising contextual dispatch and a callback on its own runtime.

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Connection, Error, Interface, InterfaceName, MemberName, Result};
use tinybus_module::{ModuleSetupContext, TrustedHost};

#[derive(serde::Deserialize)]
#[serde(default)]
struct Config {
    marker: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            marker: "initial".to_string(),
        }
    }
}

struct ContextService(
    Connection,
    std::sync::Arc<tokio::sync::Mutex<tokio::sync::broadcast::Receiver<tinybus::message::Message>>>,
    Option<TrustedHost>,
    String,
    std::sync::Arc<tokio::sync::Mutex<Option<CallContext>>>,
);

#[async_trait]
impl Interface for ContextService {
    fn name(&self) -> InterfaceName {
        "org.example.Context".parse().unwrap()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![
            "Inspect".parse().unwrap(),
            "Callback".parse().unwrap(),
            "ConfidentialCallback".parse().unwrap(),
            "WaitForWatchChanged".parse().unwrap(),
            "Configuration".parse().unwrap(),
            "TrustedContext".parse().unwrap(),
        ]
    }
    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("authenticated incoming context required"))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        let sender = context
            .authenticated_sender()
            .ok_or_else(|| Error::failed("unverified caller"))?;
        if args != serde_json::json!([]) {
            return Err(Error::failed("expected no arguments"));
        }
        match member.as_str() {
            "Inspect" => Ok(serde_json::json!(sender)),
            "Configuration" => Ok(serde_json::json!(&self.3)),
            "TrustedContext" => {
                let previous = self.4.lock().await.clone();
                let previous_authorized = self
                    .2
                    .as_ref()
                    .zip(previous.as_ref())
                    .is_some_and(|(trusted_host, context)| trusted_host.authorizes(context));
                let authorized = self
                    .2
                    .as_ref()
                    .is_some_and(|trusted_host| trusted_host.authorizes(context));
                if authorized {
                    *self.4.lock().await = Some(context.clone());
                }
                Ok(serde_json::json!({
                    "authorized": authorized,
                    "previousAuthorized": previous_authorized,
                }))
            }
            "Callback" | "ConfidentialCallback" => {
                let proxy = self
                    .0
                    .proxy(sender.as_str(), "/callback", "org.example.Callback")?;
                let reply: String = if member.as_str() == "ConfidentialCallback" {
                    proxy.call_confidential("Observe", ()).await?
                } else {
                    proxy.call("Observe", ()).await?
                };
                Ok(serde_json::json!([sender, reply]))
            }
            "WaitForWatchChanged" => {
                let message = self
                    .1
                    .lock()
                    .await
                    .recv()
                    .await
                    .map_err(|_| Error::failed("directed signal receiver closed"))?;
                Ok(message.body)
            }
            _ => Err(Error::failed("unknown method")),
        }
    }
}

async fn setup(context: ModuleSetupContext, config: Config) -> Result<()> {
    let connection = context.connection().clone();
    let trusted_host = context.trusted_host().cloned();
    let signals = connection
        .proxy("org.example.Context", "/context", "org.example.Context")?
        .receive_directed_signal("WatchChanged")
        .await?;
    connection
        .serve_at(
            "/context".parse()?,
            ContextService(
                connection.clone(),
                std::sync::Arc::new(tokio::sync::Mutex::new(signals)),
                trusted_host,
                config.marker,
                std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            ),
        )
        .await?;
    connection.request_name("org.example.Context").await
}

tinybus_module::module_export! {
    setup_with_context = setup,
    config = Config,
    worker_threads = 1,
    provides = ["org.example.Context"],
    methods = ["Inspect", "Callback", "ConfidentialCallback", "WaitForWatchChanged"],
    signals = ["WatchChanged"], requires = [], optional = [], lazy = true,
}
