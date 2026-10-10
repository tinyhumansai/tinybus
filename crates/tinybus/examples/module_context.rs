//! Native fixture exercising contextual dispatch and a callback on its own runtime.

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Connection, Error, Interface, InterfaceName, MemberName, Result};

struct ContextService(Connection);

#[async_trait]
impl Interface for ContextService {
    fn name(&self) -> InterfaceName {
        "org.example.Context".parse().unwrap()
    }
    fn members(&self) -> Vec<MemberName> {
        vec!["Inspect".parse().unwrap(), "Callback".parse().unwrap()]
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
            "Callback" => {
                let reply: String = self
                    .0
                    .proxy(sender.as_str(), "/callback", "org.example.Callback")?
                    .call("Observe", ())
                    .await?;
                Ok(serde_json::json!([sender, reply]))
            }
            _ => Err(Error::failed("unknown method")),
        }
    }
}

async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at("/context".parse()?, ContextService(connection.clone()))
        .await?;
    connection.request_name("org.example.Context").await
}

tinybus_module::module_export! {
    setup = setup,
    worker_threads = 1,
    provides = ["org.example.Context"],
    methods = ["Inspect", "Callback"],
    signals = [], requires = [], optional = [], lazy = true,
}
