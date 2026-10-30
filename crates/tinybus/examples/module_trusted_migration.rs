//! Compiled migration fixture requiring TinyBus's loader-bound host capability.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Error, Interface, InterfaceName, MemberName, Result};
use tinybus_module::{ModuleSetupContext, TrustedHost};

struct MigrationService {
    trusted_host: TrustedHost,
    schema_read: Arc<AtomicBool>,
    transformed: Arc<AtomicBool>,
    lease_registered: Arc<AtomicBool>,
}

#[async_trait]
impl Interface for MigrationService {
    fn name(&self) -> InterfaceName {
        "org.example.TrustedMigration".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![
            "Schema".parse().unwrap(),
            "Transform".parse().unwrap(),
            "RegisterOrdinaryLease".parse().unwrap(),
        ]
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("authenticated migration host required"))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        if !self.trusted_host.authorizes(context) {
            return Err(Error::failed("trusted migration host required"));
        }
        match member.as_str() {
            "Schema" if args == serde_json::json!([]) => {
                self.schema_read.store(true, Ordering::Release);
                Ok(serde_json::json!({
                    "type": "object",
                    "required": ["id"],
                    "properties": {"id": {"type": "string"}}
                }))
            }
            "Transform" => {
                let Some(id) = args
                    .as_array()
                    .and_then(|values| values.first())
                    .and_then(Value::as_str)
                else {
                    return Err(Error::failed("migration transform input is invalid"));
                };
                if !self.schema_read.load(Ordering::Acquire)
                    || self.lease_registered.load(Ordering::Acquire)
                {
                    return Err(Error::failed("migration ordering is invalid"));
                }
                self.transformed.store(true, Ordering::Release);
                Ok(serde_json::json!({
                    "id": format!("record-{id}"),
                    "ordinary_lease_registered": false
                }))
            }
            "RegisterOrdinaryLease" if args == serde_json::json!([]) => {
                if !self.transformed.load(Ordering::Acquire) {
                    return Err(Error::failed(
                        "migration must finish before lease registration",
                    ));
                }
                self.lease_registered.store(true, Ordering::Release);
                Ok(serde_json::json!(true))
            }
            _ => Err(Error::failed("unknown migration method")),
        }
    }
}

async fn setup(context: ModuleSetupContext) -> Result<()> {
    let trusted_host = context
        .trusted_host()
        .cloned()
        .ok_or_else(|| Error::failed("trusted migration host binding unavailable"))?;
    let connection = context.connection().clone();
    connection
        .serve_at(
            "/migration".parse()?,
            MigrationService {
                trusted_host,
                schema_read: Arc::new(AtomicBool::new(false)),
                transformed: Arc::new(AtomicBool::new(false)),
                lease_registered: Arc::new(AtomicBool::new(false)),
            },
        )
        .await?;
    connection
        .request_name("org.example.TrustedMigration")
        .await
}

tinybus_module::module_export! {
    setup_with_context = setup,
    worker_threads = 1,
    provides = ["org.example.TrustedMigration"],
    methods = ["Schema", "Transform", "RegisterOrdinaryLease"],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}
