//! Compiled fixture for configured setup factories that need loader context.
#![deny(missing_docs)]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tinybus::{CallContext, Error, Interface, InterfaceName, MemberName, Result};
use tinybus_module::{ModuleSetupContext, TrustedHost};

static FACTORY_IDS: AtomicUsize = AtomicUsize::new(0);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    restricted: bool,
}

struct Service {
    restricted: bool,
    generation: usize,
    factory_id: usize,
    trusted_host: TrustedHost,
}

#[async_trait]
impl Interface for Service {
    fn name(&self) -> InterfaceName {
        "org.example.ContextFactory".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec!["Inspect".parse().unwrap()]
    }

    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("context-aware dispatch required"))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        if !self.trusted_host.authorizes(context) {
            return Err(Error::failed("factory context authority unavailable"));
        }
        if member.as_str() != "Inspect" {
            return Err(Error::failed("unknown method"));
        }
        if args != serde_json::json!([]) {
            return Err(Error::failed("expected no arguments"));
        }
        Ok(serde_json::json!({
            "restricted": self.restricted,
            "generation": self.generation,
            "factory_id": self.factory_id,
        }))
    }
}

type SetupFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

fn setup_factory() -> impl Fn(ModuleSetupContext, Config) -> SetupFuture + Send + Sync + 'static {
    let factory_id = FACTORY_IDS.fetch_add(1, Ordering::SeqCst) + 1;
    let restricted = Arc::new(tokio::sync::Mutex::new(None));
    let generation = Arc::new(AtomicUsize::new(0));
    move |context, config: Config| {
        let restricted = Arc::clone(&restricted);
        let generation = Arc::clone(&generation);
        Box::pin(async move {
            let mut admitted = restricted.lock().await;
            if admitted.is_some_and(|initial| initial != config.restricted) {
                return Err(Error::failed("immutable mode"));
            }
            *admitted = Some(config.restricted);
            drop(admitted);
            let trusted_host = context
                .trusted_host()
                .cloned()
                .ok_or_else(|| Error::failed("trusted host unavailable"))?;
            let connection = context.connection();
            connection
                .serve_at(
                    "/context_factory".parse()?,
                    Service {
                        restricted: config.restricted,
                        generation: generation.fetch_add(1, Ordering::SeqCst) + 1,
                        factory_id,
                        trusted_host,
                    },
                )
                .await?;
            connection.request_name("org.example.ContextFactory").await
        })
    }
}

tinybus_module::module_export! {
    setup_factory_with_context = setup_factory,
    config = Config,
    worker_threads = 1,
    provides = ["org.example.ContextFactory"],
    methods = ["Inspect"],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}
