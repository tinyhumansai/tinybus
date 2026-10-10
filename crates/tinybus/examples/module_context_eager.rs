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
    @plain export = {#[unsafe(no_mangle)]},
    start = eager_start,
    setup = setup,
    worker_threads = 1,
    provides = ["org.example.Context"],
    methods = ["Inspect", "Callback"],
    signals = [], requires = [], optional = [], lazy = false,
}

// The initializer cannot return (and therefore cannot be admitted) until the
// independent SDK worker has sent Hello. This forces the eager race without
// assuming which thread the scheduler runs first.
static HELLO: (std::sync::Mutex<bool>, std::sync::Condvar) =
    (std::sync::Mutex::new(false), std::sync::Condvar::new());
static SEND: std::sync::OnceLock<
    unsafe extern "C" fn(*mut std::ffi::c_void, *const u8, usize) -> i32,
> = std::sync::OnceLock::new();

unsafe extern "C" fn observe_send(ctx: *mut std::ffi::c_void, ptr: *const u8, len: usize) -> i32 {
    let code = unsafe { SEND.get().unwrap()(ctx, ptr, len) };
    *HELLO.0.lock().unwrap() = true;
    HELLO.1.notify_one();
    code
}

unsafe fn eager_start<F, Fut>(
    host: *const tinybus::module::abi::TbHostVtable,
    out: *mut tinybus::module::abi::TbModuleVtable,
    workers: usize,
    detach: bool,
    setup: F,
) -> i32
where
    F: FnOnce(Connection) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut table = unsafe { tinybus::module::abi::TbHostVtable::read_compatible(host) }.unwrap();
    // Eager activation has not attached its private transport to the broker.
    assert_eq!(unsafe { table.broker_routing.unwrap()(table.host_ctx) }, 0);
    SEND.set(table.send).unwrap();
    table.send = observe_send;
    let code = unsafe { tinybus_module::start_module(&table, out, workers, detach, setup) };
    if code == tinybus::module::abi::TB_OK {
        let hello = HELLO.0.lock().unwrap();
        let (hello, timeout) = HELLO
            .1
            .wait_timeout_while(hello, std::time::Duration::from_secs(10), |sent| !*sent)
            .unwrap();
        assert!(
            *hello && !timeout.timed_out(),
            "SDK must send Hello before admission"
        );
    }
    code
}
