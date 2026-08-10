//! Second cdylib proving identical exported symbol names stay handle-local.

use tinybus::{Connection, Result};

struct SecondClock;

#[tinybus::interface(name = "ai.tinyhumans.openhuman.SecondClock")]
impl SecondClock {
    async fn identify(&self) -> Result<String> {
        Ok("second-clock".to_string())
    }
}

async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at(
            "/ai/tinyhumans/openhuman/SecondClock".try_into()?,
            SecondClock,
        )
        .await?;
    connection
        .request_name("ai.tinyhumans.openhuman.SecondClock")
        .await?;
    Ok(())
}

#[unsafe(no_mangle)]
/// ABI descriptor inspected by the host before loading this fixture.
pub static TINYBUS_MODULE_ABI_V1: tinybus::module::abi::TbAbiDescriptor =
    tinybus::module::abi::TbAbiDescriptor::current("module-clock-two", env!("CARGO_PKG_VERSION"));

#[unsafe(no_mangle)]
/// Return the fixture's v1 manifest as process-lifetime bytes.
pub extern "C" fn tinybus_module_manifest_v1() -> tinybus::module::abi::TbSlice {
    tinybus_module::manifest_slice(tinybus_module::ManifestDeclaration {
        name: "module-clock-two",
        version: env!("CARGO_PKG_VERSION"),
        provides: &["ai.tinyhumans.openhuman.SecondClock"],
        methods: &["Identify"],
        signals: &[],
        requires: &[],
        optional: &[],
        lazy: false,
        worker_threads: 1,
    })
}

#[unsafe(no_mangle)]
/// Initialize through the v1 ABI after the host has admitted the descriptor.
///
/// # Safety
///
/// The host must supply valid v1 vtable pointers for the process lifetime.
pub unsafe extern "C" fn tinybus_module_init_v1(
    host: *const tinybus::module::abi::TbHostVtable,
    out: *mut tinybus::module::abi::TbModuleVtable,
) -> i32 {
    unsafe { tinybus_module::start_module(host, out, 1, true, setup) }
}
