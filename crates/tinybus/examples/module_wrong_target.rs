//! Loadable cdylib with a deliberately incompatible target descriptor.

const fn wrong_descriptor() -> tinybus::module::abi::TbAbiDescriptor {
    let mut descriptor =
        tinybus::module::abi::TbAbiDescriptor::current("wrong-target", env!("CARGO_PKG_VERSION"));
    descriptor.target_triple[0] = b'!';
    descriptor
}

#[unsafe(no_mangle)]
/// Deliberately incompatible descriptor used to verify pre-init rejection.
pub static TINYBUS_MODULE_ABI_V1: tinybus::module::abi::TbAbiDescriptor = wrong_descriptor();

#[unsafe(no_mangle)]
/// Return the fixture's v1 manifest as process-lifetime bytes.
pub extern "C" fn tinybus_module_manifest_v1() -> tinybus::module::abi::TbSlice {
    tinybus_module::manifest_slice(tinybus_module::ManifestDeclaration {
        name: "wrong-target",
        version: env!("CARGO_PKG_VERSION"),
        provides: &["ai.tinyhumans.openhuman.WrongTarget"],
        methods: &[],
        signals: &[],
        requires: &[],
        optional: &[],
        lazy: false,
        worker_threads: 1,
    })
}

#[unsafe(no_mangle)]
/// Entry point that the gate must refuse to call.
///
/// # Safety
///
/// This function is an ABI fixture and accepts only valid v1 pointers, though
/// its deliberately incompatible descriptor means a correct host never calls
/// it.
pub unsafe extern "C" fn tinybus_module_init_v1(
    _: *const tinybus::module::abi::TbHostVtable,
    _: *mut tinybus::module::abi::TbModuleVtable,
) -> i32 {
    tinybus::module::abi::TB_CLOSED
}
