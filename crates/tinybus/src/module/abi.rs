//! Stable, C-shaped module ABI. No Rust-layout value crosses this boundary.

use std::ffi::c_void;

/// Current ABI revision. It is also part of every exported symbol name.
pub const ABI_REVISION: u32 = 1;
/// `TBUSMOD` followed by the ABI revision byte.
pub const ABI_MAGIC: u64 = u64::from_le_bytes(*b"TBUSMOD\x01");
/// Frozen descriptor prefix size.
pub const DESCRIPTOR_PREFIX_SIZE: u32 = 16;
/// Defensive maximum before reading descriptor bytes controlled by a module.
pub const MAX_DESCRIPTOR_SIZE: u32 = 4096;

/// The module was accepted and the operation completed.
pub const TB_OK: i32 = 0;
/// The receiving queue has no room right now.
pub const TB_BACKPRESSURE: i32 = -1;
/// The other side has closed.
pub const TB_CLOSED: i32 = -2;
/// Module code panicked at the FFI boundary.
pub const TB_PANICKED: i32 = -3;
/// Shutdown did not finish before its deadline.
pub const TB_TIMEOUT: i32 = -4;
/// A pointer, length, or frame was invalid.
pub const TB_BAD_ARGUMENT: i32 = -5;

/// A borrowed byte slice returned across the ABI.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TbSlice {
    /// First byte, valid for `len` bytes and only for the duration documented
    /// by the function returning this value.
    pub ptr: *const u8,
    /// Number of readable bytes.
    pub len: usize,
}

/// Facts that must match before module initialization is allowed.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TbAbiDescriptor {
    /// [`ABI_MAGIC`].
    pub magic: u64,
    /// [`ABI_REVISION`].
    pub abi_revision: u32,
    /// Size of the full descriptor supplied by the module.
    pub descriptor_size: u32,
    /// Bit 0: unwind panics, bit 1: debug assertions, bit 2: little endian.
    pub flags: u32,
    /// Target pointer width in bits.
    pub pointer_width: u32,
    /// tinybus major version.
    pub tinybus_major: u32,
    /// tinybus minor version.
    pub tinybus_minor: u32,
    /// tinybus patch version.
    pub tinybus_patch: u32,
    /// Additive tinybus Cargo feature bits.
    pub tinybus_feature_bits: u64,
    /// NUL-padded target triple.
    pub target_triple: [u8; 64],
    /// NUL-padded rustc release.
    pub rustc_version: [u8; 48],
    /// NUL-padded module package name.
    pub module_name: [u8; 64],
    /// NUL-padded module package version.
    pub module_version: [u8; 32],
}

impl TbAbiDescriptor {
    /// Construct the descriptor for the current build and module identity.
    pub const fn current(module_name: &str, module_version: &str) -> Self {
        let version = crate_version();
        Self {
            magic: ABI_MAGIC,
            abi_revision: ABI_REVISION,
            descriptor_size: size_of::<Self>() as u32,
            flags: (if cfg!(panic = "unwind") { 1 } else { 0 })
                | (if cfg!(debug_assertions) { 1 << 1 } else { 0 })
                | (if cfg!(target_endian = "little") { 1 << 2 } else { 0 }),
            pointer_width: usize::BITS,
            tinybus_major: version[0],
            tinybus_minor: version[1],
            tinybus_patch: version[2],
            tinybus_feature_bits: crate::build_info::FEATURE_BITS,
            target_triple: fixed(crate::build_info::TARGET),
            rustc_version: fixed(crate::build_info::RUSTC_VERSION),
            module_name: fixed(module_name),
            module_version: fixed(module_version),
        }
    }
}

/// Calls from a module into its host.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TbHostVtable {
    /// Size of this vtable, allowing additive growth.
    pub size: u32,
    /// Reserved and zero in v1.
    pub _reserved: u32,
    /// Opaque host context, never dereferenced by the module.
    pub host_ctx: *mut c_void,
    /// Send one complete JSON message frame to the host.
    pub send: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32,
    /// Tell the host the module's inbound queue has room again.
    pub wake: unsafe extern "C" fn(*mut c_void),
    /// Forward one log record to the host.
    pub log: unsafe extern "C" fn(*mut c_void, u32, *const u8, usize),
    /// Mark the module failed and detach it.
    pub fault: unsafe extern "C" fn(*mut c_void, *const u8, usize),
}

/// Calls from a host into one initialized module.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TbModuleVtable {
    /// Size of this vtable, allowing additive growth.
    pub size: u32,
    /// Reserved and zero in v1.
    pub _reserved: u32,
    /// Opaque module context, never dereferenced by the host.
    pub module_ctx: *mut c_void,
    /// Deliver one complete JSON message frame without blocking.
    pub deliver: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32,
    /// Stop module tasks, bounded by `deadline_ms`.
    pub shutdown: unsafe extern "C" fn(*mut c_void, u64) -> i32,
}

impl Default for TbModuleVtable {
    fn default() -> Self {
        Self {
            size: size_of::<Self>() as u32,
            _reserved: 0,
            module_ctx: std::ptr::null_mut(),
            deliver: invalid_deliver,
            shutdown: invalid_shutdown,
        }
    }
}

unsafe extern "C" fn invalid_deliver(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_CLOSED
}

unsafe extern "C" fn invalid_shutdown(_: *mut c_void, _: u64) -> i32 {
    TB_CLOSED
}

const fn fixed<const N: usize>(value: &str) -> [u8; N] {
    let bytes = value.as_bytes();
    let mut out = [0; N];
    let mut index = 0;
    while index < bytes.len() && index < N {
        out[index] = bytes[index];
        index += 1;
    }
    out
}

const fn crate_version() -> [u32; 3] {
    [
        parse_component(env!("CARGO_PKG_VERSION_MAJOR")),
        parse_component(env!("CARGO_PKG_VERSION_MINOR")),
        parse_component(env!("CARGO_PKG_VERSION_PATCH")),
    ]
}

const fn parse_component(value: &str) -> u32 {
    let bytes = value.as_bytes();
    let mut out = 0u32;
    let mut index = 0;
    while index < bytes.len() {
        out = out * 10 + (bytes[index] - b'0') as u32;
        index += 1;
    }
    out
}

/// Read a NUL-padded field without trusting it as UTF-8.
pub fn field_bytes<const N: usize>(field: &[u8; N]) -> &[u8] {
    let end = field.iter().position(|byte| *byte == 0).unwrap_or(N);
    &field[..end]
}
