//! Stable, C-shaped module ABI. No Rust-layout value crosses this boundary.
//!
//! ABI revision one grows through optional vtable tails. Read the frozen prefix
//! first and check its declared size before reading a tail; an older host has no
//! broker-routing callback and therefore supplies no authenticated call context.
//! Each optional field has its own complete-field size check.

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
/// A caller-owned output buffer is too small; `out_len` carries the required size.
pub const TB_BUFFER_TOO_SMALL: i32 = -6;
/// The optional host capability is unavailable or has been revoked.
pub const TB_UNSUPPORTED: i32 = -7;

/// Exactly this value permits native per-instance admission. Zero, unknown
/// bits, or any combination with terminal bits must deny admission. A sampled
/// value is not a reusable permit for a later operation.
pub const TB_NATIVE_TERMINAL_LIVE: u32 = 1;
/// Terminal stop intent; once committed, the instance can never become live.
pub const TB_NATIVE_TERMINAL_STOP_INTENT: u32 = 2;
/// Actual fault intent, with public fault state taking precedence over stop.
pub const TB_NATIVE_TERMINAL_FAULT: u32 = 4;
/// An accepted opaque shutdown callback actually returned TB_OK. Fault and
/// stop intent remain set and continue to deny admission even with this bit.
/// A legacy callback's TB_OK alone proves neither independent worker completion
/// nor receiver backing destruction. For native terminal hooks, TB_OK must
/// additionally require actual worker/resource finish without sticky uncertainty
/// or fault; that stronger module contract is separate from this host evidence.
pub const TB_NATIVE_TERMINAL_PHYSICALLY_STOPPED: u32 = 8;

/// Revision-one module initialization entrypoint.
pub type TbModuleInit = unsafe extern "C" fn(*const TbHostVtable, *mut TbModuleVtable) -> i32;

/// Reconfigure one initialized module from a borrowed JSON document.
pub type TbModuleReinitialize = unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32;

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
                | (if cfg!(target_endian = "little") {
                    1 << 2
                } else {
                    0
                }),
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

/// Frozen revision-one host vtable prefix, accepted from older hosts.
///
/// Its layout ends before [`TbHostVtable::broker_routing`]. Older modules can
/// read this prefix from a newer host without interpreting the additive tail.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TbHostVtableV1Prefix {
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
    /// Borrowed JSON configuration. The module must copy it during init and
    /// must not retain this pointer.
    pub config: TbSlice,
    /// Module setup completed and its declared bus surface is ready.
    pub ready: unsafe extern "C" fn(*mut c_void),
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
    /// Borrowed JSON configuration. The module must copy it during init and
    /// must not retain this pointer.
    pub config: TbSlice,
    /// Module setup completed and its declared bus surface is ready.
    pub ready: unsafe extern "C" fn(*mut c_void),
    /// Optional ABI-v1 routing assertion, read only when `size` covers the tail.
    ///
    /// The SDK samples this callback when each frame enters its inbound queue.
    /// Exactly 1 asserts exclusive delivery from the actual broker pipeline,
    /// which overwrites sender on ingress; other values are unverified. Later
    /// admission does not authenticate a frame already queued without proof.
    /// Older hosts omit this field and therefore supply unverified context.
    /// The SDK trusts this callback under the unsafe module-init host contract;
    /// it is not authentication of arbitrary user-provided C vtables.
    pub broker_routing: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    /// Optional live query for this native module instance, read only when
    /// `size` covers this entire field. Absence means unsupported and must deny
    /// native admission. Only exactly [`TB_NATIVE_TERMINAL_LIVE`] permits it;
    /// zero, unknown bits, combined values and terminal flags deny. Terminal
    /// intent is monotonic and must never return to Live.
    ///
    /// Modules copy this callback and `host_ctx`, never retain the borrowed
    /// vtable or config storage. The host must keep the copied callback and
    /// context valid and thread-safe for every retained view use, including
    /// independent workers after runtime shutdown and host destruction. A
    /// destroyed host leaves a terminal tombstone, never recycles or reopens
    /// this context. Custom unsafe hosts must truthfully uphold this contract;
    /// the SDK cannot authenticate an arbitrary in-process C host.
    ///
    /// Query success linearizes admission only; it neither grants publication
    /// authority nor cancels an operation already admitted before later intent.
    /// The SDK's existing process-lifetime context retention is unbounded;
    /// this optional callback adds no aggregate resource bound.
    pub native_terminal_state: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    /// Optional query for the controller peer pinned when this module transport
    /// was created. Read only when `size` covers the complete field. The host
    /// writes the peer's bytes to caller-owned storage and never retains either
    /// output pointer. Absence means no trusted migration host is available.
    /// The query is valid during admitted setup before `ready`, and must return
    /// `TB_CLOSED` after the module or pinned controller becomes inactive.
    pub trusted_host_binding:
        Option<unsafe extern "C" fn(*mut c_void, *mut u8, usize, *mut usize) -> i32>,
}

/// Size required from an older host before reading its frozen prefix.
pub const TB_HOST_VTABLE_BASE_SIZE: u32 = size_of::<TbHostVtableV1Prefix>() as u32;

impl TbHostVtable {
    /// Copy the available host prefix and normalize an absent additive tail.
    ///
    /// Returns `None` for a null pointer or a size below
    /// [`TB_HOST_VTABLE_BASE_SIZE`]. A prefix-only or incomplete-tail host is
    /// accepted. Each absent or incomplete optional callback normalizes to
    /// `None`, independently of any later tail field.
    ///
    /// # Safety
    /// A non-null `host` must be aligned and readable for its first size field
    /// and every byte that size declares. Its callbacks must be valid and
    /// thread-safe, and their context must outlive the module runtime. If
    /// `native_terminal_state` is supplied, its copied callback and context
    /// must remain valid for every retained view use, even after runtime
    /// shutdown or host destruction, as documented on that field. An
    /// affirmative routing callback (returning exactly 1) must truthfully
    /// guarantee exclusive broker delivery at the time each frame is queued.
    #[doc(hidden)]
    pub unsafe fn read_compatible(host: *const Self) -> Option<Self> {
        if host.is_null() || unsafe { host.cast::<u32>().read() } < TB_HOST_VTABLE_BASE_SIZE {
            return None;
        }
        let base = unsafe { host.cast::<TbHostVtableV1Prefix>().read() };
        let query_size = size_of::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>();
        let broker_routing =
            if base.size as usize >= std::mem::offset_of!(Self, broker_routing) + query_size {
                unsafe { std::ptr::addr_of!((*host).broker_routing).read() }
            } else {
                None
            };
        let native_terminal_state = if base.size as usize
            >= std::mem::offset_of!(Self, native_terminal_state) + query_size
        {
            unsafe { std::ptr::addr_of!((*host).native_terminal_state).read() }
        } else {
            None
        };
        let trusted_host_binding = if base.size as usize
            >= std::mem::offset_of!(Self, trusted_host_binding)
                + size_of::<
                    Option<unsafe extern "C" fn(*mut c_void, *mut u8, usize, *mut usize) -> i32>,
                >() {
            unsafe { std::ptr::addr_of!((*host).trusted_host_binding).read() }
        } else {
            None
        };
        Some(Self {
            size: base.size,
            _reserved: base._reserved,
            host_ctx: base.host_ctx,
            send: base.send,
            wake: base.wake,
            log: base.log,
            fault: base.fault,
            config: base.config,
            ready: base.ready,
            broker_routing,
            native_terminal_state,
            trusted_host_binding,
        })
    }
}

/// Calls from a host into one initialized module.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TbModuleVtableV1Prefix {
    /// Bytes available in the host-provided output allocation.
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

/// Calls from a host into one initialized module.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TbModuleVtable {
    /// Bytes available in the host-provided output allocation on entry; size
    /// written by the module on success. This handshake permits additive tail
    /// fields without overrunning an older host's allocation.
    pub size: u32,
    /// Reserved and zero in v1.
    pub _reserved: u32,
    /// Opaque module context, never dereferenced by the host.
    pub module_ctx: *mut c_void,
    /// Deliver one complete JSON message frame without blocking.
    pub deliver: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32,
    /// Stop module tasks, bounded by `deadline_ms`.
    pub shutdown: unsafe extern "C" fn(*mut c_void, u64) -> i32,
    /// Apply replacement JSON configuration without replacing the runtime.
    /// Hosts must check `size` before reading this additive tail field.
    pub reinitialize: Option<TbModuleReinitialize>,
}

/// Size of the original ABI-v1 module vtable, before reinitialization was
/// added as an optional tail callback.
pub const TB_MODULE_VTABLE_BASE_SIZE: u32 = size_of::<TbModuleVtableV1Prefix>() as u32;

impl Default for TbModuleVtable {
    fn default() -> Self {
        Self {
            size: size_of::<Self>() as u32,
            _reserved: 0,
            module_ctx: std::ptr::null_mut(),
            deliver: invalid_deliver,
            shutdown: invalid_shutdown,
            reinitialize: None,
        }
    }
}

unsafe extern "C" fn invalid_deliver(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_CLOSED
}

unsafe extern "C" fn invalid_shutdown(_: *mut c_void, _: u64) -> i32 {
    TB_CLOSED
}

// Oversized values are deliberately truncated. Host comparisons use the full
// expected value, so truncation fails closed at admission instead of matching
// a different target or toolchain accidentally.
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

#[cfg(test)]
#[path = "abi_tests.rs"]
mod tests;
