//! Platform loader: resolve the three v1 symbols and deliberately never unload.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use crate::error::{Error, Result};
use crate::module::abi::{
    ABI_MAGIC, ABI_REVISION, DESCRIPTOR_PREFIX_SIZE, MAX_DESCRIPTOR_SIZE, TbAbiDescriptor,
    TbModuleInit, TbSlice, field_bytes,
};
use crate::module::manifest::ModuleManifest;
use crate::{build_info, version::Version};

type ManifestFn = unsafe extern "C" fn() -> TbSlice;

/// Largest manifest a module may hand the loader.
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct LoadedArtifact {
    pub(crate) descriptor: TbAbiDescriptor,
    pub(crate) manifest: ModuleManifest,
    pub(crate) init: TbModuleInit,
    pub(crate) library_sha256: Option<String>,
}

#[derive(Clone)]
struct MappedImage {
    file_sha256: String,
    trusted_library_sha256: Option<String>,
}

fn mapped_images() -> &'static Mutex<HashMap<usize, MappedImage>> {
    static IMAGES: OnceLock<Mutex<HashMap<usize, MappedImage>>> = OnceLock::new();
    IMAGES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn record_mapped_image(
    images: &mut HashMap<usize, MappedImage>,
    handle: usize,
    file_sha256: String,
    expected_library_sha256: Option<&str>,
) -> std::result::Result<Option<String>, &'static str> {
    if let Some(mapped) = images.get(&handle) {
        if mapped.file_sha256 != file_sha256 {
            return Err("mapped library bytes differ from the original image");
        }
        if expected_library_sha256
            .is_some_and(|expected| mapped.trusted_library_sha256.as_deref() != Some(expected))
        {
            return Err("mapped image has no matching library digest proof");
        }
        return Ok(expected_library_sha256.map(str::to_string));
    }
    let trusted_library_sha256 = expected_library_sha256.map(str::to_string);
    images.insert(
        handle,
        MappedImage {
            file_sha256,
            trusted_library_sha256: trusted_library_sha256.clone(),
        },
    );
    Ok(trusted_library_sha256)
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DescriptorPrefix {
    magic: u64,
    abi_revision: u32,
    descriptor_size: u32,
}

#[allow(dead_code)] // Legacy unverified load remains available to internal fixtures.
pub(crate) fn load(path: &Path, strict: bool) -> Result<LoadedArtifact> {
    load_verified(path, strict, None)
}

/// Load one library, binding an optional expected digest to the first native
/// image TinyBus maps for that handle. Shared library handles are never closed,
/// so a later file at the same path cannot inherit proof for an older image.
pub(crate) fn load_verified(
    path: &Path,
    strict: bool,
    expected_library_sha256: Option<&str>,
) -> Result<LoadedArtifact> {
    let mut images = mapped_images()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let actual_sha256 = crate::module::hash::file_hex(
        std::fs::File::open(path)
            .map_err(|_| Error::module_refused(path, "dynamic loader rejected the artifact"))?,
    )
    .map_err(|_| Error::module_refused(path, "module library could not be hashed"))?;
    if let Some(expected) = expected_library_sha256 {
        if !crate::attest::is_hex_sha256(expected) {
            return Err(Error::module_refused(path, "library digest pin is invalid"));
        }
        if !actual_sha256.eq_ignore_ascii_case(expected) {
            return Err(Error::module_refused(
                path,
                "module library hash does not match its pin",
            ));
        }
    }
    let handle = platform::open(path)?;
    let library_sha256 = record_mapped_image(
        &mut images,
        handle as usize,
        actual_sha256,
        expected_library_sha256,
    )
    .map_err(|reason| Error::module_refused(path, reason))?;
    drop(images);
    let descriptor_ptr = platform::symbol(handle, b"TINYBUS_MODULE_ABI_V1\0", path)?;

    let prefix = unsafe { descriptor_ptr.cast::<DescriptorPrefix>().read_unaligned() };
    gate_prefix(
        path,
        prefix.magic,
        prefix.abi_revision,
        prefix.descriptor_size,
    )?;
    let descriptor = unsafe { descriptor_ptr.cast::<TbAbiDescriptor>().read_unaligned() };
    gate_descriptor(path, &descriptor, strict)?;

    let manifest_fn: ManifestFn = unsafe {
        std::mem::transmute(platform::symbol(
            handle,
            b"tinybus_module_manifest_v1\0",
            path,
        )?)
    };
    let manifest = unsafe { manifest_from(path, manifest_fn())? };

    let init: TbModuleInit = unsafe {
        std::mem::transmute(platform::symbol(handle, b"tinybus_module_init_v1\0", path)?)
    };
    Ok(LoadedArtifact {
        descriptor,
        manifest,
        init,
        library_sha256,
    })
}

/// The checks an ABI descriptor's prefix must pass before the full descriptor
/// may be read through it, and again on the full descriptor.
fn gate_prefix(path: &Path, magic: u64, abi_revision: u32, descriptor_size: u32) -> Result<()> {
    let refuse = |reason| Err(Error::module_refused(path, reason));
    if magic != ABI_MAGIC {
        return refuse("ABI magic does not match");
    }
    if abi_revision != ABI_REVISION {
        return refuse("ABI revision does not match");
    }
    if !(DESCRIPTOR_PREFIX_SIZE..=MAX_DESCRIPTOR_SIZE).contains(&descriptor_size) {
        return refuse("descriptor size is invalid");
    }
    if descriptor_size < size_of::<TbAbiDescriptor>() as u32 {
        return refuse("descriptor is too small");
    }
    Ok(())
}

/// Parse the manifest `tinybus_module_manifest_v1` returned.
///
/// # Safety
///
/// Unless `slice.ptr` is null or `slice.len` is over the cap, `slice` must be
/// readable for `slice.len` bytes.
unsafe fn manifest_from(path: &Path, slice: TbSlice) -> Result<ModuleManifest> {
    if slice.ptr.is_null() || slice.len > MAX_MANIFEST_BYTES {
        return Err(Error::module_refused(path, "manifest bytes are invalid"));
    }
    let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
    serde_json::from_slice(bytes)
        .map_err(|_| Error::module_refused(path, "manifest is not valid JSON"))
}

pub(crate) fn gate_descriptor(
    path: &Path,
    descriptor: &TbAbiDescriptor,
    strict: bool,
) -> Result<()> {
    let refuse = |reason| Error::module_refused(path, reason);
    gate_prefix(
        path,
        descriptor.magic,
        descriptor.abi_revision,
        descriptor.descriptor_size,
    )?;
    if descriptor.pointer_width != usize::BITS {
        return Err(refuse("pointer width does not match"));
    }
    if (descriptor.flags & (1 << 2) != 0) != cfg!(target_endian = "little") {
        return Err(refuse("target endianness does not match"));
    }
    if field_bytes(&descriptor.target_triple) != build_info::TARGET.as_bytes() {
        return Err(refuse("target triple does not match"));
    }
    if descriptor.flags & 1 == 0 {
        return Err(refuse("module was built with panic abort"));
    }
    let host_version = Version::parse(crate::VERSION).expect("crate version is semver");
    let module_version = Version::new(
        descriptor.tinybus_major.into(),
        descriptor.tinybus_minor.into(),
        descriptor.tinybus_patch.into(),
    );
    if !host_version.compatible_series().accepts(&module_version) {
        return Err(Error::module_refused(
            path,
            format!(
                "tinybus version is incompatible: host {host_version}, module {module_version}"
            ),
        ));
    }
    let missing_features = descriptor.tinybus_feature_bits & !build_info::FEATURE_BITS;
    if missing_features != 0 {
        let bit = 1u64 << missing_features.trailing_zeros();
        return Err(Error::module_refused(
            path,
            format!(
                "module requires unavailable tinybus feature {}",
                build_info::feature_name(bit)
            ),
        ));
    }
    if strict && field_bytes(&descriptor.rustc_version) != build_info::RUSTC_VERSION.as_bytes() {
        return Err(refuse("rustc version does not match in strict mode"));
    }
    Ok(())
}

#[cfg(unix)]
mod platform {
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    use crate::error::{Error, Result};

    #[cfg_attr(all(target_os = "linux", target_env = "gnu"), link(name = "dl"))]
    unsafe extern "C" {
        fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
        fn dlerror() -> *mut c_char;
    }

    const RTLD_NOW: c_int = 2;
    #[cfg(target_os = "macos")]
    const RTLD_LOCAL: c_int = 4;
    #[cfg(not(target_os = "macos"))]
    const RTLD_LOCAL: c_int = 0;

    pub(super) type Handle = *mut c_void;

    pub(super) fn open(path: &Path) -> Result<Handle> {
        let path_bytes = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::module_refused(path, "artifact path is invalid"))?;
        let handle = unsafe { dlopen(path_bytes.as_ptr(), RTLD_NOW | RTLD_LOCAL) };
        if handle.is_null() {
            log_last_error();
            return Err(Error::module_refused(
                path,
                "dynamic loader rejected the artifact",
            ));
        }
        // No Drop wrapper on purpose. Calling dlclose would invalidate code,
        // TLS, panic metadata, and callbacks that may still be reachable.
        Ok(handle)
    }

    pub(super) fn symbol(handle: Handle, name: &[u8], path: &Path) -> Result<*mut c_void> {
        let pointer = unsafe { dlsym(handle, name.as_ptr().cast()) };
        if pointer.is_null() {
            log_last_error();
            return Err(Error::module_refused(
                path,
                "required ABI symbol is missing",
            ));
        }
        Ok(pointer)
    }

    fn log_last_error() {
        let pointer = unsafe { dlerror() };
        if !pointer.is_null() {
            let message = unsafe { std::ffi::CStr::from_ptr(pointer) }.to_string_lossy();
            tracing::debug!(loader_error = %message, "module loader detail");
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::{c_char, c_void};
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use crate::error::{Error, Result};

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LoadLibraryExW(path: *const u16, file: *mut c_void, flags: u32) -> *mut c_void;
        fn GetProcAddress(handle: *mut c_void, name: *const c_char) -> *mut c_void;
        fn GetLastError() -> u32;
    }

    const LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR: u32 = 0x0000_0100;
    const LOAD_LIBRARY_SEARCH_SYSTEM32: u32 = 0x0000_0800;
    pub(super) type Handle = *mut c_void;

    pub(super) fn open(path: &Path) -> Result<Handle> {
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| Error::module_refused(path, "module artifact path is invalid"))?;
        let wide: Vec<u16> = canonical.as_os_str().encode_wide().chain([0]).collect();
        let handle = unsafe {
            LoadLibraryExW(
                wide.as_ptr(),
                std::ptr::null_mut(),
                // Search only beside the admitted module and in System32. The
                // load-dir flag alone excludes System32, where OS and installed
                // VC runtime DLLs live; default directories would also search
                // application-controlled paths for unverified dependencies.
                LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_SYSTEM32,
            )
        };
        if handle.is_null() {
            let code = unsafe { GetLastError() };
            tracing::debug!(loader_error = code, "module loader detail");
            return Err(Error::module_refused(
                path,
                format!("dynamic loader rejected the artifact (Windows error {code})"),
            ));
        }
        Ok(handle)
    }

    pub(super) fn symbol(handle: Handle, name: &[u8], path: &Path) -> Result<*mut c_void> {
        let pointer = unsafe { GetProcAddress(handle, name.as_ptr().cast()) };
        if pointer.is_null() {
            tracing::debug!(
                loader_error = unsafe { GetLastError() },
                "module loader detail"
            );
            return Err(Error::module_refused(
                path,
                "required ABI symbol is missing",
            ));
        }
        Ok(pointer)
    }
}

#[cfg(test)]
#[path = "loader_tests.rs"]
mod tests;
