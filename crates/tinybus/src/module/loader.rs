//! Platform loader: resolve the three v1 symbols and deliberately never unload.

use std::path::Path;

use crate::error::{Error, Result};
use crate::module::abi::{
    ABI_MAGIC, ABI_REVISION, DESCRIPTOR_PREFIX_SIZE, MAX_DESCRIPTOR_SIZE, TbAbiDescriptor,
    TbHostVtable, TbModuleVtable, TbSlice,
};
use crate::module::manifest::ModuleManifest;

pub(crate) type InitFn = unsafe extern "C" fn(*const TbHostVtable, *mut TbModuleVtable) -> i32;
type ManifestFn = unsafe extern "C" fn() -> TbSlice;

#[derive(Clone)]
pub(crate) struct LoadedArtifact {
    pub(crate) descriptor: TbAbiDescriptor,
    pub(crate) manifest: ModuleManifest,
    pub(crate) init: InitFn,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DescriptorPrefix {
    magic: u64,
    abi_revision: u32,
    descriptor_size: u32,
}

pub(crate) fn load(path: &Path) -> Result<LoadedArtifact> {
    let handle = platform::open(path)?;
    let descriptor_ptr = platform::symbol(handle, b"TINYBUS_MODULE_ABI_V1\0", path)?;

    let prefix = unsafe { descriptor_ptr.cast::<DescriptorPrefix>().read_unaligned() };
    if prefix.magic != ABI_MAGIC {
        return Err(Error::module_refused(path, "ABI magic does not match"));
    }
    if prefix.abi_revision != ABI_REVISION {
        return Err(Error::module_refused(path, "ABI revision does not match"));
    }
    if !(DESCRIPTOR_PREFIX_SIZE..=MAX_DESCRIPTOR_SIZE).contains(&prefix.descriptor_size) {
        return Err(Error::module_refused(path, "descriptor size is invalid"));
    }
    if prefix.descriptor_size < size_of::<TbAbiDescriptor>() as u32 {
        return Err(Error::module_refused(path, "descriptor is too small"));
    }
    let descriptor = unsafe { descriptor_ptr.cast::<TbAbiDescriptor>().read_unaligned() };

    let manifest_fn: ManifestFn = unsafe {
        std::mem::transmute(platform::symbol(
            handle,
            b"tinybus_module_manifest_v1\0",
            path,
        )?)
    };
    let slice = unsafe { manifest_fn() };
    if slice.ptr.is_null() || slice.len > 1024 * 1024 {
        return Err(Error::module_refused(path, "manifest bytes are invalid"));
    }
    let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
    let manifest = serde_json::from_slice(bytes)
        .map_err(|_| Error::module_refused(path, "manifest is not valid JSON"))?;

    let init: InitFn = unsafe {
        std::mem::transmute(platform::symbol(
            handle,
            b"tinybus_module_init_v1\0",
            path,
        )?)
    };
    Ok(LoadedArtifact {
        descriptor,
        manifest,
        init,
    })
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
            return Err(Error::module_refused(path, "dynamic loader rejected the artifact"));
        }
        // No Drop wrapper on purpose. Calling dlclose would invalidate code,
        // TLS, panic metadata, and callbacks that may still be reachable.
        Ok(handle)
    }

    pub(super) fn symbol(handle: Handle, name: &[u8], path: &Path) -> Result<*mut c_void> {
        let pointer = unsafe { dlsym(handle, name.as_ptr().cast()) };
        if pointer.is_null() {
            log_last_error();
            return Err(Error::module_refused(path, "required ABI symbol is missing"));
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
    pub(super) type Handle = *mut c_void;

    pub(super) fn open(path: &Path) -> Result<Handle> {
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
        let handle = unsafe {
            LoadLibraryExW(
                wide.as_ptr(),
                std::ptr::null_mut(),
                LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR,
            )
        };
        if handle.is_null() {
            tracing::debug!(loader_error = unsafe { GetLastError() }, "module loader detail");
            return Err(Error::module_refused(path, "dynamic loader rejected the artifact"));
        }
        Ok(handle)
    }

    pub(super) fn symbol(handle: Handle, name: &[u8], path: &Path) -> Result<*mut c_void> {
        let pointer = unsafe { GetProcAddress(handle, name.as_ptr().cast()) };
        if pointer.is_null() {
            tracing::debug!(loader_error = unsafe { GetLastError() }, "module loader detail");
            return Err(Error::module_refused(path, "required ABI symbol is missing"));
        }
        Ok(pointer)
    }
}
