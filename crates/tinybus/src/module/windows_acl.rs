//! Windows ACL policy for the module release cache. Feature `modules`.
//!
//! The directory gate in `host.rs` refuses a module directory when any principal
//! other than the current user, Administrators, SYSTEM, TrustedInstaller or
//! CREATOR OWNER holds a write ACE. On Unix the cache is created `0700` and an
//! older permissive one is repaired; this module does the same on Windows by
//! giving the cache a protected DACL that grants only the current user and
//! SYSTEM. A cache that inherits a group write ACE (a managed or redirected
//! `LOCALAPPDATA`, for example) is repaired instead of refused for good.
//!
//! The decisions (is this ACE acceptable, may this directory be repaired, what
//! DACL is written) are pure functions that run on every platform. Only the
//! Win32 calls are gated behind `cfg(windows)`. The gate itself is not loosened.

use std::path::Path;

/// Access-mask bits that let a principal change a directory or file: write
/// data, append, write EA and attributes, delete child, delete, WRITE_DAC,
/// WRITE_OWNER, GENERIC_WRITE and GENERIC_ALL.
pub(super) const WRITE_MASK: u32 =
    0x2 | 0x4 | 0x10 | 0x40 | 0x100 | 0x1_0000 | 0x4_0000 | 0x8_0000 | 0x1000_0000 | 0x4000_0000;
/// `ACCESS_ALLOWED_ACE_TYPE`.
pub(super) const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
/// `INHERIT_ONLY_ACE`.
pub(super) const INHERIT_ONLY_ACE: u8 = 0x08;

/// Whether one DACL entry lets an untrusted principal write the object itself.
///
/// `principal_trusted` says whether the ACE's SID is the current user,
/// Administrators, SYSTEM, TrustedInstaller or CREATOR OWNER. Entries that are
/// not allow-ACEs, that only seed children (inherit-only), or that grant no
/// write access never count.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn ace_grants_untrusted_write(
    ace_type: u8,
    ace_flags: u8,
    mask: u32,
    principal_trusted: bool,
) -> bool {
    ace_type == ACCESS_ALLOWED_ACE_TYPE
        && ace_flags & INHERIT_ONLY_ACE == 0
        && mask & WRITE_MASK != 0
        && !principal_trusted
}

/// Whether a directory may be rewritten by the repair pass: it is a real
/// directory (not a link), this user (or the Administrators group, the owner of
/// what an elevated administrator creates) owns it, and the gate would refuse it.
/// Anything else is left for the gate to judge.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn should_repair(is_real_dir: bool, owned_by_current_user: bool, refused: bool) -> bool {
    is_real_dir && owned_by_current_user && refused
}

/// Whether `directory` is inside the part of the tree the repair pass may
/// rewrite: at or below `install_root`, or strictly below `base` (the user's
/// local application data directory, never `base` itself or anything above it).
/// A path with a `..` component is never in scope.
/// Windows paths are case-insensitive, so components compare case-folded.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn in_repair_scope(directory: &Path, install_root: &Path, base: Option<&Path>) -> bool {
    // A `..` could climb out of the prefix the comparison below accepts.
    if directory
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let fold = |path: &Path| -> Vec<String> {
        path.components()
            .map(|component| component.as_os_str().to_string_lossy().to_lowercase())
            .collect()
    };
    let directory = fold(directory);
    let inside = |root: Vec<String>, strictly: bool| {
        directory.starts_with(&root) && (!strictly || directory.len() > root.len())
    };
    inside(fold(install_root), false) || base.is_some_and(|base| inside(fold(base), true))
}

/// The SDDL for an owner-only, inheritance-protected directory DACL:
/// full control for `owner_sid` and SYSTEM, inherited by children, nothing
/// inherited from the parent. `None` when `owner_sid` is not a plain SID string,
/// so a malformed value can never smuggle extra ACEs into the descriptor.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn owner_only_sddl(owner_sid: &str) -> Option<String> {
    let valid = owner_sid.starts_with("S-1-")
        && owner_sid.len() <= 184
        && owner_sid[2..]
            .split('-')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()));
    valid.then(|| format!("D:P(A;OICI;FA;;;{owner_sid})(A;OICI;FA;;;SY)"))
}

/// `create_dir_all`, with every directory it creates owner-only.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        win32::create_private_dir_all(path)
    }
    #[cfg(not(windows))]
    {
        std::fs::create_dir_all(path)
    }
}

/// Whether `path` is itself a symlink or junction (never followed).
#[cfg_attr(not(windows), allow(dead_code))]
fn is_link(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink())
}

/// Replace the DACL of release-cache directories this user owns when the gate
/// would refuse them. Best effort: a failure leaves the gate's verdict as is.
#[cfg_attr(not(windows), allow(dead_code))]
pub(super) fn secure_release_cache(install_root: &Path, dir: &Path) {
    #[cfg(windows)]
    {
        let base = std::env::var_os("LOCALAPPDATA").map(std::path::PathBuf::from);
        let scoped: Vec<&Path> = dir
            .ancestors()
            .take_while(|directory| in_repair_scope(directory, install_root, base.as_deref()))
            .collect();
        // A junction or symlink anywhere in the chain could carry a repair
        // outside the cache tree, so repair nothing when one is present.
        if scoped.iter().any(|directory| is_link(directory)) {
            tracing::debug!("[modules] release cache path crosses a link; not repaired");
            return;
        }
        for directory in scoped {
            win32::repair_owned_directory(directory);
        }
    }
    #[cfg(not(windows))]
    {
        let _ = (install_root, dir);
    }
}

#[cfg(windows)]
mod win32 {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use super::{owner_only_sddl, should_repair};

    #[repr(C)]
    struct SecurityAttributes {
        length: u32,
        descriptor: *mut c_void,
        inherit_handle: i32,
    }
    #[repr(C)]
    struct SidAndAttributes {
        sid: *mut c_void,
        attributes: u32,
    }

    // `host.rs` declares `GetNamedSecurityInfoW` with a typed DACL out-pointer;
    // this one only reads the owner and passes no DACL pointer.
    #[allow(clashing_extern_declarations)]
    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl: *const u16,
            revision: u32,
            descriptor: *mut *mut c_void,
            size: *mut u32,
        ) -> i32;
        fn ConvertSidToStringSidW(sid: *const c_void, string: *mut *mut u16) -> i32;
        fn GetSecurityDescriptorDacl(
            descriptor: *const c_void,
            present: *mut i32,
            dacl: *mut *mut c_void,
            defaulted: *mut i32,
        ) -> i32;
        fn SetNamedSecurityInfoW(
            name: *mut u16,
            object_type: u32,
            security_info: u32,
            owner: *mut c_void,
            group: *mut c_void,
            dacl: *mut c_void,
            sacl: *mut c_void,
        ) -> u32;
        fn GetNamedSecurityInfoW(
            name: *mut u16,
            object_type: u32,
            security_info: u32,
            owner: *mut *mut c_void,
            group: *mut *mut c_void,
            dacl: *mut *mut c_void,
            sacl: *mut *mut c_void,
            descriptor: *mut *mut c_void,
        ) -> u32;
        fn EqualSid(first: *const c_void, second: *const c_void) -> i32;
        fn OpenProcessToken(process: *mut c_void, access: u32, token: *mut *mut c_void) -> i32;
        fn GetTokenInformation(
            token: *mut c_void,
            class: u32,
            information: *mut c_void,
            length: u32,
            returned_length: *mut u32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateDirectoryW(path: *const u16, attributes: *const SecurityAttributes) -> i32;
        fn GetLastError() -> u32;
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
        fn GetCurrentProcess() -> *mut c_void;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    const SDDL_REVISION_1: u32 = 1;
    const SE_FILE_OBJECT: u32 = 1;
    const OWNER_SECURITY_INFORMATION: u32 = 0x1;
    const DACL_SECURITY_INFORMATION: u32 = 0x4;
    const PROTECTED_DACL_SECURITY_INFORMATION: u32 = 0x8000_0000;
    const TOKEN_QUERY: u32 = 0x8;
    const TOKEN_USER: u32 = 1;
    const ERROR_ALREADY_EXISTS: u32 = 183;

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain([0]).collect()
    }

    /// The process token's user SID: a `usize`-aligned buffer holding the
    /// token information, and a pointer into it that borrows from it.
    fn current_user() -> Option<(Vec<usize>, *mut c_void)> {
        let mut token = std::ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return None;
        }
        let mut len = 0;
        unsafe { GetTokenInformation(token, TOKEN_USER, std::ptr::null_mut(), 0, &mut len) };
        if (len as usize) < size_of::<SidAndAttributes>() {
            unsafe { CloseHandle(token) };
            return None;
        }
        let mut buffer = vec![0usize; (len as usize).div_ceil(size_of::<usize>())];
        let read = unsafe {
            GetTokenInformation(token, TOKEN_USER, buffer.as_mut_ptr().cast(), len, &mut len)
        };
        unsafe { CloseHandle(token) };
        if read == 0 {
            return None;
        }
        let sid = unsafe { (*buffer.as_ptr().cast::<SidAndAttributes>()).sid };
        (!sid.is_null()).then_some((buffer, sid))
    }

    fn sid_string(sid: *const c_void) -> Option<String> {
        let mut text = std::ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 || text.is_null() {
            return None;
        }
        let mut length = 0;
        while unsafe { *text.add(length) } != 0 {
            length += 1;
        }
        let value = String::from_utf16(unsafe { std::slice::from_raw_parts(text, length) }).ok();
        unsafe { LocalFree(text.cast()) };
        value
    }

    /// A security descriptor built from the owner-only SDDL; freed on drop.
    struct Descriptor(*mut c_void);

    impl Descriptor {
        fn owner_only() -> Option<Self> {
            let (_buffer, sid) = current_user()?;
            let sddl = owner_only_sddl(&sid_string(sid)?)?;
            let sddl = sddl.encode_utf16().chain([0]).collect::<Vec<_>>();
            let mut descriptor = std::ptr::null_mut();
            let converted = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    std::ptr::null_mut(),
                )
            };
            (converted != 0 && !descriptor.is_null()).then_some(Self(descriptor))
        }
    }

    impl Drop for Descriptor {
        fn drop(&mut self) {
            unsafe { LocalFree(self.0) };
        }
    }

    pub(super) fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
        create_private(path, true)
    }

    /// `leaf` is the directory the caller asked for: an existing link there is
    /// refused (a write through it would land elsewhere). Its parents are only
    /// required to resolve to directories, since a redirected profile folder
    /// high in the path is legitimate.
    fn create_private(path: &Path, leaf: bool) -> std::io::Result<()> {
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            let plain_dir = metadata.file_type().is_dir();
            if plain_dir || (!leaf && path.is_dir()) {
                return Ok(());
            }
            return Err(std::io::Error::other(
                "a release cache path is not a plain directory",
            ));
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            create_private(parent, false)?;
        }
        let descriptor = Descriptor::owner_only().ok_or_else(|| {
            std::io::Error::other("an owner-only directory ACL could not be built")
        })?;
        let attributes = SecurityAttributes {
            length: size_of::<SecurityAttributes>() as u32,
            descriptor: descriptor.0,
            inherit_handle: 0,
        };
        let path_wide = wide(path);
        if unsafe { CreateDirectoryW(path_wide.as_ptr(), &attributes) } != 0 {
            return Ok(());
        }
        let code = unsafe { GetLastError() };
        if code == ERROR_ALREADY_EXISTS
            && std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
        {
            return Ok(());
        }
        Err(std::io::Error::from_raw_os_error(code as i32))
    }

    pub(super) fn repair_owned_directory(directory: &Path) {
        let Ok(metadata) = std::fs::symlink_metadata(directory) else {
            return;
        };
        let is_real_dir = metadata.file_type().is_dir();
        if !is_real_dir {
            return;
        }
        let owned = owned_by_current_user_or_admins(directory);
        let refused = owned
            && super::super::host::windows_path_grants_untrusted_write(directory).unwrap_or(false);
        if !should_repair(is_real_dir, owned, refused) {
            return;
        }
        let Some(descriptor) = Descriptor::owner_only() else {
            tracing::debug!("[modules] could not build an owner-only ACL for a release cache");
            return;
        };
        let mut dacl = std::ptr::null_mut();
        let (mut present, mut defaulted) = (0, 0);
        let have_dacl = unsafe {
            GetSecurityDescriptorDacl(descriptor.0, &mut present, &mut dacl, &mut defaulted)
        };
        if have_dacl == 0 || present == 0 || dacl.is_null() {
            return;
        }
        let mut name = wide(directory);
        let status = unsafe {
            SetNamedSecurityInfoW(
                name.as_mut_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dacl,
                std::ptr::null_mut(),
            )
        };
        if status == 0 {
            tracing::info!(
                "[modules] replaced the ACL of a release cache directory with an owner-only one"
            );
        } else {
            tracing::debug!("[modules] could not repair a release cache ACL (error {status})");
        }
    }

    /// BUILTIN\Administrators. An elevated administrator's token makes the
    /// group, not the user, the owner of everything it creates, so a cache that
    /// user made is owned by this SID.
    const ADMINISTRATORS_SID: &str = "S-1-5-32-544";

    /// Whether the directory's owner is the current user, or the Administrators
    /// group (the default owner of what an elevated administrator creates).
    /// Rewriting the DACL still needs WRITE_DAC, so another account's directory
    /// stays untouched: the call fails and the gate's verdict stands.
    fn owned_by_current_user_or_admins(directory: &Path) -> bool {
        let Some((_buffer, user)) = current_user() else {
            return false;
        };
        let mut name = wide(directory);
        let mut owner = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        let status = unsafe {
            GetNamedSecurityInfoW(
                name.as_mut_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 || descriptor.is_null() {
            return false;
        }
        let same = !owner.is_null()
            && (unsafe { EqualSid(owner, user) } != 0
                || sid_string(owner).as_deref() == Some(ADMINISTRATORS_SID));
        unsafe { LocalFree(descriptor) };
        same
    }
}

#[cfg(test)]
#[path = "windows_acl_tests.rs"]
mod tests;
