//! In-memory exposure reduction for sensitive byte buffers.
//!
//! [`Secret`] is not a security boundary — see the module README at
//! `docs/modules/secret/README.md` for the honest threat model. It is a
//! best-effort reduction of the ways plaintext held in this process's own
//! address space can leak *outside* that address space: into swap, into a
//! core dump, or into crash telemetry. Anything that can already read this
//! process's memory (a debugger, `/proc/<pid>/mem`, root, or other code
//! sharing the address space) is unaffected by any of this.
//!
//! Three mechanisms, all free and dependency-free:
//!
//! 1. `mlock`/`VirtualLock` the buffer's pages on construction, so the pages
//!    are pinned in RAM and never written to a swap file or hibernation
//!    image. Best-effort: `RLIMIT_MEMLOCK` is commonly a few hundred
//!    kilobytes for an unprivileged process, so this routinely fails, and a
//!    failure must never be fatal (see [`Secret::new`]).
//! 2. `madvise(MADV_DONTDUMP)` on Linux, so the pages are excluded from a
//!    core dump. A no-op everywhere else — this crate does not fake platform
//!    support it does not have.
//! 3. Zeroize on [`Drop`], via a volatile write loop the compiler cannot
//!    elide, so the plaintext does not linger in freed memory that gets
//!    reused (and possibly paged or dumped) later. This covers the buffer's
//!    full `capacity`, not just its `len` — see [`Secret::new`] for why that
//!    distinction matters.
//!
//! [`harden_process`] is a separate, opt-in, process-wide knob: it does not
//! run automatically anywhere in this crate.

use std::ffi::c_void;
use std::sync::atomic::{Ordering, compiler_fence};

/// A byte buffer holding sensitive material, hardened against *accidental*
/// exposure via swap, core dumps and dangling plaintext — not against a
/// privileged or co-resident attacker. Read `docs/modules/secret/README.md`
/// before relying on this for anything beyond exposure reduction.
///
/// Construction locks the buffer's pages in memory and (on Linux) excludes
/// them from core dumps, best-effort. [`Drop`] zeroizes the bytes before the
/// backing allocation is freed.
pub struct Secret {
    bytes: Vec<u8>,
    /// Whether `mlock`/`VirtualLock` succeeded, so `Drop` knows whether an
    /// unlock call is needed. Not part of the public API: a caller cannot
    /// act on it, and exposing it would just be a way to ask the OS whether
    /// hardening is present without changing what to do about it.
    locked: bool,
}

impl Secret {
    /// Takes ownership of `bytes` and hardens the resulting buffer:
    /// attempts to lock its pages in memory and, on Linux, exclude them from
    /// core dumps.
    ///
    /// Both attempts are best-effort and their failure is never fatal —
    /// deliberately. `RLIMIT_MEMLOCK` is commonly 64 KiB to a few MiB for an
    /// unprivileged process, so `mlock` genuinely fails in normal operation
    /// long before a bus's worth of secrets would exceed it. A message bus
    /// that refused to hold a secret because it could not lock a page would
    /// be a worse outcome than one that holds it unlocked; failures are
    /// logged at `debug` and construction proceeds with a still-correct,
    /// just less hardened, `Secret`.
    ///
    /// The lock/no-dump hardening is scoped to `bytes.len()` at the moment of
    /// construction — deliberately, not `capacity()`: `mlock`/`madvise`
    /// operate at page granularity anyway, so locking the unused tail of the
    /// allocation buys nothing, and it would spend more of the caller's
    /// (often small) `RLIMIT_MEMLOCK` budget on bytes that were never
    /// populated. `Drop`'s zeroization does *not* make the same choice — it
    /// covers the full allocation, because a `Vec` built by, say, reading
    /// key material and then `truncate`-ing it can leave real secret bytes
    /// sitting in the spare capacity, and those still get freed by this
    /// call whether or not they were ever "in use".
    ///
    /// Even with that fixed, `Secret::new` cannot protect intermediate
    /// buffers the caller's own `Vec` already reallocated away while it was
    /// being built — e.g. the old, smaller allocations left behind by
    /// repeated `push` calls that grew the vector's capacity. Only the
    /// allocation handed to this constructor is hardened; construct the
    /// buffer inside a `Secret`-owned allocation (or with `Vec::with_capacity`
    /// sized up front) if that gap matters.
    pub fn new(bytes: Vec<u8>) -> Self {
        let mut bytes = bytes;
        let locked = harden_buffer(bytes.as_mut_ptr(), bytes.len());
        Self { bytes, locked }
    }

    /// Borrows the underlying bytes.
    ///
    /// Named to make every call site read as a deliberate exposure: this is
    /// the one place the plaintext leaves the type's control, so `grep`-ing
    /// `expose_secret` finds every use.
    pub fn expose_secret(&self) -> &[u8] {
        &self.bytes
    }

    /// The number of bytes held.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        // Zeroize the *whole allocation* — `capacity`, not `len` — before
        // unlocking and freeing. A `Vec<u8>` handed to `Secret::new` after
        // e.g. `key.truncate(32)` still has the truncated tail sitting in its
        // spare capacity; `len` alone would free that tail in the clear.
        //
        // SAFETY: `self.bytes.as_mut_ptr()` is valid for `self.bytes.capacity()`
        // bytes of writes — that is the definition of a `Vec`'s allocation —
        // for as long as `self.bytes` has not been dropped, which it has not:
        // `Vec`'s own `Drop` runs after this function returns. Bytes past
        // `len` may be uninitialized; `zeroize_raw` only ever writes through
        // the raw pointer and never reads or forms a `&mut [u8]` over that
        // range, so the possible uninitialization is never observed.
        unsafe { zeroize_raw(self.bytes.as_mut_ptr(), self.bytes.capacity()) };
        if self.locked {
            unlock_buffer(self.bytes.as_mut_ptr(), self.bytes.len());
        }
        // `self.bytes` (now all zero across its full allocation) is freed by
        // `Vec`'s own `Drop`, which runs immediately after this function
        // returns.
    }
}

impl std::fmt::Debug for Secret {
    // Hand-written, not derived: a derived `Debug` on a `Vec<u8>` field would
    // print every byte. This crate treats "never print the payload" as an
    // invariant elsewhere too — see `Proxy`'s hand-written `Debug` and
    // `Error::bad_arguments`'s redaction.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret([redacted], {} bytes)", self.bytes.len())
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret([redacted], {} bytes)", self.bytes.len())
    }
}

/// Overwrites `bytes` with zero via a volatile write to every element,
/// followed by a `SeqCst` compiler fence.
///
/// A plain `bytes.fill(0)` immediately before the memory is freed is exactly
/// the kind of store LLVM is permitted to prove dead and remove — nothing
/// downstream ever reads it before the free. `write_volatile` forbids that
/// optimization per-write, and the fence stops the compiler reordering *other*
/// memory operations across the zeroization, so a caller that checks "is this
/// zeroed yet" cannot observe the write out of order.
///
/// Takes an already-initialized `&mut [u8]`, so every byte in range is safe
/// to address as a reference. [`zeroize_raw`] is the pointer-based sibling
/// this delegates to, used directly wherever the range may include
/// uninitialized bytes (a `Vec`'s spare capacity).
fn zeroize(bytes: &mut [u8]) {
    // SAFETY: `bytes.as_mut_ptr()` is valid for `bytes.len()` writes — the
    // slice's own guarantee — and every one of those bytes is initialized,
    // so nothing here relies on write-without-read soundness that `bytes`
    // itself does not already provide.
    unsafe { zeroize_raw(bytes.as_mut_ptr(), bytes.len()) };
}

/// Overwrites `len` bytes at `ptr` with zero via a volatile write to each
/// byte, followed by a `SeqCst` compiler fence — see [`zeroize`] for why
/// both of those matter.
///
/// Deliberately takes a raw pointer rather than a `&mut [u8]`: the caller in
/// [`Secret`]'s `Drop` needs to zero a `Vec`'s full `capacity`, and the bytes
/// between `len` and `capacity` are typically uninitialized. Forming a
/// `&mut [u8]` over uninitialized memory is its own footgun (references are
/// expected to point at initialized values); going through a raw pointer and
/// only ever *writing*, never reading, sidesteps that entirely — writing an
/// arbitrary bit pattern to memory of a type with no invalid bit patterns
/// (`u8`) is sound regardless of what was there before.
///
/// # Safety
///
/// `ptr` must be valid for `len` bytes of writes for the duration of this
/// call (i.e. non-null, non-dangling, and not aliased by a live reference
/// elsewhere). The memory does not need to be initialized.
unsafe fn zeroize_raw(ptr: *mut u8, len: usize) {
    for offset in 0..len {
        // SAFETY: `ptr.add(offset)` is in-bounds for `len` bytes per this
        // function's own contract; `write_volatile` writes without ever
        // reading the destination, so its prior initialization state does
        // not matter.
        unsafe { std::ptr::write_volatile(ptr.add(offset), 0) };
    }
    compiler_fence(Ordering::SeqCst);
}

/// Locks `len` bytes at `ptr` in memory and, on Linux, excludes them from
/// core dumps. Returns whether the memory lock succeeded. `len == 0` is a
/// no-op: an empty buffer has nothing to lock and most platforms treat a
/// zero-length `mlock`/`VirtualLock` as at best meaningless.
///
/// Both syscalls are attempted independently and neither failure is
/// propagated to the caller — see [`Secret::new`] for why.
fn harden_buffer(ptr: *mut u8, len: usize) -> bool {
    if len == 0 {
        return false;
    }

    let mut locked = false;

    #[cfg(unix)]
    // SAFETY: `ptr` is a valid pointer to `len` initialized bytes owned by
    // the `Vec` this call is hardening; `mlock` only reads the address
    // range's page mapping and does not dereference through `ptr` itself.
    unsafe {
        if mlock(ptr as *const c_void, len) == 0 {
            locked = true;
        } else {
            tracing::debug!(
                len,
                "Secret: mlock failed (RLIMIT_MEMLOCK likely exceeded); \
                 continuing with an unlocked, unhardened-against-swap buffer"
            );
        }
    }

    #[cfg(windows)]
    // SAFETY: same as the `mlock` call above, for the Win32 equivalent.
    unsafe {
        if VirtualLock(ptr as *mut c_void, len) != 0 {
            locked = true;
        } else {
            tracing::debug!(
                len,
                "Secret: VirtualLock failed; continuing with an unlocked, \
                 unhardened-against-swap buffer"
            );
        }
    }

    #[cfg(target_os = "linux")]
    // SAFETY: same validity argument as `mlock`; `madvise` also only acts on
    // the page mapping, not the bytes themselves.
    unsafe {
        if madvise(ptr as *mut c_void, len, MADV_DONTDUMP) != 0 {
            tracing::debug!(
                len,
                "Secret: madvise(MADV_DONTDUMP) failed; this buffer may appear \
                 in a core dump"
            );
        }
    }

    locked
}

/// Reverses [`harden_buffer`]'s memory lock. `len == 0` mirrors the guard in
/// `harden_buffer`, since nothing was ever locked for an empty buffer.
fn unlock_buffer(ptr: *mut u8, len: usize) {
    if len == 0 {
        return;
    }

    #[cfg(unix)]
    // SAFETY: `ptr`/`len` describe the same, still-live allocation that was
    // just locked by `harden_buffer`; called from `Drop` before the `Vec`
    // backing it is freed.
    unsafe {
        munlock(ptr as *const c_void, len);
    }

    #[cfg(windows)]
    // SAFETY: same as above, for the Win32 equivalent.
    unsafe {
        VirtualUnlock(ptr as *mut c_void, len);
    }
}

#[cfg(unix)]
// Declared by hand rather than taking a `libc` dependency for two calls —
// this crate hand-rolls SHA-256 for the same reason. `mlock`/`munlock` are
// POSIX and have had this exact signature since 4.4BSD.
unsafe extern "C" {
    fn mlock(addr: *const c_void, len: usize) -> i32;
    fn munlock(addr: *const c_void, len: usize) -> i32;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
}

#[cfg(target_os = "linux")]
// From `<sys/mman.h>`; stable across Linux architectures since its
// introduction in 3.4 (glibc does not expose it as a named constant, so it is
// declared here rather than imported).
const MADV_DONTDUMP: i32 = 16;

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn VirtualLock(lpAddress: *mut c_void, dwSize: usize) -> i32;
    fn VirtualUnlock(lpAddress: *mut c_void, dwSize: usize) -> i32;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn prctl(option: i32, arg2: u64, arg3: u64, arg4: u64, arg5: u64) -> i32;
}

#[cfg(target_os = "linux")]
const PR_SET_DUMPABLE: i32 = 4;

/// Disables core dumps for the **entire current process** and, as a side
/// effect on Linux, blocks a same-uid `ptrace` attach against it. A no-op on
/// every platform other than Linux.
///
/// # This is process-wide and opt-in — call it deliberately, not by default
///
/// This is not scoped to `Secret` or to this crate: it changes the dumpable
/// bit for the whole process, which changes the ownership of the process's
/// `/proc/<pid>` files and disables `gdb`/`ptrace`-based debugging and crash
/// reporting for everything the process does, not only its secrets. A
/// library must not impose that on whatever embeds it. Call this only from
/// an application's own startup path, after weighing that a crash in
/// production will no longer produce a core dump or attach a debugger.
///
/// Never called by this crate itself.
pub fn harden_process() {
    #[cfg(target_os = "linux")]
    // SAFETY: `prctl(PR_SET_DUMPABLE, 0, ...)` takes no pointers and cannot
    // be unsafe in the memory-safety sense; it is `unsafe` only because it is
    // an FFI call.
    unsafe {
        if prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0 {
            tracing::warn!(
                "harden_process: PR_SET_DUMPABLE failed; core dumps and \
                 same-uid ptrace attach remain enabled for this process"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_round_trips_the_bytes_it_was_built_from() {
        let secret = Secret::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(secret.expose_secret(), &[1, 2, 3, 4, 5]);
        assert_eq!(secret.len(), 5);
        assert!(!secret.is_empty());
    }

    #[test]
    fn an_empty_secret_reports_empty_without_touching_a_null_pointer() {
        let secret = Secret::new(Vec::new());
        assert!(secret.is_empty());
        assert_eq!(secret.len(), 0);
        assert_eq!(secret.expose_secret(), &[] as &[u8]);
    }

    #[test]
    fn a_secret_never_prints_its_contents_when_debug_formatted() {
        let secret = Secret::new(b"correct horse battery staple".to_vec());
        let printed = format!("{secret:?}");
        assert!(!printed.contains("correct"));
        assert!(!printed.contains("horse"));
        assert!(!printed.contains("battery"));
        assert!(!printed.contains("staple"));
        assert_eq!(printed, "Secret([redacted], 28 bytes)");
    }

    #[test]
    fn a_secret_never_prints_its_contents_when_display_formatted() {
        let secret = Secret::new(b"top secret payload".to_vec());
        let printed = format!("{secret}");
        assert!(!printed.contains("top"));
        assert!(!printed.contains("secret"));
        assert!(!printed.contains("payload"));
        assert_eq!(printed, "Secret([redacted], 18 bytes)");
    }

    #[test]
    fn a_secrets_debug_output_does_not_leak_length_derived_secrets() {
        // The length itself is reported by design (it is not sensitive on its
        // own), but nothing *derived* from the bytes — a checksum, a prefix,
        // anything — should ever show up alongside it.
        let secret = Secret::new(vec![0xAB; 8]);
        let printed = format!("{secret:?}");
        assert_eq!(printed, "Secret([redacted], 8 bytes)");
    }

    #[test]
    fn construction_succeeds_even_when_the_memory_lock_would_fail() {
        // `mlock` routinely fails under a low RLIMIT_MEMLOCK; a `Secret`
        // large enough to blow past a typical unprivileged limit still has
        // to construct successfully and hold its bytes correctly. This does
        // not assert on `locked` (there is no portable way to force the
        // syscall to fail), only that a large buffer still round-trips.
        let big = vec![0x42u8; 4 * 1024 * 1024];
        let secret = Secret::new(big.clone());
        assert_eq!(secret.expose_secret(), big.as_slice());
    }

    #[test]
    fn zeroizing_a_live_buffer_overwrites_every_byte_with_zero() {
        // Exercises the zeroization routine directly on a buffer this test
        // still owns, rather than reading a `Secret` after it has been
        // dropped (which would be a read of freed memory and undefined
        // behaviour).
        let mut bytes = vec![1u8, 2, 3, 4, 5, 255, 128, 7];
        zeroize(&mut bytes);
        assert_eq!(bytes, vec![0u8; 8]);
    }

    #[test]
    fn zeroizing_an_empty_buffer_is_a_harmless_no_op() {
        let mut bytes: Vec<u8> = Vec::new();
        zeroize(&mut bytes);
        assert!(bytes.is_empty());
    }

    #[test]
    fn harden_and_unlock_round_trip_without_panicking_on_a_live_allocation() {
        // Exercises the lock/unlock pair directly on a buffer this test
        // still owns and frees itself, independent of `Secret`'s `Drop`.
        // Locking may or may not succeed depending on the sandbox's
        // RLIMIT_MEMLOCK; either outcome is acceptable, only a panic is not.
        let mut bytes = vec![9u8; 4096];
        let locked = harden_buffer(bytes.as_mut_ptr(), bytes.len());
        if locked {
            unlock_buffer(bytes.as_mut_ptr(), bytes.len());
        }
    }

    #[test]
    fn dropping_a_secret_does_not_panic_regardless_of_lock_state() {
        // The `Drop` impl's zeroize-then-maybe-unlock sequence is exercised
        // implicitly by every other test via scope exit; this test makes the
        // property explicit for both the locked and empty cases.
        {
            let _secret = Secret::new(vec![1, 2, 3]);
        }
        {
            let _secret = Secret::new(Vec::new());
        }
    }
}
