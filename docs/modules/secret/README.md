# `secret`

`Secret` reduces how long and how widely a sensitive byte buffer stays exposed
in this process's own address space. It is a hardening measure, not an access
control.

## Threat model — read this before using it

**This is exposure reduction, not a security boundary.** `Secret` defends
against a specific, narrow set of *accidental* leaks: the plaintext ending up
somewhere that outlives the process or gets shipped off the machine without
anyone deciding to send it there.

What it defeats:

- **Swap / hibernation.** `mlock`/`VirtualLock` pins the buffer's pages in
  RAM, so the plaintext is never written to a swap file or a hibernation
  image that could sit on disk long after the process exits.
- **Core dumps.** `madvise(MADV_DONTDUMP)` on Linux excludes the buffer's
  pages from a core dump, so a crash report generated for a bug in unrelated
  code does not also hand over every secret that happened to be resident.
- **Lingering plaintext.** Zeroizing on `Drop` shrinks the window during
  which freed-but-not-yet-reused memory holds a readable copy, and stops a
  later heap reuse (or a *future* core dump of a *different* crash) from
  turning up bytes that should already be gone. This zeroizes the buffer's
  full `capacity`, not just its `len` — a `Vec<u8>` that was built and then
  shrunk (`v.truncate(..)`) still has the shrunk-away bytes sitting in its
  spare capacity, and those get freed in the clear unless the whole
  allocation is cleared.

What it does **not** defend against, and cannot:

- **Intermediate buffers `Secret::new` never saw.** `Secret::new` takes
  ownership of an already-built `Vec<u8>` and hardens *that* allocation. It
  cannot reach back and clear allocations the `Vec` already reallocated away
  while the caller was constructing it — e.g. the smaller, now-freed buffers
  left behind by growth reallocations during a loop of `push` calls. If that
  gap matters, build the buffer with its final capacity reserved up front
  (`Vec::with_capacity`), so there is only ever one allocation for `Secret`
  to harden.

- A debugger or `ptrace` attached to the process. `Secret`'s bytes are, by
  necessity, plaintext in normal memory while in use — that is what makes
  them usable. Anything that can read this process's memory reads them too.
- `/proc/<pid>/mem`, or any other same-machine, same-privilege introspection.
- Root, or any principal with more privilege than the process itself.
- Other code running **in the same address space**. An in-process module
  loaded with `dlopen` (see `crates/tinybus/src/module/`) is inside the trust
  boundary already — it can read a `Secret` as easily as the code that
  created it. `Secret` was never going to change that; nothing dependency-free
  and running in the same process can.

If the threat you are worried about is any of the above, `Secret` is the
wrong tool. The right tool is process isolation: don't hold the secret in a
process an untrusted party can attach to.

## Why `mlock` failure is non-fatal

`RLIMIT_MEMLOCK` is commonly 64 KiB to a few MiB for an unprivileged process.
A bus holding a modest number of secrets will exceed that limit in completely
ordinary operation, long before anything is misbehaving. `Secret::new` treats
a locking failure as expected, logs it at `debug`, and returns a `Secret`
that is fully correct — just not locked against swap. A message bus that
refused to run because it could not lock a page would be a strictly worse
outcome than one that ran with slightly weaker hardening.

## `harden_process` is opt-in, and process-wide

`harden_process()` calls `prctl(PR_SET_DUMPABLE, 0)` on Linux, which disables
core dumps for the *entire* process and, as a side effect, blocks a same-uid
`ptrace` attach against it. It is exported but never called by this crate:
it changes `/proc/<pid>` file ownership and breaks debuggers and crash
reporting for everything the process does, not just its secrets, so only the
embedding application — after deciding that tradeoff is worth it in its own
deployment — should call it.

## Not wired in yet

`Secret` is a standalone primitive in this change. Nothing in
`message`, `broker`, or `router` constructs or stores one; that adoption is
deliberately a separate change.

## No new dependencies

Everything here is a hand-declared `unsafe extern "C"` / `unsafe extern
"system"` block, matching the precedent set by `module::host` (Windows ACL
calls via `#[link(name = "advapi32")]`) and `bin/tinybus` (`libc_getuid` via
`#[link_name = "getuid"]`). This crate already hand-rolls SHA-256 rather than
take a dependency for it; two or three syscalls do not earn one either.

## Future options, not implemented here

- **Linux `memfd_secret(2)`** (5.14+): pages that are unmapped from the
  kernel's own direct map, so not even the kernel can read them without
  explicitly mapping the memfd first, and which die with the process. A
  meaningfully stronger primitive than `mlock`, gated on kernel version and
  currently unused here.
- **Windows `CryptProtectMemory(CRYPTPROTECTMEMORY_SAME_PROCESS)`**: encrypts
  a buffer in place with a per-boot session key, so a same-process reader
  still needs to call the decrypt API rather than dereferencing a pointer.
