//! A library digest remembered for the life of the process.
//!
//! Loading a module checks its library against the allowlist twice, once when
//! the release cache is consulted and again at the load gate. Hashing a 25 MB
//! library is the largest single cost of a bundled module's startup, so the
//! second check reuses the first answer when it can prove the file is the same
//! one: see [`file_hex_remembered`]. The hashing itself is `hash::file_hex`.

use std::fs::File;
use std::io;

/// `hash::file_hex` over an open file, remembering the answer for this process.
///
/// A file is the same file only if its device, inode, length, modification
/// time and status-change time all match what they were when it was hashed.
/// The status-change time moves on every write and cannot be set from user
/// space, so a library rewritten in place does not match, even if its length
/// and modification time were put back. All of it is read from the open handle
/// being hashed, never from a path, so it cannot describe a different file
/// than the bytes read.
///
/// Nothing is remembered for a file touched within the last
/// `REMEMBER_AFTER` (two seconds), because a write in the same clock tick as the hash would
/// leave the identity unchanged. Platforms without the identity fields hash
/// every time.
pub(crate) fn file_hex_remembered(file: File) -> io::Result<String> {
    imp::hex(file, imp::REMEMBER_AFTER).map(|(hex, _)| hex)
}

#[cfg(unix)]
mod imp {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::MetadataExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub(super) const REMEMBER_AFTER: Duration = Duration::from_secs(2);

    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Identity {
        dev: u64,
        ino: u64,
        len: u64,
        mtime: (i64, i64),
        ctime: (i64, i64),
    }

    fn identity(file: &File) -> io::Result<Identity> {
        let meta = file.metadata()?;
        Ok(Identity {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
            ctime: (meta.ctime(), meta.ctime_nsec()),
        })
    }

    fn quiet_for(identity: &Identity, age: Duration) -> bool {
        let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return false;
        };
        let newest = identity.mtime.0.max(identity.ctime.0);
        u64::try_from(newest).is_ok_and(|newest| Duration::from_secs(newest) + age <= now)
    }

    fn memory() -> &'static Mutex<HashMap<Identity, String>> {
        static MEMORY: OnceLock<Mutex<HashMap<Identity, String>>> = OnceLock::new();
        MEMORY.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// The digest, and whether it was served from memory.
    pub(super) fn hex(file: File, age: Duration) -> io::Result<(String, bool)> {
        let before = identity(&file)?;
        let eligible = quiet_for(&before, age);
        if eligible {
            if let Some(known) = memory().lock().ok().and_then(|m| m.get(&before).cloned()) {
                return Ok((known, true));
            }
        }
        let digest = crate::module::hash::file_hex(&file)?;
        // Hashed bytes are only attributable to this identity if the file did
        // not change underneath the read.
        if eligible && identity(&file).is_ok_and(|after| after == before) {
            if let Ok(mut memory) = memory().lock() {
                memory.insert(before, digest.clone());
            }
        }
        Ok((digest, false))
    }
}

#[cfg(not(unix))]
mod imp {
    use std::io;
    use std::time::Duration;

    pub(super) const REMEMBER_AFTER: Duration = Duration::from_secs(2);

    pub(super) fn hex(file: std::fs::File, _age: Duration) -> io::Result<(String, bool)> {
        crate::module::hash::file_hex(file).map(|digest| (digest, false))
    }
}

#[cfg(all(test, unix))]
#[path = "remembered_hash_tests.rs"]
mod tests;
