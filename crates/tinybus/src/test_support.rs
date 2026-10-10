//! Shared helpers for integration tests that load a built TinyBus module.
//!
//! These helpers are deliberately opt-in through the `test-support` feature.
//! They locate the platform library name, verify its adjacent `modules.toml`
//! pin before asking TinyBus to load it, and provide deadline-based lifecycle
//! waits for tests using an in-memory broker.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Connection, Proxy, Result,
    broker::Broker,
    module::{ModuleHost, ModuleInfo, ModuleState, sha256_file},
    transport::memory::MemoryBus,
};

const MODULE_UNLOADED: u8 = 0;
const MODULE_LOADING: u8 = 1;
const MODULE_ADMITTED: u8 = 2;

static MODULE_LOAD_STATE: AtomicU8 = AtomicU8::new(MODULE_UNLOADED);

struct LoadReservation<'a> {
    state: &'a AtomicU8,
    admitted: bool,
}

impl<'a> LoadReservation<'a> {
    fn reserve(state: &'a AtomicU8) -> Result<Self> {
        state
            .compare_exchange(
                MODULE_UNLOADED,
                MODULE_LOADING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                crate::Error::failed("only one dynamic module may be loaded per test process")
            })?;
        Ok(Self {
            state,
            admitted: false,
        })
    }

    fn admitted(&mut self) {
        self.state.store(MODULE_ADMITTED, Ordering::Release);
        self.admitted = true;
    }
}

impl Drop for LoadReservation<'_> {
    fn drop(&mut self) {
        if !self.admitted {
            let _ = self.state.compare_exchange(
                MODULE_LOADING,
                MODULE_UNLOADED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

/// Resolve the library filename Cargo produces for `crate_name` on this OS.
///
/// `crate_name` may be written as a Cargo package name (`tiny-docs-module`) or
/// as its Rust library name (`tiny_docs_module`).
pub fn artifact_path(target_dir: impl AsRef<Path>, crate_name: &str) -> PathBuf {
    let crate_name = crate_name.replace('-', "_");
    let file_name = artifact_filename(&crate_name);
    target_dir.as_ref().join(file_name)
}

#[cfg(target_os = "windows")]
fn artifact_filename(crate_name: &str) -> String {
    format!("{crate_name}.dll")
}

#[cfg(target_os = "macos")]
fn artifact_filename(crate_name: &str) -> String {
    format!("lib{crate_name}.dylib")
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn artifact_filename(crate_name: &str) -> String {
    format!("lib{crate_name}.so")
}

/// Load the single module artifact named by `env_var` through `host`.
///
/// The artifact must be in a dedicated directory with an adjacent
/// `modules.toml` entry whose SHA-256 matches the file. The guard is
/// process-wide: TinyBus intentionally never unloads a module, so attempting
/// a second load in the same test process fails before mapping another library.
pub fn admit_module(
    host: &ModuleHost,
    env_var: &str,
    expected_module_name: &str,
) -> Result<ModuleInfo> {
    let artifact = std::env::var_os(env_var)
        .map(PathBuf::from)
        .ok_or_else(|| crate::Error::failed(format!("{env_var} must point to the built module")))?;
    verify_modules_pin(&artifact)?;
    let directory = artifact
        .parent()
        .ok_or_else(|| crate::Error::failed("module artifact has no parent directory"))?;
    verify_single_artifact(directory, &artifact)?;
    let mut reservation = LoadReservation::reserve(&MODULE_LOAD_STATE)?;
    let outcomes = host.load_dir(directory)?;
    let Some(result) = outcomes.into_iter().next() else {
        return Err(crate::Error::failed(format!(
            "TinyBus did not admit module `{expected_module_name}` from the test artifact directory"
        )));
    };
    let info = result?;
    reservation.admitted();
    if info.name != expected_module_name {
        return Err(crate::Error::failed(format!(
            "expected TinyBus module `{expected_module_name}`, admitted `{}`",
            info.name
        )));
    }
    Ok(info)
}

/// Start an in-memory broker and return its host, client, and task handle.
pub async fn start_bus() -> Result<(ModuleHost, Connection, tokio::task::JoinHandle<Result<()>>)> {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let client = Connection::connect(bus.connect().await?).await?;
    Ok((host, client, broker_task))
}

/// Wait until `client` observes `name`, failing when `timeout` expires.
pub async fn wait_until_serving(client: &Connection, name: &str, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if client
                .list_names()
                .await?
                .iter()
                .any(|candidate| candidate.as_str() == name)
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| crate::Error::failed(format!("module `{name}` did not become ready in time")))?
}

/// Wait for `name` to return to Ready after its calls have completed.
pub async fn wait_until_idle(host: &ModuleHost, name: &str, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            let Some(module) = host.list().into_iter().find(|module| module.name == name) else {
                return Err(crate::Error::failed(format!(
                    "module `{name}` is not loaded"
                )));
            };
            match module.state {
                ModuleState::Ready => return Ok(()),
                ModuleState::Serving => tokio::task::yield_now().await,
                state => {
                    return Err(crate::Error::failed(format!(
                        "module `{name}` left service state: {state:?}"
                    )));
                }
            }
        }
    })
    .await
    .map_err(|_| crate::Error::failed(format!("module `{name}` did not become idle in time")))?
}

/// Make a typed member call through a TinyBus proxy.
pub async fn call<R, A>(proxy: &Proxy, member: &str, arguments: A) -> Result<R>
where
    R: DeserializeOwned,
    A: Serialize,
{
    proxy.call(member, arguments).await
}

fn verify_modules_pin(artifact: &Path) -> Result<()> {
    let directory = artifact
        .parent()
        .ok_or_else(|| crate::Error::failed("module artifact has no parent directory"))?;
    let file_name = artifact
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| crate::Error::failed("module artifact filename is not UTF-8"))?;
    let manifest_path = directory.join("modules.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .map_err(|_| crate::Error::failed("cannot read modules.toml beside the module artifact"))?;
    let expected = crate::attest::parse_allowlist(&manifest)
        .find_map(|(name, digest)| (name == file_name).then_some(digest))
        .ok_or_else(|| crate::Error::failed(format!("modules.toml does not pin `{file_name}`")))?;
    let actual = sha256_file(artifact)?;
    if !actual.eq_ignore_ascii_case(&expected) {
        return Err(crate::Error::failed(format!(
            "module `{file_name}` digest does not match modules.toml"
        )));
    }
    Ok(())
}

fn verify_single_artifact(directory: &Path, selected: &Path) -> Result<()> {
    let entries = std::fs::read_dir(directory)
        .map_err(|_| crate::Error::failed("cannot inspect the test module artifact directory"))?;
    let mut libraries = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| {
            crate::Error::failed("cannot inspect the test module artifact directory")
        })?;
        let path = entry.path();
        let extension = path.extension().and_then(|value| value.to_str());
        if matches!(extension, Some("dll" | "dylib" | "so")) {
            libraries.push(path);
        }
    }
    if libraries.len() != 1 || libraries[0] != selected {
        return Err(crate::Error::failed(
            "the test module artifact directory must contain only the selected library",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod tests;
