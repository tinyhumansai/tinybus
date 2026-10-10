//! Shared helpers for integration tests that load a built TinyBus module.
//!
//! These helpers are deliberately opt-in through the `test-support` feature.
//! They locate the platform library name, verify its adjacent `modules.toml`
//! pin before asking TinyBus to load it, and provide deadline-based lifecycle
//! waits for tests using an in-memory broker.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Connection, Proxy, Result,
    broker::Broker,
    module::{ModuleHost, ModuleInfo, ModuleState, sha256_file},
    transport::memory::MemoryBus,
};

static MODULE_LOADED: AtomicBool = AtomicBool::new(false);

/// Resolve the library filename Cargo produces for `crate_name` on this OS.
///
/// `crate_name` may be written as a Cargo package name (`tiny-docs-module`) or
/// as its Rust library name (`tiny_docs_module`).
pub fn artifact_path(target_dir: impl AsRef<Path>, crate_name: &str) -> PathBuf {
    let crate_name = crate_name.replace('-', "_");
    let file_name = if cfg!(target_os = "windows") {
        format!("{crate_name}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{crate_name}.dylib")
    } else {
        format!("lib{crate_name}.so")
    };
    target_dir.as_ref().join(file_name)
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
    if MODULE_LOADED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(crate::Error::failed(
            "only one dynamic module may be loaded per test process",
        ));
    }

    let artifact = std::env::var_os(env_var)
        .map(PathBuf::from)
        .ok_or_else(|| crate::Error::failed(format!("{env_var} must point to the built module")))?;
    verify_modules_pin(&artifact)?;
    let directory = artifact
        .parent()
        .ok_or_else(|| crate::Error::failed("module artifact has no parent directory"))?;
    let outcomes = host.load_dir(directory)?;
    let info = outcomes
        .into_iter()
        .find_map(|result| match result {
            Ok(info) if info.name == expected_module_name => Some(Ok(info)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .transpose()?
        .ok_or_else(|| {
            crate::Error::failed(format!(
                "TinyBus did not admit module `{expected_module_name}` from {}",
                artifact.display()
            ))
        })?;
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
    let manifest = std::fs::read_to_string(&manifest_path).map_err(|error| {
        crate::Error::failed(format!(
            "cannot read module digest manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
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

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod tests;
