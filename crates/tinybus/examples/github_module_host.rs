//! Minimal GitHub release loader for a tinybus module host.

use std::env;

use tinybus::{broker::Broker, module::ModuleHost};

#[tokio::main]
async fn main() -> tinybus::Result<()> {
    let mut args = env::args().skip(1);
    let release_url = args.next().ok_or_else(|| {
        tinybus::Error::failed(
            "usage: github_module_host <release-tag-url> <archive-name> <sha256>",
        )
    })?;
    let archive_name = args.next().ok_or_else(|| {
        tinybus::Error::failed(
            "usage: github_module_host <release-tag-url> <archive-name> <sha256>",
        )
    })?;
    let sha256 = args.next().ok_or_else(|| {
        tinybus::Error::failed(
            "usage: github_module_host <release-tag-url> <archive-name> <sha256>",
        )
    })?;

    let host = ModuleHost::new(Broker::new());
    let module = host.load_github_release(
        release_url,
        archive_name,
        Some(&sha256),
        serde_json::json!({}),
    )?;
    println!("loaded {} {}", module.name, module.version);
    Ok(())
}
