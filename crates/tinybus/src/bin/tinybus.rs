//! The `tinybus` CLI: run the broker, and poke at a running one.
//!
//! Every subcommand is declared even where its milestone has not landed, so
//! runbooks and scripts can be written against a stable surface and an
//! unimplemented one fails with a message naming the milestone rather than
//! "unknown subcommand".
//!
//! The important one is `monitor`. A bus whose traffic you cannot watch is a
//! bus you debug by adding logging to two processes and restarting both; with
//! it, "did the kernel actually call the wallet" is one command.

use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand};
use tinybus::broker::Broker;
use tinybus::connection::Connection;
use tinybus::message::MessageKind;
use tinybus::router::MatchRule;
use tinybus::transport::unix::{UnixListenerAdapter, UnixTransport};
use tinybus::{Error, Result};

#[derive(Parser)]
#[command(name = "tinybus", version, about, long_about = None)]
struct Cli {
    /// Path to the bus socket.
    #[arg(long, env = tinybus::DEFAULT_SOCKET_ENV, global = true)]
    address: Option<PathBuf>,

    /// Seconds to wait for a reply before giving up.
    #[arg(long, default_value_t = 30, global = true)]
    timeout: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the broker until interrupted.
    Serve,

    /// Call a method and print the reply as JSON.
    Call {
        /// The destination name, e.g. `ai.tinyhumans.openhuman.Voice`.
        destination: String,
        /// The object path.
        path: String,
        /// The interface name.
        interface: String,
        /// The member to call.
        member: String,
        /// Positional arguments as a JSON array. Defaults to `[]`.
        #[arg(default_value = "[]")]
        args: String,
        /// Send the body confidentially: the bus refuses to deliver it unless
        /// it has verified the destination's artifact itself.
        #[arg(long)]
        confidential: bool,
    },

    /// Emit a signal.
    Emit {
        /// The object path the signal comes from.
        path: String,
        /// The interface name.
        interface: String,
        /// The member to emit.
        member: String,
        /// The body as a JSON array. Defaults to `[]`.
        #[arg(default_value = "[]")]
        args: String,
    },

    /// List every name currently owned on the bus.
    List,

    /// Print every message matching a rule, until interrupted.
    Monitor {
        /// A match rule, e.g. `type=signal,interface=ai.tinyhumans.openhuman.Mail`.
        #[arg(default_value = "type=signal")]
        rule: String,
    },

    /// Check that the bus is reachable and report what is on it.
    Doctor,

    /// Inspect and control trusted in-process modules.
    Modules {
        #[command(subcommand)]
        command: ModulesCommand,
    },
}

#[derive(Subcommand)]
enum ModulesCommand {
    /// List discovered modules.
    List {
        /// Keep only modules in this state.
        #[arg(long)]
        state: Option<String>,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Show one module and its manifest.
    Show {
        /// Stable module name.
        name: String,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Ask the running host to scan module directories.
    Scan {
        /// Directory to inspect. Repeat to scan several paths.
        #[arg(long = "path")]
        paths: Vec<PathBuf>,
        /// Report admissions without initializing or attaching modules.
        #[arg(long)]
        dry_run: bool,
    },
    /// Load one newly installed dynamic library.
    Load {
        /// Path to the `.so`, `.dylib`, or `.dll`.
        path: PathBuf,
        /// JSON file passed privately to setup; use `-` to read stdin.
        #[arg(long)]
        config_file: Option<PathBuf>,
    },
    /// Download, verify, extract, and load a GitHub release module.
    LoadGithub {
        /// GitHub release tag URL.
        release_url: String,
        /// Release archive asset name, usually ending in `.tar.gz`.
        asset: String,
        /// Expected SHA-256 for the release archive.
        sha256: String,
        /// JSON file passed privately to setup; use `-` to read stdin.
        #[arg(long)]
        config_file: Option<PathBuf>,
    },
    /// Apply replacement configuration to a running module.
    Reinitialize {
        /// Stable module name.
        name: String,
        /// JSON file passed privately to setup; use `-` to read stdin.
        #[arg(long)]
        config_file: Option<PathBuf>,
    },
    /// Generate a checksum.toml for release assets.
    Checksum {
        /// Release assets to hash. Repeat this option for multiple assets.
        #[arg(long = "path", required = true)]
        paths: Vec<PathBuf>,
        /// Write the manifest to a file instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Stop a loaded module without unloading its library.
    Stop {
        /// Stable module name.
        name: String,
        /// Milliseconds the host waits for the module to stop.
        #[arg(long, default_value_t = 5_000)]
        deadline_ms: u64,
    },
    /// Enable a known module for future scans.
    Enable {
        /// Stable module name.
        name: String,
    },
    /// Disable a known module for future scans.
    Disable {
        /// Stable module name.
        name: String,
    },
    /// Report stopped modules and toolchain mismatches.
    Doctor,
}

fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("TINYBUS_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    // Errors are printed with `Display`, not `Debug`. Returning `Result` from
    // `main` would print the derived `Debug` — `MethodFailed { name: … }` —
    // which is a worse first line of a bug report than the message the error
    // type was written to produce.
    match runtime().and_then(|rt| rt.block_on(run(cli))) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("tinybus: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

async fn run(cli: Cli) -> Result<()> {
    let address = resolve_address(cli.address)?;
    let timeout = Duration::from_secs(cli.timeout);

    match cli.command {
        Command::Serve => {
            let listener = UnixListenerAdapter::bind(&address).await?;
            let broker = Broker::new();
            // Serve and Ctrl-C race, and whichever wins ends the process. The
            // listener's Drop unlinks the socket either way, so the next start
            // does not trip over a leftover.
            tokio::select! {
                result = broker.serve(listener) => result,
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("interrupted; shutting down");
                    Ok(())
                }
            }
        }

        Command::Call {
            destination,
            path,
            interface,
            member,
            args,
            confidential,
        } => {
            let connection = connect(&address).await?;
            let args: serde_json::Value = serde_json::from_str(&args)?;
            let proxy = connection
                .proxy(&destination, &path, &interface)?
                .with_timeout(timeout);
            let reply: serde_json::Value = if confidential {
                proxy.call_confidential(&member, args).await?
            } else {
                proxy.call(&member, args).await?
            };
            println!("{}", serde_json::to_string_pretty(&reply)?);
            Ok(())
        }

        Command::Emit {
            path,
            interface,
            member,
            args,
        } => {
            let connection = connect(&address).await?;
            let args: serde_json::Value = serde_json::from_str(&args)?;
            connection
                .emit(
                    path.as_str().try_into()?,
                    interface.as_str().try_into()?,
                    member.as_str().try_into()?,
                    args,
                )
                .await
        }

        Command::List => {
            let connection = connect(&address).await?;
            for name in connection.list_names().await? {
                println!("{name}");
            }
            Ok(())
        }

        Command::Monitor { rule } => {
            let connection = connect(&address).await?;
            let mut signals = connection.add_match(MatchRule::parse(&rule)?).await?;
            eprintln!("watching {address:?} for `{rule}`; Ctrl-C to stop");
            loop {
                tokio::select! {
                    received = signals.recv() => match received {
                        Ok(message) => println!("{}", render(&message)),
                        // Lagging is worth saying out loud: the alternative is
                        // a monitor that quietly shows an incomplete picture.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            eprintln!("(dropped {n} messages: this monitor is behind)");
                        }
                        Err(_) => return Err(Error::ConnectionClosed),
                    },
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                }
            }
        }

        Command::Doctor => {
            let connection = connect(&address).await?;
            let bus = connection
                .proxy(tinybus::BUS_NAME, tinybus::BUS_PATH, tinybus::BUS_INTERFACE)?
                .with_timeout(timeout);
            let id: String = bus.call("GetId", ()).await?;
            let names = connection.list_names().await?;
            let (well_known, unique): (Vec<_>, Vec<_>) =
                names.into_iter().partition(|n| !n.is_unique());

            println!("socket    {}", address.display());
            println!("broker    {id}");
            println!("client    tinybus {}", tinybus::VERSION);
            println!("peers     {}", unique.len());
            println!("services  {}", well_known.len());
            for name in well_known {
                println!("          {name}");
            }
            Ok(())
        }

        Command::Modules { command } => run_modules(&address, timeout, command).await,
    }
}

async fn run_modules(address: &Path, timeout: Duration, command: ModulesCommand) -> Result<()> {
    if let ModulesCommand::Checksum {
        ref paths,
        ref output,
    } = command
    {
        return write_checksum_manifest(paths, output.as_deref());
    }
    let connection = connect(address).await?;
    let bus = connection
        .proxy(tinybus::BUS_NAME, tinybus::BUS_PATH, tinybus::BUS_INTERFACE)?
        .with_timeout(timeout);

    match command {
        ModulesCommand::List { state, json } => {
            let mut modules: Vec<serde_json::Value> = bus.call("ListModules", ()).await?;
            if let Some(state) = state {
                modules.retain(|module| module["state"].as_str() == Some(&state));
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&modules)?);
            } else {
                for module in modules {
                    println!(
                        "{:<24} {:<10} {}",
                        module["name"].as_str().unwrap_or("?"),
                        module["state"].as_str().unwrap_or("?"),
                        module["version"].as_str().unwrap_or("?")
                    );
                }
            }
            Ok(())
        }
        ModulesCommand::Show { name, json } => {
            let module: Option<serde_json::Value> = bus.call("GetModule", (name,)).await?;
            let module = module.ok_or_else(|| Error::failed("module is not known"))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&module)?);
            } else {
                println!("name      {}", module["name"].as_str().unwrap_or("?"));
                println!("version   {}", module["version"].as_str().unwrap_or("?"));
                println!("state     {}", module["state"].as_str().unwrap_or("?"));
                println!("artifact  {}", module["file"].as_str().unwrap_or("?"));
                println!(
                    "rustc     {}",
                    module["rustc_version"].as_str().unwrap_or("?")
                );
                println!("manifest  {}", serde_json::to_string(&module["manifest"])?);
            }
            Ok(())
        }
        ModulesCommand::Scan { paths, dry_run } => {
            let paths = paths
                .into_iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let modules: Vec<serde_json::Value> =
                bus.call("RescanModules", (paths, dry_run)).await?;
            println!("{}", serde_json::to_string_pretty(&modules)?);
            Ok(())
        }
        ModulesCommand::Load { path, config_file } => {
            let config = read_private_config(config_file.as_deref())?;
            let module: serde_json::Value = bus
                .call_sensitive("LoadModule", (path.to_string_lossy().to_string(), config))
                .await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::LoadGithub {
            release_url,
            asset,
            sha256,
            config_file,
        } => {
            let config = read_private_config(config_file.as_deref())?;
            let module: serde_json::Value = bus
                .call_sensitive("LoadGithubModule", (release_url, asset, sha256, config))
                .await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::Reinitialize { name, config_file } => {
            let config = read_private_config(config_file.as_deref())?;
            let module: serde_json::Value = bus
                .call_sensitive("ReinitializeModule", (name, config))
                .await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::Checksum { paths, output } => {
            write_checksum_manifest(&paths, output.as_deref())
        }
        ModulesCommand::Stop { name, deadline_ms } => {
            if Duration::from_millis(deadline_ms) >= timeout {
                return Err(Error::failed(
                    "module stop deadline must be shorter than the RPC timeout",
                ));
            }
            let module: serde_json::Value = bus.call("StopModule", (name, deadline_ms)).await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::Enable { name } => {
            let module: serde_json::Value = bus.call("EnableModule", (name, true)).await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::Disable { name } => {
            let module: serde_json::Value = bus.call("EnableModule", (name, false)).await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::Doctor => {
            let modules: Vec<serde_json::Value> = bus.call("ListModules", ()).await?;
            let mut problems = 0;
            for module in modules {
                let state = module["state"].as_str().unwrap_or("unknown");
                let mismatch = module["rustc_mismatch"].as_bool().unwrap_or(false);
                if !matches!(state, "ready" | "serving") || mismatch {
                    problems += 1;
                    println!(
                        "{}: state={state}, rustc_mismatch={mismatch}",
                        module["name"].as_str().unwrap_or("?")
                    );
                }
            }
            if problems == 0 {
                println!("modules are healthy");
            }
            Ok(())
        }
    }
}

fn read_private_config(path: Option<&Path>) -> Result<serde_json::Value> {
    use std::io::Read as _;

    let Some(path) = path else {
        return Ok(serde_json::json!({}));
    };
    let mut bytes = Vec::new();
    if path == Path::new("-") {
        std::io::stdin().read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(path)?.read_to_end(&mut bytes)?;
    }
    let bytes = tinybus::Secret::new(bytes);
    Ok(serde_json::from_slice(bytes.expose_secret())?)
}

fn write_checksum_manifest(paths: &[PathBuf], output: Option<&Path>) -> Result<()> {
    let mut manifest = String::from("[sha256]\n");
    for path in paths {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::failed("checksum path has no safe filename"))?;
        let digest = tinybus::module::sha256_file(path)?;
        manifest.push_str(&format!("{name:?} = \"{digest}\"\n"));
    }
    if let Some(output) = output {
        std::fs::write(output, manifest)?;
    } else {
        print!("{manifest}");
    }
    Ok(())
}

async fn connect(address: &Path) -> Result<Connection> {
    Connection::connect(Box::new(UnixTransport::connect(address).await?)).await
}

/// Work out where the bus lives: `--address`, then `$TINYBUS_ADDRESS` (which
/// clap already folded into `--address`), then the user's runtime directory.
///
/// `$XDG_RUNTIME_DIR` before `/tmp` on purpose — the socket is a capability
/// handle to every integration, and the runtime dir is the only one of the two
/// that is private to the user by default.
fn resolve_address(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        if !dir.is_empty() {
            return Ok(PathBuf::from(dir).join("tinybus").join("bus"));
        }
    }
    let uid = unsafe { libc_getuid() };
    Ok(PathBuf::from(format!("/tmp/tinybus-{uid}/bus")))
}

// The current uid, so the `/tmp` fallback is at least per-user. Declared here
// rather than taking a `libc` dependency for one call: the fallback path only
// exists for systems without `$XDG_RUNTIME_DIR`, and a whole crate in the graph
// for that is a poor trade in a project about dependency graphs.
unsafe extern "C" {
    #[link_name = "getuid"]
    fn libc_getuid() -> u32;
}

/// One line per message, for `monitor`.
fn render(message: &tinybus::Message) -> String {
    let h = &message.header;
    let kind = match h.kind {
        MessageKind::Signal => "signal",
        MessageKind::MethodCall => "call",
        MessageKind::MethodReturn => "return",
        MessageKind::Error => "error",
        _ => "?",
    };
    let sender = h.sender.as_ref().map(|s| s.to_string()).unwrap_or_default();
    let path = h.path.as_ref().map(|p| p.to_string()).unwrap_or_default();
    let interface = h
        .interface
        .as_ref()
        .map(|i| i.to_string())
        .unwrap_or_default();
    let member = h.member.as_ref().map(|m| m.to_string()).unwrap_or_default();
    // The monitor is a terminal, a scrollback buffer and often a pasted bug
    // report. A private body must not reach any of them, and the routing rules
    // mean one should never arrive here in the first place — so this is the
    // second lock on a door that is already shut.
    let body = if h.confidential || h.sensitive {
        "<confidential>".to_string()
    } else {
        message.body.to_string()
    };
    format!("{kind:<6} {sender:<10} {path} {interface}.{member} {body}")
}

#[cfg(test)]
#[path = "tinybus/tinybus_tests.rs"]
mod tests;
