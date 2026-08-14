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
        /// JSON object passed to the module's setup function.
        #[arg(long, default_value = "{}")]
        config: String,
    },
    /// Download, verify, extract, and load a GitHub release module.
    LoadGithub {
        /// GitHub release tag URL.
        release_url: String,
        /// Release archive asset name, usually ending in `.tar.gz`.
        asset: String,
        /// Expected SHA-256 for the release archive.
        sha256: String,
        /// JSON object passed to the module's setup function.
        #[arg(long, default_value = "{}")]
        config: String,
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
        ModulesCommand::Load { path, config } => {
            let config: serde_json::Value = serde_json::from_str(&config)?;
            let module: serde_json::Value = bus
                .call("LoadModule", (path.to_string_lossy().to_string(), config))
                .await?;
            println!("{}", serde_json::to_string_pretty(&module)?);
            Ok(())
        }
        ModulesCommand::LoadGithub {
            release_url,
            asset,
            sha256,
            config,
        } => {
            let config: serde_json::Value = serde_json::from_str(&config)?;
            let module: serde_json::Value = bus
                .call("LoadGithubModule", (release_url, asset, sha256, config))
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
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR")
        && !dir.is_empty()
    {
        return Ok(PathBuf::from(dir).join("tinybus").join("bus"));
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
    // report. A confidential body must not reach any of them, and the routing
    // rules mean one should never arrive here in the first place — so this is
    // the second lock on a door that is already shut.
    let body = if h.confidential {
        "<confidential>".to_string()
    } else {
        message.body.to_string()
    };
    format!("{kind:<6} {sender:<10} {path} {interface}.{member} {body}")
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use clap::Parser;
    use serde_json::Value;

    use super::*;
    use tinybus::module::ModuleHost;
    use tinybus::name::{BusName, InterfaceName, MemberName, ObjectPath};
    use tinybus::service::Interface;

    const DESTINATION: &str = "ai.tinyhumans.openhuman.Echo";
    const PATH: &str = "/ai/tinyhumans/openhuman/Echo";
    const INTERFACE: &str = "ai.tinyhumans.openhuman.Echo";

    struct Echo;

    #[async_trait]
    impl Interface for Echo {
        fn name(&self) -> InterfaceName {
            InterfaceName::new(INTERFACE).unwrap()
        }

        fn members(&self) -> Vec<MemberName> {
            vec![MemberName::new("Echo").unwrap()]
        }

        async fn call(&self, _member: &MemberName, args: Value) -> tinybus::Result<Value> {
            Ok(args)
        }
    }

    async fn broker_and_service() -> (tempfile::TempDir, PathBuf, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let address = dir.path().join("bus");
        let listener = UnixListenerAdapter::bind(&address).await.unwrap();
        Broker::new().spawn(listener);
        let service =
            Connection::connect(Box::new(UnixTransport::connect(&address).await.unwrap()))
                .await
                .unwrap();
        service.request_name(DESTINATION).await.unwrap();
        service
            .serve_at(ObjectPath::new(PATH).unwrap(), Echo)
            .await
            .unwrap();
        (dir, address, service)
    }

    async fn broker_with_module_host() -> (tempfile::TempDir, PathBuf, ModuleHost) {
        let dir = tempfile::tempdir().unwrap();
        let address = dir.path().join("bus");
        let listener = UnixListenerAdapter::bind(&address).await.unwrap();
        let broker = Broker::new();
        // The broker retains only a weak module control, so callers must keep
        // this host bound while they issue module commands.
        let host = ModuleHost::new(broker.clone());
        broker.spawn(listener);
        (dir, address, host)
    }

    #[test]
    fn the_cli_parses_its_declared_commands_and_uses_explicit_addresses() {
        let cli = Cli::try_parse_from([
            "tinybus",
            "--address",
            "/run/user/1000/tinybus/bus",
            "--timeout",
            "7",
            "call",
            DESTINATION,
            PATH,
            INTERFACE,
            "Echo",
            "[1]",
        ])
        .unwrap();
        assert_eq!(cli.timeout, 7);
        assert_eq!(
            resolve_address(cli.address).unwrap(),
            PathBuf::from("/run/user/1000/tinybus/bus")
        );
        assert!(matches!(cli.command, Command::Call { args, .. } if args == "[1]"));
        assert!(matches!(
            Cli::try_parse_from(["tinybus", "serve"]).unwrap().command,
            Command::Serve
        ));
        assert!(matches!(
            Cli::try_parse_from(["tinybus", "list"]).unwrap().command,
            Command::List
        ));
        assert!(matches!(
            Cli::try_parse_from(["tinybus", "doctor"]).unwrap().command,
            Command::Doctor
        ));
        assert!(matches!(
            Cli::try_parse_from(["tinybus", "emit", PATH, INTERFACE, "Changed"])
                .unwrap()
                .command,
            Command::Emit { .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["tinybus", "monitor"]).unwrap().command,
            Command::Monitor { .. }
        ));
    }

    #[test]
    fn runtime_and_monitor_rendering_are_usable() {
        assert!(runtime().is_ok());
        let call = tinybus::Message::method_call(
            BusName::new(DESTINATION).unwrap(),
            ObjectPath::new(PATH).unwrap(),
            InterfaceName::new(INTERFACE).unwrap(),
            MemberName::new("Echo").unwrap(),
            serde_json::json!(["hello"]),
        );
        assert!(render(&call).starts_with("call"));
        let signal = tinybus::Message::signal(
            ObjectPath::new(PATH).unwrap(),
            InterfaceName::new(INTERFACE).unwrap(),
            MemberName::new("Changed").unwrap(),
            serde_json::json!([]),
        );
        assert!(render(&signal).starts_with("signal"));
        assert!(
            render(&tinybus::Message::method_return(&call.header, Value::Null))
                .starts_with("return")
        );
        assert!(
            render(&tinybus::Message::error_reply(
                &call.header,
                &Error::ConnectionClosed
            ))
            .starts_with("error")
        );
    }

    #[tokio::test]
    async fn call_emit_list_and_doctor_use_a_running_broker() {
        let (_dir, address, service) = broker_and_service().await;

        run(Cli {
            address: Some(address.clone()),
            timeout: 1,
            command: Command::Call {
                confidential: false,
                destination: DESTINATION.into(),
                path: PATH.into(),
                interface: INTERFACE.into(),
                member: "Echo".into(),
                args: "[\"hello\"]".into(),
            },
        })
        .await
        .unwrap();
        run(Cli {
            address: Some(address.clone()),
            timeout: 1,
            command: Command::Emit {
                path: PATH.into(),
                interface: INTERFACE.into(),
                member: "Changed".into(),
                args: "[]".into(),
            },
        })
        .await
        .unwrap();
        run(Cli {
            address: Some(address.clone()),
            timeout: 1,
            command: Command::List,
        })
        .await
        .unwrap();
        run(Cli {
            address: Some(address),
            timeout: 1,
            command: Command::Doctor,
        })
        .await
        .unwrap();
        assert!(service.unique_name().is_some());
    }

    #[tokio::test]
    async fn module_commands_report_errors_and_valid_commands_succeed() {
        let (_dir, address, _host) = broker_with_module_host().await;
        let commands = [
            ModulesCommand::Show {
                name: "missing".into(),
                json: false,
            },
            ModulesCommand::Scan {
                paths: vec![PathBuf::from("/not/a/module")],
                dry_run: true,
            },
            ModulesCommand::Load {
                path: PathBuf::from("/not/a/module"),
                config: "{}".into(),
            },
            ModulesCommand::LoadGithub {
                release_url: "https://example.com/not-github".into(),
                asset: "module.tar.gz".into(),
                sha256: "0".repeat(64),
                config: "{}".into(),
            },
            ModulesCommand::Stop {
                name: "missing".into(),
                deadline_ms: 1_000,
            },
            ModulesCommand::Enable {
                name: "missing".into(),
            },
            ModulesCommand::Disable {
                name: "missing".into(),
            },
        ];
        for command in commands {
            assert!(
                run_modules(&address, Duration::from_secs(2), command)
                    .await
                    .is_err()
            );
        }

        run_modules(
            &address,
            Duration::from_secs(2),
            ModulesCommand::List {
                state: Some("ready".into()),
                json: true,
            },
        )
        .await
        .unwrap();

        let asset = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(asset.path(), b"release asset").unwrap();
        let manifest = tempfile::NamedTempFile::new().unwrap();
        run_modules(
            &address,
            Duration::from_secs(2),
            ModulesCommand::Checksum {
                paths: vec![asset.path().to_path_buf()],
                output: Some(manifest.path().to_path_buf()),
            },
        )
        .await
        .unwrap();
        assert!(
            std::fs::read_to_string(manifest.path())
                .unwrap()
                .contains("[sha256]")
        );
        run_modules(&address, Duration::from_secs(2), ModulesCommand::Doctor)
            .await
            .unwrap();
        run_modules(
            &address,
            Duration::from_secs(2),
            ModulesCommand::Scan {
                paths: Vec::new(),
                dry_run: true,
            },
        )
        .await
        .unwrap();

        let (_dir, address, _service) = broker_and_service().await;
        for command in [
            Command::Call {
                destination: DESTINATION.into(),
                path: PATH.into(),
                interface: INTERFACE.into(),
                member: "Echo".into(),
                args: "not json".into(),
                confidential: false,
            },
            Command::Emit {
                path: PATH.into(),
                interface: INTERFACE.into(),
                member: "Changed".into(),
                args: "not json".into(),
            },
        ] {
            assert!(
                run(Cli {
                    address: Some(address.clone()),
                    timeout: 1,
                    command,
                })
                .await
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn a_module_stop_deadline_cannot_outlive_the_call_deadline() {
        let (_dir, address, _service) = broker_and_service().await;
        let error = run_modules(
            &address,
            Duration::from_millis(10),
            ModulesCommand::Stop {
                name: "missing".into(),
                deadline_ms: 10,
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("shorter than the RPC timeout"));
    }

    #[tokio::test]
    #[ignore = "requires TINYBUS_TEST_MODULE to point at the built cdylib"]
    async fn module_cli_controls_a_real_dynamic_module_lifecycle() {
        let path = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
        let (_dir, address, _host) = broker_with_module_host().await;
        let timeout = Duration::from_secs(2);

        run_modules(
            &address,
            timeout,
            ModulesCommand::Load {
                path,
                config: r#"{"prefix":"cli:"}"#.into(),
            },
        )
        .await
        .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::List {
                state: None,
                json: true,
            },
        )
        .await
        .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::List {
                state: None,
                json: false,
            },
        )
        .await
        .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::Show {
                name: "tinybus".into(),
                json: false,
            },
        )
        .await
        .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::Show {
                name: "tinybus".into(),
                json: true,
            },
        )
        .await
        .unwrap();
        run_modules(&address, timeout, ModulesCommand::Doctor)
            .await
            .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::Enable {
                name: "tinybus".into(),
            },
        )
        .await
        .unwrap();
        run_modules(
            &address,
            timeout,
            ModulesCommand::Stop {
                name: "tinybus".into(),
                deadline_ms: 500,
            },
        )
        .await
        .unwrap();
        assert!(
            run_modules(
                &address,
                timeout,
                ModulesCommand::Disable {
                    name: "tinybus".into(),
                },
            )
            .await
            .is_err()
        );
    }
}
