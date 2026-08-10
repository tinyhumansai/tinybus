//! Records build facts that participate in the module ABI gate.

use std::process::Command;

fn main() {
    println!("cargo::rerun-if-env-changed=TARGET");
    let target = std::env::var("TARGET").expect("Cargo always sets TARGET");
    println!("cargo::rustc-env=TINYBUS_TARGET={target}");

    let output = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .arg("-vV")
        .output()
        .expect("rustc -vV must run while building tinybus");
    let text = String::from_utf8(output.stdout).expect("rustc -vV is UTF-8");
    let release = text
        .lines()
        .find_map(|line| line.strip_prefix("release: "))
        .expect("rustc -vV contains a release line");
    println!("cargo::rustc-env=TINYBUS_RUSTC_VERSION={release}");
}
