#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
case "${RUNNER_OS:-$(uname -s)}" in
  Linux|Darwin|macOS)
    python3 scripts/test_check_coverage.py
    ;;
  Windows|MINGW*|MSYS*|CYGWIN*) ;;
esac
cargo check --locked --workspace --no-default-features
# Compile TinyBus' own default-feature test targets without workspace feature
# unification, so dev-only fixture dependencies select their own backends.
cargo check --locked --package tinybus --tests
cargo check --locked --package tinybus --no-default-features --features modules
cargo metadata --locked --format-version 1 --manifest-path examples/Cargo.toml >/dev/null
cargo test --locked --workspace --no-default-features --features macros
cargo test --locked --workspace --all-features
