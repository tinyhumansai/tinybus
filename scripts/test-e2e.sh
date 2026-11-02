#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo build --locked --example module_clock --example module_clock_two --example module_wrong_target --example module_context --example module_context_configured --example module_context_factory --example module_context_eager --example module_trusted_migration --example module_host_callback --example module_host_stream_callback --example module_controlled_init --example module_terminal --no-default-features --features uds,modules,macros,__native-fixture-support
cargo test --locked --no-default-features --features uds,modules,macros,test-support test_support::tests
case "${RUNNER_OS:-$(uname -s)}" in
  Windows|MINGW*|MSYS*|CYGWIN*) pwsh -NoProfile -File scripts/test-e2e.ps1 ;;
  *)
set -euo pipefail
target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_clock.so' -o -name 'libmodule_clock.dylib' \) -print -quit)
module_two=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_clock_two.so' -o -name 'libmodule_clock_two.dylib' \) -print -quit)
wrong_target=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_wrong_target.so' -o -name 'libmodule_wrong_target.dylib' \) -print -quit)
context_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_context.so' -o -name 'libmodule_context.dylib' \) -print -quit)
configured_context_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_context_configured.so' -o -name 'libmodule_context_configured.dylib' \) -print -quit)
context_factory_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_context_factory.so' -o -name 'libmodule_context_factory.dylib' \) -print -quit)
context_eager_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_context_eager.so' -o -name 'libmodule_context_eager.dylib' \) -print -quit)
trusted_migration_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_trusted_migration.so' -o -name 'libmodule_trusted_migration.dylib' \) -print -quit)
host_callback_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_host_callback.so' -o -name 'libmodule_host_callback.dylib' \) -print -quit)
host_stream_callback_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_host_stream_callback.so' -o -name 'libmodule_host_stream_callback.dylib' \) -print -quit)
controlled_init_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_controlled_init.so' -o -name 'libmodule_controlled_init.dylib' \) -print -quit)
terminal_module=$(find "$target_dir/debug/examples" -maxdepth 1 -type f \( -name 'libmodule_terminal.so' -o -name 'libmodule_terminal.dylib' \) -print -quit)
for artifact in "$module" "$module_two" "$wrong_target" "$context_module" "$configured_context_module" "$context_factory_module" "$context_eager_module" "$trusted_migration_module" "$host_callback_module" "$host_stream_callback_module" "$controlled_init_module" "$terminal_module"; do
  if [ -z "$artifact" ] || [ ! -f "$artifact" ]; then
    echo "a module example artifact is missing; the loader tests would not exercise it" >&2
    exit 1
  fi
done
private="$PWD/target/private-module-tests/module-clock-two"
mkdir -p "$private"
chmod 700 "$private"
cp "$module_two" "$private/"
library="$(basename "$module_two")"
if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$private/$library" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$private/$library" | awk '{print $1}')"
fi
printf '"%s" = "%s"\n' "$library" "$digest" > "$private/modules.toml"
# Terminal fixtures can retain uncertain owners; pin a private copy and run
# each SDK scenario in a fresh process so its retained state cannot leak.
terminal_private="$PWD/target/private-module-tests/module-terminal"
mkdir -p "$terminal_private"
chmod 700 "$terminal_private"
cp "$terminal_module" "$terminal_private/"
terminal_library="$(basename "$terminal_module")"
if command -v sha256sum >/dev/null 2>&1; then
  terminal_digest="$(sha256sum "$terminal_private/$terminal_library" | awk '{print $1}')"
else
  terminal_digest="$(shasum -a 256 "$terminal_private/$terminal_library" | awk '{print $1}')"
fi
printf '"%s" = "%s"\n' "$terminal_library" "$terminal_digest" > "$terminal_private/modules.toml"
export TINYBUS_TEST_TERMINAL_MODULE="$terminal_private/$terminal_library"
export TINYBUS_TEST_MODULE="$module" TINYBUS_TEST_MODULE_TWO="$private/$library" TINYBUS_TEST_WRONG_TARGET="$wrong_target" TINYBUS_TEST_CONTEXT_MODULE="$context_module" TINYBUS_TEST_CONFIGURED_CONTEXT_MODULE="$configured_context_module" TINYBUS_TEST_CONTEXT_FACTORY_MODULE="$context_factory_module" TINYBUS_TEST_CONTEXT_EAGER_MODULE="$context_eager_module" TINYBUS_TEST_MIGRATION_MODULE="$trusted_migration_module" TINYBUS_TEST_HOST_CALLBACK_MODULE="$host_callback_module" TINYBUS_TEST_HOST_STREAM_CALLBACK_MODULE="$host_stream_callback_module" TINYBUS_TEST_CONTROLLED_INIT_MODULE="$controlled_init_module"
cargo test --locked --no-default-features --workspace --features uds,modules,macros,test-support,__native-fixture-support -- --ignored --skip terminal_runtime_tests::compiled_native_

terminal_tests=$(cargo test --locked -p tinybus-module --features __native-fixture-support,tinybus/macros,tinybus/uds -- --list --ignored | sed -n 's/^\(terminal_runtime_tests::compiled_native_.*\): test$/\1/p')
[ -n "$terminal_tests" ] || { echo "no compiled terminal scenarios found" >&2; exit 1; }
while IFS= read -r test; do
  cargo test --locked -p tinybus-module --features __native-fixture-support,tinybus/macros,tinybus/uds "$test" -- --ignored --exact
done <<< "$terminal_tests"

cargo test --locked --no-default-features --features uds,modules,macros admission_tests -- --include-ignored
TINYBUS_TEST_UMASK=1 cargo test --locked --no-default-features --features uds,modules,macros permissive_umask -- --test-threads=1
    ;;
esac
