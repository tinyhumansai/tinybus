#!/usr/bin/env sh
set -eu

# Package already-built example cdylibs and publish the manifest as a release
# asset. Run from crates/tinybus after a release build.
# Usage: examples/create-release-assets.sh <output-directory> <module>...

output=${1:?output directory is required}
shift
mkdir -p "$output"

for module in "$@"; do
    staging=$(mktemp -d)
    trap 'rm -rf "$staging"' EXIT HUP INT TERM
    cp "target/release/examples/lib${module}.so" "$staging/${module}.so"
    tar -czf "$output/${module}.tar.gz" -C "$staging" "${module}.so"
    rm -rf "$staging"
    trap - EXIT HUP INT TERM
done

(
    printf '%s\n' '[sha256]'
    for asset in "$output"/*.tar.gz; do
        printf '"%s" = "%s"\n' "$(basename "$asset")" "$(sha256sum "$asset" | awk '{print $1}')"
    done
) > "$output/checksum.toml"
