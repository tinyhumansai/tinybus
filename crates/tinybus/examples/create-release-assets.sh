#!/usr/bin/env sh
set -eu

# Package already-built example cdylibs and publish the manifest as a release
# asset. Run from crates/tinybus after a release build on Linux or macOS.
# Usage: examples/create-release-assets.sh <output-directory> <module>...

output=${1:?output directory is required}
shift
mkdir -p "$output"

for module in "$@"; do
    case "$(uname -s)" in
        Darwin) extension=dylib; platform=macos;;
        Linux) extension=so; platform=linux;;
        *) echo "unsupported host; use the PowerShell helper on Windows" >&2; exit 1;;
    esac
    staging=$(mktemp -d)
    trap 'rm -rf "$staging"' EXIT HUP INT TERM
    cp "target/release/examples/lib${module}.${extension}" "$staging/${module}.${extension}"
    tar -czf "$output/${module}-${platform}.tar.gz" -C "$staging" "${module}.${extension}"
    rm -rf "$staging"
    trap - EXIT HUP INT TERM
done

(
    printf '%s\n' '[sha256]'
    for asset in "$output"/*.tar.gz; do
        printf '"%s" = "%s"\n' "$(basename "$asset")" "$(sha256sum "$asset" | awk '{print $1}')"
    done
) > "$output/checksum.toml"
