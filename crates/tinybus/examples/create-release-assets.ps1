$ErrorActionPreference = "Stop"

# Package already-built example DLLs and publish checksum.toml.
# Usage: .\examples\create-release-assets.ps1 <output-directory> <module>...

param(
    [Parameter(Mandatory = $true, Position = 0)] [string] $Output,
    [Parameter(Mandatory = $true, Position = 1)] [string[]] $Module
)

New-Item -ItemType Directory -Force $Output | Out-Null
foreach ($Name in $Module) {
    $staging = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid())
    New-Item -ItemType Directory -Force $staging | Out-Null
    try {
        Copy-Item "target/release/examples/$Name.dll" (Join-Path $staging "$Name.dll")
        Compress-Archive -Path (Join-Path $staging "$Name.dll") -DestinationPath (Join-Path $Output "$Name-windows.zip") -Force
    } finally {
        Remove-Item $staging -Recurse -Force
    }
}

$lines = @('[sha256]')
Get-ChildItem (Join-Path $Output '*.zip') | ForEach-Object {
    $hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $lines += ('"{0}" = "{1}"' -f $_.Name, $hash)
}
Set-Content (Join-Path $Output 'checksum.toml') $lines
