use super::*;

static ASSETS: [ReleaseAsset<'static>; 2] = [
    ReleaseAsset {
        host_key: "ubuntu-24.04-x86_64",
        archive: "tinybus-module-linux-x86_64.tar.gz",
        sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        library_sha256: None,
    },
    ReleaseAsset {
        host_key: "windows-2025-x86_64",
        archive: "tinybus-module-windows-x86_64.zip",
        sha256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        library_sha256: Some("123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"),
    },
];

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "release-lookup",
    bus_name: "ai.tinyhumans.test.ReleaseLookup",
    object_path: "/ai/tinyhumans/test/ReleaseLookup",
    version: "1.2.3",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v1.2.3",
    assets: &ASSETS,
};

#[test]
fn an_asset_is_selected_only_for_its_exact_host_key() {
    let asset = RECORD.asset_for("ubuntu-24.04-x86_64");
    assert_eq!(asset, Some(&ASSETS[0]));
    assert_eq!(asset.map(|asset| asset.archive), Some(ASSETS[0].archive));
    assert_eq!(asset.map(|asset| asset.sha256), Some(ASSETS[0].sha256));
    assert_eq!(RECORD.asset_for(ASSETS[1].host_key), Some(&ASSETS[1]));
    assert!(RECORD.asset_for("ubuntu-22.04-x86_64").is_none());
    assert!(RECORD.asset_for("unknown-host").is_none());
}
