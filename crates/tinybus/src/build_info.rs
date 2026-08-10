//! Compile-time facts used to admit dynamically loaded modules.

/// The target triple Cargo built this copy of tinybus for.
pub const TARGET: &str = env!("TINYBUS_TARGET");

/// The rustc release that built this copy of tinybus.
pub const RUSTC_VERSION: &str = env!("TINYBUS_RUSTC_VERSION");

/// Feature bit for the Unix-domain-socket adapter.
pub const FEATURE_UDS: u64 = 1 << 0;
/// Feature bit for the interface proc macro.
pub const FEATURE_MACROS: u64 = 1 << 1;
/// Feature bit for the CLI.
pub const FEATURE_CLI: u64 = 1 << 2;
/// Feature bit for the module loader.
pub const FEATURE_MODULES: u64 = 1 << 3;

/// Features compiled into this copy of tinybus.
pub const FEATURE_BITS: u64 = (if cfg!(feature = "uds") {
    FEATURE_UDS
} else {
    0
}) | (if cfg!(feature = "macros") {
    FEATURE_MACROS
} else {
    0
}) | (if cfg!(feature = "cli") {
    FEATURE_CLI
} else {
    0
}) | (if cfg!(feature = "modules") {
    FEATURE_MODULES
} else {
    0
});

/// Render a known feature bit for admission diagnostics.
pub const fn feature_name(bit: u64) -> &'static str {
    match bit {
        FEATURE_UDS => "uds",
        FEATURE_MACROS => "macros",
        FEATURE_CLI => "cli",
        FEATURE_MODULES => "modules",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_known_feature_bit_has_a_stable_name() {
        assert_eq!(feature_name(FEATURE_UDS), "uds");
        assert_eq!(feature_name(FEATURE_MACROS), "macros");
        assert_eq!(feature_name(FEATURE_CLI), "cli");
        assert_eq!(feature_name(FEATURE_MODULES), "modules");
    }

    #[test]
    fn unknown_feature_bits_do_not_leak_an_untrusted_value() {
        assert_eq!(feature_name(1 << 63), "unknown");
    }
}
