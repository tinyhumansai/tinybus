use super::*;
use std::path::PathBuf;

const FILE_ALL_ACCESS: u32 = 0x1F_01FF;
const READ_ONLY: u32 = 0x12_00A9;
const MODIFY: u32 = 0x13_01BF;
const ACCESS_DENIED_ACE_TYPE: u8 = 1;
const OBJECT_INHERIT_AND_CONTAINER_INHERIT: u8 = 0x03;

#[test]
fn an_untrusted_modify_grant_on_the_directory_is_refused() {
    // The report: Authenticated Users holding Modify on the cache directory.
    assert!(ace_grants_untrusted_write(0, 0, MODIFY, false));
    assert!(ace_grants_untrusted_write(
        0,
        OBJECT_INHERIT_AND_CONTAINER_INHERIT,
        FILE_ALL_ACCESS,
        false
    ));
}

#[test]
fn every_single_write_right_counts() {
    for bit in [
        0x2u32,
        0x4,
        0x10,
        0x40,
        0x100,
        0x1_0000,
        0x4_0000,
        0x8_0000,
        0x1000_0000,
        0x4000_0000,
    ] {
        assert!(ace_grants_untrusted_write(0, 0, bit, false), "{bit:#x}");
    }
}

#[test]
fn a_trusted_principal_may_write() {
    assert!(!ace_grants_untrusted_write(0, 0, FILE_ALL_ACCESS, true));
}

#[test]
fn read_only_grants_to_anyone_are_accepted() {
    assert!(!ace_grants_untrusted_write(0, 0, READ_ONLY, false));
}

#[test]
fn an_inherit_only_grant_does_not_apply_to_the_directory() {
    assert!(!ace_grants_untrusted_write(0, 0x08 | 0x03, MODIFY, false));
}

#[test]
fn deny_entries_never_count_as_a_grant() {
    assert!(!ace_grants_untrusted_write(
        ACCESS_DENIED_ACE_TYPE,
        0,
        MODIFY,
        false
    ));
}

#[test]
fn only_an_owned_real_directory_the_gate_refuses_is_repaired() {
    assert!(should_repair(true, true, true));
    assert!(!should_repair(false, true, true), "a link or file");
    assert!(!should_repair(true, false, true), "another account's");
    assert!(!should_repair(true, true, false), "already acceptable");
}

#[test]
fn the_sddl_is_protected_and_names_only_the_owner_and_system() {
    let sid = "S-1-5-21-1004336348-1177238915-682003330-1000";
    let sddl = owner_only_sddl(sid).unwrap();
    assert_eq!(sddl, format!("D:P(A;OICI;FA;;;{sid})(A;OICI;FA;;;SY)"));
    assert!(
        sddl.starts_with("D:P("),
        "inheritance from the parent is cut"
    );
    assert_eq!(sddl.matches("(A;").count(), 2);
}

#[test]
fn a_malformed_sid_never_reaches_the_sddl() {
    for bad in [
        "",
        "S-1-",
        "S-1-5-21-",
        "S-1-5-x",
        "S-1-5-1)(A;OICI;FA;;;WD",
        "S-1-5-1 ",
        "X-1-5-1",
        "S-1--5",
    ] {
        assert_eq!(owner_only_sddl(bad), None, "{bad:?}");
    }
    assert_eq!(
        owner_only_sddl(&format!("S-1-5-{}", "1-".repeat(100))),
        None
    );
}

#[test]
fn the_repair_covers_the_cache_tree_but_not_its_container() {
    let base = PathBuf::from("users/ana/appdata/local");
    let root = base.join("openhuman").join("modules");
    let cache = root.join("tinydocs").join("0.1.15").join("windows");
    assert!(in_repair_scope(&cache, &root, Some(&base)));
    assert!(in_repair_scope(&root, &root, Some(&base)));
    // `%LOCALAPPDATA%\openhuman`, strictly inside the base, is ours to fix.
    assert!(in_repair_scope(&base.join("openhuman"), &root, Some(&base)));
    // The base itself and everything above it are never rewritten.
    assert!(!in_repair_scope(&base, &root, Some(&base)));
    assert!(!in_repair_scope(base.parent().unwrap(), &root, Some(&base)));
}

#[test]
fn a_parent_component_never_counts_as_in_scope() {
    let base = PathBuf::from("users/ana/appdata/local");
    let root = base.join("openhuman");
    let escaping = root
        .join("cache")
        .join("..")
        .join("..")
        .join("..")
        .join("outside");
    assert!(!in_repair_scope(&escaping, &root, Some(&base)));
    assert!(!in_repair_scope(&root.join("..").join("x"), &root, None));
    // A root that itself contains `..` is not trusted either.
    let odd_root = base.join("x").join("..").join("openhuman");
    assert!(!in_repair_scope(
        &root.join("modules"),
        &odd_root,
        Some(&base)
    ));
    let odd_base = base.join("..").join("local");
    assert!(!in_repair_scope(
        &root.join("modules"),
        &root,
        Some(&odd_base)
    ));
}

#[test]
fn scope_comparison_ignores_case_and_requires_a_real_prefix() {
    let base = PathBuf::from("/users/ana/appdata/local");
    let root = base.join("openhuman");
    assert!(in_repair_scope(
        &PathBuf::from("/Users/ANA/AppData/Local/OpenHuman/modules"),
        &root,
        Some(&base)
    ));
    assert!(!in_repair_scope(
        &PathBuf::from("/users/ana/appdata/local2/openhuman"),
        &root,
        Some(&base)
    ));
    // Without a base, only the install root's own tree qualifies.
    assert!(!in_repair_scope(&base.join("other"), &root, None));
    assert!(in_repair_scope(&root.join("modules"), &root, None));
}

#[cfg(windows)]
fn icacls_grant(path: &Path, grant: &str) {
    let output = std::process::Command::new("icacls")
        .arg(path)
        .args(["/grant", grant])
        .output()
        .expect("icacls is installed on Windows");
    assert!(output.status.success(), "icacls failed: {output:?}");
}

#[cfg(windows)]
fn icacls_show(path: &Path) -> String {
    let output = std::process::Command::new("icacls")
        .arg(path)
        .output()
        .expect("icacls is installed on Windows");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[cfg(windows)]
use super::super::host::windows_path_grants_untrusted_write as refused;

#[cfg(windows)]
#[test]
fn a_directory_created_private_is_accepted_and_not_open_to_others() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("a").join("b").join("c");
    create_private_dir_all(&nested).unwrap();
    assert!(nested.is_dir());
    assert!(!refused(&nested).unwrap());
    // Idempotent on an existing tree.
    create_private_dir_all(&nested).unwrap();
}

#[cfg(windows)]
#[test]
fn a_cache_inheriting_a_group_write_grant_is_repaired() {
    let install = tempfile::tempdir().unwrap();
    icacls_grant(install.path(), "*S-1-5-11:(OI)(CI)(M)"); // Authenticated Users
    let cache = install
        .path()
        .join("tinydocs")
        .join("0.1.15")
        .join("windows");
    std::fs::create_dir_all(&cache).unwrap();
    assert!(refused(&cache).unwrap(), "the inherited grant is refused");

    secure_release_cache(install.path(), &cache);

    assert!(
        !refused(&cache).unwrap(),
        "the repaired cache is accepted; its ACL is now: {}",
        icacls_show(&cache)
    );
}

#[cfg(windows)]
#[test]
fn a_junction_in_the_cache_path_stops_the_repair() {
    let install = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    icacls_grant(outside.path(), "*S-1-5-11:(OI)(CI)(M)");
    let junction = install.path().join("linked");
    let made = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(&junction)
        .arg(outside.path())
        .output()
        .expect("cmd is available on Windows");
    assert!(made.status.success(), "mklink failed: {made:?}");
    let through = junction.join("cache");
    std::fs::create_dir_all(&through).unwrap();
    assert!(refused(&through).unwrap(), "the target inherits the grant");

    secure_release_cache(install.path(), &through);

    assert!(
        refused(&through).unwrap(),
        "nothing behind a junction is rewritten"
    );
    assert!(refused(outside.path()).unwrap());
    assert!(
        create_private_dir_all(&junction).is_err(),
        "a link is not a cache directory"
    );
}

#[cfg(windows)]
#[test]
fn a_directory_outside_the_install_root_is_left_alone() {
    let install = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    icacls_grant(other.path(), "*S-1-1-0:(OI)(CI)(M)");
    secure_release_cache(install.path(), other.path());
    assert!(refused(other.path()).unwrap(), "the gate still decides it");
}

#[cfg(not(windows))]
#[test]
fn off_windows_private_creation_is_plain_create_dir_all_and_repair_is_a_no_op() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("a").join("b");
    create_private_dir_all(&nested).unwrap();
    assert!(nested.is_dir());
    // Idempotent on an existing tree.
    create_private_dir_all(&nested).unwrap();
    secure_release_cache(root.path(), &nested);
    assert!(nested.is_dir());
}
