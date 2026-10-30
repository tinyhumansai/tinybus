use super::*;
use syn::parse::Parser;

fn args(source: &str) -> Punctuated<Meta, Comma> {
    Punctuated::<Meta, Comma>::parse_terminated
        .parse_str(source)
        .unwrap()
}

fn implementation(source: &str) -> ItemImpl {
    syn::parse_str(source).unwrap()
}

#[test]
fn snake_case_becomes_pascal_case() {
    assert_eq!(pascal_case("transcribe"), "Transcribe");
    assert_eq!(pascal_case("get_balance"), "GetBalance");
    assert_eq!(pascal_case("list_ssh_keys"), "ListSshKeys");
    // Already-Pascal names survive, so an `impl` written to match an
    // existing contract does not need an override on every method.
    assert_eq!(pascal_case("Transcribe"), "Transcribe");
}

#[test]
fn a_leading_underscore_does_not_produce_an_empty_member() {
    assert_eq!(pascal_case("_internal"), "Internal");
}

#[test]
fn expansion_generates_dispatch_members_and_strips_helper_attributes() {
    let expanded = expand(
        args("name = \"ai.tinyhumans.Example\""),
        implementation(
            r#"
                impl Example {
                    #[tinybus(name = "Ping")]
                    async fn ping(&self, value: u32) -> tinybus::Result<u32> { Ok(value) }
                    #[tinybus(skip)]
                    async fn private(&self) -> tinybus::Result<()> { Ok(()) }
                    async fn zero(&self) -> tinybus::Result<()> { Ok(()) }
                    fn constructor() -> Self { Self }
                }
                "#,
        ),
    )
    .unwrap()
    .to_string();

    assert!(expanded.contains("impl :: tinybus :: service :: Interface for Example"));
    assert!(expanded.contains("Ping"));
    assert!(expanded.contains("Zero"));
    let parsed: syn::File = syn::parse_str(&expanded).unwrap();
    assert!(parsed.items.iter().all(|item| {
        let syn::Item::Impl(item) = item else {
            return true;
        };
        item.items.iter().all(|item| {
            let syn::ImplItem::Fn(method) = item else {
                return true;
            };
            method
                .attrs
                .iter()
                .all(|attr| !attr.path().is_ident("tinybus"))
        })
    }));
}

#[test]
fn expansion_skips_nonmethods_and_tracks_confidential_wildcard_arguments() {
    let expanded = expand(
        args("name = PATH, name = 7, name = \"ai.tinyhumans.Example\""),
        implementation(
            r#"
                impl Example {
                    const VERSION: u32 = 1;
                    #[allow(dead_code)]
                    fn constructor() -> Self { Self }
                    #[tinybus(confidential)]
                    async fn inspect(&self, _: u32) -> tinybus::Result<()> { Ok(()) }
                }
                "#,
        ),
    )
    .unwrap()
    .to_string();

    assert!(expanded.contains("__arg0"));
    assert!(expanded.contains("matches"));
    assert!(expanded.contains("Inspect"));
}

#[test]
fn expansion_rejects_invalid_interface_method_signatures() {
    let name = args("name = \"ai.tinyhumans.Example\"");
    for source in [
        "impl Example { fn blocking(&self) -> tinybus::Result<()> { Ok(()) } }",
        "impl Example { async fn missing_result(&self) {} }",
        "impl Example { async fn destructure(&self, (a, b): (u32, u32)) -> tinybus::Result<()> { Ok(()) } }",
    ] {
        assert!(
            expand(name.clone(), implementation(source)).is_err(),
            "{source}"
        );
    }
}

#[test]
fn attributes_and_interface_names_are_validated() {
    assert_eq!(
        interface_name(&args("name = \"ai.tinyhumans.Example\"")).unwrap(),
        "ai.tinyhumans.Example"
    );
    assert!(interface_name(&args("other = \"value\"")).is_err());
    assert_eq!(
        interface_name(&args(
            "name = PATH, name = 7, name = \"ai.tinyhumans.Valid\""
        ))
        .unwrap(),
        "ai.tinyhumans.Valid"
    );

    let declared = implementation(
        "impl Example { #[tinybus(skip, confidential, name = \"WireName\")] async fn call(&self) -> tinybus::Result<()> { Ok(()) } }",
    );
    let method = match &declared.items[0] {
        ImplItem::Fn(method) => method,
        _ => unreachable!(),
    };
    let attributes = method_attrs(method).unwrap();
    assert!(attributes.skip);
    assert!(attributes.confidential);
    assert_eq!(attributes.name.as_deref(), Some("WireName"));

    let invalid_declared = implementation(
        "impl Example { #[tinybus(unknown)] async fn call(&self) -> tinybus::Result<()> { Ok(()) } }",
    );
    let invalid = match &invalid_declared.items[0] {
        ImplItem::Fn(method) => method,
        _ => unreachable!(),
    };
    assert!(method_attrs(invalid).is_err());
}
