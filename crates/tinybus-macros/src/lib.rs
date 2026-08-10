//! `#[tinybus::interface]` — the attribute that turns an `impl` block into a
//! dispatchable bus interface.
//!
//! # What it generates, and why by hand rather than by trait
//!
//! `tinybus::service::Interface` has to be object-safe: one connection holds
//! a heterogeneous list of interfaces behind `dyn`, so its `call` takes and
//! returns `serde_json::Value`. Writing that dispatch by hand for every service
//! means a `match` on a string, a positional deserialize, and a serialize —
//! three places per method for a typo to hide, and a fourth if the member list
//! for introspection drifts from the `match`. The macro derives all four from
//! one typed signature, so they cannot disagree.
//!
//! ```text
//! struct Voice { model: WhisperModel }
//!
//! #[tinybus::interface(name = "ai.tinyhumans.openhuman.Voice")]
//! impl Voice {
//!     async fn transcribe(&self, path: String) -> tinybus::Result<String> { … }
//!     async fn languages(&self) -> tinybus::Result<Vec<String>> { … }
//! }
//! ```
//!
//! generates `Transcribe` and `Languages` as members, deserializing a
//! positional JSON array into each method's parameters.
//!
//! # The conventions, stated once
//!
//! - **`snake_case` becomes `PascalCase`.** `get_balance` is `GetBalance` on
//!   the wire. Rust code stays idiomatic Rust; the wire stays conventional for
//!   a bus. Override with `#[tinybus(name = "…")]` on the method when a member
//!   has to match an existing contract exactly.
//! - **Methods must be `async fn` taking `&self`** and returning
//!   `tinybus::Result<T>` where `T: Serialize`. Not a stylistic preference: a
//!   `&mut self` method would need exclusive access on a shared `Arc<dyn
//!   Interface>`, which would serialise every call to the service.
//! - **`#[tinybus(skip)]`** leaves a method off the bus entirely — helpers on
//!   the same `impl` block do not have to move elsewhere to stay private.
//! - **Arguments are positional.** Parameter names are a detail of the Rust
//!   signature and renaming one must not break a caller.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Error, FnArg, ImplItem, ItemImpl, LitStr, Meta, Pat, ReturnType, parse_macro_input,
    punctuated::Punctuated, token::Comma,
};

/// Turn an `impl` block into a `tinybus::service::Interface` implementation.
///
/// Takes the interface name: `#[interface(name = "ai.tinyhumans.Example")]`.
#[proc_macro_attribute]
pub fn interface(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let input = parse_macro_input!(item as ItemImpl);

    match expand(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(
    args: Punctuated<Meta, Comma>,
    mut input: ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let interface_name = interface_name(&args)?;
    let self_ty = input.self_ty.clone();

    let mut members = Vec::new();
    let mut arms = Vec::new();

    for item in &input.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let attrs = method_attrs(method)?;
        if attrs.skip {
            continue;
        }
        // A method that takes no receiver is an associated function; there is
        // no instance to dispatch it against, so it is silently not a member
        // rather than an error — constructors live on these `impl` blocks too.
        if !matches!(method.sig.inputs.first(), Some(FnArg::Receiver(_))) {
            continue;
        }
        if method.sig.asyncness.is_none() {
            return Err(Error::new_spanned(
                &method.sig,
                "tinybus interface methods must be `async fn`; a blocking method would stall \
                 the connection's dispatch task for every other caller",
            ));
        }
        if matches!(method.sig.output, ReturnType::Default) {
            return Err(Error::new_spanned(
                &method.sig,
                "tinybus interface methods must return `tinybus::Result<T>` so a failure can \
                 become an error reply rather than a successful-looking empty one",
            ));
        }

        let ident = &method.sig.ident;
        let member = attrs
            .name
            .unwrap_or_else(|| pascal_case(&ident.to_string()));

        let mut types = Vec::new();
        let mut binds = Vec::new();
        for (index, arg) in method.sig.inputs.iter().skip(1).enumerate() {
            let FnArg::Typed(typed) = arg else {
                return Err(Error::new_spanned(arg, "unexpected receiver"));
            };
            if matches!(&*typed.pat, Pat::Wild(_)) {
                // A `_`-named parameter still occupies a wire position, so it
                // needs a binding — just one nothing refers to.
                binds.push(format_ident!("__arg{}", index));
            } else if let Pat::Ident(pat) = &*typed.pat {
                binds.push(pat.ident.clone());
            } else {
                return Err(Error::new_spanned(
                    &typed.pat,
                    "tinybus interface methods need plain parameter names; destructuring \
                     patterns have no stable position on the wire",
                ));
            }
            types.push(&typed.ty);
        }

        // The zero-argument case is spelled out because `()` deserializes from
        // JSON `null`, not from `[]`, and every caller sends `[]`.
        let decode = if binds.is_empty() {
            quote! {}
        } else {
            quote! {
                let (#(#binds,)*): (#(#types,)*) = ::tinybus::__private::decode_args(
                    __member, __args,
                )?;
            }
        };

        members.push(member.clone());
        arms.push(quote! {
            #member => {
                #decode
                let __value = self.#ident(#(#binds),*).await?;
                ::tinybus::__private::encode_reply(&__value)
            }
        });
    }

    let member_literals = members.iter();

    // `#[tinybus(…)]` on a method is an inert helper this macro consumes.
    // Attribute macros cannot register helper attributes the way derives can,
    // so leaving them on the re-emitted block would be an "attribute not found"
    // error at the user's method rather than a working annotation.
    for item in &mut input.items {
        if let ImplItem::Fn(method) = item {
            method.attrs.retain(|attr| !attr.path().is_ident("tinybus"));
        }
    }

    Ok(quote! {
        #input

        #[::tinybus::__private::async_trait]
        impl ::tinybus::service::Interface for #self_ty {
            fn name(&self) -> ::tinybus::name::InterfaceName {
                ::tinybus::__private::parse_interface(#interface_name)
            }

            fn members(&self) -> ::std::vec::Vec<::tinybus::name::MemberName> {
                ::std::vec![
                    #(::tinybus::__private::parse_member(#member_literals)),*
                ]
            }

            async fn call(
                &self,
                __member: &::tinybus::name::MemberName,
                __args: ::tinybus::__private::Value,
            ) -> ::tinybus::Result<::tinybus::__private::Value> {
                match __member.as_str() {
                    #(#arms)*
                    __other => ::std::result::Result::Err(
                        ::tinybus::__private::unknown_method(#interface_name, __other),
                    ),
                }
            }
        }
    })
}

/// Read `name = "…"` off the attribute.
fn interface_name(args: &Punctuated<Meta, Comma>) -> syn::Result<String> {
    for arg in args {
        if let Meta::NameValue(nv) = arg
            && nv.path.is_ident("name")
            && let syn::Expr::Lit(lit) = &nv.value
            && let syn::Lit::Str(s) = &lit.lit
        {
            return Ok(s.value());
        }
    }
    Err(Error::new(
        proc_macro2::Span::call_site(),
        "#[interface] needs the interface name: \
         #[interface(name = \"ai.tinyhumans.example.Thing\")]",
    ))
}

#[derive(Default)]
struct MethodAttrs {
    skip: bool,
    name: Option<String>,
}

/// Read the `#[tinybus(…)]` helper attributes off one method.
fn method_attrs(method: &syn::ImplItemFn) -> syn::Result<MethodAttrs> {
    let mut out = MethodAttrs::default();
    for attr in &method.attrs {
        if !attr.path().is_ident("tinybus") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                out.skip = true;
                Ok(())
            } else if meta.path.is_ident("name") {
                let value: LitStr = meta.value()?.parse()?;
                out.name = Some(value.value());
                Ok(())
            } else {
                Err(meta.error("unknown tinybus attribute; expected `skip` or `name = \"…\"`"))
            }
        })?;
    }
    Ok(out)
}

/// `get_balance` → `GetBalance`.
fn pascal_case(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut upper = true;
    for c in input.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
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
        assert!(!expanded.contains("# [ tinybus"));
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
            interface_name(&args("name = \"ai.tinyhumans.Example\"")),
            "ai.tinyhumans.Example"
        );
        assert!(interface_name(&args("other = \"value\"")).is_err());

        let method = match &implementation(
            "impl Example { #[tinybus(skip, name = \"WireName\")] async fn call(&self) -> tinybus::Result<()> { Ok(()) } }",
        ).items[0] {
            ImplItem::Fn(method) => method,
            _ => unreachable!(),
        };
        let attributes = method_attrs(method).unwrap();
        assert!(attributes.skip);
        assert_eq!(attributes.name.as_deref(), Some("WireName"));

        let invalid = match &implementation(
            "impl Example { #[tinybus(unknown)] async fn call(&self) -> tinybus::Result<()> { Ok(()) } }",
        ).items[0] {
            ImplItem::Fn(method) => method,
            _ => unreachable!(),
        };
        assert!(method_attrs(invalid).is_err());
    }
}
