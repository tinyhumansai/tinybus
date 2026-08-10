//! Pure manifest dependency resolution and deterministic load ordering.

use std::collections::{HashMap, HashSet};

use crate::module::manifest::ModuleManifest;

/// Result indexes refer to the input manifest slice.
pub(crate) struct Resolution {
    pub(crate) order: Vec<usize>,
    pub(crate) unresolved: Vec<(usize, &'static str)>,
}

/// Reject collisions, ignore absent optional dependencies, and topologically
/// order every remaining manifest.
pub(crate) fn resolve(
    manifests: &[ModuleManifest],
    initially_available: &HashSet<String>,
) -> Resolution {
    let duplicate_modules = duplicates(manifests.iter().map(|manifest| &manifest.module.name));
    let duplicate_bus_names = duplicates(manifests.iter().map(|manifest| &manifest.bus_name));
    let mut unresolved = Vec::new();
    let mut pending = Vec::new();
    for (index, manifest) in manifests.iter().enumerate() {
        if duplicate_modules.contains(&manifest.module.name) {
            unresolved.push((index, "two artifacts declare the same module name"));
        } else if duplicate_bus_names.contains(&manifest.bus_name) {
            unresolved.push((index, "two modules claim the same bus name"));
        } else {
            pending.push(index);
        }
    }

    let declared = pending
        .iter()
        .flat_map(|index| manifests[*index].provides.iter())
        .map(|provided| provided.version.interface.to_string())
        .collect::<HashSet<_>>();
    let mut available = initially_available.clone();
    let mut order = Vec::new();
    while !pending.is_empty() {
        let ready = pending.iter().position(|index| {
            manifests[*index]
                .requires
                .iter()
                .filter(|dependency| !dependency.optional)
                .all(|dependency| available.contains(dependency.interface.interface.as_str()))
        });
        if let Some(position) = ready {
            let index = pending.remove(position);
            available.extend(
                manifests[index]
                    .provides
                    .iter()
                    .map(|provided| provided.version.interface.to_string()),
            );
            order.push(index);
            continue;
        }

        for index in pending.drain(..) {
            let missing = manifests[index]
                .requires
                .iter()
                .filter(|dependency| !dependency.optional)
                .any(|dependency| {
                    !available.contains(dependency.interface.interface.as_str())
                        && !declared.contains(dependency.interface.interface.as_str())
                });
            unresolved.push((
                index,
                if missing {
                    "a required interface has no provider"
                } else {
                    "module dependency cycle detected"
                },
            ));
        }
    }
    Resolution { order, unresolved }
}

fn duplicates<T: Eq + std::hash::Hash>(values: impl Iterator<Item = T>) -> HashSet<T> {
    let mut counts = HashMap::new();
    for value in values {
        *counts.entry(value).or_insert(0usize) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::manifest::{
        Dependency, MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy, ProvidedInterface,
    };
    use crate::{BusName, InterfaceName, InterfaceVersion, ObjectPath, Version};

    fn module(name: &str, provides: &[&str], requires: &[(&str, bool)]) -> ModuleManifest {
        let version = Version::new(1, 0, 0);
        ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: name.to_string(),
                version: version.clone(),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(format!("ai.tinyhumans.module.{name}")).unwrap(),
            object_path: ObjectPath::new(format!("/ai/tinyhumans/module/{name}")).unwrap(),
            provides: provides
                .iter()
                .map(|name| ProvidedInterface {
                    version: InterfaceVersion::provided(
                        InterfaceName::new(*name).unwrap(),
                        version.clone(),
                    ),
                    methods: Vec::new(),
                    signals: Vec::new(),
                })
                .collect(),
            requires: requires
                .iter()
                .map(|(name, optional)| Dependency {
                    interface: InterfaceVersion::consumed(
                        InterfaceName::new(*name).unwrap(),
                        version.clone(),
                    ),
                    optional: *optional,
                    reason: String::new(),
                })
                .collect(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        }
    }

    #[test]
    fn a_required_dependency_no_module_provides_leaves_the_module_unresolved_and_names_the_interface()
     {
        let manifests = [module(
            "Consumer",
            &[],
            &[("ai.tinyhumans.module.Missing", false)],
        )];
        let result = resolve(&manifests, &HashSet::new());
        assert!(result.order.is_empty());
        assert_eq!(
            result.unresolved[0].1,
            "a required interface has no provider"
        );
    }

    #[test]
    fn an_optional_dependency_that_is_missing_still_resolves() {
        let manifests = [module(
            "Consumer",
            &[],
            &[("ai.tinyhumans.module.Missing", true)],
        )];
        assert_eq!(resolve(&manifests, &HashSet::new()).order, [0]);
    }

    #[test]
    fn a_dependency_cycle_is_reported_rather_than_loaded() {
        let manifests = [
            module(
                "One",
                &["ai.tinyhumans.module.One"],
                &[("ai.tinyhumans.module.Two", false)],
            ),
            module(
                "Two",
                &["ai.tinyhumans.module.Two"],
                &[("ai.tinyhumans.module.One", false)],
            ),
        ];
        let result = resolve(&manifests, &HashSet::new());
        assert!(result.order.is_empty());
        assert!(
            result
                .unresolved
                .iter()
                .all(|(_, reason)| *reason == "module dependency cycle detected")
        );
    }

    #[test]
    fn a_module_is_initialized_after_every_module_it_depends_on() {
        let manifests = [
            module("Consumer", &[], &[("ai.tinyhumans.module.Provider", false)]),
            module("Provider", &["ai.tinyhumans.module.Provider"], &[]),
        ];
        assert_eq!(resolve(&manifests, &HashSet::new()).order, [1, 0]);
    }

    #[test]
    fn two_modules_claiming_one_bus_name_are_both_reported_and_neither_is_initialized() {
        let first = module("One", &[], &[]);
        let mut second = module("Two", &[], &[]);
        second.bus_name = first.bus_name.clone();
        let result = resolve(&[first, second], &HashSet::new());
        assert!(result.order.is_empty());
        assert_eq!(result.unresolved.len(), 2);
    }
}
