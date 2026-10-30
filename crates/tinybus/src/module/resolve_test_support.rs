//! Shared fixtures for resolver tests.

use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::module::failure::{FailureReport, FailureSink};
use crate::module::runtime::RuntimeNames;

#[derive(Default)]
pub(super) struct Recording(pub(super) Mutex<Vec<(String, Reason)>>);

impl FailureSink for Recording {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((report.module.to_string(), report.reason));
    }
}

pub(super) fn shared_runtime() -> &'static ModuleRuntime {
    static RUNTIME: std::sync::OnceLock<ModuleRuntime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| ModuleRuntime::start(&RuntimeNames::default(), None).unwrap())
}

/// The resolution table is process-global, so each test owns a distinct id.
pub(super) fn record(id: &'static str) -> &'static ReleaseRecord {
    Box::leak(Box::new(ReleaseRecord {
        id,
        bus_name: Box::leak(format!("ai.tinyhumans.test.{id}").into_boxed_str()),
        object_path: "/ai/tinyhumans/test/Module",
        version: "0.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
        assets: &[],
    }))
}

pub(super) struct Fixture {
    pub(super) sink: Arc<Recording>,
    pub(super) config_calls: Arc<AtomicUsize>,
    pub(super) runtime_calls: Arc<AtomicUsize>,
    pub(super) resolver: Resolver,
}

pub(super) fn fixture(
    record: &'static ReleaseRecord,
    enabled: bool,
    sources: HashMap<String, ModuleSource>,
    install_dir: Option<PathBuf>,
) -> Fixture {
    let sink = Arc::new(Recording::default());
    let config_calls = Arc::new(AtomicUsize::new(0));
    let runtime_calls = Arc::new(AtomicUsize::new(0));
    let counted = config_calls.clone();
    let started = runtime_calls.clone();
    let resolver = Resolver::new(ResolverInputs {
        enabled,
        install_dir,
        allow_download: false,
        bundled_dir: None,
        sources,
        linked: Arc::new(LinkedRegistry::new()),
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![record],
        terminal_marker: "This is terminal for the running process".to_string(),
        runtime: Arc::new(move || {
            started.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(shared_runtime()) })
        }),
        module_config: Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            serde_json::json!({})
        }),
        async_module_config: None,
        reporter: Arc::new(FailureReporter::new(sink.clone())),
    });
    Fixture {
        sink,
        config_calls,
        runtime_calls,
        resolver,
    }
}
