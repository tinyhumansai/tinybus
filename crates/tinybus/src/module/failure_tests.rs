use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct Recording {
    reports: Mutex<Vec<(String, String, Reason, String)>>,
    unready: AtomicBool,
}

impl FailureSink for Recording {
    fn ready(&self) -> bool {
        !self.unready.load(Ordering::SeqCst)
    }
    fn capture(&self, report: &FailureReport<'_>) {
        self.reports.lock().unwrap().push((
            report.module.to_string(),
            report.version.to_string(),
            report.reason,
            report.platform.to_string(),
        ));
    }
}

fn reporter() -> (Arc<Recording>, FailureReporter) {
    let sink = Arc::new(Recording::default());
    (sink.clone(), FailureReporter::new(sink))
}

#[test]
fn a_terminal_report_is_delivered_once_per_module_and_reason() {
    let (sink, reporter) = reporter();
    reporter.report("tinydocs", "1.0.0", Reason::ModuleFault);
    reporter.report("tinydocs", "1.0.0", Reason::ModuleFault);
    reporter.report("tinydocs", "1.0.0", Reason::Disabled);
    reporter.report("tinysearch", "1.0.0", Reason::ModuleFault);
    let reports = sink.reports.lock().unwrap();
    assert_eq!(reports.len(), 3);
    assert_eq!(
        reports[0].3,
        format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
    );
}

#[test]
fn invocation_failures_are_never_deduplicated() {
    let (sink, reporter) = reporter();
    reporter.report_invocation("tinydocs", "1.0.0", Reason::ModuleFault);
    reporter.report_invocation("tinydocs", "1.0.0", Reason::ModuleFault);
    assert_eq!(sink.reports.lock().unwrap().len(), 2);
}

#[test]
fn unknown_modules_share_one_anonymous_key() {
    let (sink, reporter) = reporter();
    reporter.report_unknown_module();
    reporter.report_unknown_module();
    let reports = sink.reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].0, "unregistered");
    assert_eq!(reports[0].2, Reason::UnknownModule);
}

#[test]
fn report_for_uses_only_a_matching_registry_record() {
    let (sink, reporter) = reporter();
    let records = [crate::module::ReleaseRecord {
        id: "registry-module",
        bus_name: "ai.tinyhumans.test.RegistryModule",
        object_path: "/ai/tinyhumans/test/RegistryModule",
        version: "1.2.3",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v1.2.3",
        assets: &[],
    }];
    reporter.report_for(&records, "registry-module", Reason::ModuleFault);
    reporter.report_for(&records, "registry-module", Reason::ModuleFault);
    reporter.report_for(&records, "attacker-controlled-id", Reason::ModuleFault);
    assert_eq!(
        sink.reports.lock().unwrap().as_slice(),
        &[
            (
                "registry-module".to_string(),
                "1.2.3".to_string(),
                Reason::ModuleFault,
                format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            ),
            (
                "unregistered".to_string(),
                "unknown".to_string(),
                Reason::UnknownModule,
                format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            ),
        ]
    );
}

#[test]
fn a_sink_that_is_not_ready_does_not_spend_the_key() {
    let (sink, reporter) = reporter();
    sink.unready.store(true, Ordering::SeqCst);
    reporter.report("tinydocs", "1.0.0", Reason::ResolutionFailed);
    sink.unready.store(false, Ordering::SeqCst);
    reporter.report("tinydocs", "1.0.0", Reason::ResolutionFailed);
    reporter.report("tinydocs", "1.0.0", Reason::ResolutionFailed);
    // The first went to a sink that could not receive it; the second is the
    // first the sink can keep, and the third is the duplicate.
    assert_eq!(sink.reports.lock().unwrap().len(), 2);
}

#[test]
fn reason_codes_and_stages_are_the_closed_vocabulary() {
    let cases = [
        (Reason::UnknownModule, "unknown_module", "registry"),
        (Reason::Disabled, "disabled", "availability"),
        (Reason::LoaderDisabled, "loader_disabled", "availability"),
        (Reason::ResolutionFailed, "resolution_failed", "resolve"),
        (
            Reason::IncompatibleContract,
            "incompatible_contract",
            "contract",
        ),
        (Reason::TransportFailed, "transport_failed", "transport"),
        (Reason::ModuleFault, "module_fault", "execution"),
        (Reason::ModuleUnavailable, "module_unavailable", "execution"),
    ];
    for (reason, code, stage) in cases {
        assert_eq!((reason.code(), reason.stage()), (code, stage));
    }
}
