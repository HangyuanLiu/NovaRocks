use std::sync::Arc;

use novarocks::runtime::fragment::io::{FragmentEvent, FragmentEventSink};

pub(crate) fn compat_fragment_event_sink() -> Arc<dyn FragmentEventSink> {
    Arc::new(CompatFragmentEventSink)
}

struct CompatFragmentEventSink;

impl FragmentEventSink for CompatFragmentEventSink {
    fn record(&self, event: FragmentEvent) {
        if let FragmentEvent::Progress(progress) = event {
            novarocks::service::fe_report::report_exec_state(progress.fragment_instance_id());
        }
    }
}
