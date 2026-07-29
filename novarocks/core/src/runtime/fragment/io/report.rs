use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;

/// Protocol-neutral inputs captured when a fragment becomes reportable.
#[derive(Clone)]
pub struct FragmentReportRegistration {
    fragment_instance_id: UniqueId,
    query_id: QueryId,
    backend_num: i32,
    enable_profile: bool,
    profiler: Option<Profiler>,
    fragment_mem_tracker: Option<Arc<MemTracker>>,
    query_mem_tracker: Option<Arc<MemTracker>>,
    report_interval_ns: Option<i64>,
}

impl FragmentReportRegistration {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fragment_instance_id: UniqueId,
        query_id: QueryId,
        backend_num: i32,
        enable_profile: bool,
        profiler: Option<Profiler>,
        fragment_mem_tracker: Option<Arc<MemTracker>>,
        query_mem_tracker: Option<Arc<MemTracker>>,
        report_interval_ns: Option<i64>,
    ) -> Self {
        Self {
            fragment_instance_id,
            query_id,
            backend_num,
            enable_profile,
            profiler,
            fragment_mem_tracker,
            query_mem_tracker,
            report_interval_ns,
        }
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }
    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }
    pub const fn enable_profile(&self) -> bool {
        self.enable_profile
    }
    pub fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }
    pub fn fragment_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.fragment_mem_tracker.as_ref()
    }
    pub fn query_mem_tracker(&self) -> Option<&Arc<MemTracker>> {
        self.query_mem_tracker.as_ref()
    }
    pub const fn report_interval_ns(&self) -> Option<i64> {
        self.report_interval_ns
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FragmentTerminalReport {
    error: Option<String>,
    include_runtime_filter_profile: bool,
}

impl FragmentTerminalReport {
    pub fn new(error: Option<String>, include_runtime_filter_profile: bool) -> Self {
        Self {
            error,
            include_runtime_filter_profile,
        }
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub const fn include_runtime_filter_profile(&self) -> bool {
        self.include_runtime_filter_profile
    }
}

/// Per-fragment report lifecycle. Implementations must make terminal reporting
/// and dropping/unregistering idempotent.
pub trait FragmentReportHandle: Send + Sync + 'static {
    fn report_progress(&self);
    fn report_terminal(&self, terminal: FragmentTerminalReport);
}

/// Destination-specific registration boundary for fragment report adapters.
pub trait FragmentReportSink: Send + Sync + 'static {
    fn register(
        &self,
        registration: FragmentReportRegistration,
    ) -> Result<Arc<dyn FragmentReportHandle>, String>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{FragmentReportHandle, FragmentTerminalReport};

    struct OnceHandle {
        progress: AtomicUsize,
        terminal: AtomicUsize,
    }

    impl FragmentReportHandle for OnceHandle {
        fn report_progress(&self) {
            self.progress.fetch_add(1, Ordering::Relaxed);
        }

        fn report_terminal(&self, _terminal: FragmentTerminalReport) {
            if self
                .terminal
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count == 0).then_some(1)
                })
                .is_ok()
            {
                self.progress.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn report_handle_contract_allows_only_one_terminal_transition() {
        let handle = OnceHandle {
            progress: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        };
        handle.report_progress();
        handle.report_terminal(FragmentTerminalReport::default());
        handle.report_terminal(FragmentTerminalReport::default());
        assert_eq!(handle.progress.load(Ordering::Relaxed), 2);
        assert_eq!(handle.terminal.load(Ordering::Relaxed), 1);
    }
}
