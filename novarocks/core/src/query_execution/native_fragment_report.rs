//! Native NovaRocks fragment execution-status reporting.
//!
//! This owner deliberately contains no StarRocks Thrift state, endpoint or
//! worker. StarRocks reporting lives in `novarocks-compat`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::common::types::UniqueId;
use crate::novarocks_logging::{debug, warn};
use crate::proto::{common, novarocks};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::fragment::io::{FragmentReportRegistration, FragmentTerminalReport};
use crate::runtime::profile::{ProfileUnit, merge_pipeline_profiles};
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime::sink_commit;
use crate::service::standalone_exec_state_reporter::{self, StandaloneExecStateReportTask};

#[derive(Clone)]
struct NativeReportInstance {
    registration: FragmentReportRegistration,
    endpoint: RuntimeEndpoint,
}

static INSTANCES: OnceLock<Mutex<HashMap<UniqueId, NativeReportInstance>>> = OnceLock::new();
static WORKER_STARTED: OnceLock<()> = OnceLock::new();
static WORKER_STOP: AtomicBool = AtomicBool::new(false);

fn instances() -> &'static Mutex<HashMap<UniqueId, NativeReportInstance>> {
    INSTANCES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(registration: FragmentReportRegistration, endpoint: RuntimeEndpoint) {
    ensure_worker_started();
    instances()
        .lock()
        .expect("native report registry lock")
        .insert(
            registration.fragment_instance_id(),
            NativeReportInstance {
                registration,
                endpoint,
            },
        );
}

pub fn unregister(finst_id: UniqueId) {
    instances()
        .lock()
        .expect("native report registry lock")
        .remove(&finst_id);
}

pub fn is_registered(finst_id: UniqueId) -> bool {
    instances()
        .lock()
        .expect("native report registry lock")
        .contains_key(&finst_id)
}

pub fn report_progress(finst_id: UniqueId) {
    let instance = instances()
        .lock()
        .expect("native report registry lock")
        .get(&finst_id)
        .cloned();
    let Some(instance) = instance else {
        debug!(target: "novarocks::report", finst_id = %finst_id, "native report instance missing");
        return;
    };
    let task = task_for(instance, None);
    if let Err(error) = standalone_exec_state_reporter::enqueue_non_final(task) {
        warn!(target: "novarocks::report", finst_id = %finst_id, error = %error, "failed to enqueue native reportExecStatus");
    }
}

pub fn report_terminal(finst_id: UniqueId, terminal: FragmentTerminalReport) {
    let instance = instances()
        .lock()
        .expect("native report registry lock")
        .remove(&finst_id);
    let Some(instance) = instance else {
        debug!(target: "novarocks::report", finst_id = %finst_id, "native report instance missing");
        sink_commit::unregister(finst_id);
        return;
    };
    standalone_exec_state_reporter::enqueue_final(task_for(instance, Some(terminal)));
    sink_commit::unregister(finst_id);
}

pub fn stop() {
    WORKER_STOP.store(true, Ordering::Release);
}

fn task_for(
    instance: NativeReportInstance,
    terminal: Option<FragmentTerminalReport>,
) -> StandaloneExecStateReportTask {
    let registration = &instance.registration;
    let finst_id = registration.fragment_instance_id();
    let done = terminal.is_some();
    let include_runtime_filters = terminal
        .as_ref()
        .map(FragmentTerminalReport::include_runtime_filter_profile)
        .unwrap_or(false);
    let status = match terminal.and_then(|terminal| terminal.error().map(str::to_owned)) {
        Some(message) => common::Status { code: 1, message },
        None => common::Status {
            code: 0,
            message: String::new(),
        },
    };
    let snapshot = sink_commit::report_snapshot(finst_id);
    let (loaded_rows, sink_load_bytes, filtered_rows) = load_stats(&snapshot);
    StandaloneExecStateReportTask {
        finst_id,
        query_id: registration.query_id(),
        coord: instance.endpoint,
        report: novarocks::ExecStatusReport {
            query_id: Some(common::UniqueId {
                hi: registration.query_id().hi(),
                lo: registration.query_id().lo(),
            }),
            fragment_instance_id: Some(common::UniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            backend_num: registration.backend_num(),
            status: Some(status),
            done,
            iceberg_commits: snapshot.iceberg_commits,
            loaded_rows,
            sink_load_bytes,
            filtered_rows,
            profile: build_native_profile(registration, include_runtime_filters),
        },
    }
}

fn load_stats(snapshot: &sink_commit::SinkCommitReportSnapshot) -> (i64, i64, i64) {
    let mut rows = snapshot.load_stats.loaded_rows.max(0);
    let mut bytes = snapshot.load_stats.loaded_bytes.max(0);
    let filtered = snapshot.load_stats.filtered_rows.max(0);
    for info in &snapshot.iceberg_commits {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(value) = file.record_count {
                rows = rows.saturating_add(value);
            }
            if let Some(value) = file.file_size_in_bytes {
                bytes = bytes.saturating_add(value);
            }
        }
    }
    (rows, bytes, filtered)
}

fn build_native_profile(
    registration: &FragmentReportRegistration,
    include_runtime_filters: bool,
) -> Option<novarocks::RuntimeProfileTree> {
    if !registration.enable_profile() {
        return None;
    }
    let profiler = registration.profiler()?;
    let merged = merge_pipeline_profiles(profiler);
    if include_runtime_filters {
        RuntimeFilterLifecycleRegistry::global().export_to_profile(
            QueryKey::from_hi_lo(registration.query_id().hi(), registration.query_id().lo()),
            &merged,
        );
    }
    if let Some(tracker) = registration.fragment_mem_tracker() {
        merged.counter_set(
            "InstancePeakMemoryUsage",
            ProfileUnit::Bytes,
            tracker.peak(),
        );
        merged.counter_set(
            "InstanceAllocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.allocated(),
        );
        merged.counter_set(
            "InstanceDeallocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.deallocated(),
        );
    }
    if let Some(tracker) = registration.query_mem_tracker() {
        merged.counter_set("QueryPeakMemoryUsage", ProfileUnit::Bytes, tracker.peak());
    }
    Some(merged.to_proto())
}

fn ensure_worker_started() {
    WORKER_STARTED.get_or_init(|| {
        WORKER_STOP.store(false, Ordering::Release);
        std::thread::Builder::new()
            .name("native-profile-report".to_string())
            .spawn(run_periodic_worker)
            .expect("start native report worker");
    });
}

fn run_periodic_worker() {
    let mut last_report = HashMap::<UniqueId, Instant>::new();
    while !WORKER_STOP.load(Ordering::Acquire) {
        let snapshot = instances()
            .lock()
            .expect("native report registry lock")
            .clone();
        let now = Instant::now();
        let active = snapshot.keys().copied().collect::<HashSet<_>>();
        for (finst_id, instance) in snapshot {
            let registration = &instance.registration;
            if !registration.enable_profile() {
                continue;
            }
            let Some(interval_ns) = registration.report_interval_ns() else {
                continue;
            };
            if last_report.get(&finst_id).is_none_or(|last| {
                now.duration_since(*last) >= Duration::from_nanos(interval_ns.max(1) as u64)
            }) {
                report_progress(finst_id);
                last_report.insert(finst_id, now);
            }
        }
        last_report.retain(|finst_id, _| active.contains(finst_id));
        std::thread::sleep(Duration::from_secs(1));
    }
}
