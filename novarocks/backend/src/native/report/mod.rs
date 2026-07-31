// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Backend-local native fragment report ownership.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use novarocks::UniqueId;
use novarocks::common::config;
use novarocks::common::engine_error::{
    EngineErrorCode, REPORT_EXEC_STATUS_OK, REPORT_EXEC_STATUS_QUERY_GONE,
};
use novarocks::novarocks_logging::{error, warn};
use novarocks::runtime::endpoint::RuntimeEndpoint;
use novarocks::runtime::exchange;
use novarocks::runtime::fragment::io::{
    FragmentReportHandle, FragmentReportRegistration, FragmentTerminalReport,
};
use novarocks::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use novarocks::runtime::profile::{
    ProfileUnit, encode_native_runtime_profile, merge_pipeline_profiles,
};
use novarocks::runtime::query_context::QueryId;
use novarocks::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use novarocks::runtime::sink_commit;
use novarocks::service::grpc_client::{NovaRocksGrpcRemoteClient, proto};

const NORMAL_QUEUE_LIMIT: usize = 1_000;

#[derive(Clone)]
struct ReportInstance {
    registration: FragmentReportRegistration,
    endpoint: RuntimeEndpoint,
}

#[derive(Clone, Debug)]
struct ReportTask {
    finst_id: UniqueId,
    query_id: QueryId,
    endpoint: RuntimeEndpoint,
    report: proto::novarocks::ExecStatusReport,
}

#[derive(Clone, Copy)]
struct Settings {
    normal_workers: usize,
    final_workers: usize,
    final_retry_limit: usize,
}

impl Settings {
    fn from_config() -> Self {
        Self {
            normal_workers: config::exec_state_report_max_threads(),
            final_workers: config::priority_exec_state_report_max_threads(),
            final_retry_limit: config::report_exec_rpc_request_retry_num(),
        }
    }
}

trait Sender: Send + Sync {
    fn send(&self, task: &ReportTask) -> Result<(), String>;
}

trait Sleeper: Send + Sync {
    fn sleep(&self, duration: Duration);
}

trait FailClose: Send + Sync {
    fn fail_local(&self, query_id: QueryId, finst_id: UniqueId, error: String);
}

struct GrpcSender;

impl Sender for GrpcSender {
    fn send(&self, task: &ReportTask) -> Result<(), String> {
        let client =
            NovaRocksGrpcRemoteClient::connect_blocking(report_socket_addr(&task.endpoint)?)?;
        let response =
            client.blocking_report_exec_status(proto::novarocks::ReportExecStatusRequest {
                report: Some(task.report.clone()),
            })?;
        interpret_response(response)
    }
}

struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

struct RuntimeFailClose;

impl FailClose for RuntimeFailClose {
    fn fail_local(&self, query_id: QueryId, finst_id: UniqueId, error: String) {
        let mut finst_ids = NativeFragmentQueryRuntime::global().cancel_query(query_id, error);
        if !finst_ids.contains(&finst_id) {
            finst_ids.push(finst_id);
        }
        for finst_id in finst_ids {
            exchange::cancel_fragment(finst_id.hi, finst_id.lo);
        }
    }
}

#[derive(Default)]
struct Queue {
    state: Mutex<QueueState>,
    cv: Condvar,
}

struct QueueState {
    tasks: VecDeque<ReportTask>,
    accepting: bool,
}

impl Default for QueueState {
    fn default() -> Self {
        Self {
            tasks: VecDeque::new(),
            accepting: true,
        }
    }
}

impl Queue {
    fn push(&self, task: ReportTask, limit: Option<usize>) -> Result<(), String> {
        let mut state = self.state.lock().expect("native report queue lock");
        if !state.accepting {
            return Err("native report manager is stopping".to_string());
        }
        if limit.is_some_and(|limit| state.tasks.len() >= limit) {
            return Err(format!(
                "NativeReportManager normal queue is full: limit={}",
                limit.expect("checked above")
            ));
        }
        state.tasks.push_back(task);
        self.cv.notify_one();
        Ok(())
    }

    fn take(&self) -> Option<ReportTask> {
        let mut state = self.state.lock().expect("native report queue lock");
        loop {
            if let Some(task) = state.tasks.pop_front() {
                return Some(task);
            }
            if !state.accepting {
                return None;
            }
            state = self.cv.wait(state).expect("native report queue wait");
        }
    }

    fn close(&self, discard: bool) {
        let mut state = self.state.lock().expect("native report queue lock");
        state.accepting = false;
        if discard {
            state.tasks.clear();
        }
        self.cv.notify_all();
    }
}

struct State {
    settings: Settings,
    sender: Arc<dyn Sender>,
    sleeper: Arc<dyn Sleeper>,
    fail_close: Arc<dyn FailClose>,
    accepting_registrations: AtomicBool,
    periodic_running: AtomicBool,
    registrations: Mutex<HashMap<UniqueId, ReportInstance>>,
    normal: Queue,
    final_reports: Queue,
    periodic_cv: Condvar,
    periodic_lock: Mutex<()>,
}

/// Instance owner for all native report registrations and workers.
pub(crate) struct NativeReportManager {
    state: Arc<State>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// Cloneable report endpoint attached to one physical fragment.
#[derive(Clone)]
pub(crate) struct NativeReportHandle {
    state: Arc<State>,
    finst_id: UniqueId,
    terminal: Arc<AtomicBool>,
}

impl NativeReportManager {
    pub(crate) fn new() -> Self {
        Self::with_components(
            Settings::from_config(),
            Arc::new(GrpcSender),
            Arc::new(ThreadSleeper),
            Arc::new(RuntimeFailClose),
        )
    }

    fn with_components(
        settings: Settings,
        sender: Arc<dyn Sender>,
        sleeper: Arc<dyn Sleeper>,
        fail_close: Arc<dyn FailClose>,
    ) -> Self {
        let state = Arc::new(State {
            settings,
            sender,
            sleeper,
            fail_close,
            accepting_registrations: AtomicBool::new(true),
            periodic_running: AtomicBool::new(true),
            registrations: Mutex::new(HashMap::new()),
            normal: Queue::default(),
            final_reports: Queue::default(),
            periodic_cv: Condvar::new(),
            periodic_lock: Mutex::new(()),
        });
        let mut workers = Vec::new();
        for index in 0..settings.normal_workers {
            workers.push(spawn(
                format!("native-report-normal-{index}"),
                Arc::clone(&state),
                normal_worker,
            ));
        }
        for index in 0..settings.final_workers {
            workers.push(spawn(
                format!("native-report-final-{index}"),
                Arc::clone(&state),
                final_worker,
            ));
        }
        workers.push(spawn(
            "native-report-periodic".to_string(),
            Arc::clone(&state),
            periodic_worker,
        ));
        Self {
            state,
            workers: Mutex::new(workers),
        }
    }

    pub(crate) fn register(
        &self,
        registration: FragmentReportRegistration,
        endpoint: RuntimeEndpoint,
    ) -> Result<NativeReportHandle, String> {
        if !self.state.accepting_registrations.load(Ordering::Acquire) {
            return Err("native report manager is stopping".to_string());
        }
        let finst_id = registration.fragment_instance_id();
        self.state
            .registrations
            .lock()
            .expect("native report registry lock")
            .insert(
                finst_id,
                ReportInstance {
                    registration,
                    endpoint,
                },
            );
        Ok(NativeReportHandle {
            state: Arc::clone(&self.state),
            finst_id,
            terminal: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn unregister(&self, finst_id: UniqueId) {
        self.state
            .registrations
            .lock()
            .expect("native report registry lock")
            .remove(&finst_id);
        sink_commit::unregister(finst_id);
    }

    pub(crate) fn report_progress(&self, finst_id: UniqueId) {
        enqueue_progress(&self.state, finst_id);
    }

    /// The shutdown order is intentionally strict: no new registration,
    /// periodic reports stop, normal work is discarded, final work drains, and
    /// every worker is joined before this method returns.
    pub(crate) fn shutdown(&self) {
        self.state
            .accepting_registrations
            .store(false, Ordering::Release);
        self.state.periodic_running.store(false, Ordering::Release);
        self.state.periodic_cv.notify_all();
        self.state.normal.close(true);
        self.state.final_reports.close(false);
        let mut workers = self.workers.lock().expect("native report worker lock");
        for worker in workers.drain(..) {
            if worker.join().is_err() {
                error!(target: "novarocks::report", "native report worker panicked during shutdown");
            }
        }
        self.state
            .registrations
            .lock()
            .expect("native report registry lock")
            .clear();
    }
}

impl Drop for NativeReportManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FragmentReportHandle for NativeReportHandle {
    fn report_progress(&self) {
        enqueue_progress(&self.state, self.finst_id);
    }

    fn report_terminal(&self, terminal: FragmentTerminalReport) {
        if self
            .terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if let Some(instance) = self
            .state
            .registrations
            .lock()
            .expect("native report registry lock")
            .remove(&self.finst_id)
        {
            if let Err(error) = self.final_report(task_for(instance, Some(terminal))) {
                warn!(target: "novarocks::report", finst_id = %self.finst_id, error = %error, "native final report dropped during shutdown");
            }
        }
        sink_commit::unregister(self.finst_id);
    }
}

impl NativeReportHandle {
    fn final_report(&self, task: ReportTask) -> Result<(), String> {
        self.state.final_reports.push(task, None)
    }
}

fn spawn(name: String, state: Arc<State>, run: fn(Arc<State>)) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name(name)
        .spawn(move || run(state))
        .expect("start native report worker")
}

fn enqueue_progress(state: &Arc<State>, finst_id: UniqueId) {
    let instance = state
        .registrations
        .lock()
        .expect("native report registry lock")
        .get(&finst_id)
        .cloned();
    if let Some(instance) = instance {
        if let Err(error) = state
            .normal
            .push(task_for(instance, None), Some(NORMAL_QUEUE_LIMIT))
        {
            warn!(target: "novarocks::report", finst_id = %finst_id, error = %error, "failed to enqueue native reportExecStatus");
        }
    }
}

fn normal_worker(state: Arc<State>) {
    while let Some(task) = state.normal.take() {
        if let Err(error) = state.sender.send(&task) {
            warn!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, error = %error, "native best-effort reportExecStatus failed");
        }
    }
}

fn final_worker(state: Arc<State>) {
    while let Some(task) = state.final_reports.take() {
        if let Err(error) = send_final(&state, &task) {
            let message = format!("native final reportExecStatus failed: {error}");
            error!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, error = %error, "native final reportExecStatus exhausted retries");
            state
                .fail_close
                .fail_local(task.query_id, task.finst_id, message);
        }
    }
}

fn periodic_worker(state: Arc<State>) {
    let mut last = HashMap::<UniqueId, Instant>::new();
    while state.periodic_running.load(Ordering::Acquire) {
        let instances = state
            .registrations
            .lock()
            .expect("native report registry lock")
            .clone();
        let now = Instant::now();
        let active = instances.keys().copied().collect::<HashSet<_>>();
        for (finst_id, instance) in instances {
            let registration = &instance.registration;
            if registration.enable_profile()
                && let Some(interval_ns) = registration.report_interval_ns()
                && last.get(&finst_id).is_none_or(|then| {
                    now.duration_since(*then) >= Duration::from_nanos(interval_ns.max(1) as u64)
                })
            {
                enqueue_progress(&state, finst_id);
                last.insert(finst_id, now);
            }
        }
        last.retain(|finst_id, _| active.contains(finst_id));
        let guard = state
            .periodic_lock
            .lock()
            .expect("native report periodic lock");
        let _ = state
            .periodic_cv
            .wait_timeout(guard, Duration::from_secs(1))
            .expect("native report periodic wait");
    }
}

fn send_final(state: &State, task: &ReportTask) -> Result<(), String> {
    let mut last_error = String::new();
    let limit = state.settings.final_retry_limit.max(1);
    for attempt in 1..=limit {
        match state.sender.send(task) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = error;
                warn!(target: "novarocks::report", finst_id = %task.finst_id, query_id = %task.query_id, attempt, error = %last_error, "native final reportExecStatus failed");
            }
        }
        if attempt < limit {
            state.sleeper.sleep(backoff(attempt));
        }
    }
    Err(last_error)
}

fn task_for(instance: ReportInstance, terminal: Option<FragmentTerminalReport>) -> ReportTask {
    let registration = &instance.registration;
    let finst_id = registration.fragment_instance_id();
    let done = terminal.is_some();
    let include_runtime_filters = terminal
        .as_ref()
        .is_some_and(FragmentTerminalReport::include_runtime_filter_profile);
    let status = match terminal.and_then(|terminal| terminal.error().map(str::to_owned)) {
        Some(message) => proto::common::Status { code: 1, message },
        None => proto::common::Status {
            code: 0,
            message: String::new(),
        },
    };
    let snapshot = sink_commit::report_snapshot(finst_id);
    let mut loaded_rows = snapshot.load_stats.loaded_rows.max(0);
    let mut sink_load_bytes = snapshot.load_stats.loaded_bytes.max(0);
    for commit in &snapshot.iceberg_commits {
        if let Some(file) = commit.iceberg_data_file.as_ref() {
            loaded_rows = loaded_rows.saturating_add(file.record_count.unwrap_or_default());
            sink_load_bytes =
                sink_load_bytes.saturating_add(file.file_size_in_bytes.unwrap_or_default());
        }
    }
    ReportTask {
        finst_id,
        query_id: registration.query_id(),
        endpoint: instance.endpoint,
        report: proto::novarocks::ExecStatusReport {
            query_id: Some(proto::common::UniqueId {
                hi: registration.query_id().hi(),
                lo: registration.query_id().lo(),
            }),
            fragment_instance_id: Some(proto::common::UniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            backend_num: registration.backend_num(),
            status: Some(status),
            done,
            iceberg_commits: snapshot.iceberg_commits,
            loaded_rows,
            sink_load_bytes,
            filtered_rows: snapshot.load_stats.filtered_rows.max(0),
            profile: build_profile(registration, include_runtime_filters),
        },
    }
}

fn build_profile(
    registration: &FragmentReportRegistration,
    include_runtime_filters: bool,
) -> Option<proto::novarocks::RuntimeProfileTree> {
    if !registration.enable_profile() {
        return None;
    }
    let merged = merge_pipeline_profiles(registration.profiler()?);
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
    Some(encode_native_runtime_profile(&merged))
}

fn report_socket_addr(endpoint: &RuntimeEndpoint) -> Result<SocketAddr, String> {
    let host = endpoint.host().trim();
    if host.is_empty() {
        return Err("invalid native report host '': empty host".to_string());
    }
    let port = endpoint.port() as u16;
    let lookup = if let Some(inner) = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        format!("[{inner}]:{port}")
    } else if host
        .parse::<IpAddr>()
        .is_ok_and(|address| matches!(address, IpAddr::V6(_)))
    {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    lookup
        .to_socket_addrs()
        .map_err(|error| format!("invalid native report host '{}': {error}", endpoint.host()))?
        .next()
        .ok_or_else(|| {
            format!(
                "invalid native report host '{}': no socket addresses resolved",
                endpoint.host()
            )
        })
}

fn interpret_response(response: proto::novarocks::ReportExecStatusResponse) -> Result<(), String> {
    match response.status_code {
        REPORT_EXEC_STATUS_OK => Ok(()),
        REPORT_EXEC_STATUS_QUERY_GONE
            if response.error_code == EngineErrorCode::WriteCoordinatorGone.as_str() =>
        {
            Ok(())
        }
        REPORT_EXEC_STATUS_QUERY_GONE => Err(format!(
            "native reportExecStatus returned QUERY_GONE with error_code={}; expected error_code={}",
            response.error_code,
            EngineErrorCode::WriteCoordinatorGone.as_str()
        )),
        _ => Err(format!(
            "native reportExecStatus returned status_code={}: {}",
            response.status_code, response.message
        )),
    }
}

fn backoff(attempt: usize) -> Duration {
    Duration::from_millis(100 * (1_u64 << attempt.saturating_sub(1).min(10)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct TestSender {
        attempts: AtomicUsize,
        failures: usize,
    }

    impl Sender for TestSender {
        fn send(&self, _task: &ReportTask) -> Result<(), String> {
            let attempt = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
            (attempt > self.failures)
                .then_some(())
                .ok_or_else(|| "coordinator unavailable".to_string())
        }
    }

    #[derive(Default)]
    struct TestSleeper(Mutex<Vec<Duration>>);

    impl Sleeper for TestSleeper {
        fn sleep(&self, duration: Duration) {
            self.0.lock().expect("test sleeps").push(duration);
        }
    }

    #[derive(Default)]
    struct TestFailClose(Mutex<Vec<(QueryId, UniqueId, String)>>);

    impl FailClose for TestFailClose {
        fn fail_local(&self, query_id: QueryId, finst_id: UniqueId, error: String) {
            self.0
                .lock()
                .expect("test fail-close")
                .push((query_id, finst_id, error));
        }
    }

    #[test]
    fn final_report_retries_with_injected_sender_and_sleeper() {
        let sender = Arc::new(TestSender {
            attempts: AtomicUsize::new(0),
            failures: 2,
        });
        let sleeper = Arc::new(TestSleeper::default());
        let state = test_state(
            3,
            sender.clone(),
            sleeper.clone(),
            Arc::new(TestFailClose::default()),
        );
        send_final(&state, &task()).expect("third attempt succeeds");
        assert_eq!(sender.attempts.load(Ordering::Acquire), 3);
        assert_eq!(
            *sleeper.0.lock().expect("test sleeps"),
            vec![Duration::from_millis(100), Duration::from_millis(200)]
        );
    }

    #[test]
    fn query_gone_requires_expected_error_code() {
        let accepted = proto::novarocks::ReportExecStatusResponse {
            status_code: REPORT_EXEC_STATUS_QUERY_GONE,
            message: String::new(),
            error_code: EngineErrorCode::WriteCoordinatorGone.as_str().to_string(),
        };
        interpret_response(accepted).expect("query gone is terminal success");
        let invalid = proto::novarocks::ReportExecStatusResponse {
            status_code: REPORT_EXEC_STATUS_QUERY_GONE,
            message: String::new(),
            error_code: String::new(),
        };
        assert!(interpret_response(invalid).is_err());
    }

    #[test]
    fn exhaustion_calls_only_injected_local_fail_close() {
        let sender = Arc::new(TestSender {
            attempts: AtomicUsize::new(0),
            failures: usize::MAX,
        });
        let fail_close = Arc::new(TestFailClose::default());
        let state = test_state(
            2,
            sender,
            Arc::new(TestSleeper::default()),
            fail_close.clone(),
        );
        let report_task = task();
        let error = send_final(&state, &report_task).expect_err("retry exhaustion");
        state
            .fail_close
            .fail_local(report_task.query_id, report_task.finst_id, error);
        assert_eq!(fail_close.0.lock().expect("test fail-close").len(), 1);
    }

    #[test]
    fn shutdown_discards_normal_work_drains_final_work_and_joins_workers() {
        let sender = Arc::new(TestSender {
            attempts: AtomicUsize::new(0),
            failures: 0,
        });
        let worker_sender: Arc<dyn Sender> = sender.clone();
        let manager = NativeReportManager::with_components(
            Settings {
                normal_workers: 0,
                final_workers: 1,
                final_retry_limit: 1,
            },
            worker_sender,
            Arc::new(TestSleeper::default()),
            Arc::new(TestFailClose::default()),
        );
        manager
            .state
            .normal
            .push(task(), Some(NORMAL_QUEUE_LIMIT))
            .expect("normal queue");
        manager
            .state
            .final_reports
            .push(task(), None)
            .expect("final queue");
        manager.shutdown();
        assert!(manager.state.normal.take().is_none());
        assert_eq!(sender.attempts.load(Ordering::Acquire), 1);
        assert!(manager.state.final_reports.take().is_none());
        assert!(manager.workers.lock().expect("workers").is_empty());
    }

    fn test_state(
        retry_limit: usize,
        sender: Arc<dyn Sender>,
        sleeper: Arc<dyn Sleeper>,
        fail_close: Arc<dyn FailClose>,
    ) -> State {
        State {
            settings: Settings {
                normal_workers: 0,
                final_workers: 0,
                final_retry_limit: retry_limit,
            },
            sender,
            sleeper,
            fail_close,
            accepting_registrations: AtomicBool::new(true),
            periodic_running: AtomicBool::new(true),
            registrations: Mutex::new(HashMap::new()),
            normal: Queue::default(),
            final_reports: Queue::default(),
            periodic_cv: Condvar::new(),
            periodic_lock: Mutex::new(()),
        }
    }

    fn task() -> ReportTask {
        ReportTask {
            finst_id: UniqueId { hi: 3, lo: 4 },
            query_id: QueryId::new(1, 2),
            endpoint: RuntimeEndpoint::new("127.0.0.1", 18040).expect("endpoint"),
            report: proto::novarocks::ExecStatusReport {
                query_id: Some(proto::common::UniqueId { hi: 1, lo: 2 }),
                fragment_instance_id: Some(proto::common::UniqueId { hi: 3, lo: 4 }),
                backend_num: 0,
                status: None,
                done: true,
                iceberg_commits: Vec::new(),
                loaded_rows: 0,
                sink_load_bytes: 0,
                filtered_rows: 0,
                profile: None,
            },
        }
    }
}
