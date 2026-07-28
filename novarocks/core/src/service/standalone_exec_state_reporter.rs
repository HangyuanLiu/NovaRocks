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

use std::collections::VecDeque;
#[cfg(test)]
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::common::config;
use crate::common::types::UniqueId;
use crate::novarocks_logging::{error, warn};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;
use crate::service::grpc_client::{NovaRocksGrpcRemoteClient, proto};

const NORMAL_REPORT_QUEUE_LIMIT: usize = 1_000;

#[derive(Clone, Debug)]
pub(crate) struct StandaloneExecStateReportTask {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) coord: RuntimeEndpoint,
    pub(crate) report: proto::novarocks::ExecStatusReport,
}

#[derive(Clone, Copy)]
struct StandaloneExecStateReporterSettings {
    normal_threads: usize,
    priority_threads: usize,
    final_retry_limit: usize,
}

impl StandaloneExecStateReporterSettings {
    fn from_config() -> Self {
        Self {
            normal_threads: config::exec_state_report_max_threads(),
            priority_threads: config::priority_exec_state_report_max_threads(),
            final_retry_limit: config::report_exec_rpc_request_retry_num(),
        }
    }
}

#[derive(Default)]
struct ReportQueue {
    state: Mutex<VecDeque<StandaloneExecStateReportTask>>,
    cv: Condvar,
}

pub(crate) struct StandaloneExecStateReporter {
    settings: StandaloneExecStateReporterSettings,
    normal: ReportQueue,
    priority: ReportQueue,
    started: OnceLock<()>,
    #[cfg(test)]
    priority_pending: Mutex<HashMap<(QueryId, UniqueId), usize>>,
    #[cfg(test)]
    priority_final_report_enqueued: Mutex<HashSet<(QueryId, UniqueId)>>,
}

#[cfg(test)]
struct FinalReportPendingGuard<'a> {
    reporter: &'a StandaloneExecStateReporter,
    query_id: QueryId,
    finst_id: UniqueId,
}

#[cfg(test)]
impl Drop for FinalReportPendingGuard<'_> {
    fn drop(&mut self) {
        self.reporter
            .decrement_final_report_pending(self.query_id, self.finst_id);
    }
}

impl StandaloneExecStateReporter {
    fn new() -> Self {
        Self {
            settings: StandaloneExecStateReporterSettings::from_config(),
            normal: ReportQueue::default(),
            priority: ReportQueue::default(),
            started: OnceLock::new(),
            #[cfg(test)]
            priority_pending: Mutex::new(HashMap::new()),
            #[cfg(test)]
            priority_final_report_enqueued: Mutex::new(HashSet::new()),
        }
    }

    fn shared() -> &'static Self {
        static INSTANCE: OnceLock<StandaloneExecStateReporter> = OnceLock::new();
        INSTANCE.get_or_init(StandaloneExecStateReporter::new)
    }

    fn ensure_started(&'static self) {
        self.started.get_or_init(|| {
            for idx in 0..self.settings.normal_threads {
                std::thread::Builder::new()
                    .name(format!("standalone-report-normal-{idx}"))
                    .spawn(move || run_normal_worker(self))
                    .expect("start standalone normal report worker");
            }
            for idx in 0..self.settings.priority_threads {
                std::thread::Builder::new()
                    .name(format!("standalone-report-final-{idx}"))
                    .spawn(move || run_priority_worker(self))
                    .expect("start standalone final report worker");
            }
        });
    }

    fn enqueue_non_final(&self, task: StandaloneExecStateReportTask) -> Result<(), String> {
        let mut guard = self
            .normal
            .state
            .lock()
            .expect("standalone normal report queue lock");
        if guard.len() >= NORMAL_REPORT_QUEUE_LIMIT {
            return Err(format!(
                "StandaloneExecStateReporter normal queue is full: limit={NORMAL_REPORT_QUEUE_LIMIT}"
            ));
        }
        guard.push_back(task);
        self.normal.cv.notify_one();
        Ok(())
    }

    fn enqueue_final(&self, task: StandaloneExecStateReportTask) {
        let mut guard = self
            .priority
            .state
            .lock()
            .expect("standalone priority report queue lock");
        #[cfg(test)]
        {
            self.increment_final_report_pending(task.query_id, task.finst_id);
            self.priority_final_report_enqueued
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert((task.query_id, task.finst_id));
        }
        guard.push_back(task);
        self.priority.cv.notify_one();
    }

    fn take_non_final_task(&self) -> StandaloneExecStateReportTask {
        take_task(&self.normal, "standalone normal report queue wait")
    }

    fn take_final_task(&self) -> StandaloneExecStateReportTask {
        take_task(&self.priority, "standalone priority report queue wait")
    }

    #[cfg(test)]
    fn increment_final_report_pending(&self, query_id: QueryId, finst_id: UniqueId) {
        let mut pending = self
            .priority_pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *pending.entry((query_id, finst_id)).or_default() += 1;
    }

    #[cfg(test)]
    fn decrement_final_report_pending(&self, query_id: QueryId, finst_id: UniqueId) {
        let mut pending = self
            .priority_pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let scope = (query_id, finst_id);
        let remove = match pending.get_mut(&scope) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };
        if remove {
            pending.remove(&scope);
        }
    }

    #[cfg(test)]
    fn final_report_pending_guard(
        &self,
        task: &StandaloneExecStateReportTask,
    ) -> FinalReportPendingGuard<'_> {
        FinalReportPendingGuard {
            reporter: self,
            query_id: task.query_id,
            finst_id: task.finst_id,
        }
    }

    #[cfg(test)]
    fn final_reports_pending_for_test(&self, query_id: QueryId, finst_id: UniqueId) -> usize {
        self.priority_pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(query_id, finst_id))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn final_reports_pending_for_query_for_test(&self, query_id: QueryId) -> usize {
        self.priority_pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|((pending_query_id, _), count)| {
                (*pending_query_id == query_id).then_some(*count)
            })
            .sum()
    }

    #[cfg(test)]
    fn final_report_was_enqueued_for_test(&self, query_id: QueryId, finst_id: UniqueId) -> bool {
        self.priority_final_report_enqueued
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&(query_id, finst_id))
    }
}

pub(crate) fn ensure_started() {
    StandaloneExecStateReporter::shared().ensure_started();
}

pub(crate) fn enqueue_non_final(task: StandaloneExecStateReportTask) -> Result<(), String> {
    let reporter = StandaloneExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_non_final(task)
}

pub(crate) fn enqueue_final(task: StandaloneExecStateReportTask) {
    let reporter = StandaloneExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_final(task);
}

#[cfg(test)]
pub(crate) fn final_reports_pending_for_test(query_id: QueryId, finst_id: UniqueId) -> usize {
    StandaloneExecStateReporter::shared().final_reports_pending_for_test(query_id, finst_id)
}

#[cfg(test)]
pub(crate) fn final_reports_pending_for_query_for_test(query_id: QueryId) -> usize {
    StandaloneExecStateReporter::shared().final_reports_pending_for_query_for_test(query_id)
}

#[cfg(test)]
pub(crate) fn wait_for_final_reports_for_query_for_test(
    query_id: QueryId,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while final_reports_pending_for_query_for_test(query_id) != 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    final_reports_pending_for_query_for_test(query_id) == 0
}

#[cfg(test)]
pub(crate) fn wait_for_final_reports_for_finsts_for_test(
    query_id: QueryId,
    finst_ids: &[UniqueId],
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while (!finst_ids.iter().all(|finst_id| {
        StandaloneExecStateReporter::shared()
            .final_report_was_enqueued_for_test(query_id, *finst_id)
    }) || final_reports_pending_for_query_for_test(query_id) != 0)
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    finst_ids.iter().all(|finst_id| {
        StandaloneExecStateReporter::shared()
            .final_report_was_enqueued_for_test(query_id, *finst_id)
    }) && final_reports_pending_for_query_for_test(query_id) == 0
}

fn take_task(queue: &ReportQueue, wait_msg: &'static str) -> StandaloneExecStateReportTask {
    let mut guard = queue.state.lock().expect("standalone report queue lock");
    loop {
        if let Some(task) = guard.pop_front() {
            return task;
        }
        guard = queue.cv.wait(guard).expect(wait_msg);
    }
}

fn run_normal_worker(reporter: &'static StandaloneExecStateReporter) {
    loop {
        let task = reporter.take_non_final_task();
        if let Err(err) = send_once(&task) {
            warn!(
                target: "novarocks::report",
                finst_id = %task.finst_id,
                query_id = %task.query_id,
                error = %err,
                "standalone best-effort reportExecStatus failed"
            );
        }
    }
}

fn run_priority_worker(reporter: &'static StandaloneExecStateReporter) {
    loop {
        let task = reporter.take_final_task();
        #[cfg(test)]
        let _pending = reporter.final_report_pending_guard(&task);
        let result = send_final_report_with(
            task.clone(),
            reporter.settings.final_retry_limit,
            send_once,
            std::thread::sleep,
        );
        if let Err(err) = result {
            handle_final_report_exhaustion_with(
                task,
                err,
                fail_local_query_after_report_exhaustion,
            );
        }
    }
}

/// Stop only fragment/query resources owned by this backend after its final
/// report cannot reach the frontend. Frontend query-wide failure ownership
/// remains behind the report transport boundary.
fn fail_local_query_after_report_exhaustion(query_id: QueryId, finst_id: UniqueId, error: String) {
    let manager = crate::runtime::query_context::query_context_manager();
    let mut local_finsts = manager.cancel_query(query_id, error.clone());
    if !local_finsts.contains(&finst_id) {
        local_finsts.push(finst_id);
    }
    for local_finst_id in local_finsts {
        crate::runtime::result_buffer::close_error(local_finst_id, error.clone());
        crate::runtime::exchange::cancel_fragment(local_finst_id.hi, local_finst_id.lo);
    }
}

fn send_once(task: &StandaloneExecStateReportTask) -> Result<(), String> {
    let addr = standalone_report_socket_addr(&task.coord)?;
    let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)?;
    let resp = client.blocking_report_exec_status(proto::novarocks::ReportExecStatusRequest {
        report: Some(task.report.clone()),
    })?;
    interpret_report_exec_status_response(resp)
}

fn interpret_report_exec_status_response(
    resp: proto::novarocks::ReportExecStatusResponse,
) -> Result<(), String> {
    match resp.status_code {
        crate::service::grpc_server::REPORT_EXEC_STATUS_OK => Ok(()),
        crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE => {
            let expected = crate::common::engine_error_codes::EngineErrorCode::WriteCoordinatorGone;
            if resp.error_code == expected.as_str() {
                Ok(())
            } else {
                Err(format!(
                    "standalone reportExecStatus returned QUERY_GONE with error_code={}; expected error_code={}",
                    resp.error_code,
                    expected.as_str()
                ))
            }
        }
        _ => Err(format!(
            "standalone reportExecStatus returned status_code={}: {}",
            resp.status_code, resp.message
        )),
    }
}

fn standalone_report_socket_addr(endpoint: &RuntimeEndpoint) -> Result<SocketAddr, String> {
    let port = endpoint.port() as u16;
    let host = endpoint.host().trim();
    if host.is_empty() {
        return Err("invalid standalone report host '': empty host".to_string());
    }

    let lookup_endpoint = socket_lookup_endpoint(host, port);
    lookup_endpoint
        .to_socket_addrs()
        .map_err(|e| format!("invalid standalone report host '{}': {e}", endpoint.host()))?
        .next()
        .ok_or_else(|| {
            format!(
                "invalid standalone report host '{}': no socket addresses resolved",
                endpoint.host()
            )
        })
}

fn socket_lookup_endpoint(host: &str, port: u16) -> String {
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return format!("[{inner}]:{port}");
    }

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{port}"),
        Ok(IpAddr::V4(_)) | Err(_) => format!("{host}:{port}"),
    }
}

fn send_final_report_with<F, S>(
    task: StandaloneExecStateReportTask,
    retry_limit: usize,
    mut send: F,
    mut sleep: S,
) -> Result<(), String>
where
    F: FnMut(&StandaloneExecStateReportTask) -> Result<(), String>,
    S: FnMut(Duration),
{
    let retry_limit = retry_limit.max(1);
    let mut last_error = String::new();
    for attempt in 1..=retry_limit {
        match send(&task) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = err;
                warn!(
                    target: "novarocks::report",
                    finst_id = %task.finst_id,
                    query_id = %task.query_id,
                    attempt,
                    error = %last_error,
                    "standalone final reportExecStatus failed"
                );
            }
        }
        if attempt < retry_limit {
            sleep(backoff_for_attempt(attempt));
        }
    }
    Err(last_error)
}

fn handle_final_report_exhaustion_with<F>(
    task: StandaloneExecStateReportTask,
    err: String,
    mark_failed: F,
) where
    F: FnOnce(QueryId, UniqueId, String),
{
    error!(
        target: "novarocks::report",
        finst_id = %task.finst_id,
        query_id = %task.query_id,
        error = %err,
        "standalone final reportExecStatus exhausted retries"
    );
    mark_failed(
        task.query_id,
        task.finst_id,
        format!("standalone final reportExecStatus failed: {err}"),
    );
}

fn backoff_for_attempt(attempt: usize) -> Duration {
    let shift = attempt.saturating_sub(1).min(10);
    Duration::from_millis(100 * (1u64 << shift))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn final_report_retries_and_returns_error_after_limit() {
        let attempts = AtomicUsize::new(0);
        let sleeps = Mutex::new(Vec::new());
        let result = send_final_report_with(
            test_task(),
            3,
            |_| {
                attempts.fetch_add(1, Ordering::AcqRel);
                Err("network down".to_string())
            },
            |duration| sleeps.lock().expect("sleep record").push(duration),
        );

        let err = result.expect_err("retry exhaustion must be an error");
        assert!(err.contains("network down"), "{err}");
        assert_eq!(attempts.load(Ordering::Acquire), 3);
        assert_eq!(
            *sleeps.lock().expect("sleep record"),
            vec![Duration::from_millis(100), Duration::from_millis(200)]
        );
    }

    #[test]
    fn final_report_succeeds_after_retry() {
        let attempts = AtomicUsize::new(0);
        let sleeps = Mutex::new(Vec::new());

        let result = send_final_report_with(
            test_task(),
            3,
            |_| {
                let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
                if attempt < 2 {
                    Err("temporary outage".to_string())
                } else {
                    Ok(())
                }
            },
            |duration| sleeps.lock().expect("sleep record").push(duration),
        );

        result.expect("retry should eventually succeed");
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(
            *sleeps.lock().expect("sleep record"),
            vec![Duration::from_millis(100)]
        );
    }

    #[test]
    fn query_gone_report_response_is_terminal_success() {
        let response = proto::novarocks::ReportExecStatusResponse {
            status_code: crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE,
            message: "write coordinator not found for query 1/2".to_string(),
            error_code: "WriteCoordinatorGone".to_string(),
        };

        assert_eq!(response.error_code, "WriteCoordinatorGone");
        interpret_report_exec_status_response(response)
            .expect("query-gone report response is terminal success");
    }

    #[test]
    fn query_gone_report_response_requires_write_coordinator_error_code() {
        let response = proto::novarocks::ReportExecStatusResponse {
            status_code: crate::service::grpc_server::REPORT_EXEC_STATUS_QUERY_GONE,
            message: "write coordinator not found for query 1/2".to_string(),
            error_code: String::new(),
        };

        let err = interpret_report_exec_status_response(response)
            .expect_err("missing query-gone code should fail");

        assert!(
            err.contains("expected error_code=WriteCoordinatorGone"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn final_report_failure_records_fragment_error() {
        let task = test_task();
        let expected_query_id = task.query_id;
        let expected_finst_id = task.finst_id;
        let mut captured = None;

        handle_final_report_exhaustion_with(
            task,
            "coordinator unreachable".to_string(),
            |query_id, finst_id, error| {
                captured = Some((query_id, finst_id, error));
            },
        );

        let (query_id, finst_id, error) =
            captured.expect("final report failure must mark query failed");
        assert_eq!(query_id, expected_query_id);
        assert_eq!(finst_id, expected_finst_id);
        assert!(
            error.contains("standalone final reportExecStatus failed"),
            "{error}"
        );
        assert!(error.contains("coordinator unreachable"), "{error}");
    }

    #[test]
    fn non_final_enqueue_is_best_effort_queue_insert() {
        let reporter = StandaloneExecStateReporter::new();

        reporter
            .enqueue_non_final(test_task())
            .expect("non-final queue insert");

        assert_eq!(
            reporter
                .normal
                .state
                .lock()
                .expect("standalone normal queue")
                .len(),
            1
        );
    }

    #[test]
    fn final_report_pending_counts_are_scoped_by_query_and_fragment_instance() {
        let reporter = StandaloneExecStateReporter::new();
        let first = test_task_with_ids(QueryId { hi: 1, lo: 2 }, UniqueId { hi: 3, lo: 4 });
        let same_query_other_finst = test_task_with_ids(first.query_id, UniqueId { hi: 5, lo: 6 });
        let other_query_same_finst = test_task_with_ids(QueryId { hi: 7, lo: 8 }, first.finst_id);

        reporter.enqueue_final(first.clone());
        reporter.enqueue_final(first.clone());
        reporter.enqueue_final(same_query_other_finst.clone());
        reporter.enqueue_final(other_query_same_finst.clone());

        assert_eq!(
            reporter.final_reports_pending_for_test(first.query_id, first.finst_id),
            2
        );
        assert_eq!(
            reporter.final_reports_pending_for_test(
                same_query_other_finst.query_id,
                same_query_other_finst.finst_id,
            ),
            1
        );
        assert_eq!(
            reporter.final_reports_pending_for_test(
                other_query_same_finst.query_id,
                other_query_same_finst.finst_id,
            ),
            1
        );
        assert_eq!(
            reporter.final_reports_pending_for_query_for_test(first.query_id),
            3,
            "query-scoped cleanup sees every pending final report for that query"
        );
        assert_eq!(
            reporter.final_reports_pending_for_query_for_test(other_query_same_finst.query_id),
            1,
            "query-scoped cleanup excludes final reports owned by another query"
        );
        assert!(reporter.final_report_was_enqueued_for_test(first.query_id, first.finst_id));
        assert!(reporter.final_report_was_enqueued_for_test(
            same_query_other_finst.query_id,
            same_query_other_finst.finst_id,
        ));
        assert!(
            !reporter
                .final_report_was_enqueued_for_test(first.query_id, UniqueId { hi: 9, lo: 10 },)
        );
        assert_eq!(
            reporter.final_reports_pending_for_test(
                QueryId { hi: 99, lo: 100 },
                UniqueId { hi: 101, lo: 102 },
            ),
            0
        );
    }

    #[test]
    fn final_report_pending_guard_decrements_its_scope_during_unwind() {
        let reporter = StandaloneExecStateReporter::new();
        let task = test_task();
        reporter.enqueue_final(task.clone());
        let task = reporter.take_final_task();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _pending = reporter.final_report_pending_guard(&task);
            panic!("simulated final report worker panic");
        }));

        assert!(panic.is_err());
        assert_eq!(
            reporter.final_reports_pending_for_test(task.query_id, task.finst_id),
            0
        );
    }

    #[test]
    fn report_socket_addr_accepts_ipv4_literal() {
        let addr = standalone_report_socket_addr(&runtime_endpoint("127.0.0.1", 18040))
            .expect("ipv4 literal");

        assert_eq!(addr.to_string(), "127.0.0.1:18040");
    }

    #[test]
    fn report_socket_addr_accepts_bare_ipv6_literal() {
        let addr = standalone_report_socket_addr(&runtime_endpoint("::1", 18040))
            .expect("bare ipv6 literal");

        assert_eq!(addr.to_string(), "[::1]:18040");
    }

    #[test]
    fn report_socket_addr_accepts_bracketed_ipv6_literal() {
        let addr = standalone_report_socket_addr(&runtime_endpoint("[::1]", 18040))
            .expect("bracketed ipv6 literal");

        assert_eq!(addr.to_string(), "[::1]:18040");
    }

    #[test]
    fn report_socket_addr_accepts_localhost_hostname() {
        let addr = standalone_report_socket_addr(&runtime_endpoint("localhost", 18040))
            .expect("localhost");

        assert_eq!(addr.port(), 18040);
        assert!(addr.ip().is_loopback(), "{addr}");
    }

    #[test]
    fn report_socket_addr_rejects_invalid_host() {
        let err = standalone_report_socket_addr(&runtime_endpoint("bad host with spaces", 18040))
            .expect_err("invalid host must fail");

        assert!(err.contains("invalid standalone report host"), "{err}");
    }

    #[test]
    fn report_socket_addr_rejects_zero_port() {
        let err = RuntimeEndpoint::new("127.0.0.1", 0).expect_err("port 0 must fail");

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    #[test]
    fn report_socket_addr_rejects_too_large_port() {
        let err = RuntimeEndpoint::new("127.0.0.1", 70_000).expect_err("too-large port must fail");

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    fn runtime_endpoint(host: &str, port: i32) -> RuntimeEndpoint {
        RuntimeEndpoint::new(host, port).expect("runtime endpoint")
    }

    fn test_task() -> StandaloneExecStateReportTask {
        test_task_with_ids(QueryId { hi: 501, lo: 601 }, UniqueId { hi: 301, lo: 401 })
    }

    fn test_task_with_ids(query_id: QueryId, finst_id: UniqueId) -> StandaloneExecStateReportTask {
        StandaloneExecStateReportTask {
            finst_id,
            query_id,
            coord: runtime_endpoint("127.0.0.1", 18040),
            report: proto::novarocks::ExecStatusReport {
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                }),
                backend_num: 0,
                status: Some(proto::common::Status {
                    code: 0,
                    message: String::new(),
                }),
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
