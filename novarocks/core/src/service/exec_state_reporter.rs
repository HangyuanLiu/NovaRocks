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

#![cfg(feature = "compat")]

use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::UniqueId;
use crate::common::config;
use crate::novarocks_logging::{debug, warn};
use crate::runtime::query_context::QueryId;
use crate::service::fe_report_compat as fe_report;
use crate::service::frontend_rpc::{FrontendRpcError, FrontendRpcKind, FrontendRpcManager};
use crate::thrift::frontend_service;
use crate::thrift::types;

const NORMAL_REPORT_QUEUE_LIMIT: usize = 1_000;

#[derive(Clone, Debug)]
pub(crate) struct ExecStateReportTask {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) coord: types::TNetworkAddress,
    pub(crate) params: frontend_service::TReportExecStatusParams,
}

#[derive(Clone, Copy)]
struct ExecStateReporterSettings {
    normal_threads: usize,
    priority_threads: usize,
    final_retry_limit: usize,
    batch_flush_interval: Duration,
    batch_max_size: usize,
}

impl ExecStateReporterSettings {
    fn from_config() -> Self {
        Self {
            normal_threads: config::exec_state_report_max_threads(),
            priority_threads: config::priority_exec_state_report_max_threads(),
            final_retry_limit: config::report_exec_rpc_request_retry_num(),
            batch_flush_interval: Duration::from_millis(
                config::report_exec_batch_flush_interval_ms(),
            ),
            batch_max_size: config::report_exec_batch_max_size(),
        }
    }
}

#[derive(Default)]
struct NormalQueue {
    state: Mutex<NormalQueueState>,
    cv: Condvar,
}

#[derive(Default)]
struct NormalQueueState {
    pending_by_fe: HashMap<String, PendingBatch>,
    total_pending: usize,
}

struct PendingBatch {
    reports: HashMap<UniqueId, PendingReport>,
}

struct PendingReport {
    enqueued_at: Instant,
    task: ExecStateReportTask,
}

#[derive(Default)]
struct PriorityQueue {
    state: Mutex<VecDeque<ExecStateReportTask>>,
    cv: Condvar,
}

pub(crate) struct ExecStateReporter {
    settings: ExecStateReporterSettings,
    normal: NormalQueue,
    priority: PriorityQueue,
    started: OnceLock<()>,
}

impl ExecStateReporter {
    fn new() -> Self {
        Self {
            settings: ExecStateReporterSettings::from_config(),
            normal: NormalQueue::default(),
            priority: PriorityQueue::default(),
            started: OnceLock::new(),
        }
    }

    fn shared() -> &'static Self {
        static INSTANCE: OnceLock<ExecStateReporter> = OnceLock::new();
        INSTANCE.get_or_init(ExecStateReporter::new)
    }

    fn ensure_started(&'static self) {
        self.started.get_or_init(|| {
            for idx in 0..self.settings.normal_threads {
                std::thread::Builder::new()
                    .name(format!("fe-report-batch-{idx}"))
                    .spawn(move || run_normal_worker(self))
                    .expect("start FE batch report worker");
            }
            for idx in 0..self.settings.priority_threads {
                std::thread::Builder::new()
                    .name(format!("fe-report-final-{idx}"))
                    .spawn(move || run_priority_worker(self))
                    .expect("start FE final report worker");
            }
        });
    }

    fn enqueue_non_final(&self, task: ExecStateReportTask) -> Result<(), String> {
        let fe_key = format_addr(&task.coord);
        let mut guard = self.normal.state.lock().expect("normal report queue lock");
        let needs_new_entry = guard
            .pending_by_fe
            .get(&fe_key)
            .and_then(|batch| batch.reports.get(&task.finst_id))
            .is_none();
        if needs_new_entry && guard.total_pending >= NORMAL_REPORT_QUEUE_LIMIT {
            return Err(format!(
                "ExecStateReporter normal queue is full: limit={NORMAL_REPORT_QUEUE_LIMIT}"
            ));
        }
        let batch = guard
            .pending_by_fe
            .entry(fe_key)
            .or_insert_with(|| PendingBatch {
                reports: HashMap::new(),
            });
        if let Some(existing) = batch.reports.get_mut(&task.finst_id) {
            existing.task = task;
        } else {
            batch.reports.insert(
                task.finst_id,
                PendingReport {
                    enqueued_at: Instant::now(),
                    task,
                },
            );
            guard.total_pending += 1;
        }
        self.normal.cv.notify_one();
        Ok(())
    }

    fn enqueue_final(&self, task: ExecStateReportTask) {
        let mut guard = self
            .priority
            .state
            .lock()
            .expect("priority report queue lock");
        guard.push_back(task);
        self.priority.cv.notify_one();
    }

    fn take_normal_batch(&self) -> Vec<ExecStateReportTask> {
        let mut guard = self.normal.state.lock().expect("normal report queue lock");
        loop {
            if let Some(fe_key) = select_ready_batch_key(
                &guard.pending_by_fe,
                Instant::now(),
                self.settings.batch_flush_interval,
                self.settings.batch_max_size,
            ) {
                let mut batch = guard
                    .pending_by_fe
                    .remove(&fe_key)
                    .expect("selected FE batch exists");
                let finst_ids = batch
                    .reports
                    .keys()
                    .copied()
                    .take(self.settings.batch_max_size)
                    .collect::<Vec<_>>();
                let mut tasks = Vec::with_capacity(finst_ids.len());
                let mut removed = 0usize;
                for finst_id in finst_ids {
                    if let Some(report) = batch.reports.remove(&finst_id) {
                        tasks.push(report.task);
                        removed += 1;
                    }
                }
                guard.total_pending = guard.total_pending.saturating_sub(removed);
                if !batch.reports.is_empty() {
                    guard.pending_by_fe.insert(fe_key, batch);
                }
                return tasks;
            }

            if guard.pending_by_fe.is_empty() {
                guard = self
                    .normal
                    .cv
                    .wait(guard)
                    .expect("normal report queue wait");
                continue;
            }

            let wait_for = next_batch_wait_duration(
                &guard.pending_by_fe,
                Instant::now(),
                self.settings.batch_flush_interval,
            );
            let (next_guard, _) = self
                .normal
                .cv
                .wait_timeout(guard, wait_for)
                .expect("normal report queue wait_timeout");
            guard = next_guard;
        }
    }

    fn take_final_task(&self) -> ExecStateReportTask {
        let mut guard = self
            .priority
            .state
            .lock()
            .expect("priority report queue lock");
        loop {
            if let Some(task) = guard.pop_front() {
                return task;
            }
            guard = self
                .priority
                .cv
                .wait(guard)
                .expect("priority report queue wait");
        }
    }
}

pub(crate) fn ensure_started() {
    ExecStateReporter::shared().ensure_started();
}

pub(crate) fn enqueue_non_final(task: ExecStateReportTask) -> Result<(), String> {
    let reporter = ExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_non_final(task)
}

pub(crate) fn enqueue_final(task: ExecStateReportTask) {
    let reporter = ExecStateReporter::shared();
    reporter.ensure_started();
    reporter.enqueue_final(task);
}

fn run_normal_worker(reporter: &'static ExecStateReporter) {
    loop {
        let batch = reporter.take_normal_batch();
        if batch.is_empty() {
            continue;
        }
        send_non_final_batch(batch);
    }
}

fn run_priority_worker(reporter: &'static ExecStateReporter) {
    loop {
        let task = reporter.take_final_task();
        send_final_report(task, reporter.settings.final_retry_limit);
    }
}

fn send_non_final_batch(tasks: Vec<ExecStateReportTask>) {
    let coord = tasks
        .first()
        .map(|task| task.coord.clone())
        .expect("non-final report batch is non-empty");
    let params = frontend_service::TBatchReportExecStatusParams::new(
        tasks.iter().map(|task| task.params.clone()).collect(),
    );
    let result = FrontendRpcManager::shared().call(FrontendRpcKind::ExecStatus, &coord, |client| {
        client
            .batch_report_exec_status(params.clone())
            .map_err(FrontendRpcError::from_thrift)
    });
    match result {
        Ok(response) => handle_batch_response(tasks, response.status_list),
        Err(err) => {
            warn!(
                target: "novarocks::report",
                fe_addr = %format_addr(&coord),
                batch_size = tasks.len(),
                error = %err,
                "batchReportExecStatus failed"
            );
        }
    }
}

fn handle_batch_response(
    tasks: Vec<ExecStateReportTask>,
    status_list: Option<Vec<crate::thrift::status::TStatus>>,
) {
    let statuses = status_list.unwrap_or_default();
    if !statuses.is_empty() && statuses.len() != tasks.len() {
        warn!(
            target: "novarocks::report",
            expected = tasks.len(),
            actual = statuses.len(),
            "batchReportExecStatus returned mismatched status count"
        );
    }

    for (idx, task) in tasks.into_iter().enumerate() {
        let Some(status) = statuses.get(idx) else {
            continue;
        };
        if status.status_code == crate::thrift::status_code::TStatusCode::OK {
            continue;
        }
        if fe_report::is_query_gone_status(status) {
            fe_report::mark_fe_query_gone(task.finst_id);
            debug!(
                target: "novarocks::report",
                finst_id = %task.finst_id,
                query_id = %task.query_id,
                "suppress future reportExecStatus because FE query is already gone"
            );
            continue;
        }
        warn!(
            target: "novarocks::report",
            finst_id = %task.finst_id,
            query_id = %task.query_id,
            status = ?status,
            "batchReportExecStatus returned non-OK status"
        );
    }
}

fn send_final_report(task: ExecStateReportTask, retry_limit: usize) {
    send_final_report_with(
        task,
        retry_limit,
        |task| {
            FrontendRpcManager::shared()
                .call(FrontendRpcKind::ExecStatus, &task.coord, |client| {
                    client
                        .report_exec_status(task.params.clone())
                        .map_err(FrontendRpcError::from_thrift)
                })
                .map(|response| response.status)
        },
        std::thread::sleep,
    );
}

fn send_final_report_with<F, S>(
    task: ExecStateReportTask,
    retry_limit: usize,
    mut send: F,
    mut sleep: S,
) where
    F: FnMut(
        &ExecStateReportTask,
    ) -> Result<Option<crate::thrift::status::TStatus>, FrontendRpcError>,
    S: FnMut(Duration),
{
    let retry_limit = retry_limit.max(1);
    for attempt in 1..=retry_limit {
        match send(&task) {
            Ok(Some(status)) => {
                if status.status_code == crate::thrift::status_code::TStatusCode::OK {
                    return;
                }
                if fe_report::is_query_gone_status(&status) {
                    fe_report::mark_fe_query_gone(task.finst_id);
                    debug!(
                        target: "novarocks::report",
                        finst_id = %task.finst_id,
                        query_id = %task.query_id,
                        "skip final reportExecStatus because FE query is already gone"
                    );
                    return;
                }
                warn!(
                    target: "novarocks::report",
                    finst_id = %task.finst_id,
                    query_id = %task.query_id,
                    attempt,
                    status = ?status,
                    "final reportExecStatus returned non-OK status"
                );
            }
            Ok(None) => {
                return;
            }
            Err(err) => {
                warn!(
                    target: "novarocks::report",
                    finst_id = %task.finst_id,
                    query_id = %task.query_id,
                    attempt,
                    error = %err,
                    "final reportExecStatus failed"
                );
            }
        }

        if attempt < retry_limit {
            sleep(backoff_for_attempt(attempt));
        }
    }
}

fn select_ready_batch_key(
    pending_by_fe: &HashMap<String, PendingBatch>,
    now: Instant,
    flush_interval: Duration,
    batch_max_size: usize,
) -> Option<String> {
    if let Some((fe_key, _)) = pending_by_fe
        .iter()
        .find(|(_, batch)| batch.reports.len() >= batch_max_size)
    {
        return Some(fe_key.clone());
    }

    pending_by_fe
        .iter()
        .filter_map(|(fe_key, batch)| {
            oldest_report_at(batch).and_then(|oldest| {
                (now.saturating_duration_since(oldest) >= flush_interval)
                    .then_some((fe_key.clone(), oldest))
            })
        })
        .min_by_key(|(_, oldest)| *oldest)
        .map(|(fe_key, _)| fe_key)
}

fn next_batch_wait_duration(
    pending_by_fe: &HashMap<String, PendingBatch>,
    now: Instant,
    flush_interval: Duration,
) -> Duration {
    pending_by_fe
        .values()
        .filter_map(oldest_report_at)
        .map(|oldest| {
            let deadline = oldest + flush_interval;
            deadline.saturating_duration_since(now)
        })
        .min()
        .unwrap_or(flush_interval)
}

fn oldest_report_at(batch: &PendingBatch) -> Option<Instant> {
    batch
        .reports
        .values()
        .map(|report| report.enqueued_at)
        .min()
}

fn backoff_for_attempt(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(100),
        2 => Duration::from_millis(200),
        3 => Duration::from_millis(400),
        4 => Duration::from_millis(800),
        5 => Duration::from_millis(1_600),
        _ => Duration::from_millis(2_000),
    }
}

fn format_addr(addr: &types::TNetworkAddress) -> String {
    format!("{}:{}", addr.hostname, addr.port)
}
