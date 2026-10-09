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

use super::*;
use crate::query_execution::completion_facts::{
    CompletionFactOwners, FrontendStatisticsFacts, StatementFactScope,
};
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::preparation::StatisticsFactPort;
use novarocks_workload_control::{CancellationReason, ResultWindowClass};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

struct HeldReadExit {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    exited: Arc<AtomicBool>,
}
impl Drop for HeldReadExit {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        self.exited.store(true, Ordering::Release);
    }
}

struct HeldStatisticsReader {
    identity: Arc<ContextObservingProvider>,
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    exit: Mutex<Option<HeldReadExit>>,
    reads: AtomicUsize,
    stop_seen: AtomicBool,
    exact_request: AtomicBool,
}
impl StatisticsReader for HeldStatisticsReader {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.identity.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.identity.incarnation
    }
    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        let _exit = self.exit.lock().unwrap().take();
        self.exact_request.store(
            request.table.owner() == &self.identity.descriptor.instance_id
                && request.table.payload().as_ref() == b"orders"
                && request.data_version
                    == StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap()
                && request.metrics.metrics() == [StatisticsMetric::RowCount]
                && request.narrowed_read.is_none(),
            Ordering::Release,
        );
        let _ = self.started.send(());
        let _ = self.release.lock().unwrap().recv();
        let stopped = request.context.is_cancelled();
        self.stop_seen.store(stopped, Ordering::Release);
        Err(ConnectorError::new(
            if stopped {
                ConnectorErrorKind::Cancelled
            } else {
                ConnectorErrorKind::Internal
            },
            "held statistics reader reached its actual stop check",
        ))
    }
}
impl ConnectorStatistics for HeldStatisticsReader {}

fn held_statistics_binding(provider: Arc<HeldStatisticsReader>) -> QueryTableBinding {
    let identity = &provider.identity;
    let control = Arc::new(
        ConnectorControlBinding::try_new_with_statistics(
            identity.descriptor.clone(),
            identity.incarnation,
            identity.clone(),
            identity.clone(),
            identity.clone(),
            None,
            Some(provider.clone()),
        )
        .expect("exact statistics fixture control"),
    );
    let mut binding = local_binding("ice.main", "db", "orders", 173);
    binding.statistics_pin = Some(crate::connector::backend::ResolvedTableStatisticsPin {
        table: ConnectorTableHandle::try_new(
            identity.descriptor.instance_id.clone(),
            Bytes::from_static(b"orders"),
        )
        .unwrap(),
        data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap(),
    });
    binding.admission =
        QueryTableBindingAdmission::Exact(ConnectorControlPlanningLease::new(control, || {}));
    binding
}

fn empty_completion_owners(
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
) -> CompletionFactOwners {
    let inventory = novarocks_mv_application::readiness::MvReadinessService::new(
        Arc::new(novarocks_mv_application::test_repository::InMemoryMvRepository::default()),
        Arc::new(novarocks_mv_application::process_runtime::ProcessRuntime::default()),
    );
    CompletionFactOwners::new(
        Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
        None,
        Arc::new(novarocks_catalog_application::ConnectorControlHost::new()),
        Arc::new(UnifiedStatisticsResolver::default()),
        crate::mv::domain::readiness::MvCandidateReader::new(inventory.candidate_reader(), runtime),
        Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
        blocking,
    )
}

fn observe_real_statistics_port(class: ResultWindowClass) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let blocking = crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
        runtime.handle().clone(),
    );
    let (control, root, window) = crate::task_execution::blocking_io::tests::admitted_class(class);
    let capacity =
        QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
    let (started, entered) = mpsc::channel();
    let (release_call, held_call) = mpsc::channel();
    let (exit_started, exit_entered) = mpsc::channel();
    let (release_exit, held_exit) = mpsc::channel();
    let exited = Arc::new(AtomicBool::new(false));
    // Rescue exists before the original actual provider callback is submitted.
    let (disarm, watch) = mpsc::channel();
    let rescue_call = release_call.clone();
    let rescue_exit = release_exit.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(6)).is_err() {
            let _ = rescue_call.send(());
            let _ = rescue_exit.send(());
        }
    });
    let provider = Arc::new(HeldStatisticsReader {
        identity: observing_provider(),
        started,
        release: Mutex::new(held_call),
        exit: Mutex::new(Some(HeldReadExit {
            entered: exit_started,
            release: held_exit,
            exited: Arc::clone(&exited),
        })),
        reads: AtomicUsize::new(0),
        stop_seen: AtomicBool::new(false),
        exact_request: AtomicBool::new(false),
    });
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let binding = held_statistics_binding(provider.clone());
    let binding_id = bindings
        .resolve_or_insert(
            crate::catalog_application::query_bindings::QueryTableBindingKey::strict_base(
                "ice.main", "db", "orders",
            ),
            || Ok(binding),
        )
        .expect("original binding store token");
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let scope = StatementFactScope::new(
        bindings.clone(),
        context,
        Some("ice.main"),
        &root.owner.scope(),
        capacity.clone(),
    )
    .unwrap();
    let owners = empty_completion_owners(blocking.clone(), runtime.handle().clone());
    let port = FrontendStatisticsFacts::new(owners, scope);
    let need = novarocks_sql::compiler::fixtures::statistics_need(
        1,
        binding_id,
        vec![StatisticsMetric::RowCount],
    )
    .unwrap();
    let delivered = Arc::new(AtomicBool::new(false));
    let published = Arc::clone(&delivered);
    let mut waiter = runtime.spawn(async move {
        let answer = port.resolve_statistics(&[need]).await;
        published.store(true, Ordering::Release);
        answer
    });
    let call_entered = entered.recv_timeout(Duration::from_secs(2)).is_ok();
    stop.request_stop();
    root.owner.cancel(CancellationReason::Requested);
    // Exercise the production-derived local child Cancel, without test ack.
    control.expire_deadlines();
    waiter.abort();
    let waiter_join =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await });
    let waiter_cancelled = matches!(waiter_join, Ok(Err(ref e)) if e.is_cancelled());
    drop(waiter_join);
    drop(waiter);
    drop(capacity);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drop(window);
    let during_call = control.snapshot();
    let _ = release_call.send(());
    let drop_entered = exit_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let during_exit = control.snapshot();
    let still_held = !exited.load(Ordering::Acquire);
    let _ = release_exit.send(());
    let _ = disarm.send(());
    let watchdog_joined = watchdog.join().is_ok();
    let deadline = Instant::now() + Duration::from_secs(2);
    let after = loop {
        let facts = control.snapshot();
        if facts.scopes.is_empty() || Instant::now() >= deadline {
            break facts;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    control.close_admission();
    let workload_exit = control.shutdown();
    // No arbitrary error/panic payload formatting, and no synthetic completion.
    if let Some(error) = blocking.take_original_failure() {
        blocking.retire_original_failure(error);
    }
    drop(bindings);
    drop(blocking);
    drop(runtime);

    assert!(call_entered, "original provider callback was not entered");
    assert!(
        drop_entered,
        "original provider exit destructor was not entered"
    );
    assert!(still_held, "original provider exit was not held");
    assert!(
        waiter_cancelled,
        "original caller did not join as cancelled"
    );
    assert!(watchdog_joined, "rescue watchdog did not join");
    let expected = if class == ResultWindowClass::Local {
        [0, 1, 0, 0]
    } else {
        [0, 0, 1, 0]
    };
    for facts in [&during_call, &during_exit] {
        assert_eq!(facts.root_responsibilities, 1);
        assert_eq!(facts.result_windows.held_positions, expected);
        assert!(
            facts
                .scopes
                .iter()
                .any(|s| s.parent.is_some() && !s.own_completed),
            "real StatisticsFacts call omitted its admitted local child"
        );
    }
    assert_eq!(provider.reads.load(Ordering::Acquire), 1);
    assert!(provider.exact_request.load(Ordering::Acquire));
    assert!(provider.stop_seen.load(Ordering::Acquire));
    assert!(exited.load(Ordering::Acquire));
    assert!(
        !delivered.load(Ordering::Acquire),
        "abandoned statement accepted a late statistics fact"
    );
    assert_eq!(after.result_windows.held_positions, [0; 4]);
    assert!(after.scopes.is_empty());
    assert!(
        workload_exit.is_ok(),
        "same original workload did not drain"
    );
}

#[test]
fn actual_statistics_port_keeps_local_window_through_stop_and_original_callback_exit() {
    observe_real_statistics_port(ResultWindowClass::Local);
}
#[test]
fn actual_statistics_port_keeps_internal_window_through_stop_and_original_callback_exit() {
    observe_real_statistics_port(ResultWindowClass::Internal);
}
