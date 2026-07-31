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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use novarocks::UniqueId;
    use novarocks::query_execution::contract::QueryId;
    use novarocks::query_execution::contract_test_support::{
        assert_profile_outcome_preserved, assert_result_outcome_preserved,
        assert_write_commit_outcome_preserved, assert_write_outcome_preserved,
        non_empty_profile_contract_fixture,
        non_empty_profile_contract_fixture_with_query_timeout_seconds,
        non_empty_result_contract_fixture, non_empty_runtime_filter_contract_fixture,
        non_empty_write_contract_fixture,
        non_empty_write_contract_fixture_with_query_timeout_seconds,
    };
    use novarocks::query_execution::fragment_transport::{
        FetchOutcome, FetchedQueryBatch, FragmentDispatcher,
    };
    use novarocks::query_execution::lifecycle::{
        FragmentTerminalOutcome, FragmentTerminalSnapshot, ParticipantBackendIdentity,
        ParticipantManifestDigest, ParticipantRole, QueryAbortRequest, QueryControlAttach,
        QueryControlCommand, QueryControlEvent, QueryControlSession, QueryExecutionId,
        QueryInitAck, QueryInitOutcome, QueryInitRequest, QueryLifecycleTarget,
        QueryLifecycleTransport, QueryLifecycleTransportError, QueryLifecycleTransportErrorKind,
        QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
        QueryStartRequest, QueryTerminalSnapshot, QueryTerminationAck, QueryTerminationReason,
        StageFragment,
    };
    use novarocks::query_execution::write::NativeExecutionReport;

    use crate::coordinator::FrontendDistributedQueryCoordinator;
    use crate::coordinator::query_registry::FrontendQueryRegistry;
    use crate::coordinator::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
    use crate::topology::ClusterBackendService;

    fn report_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19040)
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        submissions: Mutex<Vec<(usize, UniqueId)>>,
        staged_by_backend: Mutex<BTreeMap<usize, Vec<UniqueId>>>,
        fetches: Mutex<Vec<(usize, UniqueId)>>,
        cancellations: Mutex<Vec<(usize, Vec<UniqueId>)>>,
        cancellation_query_ids: Mutex<Vec<QueryId>>,
        outcomes: Mutex<VecDeque<FetchOutcome>>,
        cancel_on_submit: Mutex<
            Option<(
                usize,
                novarocks::query_execution::cancellation::QueryCancellationSource,
            )>,
        >,
        report_on_submit: Mutex<Option<(usize, Box<dyn FnOnce() + Send>)>>,
        terminal_fragments: Mutex<BTreeMap<UniqueId, FragmentTerminalSnapshot>>,
        events: Option<Arc<Mutex<Vec<&'static str>>>>,
    }

    impl RecordingDispatcher {
        fn stage_fragments(&self, backend_idx: usize, fragments: &[StageFragment]) {
            self.staged_by_backend.lock().unwrap().insert(
                backend_idx,
                fragments
                    .iter()
                    .map(StageFragment::fragment_instance_id)
                    .collect(),
            );
        }

        fn start_staged_fragments(&self, backend_idx: usize) {
            let fragments = self
                .staged_by_backend
                .lock()
                .unwrap()
                .remove(&backend_idx)
                .unwrap_or_default();
            for fragment_instance_id in fragments {
                let submission_count = {
                    let mut submissions = self.submissions.lock().unwrap();
                    if let Some(events) = &self.events {
                        events.lock().unwrap().push("start");
                    }
                    submissions.push((backend_idx, fragment_instance_id));
                    submissions.len()
                };
                let should_cancel = self
                    .cancel_on_submit
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|(submit_count, _)| submission_count == *submit_count);
                if should_cancel {
                    if let Some((_, cancellation)) = self.cancel_on_submit.lock().unwrap().take() {
                        let _ = cancellation.request(
                            novarocks::query_execution::cancellation::QueryCancellationReason::ClientDisconnected,
                        );
                    }
                }
                let report_callback = {
                    let mut report_on_submit = self.report_on_submit.lock().unwrap();
                    if report_on_submit
                        .as_ref()
                        .is_some_and(|(submit_count, _)| submission_count == *submit_count)
                    {
                        report_on_submit.take().map(|(_, callback)| callback)
                    } else {
                        None
                    }
                };
                if let Some(report_callback) = report_callback {
                    report_callback();
                }
            }
        }

        fn with_result(batch: FetchedQueryBatch) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                ..Self::default()
            }
        }

        fn with_results(batches: Vec<FetchedQueryBatch>) -> Self {
            let outcomes = batches
                .into_iter()
                .flat_map(|batch| [FetchOutcome::Ready(batch), FetchOutcome::Eof])
                .collect();
            Self {
                outcomes: Mutex::new(outcomes),
                ..Self::default()
            }
        }

        fn with_result_and_cancellation(
            batch: FetchedQueryBatch,
            cancellation: novarocks::query_execution::cancellation::QueryCancellationSource,
            submit_count: usize,
        ) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                cancel_on_submit: Mutex::new(Some((submit_count, cancellation))),
                ..Self::default()
            }
        }

        fn with_cancellation(
            cancellation: novarocks::query_execution::cancellation::QueryCancellationSource,
            submit_count: usize,
        ) -> Self {
            Self {
                cancel_on_submit: Mutex::new(Some((submit_count, cancellation))),
                ..Self::default()
            }
        }

        fn report_on_submit(
            &self,
            submit_count: usize,
            report: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            self.reports_on_submit(submit_count, vec![report], coordinator);
        }

        fn native_wire_report_on_submit(
            &self,
            submit_count: usize,
            report: novarocks_protocol::novarocks::ExecStatusReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    novarocks::query_execution::report::NativeReportHandler::handle_native_report(
                        &handler, report,
                    )
                    .expect("frontend native report ingress accepts the active query");
                }),
            ));
        }

        fn reports_on_submit(
            &self,
            submit_count: usize,
            reports: Vec<NativeExecutionReport>,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            self.record_successful_terminal_fragments(&reports);
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    for report in reports {
                        handler
                            .handle_native_report(report)
                            .expect("frontend report handler accepts active query report");
                    }
                }),
            ));
        }

        fn delayed_reports_on_submit(
            &self,
            submit_count: usize,
            reports: Vec<NativeExecutionReport>,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            self.record_successful_terminal_fragments(&reports);
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    std::thread::spawn(move || {
                        std::thread::sleep(Duration::from_millis(25));
                        for report in reports {
                            handler.handle_native_report(report).expect(
                                "frontend report handler accepts delayed active query report",
                            );
                        }
                    });
                }),
            ));
        }

        fn rejected_report_on_submit(
            &self,
            submit_count: usize,
            report: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    let error = handler
                        .handle_native_report(report)
                        .expect_err("unexpected writer output must be rejected");
                    assert_eq!(
                        error.kind(),
                        novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
                    );
                }),
            ));
        }

        fn accepted_then_rejected_reports_on_submit(
            &self,
            submit_count: usize,
            accepted: NativeExecutionReport,
            rejected: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            self.record_successful_terminal_fragments(std::slice::from_ref(&accepted));
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    handler
                        .handle_native_report(accepted)
                        .expect("first writer final is accepted");
                    let error = handler
                        .handle_native_report(rejected)
                        .expect_err("conflicting writer final must be rejected immediately");
                    assert_eq!(
                        error.kind(),
                        novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
                    );
                }),
            ));
        }

        fn record_successful_terminal_fragments(&self, reports: &[NativeExecutionReport]) {
            let mut terminal_fragments = self.terminal_fragments.lock().unwrap();
            for report in reports {
                let fragment = report
                    .successful_terminal_snapshot_for_contract_test()
                    .expect("contract report produces a terminal fragment fixture");
                terminal_fragments.insert(fragment.fragment_instance_id(), fragment);
            }
        }

        fn terminal_fragment(
            &self,
            fragment_instance_id: UniqueId,
        ) -> Option<FragmentTerminalSnapshot> {
            self.terminal_fragments
                .lock()
                .unwrap()
                .get(&fragment_instance_id)
                .cloned()
        }

        fn backend_loss_on_submit(
            &self,
            submit_count: usize,
            backend_idx: usize,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let activity = coordinator.backend_query_activity();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    assert_eq!(activity.backend_lost(backend_idx).len(), 1);
                }),
            ));
        }
    }

    impl FragmentDispatcher for RecordingDispatcher {
        fn fetch_result(
            &self,
            backend_idx: usize,
            finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<
                novarocks::query_execution::fragment_transport::ExpectedOutputSchemaView<'_>,
            >,
        ) -> Result<FetchOutcome, String> {
            self.fetches.lock().unwrap().push((backend_idx, finst_id));
            Ok(self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(FetchOutcome::Eof))
        }

        fn cancel_fragments(&self, backend_idx: usize, query_id: QueryId, finst_ids: &[UniqueId]) {
            if let Some(events) = &self.events {
                events.lock().unwrap().push("cancel");
            }
            self.cancellation_query_ids.lock().unwrap().push(query_id);
            self.cancellations
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            2
        }

        fn needs_fragment_status_report(&self) -> bool {
            true
        }
    }

    #[derive(Clone, Default)]
    struct TerminalSnapshotFixtureStore {
        participants: Arc<Mutex<BTreeMap<usize, ParticipantTerminalFixture>>>,
    }

    struct ParticipantTerminalFixture {
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        digest: ParticipantManifestDigest,
        fragments: BTreeMap<UniqueId, FragmentTerminalSnapshot>,
    }

    impl TerminalSnapshotFixtureStore {
        fn record_init(&self, backend_idx: usize, request: &QueryInitRequest) {
            self.participants.lock().unwrap().insert(
                backend_idx,
                ParticipantTerminalFixture {
                    execution_id: request.manifest().execution_id(),
                    backend: request.manifest().backend().clone(),
                    digest: request.digest(),
                    fragments: BTreeMap::new(),
                },
            );
        }

        fn record_stage(&self, backend_idx: usize, request: &QueryStageRequest) {
            let mut participants = self.participants.lock().unwrap();
            let participant = participants
                .get_mut(&backend_idx)
                .expect("Stage follows Init in the contract fixture");
            participant.fragments = request
                .fragments()
                .iter()
                .map(|fragment| {
                    let fragment_instance_id = fragment.fragment_instance_id();
                    let fact = FragmentTerminalSnapshot::new(
                        fragment_instance_id,
                        fragment.instance_params().backend_num,
                        FragmentTerminalOutcome::Succeeded,
                        Default::default(),
                        None,
                    )
                    .expect("staged contract fragment produces a terminal fact");
                    (fragment_instance_id, fact)
                })
                .collect();
        }

        fn snapshot(
            &self,
            backend_idx: usize,
            dispatcher: Option<&RecordingDispatcher>,
        ) -> Result<QueryTerminalSnapshot, QueryLifecycleTransportError> {
            let participants = self.participants.lock().unwrap();
            let participant = participants.get(&backend_idx).ok_or_else(|| {
                QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    format!("terminal fixture has no participant for backend {backend_idx}"),
                )
            })?;
            let fragments = participant
                .fragments
                .iter()
                .map(|(fragment_instance_id, staged)| {
                    dispatcher
                        .and_then(|dispatcher| dispatcher.terminal_fragment(*fragment_instance_id))
                        .unwrap_or_else(|| staged.clone())
                })
                .collect();
            QueryTerminalSnapshot::new(
                participant.execution_id,
                participant.backend.clone(),
                participant.digest,
                fragments,
            )
            .map_err(|error| {
                QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::InvalidResponse,
                    error.to_string(),
                )
            })
        }
    }

    struct RecordingLifecycleSession {
        backend_idx: usize,
        terminal_store: TerminalSnapshotFixtureStore,
        dispatcher: Arc<RecordingDispatcher>,
        events: Mutex<VecDeque<QueryControlEvent>>,
    }

    fn ready_and_locally_drained_events() -> VecDeque<QueryControlEvent> {
        VecDeque::from([
            QueryControlEvent::ControlReady,
            QueryControlEvent::LocalDrained,
        ])
    }

    impl QueryControlSession for RecordingLifecycleSession {
        fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
            let event = match command {
                QueryControlCommand::Heartbeat { sequence, .. } => {
                    QueryControlEvent::HeartbeatAck { sequence }
                }
                QueryControlCommand::Abort { .. } => QueryControlEvent::TerminationAccepted {
                    reason: QueryTerminationReason::CoordinatorAbort,
                },
                QueryControlCommand::Finalize => {
                    let snapshot = self
                        .terminal_store
                        .snapshot(self.backend_idx, Some(self.dispatcher.as_ref()))?;
                    let mut events = self.events.lock().unwrap();
                    events.push_back(QueryControlEvent::TerminalSnapshot { snapshot });
                    events.push_back(QueryControlEvent::TerminationAccepted {
                        reason: QueryTerminationReason::CoordinatorFinalize,
                    });
                    return Ok(());
                }
                QueryControlCommand::TerminalAck { .. } => return Ok(()),
            };
            self.events.lock().unwrap().push_back(event);
            Ok(())
        }

        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
            self.events.lock().unwrap().pop_front().ok_or_else(|| {
                QueryLifecycleTransportError::new(
                    novarocks::query_execution::lifecycle::QueryLifecycleTransportErrorKind::DeadlineExceeded,
                    "recording lifecycle session has no pending event",
                )
            })
        }
    }

    struct RecordingLifecycleTransport {
        dispatcher: Arc<RecordingDispatcher>,
        terminal_store: TerminalSnapshotFixtureStore,
    }

    impl RecordingLifecycleTransport {
        fn new(dispatcher: Arc<RecordingDispatcher>) -> Self {
            Self {
                dispatcher,
                terminal_store: TerminalSnapshotFixtureStore::default(),
            }
        }
    }

    impl QueryLifecycleTransport for RecordingLifecycleTransport {
        fn init_query(
            &self,
            target: QueryLifecycleTarget,
            request: QueryInitRequest,
            _timeout: Duration,
        ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
            self.terminal_store
                .record_init(target.backend_idx(), &request);
            Ok(QueryInitAck::new(
                request.manifest().execution_id(),
                request.digest(),
                QueryInitOutcome::Applied,
            ))
        }

        fn attach_control(
            &self,
            target: QueryLifecycleTarget,
            _attach: QueryControlAttach,
            _timeout: Duration,
        ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
            Ok(Arc::new(RecordingLifecycleSession {
                backend_idx: target.backend_idx(),
                terminal_store: self.terminal_store.clone(),
                dispatcher: Arc::clone(&self.dispatcher),
                events: Mutex::new(ready_and_locally_drained_events()),
            }))
        }

        fn stage_fragments(
            &self,
            target: QueryLifecycleTarget,
            request: &QueryStageRequest,
            _timeout: Duration,
        ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
            self.terminal_store
                .record_stage(target.backend_idx(), request);
            self.dispatcher
                .stage_fragments(target.backend_idx(), request.fragments());
            Ok(QueryStageAck::new(
                request.execution_id(),
                request.digest_version(),
                request.digest(),
                QueryStageOutcome::Applied,
                "test participant staged",
            ))
        }

        fn start_prepared_query(
            &self,
            target: QueryLifecycleTarget,
            request: &QueryStartRequest,
            _timeout: Duration,
        ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
            self.dispatcher.start_staged_fragments(target.backend_idx());
            Ok(QueryStartAck::new(
                request.execution_id(),
                request.digest_version(),
                request.digest(),
                QueryStartOutcome::Applied,
                "test participant started",
            ))
        }

        fn abort_query(
            &self,
            _target: QueryLifecycleTarget,
            request: QueryAbortRequest,
            _timeout: Duration,
        ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
            Ok(QueryTerminationAck::new(
                request.execution_id(),
                QueryTerminationReason::CoordinatorAbort,
            ))
        }
    }

    #[derive(Default)]
    struct AllReadyBoundaryState {
        initialized: BTreeSet<usize>,
        ready: BTreeSet<usize>,
        first_submit_entered: bool,
        release_first_submit: bool,
        all_ready_at_first_submit: bool,
        saw_fragment_executor: bool,
        saw_runtime_filter_service: bool,
    }

    #[derive(Clone, Default)]
    struct AllReadyBoundary {
        state: Arc<(Mutex<AllReadyBoundaryState>, Condvar)>,
        terminal_store: TerminalSnapshotFixtureStore,
    }

    struct AllReadySession {
        backend_idx: usize,
        boundary: AllReadyBoundary,
        events: Mutex<VecDeque<QueryControlEvent>>,
    }

    impl QueryControlSession for AllReadySession {
        fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError> {
            let event = match command {
                QueryControlCommand::Heartbeat { sequence, .. } => {
                    QueryControlEvent::HeartbeatAck { sequence }
                }
                QueryControlCommand::Abort { .. } => QueryControlEvent::TerminationAccepted {
                    reason: QueryTerminationReason::CoordinatorAbort,
                },
                QueryControlCommand::Finalize => {
                    let snapshot = self
                        .boundary
                        .terminal_store
                        .snapshot(self.backend_idx, None)?;
                    let mut events = self.events.lock().expect("all-ready events");
                    events.push_back(QueryControlEvent::TerminalSnapshot { snapshot });
                    events.push_back(QueryControlEvent::TerminationAccepted {
                        reason: QueryTerminationReason::CoordinatorFinalize,
                    });
                    drop(events);
                    self.boundary.state.1.notify_all();
                    return Ok(());
                }
                QueryControlCommand::TerminalAck { .. } => return Ok(()),
            };
            self.events
                .lock()
                .expect("all-ready events")
                .push_back(event);
            self.boundary.state.1.notify_all();
            Ok(())
        }

        fn recv_timeout(
            &self,
            timeout: Duration,
        ) -> Result<QueryControlEvent, QueryLifecycleTransportError> {
            let deadline = std::time::Instant::now() + timeout;
            loop {
                if let Some(event) = self.events.lock().expect("all-ready events").pop_front() {
                    if event == QueryControlEvent::ControlReady {
                        let mut state = self.boundary.state.0.lock().expect("all-ready boundary");
                        state.ready.insert(self.backend_idx);
                        drop(state);
                        self.boundary.state.1.notify_all();
                    }
                    return Ok(event);
                }
                if std::time::Instant::now() >= deadline {
                    return Err(QueryLifecycleTransportError::new(
                        novarocks::query_execution::lifecycle::QueryLifecycleTransportErrorKind::DeadlineExceeded,
                        "all-ready session receive timed out",
                    ));
                }
                std::thread::yield_now();
            }
        }
    }

    impl QueryLifecycleTransport for AllReadyBoundary {
        fn init_query(
            &self,
            target: QueryLifecycleTarget,
            request: QueryInitRequest,
            _timeout: Duration,
        ) -> Result<QueryInitAck, QueryLifecycleTransportError> {
            self.terminal_store
                .record_init(target.backend_idx(), &request);
            let mut state = self.state.0.lock().expect("all-ready boundary");
            state.initialized.insert(target.backend_idx());
            state.saw_fragment_executor |= request
                .manifest()
                .roles()
                .contains(&ParticipantRole::FragmentExecutor);
            state.saw_runtime_filter_service |= request
                .manifest()
                .roles()
                .contains(&ParticipantRole::RuntimeFilterService);
            drop(state);
            Ok(QueryInitAck::new(
                request.manifest().execution_id(),
                request.digest(),
                QueryInitOutcome::Applied,
            ))
        }

        fn attach_control(
            &self,
            target: QueryLifecycleTarget,
            _attach: QueryControlAttach,
            _timeout: Duration,
        ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError> {
            Ok(Arc::new(AllReadySession {
                backend_idx: target.backend_idx(),
                boundary: self.clone(),
                events: Mutex::new(ready_and_locally_drained_events()),
            }))
        }

        fn stage_fragments(
            &self,
            target: QueryLifecycleTarget,
            request: &QueryStageRequest,
            _timeout: Duration,
        ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
            self.terminal_store
                .record_stage(target.backend_idx(), request);
            let (lock, ready) = &*self.state;
            let mut state = lock.lock().expect("all-ready boundary");
            if !state.first_submit_entered {
                state.first_submit_entered = true;
                state.all_ready_at_first_submit =
                    !state.initialized.is_empty() && state.ready == state.initialized;
                ready.notify_all();
                while !state.release_first_submit {
                    state = ready.wait(state).expect("release first StageFragments");
                }
            }
            drop(state);
            Ok(QueryStageAck::new(
                request.execution_id(),
                request.digest_version(),
                request.digest(),
                QueryStageOutcome::Applied,
                format!("backend {} staged", target.backend_idx()),
            ))
        }

        fn start_prepared_query(
            &self,
            _target: QueryLifecycleTarget,
            request: &QueryStartRequest,
            _timeout: Duration,
        ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
            Ok(QueryStartAck::new(
                request.execution_id(),
                request.digest_version(),
                request.digest(),
                QueryStartOutcome::Applied,
                "test participant started",
            ))
        }

        fn abort_query(
            &self,
            _target: QueryLifecycleTarget,
            request: QueryAbortRequest,
            _timeout: Duration,
        ) -> Result<QueryTerminationAck, QueryLifecycleTransportError> {
            Ok(QueryTerminationAck::new(
                request.execution_id(),
                QueryTerminationReason::CoordinatorAbort,
            ))
        }
    }

    fn test_coordinator(
        query_id: QueryId,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<RecordingDispatcher>,
    ) -> FrontendDistributedQueryCoordinator {
        let lifecycle = Arc::new(RecordingLifecycleTransport::new(Arc::clone(&dispatcher)));
        FrontendDistributedQueryCoordinator::new_for_test(
            query_id,
            report_endpoint(),
            scheduler,
            dispatcher,
            NonZeroUsize::new(1).unwrap(),
            Arc::new(()),
            lifecycle,
        )
    }

    fn expect_execution_error(
        outcome: Result<
            novarocks::query_execution::contract::DistributedQueryOutcome,
            novarocks::query_execution::contract::DistributedQueryError,
        >,
        context: &str,
    ) -> novarocks::query_execution::contract::DistributedQueryError {
        match outcome {
            Ok(_) => panic!("{context}"),
            Err(error) => error,
        }
    }

    #[test]
    fn frontend_consumes_non_empty_result_contract() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes fixture");

        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn query_control_barrier_precedes_submission() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let boundary = AllReadyBoundary::default();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let lifecycle = Arc::new(RecordingLifecycleTransport::new(Arc::clone(&dispatcher)));
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher,
            NonZeroUsize::new(2).unwrap(),
            Arc::new(()),
            Arc::new(boundary.clone()),
        );
        let execution = std::thread::spawn(move || coordinator.execute(request));

        let (lock, ready) = &*boundary.state;
        let mut state = lock.lock().expect("all-ready boundary");
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !state.first_submit_entered {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "first dispatcher submit was not reached"
            );
            let (next, timeout) = ready
                .wait_timeout(state, remaining)
                .expect("wait for first dispatcher submit");
            state = next;
            assert!(
                !timeout.timed_out() || state.first_submit_entered,
                "first dispatcher submit was not reached"
            );
        }
        assert_eq!(
            state.ready, state.initialized,
            "every materialized participant must emit ControlReady before submission"
        );
        assert!(
            state.all_ready_at_first_submit,
            "the first actual dispatcher submission crossed the all-ready barrier"
        );
        assert!(
            state.initialized.len() >= 2,
            "the production-boundary fixture must cover multiple participants"
        );
        assert!(
            state.saw_fragment_executor && state.saw_runtime_filter_service,
            "the production-boundary fixture must cover fragment and service roles"
        );
        state.release_first_submit = true;
        drop(state);
        ready.notify_all();

        let outcome = execution
            .join()
            .expect("coordinator execution thread")
            .expect("coordinator completes after first submission resumes");
        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
    }

    #[test]
    fn successful_stage_updates_frontend_topology_fragment_telemetry() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let topology = Arc::new(ClusterBackendService::from_captured_targets_for_test(
            &scheduler.live_targets(),
        ));
        let lifecycle = Arc::new(RecordingLifecycleTransport::new(Arc::clone(&dispatcher)));
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_topology(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(()),
            lifecycle,
            topology.clone(),
        );

        coordinator
            .execute(request)
            .expect("frontend executes fixture");

        let submissions = dispatcher.submissions.lock().unwrap().clone();
        for backend_idx in 0..topology.backend_count_for_test() {
            let expected = submissions
                .iter()
                .filter(|(submitted_backend_idx, _)| *submitted_backend_idx == backend_idx)
                .count() as u64;
            assert_eq!(
                topology.scheduled_fragment_count_for_test(backend_idx),
                expected,
                "frontend topology telemetry must count fragments in successful Stage batches per backend"
            );
        }
    }

    #[test]
    fn frontend_native_report_ingress_shares_the_execution_registry() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.successful_fragment_report_proto();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());
        dispatcher.native_wire_report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend accepts native report through the transport port");

        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
    }

    #[test]
    fn frontend_reuses_a_compatible_backend_snapshot_for_each_query() {
        let first = non_empty_result_contract_fixture();
        let backends = first.backends().to_vec();
        let first_batch = first.result_batch();
        let first_request = first.into_request();
        let second = non_empty_result_contract_fixture();
        let second_batch = second.result_batch();
        let second_request = second.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_results(vec![
            first_batch,
            second_batch,
        ]));
        let schedulers = (0..2)
            .map(|_| {
                FrontendFragmentScheduler::new(
                    FrontendBackendSnapshot::for_test(backends.clone()).unwrap(),
                )
            })
            .collect();
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence(
            QueryId::new(41, 73),
            report_endpoint(),
            schedulers,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(()),
            Arc::new(RecordingLifecycleTransport::new(Arc::clone(&dispatcher))),
        );

        assert_result_outcome_preserved(coordinator.execute(first_request).unwrap(), 1).unwrap();
        assert_result_outcome_preserved(coordinator.execute(second_request).unwrap(), 1).unwrap();

        let submitted_backends = dispatcher
            .submissions
            .lock()
            .unwrap()
            .iter()
            .map(|(backend_idx, _)| *backend_idx)
            .collect::<Vec<_>>();
        assert_eq!(submitted_backends, vec![8, 8, 8, 8]);
    }

    #[test]
    fn frontend_returns_non_empty_write_contract() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.successful_non_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.delayed_reports_on_submit(2, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes write fixture");

        assert_write_outcome_preserved(outcome).expect("engine consumes Write payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn duplicate_identical_writer_report_is_idempotent() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.successful_non_writer_report(),
            fixture.successful_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.reports_on_submit(2, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("identical final writer retries must remain idempotent");

        assert_write_outcome_preserved(outcome).expect("duplicate report preserves commit");
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn conflicting_final_writer_retry_fences_the_terminal_attempt() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let successful = fixture.successful_writer_report();
        let conflicting = fixture.conflicting_writer_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.accepted_then_rejected_reports_on_submit(
            2,
            successful,
            conflicting,
            &coordinator,
        );

        let error = expect_execution_error(
            coordinator.execute(request),
            "conflicting writer retry must fence terminal finalization",
        );
        assert!(
            error.message().contains("conflicting final writer output"),
            "{error}"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn multi_writer_conflict_latches_without_waiting_for_the_missing_writer() {
        let fixture = non_empty_write_contract_fixture();
        let successful = fixture.successful_writer_report();
        let writer = successful.fragment_instance_id();
        let conflicting = fixture.conflicting_writer_report();
        let missing_writer = UniqueId {
            hi: 51,
            lo: i64::from(24_u32) << 16,
        };
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(51, 91);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Write,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3, 8]).unwrap();
        registry
            .set_writer_instances(query_id, &[(writer, 0), (missing_writer, 1)])
            .unwrap();
        registry.record_attempt(query_id, 3, writer).unwrap();
        registry.finish_attempt(query_id).unwrap();
        registry
            .record_attempt(query_id, 8, missing_writer)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        registry.record_report(successful).unwrap();
        let error = registry
            .record_report(conflicting)
            .expect_err("conflicting retry is rejected at report ingress");

        assert!(error.message().contains("conflicting final writer output"));
        let progress = registry
            .report_progress(query_id, &[writer, missing_writer])
            .unwrap();
        assert_eq!(progress.0, 1);
        assert!(
            progress
                .1
                .as_deref()
                .is_some_and(|message| message.contains("conflicting final writer output"))
        );
        assert!(
            dispatcher.cancellations.lock().unwrap().is_empty(),
            "query cancellation must not bypass the lifecycle control stream"
        );
        assert_eq!(
            registry.seal_and_take_completion(query_id).unwrap().1.len(),
            1,
            "only the canonical writer final is retained"
        );
    }

    #[test]
    fn late_writer_retry_after_completion_snapshot_is_rejected_without_mutation() {
        let fixture = non_empty_write_contract_fixture();
        let report = fixture.successful_writer_report();
        let retry = fixture.successful_writer_report();
        let writer = report.fragment_instance_id();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(51, 91);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Write,
                dispatcher,
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .set_writer_instances(query_id, &[(writer, 0)])
            .unwrap();
        registry.record_attempt(query_id, 3, writer).unwrap();
        registry.finish_attempt(query_id).unwrap();
        registry.record_report(report).unwrap();

        let (failure, reports) = registry.seal_and_take_completion(query_id).unwrap();
        assert_eq!(failure, None);
        assert_eq!(reports.len(), 1);

        let error = registry
            .record_report(retry)
            .expect_err("late retry after the atomic completion snapshot is rejected");
        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Rejected
        );
        assert_eq!(registry.first_failure(query_id), None);
    }

    #[test]
    fn wrong_backend_writer_identity_fences_the_terminal_attempt() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.wrong_backend_writer_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "writer identity mismatch must fence terminal finalization",
        );
        assert!(
            error
                .message()
                .contains("unknown writer report with write metadata"),
            "{error}"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn failed_non_writer_report_fences_the_terminal_attempt() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.failed_non_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.reports_on_submit(2, reports, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "producer failure must fence terminal finalization",
        );
        assert!(
            error.message().contains("contract producer failure"),
            "{error}"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn non_writer_write_metadata_rejection_fences_the_terminal_attempt() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.non_writer_report_with_write_metadata();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "unexpected writer metadata must fence terminal finalization",
        );
        assert!(
            error
                .message()
                .contains("unknown writer report with write metadata"),
            "{error}"
        );
    }

    #[test]
    fn nonfinal_non_writer_write_metadata_rejection_fences_the_terminal_attempt() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.nonfinal_non_writer_report_with_write_metadata();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "periodic unexpected writer metadata must fence terminal finalization",
        );
        assert!(
            error
                .message()
                .contains("unknown writer report with write metadata"),
            "{error}"
        );
    }

    #[test]
    fn backend_loss_fences_terminal_finalization() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.backend_loss_on_submit(1, 3, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "backend loss must fence terminal finalization",
        );
        assert!(error.message().contains("backend 3 lost"), "{error}");
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn missing_legacy_writer_report_does_not_gate_terminal_snapshot_commit() {
        let fixture = non_empty_write_contract_fixture_with_query_timeout_seconds(0);
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator =
            test_coordinator(QueryId::new(51, 91), scheduler, Arc::clone(&dispatcher));

        let outcome = coordinator
            .execute(request)
            .expect("terminal snapshots are the writer completion source of truth");

        assert_write_commit_outcome_preserved(outcome)
            .expect("terminal snapshots preserve a non-empty writer commit");
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn write_cancellation_after_partial_submit_fences_terminal_finalization() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let cancellation = fixture.cancellation_source();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_cancellation(cancellation, 1));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());

        let error = expect_execution_error(
            coordinator.execute(request),
            "write cancellation must fence terminal finalization",
        );
        assert!(error.message().contains("query cancelled"), "{error}");
        assert!(
            !dispatcher.submissions.lock().unwrap().is_empty(),
            "the cancellation must be observed after at least one StartPreparedQuery release"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_returns_non_empty_profile_contract() {
        let fixture = non_empty_profile_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let reports = fixture.fragment_profile_reports();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(61, 101), scheduler, dispatcher.clone());
        dispatcher.delayed_reports_on_submit(1, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes profile fixture");

        assert_profile_outcome_preserved(outcome, 1)
            .expect("engine consumes non-empty Profile payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn missing_terminal_profile_is_rejected_without_direct_cancellation() {
        let fixture = non_empty_profile_contract_fixture_with_query_timeout_seconds(1);
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator =
            test_coordinator(QueryId::new(61, 101), scheduler, Arc::clone(&dispatcher));

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("profile completion must not succeed without profile reports"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("terminal snapshot is missing its final profile"),
            "{error}"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn profile_progress_requires_profile_payloads_not_only_final_status() {
        let fixture = non_empty_profile_contract_fixture();
        let reports = fixture.fragment_final_reports_without_profiles();
        let expected_instances = reports
            .iter()
            .map(NativeExecutionReport::fragment_instance_id)
            .collect::<Vec<_>>();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(61, 101);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Profile,
                dispatcher,
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        for report in reports {
            registry
                .record_attempt(query_id, 3, report.fragment_instance_id())
                .unwrap();
            registry.finish_attempt(query_id).unwrap();
            registry.record_report(report).unwrap();
        }

        assert_eq!(
            registry
                .report_progress(query_id, &expected_instances)
                .unwrap(),
            (0, None, false)
        );
    }

    #[test]
    fn unattempted_profile_report_is_rejected_without_aggregation_or_cancellation() {
        let fixture = non_empty_profile_contract_fixture();
        let mut reports = fixture.fragment_profile_reports().into_iter();
        let attempted_instance = reports.next().unwrap().fragment_instance_id();
        let unattempted_report = reports.next().unwrap();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(61, 101);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Profile,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .record_attempt(query_id, 3, attempted_instance)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        let error = registry
            .record_report(unattempted_report)
            .expect_err("an unattempted profile instance must be rejected at ingress");

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
        );
        assert!(error.message().contains("unattempted fragment instance"));
        assert_eq!(registry.first_failure(query_id), None);
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(
            registry
                .seal_and_take_completion(query_id)
                .unwrap()
                .1
                .is_empty()
        );
    }

    #[test]
    fn unattempted_failed_report_is_rejected_without_poisoning_the_real_query() {
        let fixture = non_empty_result_contract_fixture();
        let unattempted_report = fixture.failed_fragment_report();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(41, 73);
        let attempted_instance = UniqueId { hi: 999, lo: 1_999 };
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Result,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .record_attempt(query_id, 3, attempted_instance)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        let error = registry
            .record_report(unattempted_report)
            .expect_err("an unattempted failure must be rejected at ingress");

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
        );
        assert!(error.message().contains("unattempted fragment instance"));
        assert_eq!(registry.first_failure(query_id), None);
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_contract_cancels_only_through_lifecycle_control() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let cancellation = fixture.cancellation_source();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result_and_cancellation(
            batch,
            cancellation,
            2,
        ));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("cancellation must stop execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn failed_report_cancels_via_lifecycle_control_without_local_cleanup() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.failed_fragment_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());
        dispatcher.report_on_submit(2, report, &coordinator);

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("failed native report must fail execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);

        let write_fixture = non_empty_write_contract_fixture();
        let backends = write_fixture.backends().to_vec();
        let report = write_fixture.failed_writer_report();
        let request = write_fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.report_on_submit(2, report, &coordinator);

        let error = expect_execution_error(
            coordinator.execute(request),
            "failed writer report fences terminal finalization",
        );

        assert!(
            error.message().contains("contract writer failure"),
            "{error}"
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn real_frontend_report_handler_uses_lifecycle_control_only() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.failed_fragment_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::for_test(backends).unwrap());
        let lifecycle = Arc::new(RecordingLifecycleTransport::new(Arc::clone(&dispatcher)));
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(()),
            lifecycle,
        );
        dispatcher.report_on_submit(2, report, &coordinator);

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("failed native report must fail the active query"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }
}
