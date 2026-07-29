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
use std::collections::VecDeque;
#[cfg(test)]
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks::query_execution::artifact::PreparedNativeExecutionParts;
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::cancellation::QueryCancellationView;
use novarocks::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest, ProfileReportBuilder,
    QueryId,
};
use novarocks::query_execution::fragment_transport::{
    FetchOutcome, FragmentDispatcher, new_grpc_fragment_dispatcher,
};
use novarocks::query_execution::lifecycle::{
    AttemptId, QueryExecutionId, QueryInitOptions, QueryLifecycleTransport,
};
use novarocks::query_execution::write::WriteReportBuilder;
use novarocks::service::grpc_query_lifecycle_client::new_grpc_query_lifecycle_transport;

use super::backend_events::BackendQueryActivity;
use super::query_lifecycle::{FrontendQueryLifecycleBarrier, FrontendQueryLifecycleConfig};
use super::query_registry::FrontendQueryRegistry;
use super::report::FrontendCoordinatorReportHandler;
use super::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};

trait QueryIdSource: Send + Sync + 'static {
    fn next_query_id(&self) -> QueryId;
}

struct UniqueQueryIdSource {
    next_low: AtomicI64,
}

impl Default for UniqueQueryIdSource {
    fn default() -> Self {
        Self {
            next_low: AtomicI64::new(100),
        }
    }
}

impl QueryIdSource for UniqueQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
        QueryId::new(
            high as i64,
            self.next_low.fetch_add(1_000, Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
struct FixedQueryIdSource(QueryId);

#[cfg(test)]
impl QueryIdSource for FixedQueryIdSource {
    fn next_query_id(&self) -> QueryId {
        self.0
    }
}

pub(crate) struct FrontendLiveBackendTopology {
    state: Mutex<FrontendLiveBackendTopologyState>,
}

struct FrontendLiveBackendTopologyState {
    revision: u64,
    live: Vec<LiveBackendTarget>,
}

impl FrontendLiveBackendTopology {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(FrontendLiveBackendTopologyState {
                revision: 0,
                live: Vec::new(),
            }),
        }
    }

    fn snapshot(&self) -> Vec<LiveBackendTarget> {
        self.state
            .lock()
            .expect("frontend live backend topology lock")
            .live
            .clone()
    }

    pub(crate) fn replace(&self, revision: u64, live: Vec<LiveBackendTarget>) {
        let mut state = self
            .state
            .lock()
            .expect("frontend live backend topology lock");
        if revision >= state.revision {
            state.revision = revision;
            state.live = live;
        }
    }
}

struct FrontendReportEndpointBinding {
    advertised_host: String,
    configured_port: u16,
    bound_port: AtomicU16,
}

impl FrontendReportEndpointBinding {
    fn new(advertised_host: String, configured_port: u16) -> Self {
        Self {
            advertised_host,
            configured_port,
            bound_port: AtomicU16::new(0),
        }
    }

    #[cfg(test)]
    fn from_socket_addr(endpoint: SocketAddr) -> Self {
        Self::new(endpoint.ip().to_string(), endpoint.port())
    }

    fn resolve(
        &self,
    ) -> Result<novarocks::query_execution::backend::CoordinatorReportEndpoint, DistributedQueryError>
    {
        let port = if self.configured_port == 0 {
            let bound = self.bound_port.load(Ordering::Acquire);
            if bound == 0 {
                return Err(failed(
                    "frontend coordinator report endpoint is not bound yet",
                ));
            }
            bound
        } else {
            self.configured_port
        };
        novarocks::query_execution::backend::CoordinatorReportEndpoint::new(
            self.advertised_host.clone(),
            port,
        )
        .map_err(failed)
    }
}

impl novarocks::query_execution::backend::CoordinatorReportEndpointSink
    for FrontendReportEndpointBinding
{
    fn set_bound_port(&self, port: u16) {
        self.bound_port.store(port, Ordering::Release);
    }
}

enum BackendServicesSource {
    Live(Arc<FrontendLiveBackendTopology>),
    #[cfg(test)]
    Fixed {
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    },
    #[cfg(test)]
    Sequence {
        schedulers: Mutex<VecDeque<FrontendFragmentScheduler>>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    },
}

struct QueryBackendServices {
    scheduler: FrontendFragmentScheduler,
    dispatcher: Arc<dyn FragmentDispatcher>,
    lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    live_backends: Vec<LiveBackendTarget>,
}

#[cfg(test)]
pub(crate) fn unavailable_lifecycle_transport_for_test() -> Arc<dyn QueryLifecycleTransport> {
    Arc::new(UnavailableLifecycleTransportForTest)
}

#[cfg(test)]
pub(crate) fn ready_lifecycle_transport_for_test() -> Arc<dyn QueryLifecycleTransport> {
    Arc::new(ReadyLifecycleTransportForTest)
}

#[cfg(test)]
struct UnavailableLifecycleTransportForTest;

#[cfg(test)]
struct ReadyLifecycleTransportForTest;

#[cfg(test)]
struct ReadyLifecycleSessionForTest {
    events: Mutex<VecDeque<novarocks::query_execution::lifecycle::QueryControlEvent>>,
}

#[cfg(test)]
impl novarocks::query_execution::lifecycle::QueryControlSession for ReadyLifecycleSessionForTest {
    fn send(
        &self,
        command: novarocks::query_execution::lifecycle::QueryControlCommand,
    ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleTransportError> {
        use novarocks::query_execution::lifecycle::{
            QueryControlCommand, QueryControlEvent, QueryTerminationReason,
        };
        let event = match command {
            QueryControlCommand::Heartbeat { sequence, .. } => {
                QueryControlEvent::HeartbeatAck { sequence }
            }
            QueryControlCommand::Abort { .. } => QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort,
            },
            QueryControlCommand::Finalize => QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            },
        };
        self.events
            .lock()
            .expect("ready lifecycle session")
            .push_back(event);
        Ok(())
    }

    fn recv_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryControlEvent,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        self.events
            .lock()
            .expect("ready lifecycle session")
            .pop_front()
            .ok_or_else(|| {
                novarocks::query_execution::lifecycle::QueryLifecycleTransportError::new(
                    novarocks::query_execution::lifecycle::QueryLifecycleTransportErrorKind::DeadlineExceeded,
                    "ready lifecycle session has no pending event",
                )
            })
    }
}

#[cfg(test)]
impl QueryLifecycleTransport for ReadyLifecycleTransportForTest {
    fn init_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: novarocks::query_execution::lifecycle::QueryInitRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryInitAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(novarocks::query_execution::lifecycle::QueryInitAck::new(
            request.manifest().execution_id(),
            request.digest(),
            novarocks::query_execution::lifecycle::QueryInitOutcome::Applied,
        ))
    }

    fn attach_control(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        _attach: novarocks::query_execution::lifecycle::QueryControlAttach,
        _timeout: Duration,
    ) -> Result<
        Arc<dyn novarocks::query_execution::lifecycle::QueryControlSession>,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(Arc::new(ReadyLifecycleSessionForTest {
            events: Mutex::new(VecDeque::from([
                novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady,
            ])),
        }))
    }

    fn abort_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        request: novarocks::query_execution::lifecycle::QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryTerminationAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(
            novarocks::query_execution::lifecycle::QueryTerminationAck::new(
                request.execution_id(),
                novarocks::query_execution::lifecycle::QueryTerminationReason::CoordinatorAbort,
            ),
        )
    }
}

#[cfg(test)]
impl QueryLifecycleTransport for UnavailableLifecycleTransportForTest {
    fn init_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        _request: novarocks::query_execution::lifecycle::QueryInitRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryInitAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Err(unavailable_lifecycle_transport_error_for_test())
    }

    fn attach_control(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        _attach: novarocks::query_execution::lifecycle::QueryControlAttach,
        _timeout: Duration,
    ) -> Result<
        Arc<dyn novarocks::query_execution::lifecycle::QueryControlSession>,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Err(unavailable_lifecycle_transport_error_for_test())
    }

    fn abort_query(
        &self,
        _target: novarocks::query_execution::lifecycle::QueryLifecycleTarget,
        _request: novarocks::query_execution::lifecycle::QueryAbortRequest,
        _timeout: Duration,
    ) -> Result<
        novarocks::query_execution::lifecycle::QueryTerminationAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Err(unavailable_lifecycle_transport_error_for_test())
    }
}

#[cfg(test)]
fn unavailable_lifecycle_transport_error_for_test()
-> novarocks::query_execution::lifecycle::QueryLifecycleTransportError {
    novarocks::query_execution::lifecycle::QueryLifecycleTransportError::new(
        novarocks::query_execution::lifecycle::QueryLifecycleTransportErrorKind::Unavailable,
        "test lifecycle transport was not injected",
    )
}

impl BackendServicesSource {
    fn resolve(&self) -> Result<QueryBackendServices, DistributedQueryError> {
        match self {
            Self::Live(topology) => {
                let targets = topology.snapshot();
                let entries = targets
                    .iter()
                    .map(|target| (target.backend_idx(), target.endpoint()))
                    .collect::<Vec<_>>();
                let lifecycle_transport =
                    new_grpc_query_lifecycle_transport(&targets).map_err(failed)?;
                let snapshot = FrontendBackendSnapshot::from_live_targets(targets.clone())?;
                let dispatcher = new_grpc_fragment_dispatcher(&entries).map_err(failed)?;
                Ok(QueryBackendServices {
                    scheduler: FrontendFragmentScheduler::new(snapshot),
                    dispatcher,
                    lifecycle_transport,
                    live_backends: targets,
                })
            }
            #[cfg(test)]
            Self::Fixed {
                scheduler,
                dispatcher,
                lifecycle_transport,
            } => Ok(QueryBackendServices {
                scheduler: scheduler.clone(),
                dispatcher: Arc::clone(dispatcher),
                lifecycle_transport: Arc::clone(lifecycle_transport),
                live_backends: scheduler.live_targets(),
            }),
            #[cfg(test)]
            Self::Sequence {
                schedulers,
                dispatcher,
                lifecycle_transport,
            } => {
                let scheduler = schedulers
                    .lock()
                    .expect("frontend test backend sequence lock")
                    .pop_front()
                    .expect("frontend test backend sequence exhausted");
                let live_backends = scheduler.live_targets();
                Ok(QueryBackendServices {
                    scheduler,
                    dispatcher: Arc::clone(dispatcher),
                    lifecycle_transport: Arc::clone(lifecycle_transport),
                    live_backends,
                })
            }
        }
    }
}

pub struct FrontendDistributedQueryCoordinator {
    report_endpoint: Arc<FrontendReportEndpointBinding>,
    live_topology: Arc<FrontendLiveBackendTopology>,
    backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    backend_services: BackendServicesSource,
    runtime_filter_worker_count: NonZeroUsize,
    query_ids: Arc<dyn QueryIdSource>,
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendDistributedQueryCoordinator {
    pub fn new(
        advertised_report_host: String,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    ) -> Self {
        let live_topology = Arc::new(FrontendLiveBackendTopology::new());
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::new(
                advertised_report_host,
                configured_report_port,
            )),
            live_topology: Arc::clone(&live_topology),
            backend_topology,
            backend_services: BackendServicesSource::Live(live_topology),
            runtime_filter_worker_count,
            query_ids: Arc::new(UniqueQueryIdSource::default()),
            registry: Arc::new(FrontendQueryRegistry::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    ) -> Self {
        Self::new_for_test_with_topology(
            query_id,
            report_endpoint,
            scheduler,
            dispatcher,
            runtime_filter_worker_count,
            _test_fixture,
            lifecycle_transport,
            Arc::new(crate::topology::FrontendTopologyController::new(1)),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_topology(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
        backend_topology: novarocks::query_execution::backend::BackendTopologyService,
    ) -> Self {
        let live_topology = Arc::new(FrontendLiveBackendTopology::new());
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            live_topology,
            backend_topology,
            backend_services: BackendServicesSource::Fixed {
                scheduler,
                dispatcher,
                lifecycle_transport,
            },
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_backend_sequence(
        query_id: QueryId,
        report_endpoint: SocketAddr,
        schedulers: Vec<FrontendFragmentScheduler>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        runtime_filter_worker_count: NonZeroUsize,
        _test_fixture: Arc<dyn std::any::Any + Send + Sync>,
        lifecycle_transport: Arc<dyn QueryLifecycleTransport>,
    ) -> Self {
        let live_topology = Arc::new(FrontendLiveBackendTopology::new());
        Self {
            report_endpoint: Arc::new(FrontendReportEndpointBinding::from_socket_addr(
                report_endpoint,
            )),
            live_topology,
            backend_topology: Arc::new(crate::topology::FrontendTopologyController::new(1)),
            backend_services: BackendServicesSource::Sequence {
                schedulers: Mutex::new(schedulers.into()),
                dispatcher,
                lifecycle_transport,
            },
            runtime_filter_worker_count,
            query_ids: Arc::new(FixedQueryIdSource(query_id)),
            registry: Arc::new(FrontendQueryRegistry::default()),
        }
    }

    pub fn report_handler(&self) -> FrontendCoordinatorReportHandler {
        FrontendCoordinatorReportHandler::new(Arc::clone(&self.registry))
    }

    pub fn backend_query_activity(&self) -> BackendQueryActivity {
        BackendQueryActivity::new(Arc::clone(&self.registry), Arc::clone(&self.live_topology))
    }

    pub fn report_endpoint_sink(
        &self,
    ) -> Arc<dyn novarocks::query_execution::backend::CoordinatorReportEndpointSink> {
        self.report_endpoint.clone()
    }

    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }

    fn execute_request(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let query_id = self.query_ids.next_query_id();
        let execution_id = QueryExecutionId::new(
            query_id,
            AttemptId::new(1).expect("the initial query attempt is nonzero"),
        )
        .map_err(|error| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?;
        let parts = request.into_parts();
        let intent = parts.completion.intent();
        let backend_services = self.backend_services.resolve()?;
        let dispatcher = Arc::clone(&backend_services.dispatcher);
        let _query = self
            .registry
            .register(query_id, intent, Arc::clone(&dispatcher))?;
        let schedule = backend_services
            .scheduler
            .schedule(parts.artifacts.scheduling_view(), execution_id)?;
        let scheduled_backend_ownership = backend_services
            .scheduler
            .scheduled_backend_ownership(&schedule.backend_ids())?;
        self.registry
            .set_scheduled_backend_ownership(query_id, &scheduled_backend_ownership)?;
        let scheduled = parts.artifacts.bind_schedule(schedule)?;
        let timeout_ms = parts.options.timeout_ms().max(0);
        let query_deadline_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| failed(format!("system clock precedes Unix epoch: {error}")))?
            .as_millis()
            .saturating_add(u128::from(timeout_ms.max(1) as u64))
            .try_into()
            .map_err(|_| failed("query deadline exceeds u64 milliseconds"))?;
        let runtime = &novarocks::common::app_config::config()
            .map_err(|error| failed(format!("load query lifecycle config: {error}")))?
            .runtime;
        let lifecycle_config = FrontendQueryLifecycleConfig::new(
            Duration::from_millis(runtime.query_control_heartbeat_interval_ms),
            Duration::from_millis(runtime.query_control_heartbeat_timeout_ms),
            Duration::from_millis(runtime.query_control_init_rpc_timeout_ms),
            Duration::from_millis(runtime.query_control_attach_timeout_ms),
        )?;
        let lifecycle_barrier = FrontendQueryLifecycleBarrier::new(
            Arc::clone(&backend_services.lifecycle_transport),
            Arc::clone(&self.registry),
            lifecycle_config,
        )
        .with_cancellation(parts.cancellation.clone());
        let init_options = QueryInitOptions::new(
            execution_id,
            backend_services.live_backends,
            self.runtime_filter_worker_count.get(),
            parts.options.runtime_filter_lifecycle(),
            &parts.options,
            query_deadline_unix_ms,
            Duration::from_millis(runtime.query_control_pre_start_timeout_ms),
            self.report_endpoint.resolve()?,
            dispatcher.needs_fragment_status_report() || intent == DistributedQueryIntent::Profile,
        )?;
        let execution = scheduled
            .initialize_query(init_options, &lifecycle_barrier)?
            .assemble()?;
        let PreparedNativeExecutionParts {
            submissions,
            root_fetch,
            writer_registrations,
            expected_output,
            query_lifecycle_lease,
        } = execution.into_parts();
        let mut query_lifecycle_lease = Some(query_lifecycle_lease);
        let submitted_instance_ids = submissions
            .iter()
            .map(|submission| submission.fragment_instance_id())
            .collect::<Vec<_>>();
        let writer_instance_ids = writer_registrations.fragment_instance_ids();
        let writer_identities = writer_registrations.writer_identities();
        if let Err(error) = self
            .registry
            .set_writer_instances(query_id, &writer_identities)
        {
            let kind = error.kind();
            let message =
                abort_query_lifecycle(&mut query_lifecycle_lease, error.message().to_string());
            return Err(DistributedQueryError::new(kind, message));
        }

        let submission_count = submissions.len();
        let mut submitted = 0usize;
        for submission in submissions {
            if parts.cancellation.is_cancelled() {
                let error = self.fail_cancel_then_abort_query_lifecycle(
                    query_id,
                    &mut query_lifecycle_lease,
                    "query cancelled before fragment submission",
                );
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(error);
            }
            let backend_idx = submission.backend_idx();
            let finst_id = submission.fragment_instance_id();
            if let Err(error) = self
                .registry
                .record_attempt(query_id, backend_idx, finst_id)
            {
                let message = abort_query_lifecycle(&mut query_lifecycle_lease, error.to_string());
                let _ = self
                    .registry
                    .preserve_failure_context(query_id, message.clone());
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(failed(message));
            }
            let submit_result = dispatcher.submit_fragment(backend_idx, submission.into_envelope());
            if let Err(error) = self.registry.finish_attempt(query_id) {
                let message = abort_query_lifecycle(&mut query_lifecycle_lease, error.to_string());
                let _ = self
                    .registry
                    .preserve_failure_context(query_id, message.clone());
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(failed(message));
            }
            if let Err(error) = submit_result {
                let error = self.fail_cancel_then_abort_query_lifecycle(
                    query_id,
                    &mut query_lifecycle_lease,
                    error,
                );
                if intent == DistributedQueryIntent::Write {
                    break;
                }
                return Err(error);
            }
            self.backend_topology
                .record_successful_fragment_submission(backend_idx);
            submitted += 1;
            if let Some(message) = self.registry.first_failure(query_id) {
                if intent != DistributedQueryIntent::Write {
                    let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
                    return Err(failed(message));
                }
                break;
            }
        }
        let submission_failure = self.registry.first_failure(query_id);
        if submitted != submission_count
            || submission_failure.is_some()
            || parts.cancellation.is_cancelled()
        {
            let message = submission_failure.unwrap_or_else(|| {
                if parts.cancellation.is_cancelled() {
                    "query cancelled during fragment submission".to_string()
                } else {
                    "fragment submission stopped before completion".to_string()
                }
            });
            if self.registry.first_failure(query_id).is_some() {
                let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
                let _ = self.registry.preserve_failure_context(query_id, message);
            } else {
                let _ = self.fail_cancel_then_abort_query_lifecycle(
                    query_id,
                    &mut query_lifecycle_lease,
                    message,
                );
            }
        }

        if parts.cancellation.is_cancelled() {
            let error = self.fail_cancel_then_abort_query_lifecycle(
                query_id,
                &mut query_lifecycle_lease,
                "query cancelled after fragment submission",
            );
            if intent != DistributedQueryIntent::Write {
                return Err(error);
            }
        }
        if let Some(message) = self.registry.first_failure(query_id)
            && intent != DistributedQueryIntent::Write
        {
            let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
            return Err(failed(message));
        }

        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        let mut batches = Vec::new();
        if root_fetch.uses_result_buffer() {
            loop {
                if parts.cancellation.is_cancelled() {
                    return Err(self.fail_cancel_then_abort_query_lifecycle(
                        query_id,
                        &mut query_lifecycle_lease,
                        "query cancelled while fetching result",
                    ));
                }
                if let Some(message) = self.registry.first_failure(query_id) {
                    let message = abort_query_lifecycle(&mut query_lifecycle_lease, message);
                    return Err(failed(message));
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(self.fail_cancel_then_abort_query_lifecycle(
                        query_id,
                        &mut query_lifecycle_lease,
                        format!("query timed out after {timeout_ms} ms"),
                    ));
                }
                let fetch_wait_ms = deadline
                    .saturating_duration_since(now)
                    .as_millis()
                    .clamp(1, 300) as i64;
                let fetch = match dispatcher.fetch_result(
                    root_fetch.backend_idx(),
                    root_fetch.fragment_instance_id(),
                    fetch_wait_ms,
                    Some(expected_output.fetch_view()),
                ) {
                    Ok(fetch) => fetch,
                    Err(error) => {
                        return Err(self.fail_cancel_then_abort_query_lifecycle(
                            query_id,
                            &mut query_lifecycle_lease,
                            error,
                        ));
                    }
                };
                match fetch {
                    FetchOutcome::Ready(batch) => batches.push(batch),
                    FetchOutcome::NotReady => continue,
                    FetchOutcome::Eof => break,
                    FetchOutcome::Err(error) => {
                        return Err(self.fail_cancel_then_abort_query_lifecycle(
                            query_id,
                            &mut query_lifecycle_lease,
                            error,
                        ));
                    }
                }
            }
        }

        match intent {
            DistributedQueryIntent::Result => {}
            DistributedQueryIntent::Write => {
                if let Err(error) = self.wait_for_reports(
                    query_id,
                    &writer_instance_ids,
                    deadline,
                    timeout_ms,
                    &parts.cancellation,
                    true,
                    "write final reports",
                ) {
                    if self.registry.first_failure(query_id).is_none() {
                        let _ = self
                            .registry
                            .latch_failure_and_cancel(query_id, error.message().to_string());
                    }
                    let _ = abort_query_lifecycle(
                        &mut query_lifecycle_lease,
                        error.message().to_string(),
                    );
                }
            }
            DistributedQueryIntent::Profile => {
                if let Err(error) = self.wait_for_reports(
                    query_id,
                    &submitted_instance_ids,
                    deadline,
                    timeout_ms,
                    &parts.cancellation,
                    false,
                    "fragment profile reports",
                ) {
                    let kind = error.kind();
                    let message = abort_query_lifecycle(
                        &mut query_lifecycle_lease,
                        error.message().to_string(),
                    );
                    return Err(DistributedQueryError::new(kind, message));
                }
            }
        }

        let (query_failure, reports) = match self.registry.seal_and_take_completion(query_id) {
            Ok(completion) => completion,
            Err(error) => {
                let kind = error.kind();
                let message =
                    abort_query_lifecycle(&mut query_lifecycle_lease, error.message().to_string());
                return Err(DistributedQueryError::new(kind, message));
            }
        };
        let mut lifecycle_failure = query_failure.clone();
        let outcome = (|| {
            let result = expected_output.into_query_result(batches)?;
            match intent {
                DistributedQueryIntent::Result => parts.completion.result(result),
                DistributedQueryIntent::Write => {
                    let mut builder = WriteReportBuilder::new(writer_registrations)?;
                    if let Some(message) = query_failure {
                        builder.latch_failure(message);
                    }
                    for report in reports {
                        builder.apply(report)?;
                    }
                    let report_outcome = builder.finish()?;
                    if let Some(reason) = report_outcome.abort_reason() {
                        lifecycle_failure = Some(reason.to_string());
                        let _ = self
                            .registry
                            .latch_failure_and_cancel(query_id, reason.to_string());
                    }
                    let (commit, abort) = report_outcome.into_payloads();
                    parts.completion.write(result, commit, abort)
                }
                DistributedQueryIntent::Profile => {
                    let mut builder = ProfileReportBuilder::new();
                    for report in reports {
                        builder.apply(report)?;
                    }
                    parts.completion.profile(result, builder.finish())
                }
            }
        })();
        if let Err(error) = &outcome {
            let _ = self
                .registry
                .latch_failure_and_cancel(query_id, error.message().to_string());
            let kind = error.kind();
            let message =
                abort_query_lifecycle(&mut query_lifecycle_lease, error.message().to_string());
            return Err(DistributedQueryError::new(kind, message));
        }
        if let Some(message) = lifecycle_failure {
            let _ = abort_query_lifecycle(&mut query_lifecycle_lease, message);
        } else {
            query_lifecycle_lease
                .take()
                .expect("query lifecycle lease is present through query completion")
                .finalize()?;
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn wait_for_reports(
        &self,
        query_id: QueryId,
        expected_instances: &[novarocks::UniqueId],
        deadline: Instant,
        timeout_ms: i64,
        cancellation: &QueryCancellationView,
        final_report_failure_completes_wait: bool,
        report_kind: &str,
    ) -> Result<(), DistributedQueryError> {
        const REPORT_POLL_INTERVAL: Duration = Duration::from_millis(10);

        if expected_instances.is_empty() {
            return Ok(());
        }

        loop {
            let (received, first_failure, has_failed_final_report) = self
                .registry
                .report_progress(query_id, expected_instances)?;
            if let Some(message) = first_failure {
                if final_report_failure_completes_wait && has_failed_final_report {
                    return Ok(());
                }
                return Err(self.fail_and_cancel(query_id, message));
            }
            if received >= expected_instances.len() {
                return Ok(());
            }
            if cancellation.is_cancelled() {
                return Err(self.fail_and_cancel(
                    query_id,
                    format!("query cancelled while waiting for {report_kind}"),
                ));
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(self.fail_and_cancel(
                    query_id,
                    format!(
                        "query timed out after {timeout_ms} ms waiting for {report_kind}: received {received} of {}",
                        expected_instances.len()
                    ),
                ));
            }
            std::thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(REPORT_POLL_INTERVAL),
            );
        }
    }

    fn fail_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        match self.registry.latch_failure_and_cancel(query_id, message) {
            Ok(message) => failed(message),
            Err(error) => error,
        }
    }

    fn fail_cancel_then_abort_query_lifecycle(
        &self,
        query_id: QueryId,
        lease: &mut Option<novarocks::query_execution::lifecycle::QueryLifecycleLease>,
        message: impl Into<String>,
    ) -> DistributedQueryError {
        let primary = self.fail_and_cancel(query_id, message);
        let enriched = abort_query_lifecycle(lease, primary.message().to_string());
        let _ = self
            .registry
            .preserve_failure_context(query_id, enriched.clone());
        failed(self.registry.first_failure(query_id).unwrap_or(enriched))
    }
}

impl DistributedQueryCoordinator for FrontendDistributedQueryCoordinator {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.execute_request(request)
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn abort_query_lifecycle(
    lease: &mut Option<novarocks::query_execution::lifecycle::QueryLifecycleLease>,
    message: impl Into<String>,
) -> String {
    let message = message.into();
    lease
        .take()
        .map_or(message.clone(), |lease| lease.abort_preserving(message))
}

#[cfg(test)]
mod tests {
    use super::{
        BackendServicesSource, FrontendLiveBackendTopology, FrontendReportEndpointBinding,
    };
    use novarocks::query_execution::backend::{CoordinatorReportEndpointSink, LiveBackendTarget};
    use std::sync::Arc;

    #[test]
    fn ephemeral_report_endpoint_is_unavailable_until_the_bound_port_is_published() {
        let binding = FrontendReportEndpointBinding::new("frontend.internal".to_string(), 0);

        let error = binding
            .resolve()
            .err()
            .expect("port zero must gate query submission until listener bind");
        assert!(error.message().contains("not bound yet"), "{error}");

        binding.set_bound_port(19070);

        binding
            .resolve()
            .expect("bound port publication makes the DNS endpoint available");
    }

    #[test]
    fn live_backend_source_resolves_each_query_from_the_latest_injected_snapshot() {
        let topology = Arc::new(FrontendLiveBackendTopology::new());
        let source = BackendServicesSource::Live(Arc::clone(&topology));
        let first: std::net::SocketAddr = "127.0.0.1:19071".parse().unwrap();
        let second: std::net::SocketAddr = "127.0.0.1:19072".parse().unwrap();

        assert!(
            source.resolve().is_err(),
            "an empty injected topology must not fall back to core globals or config"
        );

        topology.replace(1, vec![LiveBackendTarget::new(7, first, 11)]);
        assert_eq!(
            source
                .resolve()
                .expect("first injected topology")
                .scheduler
                .backend_entries(),
            &[(7, first)]
        );

        topology.replace(2, vec![LiveBackendTarget::new(8, second, 12)]);
        assert_eq!(
            source
                .resolve()
                .expect("replacement injected topology")
                .scheduler
                .backend_entries(),
            &[(8, second)]
        );

        topology.replace(1, vec![LiveBackendTarget::new(7, first, 11)]);
        assert_eq!(
            source
                .resolve()
                .expect("stale topology publication is ignored")
                .scheduler
                .backend_entries(),
            &[(8, second)]
        );
    }

    #[test]
    fn frontend_query_lifecycle_live_transport_is_built_with_the_scheduler_snapshot() {
        let topology = Arc::new(FrontendLiveBackendTopology::new());
        let source = BackendServicesSource::Live(Arc::clone(&topology));
        let endpoint = "127.0.0.1:19073".parse().expect("valid endpoint");
        topology.replace(1, vec![LiveBackendTarget::new(7, endpoint, 21)]);

        let services = source
            .resolve()
            .expect("one immutable snapshot builds every backend service");

        assert_eq!(services.scheduler.backend_entries(), &[(7, endpoint)]);
        assert_eq!(Arc::strong_count(&services.lifecycle_transport), 1);
    }
}
