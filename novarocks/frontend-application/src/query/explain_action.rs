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

//! EXPLAIN runs in the original registered statement, not a second Work role.

use super::statement_action::StatementReadyReceiver;
use super::*;
use crate::query::compiler::{PreparedAsyncExplain, PreparedAsyncProfile};
use crate::query_execution::logical_execution::OriginalComputationStart;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::{ExecutionHandle, LocalResultProducer, OwnedLocalResult};
use novarocks_query_application::session_control::{
    GovernedQueryStatementOwner, GovernedStatementAction, StatementActionValue,
};
use std::task::{Context, Poll};

type PreparationFuture =
    Pin<Box<dyn Future<Output = Result<PreparedExplain, QueryServiceError>> + Send>>;
type DiagnosticFuture =
    Pin<Box<dyn Future<Output = Result<QueryResult, QueryServiceError>> + Send>>;

struct ExplainPreparationInput {
    compiler: FrontendQueryCompiler,
    topology: BackendTopologyService,
    role: ClusterRole,
    optimizer_limit: u64,
    sql: String,
    explain: ast::ExplainQuery,
    state: SessionSqlState,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    scope: novarocks_workload_control::WorkScope,
    window: ResultWindowAlias,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
struct PreparedExplain {
    value: PreparedAsyncExplain,
    capacity: QueryResultCapacityBinding,
}
async fn make_explain_preparation(
    input: ExplainPreparationInput,
) -> Result<PreparedExplain, QueryServiceError> {
    if input.cancellation.is_cancelled() {
        return Err(internal_error(
            "typed statement was cancelled before query compilation began",
        ));
    }
    let topology = input
        .topology
        .snapshot()
        .map_err(|e| internal_error(e.to_string()))?;
    let topology = if topology.targets().is_empty() {
        wait_for_eligible_query_topology_after(
            &input.topology,
            &input.cancellation,
            topology.revision(),
            input
                .deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(30)),
        )
        .await?
    } else {
        topology
    };
    let (catalog, database, execution, mut optimizer, semantics) =
        input.state.into_query_attempt_inputs();
    let semantics = novarocks_sql::sql_mode::statement_sql_semantics(
        &semantics,
        &ParsedStatement::ExplainQuery(input.explain.clone()),
    )
    .map_err(|e| QueryServiceError::from_user_error(e.to_user_error(Some(&input.sql))))?;
    if optimizer.optimizer_query_mem_limit_bytes.is_none() {
        optimizer.optimizer_query_mem_limit_bytes = Some(input.optimizer_limit as f64);
    }
    let options = with_query_hints(
        query_options_from_session_settings(&execution),
        &semantics,
        Some(&input.explain.query),
    );
    let capacity = QueryResultCapacityBinding::try_new(&input.scope, input.window)
        .map_err(|e| internal_error(e.to_string()))?;
    let context = RequestContext::admit(RequestAdmission::new(
        catalog,
        database,
        input.role,
        topology,
        input.deadline,
        input.cancellation,
        optimizer,
        semantics,
    ))
    .with_result_capacity(capacity.clone())
    .map_err(|e| internal_error(e.to_string()))?;
    #[cfg(novarocks_dev_controls)]
    let context = match input
        .observation
        .as_ref()
        .and_then(|handle| handle.nomination())
    {
        Some(nomination) => context
            .with_test_statement_nomination(Arc::new(nomination))
            .map_err(internal_error)?,
        None => context,
    };
    let value = input
        .compiler
        .prepare_explain_async(&input.explain, &context, Some(options), &input.scope)
        .await
        .map_err(|e| routed_execution_service_error(routed_compiler_failure(e, &input.sql)))?;
    Ok(PreparedExplain { value, capacity })
}
struct ProfileDiagnosticInput {
    ready: crate::query_execution::internal_consumer::profile::ReadyProfileComputation,
    completion: crate::query_execution::completion::PreparedQueryCompletion,
}
async fn make_profile_diagnostics(
    mut input: ProfileDiagnosticInput,
) -> Result<QueryResult, QueryServiceError> {
    let snapshot = input.ready.wait_diagnostics().await;
    input
        .completion
        .complete_profile_observation(&input.ready, snapshot.as_deref().map_err(|e| *e))
        .map_err(internal_error)
}

struct ExplainStatementAction {
    preparation: Option<PreparationFuture>,
    preparation_started: bool,
    prepared: Option<Result<PreparedExplain, QueryServiceError>>,
    profile: Option<PreparedAsyncProfile>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    diagnostics: Option<DiagnosticFuture>,
    raw_result: Option<Result<QueryResult, QueryServiceError>>,
    local: Option<OwnedLocalResult>,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl GovernedStatementAction for ExplainStatementAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedQueryStatementOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let result = match self.poll_explain(owner, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let Some(reason) = cancellation.reason() else {
            return Poll::Ready(result);
        };
        // Preserve an already selected execution fact; otherwise retain the
        // original public cancellation policy and its compile-control sidecar.
        Poll::Ready(match result {
            Err(error) if error.execution_failure().is_some() => Err(error),
            Err(error) => {
                Err(cancellation_error(reason).with_compile_control(error.compile_control_error()))
            }
            Ok(value) => {
                drop(value);
                Err(cancellation_error(reason))
            }
        })
    }
}
impl ExplainStatementAction {
    fn poll_explain(
        &mut self,
        owner: &mut GovernedQueryStatementOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if !self.preparation_started {
            if owner.cancellation().reason().is_some() {
                return Poll::Ready(Err(internal_error(
                    "typed statement was cancelled before prepared query execution began",
                )));
            }
            self.preparation_started = true;
        }
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.prepared = Some(value),
            }
            // Original output is stable before destroying the completed F.
            drop(self.preparation.take());
        }
        if let Some(prepared) = self.prepared.take() {
            match prepared {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Lines(lines),
                    ..
                }) => {
                    self.raw_result = Some(
                        build_string_query_result("Explain String", lines).map_err(internal_error),
                    );
                }
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Profile(profile),
                    capacity,
                }) => {
                    self.profile = Some(profile);
                    self.capacity = Some(capacity);
                }
            }
        }
        if self
            .profile
            .as_ref()
            .is_some_and(|p| p.computation.is_some())
        {
            if owner.cancellation().reason().is_some() {
                return Poll::Ready(Err(internal_error(
                    "typed statement was cancelled before prepared query execution began",
                )));
            }
            let tail = match self.launcher.start_computation_in(
                &mut self.profile.as_mut().expect("original profile").computation,
                owner.execution_owner_slot(),
                self.capacity.as_ref().expect("original capacity"),
                &mut self.original,
                #[cfg(novarocks_dev_controls)]
                self.observation.clone(),
            ) {
                Ok(tail) => tail,
                Err(e) => return Poll::Ready(Err(governed_query_execution_error(e))),
            };
            if let Err(e) = tail.finish() {
                return Poll::Ready(Err(governed_query_execution_error(e)));
            }
        }
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.execution = Some(value),
            }
            drop(self.original.ready.take());
        }
        if let Some(execution) = self.execution.take() {
            let execution = match execution {
                Ok(execution) => execution,
                Err(e) => return Poll::Ready(Err(governed_query_execution_error(e))),
            };
            let token = match execution.ready_token() {
                Some(token) => *token,
                None => {
                    return Poll::Ready(Err(internal_error(
                        "Profile has no computation Ready token",
                    )));
                }
            };
            let profile = self.profile.as_mut().expect("started Profile");
            let ready = match profile.output.take_ready(token) {
                Ok(ready) => ready,
                Err(e) => return Poll::Ready(Err(governed_query_execution_error(e))),
            };
            let profile = self.profile.take().expect("same ready profile");
            self.diagnostics = Some(Box::pin(make_profile_diagnostics(ProfileDiagnosticInput {
                ready,
                completion: profile.completion,
            })));
            drop(execution);
        }
        if let Some(future) = self.diagnostics.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.raw_result = Some(value),
            }
            drop(self.diagnostics.take());
        }
        if self.local.is_none() {
            let result = match self.raw_result.take().expect("completed EXPLAIN result") {
                Ok(result) => result,
                Err(e) => return Poll::Ready(Err(e)),
            };
            let window = match owner.result_window_alias() {
                Some(window) => window,
                None => {
                    return Poll::Ready(Err(internal_error(
                        "EXPLAIN has no original result window",
                    )));
                }
            };
            let producer = match LocalResultProducer::try_new(owner.scope(), window) {
                Ok(producer) => producer,
                Err(e) => return Poll::Ready(Err(internal_error(e))),
            };
            match producer.produce(|| Ok(result)) {
                Ok(local) => self.local = Some(local),
                Err(e) => return Poll::Ready(Err(internal_error(e))),
            }
        }
        Poll::Ready(Ok(StatementActionValue::Local(
            self.local.take().expect("sealed original graph"),
        )))
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_explain(
        &self,
        sql: String,
        explain: ast::ExplainQuery,
        state: SessionSqlState,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<StatementResult, QueryServiceError> {
        let window = match statement.result_window_alias() {
            Some(window) => window,
            None => {
                return Ok(self.governed_typed_error(
                    internal_error("EXPLAIN has no original result window"),
                    statement,
                ));
            }
        };
        let input = ExplainPreparationInput {
            compiler: self.service.query_compiler.clone(),
            topology: self.service.topology.clone(),
            role: self.service.role,
            optimizer_limit: self.service.optimizer_query_mem_limit_bytes,
            sql,
            explain,
            state,
            deadline,
            cancellation: QueryCancellationView::governed(
                statement.cancellation().clone(),
                statement.timeout_ms(),
            ),
            scope: statement.scope().clone(),
            window,
            #[cfg(novarocks_dev_controls)]
            observation: observation.clone(),
        };
        let action = Box::new(ExplainStatementAction {
            preparation: Some(Box::pin(make_explain_preparation(input))),
            preparation_started: false,
            prepared: None,
            profile: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            diagnostics: None,
            raw_result: None,
            local: None,
            launcher: Arc::clone(&self.service.logical_execution_launcher),
            #[cfg(novarocks_dev_controls)]
            observation,
        });
        self.execute_observed_statement_action(statement, action)
            .await
    }
}

/// Pure layout facts from the same concrete factories used above. Variable
/// AST/plan/SQL/provider backing and the shared statement source are separate.
pub(crate) struct ExplainStatementGeometry {
    pub(crate) action: std::alloc::Layout,
    pub(crate) preparation: std::alloc::Layout,
    pub(crate) diagnostics: std::alloc::Layout,
    pub(crate) receiver: std::alloc::Layout,
}
pub(crate) fn statement_action_geometry() -> ExplainStatementGeometry {
    fn future_layout<I, F: Future>(_: impl Fn(I) -> F) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    ExplainStatementGeometry {
        action: std::alloc::Layout::new::<ExplainStatementAction>(),
        preparation: future_layout(make_explain_preparation),
        diagnostics: future_layout(make_profile_diagnostics),
        receiver: std::alloc::Layout::new::<StatementReadyReceiver>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::{
        api::{QueryExecutionFuture, ReservedExecutionIdentity},
        client_connection::ClientConnectionToken,
        query_control::QueryApplicationControl,
        session_control::StatementActionReady,
    };
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultCapacityHandle, WorkloadConfig, WorkloadControl,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoNativeLaunch;
    impl LogicalExecutionLauncher for NoNativeLaunch {
        fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
            panic!("Local EXPLAIN cannot reserve Native work")
        }
        fn start_computation_in(
            &self,
            _: &mut Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
            _: &mut Option<WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("Local EXPLAIN cannot start Native work")
        }
        fn start_computation(
            &self,
            _: crate::query_execution::logical_execution::PreparedLogicalComputation,
            _: WorkOwner,
            _: QueryResultCapacityBinding,
            _: Option<ReservedExecutionIdentity>,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> QueryExecutionFuture {
            panic!("the retired convenience start is not called")
        }
        fn start(
            &self,
            _: crate::query_execution::completion::PreparedLogicalRead,
            _: WorkOwner,
            _: Option<ResultWindowAlias>,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> QueryExecutionFuture {
            panic!("Local EXPLAIN cannot start a row computation")
        }
    }
    async fn statement() -> (
        WorkloadControl,
        ResultCapacityHandle,
        GovernedQueryStatementOwner,
        novarocks_query_application::session_control::QuerySessionLease,
    ) {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control);
        let session = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 1).unwrap(),
                "root",
            ))
            .unwrap();
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let owner = service
            .begin_queued_governed_query_statement_with_result(
                session.token(),
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
            .await
            .unwrap();
        (workload, capacity, owner, session)
    }
    fn action(preparation: PreparationFuture) -> ExplainStatementAction {
        ExplainStatementAction {
            preparation: Some(preparation),
            preparation_started: false,
            prepared: None,
            profile: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            diagnostics: None,
            raw_result: None,
            local: None,
            launcher: Arc::new(NoNativeLaunch),
            #[cfg(novarocks_dev_controls)]
            observation: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn prepared_explain_wait_leaves_native_runtime_free_to_poll_first_send() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let binding = QueryResultCapacityBinding::try_new(
            owner.scope(),
            owner.result_window_alias().unwrap(),
        )
        .unwrap();
        let (send, receive) = futures::channel::oneshot::channel();
        let mut action = action(Box::pin(async move {
            receive.await.unwrap();
            Ok(PreparedExplain {
                value: PreparedAsyncExplain::Lines(vec!["observed".into()]),
                capacity: binding,
            })
        }));
        let produce = futures::future::poll_fn(|cx| action.poll_action(&mut owner, cx));
        let signal = async move {
            tokio::task::yield_now().await;
            send.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(produce, signal);
        let StatementActionValue::Local(local) = result.unwrap() else {
            panic!("real closed Local output")
        };
        assert_eq!(local.row_count(), 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        drop(local);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn prepared_explain_checks_cancellation_before_dispatch() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let polls = Arc::new(AtomicUsize::new(0));
        let inside = Arc::clone(&polls);
        let mut action = action(Box::pin(futures::future::poll_fn(move |_| {
            inside.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        })));
        owner
            .execution_owner_slot()
            .as_ref()
            .unwrap()
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        let result = futures::future::poll_fn(|cx| action.poll_action(&mut owner, cx)).await;
        let Err(error) = result else {
            panic!("cancelled preparation cannot deliver a result");
        };
        assert_eq!(error.kind(), QueryServiceErrorKind::Interrupted);
        assert_eq!(
            error.user_error().expect("typed cancellation").message(),
            "Query execution cancellation was requested"
        );
        assert_eq!(
            error
                .execution_failure()
                .expect("original cancellation fact")
                .site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn occupied_protocol_destination_preserves_original_ready_and_owner() {
        let (_workload, _capacity, owner, _session) = statement().await;
        let token = owner.token();
        let mut source = Some(StatementActionReady {
            result: Ok(StatementActionValue::Ok),
            owner,
        });
        let mut target = Some(StatementResult::Ok);
        assert!(StatementActionReady::transfer_protocol_in(&mut source, &mut target).is_err());
        assert_eq!(source.as_ref().unwrap().owner.token(), token);
        assert!(matches!(target, Some(StatementResult::Ok)));
        target = None;
        StatementActionReady::transfer_protocol_in(&mut source, &mut target).unwrap();
        assert!(source.is_none());
        let StatementResult::GovernedCompletion(result) = target.take().unwrap() else {
            panic!("same governed completion")
        };
        assert_eq!(
            result.into_protocol().complete(),
            novarocks_query_application::session_control::GovernedStatementFinishOutcome::Completed
        );
    }
}
