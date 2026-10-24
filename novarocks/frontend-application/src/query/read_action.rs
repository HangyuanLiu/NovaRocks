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

//! Ordinary ClientRows preparation and start in the original registered action.
pub(crate) mod io_source;
use super::statement_computation::StatementStageAdmission;
use super::*;
use crate::query_execution::completion_facts::FrontendCompletionCustody;
use crate::query_execution::logical_execution::OriginalComputationStart;
use io_source::ReadMetadataIoSource;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::{
    ExecutionHandle, LocalResultProducer, LogicalExecutionExitObserver,
    LogicalExecutionExitReceipt, StatementNativeHealthFailure,
};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
    GovernedStatementResidual, StatementActionValue,
};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorMetadataMaintenanceIoGeometry,
    ConnectorMetadataMaintenanceIoProfile, SharedDependency,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::task::{Context, Poll};

struct ReadPreparationInput {
    compiler: FrontendQueryCompiler,
    topology: BackendTopologyService,
    role: ClusterRole,
    optimizer_limit: u64,
    sql: String,
    parsed: ParsedStatement,
    state: SessionSqlState,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    scope: WorkScope,
    window: ResultWindowAlias,
    purpose: FrontendQueryPurpose,
    provider: Arc<FrontendCompletionCustody>,
    io: Arc<ReadMetadataIoSource>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    statement_token: StatementToken,
}
async fn prepare_read(
    input: ReadPreparationInput,
) -> Result<PreparedQueryOperation, QueryServiceError> {
    let parsed = input
        .state
        .substitute_user_variables(input.parsed)
        .map_err(|e| internal_error(e.to_string()))?;
    let topology =
        wait_for_initial_query_topology(&input.topology, &input.cancellation, input.deadline)
            .await?;
    let (catalog, database, execution, mut optimizer, semantics) =
        input.state.into_query_attempt_inputs();
    let semantics = novarocks_sql::sql_mode::statement_sql_semantics(&semantics, &parsed)
        .map_err(|e| QueryServiceError::from_user_error(e.to_user_error(Some(&input.sql))))?;
    if optimizer.optimizer_query_mem_limit_bytes.is_none() {
        optimizer.optimizer_query_mem_limit_bytes = Some(input.optimizer_limit as f64);
    }
    let options = with_query_hints(
        query_options_from_session_settings(&execution),
        &semantics,
        match &parsed {
            ParsedStatement::Query(q) => Some(q),
            _ => None,
        },
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
    .with_result_capacity(capacity)
    .map_err(|e| internal_error(e.to_string()))?;
    #[cfg(novarocks_dev_controls)]
    let context = match input.observation.as_ref().and_then(|h| h.nomination()) {
        Some(nomination) => context
            .with_test_statement_nomination(Arc::new(nomination))
            .map_err(internal_error)?,
        None => context,
    };
    #[cfg(novarocks_dev_controls)]
    let rewrite = input
        .observation
        .as_ref()
        .and_then(|h| h.statement_target())
        .map(|(run, nominated)| {
            crate::mv::test_controls::MvRewriteStatementTarget::new(
                input.statement_token,
                nominated,
                run,
            )
        })
        .transpose()
        .map_err(internal_error)?;
    input
        .compiler
        .prepare_read_async(
            &parsed,
            input.purpose,
            &context,
            Some(options),
            &input.scope,
            &input.provider,
            input.io,
            #[cfg(novarocks_dev_controls)]
            rewrite.as_ref(),
        )
        .await
        .map_err(|e| routed_execution_service_error(routed_compiler_failure(e, &input.sql)))
}
async fn close_provider(
    source: Arc<FrontendCompletionCustody>,
) -> Result<(), StatementNativeHealthFailure> {
    source.close().await
}
type ProviderClose = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
fn poll_provider_close(
    future: &mut Option<ProviderClose>,
    result: &mut Option<Result<(), StatementNativeHealthFailure>>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), StatementNativeHealthFailure>> {
    if let Some(future) = future.as_mut() {
        match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(value) => *result = Some(value),
        }
        // The result is in the original receiving field before completed F Drop.
    }
    if result.is_none() {
        *result = Some(Err(StatementNativeHealthFailure::actual_exit_unproven(
            "Read Provider close has no original result",
            Arc::new(WorkError::Conflict),
        )));
    }
    drop(future.take());
    Poll::Ready(result.as_ref().unwrap().clone())
}
type Preparation =
    Pin<Box<dyn Future<Output = Result<PreparedQueryOperation, QueryServiceError>> + Send>>;

/// Original statement residual: it keeps the same Root and source registered
/// while actual lazy scan/credential streams and Native closure are pending.
struct ReadNativeTail {
    io: Arc<ReadMetadataIoSource>,
    stage: StatementStageAdmission,
    exit: Option<LogicalExecutionExitObserver>,
    receipt: Option<Result<LogicalExecutionExitReceipt, QueryExecutionError>>,
    launched: bool,
    health: Option<StatementNativeHealthFailure>,
}
impl ReadNativeTail {
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = &self.health {
            return Poll::Ready(Err(health.clone()));
        }
        if let Some(io) = self.io.original() {
            if let Err(error) = io.poll_continuing(cx) {
                self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read I/O continuation failed before actual exit",
                    Arc::new(error),
                ));
                return Poll::Ready(Err(self.health.clone().unwrap()));
            }
            if let Some(health) = io.health_failure() {
                self.health = Some(health);
                return Poll::Ready(Err(self.health.clone().unwrap()));
            }
        }
        if self.launched {
            if self.exit.is_none() && self.receipt.is_none() {
                self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read start has no original Native exit observer",
                    Arc::new(WorkError::Conflict),
                ));
                return Poll::Ready(Err(self.health.clone().unwrap()));
            }
            if let Some(exit) = self.exit.as_mut() {
                match exit.poll_exit(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => self.receipt = Some(result),
                }
                drop(self.exit.take());
            }
            if self.receipt.as_ref().is_some_and(Result::is_err) {
                self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read original Native exit was refused",
                    Arc::new(WorkError::Conflict),
                ));
                return Poll::Ready(Err(self.health.clone().unwrap()));
            }
        }
        if let Some(io) = self.io.original() {
            match io.poll_quiescent_continuing(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "Read I/O close failed",
                        Arc::new(error),
                    ));
                    return Poll::Ready(Err(self.health.clone().unwrap()));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        self.stage.retire_unlaunched();
        Poll::Ready(Ok(()))
    }
}
impl GovernedStatementResidual for ReadNativeTail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.poll_exit(cx)
    }
}
struct ReadStatementAction {
    provider: Arc<FrontendCompletionCustody>,
    provider_close: Option<ProviderClose>,
    provider_close_result: Option<Result<(), StatementNativeHealthFailure>>,
    local: Option<novarocks_query_application::api::OwnedLocalResult>,
    tail: Option<Box<ReadNativeTail>>,
    preparation: Option<Preparation>,
    prepared: Option<Result<PreparedQueryOperation, QueryServiceError>>,
    read: Option<crate::query_execution::completion::PreparedLogicalRead>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    error: Option<QueryServiceError>,
    completed_preparation: bool,
    preparation_started: bool,
    bootstrap_ready: bool,
    result_transferred: bool,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl ReadStatementAction {
    fn poll_read(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let tail = self
            .tail
            .as_mut()
            .expect("original Read tail remains until handoff");
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .expect("Read retains its original Root");
        match tail.io.poll_progress(work, cx) {
            Ok(ready) => self.bootstrap_ready |= ready,
            Err(e) => {
                self.error
                    .get_or_insert_with(|| internal_error(e.to_string()));
            }
        }
        if !self.bootstrap_ready && self.error.is_none() {
            return Poll::Pending;
        }
        if self.error.is_none() {
            if let Some(future) = self.preparation.as_mut() {
                self.preparation_started = true;
                match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(value) => self.prepared = Some(value),
                }
                if self.provider.local_health_failure().is_some() {
                    return Poll::Pending;
                }
                self.completed_preparation = true;
                drop(self.preparation.take());
            }
            match poll_provider_close(
                &mut self.provider_close,
                &mut self.provider_close_result,
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            if let Some(prepared) = self.prepared.take() {
                match prepared {
                    Err(error) => self.error = Some(error),
                    Ok(PreparedQueryOperation::LogicalRead(read)) => self.read = Some(read),
                    Ok(PreparedQueryOperation::Immediate(immediate)) => {
                        match immediate.into_result() {
                            StatementResult::Query(result) => {
                                match LocalResultProducer::try_new(
                                    owner.scope(),
                                    owner.result_window_alias().expect("admitted local window"),
                                )
                                .and_then(|p| p.produce(|| Ok(result)))
                                {
                                    Ok(result) => self.local = Some(result),
                                    Err(e) => self.error = Some(internal_error(e)),
                                }
                            }
                            _ => {
                                self.error = Some(internal_error(
                                    "Read immediate has a foreign protocol owner",
                                ))
                            }
                        }
                    }
                    Ok(PreparedQueryOperation::Distributed(_)) => {
                        self.error = Some(internal_error(
                            "Read completion returned a legacy distributed operation",
                        ))
                    }
                }
            }
        }
        if self.local.is_some() {
            match tail.poll_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            return Poll::Ready(Ok(StatementActionValue::Local(
                self.local.take().expect("closed local Read result"),
            )));
        }
        if self.error.is_none() && self.read.is_some() {
            let io = tail
                .io
                .original()
                .expect("Read CPU body installed original source");
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .expect("original Root");
            match tail.stage.poll_admit(&io, work, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => self.error = Some(e),
                Poll::Ready(Ok(())) => {}
            }
            if self.error.is_none() && self.capacity.is_none() {
                match tail.stage.install_client_capacity_in(
                    &io,
                    &owner.result_window_alias().expect("admitted Client window"),
                    &mut self.capacity,
                ) {
                    Ok(()) => {}
                    Err(e) => self.error = Some(e),
                }
            }
            if self.error.is_none() {
                match self.launcher.start_read_in(
                    &mut self.read,
                    tail.stage.owner_slot(),
                    self.capacity.as_ref().unwrap(),
                    &tail.io,
                    &mut self.original,
                    #[cfg(novarocks_dev_controls)]
                    self.observation.clone(),
                ) {
                    Err(e) => self.error = Some(governed_query_execution_error(e)),
                    Ok(notification) => {
                        if notification.result().is_ok() {
                            tail.launched = true;
                            tail.exit = self.original.exit.take();
                            if tail.exit.is_none() {
                                tail.health =
                                    Some(StatementNativeHealthFailure::actual_exit_unproven(
                                        "accepted Read has no original Native observer",
                                        Arc::new(WorkError::Conflict),
                                    ));
                            }
                        }
                        if let Err(error) = notification.finish() {
                            self.error = Some(governed_query_execution_error(error));
                        }
                    }
                }
            }
        }
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.execution = Some(result),
            }
            drop(self.original.ready.take());
        }
        if let Some(execution) = self.execution.take() {
            match execution {
                Ok(execution) => {
                    self.result_transferred = true;
                    drop(self.capacity.take());
                    return Poll::Ready(Ok(StatementActionValue::Rows(execution)));
                }
                Err(e) => self.error = Some(governed_query_execution_error(e)),
            }
        }
        if self.error.is_some() {
            // A preparation F which has ever been polled is not timeout-exit.
            // Continue it and keep its actual Creator source before any close.
            if !self.completed_preparation && !self.preparation_started {
                drop(self.preparation.take());
                self.completed_preparation = true;
            }
            if !self.completed_preparation {
                if let Some(future) = self.preparation.as_mut() {
                    match future.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => self.prepared = Some(result),
                    }
                    self.completed_preparation = true;
                    drop(self.preparation.take());
                }
            }
            match poll_provider_close(
                &mut self.provider_close,
                &mut self.provider_close_result,
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            if !tail.launched {
                // A refused mount left every original input in this action.
                // Dispose those inputs before retiring its unlaunched stage.
                drop(self.original.input.take());
                drop(self.original.request.take());
                drop(self.read.take());
                drop(self.capacity.take());
            }
            match tail.poll_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            return Poll::Ready(Err(self.error.take().unwrap()));
        }
        Poll::Pending
    }
}
impl GovernedStatementAction for ReadStatementAction {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.provider
            .local_health_failure()
            .or_else(|| {
                self.provider_close_result
                    .as_ref()
                    .and_then(|r| r.as_ref().err().cloned())
            })
            .or_else(|| {
                self.tail.as_ref().and_then(|t| {
                    t.io.health_failure()
                        .or_else(|| t.stage.local_health_failure())
                        .or_else(|| t.health.clone())
                })
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.result_transferred {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = Some(self.tail.take().expect("same original Read residual"));
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.poll_read(owner, cx)
    }
}
impl FrontendQuerySession {
    pub(super) async fn execute_observed_read(
        &self,
        sql: String,
        parsed: ParsedStatement,
        state: SessionSqlState,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
        purpose: FrontendQueryPurpose,
    ) -> Result<StatementResult, QueryServiceError> {
        let window = statement
            .result_window_alias()
            .ok_or_else(|| internal_error("Read has no original result window"))?;
        let geometry = self
            .service
            .query_compiler
            .read_io_geometry()
            .map_err(|e| internal_error(e.to_string()))?;
        let source_bytes = fixed_source_bytes(
            &geometry,
            statement
                .scope()
                .joint_dependency_limit()
                .map_err(|e| internal_error(e.to_string()))?,
        )
        .map_err(|e| internal_error(e.to_string()))?;
        let io = ReadMetadataIoSource::new(statement.scope().clone(), geometry, source_bytes);
        // This is the actual host-local SQL preparation body above. It remains
        // a real Source through view rewrite, pure completion and encoding;
        // Provider/service facts are independently added before their effects.
        io.configure_read_source(
            &ConnectorMetadataMaintenanceIoProfile::from_geometry(
                geometry,
                vec![SharedDependency::LocalHost],
            )
            .map_err(|e| internal_error(e.to_string()))?,
        )
        .map_err(|e| internal_error(e.to_string()))?;
        let peak = if purpose == FrontendQueryPurpose::ClientRows {
            window
                .client_all_objects_bytes()
                .map_err(|e| internal_error(e.to_string()))?
        } else {
            1
        };
        let provider = FrontendCompletionCustody::new();
        #[cfg(novarocks_dev_controls)]
        let observation = self
            .service
            .statement_observation
            .as_ref()
            .and_then(|o| o.bound_statement(statement.token()));
        let input = ReadPreparationInput {
            compiler: self.service.query_compiler.clone(),
            topology: self.service.topology.clone(),
            role: self.service.role,
            optimizer_limit: self.service.optimizer_query_mem_limit_bytes,
            sql,
            parsed,
            state,
            deadline,
            cancellation: QueryCancellationView::governed(
                statement.cancellation().clone(),
                statement.timeout_ms(),
            ),
            scope: statement.scope().clone(),
            window,
            purpose,
            provider: Arc::clone(&provider),
            io: Arc::clone(&io),
            #[cfg(novarocks_dev_controls)]
            observation: observation.clone(),
            #[cfg(novarocks_dev_controls)]
            statement_token: statement.token(),
        };
        let provider_close = Some(Box::pin(close_provider(Arc::clone(&provider))) as ProviderClose);
        let action = ReadStatementAction {
            provider,
            provider_close,
            provider_close_result: None,
            local: None,
            tail: Some(Box::new(ReadNativeTail {
                io,
                stage: StatementStageAdmission::new(statement.scope().clone(), peak),
                exit: None,
                receipt: None,
                launched: false,
                health: None,
            })),
            preparation: Some(Box::pin(prepare_read(input))),
            prepared: None,
            read: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            error: None,
            completed_preparation: false,
            preparation_started: false,
            bootstrap_ready: false,
            result_transferred: false,
            launcher: Arc::clone(&self.service.logical_execution_launcher),
            #[cfg(novarocks_dev_controls)]
            observation,
        };
        #[cfg(novarocks_dev_controls)]
        let handoff_statement = statement.token();
        #[cfg(novarocks_dev_controls)]
        let handoff_target = self
            .service
            .statement_observation
            .as_ref()
            .and_then(|o| o.bound_statement(handoff_statement))
            .and_then(|h| h.statement_target());
        let result = self
            .execute_observed_statement_action(statement, Box::new(action))
            .await?;
        #[cfg(novarocks_dev_controls)]
        if let StatementResult::StreamingQuery(stream) = &result {
            if let Some(control) = &self.service.result_handoff {
                control
                    .before_protocol(handoff_statement, handoff_target, stream.failure_view())
                    .await
                    .map_err(internal_error)?;
            }
        }
        Ok(result)
    }
}
pub(crate) fn fixed_source_bytes(
    metadata: &ConnectorMetadataMaintenanceIoGeometry,
    k: usize,
) -> Result<u64, WorkError> {
    use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    fn future_layout<I, F: Future>(_: impl Fn(I) -> F) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    let source_mutex = if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
        std::alloc::Layout::new::<(isize, [u8; 56])>()
    } else if cfg!(target_os = "linux") {
        std::alloc::Layout::new::<()>()
    } else {
        return Err(WorkError::InvalidConfig(
            "unsupported Read source mutex backing model",
        ));
    };
    let layouts = [
        std::alloc::Layout::new::<ReadStatementAction>(),
        std::alloc::Layout::new::<ReadNativeTail>(),
        future_layout(prepare_read),
        future_layout(close_provider),
        arc_layout(std::alloc::Layout::new::<ReadMetadataIoSource>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        source_mutex,
        arc_layout(std::alloc::Layout::new::<WorkError>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        // The original preparation and Client stage constructors each create
        // one activity Arc; both coexist before the original action exits.
        QueryResultCapacityBinding::fixed_backing_layout()?,
        QueryResultCapacityBinding::fixed_backing_layout()
            .map_err(|_| WorkError::ArithmeticOverflow)?,
    ];
    let mut total =
        MetadataIoAdmission::fixed_backing_bytes(metadata.max_concurrent_io().get(), k)?;
    for layout in layouts.into_iter().chain(
        FrontendCompletionCustody::known_layouts().map_err(|_| WorkError::ArithmeticOverflow)?,
    ) {
        total = total
            .checked_add(layout.size() as u64)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    total
        .checked_add(super::statement_computation::stage_heap_source_bytes()? as u64)
        .ok_or(WorkError::ArithmeticOverflow)
}

/// Pure geometry of the original local preparation used by the empty-Provider
/// real-entry fixture. It supports no external Provider factory or data read.
#[cfg(test)]
pub(crate) fn empty_provider_preparation_geometry_for_test()
-> Result<ConnectorMetadataMaintenanceIoGeometry, WorkError> {
    use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
    let header = u64::try_from(std::mem::size_of::<ReadPreparationInput>())
        .map_err(|_| WorkError::ArithmeticOverflow)?;
    Ok(ConnectorMetadataMaintenanceIoGeometry::new(
        std::num::NonZeroUsize::new(1).unwrap(),
        std::num::NonZeroU64::new(MetadataIoAdmission::request_metadata_bytes())
            .ok_or(WorkError::ArithmeticOverflow)?,
        std::num::NonZeroU64::new(header).ok_or(WorkError::ArithmeticOverflow)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::native_execution_adapter::source_cohort::tests::{finish, fixture};
    use novarocks_workload_control::{WorkClass, WorkRequest};
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        task::Waker,
    };
    fn geometry(io: usize) -> ConnectorMetadataMaintenanceIoGeometry {
        ConnectorMetadataMaintenanceIoGeometry::new(
            NonZeroUsize::new(io).unwrap(),
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        )
    }
    #[test]
    fn actual_read_source_rejects_factory_over_geometry_before_install_or_dispatch() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source = ReadMetadataIoSource::new(root.owner.scope(), geometry(1), 1);
        let actual = ConnectorMetadataMaintenanceIoProfile::from_geometry(
            geometry(2),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        assert!(source.configure_read_source(&actual).is_err());
        assert!(source.original().is_none());
        drop(source);
        finish(control, root, driver);
    }
    #[test]
    fn actual_read_request_cannot_dispatch_before_original_root_source_proof() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source = ReadMetadataIoSource::new(root.owner.scope(), geometry(1), 1);
        let profile = ConnectorMetadataMaintenanceIoProfile::from_geometry(
            geometry(1),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        source.configure_read_source(&profile).unwrap();
        let request = source.request(SharedDependency::LocalHost).unwrap();
        let mut permit = None;
        let mut cx = Context::from_waker(Waker::noop());
        assert!(request.poll_admitted_in(&mut permit, &mut cx).is_pending());
        assert!(permit.is_none());
        assert!(source.poll_read_source_ready(&mut cx).is_pending());
        assert!(
            matches!(
                root.owner.scope().try_request_joint_child(),
                Err(WorkError::Conflict)
            ),
            "empty-D child guard is still enforced"
        );
        drop((request, source));
        finish(control, root, driver);
    }
    #[test]
    fn ordinary_read_tail_cannot_treat_absent_native_observer_as_actual_exit() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut tail = ReadNativeTail {
            io: ReadMetadataIoSource::new(root.owner.scope(), geometry(1), 1),
            stage: StatementStageAdmission::new(root.owner.scope(), 1),
            exit: None,
            receipt: None,
            launched: true,
            health: None,
        };
        let result = tail.poll_exit(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(result, Poll::Ready(Err(_))));
        assert!(tail.health.is_some());
        assert!(tail.receipt.is_none());
        drop(tail);
        finish(control, root, driver);
    }
}

#[cfg(test)]
mod provider_close_tests {
    use super::*;
    use std::task::Waker;
    #[test]
    fn uninstalled_provider_still_receives_original_close_before_local_read_exit() {
        let source = FrontendCompletionCustody::new();
        let mut future = Some(Box::pin(close_provider(Arc::clone(&source))) as ProviderClose);
        let mut receipt = None;
        assert!(matches!(
            poll_provider_close(
                &mut future,
                &mut receipt,
                &mut Context::from_waker(Waker::noop())
            ),
            Poll::Ready(Ok(()))
        ));
        assert!(future.is_none());
        assert!(matches!(receipt, Some(Ok(()))));
        assert!(source.local_health_failure().is_none());
    }
}
