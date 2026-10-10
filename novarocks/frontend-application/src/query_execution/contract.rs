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

//! Core-owned distributed-query request contract.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::query_execution::artifact::{
    PreparedDistributedAttemptTemplate, PreparedDistributedQuery,
};
use crate::query_execution::outcome::{DistributedQueryOutcome, QueryOutcomeFactory};
use crate::query_execution::statistics::StatisticsCollectionProgram;
use novarocks_execution::runtime::query_options::{
    QueryCacheOptions, QueryOptions as RuntimeQueryOptions,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_query_application::cancellation::QueryCancellationView;
use novarocks_query_application::preparation::FrozenExecutionDescription;
use novarocks_types::BackendProcessId;

/// Query options resolved by core before ownership crosses into frontend.
///
/// The runtime representation stays private; frontend only receives stable
/// scalar views needed to schedule, submit, and time out native work.
pub struct ResolvedQueryOptions {
    runtime: RuntimeQueryOptions,
}

impl ResolvedQueryOptions {
    pub(crate) fn from_upstream(options: Option<QueryOptions>) -> Self {
        let mut runtime = options
            .as_ref()
            .map(reconstruct_runtime_query_options)
            .unwrap_or_default();
        let pipeline_dop = novarocks_execution::runtime::exec_env::calc_pipeline_dop(
            runtime.pipeline_dop.unwrap_or_default(),
        );
        debug_assert!(pipeline_dop > 0, "resolved pipeline DOP must be positive");
        runtime.pipeline_dop = Some(pipeline_dop);
        Self { runtime }
    }

    pub fn timeout_ms(&self) -> i64 {
        self.runtime
            .query_timeout
            .map(|seconds| i64::from(seconds) * 1_000)
            .unwrap_or(300_000)
    }

    pub fn native_submission_options(&self) -> NativeSubmissionOptionsView {
        NativeSubmissionOptionsView {
            pipeline_dop: self
                .runtime
                .pipeline_dop
                .expect("core resolves pipeline DOP before request handoff"),
            enable_profile: self.runtime.enable_profile,
        }
    }

    pub fn runtime_filter_lifecycle(&self) -> RuntimeFilterLifecycleView {
        let (delivery_expire, query_expire) =
            novarocks_execution::runtime::query_options::query_expire_durations(Some(
                &self.runtime,
            ));
        RuntimeFilterLifecycleView {
            delivery_expire,
            query_expire,
        }
    }

    /// Frozen execution options exposed to the Frontend only for its
    /// role-owned native wire projection.  This does not provide lifecycle
    /// construction or a mutable execution handle.
    pub fn runtime_options(&self) -> &RuntimeQueryOptions {
        &self.runtime
    }
}

/// Project admitted statement semantics for synthetic executions that previously
/// supplied no options. Existing sealed options must not pass through this path.
pub(crate) fn synthetic_statement_query_options(execution: &QueryExecutionContext) -> QueryOptions {
    let mut runtime = RuntimeQueryOptions::default();
    runtime.set_allow_throw_exception(execution.sql_semantics().sql_mode().allow_throw_exception());
    QueryOptions::from_proto(
        crate::native::fragment_encoder::instance::encode_query_options(&runtime),
    )
}

/// Freeze a completed plan's per-instance driver range from the same resolved
/// query DOP that its eventual native request will use. Backend count controls
/// placement, not the number of drivers inside one placed fragment instance.
pub(crate) fn completed_plan_dop_domain(
    options: Option<&QueryOptions>,
) -> Result<novarocks_physical_plan::PipelineDopDomain, String> {
    let resolved = ResolvedQueryOptions::from_upstream(options.cloned())
        .native_submission_options()
        .pipeline_dop();
    let max = u32::try_from(resolved)
        .ok()
        .filter(|dop| (1..=novarocks_physical_plan::MAX_PIPELINE_DOP).contains(dop))
        .ok_or_else(|| format!("resolved query pipeline DOP {resolved} exceeds the plan limit"))?;
    Ok(novarocks_physical_plan::PipelineDopDomain {
        min: 1,
        max,
        requires_power_of_two: false,
    })
}

/// Reconstructs the Frontend-local execution view from the protocol value
/// without creating a second wire representation or decoder.
fn reconstruct_runtime_query_options(options: &QueryOptions) -> RuntimeQueryOptions {
    let src = options.as_proto();
    RuntimeQueryOptions {
        batch_size: (src.batch_size > 0).then_some(src.batch_size),
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (src.global_runtime_filter_build_max_size > 0)
            .then_some(src.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: src.enable_scan_datacache,
            enable_populate_datacache: src.enable_populate_datacache,
            enable_datacache_async_populate_mode: src.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: src.enable_datacache_io_adaptor,
            enable_cache_select: src.enable_cache_select,
            datacache_evict_probability: src.datacache_evict_probability,
            datacache_priority: (src.datacache_priority != 0).then_some(src.datacache_priority),
            datacache_ttl_seconds: (src.datacache_ttl_seconds > 0)
                .then_some(src.datacache_ttl_seconds),
            datacache_sharing_work_period: (src.datacache_sharing_work_period > 0)
                .then_some(src.datacache_sharing_work_period),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeSubmissionOptionsView {
    pipeline_dop: i32,
    enable_profile: bool,
}

impl NativeSubmissionOptionsView {
    pub const fn pipeline_dop(self) -> i32 {
        self.pipeline_dop
    }

    pub const fn enable_profile(self) -> bool {
        self.enable_profile
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterLifecycleView {
    delivery_expire: std::time::Duration,
    query_expire: std::time::Duration,
}

impl RuntimeFilterLifecycleView {
    pub const fn delivery_expire(self) -> std::time::Duration {
        self.delivery_expire
    }

    pub const fn query_expire(self) -> std::time::Duration {
        self.query_expire
    }
}

/// The engine-visible purpose of a distributed execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryIntent {
    Result,
    /// Signed bounded selection, validated before external COW write admission.
    CowMatch,
    Write,
    Profile,
    /// Internal collection execution. Its completion carries typed evidence,
    /// never a `QueryResult` that could be returned as user MySQL rows.
    Statistics,
}

/// An owned request passed from core to the injected execution coordinator.
///
/// Every field is private so role crates cannot assemble a request from
/// unrelated prepared/native artifacts or replace its cancellation/completion
/// capabilities.
pub struct DistributedQueryRequest {
    result_capacity:
        Option<novarocks_query_application::admitted_query_context::QueryResultCapacityBinding>,
    cow_match: Option<crate::query_execution::row_mutation::CowMatchRootConsumer>,
    payload: DistributedQueryPayload,
    topology: novarocks_query_application::api::BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    completion: QueryOutcomeFactory,
    write_root_decode_contract:
        Option<crate::query_execution::write_result::RootWriteDecodeContract>,
    statistics_program: Option<StatisticsCollectionProgram>,
    // Last: the original COW session outlives every request-owned payload.
    write_stack_session: Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
}

enum DistributedQueryPayload {
    RestartableRead(Arc<RestartableReadExecution>),
    SingleUse {
        description: Arc<FrozenExecutionDescription>,
        artifacts: PreparedDistributedQuery,
        options: Arc<ResolvedQueryOptions>,
    },
}

/// Closed, immutable source for every legacy coordinator round of one
/// restartable read.
///
/// The frozen description, static native template, Connector access recipes,
/// resolved options and result intent are created together by the sole
/// finalizer. This carrier deliberately does not pretend to be an executable
/// query-application request: that move-only request can be created only when
/// the production Native adapter also supplies its exact seed.
pub(crate) struct RestartableReadExecution {
    description: Arc<FrozenExecutionDescription>,
    attempt_template: PreparedDistributedAttemptTemplate,
    options: Arc<ResolvedQueryOptions>,
    intent: DistributedQueryIntent,
}

impl RestartableReadExecution {
    pub(crate) fn instantiate_attempt(
        self: &Arc<Self>,
        execution: &QueryExecutionContext,
    ) -> DistributedQueryRequest {
        DistributedQueryRequest {
            result_capacity: execution.result_capacity().cloned(),
            cow_match: None,
            payload: DistributedQueryPayload::RestartableRead(Arc::clone(self)),
            topology: execution.topology().clone(),
            deadline: execution.deadline(),
            cancellation: execution.cancellation().clone(),
            completion: QueryOutcomeFactory::new(self.intent),
            write_stack_session: None,
            write_root_decode_contract: None,
            statistics_program: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn instantiate_artifacts_for_test(&self) -> PreparedDistributedQuery {
        self.attempt_template.instantiate()
    }
}

impl DistributedQueryRequest {
    pub(crate) fn with_cow_match_consumer(
        mut self,
        consumer: crate::query_execution::row_mutation::CowMatchRootConsumer,
    ) -> Result<Self, DistributedQueryError> {
        if self.intent() != DistributedQueryIntent::CowMatch || self.cow_match.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "COW match consumer requires its dedicated single-use intent",
            ));
        }
        self.cow_match = Some(consumer);
        Ok(self)
    }

    /// Bind runtime capacity supplied by statement admission. This never enters
    /// the frozen semantic description or constructs a new result allowance.
    pub(crate) fn with_result_window(
        mut self,
        window: novarocks_workload_control::ResultWindowAlias,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<Self, DistributedQueryError> {
        if !window.is_for_scope(scope) {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "distributed request result window belongs to a foreign scope",
            ));
        }
        if self.result_capacity.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "distributed request already owns its result window",
            ));
        }
        scope.check().map_err(|error| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?;
        self.result_capacity = Some(novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(scope, window).map_err(|error| DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error.to_string()))?);
        Ok(self)
    }

    pub fn frozen_description(&self) -> &FrozenExecutionDescription {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => read.description.as_ref(),
            DistributedQueryPayload::SingleUse { description, .. } => description.as_ref(),
        }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.completion.intent()
    }

    pub fn options(&self) -> &ResolvedQueryOptions {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => read.options.as_ref(),
            DistributedQueryPayload::SingleUse { options, .. } => options.as_ref(),
        }
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    pub fn topology(&self) -> &novarocks_query_application::api::BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn statistics_program(&self) -> Option<&StatisticsCollectionProgram> {
        self.statistics_program.as_ref()
    }

    fn write_root_targets(
        &self,
    ) -> Option<&[novarocks_spi::connector::write_stack::WriteTargetOrdinal]> {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(_) => None,
            DistributedQueryPayload::SingleUse { artifacts, .. } => artifacts.write_root_targets(),
        }
    }

    /// Return the closed capability for replacement attempts when this logical
    /// execution's frozen recovery policy permits them.
    pub(crate) fn restartable_read(&self) -> Option<Arc<RestartableReadExecution>> {
        match &self.payload {
            DistributedQueryPayload::RestartableRead(read) => Some(Arc::clone(read)),
            DistributedQueryPayload::SingleUse { .. } => None,
        }
    }

    pub fn into_parts(self) -> DistributedQueryRequestParts {
        let (description, artifacts, options) = match self.payload {
            DistributedQueryPayload::RestartableRead(read) => (
                Arc::clone(&read.description),
                read.attempt_template.instantiate(),
                Arc::clone(&read.options),
            ),
            DistributedQueryPayload::SingleUse {
                description,
                artifacts,
                options,
            } => (description, artifacts, options),
        };
        DistributedQueryRequestParts {
            result_capacity: self.result_capacity,
            cow_match: self.cow_match,
            description,
            artifacts,
            options,
            topology: self.topology,
            deadline: self.deadline,
            cancellation: self.cancellation,
            completion: self.completion,
            write_stack_session: self.write_stack_session,
            write_root_decode_contract: self.write_root_decode_contract,
            statistics_program: self.statistics_program,
        }
    }
}

/// Consuming frontend handoff. There is deliberately no constructor,
/// `Clone`, or inverse recombination API.
pub struct DistributedQueryRequestParts {
    pub(crate) result_capacity:
        Option<novarocks_query_application::admitted_query_context::QueryResultCapacityBinding>,
    pub(crate) cow_match: Option<crate::query_execution::row_mutation::CowMatchRootConsumer>,
    pub description: Arc<FrozenExecutionDescription>,
    pub artifacts: PreparedDistributedQuery,
    pub options: Arc<ResolvedQueryOptions>,
    pub topology: novarocks_query_application::api::BackendTopologySnapshot,
    pub deadline: Option<Instant>,
    pub cancellation: QueryCancellationView,
    pub completion: QueryOutcomeFactory,
    pub(crate) write_root_decode_contract:
        Option<crate::query_execution::write_result::RootWriteDecodeContract>,
    pub statistics_program: Option<StatisticsCollectionProgram>,
    pub(crate) write_stack_session:
        Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>,
}

fn validate_completed_runtime_options(
    description: &FrozenExecutionDescription,
    options: &ResolvedQueryOptions,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), DistributedQueryError> {
    description
        .validate_legacy_intrinsic_allow_throw_exception(
            options.runtime_options().allow_throw_exception,
            control,
        )
        .map_err(DistributedQueryError::from_compile)
}

pub(crate) fn build_request_from_finalized_execution(
    finalized: crate::query_execution::post_compile::FinalizedDistributedExecution,
    options: Option<QueryOptions>,
    intent: DistributedQueryIntent,
    execution: &QueryExecutionContext,
    statistics_program: Option<StatisticsCollectionProgram>,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if (intent == DistributedQueryIntent::Statistics) != statistics_program.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "statistics intent and typed StatisticsCollectionProgram must be present together",
        ));
    }
    let (description, attempt_template) = finalized.into_parts();
    let description = Arc::new(description);
    let options = Arc::new(ResolvedQueryOptions::from_upstream(options));
    let control = crate::query_execution::planning::sql_compile_control_from_execution(execution);
    validate_completed_runtime_options(&description, &options, &control)?;
    let restartable_read = matches!(
        intent,
        DistributedQueryIntent::Result | DistributedQueryIntent::Profile
    ) && description.kind()
        == novarocks_query_application::api::QueryExecutionKind::Read
        && description.recovery()
            == novarocks_query_application::coordination::RecoveryMode::RestartAttemptBeforeVisibility;
    let payload = if restartable_read {
        DistributedQueryPayload::RestartableRead(Arc::new(RestartableReadExecution {
            description,
            attempt_template,
            options,
            intent,
        }))
    } else {
        DistributedQueryPayload::SingleUse {
            description,
            artifacts: attempt_template.instantiate(),
            options,
        }
    };
    Ok(DistributedQueryRequest {
        result_capacity: execution.result_capacity().cloned(),
        cow_match: None,
        payload,
        topology: execution.topology().clone(),
        deadline: execution.deadline(),
        cancellation: execution.cancellation().clone(),
        completion: QueryOutcomeFactory::new(intent),
        write_stack_session: None,
        write_root_decode_contract: None,
        statistics_program,
    })
}

/// Build a distributed request for internal statistics collection.  The
/// program is intentionally required here rather than carried in generic
/// query options, preventing a client-result request from acquiring a
/// statistics completion capability.
/// Attach the NCP-6 write session to a sealed distributed write request.
///
/// A query carries this or the placement-deferred writer template, never both:
/// they describe the same write through two different data planes, and a query
/// that claimed both would have two answers to "did this commit".
pub(crate) fn with_connector_write_session(
    mut request: DistributedQueryRequest,
    session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    if request.intent() != DistributedQueryIntent::Write {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "a connector write session is only valid for a distributed write request",
        ));
    }
    if request.write_stack_session.is_some() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "distributed query already has a connector write session",
        ));
    }
    let query_targets = request.write_root_targets().ok_or_else(|| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "distributed write plan has no query-local TableFinish target contract",
        )
    })?;
    let decode_contract = crate::query_execution::write_result::RootWriteDecodeContract::try_new(
        query_targets,
        session.targets(),
    )
    .map_err(|error| {
        DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
    })?;
    request.write_stack_session = Some(session);
    request.write_root_decode_contract = Some(decode_contract);
    Ok(request)
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    ContractViolation,
    Rejected,
    /// The statement observed a pre-ControlReady topology disposition, but
    /// its owner cannot prove the stable semantic binding and zero-effect
    /// conditions required to construct a replacement round.
    TopologyRetryUnsupported,
    Failed,
}

/// Closed, pre-ControlReady topology outcomes that a statement-level round
/// controller may consider for one bounded replan.  This is carried as typed
/// coordinator evidence rather than reconstructed from an error string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreReadyTopologyOutcome {
    BackendDraining {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    BackendProcessMismatch {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    BackendNotEligible {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
    /// The backend rejected the FE's exact native compatibility identity
    /// before ControlReady. This is retryable only through the existing
    /// effect-gated whole-round controller.
    CompatibilityMismatch {
        backend_idx: usize,
        process_id: BackendProcessId,
    },
}

/// A coordinator failure that core can surface without naming a coordinator
/// implementation or frontend state type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryError {
    root_fetch_failure: Option<novarocks_query_application::coordination::RootResultFetchFailure>,
    kind: DistributedQueryErrorKind,
    message: String,
    pre_ready_topology_outcome: Option<PreReadyTopologyOutcome>,
    pre_ready_topology_observation: bool,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
}

impl DistributedQueryError {
    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            root_fetch_failure: None,
            kind,
            message: message.into(),
            pre_ready_topology_outcome: None,
            pre_ready_topology_observation: false,
            compile_control: None,
        }
    }

    /// Constructed only by the pre-ControlReady coordinator/barrier path.
    /// Callers must never infer this disposition from transport text or a
    /// post-ready lifecycle failure.
    pub(crate) fn pre_ready_topology(
        outcome: PreReadyTopologyOutcome,
        message: impl Into<String>,
    ) -> Self {
        Self {
            root_fetch_failure: None,
            kind: DistributedQueryErrorKind::Rejected,
            message: message.into(),
            pre_ready_topology_outcome: Some(outcome),
            pre_ready_topology_observation: false,
            compile_control: None,
        }
    }

    /// A pre-ControlReady lifecycle transport loss can wait briefly for the
    /// membership authority to prove an exact captured-process replacement.
    /// It is not retry evidence by itself and must never be constructed from
    /// display text or after ControlReady.
    pub(crate) fn pre_ready_topology_observation(message: impl Into<String>) -> Self {
        Self {
            root_fetch_failure: None,
            kind: DistributedQueryErrorKind::Failed,
            message: message.into(),
            pre_ready_topology_outcome: None,
            pre_ready_topology_observation: true,
            compile_control: None,
        }
    }

    /// Turns typed pre-ready topology evidence into a fail-closed statement
    /// result when the operation has no whole-round replanning owner. The
    /// original outcome remains available to observability and callers; it is
    /// not reduced to transport/display text.
    pub(crate) fn topology_retry_unsupported(
        outcome: PreReadyTopologyOutcome,
        message: impl Into<String>,
    ) -> Self {
        Self {
            root_fetch_failure: None,
            kind: DistributedQueryErrorKind::TopologyRetryUnsupported,
            message: message.into(),
            pre_ready_topology_outcome: Some(outcome),
            pre_ready_topology_observation: false,
            compile_control: None,
        }
    }

    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        match crate::dml::error::DmlExecutionError::from_compile(error) {
            crate::dml::error::DmlExecutionError::Control(error) => {
                Self::from_compile_control(error)
            }
            crate::dml::error::DmlExecutionError::Analyze(error) => match error.control_error() {
                Some(error) => Self::from_compile_control(error),
                None => Self::new(
                    DistributedQueryErrorKind::ContractViolation,
                    error.to_string(),
                ),
            },
            crate::dml::error::DmlExecutionError::Engine(error) => {
                Self::new(DistributedQueryErrorKind::ContractViolation, error)
            }
        }
    }
    pub(crate) fn from_encode(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        match error {
            novarocks_plan_codec::PhysicalEncodeError::Control(error) => {
                Self::from_compile_control(error)
            }
            novarocks_plan_codec::PhysicalEncodeError::Invalid(error) => {
                Self::new(DistributedQueryErrorKind::ContractViolation, error)
            }
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(message) => {
                Self::new(DistributedQueryErrorKind::ContractViolation, message)
            }
        }
    }
    fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        let mut failure = Self::new(
            DistributedQueryErrorKind::ContractViolation,
            error.to_string(),
        );
        failure.compile_control = Some(error);
        failure
    }
    pub const fn compile_control_error(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control
    }

    /// Preserve the transport verdict after this coordinator has failed its
    /// installed attempt. It grants no pre-ready or transparent retry proof.
    pub(crate) fn with_root_fetch_failure(
        mut self,
        failure: novarocks_query_application::coordination::RootResultFetchFailure,
    ) -> Self {
        use novarocks_query_application::coordination::AttemptFailureClass as C;
        self.kind = match failure.class() {
            C::ResourceGovernance => DistributedQueryErrorKind::Rejected,
            C::ContractViolation => DistributedQueryErrorKind::ContractViolation,
            _ => DistributedQueryErrorKind::Failed,
        };
        self.root_fetch_failure = Some(failure);
        self
    }
    pub(crate) fn root_fetch_failure(
        &self,
    ) -> Option<&novarocks_query_application::coordination::RootResultFetchFailure> {
        self.root_fetch_failure.as_ref()
    }

    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) const fn pre_ready_topology_outcome(&self) -> Option<PreReadyTopologyOutcome> {
        self.pre_ready_topology_outcome
    }

    pub(crate) const fn requires_pre_ready_topology_observation(&self) -> bool {
        self.pre_ready_topology_observation
    }
}

impl fmt::Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for DistributedQueryError {}

/// Frontend-owned distributed query execution port.
pub trait DistributedQueryCoordinator: Send + Sync + 'static {
    /// Reserve the identity of one logical query without creating an
    /// execution attempt. SELECT preparation uses it only for diagnostics;
    /// the coordinator mints attempt identity after the frozen operation is
    /// submitted.
    fn reserve_logical_query(
        &self,
    ) -> Result<crate::query_execution::completion::LogicalQueryReservation, DistributedQueryError>
    {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "distributed query coordinator does not reserve logical query identities",
        ))
    }

    /// Reserve the first attempt identity before connector metadata
    /// materialization. Production owns the query-id source; injected test
    /// coordinators fail closed unless they explicitly implement this port.
    fn reserve_initial_attempt(
        &self,
    ) -> Result<crate::query_execution::completion::QueryAttemptReservation, DistributedQueryError>
    {
        let logical = self.reserve_logical_query()?;
        crate::query_execution::completion::QueryAttemptReservation::first(logical.into_query_id())
    }

    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError>;

    /// Execute one non-retriable reserved attempt. Callers use this when
    /// connector planning has already observed attempt-scoped capabilities
    /// but statement semantics do not permit automatic whole-round replanning.
    fn execute_reserved(
        &self,
        _request: DistributedQueryRequest,
        _reservation: crate::query_execution::completion::QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "injected coordinator does not implement reserved distributed attempts",
        ))
    }

    /// Execute a statement operation whose replacement rounds must carry a
    /// newly derived completion formatter as well as a newly derived request.
    /// The default retains legacy single-round behavior for narrow test
    /// coordinators; the production coordinator overrides it with the
    /// statement-level pre-ready controller.
    fn execute_prepared(
        &self,
        operation: crate::query_execution::completion::PreparedDistributedQuery,
    ) -> Result<
        novarocks_query_application::protocol_delivery::QuerySessionOutput,
        DistributedQueryError,
    > {
        let (request, completion, attempt_factory, _logical_reservation) = operation.into_parts();
        if attempt_factory.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "injected coordinator does not implement statement-level pre-ready replan",
            ));
        }
        let outcome = self.execute(request)?;
        completion
            .complete(outcome)
            .map_err(|error| DistributedQueryError::new(DistributedQueryErrorKind::Failed, error))
    }

    /// Execute a statement operation whose caller retains its raw outcome.
    /// This is deliberately separate from `execute_prepared`: a distributed
    /// write must preserve connector commit/abort handles for the frontend
    /// transaction runner and cannot be rendered as a `StatementResult`.
    fn execute_prepared_raw(
        &self,
        operation: crate::query_execution::completion::PreparedRawDistributedRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let (request, reservation) = operation.into_parts();
        if reservation.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "injected coordinator does not implement reserved raw distributed attempts",
            ));
        }
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "injected coordinator does not implement raw distributed execution for {:?}",
                request.intent()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{completed_plan_dop_domain, reconstruct_runtime_query_options};
    use novarocks_proto_codec::lifecycle::QueryOptions;
    use novarocks_proto_models::novarocks;

    #[test]
    fn synthetic_options_preserve_admitted_root_hints_and_absent_options_defaults() {
        use novarocks_query_application::admitted_query_context::{
            RequestAdmission, RequestContext,
        };
        use novarocks_query_application::api::BackendTopologySnapshot;
        use novarocks_query_application::cancellation::QueryCancellationSource;
        use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings, statement_sql_semantics};

        for (session_mode, sql, expected_allow) in [
            ("32", "SELECT 1", false),
            ("ALLOW_THROW_EXCEPTION", "SELECT 1", true),
            (
                "32",
                "SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ 1",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
                false,
            ),
        ] {
            let session = SqlSemanticSettings::default()
                .with_sql_mode(SqlMode::from_assignment(session_mode));
            let statements = novarocks_parser::parse(sql).unwrap();
            let admitted_semantics = statement_sql_semantics(&session, &statements[0]).unwrap();
            let cancellation = QueryCancellationSource::new();
            let context = RequestContext::admit(RequestAdmission::new(
                None,
                "default".to_owned(),
                novarocks_types::ClusterRole::Fe,
                BackendTopologySnapshot::empty(7),
                None,
                cancellation.view(),
                novarocks_sql::compiler::SessionOptimizerSettings::default(),
                admitted_semantics,
            ));
            let options = super::synthetic_statement_query_options(context.execution());
            assert_eq!(options.as_proto().allow_throw_exception, expected_allow);
            let reconstructed = reconstruct_runtime_query_options(&options);
            let mut expected = novarocks_execution::runtime::query_options::QueryOptions::default();
            expected.set_allow_throw_exception(expected_allow);
            assert_eq!(reconstructed, expected);

            let resolved = super::ResolvedQueryOptions::from_upstream(Some(options));
            let mut absent = super::ResolvedQueryOptions::from_upstream(None);
            absent.runtime.set_allow_throw_exception(expected_allow);
            assert_eq!(resolved.runtime_options(), absent.runtime_options());
            super::validate_completed_runtime_options(
                &intrinsic_runtime_description(expected_allow),
                &resolved,
                &runtime_gate_control(None),
            )
            .unwrap();
        }
    }

    fn intrinsic_runtime_description(allow: bool) -> super::FrozenExecutionDescription {
        use arrow::datatypes::DataType;
        use novarocks_physical_plan::{
            ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, PipelineDopDomain,
            PlanBuilder, PlanVersionId, ValueOrigin, ValueType,
        };
        use novarocks_query_application::preparation::{
            CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
            FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
        };
        use novarocks_type_contract::{
            DecimalOverflowPolicy, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
            SemanticParameterValue, SemanticParameters,
        };
        let mut fragment = FragmentBuilder::new(FragmentId::new(13));
        let node = fragment.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let literal = fragment
            .add_expression(node, ty.clone(), ExprKind::Literal(LiteralValue::Int64(4)))
            .unwrap();
        let cast = fragment
            .add_expression(
                node,
                ty.clone(),
                ExprKind::Cast {
                    expr: literal,
                    target: DataType::Int64,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: SemanticParameterRef {
                        id: SemanticParameterId::new(u32::MAX),
                        expected_key: SemanticParameterKey::AllowThrowException,
                    },
                },
            )
            .unwrap();
        let value = fragment
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        fragment
            .add_values(node, Box::from([Box::from([cast])]), Box::from([value]))
            .unwrap();
        let fragment = fragment
            .finish_definition(
                node,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let mut builder = PlanBuilder::new(PlanVersionId::try_new([29; 16]).unwrap())
            .with_semantic_parameters(
                SemanticParameters::try_new([(
                    SemanticParameterId::new(u32::MAX),
                    SemanticParameterValue::AllowThrowException(allow),
                )])
                .unwrap(),
            );
        builder.add_fragment(fragment).unwrap();
        let candidate = CompletedPhysicalPlanCandidate::for_program(
            builder.finish().unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Write,
            candidate,
            Vec::new(),
            OutputContract::CompletionOnly,
            novarocks_query_application::coordination::ExecutionEffect::None,
            novarocks_query_application::coordination::RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap()
    }

    struct RuntimeGateControl {
        calls: std::sync::Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    }
    impl novarocks_type_contract::PureCompileControl for RuntimeGateControl {
        fn checkpoint(
            &self,
            phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            match self.refuse {
                Some((ordinal, cause)) if calls.len() == ordinal + 1 => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn runtime_gate_control(
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    ) -> RuntimeGateControl {
        RuntimeGateControl {
            calls: std::sync::Mutex::new(Vec::new()),
            refuse,
        }
    }

    #[test]
    fn native_runtime_gate_compares_actual_resolved_options_with_the_same_candidate() {
        for actual in [false, true] {
            let options = super::ResolvedQueryOptions::from_upstream(Some(
                QueryOptions::from_proto(novarocks::QueryOptions {
                    allow_throw_exception: actual,
                    ..Default::default()
                }),
            ));
            let description = intrinsic_runtime_description(actual);
            super::validate_completed_runtime_options(
                &description,
                &options,
                &runtime_gate_control(None),
            )
            .unwrap();
            let error = super::validate_completed_runtime_options(
                &intrinsic_runtime_description(!actual),
                &options,
                &runtime_gate_control(None),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                super::DistributedQueryErrorKind::ContractViolation
            );
            assert_eq!(error.compile_control_error(), None);
        }
        let default_options = super::ResolvedQueryOptions::from_upstream(None);
        assert!(
            super::validate_completed_runtime_options(
                &intrinsic_runtime_description(true),
                &default_options,
                &runtime_gate_control(None)
            )
            .is_err()
        );
    }

    #[test]
    fn native_runtime_gate_preserves_all_typed_control_prefixes_in_the_terminal_owner() {
        use novarocks_type_contract::CompileControlError;
        let description = intrinsic_runtime_description(true);
        let options = super::ResolvedQueryOptions::from_upstream(Some(QueryOptions::from_proto(
            novarocks::QueryOptions {
                allow_throw_exception: true,
                ..Default::default()
            },
        )));
        for success in [true, false] {
            let description = if success {
                description.clone()
            } else {
                intrinsic_runtime_description(false)
            };
            let trace = runtime_gate_control(None);
            let result = super::validate_completed_runtime_options(&description, &options, &trace);
            assert_eq!(result.is_ok(), success);
            let callbacks = trace.calls.into_inner().unwrap();
            assert_eq!(callbacks.len(), 2);
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for ordinal in 0..callbacks.len() {
                    let trace = runtime_gate_control(Some((ordinal, cause)));
                    let error =
                        super::validate_completed_runtime_options(&description, &options, &trace)
                            .unwrap_err();
                    assert_eq!(error.compile_control_error(), Some(cause));
                    assert_eq!(*trace.calls.lock().unwrap(), callbacks[..=ordinal]);
                }
            }
        }
    }

    #[test]
    fn completed_plan_dop_domain_uses_resolved_driver_width_not_backend_count() {
        let explicit = QueryOptions::from_proto(novarocks::QueryOptions {
            pipeline_dop: 5,
            ..Default::default()
        });
        assert_eq!(completed_plan_dop_domain(Some(&explicit)).unwrap().max, 5);
        assert_eq!(
            completed_plan_dop_domain(None).unwrap().max as i32,
            novarocks_execution::runtime::exec_env::calc_pipeline_dop(0)
        );
    }

    #[test]
    fn reconstructed_runtime_options_preserve_protocol_scalars() {
        let protocol = QueryOptions::from_proto(novarocks::QueryOptions {
            batch_size: 4096,
            query_timeout: 60,
            query_delivery_timeout: 30,
            enable_profile: true,
            runtime_profile_report_interval: 7,
            pipeline_dop: 8,
            query_mem_limit: 1 << 20,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(5000),
            allow_throw_exception: true,
            group_concat_max_len: Some(65_535),
            enable_join_runtime_bitset_filter: Some(false),
            global_runtime_filter_build_max_size: 1 << 19,
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            enable_scan_datacache: true,
            enable_populate_datacache: true,
            enable_datacache_async_populate_mode: true,
            enable_datacache_io_adaptor: true,
            enable_cache_select: true,
            datacache_evict_probability: Some(75),
            datacache_priority: 2,
            datacache_ttl_seconds: 3600,
            datacache_sharing_work_period: 10,
        });

        let runtime = reconstruct_runtime_query_options(&protocol);

        assert_eq!(runtime.batch_size, Some(4096));
        assert_eq!(runtime.query_timeout, Some(60));
        assert_eq!(runtime.query_delivery_timeout, Some(30));
        assert!(runtime.enable_profile);
        assert_eq!(runtime.runtime_profile_report_interval, Some(7));
        assert_eq!(runtime.pipeline_dop, Some(8));
        assert_eq!(runtime.exec_mem_limit, Some(1 << 20));
        assert_eq!(runtime.runtime_filter_scan_wait_time_ms, Some(250));
        assert_eq!(runtime.runtime_filter_wait_timeout_ms, Some(5000));
        assert!(runtime.allow_throw_exception);
        assert_eq!(runtime.group_concat_max_len, Some(65_535));
        assert_eq!(runtime.enable_join_runtime_bitset_filter, Some(false));
        assert_eq!(runtime.global_runtime_filter_build_max_size, Some(1 << 19));
        assert!(runtime.orc_use_column_names);
        assert!(runtime.enable_file_metacache);
        assert!(runtime.enable_file_pagecache);
        assert!(runtime.enable_parquet_reader_page_index);
        assert_eq!(runtime.cache.datacache_evict_probability, Some(75));
        assert_eq!(runtime.cache.datacache_priority, Some(2));
        assert_eq!(runtime.cache.datacache_ttl_seconds, Some(3600));
        assert_eq!(runtime.cache.datacache_sharing_work_period, Some(10));
    }
}
