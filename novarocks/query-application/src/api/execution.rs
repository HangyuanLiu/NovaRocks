// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use novarocks_workload_control::{CancellationReason, WorkCancellationRequester, WorkOwner};

#[cfg(test)]
use super::NativeExecutionContractError;
use super::{
    ComputationReadyToken, InternalComputationConsumer, NativeAttemptPreparationPort,
    NativeLogicalExecutionSeed,
};
use crate::coordination::{AbortQueryContextEffectPort, ReplacementQualificationEffectPort};
use crate::preparation::{ExecutionFinishBoundary, FrozenExecutionDescription};

/// A product-independent query operation.
///
/// The request directly owns SQL's typed, topology-free distributed plan and
/// its validated application contracts. Attempts read those values without a
/// serialization or decoding layer.
pub struct QueryExecutionRequest {
    description: FrozenExecutionDescription,
    native_seed: NativeLogicalExecutionSeed,
    internal_consumer: Option<Arc<dyn InternalComputationConsumer>>,
}

impl QueryExecutionRequest {
    pub const fn kind(&self) -> QueryExecutionKind {
        self.description.kind()
    }

    /// Atomically bind the semantic description to its role-local Native
    /// capabilities.
    ///
    /// The opaque seed has no public constructor and cannot be detached from
    /// this request. Its plan seal is copied from the already frozen
    /// description here, including for plans that contain no scans. A rejected
    /// request drops the exact seed; an accepted request can bind it to only
    /// the open ticket minted for that logical execution.
    pub fn bind_native(
        description: FrozenExecutionDescription,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> Self {
        let native_seed = NativeLogicalExecutionSeed::issue(
            description.plan_identity(),
            aborts,
            replacements,
            attempts,
        );
        Self {
            description,
            native_seed,
            internal_consumer: None,
        }
    }

    /// Registers the fixed application consumer for an internal computation.
    /// The frozen finish boundary determines whether this capability is legal;
    /// registering a consumer cannot change client EOF or Task completion.
    pub fn with_internal_consumer(
        mut self,
        consumer: Arc<dyn InternalComputationConsumer>,
    ) -> Result<Self, QueryExecutionError> {
        if self.description.finish_boundary() != ExecutionFinishBoundary::ComputationReady {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "an internal consumer requires the frozen computation-ready finish boundary",
            ));
        }
        if self.internal_consumer.is_some() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "an internal computation consumer is already registered",
            ));
        }
        self.internal_consumer = Some(consumer);
        Ok(self)
    }

    /// Checks registration before the supervisor may open Native resources.
    pub(crate) fn validate_internal_consumer(&self) -> Result<(), QueryExecutionError> {
        let internal =
            self.description.finish_boundary() == ExecutionFinishBoundary::ComputationReady;
        if internal != self.internal_consumer.is_some() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "the frozen finish boundary and internal consumer registration differ",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn try_from_parts(
        description: FrozenExecutionDescription,
        native_seed: NativeLogicalExecutionSeed,
    ) -> Result<Self, NativeExecutionContractError> {
        if description.plan_identity() != native_seed.plan() {
            return Err(NativeExecutionContractError::ForeignLogicalSeedPlan);
        }
        Ok(Self {
            description,
            native_seed,
            internal_consumer: None,
        })
    }

    pub const fn description(&self) -> &FrozenExecutionDescription {
        &self.description
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        FrozenExecutionDescription,
        NativeLogicalExecutionSeed,
        Option<Arc<dyn InternalComputationConsumer>>,
    ) {
        assert_eq!(
            self.description.plan_identity(),
            self.native_seed.plan(),
            "executable request must retain its atomically issued Native seed"
        );
        (self.description, self.native_seed, self.internal_consumer)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionKind {
    Read,
    Write,
    Statistics,
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionErrorKind {
    InvalidRequest,
    Rejected,
    Cancelled,
    DeadlineExceeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryExecutionErrorBody {
    Application {
        kind: QueryExecutionErrorKind,
        message: Arc<str>,
    },
    Execution(Box<crate::execution_error::ExecutionFailureInput>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionError {
    body: QueryExecutionErrorBody,
}
impl QueryExecutionError {
    /// Application/control errors with no domain execution conclusion.
    pub fn new(kind: QueryExecutionErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            body: QueryExecutionErrorBody::Application {
                kind,
                message: message.into(),
            },
        }
    }
    pub fn from_execution(input: crate::execution_error::ExecutionFailureInput) -> Self {
        Self {
            body: QueryExecutionErrorBody::Execution(Box::new(input)),
        }
    }
    pub fn execution_failure(&self) -> Option<&crate::execution_error::ExecutionFailureInput> {
        match &self.body {
            QueryExecutionErrorBody::Execution(input) => Some(input.as_ref()),
            QueryExecutionErrorBody::Application { .. } => None,
        }
    }
    pub fn kind(&self) -> QueryExecutionErrorKind {
        match &self.body {
            QueryExecutionErrorBody::Application { kind, .. } => *kind,
            QueryExecutionErrorBody::Execution(input) => input.error_kind(),
        }
    }
    pub fn message(&self) -> &str {
        match &self.body {
            QueryExecutionErrorBody::Application { message, .. } => message,
            QueryExecutionErrorBody::Execution(input) => input.detail().as_str(),
        }
    }
}
impl fmt::Display for QueryExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for QueryExecutionError {}

pub type QueryExecutionFuture =
    Pin<Box<dyn Future<Output = Result<ExecutionHandle, QueryExecutionError>> + Send + 'static>>;

/// A single initial execution reserved before statement preparation.
///
/// The identity is correlation data; starting still consumes the admitted
/// Work owner. The private issuer binds this move-only reservation to the
/// same supervisor that allocated it.
#[must_use = "a reservation can be consumed by one start or deliberately abandoned"]
pub struct ReservedExecutionIdentity {
    execution: novarocks_types::identity::QueryExecutionId,
    issuer: Arc<()>,
}

impl fmt::Debug for ReservedExecutionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReservedExecutionIdentity")
            .field("execution", &self.execution)
            .finish_non_exhaustive()
    }
}

impl ReservedExecutionIdentity {
    pub const fn execution(&self) -> novarocks_types::identity::QueryExecutionId {
        self.execution
    }

    pub(crate) fn issued(
        execution: novarocks_types::identity::QueryExecutionId,
        issuer: Arc<()>,
    ) -> Self {
        Self { execution, issuer }
    }

    pub(crate) fn consume(
        self,
        issuer: &Arc<()>,
    ) -> Result<novarocks_types::identity::QueryExecutionId, QueryExecutionError> {
        if !Arc::ptr_eq(&self.issuer, issuer) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "execution reservation belongs to a different supervisor",
            ));
        }
        Ok(self.execution)
    }
}

/// Crate-private process driver behind the bounded client capability.
pub(crate) trait QueryExecutionDriver: Send + Sync + 'static {
    fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError>;
    fn start(
        &self,
        request: QueryExecutionRequest,
        owner: WorkOwner,
        reserved: Option<ReservedExecutionIdentity>,
    ) -> QueryExecutionFuture;
}

/// Cloneable capability for starting logical executions.
///
/// Starting consumes the single work owner. A caller therefore cannot submit
/// one governed responsibility twice or omit governance accidentally.
///
/// ```compile_fail
/// use novarocks_query_application::api::{QueryExecutionClient, QueryExecutionRequest};
/// fn start_without_governance(
///     client: &QueryExecutionClient,
///     request: QueryExecutionRequest,
/// ) {
///     let _ = client.start(request);
/// }
/// ```
#[derive(Clone)]
pub struct QueryExecutionClient {
    driver: Arc<dyn QueryExecutionDriver>,
}

impl QueryExecutionClient {
    pub(crate) fn new(driver: impl QueryExecutionDriver) -> Self {
        Self {
            driver: Arc::new(driver),
        }
    }

    pub fn start(&self, request: QueryExecutionRequest, owner: WorkOwner) -> QueryExecutionFuture {
        self.driver.start(request, owner, None)
    }

    pub fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
        self.driver.reserve_identity()
    }

    pub fn start_reserved(
        &self,
        request: QueryExecutionRequest,
        owner: WorkOwner,
        reserved: ReservedExecutionIdentity,
    ) -> QueryExecutionFuture {
        self.driver.start(request, owner, Some(reserved))
    }
}

/// Driver-owned control of a running logical execution.
pub trait ExecutionControl: Send + 'static {
    fn request_cancel(&self) -> Result<(), QueryExecutionError>;
}

impl ExecutionControl for WorkCancellationRequester {
    fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.request(CancellationReason::Requested)
            .map_err(|error| {
                use novarocks_execution_contract::failure::FailureProjection;
                QueryExecutionError::from_execution(
                    crate::execution_error::ExecutionFailureInput::new(
                        error.failure_fact(),
                        novarocks_execution_contract::SafeDetail::truncating(
                            "query workload control refused the cancellation request",
                        ),
                        crate::execution_error::ExecutionFailureSite::Coordinator,
                    ),
                )
            })
    }
}

/// Move-only logical execution handle.
///
/// Dropping a handle is not a stop or resource-release fact. Bounded result
/// delivery and convergence remain owned by the supervised logical execution.
///
/// ```compile_fail
/// use novarocks_query_application::api::ExecutionHandle;
/// fn duplicate(handle: ExecutionHandle) {
///     let _copy = handle.clone();
/// }
/// ```
pub struct ExecutionHandle {
    control: Box<dyn ExecutionControl>,
    output: Option<crate::api::ExecutionOutput>,
    ready: Option<ComputationReadyToken>,
}

impl ExecutionHandle {
    pub(crate) fn new(control: impl ExecutionControl, output: crate::api::ExecutionOutput) -> Self {
        Self {
            control: Box::new(control),
            output: Some(output),
            ready: None,
        }
    }

    pub(crate) fn computation_ready(
        control: impl ExecutionControl,
        token: ComputationReadyToken,
    ) -> Self {
        Self {
            control: Box::new(control),
            output: None,
            ready: Some(token),
        }
    }

    /// Observes the accepted computation cut without granting publication rights.
    pub const fn ready_token(&self) -> Option<&ComputationReadyToken> {
        self.ready.as_ref()
    }

    pub fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.control.request_cancel()
    }

    /// Transfers the single result consumer to the protocol/application owner.
    /// Cancellation remains available on this handle while that output is in
    /// use, and taking it twice cannot create another consumer.
    pub fn take_output(&mut self) -> Option<crate::api::ExecutionOutput> {
        self.output.take()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crate::api::ExecutionOutput;
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    struct RecordingControl(Arc<AtomicBool>);

    impl ExecutionControl for RecordingControl {
        fn request_cancel(&self) -> Result<(), QueryExecutionError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn execution_handle_transfers_output_once_and_keeps_control() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut handle = ExecutionHandle::new(
            RecordingControl(Arc::clone(&cancelled)),
            ExecutionOutput::Completion,
        );
        assert!(matches!(
            handle.take_output(),
            Some(ExecutionOutput::Completion)
        ));
        assert!(handle.take_output().is_none());
        handle.request_cancel().unwrap();
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn execution_handle_requests_cancellation_without_owning_work() {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let handle = ExecutionHandle::new(
            work.owner.cancellation_requester(),
            ExecutionOutput::Completion,
        );

        handle.request_cancel().unwrap();
        assert_eq!(
            scope.cancellation().unwrap().reason(),
            Some(CancellationReason::Requested)
        );
        assert_eq!(control.snapshot().root_responsibilities, 1);

        drop(handle);
        work.owner.complete();
        work.business.release();
    }

    #[tokio::test]
    async fn cancellation_refusal_retains_the_actual_released_work_fact() {
        use crate::execution_error::ExecutionFailureSite;
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let requester = work.owner.cancellation_requester();
        work.owner.complete();
        work.business.release();
        scope.wait_released().await;
        let original = requester
            .request(CancellationReason::Requested)
            .unwrap_err();
        assert!(matches!(
            original,
            novarocks_workload_control::WorkError::Released
        ));
        let error = ExecutionControl::request_cancel(&requester).unwrap_err();
        let input = error
            .execution_failure()
            .expect("original Work fact must survive the control boundary");
        assert_eq!(input.fact(), original.failure_fact());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            input.detail().as_str(),
            "query workload control refused the cancellation request"
        );
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(control.next_control().is_none());
    }
}
