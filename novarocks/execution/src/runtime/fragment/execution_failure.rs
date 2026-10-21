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

//! Execution-owned failure payloads. These facts own no terminal latch.

use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
use novarocks_functions::{KernelDiagnostic, KernelFailure, RowDataError, Selection};
use novarocks_local_program::ProgramExpressionRootSite;
use std::{error::Error, fmt, sync::Arc};

pub type ExecutionResult<T> = Result<T, ExecutionFailure>;

/// Reports a known originating failure independently of the owner's exit.
/// Implementations decide acceptance; producers never wait for sibling cleanup
/// before submitting the cause and never infer a terminal state from this call.
pub trait ExecutionFailureReporter: Send + Sync {
    fn report_failure(&self, error: ExecutionFailure);
    fn report_cleanup(&self, cause: &ExecutionFailure);
    fn report_unresolved_exit(&self, cause: &ExecutionFailure);
}

/// A required root's remaining journal error, after its legal guards ran.
/// The original selected ordinal and message remain in the original payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequiredExpressionRowError {
    root: ProgramExpressionRootSite,
    batch_row: usize,
    error: RowDataError,
}

impl RequiredExpressionRowError {
    pub fn try_new(
        root: ProgramExpressionRootSite,
        selection: Selection<'_>,
        error: RowDataError,
    ) -> Result<Self, KernelFailure> {
        let batch_row = selection.row(error.selected_ordinal()).ok_or_else(|| {
            KernelFailure::Internal(KernelDiagnostic::new(
                "required expression error ordinal is outside its selection",
            ))
        })?;
        Ok(Self {
            root,
            batch_row,
            error,
        })
    }

    pub const fn root(&self) -> ProgramExpressionRootSite {
        self.root
    }
    pub const fn batch_row(&self) -> usize {
        self.batch_row
    }
    pub const fn error(&self) -> &RowDataError {
        &self.error
    }
}

impl fmt::Display for RequiredExpressionRowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "required expression {:?} failed at batch row {} (selected ordinal {}): {}",
            self.root,
            self.batch_row,
            self.error.selected_ordinal(),
            self.error.message()
        )
    }
}
impl Error for RequiredExpressionRowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionFailureCause {
    Domain(super::SharedDomainFailure),
    MemoryAccounting(crate::runtime::mem_tracker::MemoryAccountingFailure),
    Connector(super::connector_failure::ConnectorCallFailure),
    Writer(crate::exec::pipeline::writer_failure::WriterOwnerFailure),
    Construction(ConstructionFailure),
    Kernel(KernelFailure),
    RuntimeFilter(crate::runtime_filter::RuntimeFilterFailure),
    RuntimeScalarSource(novarocks_functions::ScalarResourceError),
    RuntimeScalarMemory(crate::runtime::scalar_memory::RuntimeScalarMemoryRefusal),
    InvocationData(novarocks_functions::InvocationData),
    ScalarInvocationData(novarocks_functions::ScalarInvocationData),
    WindowInvocationData(novarocks_functions::WindowInvocationData),
    RequiredRow(RequiredExpressionRowError),
    PreparationMetadata(crate::runtime::preparation_metadata::PreparationMetadataFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineOperation {
    Control,
    Preparation,
    Push,
    Pull,
    Finishing,
    Admission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionFailureContext {
    pub operator_ordinal: usize,
    pub operation: PipelineOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailure {
    // Rich source material is allocated only when a failure occurs. Clones
    // and added call-site context retain this same local occurrence identity.
    cause: Arc<ExecutionFailureCause>,
    context: Option<ExecutionFailureContext>,
}

impl ExecutionFailure {
    pub fn from_domain<E>(cause: E) -> Self
    where
        E: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
    {
        Self {
            cause: Arc::new(ExecutionFailureCause::Domain(
                super::SharedDomainFailure::new(cause),
            )),
            context: None,
        }
    }

    pub fn cause(&self) -> &ExecutionFailureCause {
        self.cause.as_ref()
    }
    /// A repeated observation of one originating failure is distinct from two
    /// independent failures with equal facts or text. Identity is local only;
    /// role boundaries project neutral facts and never encode this pointer.
    pub(crate) fn same_occurrence(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cause, &other.cause)
    }
    pub const fn context(&self) -> Option<ExecutionFailureContext> {
        self.context
    }

    pub fn detail(&self) -> &str {
        match self.cause.as_ref() {
            ExecutionFailureCause::Domain(error) => error.detail(),
            ExecutionFailureCause::MemoryAccounting(error) => error.detail(),
            ExecutionFailureCause::Connector(error) => error.detail(),
            ExecutionFailureCause::Writer(error) => error.detail(),
            ExecutionFailureCause::Construction(error) => error.detail(),
            ExecutionFailureCause::RuntimeFilter(error) => error.detail(),
            ExecutionFailureCause::RequiredRow(error) => error.error().message(),
            ExecutionFailureCause::InvocationData(error) => error.message(),
            ExecutionFailureCause::ScalarInvocationData(error) => error.message(),
            ExecutionFailureCause::WindowInvocationData(error) => error.message(),
            ExecutionFailureCause::PreparationMetadata(error) => error.detail(),
            ExecutionFailureCause::RuntimeScalarSource(_) => "scalar runtime source request failed",
            ExecutionFailureCause::RuntimeScalarMemory(_) => "scalar runtime memory refused",
            ExecutionFailureCause::Kernel(error) => match error {
                KernelFailure::Source(source) => source.detail(),
                KernelFailure::CompileControl(source) => source.detail(),
                KernelFailure::Capacity(_) => "kernel byte capacity was refused",
                KernelFailure::Cancelled => "kernel evaluation was cancelled",
                KernelFailure::DeadlineExceeded => "kernel evaluation deadline was exceeded",
                KernelFailure::ResourceExhausted => "kernel evaluation resources were exhausted",
                KernelFailure::InstanceFailed => "kernel instance has already failed",
                KernelFailure::InvalidProgram(message)
                | KernelFailure::Internal(message)
                | KernelFailure::Operational(message) => message.message(),
            },
        }
    }

    /// Render only neutral facts and typed execution location for status and
    /// secondary diagnostics. A provider's arbitrary Display is local source
    /// material, not a redaction guarantee, even when bounded by SafeDetail.
    pub fn safe_detail(&self) -> novarocks_execution_contract::status::SafeDetail {
        use novarocks_failure_contract::FailureProjection;
        self.safe_detail_for_fact(self.failure_fact())
    }

    /// Render one already frozen fact with this carrier's typed location.
    /// This formatting path never projects or renders the original source.
    pub fn safe_detail_for_fact(
        &self,
        fact: novarocks_failure_contract::FailureFact,
    ) -> novarocks_execution_contract::status::SafeDetail {
        struct Summary<'a>(
            &'a ExecutionFailure,
            novarocks_failure_contract::FailureFact,
        );
        impl fmt::Display for Summary<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let fact = self.1;
                write!(f, "{:?} failure (reason {})", fact.class(), fact.reason())?;
                if let Some(context) = self.0.context {
                    write!(
                        f,
                        " at operator {} during {:?}",
                        context.operator_ordinal, context.operation
                    )?;
                }
                if let Some(position) = fact.row_position() {
                    write!(
                        f,
                        " at batch row {} (selected ordinal {})",
                        position.batch_row, position.selected_ordinal
                    )?;
                }
                if let Some(resource) = fact.resource() {
                    write!(f, "; {:?}", resource.dimension)?;
                    if let Some(requested) = resource.requested {
                        write!(f, " requested={requested}")?;
                    }
                    if let Some(limit) = resource.limit {
                        write!(f, " limit={limit}")?;
                    }
                    if let Some(committed) = resource.committed {
                        write!(f, " committed={committed}")?;
                    }
                }
                if let Some(representation) = fact.representation() {
                    write!(
                        f,
                        "; {:?} layout {}:{}",
                        representation.boundary,
                        representation.layout.domain,
                        representation.layout.code
                    )?;
                    if let Some(required) = representation.required {
                        write!(f, " required={required}")?;
                    }
                    if let Some(prefix) = representation.extent_limit {
                        write!(f, " extent_limit={prefix}")?;
                    }
                }
                Ok(())
            }
        }
        novarocks_execution_contract::status::SafeDetail::truncating_display(Summary(self, fact))
    }

    /// Attach the exact driver-local operation without formatting the cause
    /// into a different failure category or allocating a second diagnostic.
    pub fn at_operator(mut self, operator_ordinal: usize, operation: PipelineOperation) -> Self {
        if self.context.is_none() {
            self.context = Some(ExecutionFailureContext {
                operator_ordinal,
                operation,
            });
        }
        self
    }
}

impl From<crate::runtime::preparation_metadata::PreparationMetadataFailure> for ExecutionFailure {
    fn from(error: crate::runtime::preparation_metadata::PreparationMetadataFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::PreparationMetadata(error)),
            context: None,
        }
    }
}

impl From<super::connector_failure::ConnectorCallFailure> for ExecutionFailure {
    fn from(error: super::connector_failure::ConnectorCallFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::Connector(error)),
            context: None,
        }
    }
}
impl From<crate::exec::pipeline::writer_failure::WriterFailure> for ExecutionFailure {
    fn from(error: crate::exec::pipeline::writer_failure::WriterFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::Writer(error.into())),
            context: None,
        }
    }
}

impl From<crate::runtime::mem_tracker::MemoryAccountingFailure> for ExecutionFailure {
    fn from(error: crate::runtime::mem_tracker::MemoryAccountingFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::MemoryAccounting(error)),
            context: None,
        }
    }
}
impl From<novarocks_functions::generate_series_core::SeriesError> for ExecutionFailure {
    fn from(cause: novarocks_functions::generate_series_core::SeriesError) -> Self {
        Self::from_domain(cause)
    }
}

impl From<KernelFailure> for ExecutionFailure {
    fn from(error: KernelFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::Kernel(error)),
            context: None,
        }
    }
}
impl From<ConstructionFailure> for ExecutionFailure {
    fn from(error: ConstructionFailure) -> Self {
        match error {
            // Preserve the originating failure, including its existing driver
            // context, rather than burying its category under construction.
            ConstructionFailure::Caller(error) => *error,
            error => Self {
                cause: Arc::new(ExecutionFailureCause::Construction(error)),
                context: None,
            },
        }
    }
}
impl From<crate::runtime::exchange::ExchangeFailure> for ExecutionFailure {
    fn from(source: crate::runtime::exchange::ExchangeFailure) -> Self {
        match source {
            crate::runtime::exchange::ExchangeFailure::Source(source) => Self {
                cause: Arc::new(ExecutionFailureCause::Domain(source)),
                context: None,
            },
            crate::runtime::exchange::ExchangeFailure::Execution(source) => source,
        }
    }
}
impl From<crate::runtime_filter::RuntimeFilterFailure> for ExecutionFailure {
    fn from(error: crate::runtime_filter::RuntimeFilterFailure) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::RuntimeFilter(error)),
            context: None,
        }
    }
}
impl From<novarocks_functions::EvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::EvaluationFailure) -> Self {
        match error {
            novarocks_functions::EvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::EvaluationFailure::InvocationData(cause) => Self {
                cause: Arc::new(ExecutionFailureCause::InvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<novarocks_functions::ScalarInvocationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::ScalarInvocationFailure) -> Self {
        match error {
            novarocks_functions::ScalarInvocationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::ScalarInvocationFailure::Data(cause) => Self {
                cause: Arc::new(ExecutionFailureCause::ScalarInvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure> for ExecutionFailure {
    fn from(error: crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure) -> Self {
        use crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure;
        let cause = match error {
            RuntimeScalarEvaluationFailure::Kernel(cause) => return cause.into(),
            RuntimeScalarEvaluationFailure::Data(cause) => {
                ExecutionFailureCause::ScalarInvocationData(cause)
            }
            RuntimeScalarEvaluationFailure::Source(cause) => {
                ExecutionFailureCause::RuntimeScalarSource(cause)
            }
            RuntimeScalarEvaluationFailure::Host(cause) => {
                ExecutionFailureCause::RuntimeScalarMemory(cause)
            }
        };
        Self {
            cause: Arc::new(cause),
            context: None,
        }
    }
}
impl From<novarocks_functions::WindowEvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::WindowEvaluationFailure) -> Self {
        match error {
            novarocks_functions::WindowEvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::WindowEvaluationFailure::InvocationData(cause) => Self {
                cause: Arc::new(ExecutionFailureCause::WindowInvocationData(cause)),
                context: None,
            },
        }
    }
}
impl From<RequiredExpressionRowError> for ExecutionFailure {
    fn from(error: RequiredExpressionRowError) -> Self {
        Self {
            cause: Arc::new(ExecutionFailureCause::RequiredRow(error)),
            context: None,
        }
    }
}
impl fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(context) = self.context {
            write!(
                f,
                "operator[{}] {:?}: ",
                context.operator_ordinal, context.operation
            )?;
        }
        match self.cause.as_ref() {
            ExecutionFailureCause::Domain(error) => error.fmt(f),
            ExecutionFailureCause::MemoryAccounting(error) => error.fmt(f),
            ExecutionFailureCause::Connector(error) => error.fmt(f),
            ExecutionFailureCause::Writer(error) => error.fmt(f),
            ExecutionFailureCause::Construction(error) => error.fmt(f),
            ExecutionFailureCause::Kernel(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeFilter(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarSource(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarMemory(error) => error.fmt(f),
            ExecutionFailureCause::RequiredRow(error) => error.fmt(f),
            ExecutionFailureCause::InvocationData(error) => error.fmt(f),
            ExecutionFailureCause::ScalarInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::WindowInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::PreparationMetadata(error) => error.fmt(f),
        }
    }
}
impl Error for ExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.cause.as_ref() {
            ExecutionFailureCause::Domain(error) => Some(error),
            ExecutionFailureCause::MemoryAccounting(error) => Some(error),
            ExecutionFailureCause::Connector(error) => Some(error),
            ExecutionFailureCause::Writer(error) => Some(error),
            ExecutionFailureCause::Construction(error) => Some(error),
            ExecutionFailureCause::Kernel(error) => Some(error),
            ExecutionFailureCause::RuntimeFilter(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarSource(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarMemory(error) => Some(error),
            ExecutionFailureCause::RequiredRow(error) => Some(error),
            ExecutionFailureCause::InvocationData(error) => Some(error),
            ExecutionFailureCause::ScalarInvocationData(error) => Some(error),
            ExecutionFailureCause::WindowInvocationData(error) => Some(error),
            ExecutionFailureCause::PreparationMetadata(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::operators::OwnedTopNBoundaryError;
    use crate::runtime_filter::contribution::{
        ContributionCodecError, ControlledContributionCodecError, ControlledOrderedTupleError,
        OrderedTupleError,
    };
    use crate::runtime_filter::{
        AggregateTopNBoundaryError, RuntimeFilterContractViolation,
        RuntimeFilterContractViolationKind, RuntimeFilterFailure,
    };

    /// Short owner-local presentation/source-chain oracle; messages are literal
    /// original contract texts. There is no service, fixture, or controller.
    #[test]
    fn typed_failure_runtime_filter_keeps_original_raw_source_formatter_and_context() {
        let contract = RuntimeFilterContractViolation::new(
            RuntimeFilterContractViolationKind::ContractMismatch,
            "ResourceExhausted: kernel evaluation was cancelled",
        );
        let unsupported = novarocks_local_program::FilterMembershipSchemaError::UnsupportedType;
        let corrupt = novarocks_local_program::FilterMembershipSchemaError::DigestMismatch;
        let cases: [(ExecutionFailure, RuntimeFilterFailure, &str); 6] = [
            (
                contract.clone().into(),
                RuntimeFilterFailure::Contract(contract),
                "runtime-filter contract violation ContractMismatch: ResourceExhausted: kernel evaluation was cancelled",
            ),
            (
                unsupported.into(),
                RuntimeFilterFailure::Schema(unsupported),
                unsupported.detail(),
            ),
            (
                corrupt.into(),
                RuntimeFilterFailure::Schema(corrupt),
                corrupt.detail(),
            ),
            (
                AggregateTopNBoundaryError::BoundLoosened.into(),
                RuntimeFilterFailure::Boundary(AggregateTopNBoundaryError::BoundLoosened),
                "aggregate TopN cumulative boundary unexpectedly loosened",
            ),
            (
                OrderedTupleError::ArityMismatch.into(),
                RuntimeFilterFailure::Tuple(OrderedTupleError::ArityMismatch),
                "ArityMismatch",
            ),
            (
                ContributionCodecError::Malformed.into(),
                RuntimeFilterFailure::Codec(ContributionCodecError::Malformed),
                "invalid runtime filter contribution: Malformed",
            ),
        ];
        for (failure, original, text) in cases {
            assert_eq!(
                failure.cause(),
                &ExecutionFailureCause::RuntimeFilter(original.clone())
            );
            assert_eq!(failure.context(), None);
            assert_eq!(failure.to_string(), text);
            let failure = failure
                .at_operator(7, PipelineOperation::Push)
                .at_operator(41, PipelineOperation::Pull);
            assert_eq!(failure.to_string(), format!("operator[7] Push: {text}"));
            let source = failure
                .source()
                .unwrap()
                .downcast_ref::<RuntimeFilterFailure>()
                .unwrap();
            assert_eq!(source, &original);
            let raw = source.source().unwrap();
            match original {
                RuntimeFilterFailure::Contract(original) => {
                    assert_eq!(
                        raw.downcast_ref::<RuntimeFilterContractViolation>(),
                        Some(&original)
                    );
                    assert_eq!(failure.detail(), original.detail());
                }
                RuntimeFilterFailure::Schema(original) => {
                    assert_eq!(
                        raw.downcast_ref::<novarocks_local_program::FilterMembershipSchemaError>(),
                        Some(&original)
                    );
                    assert_eq!(
                        novarocks_failure_contract::FailureProjection::failure_fact(&failure),
                        novarocks_failure_contract::FailureProjection::failure_fact(&original)
                    );
                    assert_eq!(failure.detail(), original.detail());
                }
                RuntimeFilterFailure::Boundary(original) => assert_eq!(
                    raw.downcast_ref::<AggregateTopNBoundaryError>(),
                    Some(&original)
                ),
                RuntimeFilterFailure::Tuple(original) => {
                    assert_eq!(raw.downcast_ref::<OrderedTupleError>(), Some(&original))
                }
                RuntimeFilterFailure::Codec(original) => assert_eq!(
                    raw.downcast_ref::<ContributionCodecError>(),
                    Some(&original)
                ),
            }
        }
    }

    #[test]
    fn typed_failure_runtime_filter_control_refusals_remain_the_original_kernel_category() {
        for original in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let failures: [ExecutionFailure; 3] = [
                OwnedTopNBoundaryError::Kernel(original.clone()).into(),
                ControlledOrderedTupleError::Kernel(original.clone()).into(),
                ControlledContributionCodecError::Kernel(original.clone()).into(),
            ];
            for failure in failures {
                assert_eq!(
                    failure.cause(),
                    &ExecutionFailureCause::Kernel(original.clone())
                );
                assert_eq!(
                    failure.source().unwrap().downcast_ref::<KernelFailure>(),
                    Some(&original)
                );
            }
        }
        let nested =
            AggregateTopNBoundaryError::CandidateContractMismatch(OrderedTupleError::TypeMismatch);
        let failure: ExecutionFailure = OwnedTopNBoundaryError::Boundary(nested.clone()).into();
        assert_eq!(
            failure.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Boundary(nested))
        );
        assert_eq!(
            failure.to_string(),
            "aggregate TopN candidate does not match its order contract: TypeMismatch"
        );
        let tuple: ExecutionFailure =
            ControlledOrderedTupleError::Tuple(OrderedTupleError::TypeMismatch).into();
        assert_eq!(
            tuple.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Tuple(
                OrderedTupleError::TypeMismatch
            ))
        );
        let codec: ExecutionFailure =
            ControlledContributionCodecError::Codec(ContributionCodecError::ResourceLimit).into();
        assert_eq!(
            codec.cause(),
            &ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Codec(
                ContributionCodecError::ResourceLimit
            ))
        );
    }

    #[test]
    fn typed_failure_text_cannot_impersonate_a_kernel_category_or_replace_context() {
        let failure = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new(
                &("ResourceExhausted: kernel evaluation was cancelled"),
            ),
        ))
        .at_operator(7, PipelineOperation::Push)
        .at_operator(41, PipelineOperation::Pull);
        assert!(matches!(
            failure.cause(),
            ExecutionFailureCause::Kernel(KernelFailure::Internal(_))
        ));
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&failure).class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        assert_eq!(
            failure.context(),
            Some(ExecutionFailureContext {
                operator_ordinal: 7,
                operation: PipelineOperation::Push
            })
        );
        assert!(failure.source().unwrap().is::<KernelFailure>());
    }
    #[test]
    fn failure_observers_share_one_occurrence_while_equal_new_failures_remain_distinct() {
        let first: ExecutionFailure =
            KernelFailure::Internal(KernelDiagnostic::new("same local message")).into();
        let observed = first.clone().at_operator(7, PipelineOperation::Push);
        assert!(first.same_occurrence(&observed));
        assert!(std::ptr::eq(first.cause(), observed.cause()));
        assert!(first.context().is_none());
        assert_eq!(observed.context().unwrap().operator_ordinal, 7);
        let independent: ExecutionFailure =
            KernelFailure::Internal(KernelDiagnostic::new("same local message")).into();
        assert_eq!(
            first, independent,
            "value comparison remains available for diagnostic oracles"
        );
        assert!(
            !first.same_occurrence(&independent),
            "text equality cannot deduplicate failures"
        );
        let forwarded: ExecutionFailure =
            ConstructionFailure::Caller(Box::new(observed.clone())).into();
        assert!(first.same_occurrence(&forwarded));
        assert_eq!(forwarded.context(), observed.context());
        assert!(
            forwarded
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>()
                .is_some()
        );
    }

    #[test]
    fn status_diagnostic_uses_facts_without_copying_arbitrary_provider_text() {
        use super::super::connector_failure::{ConnectorCallFailure, ConnectorOperation};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let secret = "private-provider-credential-marker";
        let source = ConnectorError::new(ConnectorErrorKind::PermissionDenied, secret);
        let failure: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::ReadPage, source.clone()).into();
        let failure = failure.at_operator(7, PipelineOperation::Pull);
        let safe = failure.safe_detail();
        assert!(!safe.as_str().contains(secret));
        assert!(safe.as_str().contains("reason 4:3"));
        assert!(safe.as_str().contains("operator 7"));
        let ExecutionFailureCause::Connector(call) = failure.cause() else {
            panic!("connector source must remain nominal");
        };
        assert_eq!(call.cause(), &source);
        assert_eq!(call.cause().message(), secret);
    }
    #[test]
    fn typed_failure_launch_cleanup_keeps_original_kernel_payload_and_source_chain() {
        use crate::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
        };
        let original = KernelFailure::Internal(KernelDiagnostic::new("original internal cause"));
        let error = FragmentLaunchError::from_failure(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            original.clone().into(),
        )
        .with_cleanup_diagnostics(vec![
            KernelFailure::Internal(KernelDiagnostic::new("secondary cleanup failure")).into(),
        ]);
        assert_eq!(
            error.cause().cause(),
            &ExecutionFailureCause::Kernel(original.clone())
        );
        assert_eq!(
            error.cleanup_diagnostics()[0].detail(),
            "secondary cleanup failure"
        );
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(
                &error.cleanup_diagnostics()[0]
            ),
            novarocks_failure_contract::FailureProjection::failure_fact(&KernelFailure::Internal(
                KernelDiagnostic::new("different diagnostic")
            ))
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>(),
            Some(&original)
        );
    }
}

impl novarocks_failure_contract::FailureProjection for RequiredExpressionRowError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.error.failure_fact().with_row_position(
            novarocks_failure_contract::FailureRowPosition {
                selected_ordinal: u64::try_from(self.error.selected_ordinal())
                    .expect("supported pointer widths fit u64"),
                batch_row: u64::try_from(self.batch_row).expect("supported pointer widths fit u64"),
            },
        )
    }
}
impl novarocks_failure_contract::FailureProjection for ExecutionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.failure_fact()
    }
}
impl novarocks_failure_contract::FailureProjection for ExecutionFailureCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            ExecutionFailureCause::Domain(cause) => cause.failure_fact(),
            ExecutionFailureCause::MemoryAccounting(cause) => cause.failure_fact(),
            ExecutionFailureCause::Connector(cause) => cause.failure_fact(),
            ExecutionFailureCause::Writer(cause) => cause.failure_fact(),
            ExecutionFailureCause::Construction(cause) => cause.failure_fact(),
            ExecutionFailureCause::Kernel(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeFilter(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeScalarSource(cause) => cause.failure_fact(),
            ExecutionFailureCause::RuntimeScalarMemory(cause) => cause.failure_fact(),
            ExecutionFailureCause::InvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::ScalarInvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::WindowInvocationData(cause) => cause.failure_fact(),
            ExecutionFailureCause::RequiredRow(cause) => cause.failure_fact(),
            ExecutionFailureCause::PreparationMetadata(cause) => cause.failure_fact(),
        }
    }
}

impl From<novarocks_local_program::StaticExpressionError> for ExecutionFailure {
    fn from(source: novarocks_local_program::StaticExpressionError) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_local_program::StaticSinkError> for ExecutionFailure {
    fn from(source: novarocks_local_program::StaticSinkError) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_types::value::variant::VariantFailure> for ExecutionFailure {
    fn from(source: novarocks_types::value::variant::VariantFailure) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_types::value::variant_encode::VariantEncodeFailure> for ExecutionFailure {
    fn from(source: novarocks_types::value::variant_encode::VariantEncodeFailure) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_type_contract::VariantTargetTypeError> for ExecutionFailure {
    fn from(source: novarocks_type_contract::VariantTargetTypeError) -> Self {
        Self::from_domain(source)
    }
}

impl From<crate::runtime::mem_tracker::MemTrackerLimitExceeded> for ExecutionFailure {
    fn from(source: crate::runtime::mem_tracker::MemTrackerLimitExceeded) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_functions::AggregateInputBatchError> for ExecutionFailure {
    fn from(source: novarocks_functions::AggregateInputBatchError) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_functions::largeint::LargeIntCarrierFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::largeint::LargeIntCarrierFailure) -> Self {
        Self::from_domain(cause)
    }
}

impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for ExecutionFailure {
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::from_domain(source)
    }
}

#[cfg(test)]
mod required_row_source_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn required_root_preserves_the_row_error_source_and_selection_address() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let row = RowDataError::with_reason(
            1,
            novarocks_functions::DataFailureReason::NumericOutOfRange,
            "out of range",
        );
        let expected = row.failure_fact();
        let required = RequiredExpressionRowError::try_new(
            ProgramExpressionRootSite::SinkPartition { branch: 0, key: 1 },
            selection,
            row,
        )
        .unwrap();
        assert_eq!(required.batch_row(), 4);
        let failure = ExecutionFailure::from(required);
        assert_eq!(
            failure.failure_fact(),
            expected.with_row_position(novarocks_failure_contract::FailureRowPosition {
                selected_ordinal: 1,
                batch_row: 4,
            })
        );
        let mut source: &(dyn Error + 'static) = &failure;
        loop {
            if let Some(row) = source.downcast_ref::<RowDataError>() {
                assert_eq!(row.selected_ordinal(), 1);
                assert_eq!(row.failure_fact(), expected);
                assert_eq!(row.message(), "out of range");
                break;
            }
            source = source
                .source()
                .expect("required root must keep its original row error");
        }
    }
}
