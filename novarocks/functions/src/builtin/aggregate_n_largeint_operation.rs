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
//! Lossless N operation errors under one actual same-host prefix.
use super::aggregate_n_largeint_resources::operation_prefix_bytes;
use crate::aggregate_format::AggregateFailureStage;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::ScalarWork;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateCallContract, AggregateEmissionContext, AggregateInvocationPhase, EvaluationFailure,
    InvocationData, KernelEvaluationControl, KernelFailure, Selection,
};
use std::sync::Arc;

pub(super) enum NDataDomain<'a> {
    Mutation {
        contract: &'a Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'a>,
        mapping: &'a [usize],
    },
    Emission(&'a AggregateEmissionContext<'a>),
}
impl NDataDomain<'_> {
    fn stage(&self) -> Option<AggregateFailureStage> {
        match self {
            Self::Mutation {
                phase: AggregateInvocationPhase::Update,
                ..
            } => Some(AggregateFailureStage::Update),
            Self::Mutation {
                phase: AggregateInvocationPhase::Merge,
                ..
            } => Some(AggregateFailureStage::Merge),
            Self::Emission(context) if context.phase() == AggregateInvocationPhase::Final => {
                Some(AggregateFailureStage::BuildFinal)
            }
            Self::Emission(context)
                if context.phase() == AggregateInvocationPhase::Intermediate =>
            {
                Some(AggregateFailureStage::BuildIntermediate)
            }
            _ => None,
        }
    }
    fn diagnostic(
        &self,
        message: &(impl std::fmt::Display + ?Sized),
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<InvocationData, KernelFailure> {
        let stage = self
            .stage()
            .ok_or_else(|| invalid("min_n/max_n core call has an unsupported invocation domain"))?;
        let diagnostic = HostDiagnostic::prepare(allocator, work, |writer| {
            write!(writer, "{}", stage.message(message))
        })?;
        match self {
            Self::Mutation {
                contract,
                phase,
                selection,
                mapping,
            } => InvocationData::prepare_aggregate(
                allocator,
                Arc::clone(contract),
                *phase,
                *selection,
                mapping,
                diagnostic,
                work,
            ),
            Self::Emission(context) => {
                InvocationData::prepare_emission(allocator, context, diagnostic, work)
            }
        }
    }
}
pub(super) fn project_failure(
    cause: KernelFailure,
    domain: &NDataDomain<'_>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> EvaluationFailure {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    // The producer already decided the kind. This host only attaches the
    // actual whole-invocation scope, never a maskable row attribution.
    if cause.failure_fact().class() != FailureClass::Data {
        return cause.into();
    }
    match domain.diagnostic(&cause, allocator, work) {
        Ok(data) => data.with_whole_value_source(cause).into(),
        Err(diagnostic) => KernelFailure::from_domain(NDiagnosticFailure {
            origin: cause,
            diagnostic,
            stage: domain.stage(),
        })
        .into(),
    }
}
/// Successful owned output cannot escape this temporary prefix. Output has its
/// own whole-emission reservation and immutable backing custody path.
pub(super) fn call_unit(
    charge: &OpaqueRetainedCharge,
    allocator: &HostAggregateAllocator,
    domain: NDataDomain<'_>,
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut ScalarWork<'_, '_>) -> Result<(), KernelFailure>,
) -> Result<(), EvaluationFailure> {
    let prefix = charge.reserve_operation(operation_prefix_bytes()?)?;
    if !prefix.belongs_to_allocator(allocator) {
        return Err(invalid("min_n/max_n call prefix uses another host authority").into());
    }
    let mut checkpoints = EvaluationCheckpoints::new(control);
    let result = {
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        operation(&mut work)
    };
    match result {
        Ok(()) => {
            checkpoints.finish()?;
            Ok(())
        }
        Err(error) => {
            let failure = project_failure(error, &domain, allocator, &mut checkpoints);
            drop(checkpoints);
            Err(failure)
        }
    }
}

/// A failed diagnostic is secondary to the already-known computation origin.
/// Keep both actual sources locally, without retaining the invocation graph.
#[derive(Debug)]
pub(super) struct NDiagnosticFailure {
    pub(super) origin: KernelFailure,
    pub(super) diagnostic: KernelFailure,
    stage: Option<AggregateFailureStage>,
}
impl std::fmt::Display for NDiagnosticFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.stage {
            Some(stage) => stage.message(&self.origin).fmt(f),
            None => self.origin.fmt(f),
        }
    }
}
impl std::error::Error for NDiagnosticFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.origin)
    }
}
impl novarocks_failure_contract::FailureProjection for NDiagnosticFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.origin)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for NDiagnosticFailure {}
