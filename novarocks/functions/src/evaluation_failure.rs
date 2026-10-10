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

//! Lossless whole-invocation data and separately typed kernel failures.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::{HostDiagnostic, HostShared};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateCallContract, AggregateKernelPhase, KernelFailure, Selection};
use allocator_api2::vec::Vec as HostVec;
use std::fmt;
use std::sync::Arc;

/// A transport identity, never an attribution to one input or output row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateInvocationPhase {
    Update,
    Merge,
    Intermediate,
    Final,
}
#[derive(Debug)]
struct AggregateDomain {
    phase: AggregateInvocationPhase,
    batch_rows: usize,
    emission_row_capacity: Option<usize>,
    rows: HostVec<usize, HostAggregateAllocator>,
    states: HostVec<usize, HostAggregateAllocator>,
}
#[derive(Debug)]
struct InvocationDataInner {
    contract: Arc<AggregateCallContract>,
    domain: AggregateDomain,
    diagnostic: HostDiagnostic,
}
#[derive(Clone, Debug)]
pub struct InvocationData(HostShared<InvocationDataInner>);
impl InvocationData {
    pub(crate) fn prepare_aggregate(
        allocator: &HostAggregateAllocator,
        contract: Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'_>,
        mapping: &[usize],
        diagnostic: HostDiagnostic,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        Self::prepare_domain(
            allocator, contract, phase, selection, mapping, None, diagnostic, work,
        )
    }
    fn prepare_domain(
        allocator: &HostAggregateAllocator,
        contract: Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'_>,
        mapping: &[usize],
        emission_row_capacity: Option<usize>,
        diagnostic: HostDiagnostic,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        if mapping.len() != selection.len() {
            return Err(crate::kernel_control::invalid(
                "invocation data mapping differs from selection",
            ));
        }
        let mut rows = HostVec::new_in(allocator.clone());
        let mut states = HostVec::new_in(allocator.clone());
        for output in [&mut rows, &mut states] {
            if !mapping.is_empty() {
                work.flush()?;
                output
                    .try_reserve_exact(mapping.len())
                    .map_err(|_| allocator.take_failure())?;
                work.flush()?;
            }
        }
        for (ordinal, state) in mapping.iter().copied().enumerate() {
            rows.push(selection.row(ordinal).ok_or_else(|| {
                crate::kernel_control::invalid("invocation data selection ordinal is absent")
            })?);
            states.push(state);
            work.step()?;
        }
        Ok(Self(HostShared::try_new(
            InvocationDataInner {
                contract,
                domain: AggregateDomain {
                    phase,
                    batch_rows: selection.batch_rows(),
                    emission_row_capacity,
                    rows,
                    states,
                },
                diagnostic,
            },
            allocator.clone(),
            work,
        )?))
    }
    pub(crate) fn prepare_emission(
        allocator: &HostAggregateAllocator,
        context: &crate::AggregateEmissionContext<'_>,
        diagnostic: HostDiagnostic,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        Self::prepare_domain(
            allocator,
            Arc::clone(context.contract()),
            context.phase(),
            context.selection(),
            context.state_indices(),
            Some(context.row_capacity()),
            diagnostic,
            work,
        )
    }
    pub fn message(&self) -> &str {
        self.0.diagnostic.message()
    }
    /// The same immutable prepared call author, not a name-derived identity.
    pub fn aggregate_contract(&self) -> &AggregateCallContract {
        &self.0.contract
    }
    pub fn aggregate_call_phase(&self) -> AggregateKernelPhase {
        self.0.contract.phase()
    }
    pub fn aggregate_phase(&self) -> AggregateInvocationPhase {
        self.0.domain.phase
    }
    pub fn batch_rows(&self) -> usize {
        self.0.domain.batch_rows
    }
    pub fn emission_row_capacity(&self) -> Option<usize> {
        self.0.domain.emission_row_capacity
    }
    pub fn input_rows(&self) -> &[usize] {
        &self.0.domain.rows
    }
    pub fn state_indices(&self) -> &[usize] {
        &self.0.domain.states
    }
    /// Count this shared backing once in its retaining owner, never once per loan.
    pub fn retained_bytes(&self) -> usize {
        self.0.domain.rows.allocator().metadata_bytes()
            + self.0.block_bytes()
            + self.0.diagnostic.retained_bytes()
            + self.0.domain.rows.capacity() * std::mem::size_of::<usize>()
            + self.0.domain.states.capacity() * std::mem::size_of::<usize>()
    }
    pub fn same_backing(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}
impl PartialEq for InvocationData {
    fn eq(&self, other: &Self) -> bool {
        self.same_backing(other)
            || (Arc::ptr_eq(&self.0.contract, &other.0.contract)
                && self.message() == other.message()
                && self.aggregate_call_phase() == other.aggregate_call_phase()
                && self.aggregate_phase() == other.aggregate_phase()
                && self.batch_rows() == other.batch_rows()
                && self.emission_row_capacity() == other.emission_row_capacity()
                && self.input_rows() == other.input_rows()
                && self.state_indices() == other.state_indices())
    }
}
impl Eq for InvocationData {}
impl fmt::Display for InvocationData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for InvocationData {}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvaluationFailure {
    Kernel(KernelFailure),
    InvocationData(InvocationData),
}
impl From<KernelFailure> for EvaluationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<InvocationData> for EvaluationFailure {
    fn from(cause: InvocationData) -> Self {
        Self::InvocationData(cause)
    }
}
impl fmt::Display for EvaluationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(f),
            Self::InvocationData(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for EvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Kernel(cause) => cause,
            Self::InvocationData(cause) => cause,
        })
    }
}
/// Data ends the current invocation immediately. Calling code must pass a
/// closure: evaluating a footer before calling this author would be too late.
pub(crate) fn finish_evaluation_lifecycle<T>(
    result: Result<T, EvaluationFailure>,
    post: impl FnOnce() -> Result<(), KernelFailure>,
) -> Result<T, EvaluationFailure> {
    match result {
        Err(data @ EvaluationFailure::InvocationData(_)) => Err(data),
        Ok(value) => post().map(|()| value).map_err(Into::into),
        Err(EvaluationFailure::Kernel(cause)) => {
            crate::aggregate_kernel::finish_lifecycle(Err(cause), post()).map_err(Into::into)
        }
    }
}
