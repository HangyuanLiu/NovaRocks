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

//! Selected host consumer of the one original exact-percentile computation.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{self as scalar, ScalarStateAllocator, ScalarStateError, ScalarWork};
use crate::exact_percentile_core::{self as core, ExactPercentileAllocator};
use crate::exact_percentile_failure::{PercentileDataRecipe, PercentileFailureSink};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use allocator_api2::{
    alloc::{AllocError, Allocator},
    vec::Vec as HostVec,
};
use arrow_array::{ArrayRef, builder::BinaryBuilder};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use std::{alloc::Layout, ptr::NonNull, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub(super) enum PercentileOperation {
    Continuous,
    Discrete,
}
#[derive(Debug)]
pub(super) struct PercentileKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) operation: PercentileOperation,
}
#[derive(Clone, Debug)]
pub(super) struct PercentileAllocator(HostAggregateAllocator);
// SAFETY: this wrapper forwards the same exact blocks and Layouts to its host.
unsafe impl Allocator for PercentileAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.allocate(layout)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        // SAFETY: the original allocator produced this block and Layout.
        unsafe { self.0.deallocate(pointer, layout) };
    }
}
impl ScalarStateAllocator for PercentileAllocator {
    fn scalar_allocation_error(&self, _operation: &str) -> ScalarStateError {
        ScalarStateError::Kernel(
            self.0
                .recorded_failure()
                .unwrap_or(KernelFailure::ResourceExhausted),
        )
    }
}
impl ExactPercentileAllocator for PercentileAllocator {
    type ParserReservation = HostVec<u8, Self>;
    fn reserve_percentile_transient(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<Self::ParserReservation, String> {
        self.reserve_percentile_transient_lossless(bytes, operation)
            .map_err(|error| error.to_string())
    }
    fn reserve_percentile_transient_lossless(
        &self,
        bytes: usize,
        _operation: &str,
    ) -> Result<Self::ParserReservation, ScalarStateError> {
        let mut reservation = HostVec::new_in(self.clone());
        reservation
            .try_reserve_exact(bytes)
            .map_err(|_| self.scalar_allocation_error("reserve percentile scratch"))?;
        Ok(reservation)
    }
    fn percentile_allocation_failure(&self, operation: &str) -> ScalarStateError {
        self.scalar_allocation_error(operation)
    }
    fn percentile_allocation_error(&self, _operation: &str) -> String {
        // serde needs its error carrier, while the real typed refusal stays in
        // the same host journal until the synchronous JSON failure continuation.
        "exact percentile host allocation refused".to_string()
    }
}
pub(super) struct PercentileState {
    core: core::ExactPercentileState<PercentileAllocator>,
    heap_bytes: usize,
    failed: bool,
}
impl PercentileState {
    fn latch(&mut self) {
        self.failed = true;
        self.core.rate = None;
        self.core.values = HostVec::new_in(self.core.allocator.clone());
        self.heap_bytes = 0;
    }
    fn account_new_values(
        &mut self,
        from: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        for value in &self.core.values[from..] {
            self.heap_bytes = self
                .heap_bytes
                .checked_add(
                    scalar::tracked_scalar_heap_capacity(value, work)
                        .map_err(ScalarStateError::into_kernel_failure)?,
                )
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        Ok(())
    }
}
pub(super) struct PercentileUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
}
pub(super) struct PercentileMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    mapping: HostVec<usize, HostAggregateAllocator>,
    allocator: HostAggregateAllocator,
}
enum FailureDomain<'a, 'host> {
    Mutation {
        contract: &'a Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'a>,
        mapping: &'a [usize],
    },
    Emission(&'a AggregateEmissionContext<'host>),
}
struct FailureSink<'a, 'host> {
    allocator: &'a HostAggregateAllocator,
    domain: FailureDomain<'a, 'host>,
    control: &'a dyn KernelEvaluationControl,
}
impl FailureSink<'_, '_> {
    fn diagnostic<T: std::fmt::Display + ?Sized>(&mut self, original: &T) -> EvaluationFailure {
        use crate::aggregate_format::AggregateFailureStage;
        let stage = match &self.domain {
            FailureDomain::Mutation {
                phase: AggregateInvocationPhase::Update,
                ..
            } => AggregateFailureStage::Update,
            FailureDomain::Mutation {
                phase: AggregateInvocationPhase::Merge,
                ..
            } => AggregateFailureStage::Merge,
            FailureDomain::Emission(context) => match context.phase() {
                AggregateInvocationPhase::Intermediate => AggregateFailureStage::BuildIntermediate,
                AggregateInvocationPhase::Final => AggregateFailureStage::BuildFinal,
                _ => return invalid("percentile emission diagnostic has a foreign phase").into(),
            },
            _ => return invalid("percentile mutation diagnostic has a foreign phase").into(),
        };
        // Recipe detection is not a published Data capsule. The following is
        // real host-backed original batch diagnostic construction work.
        let mut work = EvaluationCheckpoints::new(self.control);
        let result = (|| {
            let text = HostDiagnostic::prepare(self.allocator, &mut work, |writer| {
                write!(writer, "{}", stage.message(original))
            })?;
            match &self.domain {
                FailureDomain::Mutation {
                    contract,
                    phase,
                    selection,
                    mapping,
                } => InvocationData::prepare_aggregate(
                    self.allocator,
                    Arc::clone(contract),
                    *phase,
                    *selection,
                    mapping,
                    text,
                    &mut work,
                ),
                FailureDomain::Emission(context) => {
                    InvocationData::prepare_emission(self.allocator, context, text, &mut work)
                }
            }
        })();
        match result {
            Ok(data) => EvaluationFailure::InvocationData(data),
            Err(cause) => cause.into(),
        }
    }
}
impl PercentileFailureSink<PercentileAllocator> for FailureSink<'_, '_> {
    type Error = EvaluationFailure;
    fn data(&mut self, recipe: PercentileDataRecipe<'_, PercentileAllocator>) -> Self::Error {
        self.diagnostic(&recipe)
    }
    fn scalar(&mut self, error: ScalarStateError) -> Self::Error {
        match error {
            ScalarStateError::Kernel(cause) => cause.into(),
            ScalarStateError::OutputAllocation(_) => KernelFailure::ResourceExhausted.into(),
            ScalarStateError::Legacy(message) => self.reader(message),
        }
    }
    fn reader(&mut self, error: String) -> Self::Error {
        self.diagnostic(&error)
    }
    fn json(&mut self, error: &serde_json::Error, allocator: &PercentileAllocator) -> Self::Error {
        match allocator.0.take_recorded_failure() {
            Some(cause) => cause.into(),
            None => self.data(PercentileDataRecipe::Json(error)),
        }
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = operation(&mut work);
    // Whole Data and originating Kernel failures have no optional footer.
    if result.is_ok() {
        work.finish()?;
    }
    result
}
fn mapped_domain(
    mapping: &[usize],
    selection: Selection<'_>,
    host: Option<Arc<dyn AggregateStateAllocator>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<
    (
        HostAggregateAllocator,
        HostVec<usize, HostAggregateAllocator>,
    ),
    KernelFailure,
> {
    if mapping.len() != selection.len() {
        return Err(invalid("percentile mapping differs from actual selection"));
    }
    let allocator = HostAggregateAllocator::try_new(
        host.ok_or_else(|| invalid("exact percentile requires its host allocator"))?,
    )?;
    let mut owned = HostVec::new_in(allocator.clone());
    if !mapping.is_empty() {
        work.flush()?;
        owned
            .try_reserve_exact(mapping.len())
            .map_err(|_| allocator.take_failure())?;
        work.flush()?;
    }
    for index in mapping {
        owned.push(*index);
        work.step()?;
    }
    Ok((allocator, owned))
}
impl PercentileKernel {
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated scalar result")
        };
        &output.data_type
    }
    fn check_emit(
        &self,
        context: &AggregateEmissionContext<'_>,
        count: usize,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(context.contract(), &self.contract)
            || context.state_indices().len() != count
        {
            return Err(invalid(
                "percentile emission differs from its real prepared contract or state order",
            ));
        }
        Ok(())
    }
}
impl PreparedAggregateKernel for PercentileKernel {
    type State = PercentileState;
    type PreparedUpdateBatch<'a> = PercentileUpdate<'a>;
    type PreparedMergeBatch<'a> = PercentileMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.core.allocator.0.metadata_bytes()
            + state.core.values.capacity()
                * std::mem::size_of::<scalar::TrackedAggScalarValue<PercentileAllocator>>()
            + state.heap_bytes
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("exact percentile requires its host allocator"))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        let host = HostAggregateAllocator::try_new(
            allocator.ok_or_else(|| invalid("exact percentile requires its host allocator"))?,
        )?;
        Ok(PercentileState {
            core: core::ExactPercentileState::new(PercentileAllocator(host)),
            heap_bytes: 0,
            failed: false,
        })
    }
    fn prepare_update<'a>(
        &'a self,
        _input: SelectedAggregateUpdateInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        Err(invalid(
            "exact percentile requires the lossless aggregate update port",
        ))
    }
    fn update_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedUpdateBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "exact percentile requires the lossless aggregate update port",
        ))
    }
    fn prepare_merge<'a>(
        &'a self,
        _input: SelectedAggregateMergeInput<'a, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        Err(invalid(
            "exact percentile requires the lossless aggregate merge port",
        ))
    }
    fn merge_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedMergeBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid(
            "exact percentile requires the lossless aggregate merge port",
        ))
    }
    fn build_intermediate<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "exact percentile requires its real emission context",
        ))
    }
    fn build_final<'s, I>(
        &self,
        _states: I,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        Err(invalid(
            "exact percentile requires its real emission context",
        ))
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 2
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "percentile update differs from its checked phase or channels",
                )
                .into());
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(PercentileUpdate {
                input,
                mapping,
                allocator,
            })
        })
    }
    fn prepare_merge_evaluation<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, EvaluationFailure> {
        observed(control, |work| {
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(invalid(
                    "percentile merge differs from its checked phase or state carrier",
                )
                .into());
            }
            work.step()?;
            let (allocator, mapping) = mapped_domain(mapping, input.selection(), allocator, work)?;
            Ok(PercentileMerge {
                input,
                mapping,
                allocator,
            })
        })
    }
    fn update_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let input = &prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("percentile selected ordinal is absent"))?;
            work.step()?;
            let values = input.logical_arguments()[0];
            let rates = input.logical_arguments()[1];
            let vr = values.value_row(ordinal, row);
            let rr = rates.value_row(ordinal, row);
            if vr >= values.array().len() || rr >= rates.array().len() {
                return Err(internal("percentile selected address is out of bounds").into());
            }
            let mut sink = FailureSink {
                allocator: &prepared.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Update,
                    selection: input.selection(),
                    mapping: &prepared.mapping,
                },
                control,
            };
            let from = state.core.values.len();
            let mut scalar_work = ScalarWork::new(Some(work));
            core::update_from_arrays_with_sink(
                &mut state.core,
                values.array(),
                vr,
                rates.array(),
                rr,
                &mut scalar_work,
                &mut sink,
            )?;
            state.account_new_values(from, &mut scalar_work)?;
            Ok(())
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn merge_row_evaluation<'a>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), EvaluationFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = observed(control, |work| {
            let input = &prepared.input;
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("percentile selected merge ordinal is absent"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("percentile selected merge address is out of bounds").into());
            }
            let mut sink = FailureSink {
                allocator: &prepared.allocator,
                domain: FailureDomain::Mutation {
                    contract: &self.contract,
                    phase: AggregateInvocationPhase::Merge,
                    selection: input.selection(),
                    mapping: &prepared.mapping,
                },
                control,
            };
            let from = state.core.values.len();
            let mut scalar_work = ScalarWork::new(Some(work));
            core::merge_from_array_with_sink(
                &mut state.core,
                argument.array(),
                address,
                core::ExactMergeDiagnostic::Merge,
                &mut scalar_work,
                &mut sink,
            )?;
            state.account_new_values(from, &mut scalar_work)?;
            Ok(())
        });
        if result.is_err() {
            state.latch();
        }
        result
    }
    fn build_intermediate_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            let mut builder = BinaryBuilder::new();
            let mut count = 0;
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if count >= expected {
                    return Err(
                        internal("percentile state iterator exceeded its actual extent").into(),
                    );
                }
                work.step()?;
                // The original serde encoder and builder are opaque operations.
                work.flush()?;
                let encoded = core::encode_state(&state.core);
                work.flush()?;
                builder.append_value(encoded);
                work.flush()?;
                count += 1;
            }
            if count != expected {
                return Err(
                    internal("percentile state iterator shortened its actual extent").into(),
                );
            }
            let array = Arc::new(builder.finish()) as ArrayRef;
            work.flush()?;
            Ok(array)
        })
    }
    fn build_final_evaluation_with_context<'s, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            let expected = states.len();
            self.check_emit(context, expected)?;
            let host =
                HostAggregateAllocator::try_new(Arc::clone(context.allocator().ok_or_else(
                    || invalid("percentile final requires its actual host allocator"),
                )?))?;
            let mut sink = FailureSink {
                allocator: &host,
                domain: FailureDomain::Emission(context),
                control,
            };
            let mut output = Vec::new();
            output
                .try_reserve_exact(expected)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut scalar_work = ScalarWork::new(Some(work));
            for state in states {
                if state.failed {
                    return Err(KernelFailure::InstanceFailed.into());
                }
                if output.len() >= expected {
                    return Err(
                        internal("percentile final iterator exceeded its actual extent").into(),
                    );
                }
                scalar_work.step().map_err(|error| sink.scalar(error))?;
                output.push(match self.operation {
                    PercentileOperation::Continuous => core::finalize_cont_with_sink(
                        &state.core,
                        self.output_type(),
                        &mut scalar_work,
                        &mut sink,
                    ),
                    PercentileOperation::Discrete => {
                        core::finalize_disc_with_sink(&state.core, &mut scalar_work, &mut sink)
                    }
                }?);
            }
            if output.len() != expected {
                return Err(
                    internal("percentile final iterator shortened its actual extent").into(),
                );
            }
            scalar::build_scalar_array(self.output_type(), output, &mut scalar_work)
                .map_err(|error| sink.scalar(error))
        })
    }
}
pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(value),
        FunctionArgumentType::Value(_rate),
    ] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "percentile requires the original two full value arguments",
        ));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("percentile requires its original scalar result"));
    };
    let mut expected = value.clone();
    expected.nullable = true;
    if !expected
        .exactly_equals_observed::<KernelFailure>(output, || work.step().map_err(compile_failure))?
        || !FunctionValueType::new(DataType::Binary, true)
            .exactly_equals_observed::<KernelFailure>(contract.intermediate_type(), || {
                work.step().map_err(compile_failure)
            })?
    {
        return Err(invalid(
            "percentile selected output, rate or state differs from its original full signature",
        ));
    }
    // ANY is retained in full. Unsupported non-NULL values and interpolation
    // outputs fail only at their actual original computation frontier.
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_percentile_tests.rs"]
mod tests;
