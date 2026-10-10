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
//! Exact HLL payload lifecycle over the original shared HLL register/state author.
use super::aggregate_hll_core::{self as core, HLL_REGISTERS_COUNT, HllError, HllRawState, HllWork};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array};
use arrow_schema::DataType;
use std::fmt::Write;
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub(super) enum PayloadProjection {
    Bytes,
    Cardinality,
}
#[derive(Debug)]
pub(super) struct PayloadKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) projection: PayloadProjection,
}
pub(super) struct PayloadState {
    pub(super) core: HllRawState<HostAggregateAllocator>,
    pub(super) failed: bool,
    host: Arc<dyn AggregateStateAllocator>,
}
impl PayloadState {
    fn latch_failure(&mut self) {
        self.core.clear();
        self.failed = true;
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    // This owner preserves every originating failure before optional tail work.
    let result = if result.is_ok() {
        work.finish_result(result)
    } else {
        result
    };
    observation.finish(result)
}
pub(super) struct PayloadUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    diagnostic: Option<(core::HllMergeFailure, InvocationData)>,
}
pub(super) struct PayloadMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    diagnostic: Option<(core::HllMergeFailure, InvocationData)>,
}
fn kernel_hash_failure(cause: HllError) -> KernelFailure {
    match cause {
        HllError::Kernel(cause) => cause,
        HllError::Legacy(_) => {
            internal("HLL no-data operation produced an undeclared input failure")
        }
    }
}
fn observed_evaluation<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, EvaluationFailure>,
) -> Result<T, EvaluationFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    match result {
        Err(data @ EvaluationFailure::InvocationData(_)) => Err(data),
        Ok(value) => observation
            .finish(work.finish_result(Ok(value)))
            .map_err(Into::into),
        Err(EvaluationFailure::Kernel(cause)) => observation
            .finish(work.finish_result(Err(cause)))
            .map_err(Into::into),
    }
}
fn prepare_payload_diagnostic(
    contract: &Arc<AggregateCallContract>,
    phase: AggregateInvocationPhase,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    mapping: &[usize],
    allocator: Option<Arc<dyn AggregateStateAllocator>>,
    control: &dyn KernelEvaluationControl,
) -> Result<Option<(core::HllMergeFailure, InvocationData)>, EvaluationFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    if mapping.len() != selection.len() {
        return Err(invalid("HLL payload mapping differs from actual selected domain").into());
    }
    let stage = if phase == AggregateInvocationPhase::Update {
        crate::aggregate_format::AggregateFailureStage::Update
    } else {
        crate::aggregate_format::AggregateFailureStage::Merge
    };
    let reader = match core::HllPayloadInput::try_new(argument.array()) {
        Ok(reader) => reader,
        Err(recipe) => {
            // Unlike NDV, the original carrier match rejects NULL/empty unsupported input too.
            let host = HostAggregateAllocator::try_new(
                allocator
                    .ok_or_else(|| invalid("HLL payload Data requires actual aggregate host"))?,
            )?;
            let message = HostDiagnostic::prepare(&host, &mut work, |writer| {
                write!(writer, "{}", stage.message(&recipe))
            })?;
            let data = InvocationData::prepare_aggregate(
                &host,
                contract.clone(),
                phase,
                selection,
                mapping,
                message,
                &mut work,
            )?;
            return Err(data.into());
        }
    };
    let mut failure = None;
    for ordinal in 0..selection.len() {
        let row = selection
            .row(ordinal)
            .ok_or_else(|| invalid("HLL payload selected ordinal is absent"))?;
        let address = argument.value_row(ordinal, row);
        if address >= argument.array().len() {
            return Err(internal("HLL payload source address is out of bounds").into());
        }
        work.step()?;
        if let Some(bytes) = reader.row(address) {
            failure = core::hll_payload_recipe(bytes).err();
            if failure.is_some() {
                break;
            }
        }
    }
    work.flush()?;
    let Some(failure) = failure else {
        return Ok(None);
    };
    let host = HostAggregateAllocator::try_new(
        allocator.ok_or_else(|| invalid("HLL payload Data requires actual aggregate host"))?,
    )?;
    let message = HostDiagnostic::prepare(&host, &mut work, |writer| {
        write!(writer, "{}", stage.message(failure.message()))
    })?;
    let data = InvocationData::prepare_aggregate(
        &host,
        contract.clone(),
        phase,
        selection,
        mapping,
        message,
        &mut work,
    )?;
    work.finish()?;
    Ok(Some((failure, data)))
}
impl PreparedAggregateKernel for PayloadKernel {
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self {
            contract,
            projection: self.projection,
        }))
    }
    type State = PayloadState;
    type PreparedUpdateBatch<'a> = PayloadUpdate<'a>;
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_empty_update_preparation(&self) -> bool {
        // The original payload reader matches its carrier before NULL/row checks.
        true
    }
    type PreparedMergeBatch<'a> = PayloadMerge<'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.core.allocator.metadata_bytes()
            + if state.core.registers.is_some() {
                HLL_REGISTERS_COUNT
            } else {
                0
            }
    }
    fn prepared_update_retained_bytes(&self, prepared: &Self::PreparedUpdateBatch<'_>) -> usize {
        prepared
            .diagnostic
            .as_ref()
            .map_or(0, |(_, data)| data.retained_bytes())
    }
    fn prepared_merge_retained_bytes(&self, prepared: &Self::PreparedMergeBatch<'_>) -> usize {
        prepared
            .diagnostic
            .as_ref()
            .map_or(0, |(_, data)| data.retained_bytes())
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "allocation-tracked HLL payload requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let host = allocator.ok_or_else(|| {
                invalid("allocation-tracked HLL payload requires a host allocator")
            })?;
            work.step()?;
            if host.opaque_allocation_host().is_none() {
                return Err(invalid(
                    "HLL payload requires actual opaque output authority",
                ));
            }
            Ok(PayloadState {
                core: HllRawState::new(HostAggregateAllocator::try_new(host.clone())?),
                host,
                failed: false,
            })
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "HLL payload update differs from its exact phase or channels",
                ));
            }
            Ok(PayloadUpdate {
                input,
                diagnostic: None,
            })
        })
    }
    fn prepare_update_evaluation<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        mapping: &[usize],
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, EvaluationFailure> {
        let mut prepared = self.prepare_update(input, control)?;
        prepared.diagnostic = prepare_payload_diagnostic(
            &self.contract,
            AggregateInvocationPhase::Update,
            input.logical_arguments()[0],
            input.selection(),
            mapping,
            allocator,
            control,
        )?;
        Ok(prepared)
    }
    fn update_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedUpdateBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("HLL payload requires its invocation Data protocol"))
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
        let input = prepared.input;
        let result = observed_evaluation(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("HLL payload selected ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(
                    internal("HLL payload selected source address is out of bounds").into(),
                );
            }
            let reader = core::HllPayloadInput::try_new(argument.array())
                .map_err(|_| internal("HLL payload admitted update carrier changed"))?;
            core::merge_payload_rows(&reader, std::iter::once(address), |_, bytes| {
                state.core.has_value = true;
                core::merge_hll_bytes_with_failure(
                    &mut state.core,
                    bytes,
                    &mut HllWork::new(Some(work)),
                    |failure| match &prepared.diagnostic {
                        Some((expected, data)) if failure == *expected => {
                            EvaluationFailure::InvocationData(data.clone())
                        }
                        _ => internal("HLL payload violated its exact prepared failure recipe")
                            .into(),
                    },
                    |cause| EvaluationFailure::Kernel(kernel_hash_failure(cause)),
                )
            })
        });
        if result.is_err() {
            state.latch_failure()
        }
        result
    }
    fn prepare_merge<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != &DataType::Binary
            {
                return Err(invalid(
                    "HLL payload merge differs from its exact phase or Binary state",
                ));
            }
            Ok(PayloadMerge {
                input,
                diagnostic: None,
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
        let mut prepared = self.prepare_merge(input, control)?;
        prepared.diagnostic = prepare_payload_diagnostic(
            &self.contract,
            AggregateInvocationPhase::Merge,
            input.state(),
            input.selection(),
            mapping,
            allocator,
            control,
        )?;
        Ok(prepared)
    }
    fn merge_row<'a>(
        &self,
        _state: &mut Self::State,
        _input: &Self::PreparedMergeBatch<'a>,
        _ordinal: usize,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Err(invalid("HLL payload requires its invocation Data protocol"))
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
        let input = prepared.input;
        let result = observed_evaluation(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("HLL payload merge ordinal is out of bounds"))?;
            work.step()?;
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    invalid("HLL payload merge requires its exact Binary state carrier")
                })?;
            if address >= array.len() {
                return Err(internal("HLL payload merge address is out of bounds").into());
            }
            if !array.is_null(address) {
                state.core.has_value = true;
                core::merge_hll_bytes_with_failure(
                    &mut state.core,
                    array.value(address),
                    &mut HllWork::new(Some(work)),
                    |failure| match &prepared.diagnostic {
                        Some((expected, data)) if failure == *expected => {
                            EvaluationFailure::InvocationData(data.clone())
                        }
                        _ => internal(
                            "validated HLL payload violated its exact prepared failure recipe",
                        )
                        .into(),
                    },
                    |cause| EvaluationFailure::Kernel(kernel_hash_failure(cause)),
                )?;
            }
            Ok(())
        });
        if result.is_err() {
            state.latch_failure();
        }
        result
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        let mut states = states.peekable();
        if states.peek().is_some_and(|state| state.failed) {
            return Err(KernelFailure::InstanceFailed);
        }
        observed(control, |work| {
            let expected = states.len();
            work.flush()?;
            let mut builder = BinaryBuilder::new();
            work.flush()?;
            let mut count = 0;
            for state in states {
                if count == expected {
                    return Err(internal("HLL payload state iterator exceeded its extent"));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step()?;
                if !state.core.has_value {
                    builder.append_null();
                    count += 1;
                    continue;
                }
                // Original serialization has one Vec: empty 1, sparse <= 12293,
                // or dense 16385 bytes. No second register walk supplies this bound.
                let charge =
                    crate::opaque_memory::OpaqueRetainedCharge::try_new(state.host.clone())?;
                let bound = if state.core.registers.is_some() {
                    1 + HLL_REGISTERS_COUNT
                } else {
                    1
                };
                work.flush()?;
                let reservation = charge.reserve_operation(bound)?;
                let payload = core::serialize_hll_state(&state.core, &mut HllWork::new(Some(work)))
                    .map_err(kernel_hash_failure)?;
                match payload {
                    Some(payload) => {
                        work.flush()?;
                        builder.append_value(&payload);
                        drop(payload);
                        work.flush()?;
                    }
                    None => builder.append_null(),
                }
                // The original temporary is destroyed before its genuine host grant.
                drop(reservation);
                drop(charge);
                count += 1;
            }
            if count != expected {
                return Err(internal("HLL payload state iterator shortened its extent"));
            }
            work.flush()?;
            let result = Arc::new(builder.finish()) as ArrayRef;
            work.flush()?;
            Ok(result)
        })
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 's,
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        match self.projection {
            PayloadProjection::Bytes => self.build_intermediate(states, control),
            PayloadProjection::Cardinality => {
                let mut states = states.peekable();
                if states.peek().is_some_and(|state| state.failed) {
                    return Err(KernelFailure::InstanceFailed);
                }
                observed(control, |work| {
                    let expected = states.len();
                    work.flush()?;
                    let mut builder = arrow_array::builder::Int64Builder::new();
                    work.flush()?;
                    let mut count = 0;
                    for state in states {
                        if count == expected {
                            return Err(internal("HLL payload state iterator exceeded its extent"));
                        }
                        if state.failed {
                            return Err(KernelFailure::InstanceFailed);
                        }
                        work.step()?;
                        match core::nullable_payload_cardinality(
                            Some(&state.core),
                            &mut HllWork::new(Some(work)),
                        )
                        .map_err(kernel_hash_failure)?
                        {
                            Some(value) => builder.append_value(value),
                            None => builder.append_null(),
                        }
                        count += 1;
                    }
                    if count != expected {
                        return Err(internal("HLL payload state iterator shortened its extent"));
                    }
                    work.flush()?;
                    let result = Arc::new(builder.finish()) as ArrayRef;
                    work.flush()?;
                    Ok(result)
                })
            }
        }
    }
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    projection: PayloadProjection,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step().map_err(compile_failure)?;
    let [FunctionArgumentType::Value(_source)] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid("HLL payload requires exactly one value argument"));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("HLL payload requires a scalar result"));
    };
    // Generic binding is preserved: unsupported raw carriers fail in the original
    // selected hash reader only when actually demanded, rather than narrowing admission.
    for (actual, expected) in [
        (
            output,
            FunctionValueType::new(
                match projection {
                    PayloadProjection::Bytes => DataType::Binary,
                    PayloadProjection::Cardinality => DataType::Int64,
                },
                true,
            ),
        ),
        (
            contract.intermediate_type(),
            FunctionValueType::new(DataType::Binary, true),
        ),
    ] {
        if !actual.exactly_equals_observed::<KernelFailure>(&expected, || {
            work.step().map_err(compile_failure)
        })? {
            return Err(invalid(
                "HLL payload requires its exact nullable Int64 result and Binary state",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_hll_payload_tests.rs"]
mod tests;
