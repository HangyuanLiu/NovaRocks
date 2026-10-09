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
//! Exact selected adapter for the original Native single-argument call; no expression lookup.
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, ValueLogicalType};
use std::{alloc::Layout, sync::Arc};
/// Static admission from the original numeric reader and Native arity author.
/// This never reads payloads, resolves a name, prepares state, or evaluates data.
pub(super) fn original_native_profile(
    selected: &crate::FunctionBindingSelection,
    logical_argument_count: usize,
) -> bool {
    if logical_argument_count != 1
        || selected.argument_types.len() != 1
        || selected.aggregate.is_some()
    {
        return false;
    }
    let FunctionArgumentType::Value(source) = &selected.argument_types[0] else {
        return false;
    };
    // The original reader interprets FSB16 through the existing LARGEINT raw
    // author independently of nominal Physical/LargeInt/Uuid tags. Exact FVT
    // validity and canonical selection remain the original resolver's work.
    let numeric = matches!(
        source.data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::FixedSizeBinary(16)
    );
    let crate::FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    numeric
        && target.logical_type == ValueLogicalType::Physical
        && target.data_type == DataType::Binary
        && target.nullable
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    if contract.effects().argument_control != ArgumentControl::Eager
        || contract.value_argument_types().len() != 1
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(invalid(
            "percentile_hash requires its original Native N1 numeric profile",
        ));
    }
    observe()
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    host: &Arc<dyn crate::AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    // Row traversal is a separate real scope from opaque library/allocation
    // boundaries. Those boundaries must not reset its actual 256-row quantum.
    let mut rows = EvaluationCheckpoints::new(control);
    let mut bytes_work = EvaluationCheckpoints::new(control);
    let mut observer_refused = false;
    let mut encoder_refused = false;
    let mut observed = |result: Result<(), KernelFailure>| {
        if result.is_err() {
            observer_refused = true;
        }
        result
    };
    let result = (|| {
        validate_profile(input.contract(), || observed(rows.step()))?;
        let [argument] = input.arguments() else {
            return Err(invalid(
                "percentile_hash requires its original single evaluated value",
            ));
        };
        let array = argument.array();
        let FunctionArgumentType::Value(source) = &input.contract().selected().argument_types[0]
        else {
            unreachable!("profile checks every value channel")
        };
        if array.data_type() != &source.data_type {
            return Err(internal(
                "percentile_hash demanded carrier differs from its original full source type",
            ));
        }
        let selection = input.selection();
        let count = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::array::<i32>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut offsets = Vec::new();
        observed(control.checkpoint(0))?;
        offsets
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        observed(control.checkpoint(0))?;
        offsets.push(0i32);
        let mut bytes = Vec::new();
        let charge = crate::opaque_memory::OpaqueRetainedCharge::try_new(Arc::clone(host))?;
        for (ordinal, _batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, _batch_row);
            observed(rows.step())?;
            if row >= array.len() {
                return Err(internal(
                    "percentile_hash demanded address exceeds its original carrier",
                ));
            }
            if !source.nullable && array.is_null(row) {
                return Err(internal(
                    "percentile_hash nonnull source contains selected SQL NULL",
                ));
            }
            let mut encoder = Encoder {
                allocator,
                charge: &charge,
                reservation: None,
            };
            let payload = match crate::percentile_hash_core::row_with_encoder_observed(
                array,
                row,
                |event| match event {
                    crate::percentile_hash_core::Observation::Step => observed(rows.step()),
                    crate::percentile_hash_core::Observation::ReadBoundary
                    | crate::percentile_hash_core::Observation::EncodeBoundary => {
                        observed(control.checkpoint(0))
                    }
                },
                &mut encoder,
            ) {
                Ok(payload) => payload,
                Err(cause) => {
                    encoder_refused = true;
                    return Err(cause);
                }
            };
            // Original unsupported type/downcast messages remain raw full Strings.
            // They cannot arise from the original resolver's checked numeric profile
            // and canonical carrier. This is not a RowData/wholeData conversion for a
            // valid profile, nor an admission change for raw legacy callers.
            let payload = payload.map_err(|_| {
                internal(
                    "percentile_hash checked numeric carrier violated its original reader contract",
                )
            })?;
            let extent = bytes
                .len()
                .checked_add(payload.len())
                .ok_or(KernelFailure::ResourceExhausted)?;
            i32::try_from(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
            Layout::array::<u8>(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
            observed(bytes_work.flush())?;
            bytes
                .try_reserve_exact(payload.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            observed(control.checkpoint(0))?;
            for byte in payload {
                bytes.push(byte);
                observed(bytes_work.step())?;
            }
            drop(encoder);
            offsets.push(extent as i32);
            observed(rows.step())?;
        }
        observed(bytes_work.flush())?;
        let array = Arc::new(BinaryArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            None,
        )) as ArrayRef;
        observed(control.checkpoint(0))?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &input.contract().result_type().data_type,
            array,
            Box::default(),
            || observed(rows.step()),
        )
    })();
    if observer_refused
        || encoder_refused
        || matches!(
            &result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        )
    {
        return result;
    }
    bytes_work.finish()?;
    rows.finish()?;
    result
}

struct Encoder<'a> {
    allocator: &'a crate::aggregate_host_allocator::HostAggregateAllocator,
    charge: &'a crate::opaque_memory::OpaqueRetainedCharge,
    reservation: Option<crate::opaque_memory::OpaqueReservation>,
}
impl crate::percentile_hash_core::ValueEncoder for Encoder<'_> {
    type Error = KernelFailure;
    fn single(&mut self, value: f64) -> Result<Vec<u8>, KernelFailure> {
        let state =
            crate::approx_percentile_core::single_value_state_in(value, self.allocator.clone())
                .map_err(|_| {
                    self.allocator.take_recorded_failure().unwrap_or_else(|| {
                        internal(
                            "original percentile singleton violated its bounded state invariant",
                        )
                    })
                })?;
        let extent =
            crate::approx_percentile_core::scalar_hash_codec_extent(&state).ok_or_else(|| {
                internal("original percentile singleton differs from its codec reservation recipe")
            })?;
        self.reservation = Some(self.charge.reserve_operation(extent)?);
        Ok(crate::approx_percentile_core::encode_state(&state))
    }
    fn empty(&mut self) -> Result<Vec<u8>, KernelFailure> {
        let state = crate::approx_percentile_core::PercentileState::new_in(
            crate::approx_percentile_core::DEFAULT_COMPRESSION_FACTOR,
            self.allocator.clone(),
        );
        let extent =
            crate::approx_percentile_core::scalar_hash_codec_extent(&state).ok_or_else(|| {
                internal("original empty percentile differs from its codec reservation recipe")
            })?;
        self.reservation = Some(self.charge.reserve_operation(extent)?);
        Ok(crate::approx_percentile_core::encode_state(&state))
    }
}
