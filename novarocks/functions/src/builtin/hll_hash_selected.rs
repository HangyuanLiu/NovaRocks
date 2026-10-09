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

//! Selected projection over ONE original HLL_HASH reader and existing HLL encoder.
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
    let supported = matches!(
        source.data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(..)
            | DataType::Decimal128(..)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
    );
    let crate::FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    supported
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
            "hll_hash requires its original Native N1 scalar profile",
        ));
    }
    observe()
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn crate::AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut rows = EvaluationCheckpoints::new(control);
    let mut hash_work = EvaluationCheckpoints::new(control);
    let mut copy_work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || rows.step())?;
        let [argument] = input.arguments() else {
            return Err(invalid(
                "hll_hash requires its original single evaluated argument",
            ));
        };
        let array = argument.array();
        let FunctionArgumentType::Value(source) = &input.contract().selected().argument_types[0]
        else {
            unreachable!("checked value profile")
        };
        if array.data_type() != &source.data_type {
            return Err(internal(
                "hll_hash demanded carrier differs from its original full source type",
            ));
        }
        let reader = crate::hll_hash_core::Input::try_new(array).map_err(|_| {
            internal("hll_hash checked carrier violated its original reader contract")
        })?;
        let selection = input.selection();
        let count = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Layout::array::<i32>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut offsets = Vec::new();
        control.checkpoint(0)?;
        offsets
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        control.checkpoint(0)?;
        offsets.push(0);
        let max_bytes = selection
            .len()
            .checked_mul(10)
            .ok_or(KernelFailure::ResourceExhausted)?;
        i32::try_from(max_bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        Layout::array::<u8>(max_bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut bytes = Vec::new();
        control.checkpoint(0)?;
        bytes
            .try_reserve_exact(max_bytes)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        control.checkpoint(0)?;
        let charge = crate::opaque_memory::OpaqueRetainedCharge::try_new(Arc::clone(host))?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            rows.step()?;
            if row >= array.len() {
                return Err(internal(
                    "hll_hash demanded address exceeds its original carrier",
                ));
            }
            if !source.nullable && array.is_null(row) {
                return Err(internal(
                    "hll_hash nonnull source contains selected SQL NULL",
                ));
            }
            // Original singleton encoder requests exactly 10 bytes; EMPTY requests 1.
            // The real host grants the conservative 10-byte transient bound before
            // either original Vec allocation. Release follows actual payload Drop.
            control.checkpoint(0)?;
            let reservation = charge.reserve_operation(10)?;
            control.checkpoint(0)?;
            let payload = reader.row_observed(row, &mut || hash_work.step())?;
            control.checkpoint(0)?;
            for &byte in &payload {
                bytes.push(byte);
                copy_work.step()?;
            }
            drop(payload);
            drop(reservation);
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
        }
        hash_work.flush()?;
        copy_work.flush()?;
        let array = Arc::new(BinaryArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            None,
        )) as ArrayRef;
        control.checkpoint(0)?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &input.contract().result_type().data_type,
            array,
            Box::default(),
            || rows.step(),
        )
    })();
    // Real resource/control/invariant failure is first cause: no optional footer.
    if result.is_err() {
        return result;
    }
    hash_work.finish()?;
    copy_work.finish()?;
    rows.finish()?;
    result
}
