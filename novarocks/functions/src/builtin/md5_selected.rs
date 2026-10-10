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
//! Exact selected address projection delegates to the original ONE MD5 core.
use super::md5_shared::{self, CoreError, Operation};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::UInt64Array;
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::alloc::Layout;
/// Flat selected cast profiles only. Arbitrary raw display casts are not guessed.
fn source_supported(op: Operation, data_type: &DataType, logical: &ValueLogicalType) -> bool {
    if op == Operation::Md5 {
        return *logical == ValueLogicalType::Physical && *data_type == DataType::Utf8;
    }
    if matches!(
        data_type,
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary
    ) {
        return true;
    }
    *logical == ValueLogicalType::Physical
        && matches!(
            data_type,
            DataType::Null
                | DataType::Utf8View
                | DataType::BinaryView
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
        && arrow_cast::can_cast_types(data_type, &DataType::Utf8)
}
pub(super) fn validate_profile(
    op: Operation,
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    if types.is_empty() || (op == Operation::Md5 && types.len() != 1) {
        return Err(invalid(
            "MD5 family requires its exact nonempty checked arity",
        ));
    }
    for ty in types.iter() {
        step()?;
        let FunctionArgumentType::Value(source) = ty else {
            return Err(invalid("MD5 family requires value arguments"));
        };
        if !source_supported(op, &source.data_type, &source.logical_type) {
            return Err(invalid(
                "MD5 family source has no installed exact flat byte or VARCHAR cast profile",
            ));
        }
    }
    let target = contract.result_type();
    step()?;
    let expected = if op == Operation::Md5sumNumeric {
        target.data_type == DataType::FixedSizeBinary(16)
            && target.logical_type == ValueLogicalType::LargeInt
    } else {
        target.data_type == DataType::Utf8 && target.logical_type == ValueLogicalType::Physical
    };
    if !expected || !target.nullable {
        return Err(invalid(
            "MD5 family differs from its exact nullable installed result identity",
        ));
    }
    Ok(())
}
fn vector<T>(len: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
pub(super) fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?
    .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
pub(super) fn evaluate<'a>(
    op: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(op, input.contract(), || work.step())?;
        if input.arguments().len() != input.contract().selected().argument_types.len() {
            return Err(invalid(
                "MD5 family argument count differs from checked types",
            ));
        }
        let selection = input.selection();
        if op == Operation::Md5sumNumeric {
            Layout::array::<u8>(
                selection
                    .len()
                    .checked_mul(16)
                    .ok_or(KernelFailure::ResourceExhausted)?,
            )
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        } else {
            output_capacity(
                selection.len(),
                selection
                    .len()
                    .checked_mul(32)
                    .ok_or(KernelFailure::ResourceExhausted)?,
            )?;
        }
        let mut arrays = vector(input.arguments().len(), &mut work)?;
        let mut compact = vector(input.arguments().len(), &mut work)?;
        for (idx, arg) in input.arguments().iter().enumerate() {
            work.step()?;
            let mut array = arg.array().clone();
            let must_project = !matches!(
                array.data_type(),
                DataType::Utf8 | DataType::Binary | DataType::Null
            );
            for (ordinal, row) in selection.iter().enumerate() {
                work.step()?;
                if arg.value_row(ordinal, row) >= array.len() {
                    return Err(internal("MD5 family selected address is out of bounds"));
                }
            }
            if must_project {
                let mut indices = vector(selection.len(), &mut work)?;
                for (ordinal, row) in selection.iter().enumerate() {
                    work.step()?;
                    indices.push(
                        u64::try_from(arg.value_row(ordinal, row))
                            .map_err(|_| KernelFailure::ResourceExhausted)?,
                    );
                }
                work.flush()?;
                let indices = UInt64Array::from(indices);
                work.flush()?;
                array =
                    arrow_select::take::take(array.as_ref(), &indices, None).map_err(|error| {
                        internal(&format!("MD5 family selected projection failed: {error}"))
                    })?;
                work.flush()?;
            }
            let normalize = if op == Operation::Md5 {
                md5_shared::to_owned_bytes_array_observed
            } else {
                md5_shared::to_owned_bytes_array_with_varchar_cast_observed
            };
            let bytes = normalize(array, idx, &mut |event| {
                md5_shared::observe(&mut work, event)
            })
            .map_err(|error| core_failure(op, error))?;
            arrays.push(bytes);
            compact.push(must_project);
        }
        let out = md5_shared::evaluate_observed(
            op,
            &arrays,
            selection,
            |idx, ordinal, row| {
                if compact[idx] {
                    ordinal
                } else {
                    input.arguments()[idx].value_row(ordinal, row)
                }
            },
            Some(&input.contract().result_type().data_type),
            false,
            &mut |event| md5_shared::observe(&mut work, event),
        )
        .map_err(|error| core_failure(op, error))?;
        SelectedValues::try_new_observed(
            selection,
            &input.contract().result_type().data_type,
            out,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
fn core_failure(op: Operation, error: CoreError) -> KernelFailure {
    match error {
        CoreError::Kernel(failure) => failure,
        diagnostic => internal(&md5_shared::compatibility::error_text(
            diagnostic,
            md5_shared::compatibility::diagnostic_label(op),
        )),
    }
}
