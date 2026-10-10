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
//! Complete selected TO_BASE64; one original row encoder plus exact source receipt.
//! The original Latin1 loop is observed; library scratch and formal MEM remain OPEN.
use super::{
    md5_shared::{Observation, to_owned_bytes_array_observed},
    to_base64_shared::encode_row_observed,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};
fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
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
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<Option<String>>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}
pub(super) fn evaluate_string_to_base64<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 1 || arguments.len() != 1 {
            return Err(invalid("to_base64 requires its exact checked argument"));
        }
        let FunctionArgumentType::Value(source_type) = &types[0] else {
            return Err(invalid("to_base64 requires a value argument"));
        };
        if source_type.logical_type != ValueLogicalType::Physical
            || source_type.data_type != DataType::Utf8
        {
            return Err(invalid(
                "to_base64 differs from its exact installed argument profile",
            ));
        }
        work.step()?;
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "to_base64 differs from its exact installed result profile",
            ));
        }
        let source = input
            .contract()
            .to_base64_byte_source()
            .ok_or_else(|| invalid("to_base64 actual source receipt is absent"))?;
        let reader =
            to_owned_bytes_array_observed(arguments[0].array().clone(), 0, &mut |observation| {
                match observation {
                    Observation::Step => work.step(),
                    Observation::OpaqueBoundary => work.flush(),
                }
            })
            .map_err(|error| match error {
                super::md5_shared::CoreError::Kernel(failure) => failure,
                _ => internal("to_base64 exact Utf8 reader admission failed"),
            })?;
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            work.step()?;
            let row = arguments[0].value_row(ordinal, batch_row);
            if row >= reader.len() {
                return Err(internal("to_base64 selected argument row is out of bounds"));
            }
            if reader.is_null(row) && !source_type.nullable {
                return Err(internal(
                    "to_base64 non-null argument contains selected SQL NULL",
                ));
            }
            let value = if reader.is_null(row) {
                None
            } else {
                encode_row_observed(&reader, row, source, &mut |observation| match observation {
                    Observation::Step => work.step(),
                    Observation::OpaqueBoundary => work.flush(),
                })?
            };
            if let Some(text) = &value {
                total = total
                    .checked_add(text.len())
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            output_capacity(selection.len(), total)?;
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0);
        let mut has_null = false;
        for value in &values {
            match value {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(text) => {
                    for byte in text.bytes() {
                        work.step()?;
                        bytes.push(byte);
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        work.flush()?;
        let array = std::sync::Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
            || work.step(),
        )
    })();
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}
#[cfg(test)]
#[path = "string_to_base64_tests.rs"]
mod tests;
