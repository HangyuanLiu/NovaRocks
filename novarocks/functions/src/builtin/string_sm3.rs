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

//! Selected SM3 over the exact installed unary Utf8 profile.
//! Digest state and output are fixed-width; no per-row String is allocated.
//! Opaque library calls keep original-control boundaries. Checked output
//! layouts are representation gates, not a formal host memory grant.

use super::sm3_shared::{Observation, output_width, visit_digest};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
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
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
fn append_digest(
    text: &[u8],
    bytes: &mut Vec<u8>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    visit_digest(
        text,
        &mut |observation| match observation {
            Observation::Step => work.step(),
            Observation::OpaqueBoundary => work.flush(),
        },
        &mut |byte| bytes.push(byte),
    )
}

pub(super) fn evaluate_string_sm3<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 1 || arguments.len() != 1 {
            return Err(invalid("sm3 requires its exact one checked argument"));
        }
        let FunctionArgumentType::Value(source) = &types[0] else {
            return Err(invalid("sm3 requires a value argument"));
        };
        let exact =
            source.logical_type == ValueLogicalType::Physical && source.data_type == DataType::Utf8;
        work.step()?;
        if !exact {
            return Err(invalid(
                "sm3 differs from its exact installed argument profile",
            ));
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "sm3 differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("sm3 selected source is not Utf8"))?;
        let selected = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<&str>, KernelFailure> {
            let row = arguments[0].value_row(ordinal, batch_row);
            work.step()?;
            if row >= strings.len() {
                return Err(internal("sm3 selected argument row is out of bounds"));
            }
            if strings.is_null(row) {
                if !source.nullable {
                    return Err(internal("sm3 non-null argument contains selected SQL NULL"));
                }
                Ok(None)
            } else {
                Ok(Some(strings.value(row)))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, row) in selection.iter().enumerate() {
            if let Some(text) = selected(ordinal, row, &mut work)? {
                total = total
                    .checked_add(output_width(text.as_bytes()))
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), total)?;
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
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, row) in selection.iter().enumerate() {
            match selected(ordinal, row, &mut work)? {
                None => {
                    validity.append(false);
                    has_null = true;
                }
                Some(text) => {
                    let width = output_width(text.as_bytes());
                    if width > total - bytes.len() {
                        return Err(internal("sm3 exceeded its measured output extent"));
                    }
                    // The shared original author owns successful empty text and hashing.
                    append_digest(text.as_bytes(), &mut bytes, &mut work)?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal("sm3 differs from its measured output extent"));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
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
#[path = "string_sm3_tests.rs"]
mod tests;
