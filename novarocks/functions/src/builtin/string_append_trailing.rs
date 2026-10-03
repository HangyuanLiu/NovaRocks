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

//! Selected byte-suffix transform over borrowed UTF-8 spans.
//! Empty source stays empty and suffix lengths other than one byte yield NULL.
//! Output layout checks do not supply a formal host allocation grant.

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
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
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(offsets)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn row_plan<'a>(
    arrays: [&'a StringArray; 2],
    arguments: &[EvaluatedArgument<'_>],
    types: &[FunctionArgumentType],
    ordinal: usize,
    batch_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<(&'a str, Option<u8>)>, KernelFailure> {
    let mut rows = [0usize; 2];
    let mut is_null = false;
    for (index, argument) in arguments.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        rows[index] = row;
        work.step()?;
        if row >= arrays[index].len() {
            return Err(internal(
                "append_trailing selected row is outside its carrier",
            ));
        }
        if arrays[index].is_null(row) {
            let FunctionArgumentType::Value(source) = &types[index] else {
                return Err(invalid("append_trailing requires checked value arguments"));
            };
            if !source.nullable {
                return Err(internal(
                    "append_trailing non-null source contains selected SQL NULL",
                ));
            }
            is_null = true;
        }
    }
    // Both arguments have already been evaluated by the ABI. Neither hidden
    // payload is read when either selected argument is SQL NULL.
    if is_null {
        return Ok(None);
    }
    let suffix = arrays[1].value(rows[1]);
    let valid_suffix = suffix.len() == 1;
    work.step()?;
    if !valid_suffix {
        return Ok(None);
    }
    let text = arrays[0].value(rows[0]);
    let byte = suffix.as_bytes()[0];
    let append = !text.is_empty() && text.as_bytes().last() != Some(&byte);
    work.step()?;
    Ok(Some((text, append.then_some(byte))))
}

pub(super) fn evaluate_string_append_trailing<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 2 || arguments.len() != 2 {
            return Err(invalid(
                "append_trailing differs from its installed argument count",
            ));
        }
        for ty in types {
            let FunctionArgumentType::Value(source) = ty else {
                return Err(invalid("append_trailing requires checked value arguments"));
            };
            if source.logical_type != ValueLogicalType::Physical
                || source.data_type != DataType::Utf8
            {
                return Err(invalid(
                    "append_trailing selected source is outside its installed profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "append_trailing differs from its installed result profile",
            ));
        }
        let source = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("append_trailing source is not Utf8"))?;
        let suffix = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("append_trailing suffix is not Utf8"))?;
        let arrays = [source, suffix];
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some((text, append)) =
                row_plan(arrays, arguments, types, ordinal, batch_row, &mut work)?
            {
                total_bytes = total_bytes
                    .checked_add(text.len())
                    .and_then(|n| n.checked_add(usize::from(append.is_some())))
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), total_bytes)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total_bytes)
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
        for (ordinal, batch_row) in selection.iter().enumerate() {
            match row_plan(arrays, arguments, types, ordinal, batch_row, &mut work)? {
                None => {
                    validity.append(false);
                    has_null = true;
                }
                Some((text, append)) => {
                    let row_bytes = text
                        .len()
                        .checked_add(usize::from(append.is_some()))
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    if row_bytes > total_bytes - bytes.len() {
                        return Err(internal(
                            "append_trailing exceeded its measured output extent",
                        ));
                    }
                    for byte in text.bytes() {
                        bytes.push(byte);
                        work.step()?;
                    }
                    if let Some(byte) = append {
                        bytes.push(byte);
                        work.step()?;
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total_bytes {
            return Err(internal(
                "append_trailing differs from its measured output extent",
            ));
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
#[path = "string_append_trailing_tests.rs"]
mod tests;
