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

//! Selected Unicode scalar substring over the original borrowed UTF-8 values.
//! Layout checks cover requested representability, not formal host memory grants.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringSubstringOp {
    Substring,
}

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
        .map(|bits| bits / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn observe_char(
    character: char,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for _ in 0..character.len_utf8() {
        work.step()?;
    }
    Ok(())
}

fn substring_span<'a>(
    text: &'a str,
    position: i32,
    length: Option<i32>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<&'a str, KernelFailure> {
    if position == 0 || length.is_some_and(|n| n <= 0) {
        return Ok("");
    }
    let start = if position > 0 {
        (i64::from(position) - 1) as usize
    } else {
        let mut count = 0usize;
        for character in text.chars() {
            count += 1;
            observe_char(character, work)?;
        }
        let distance = (-i64::from(position)) as usize;
        if distance > count {
            return Ok("");
        }
        count - distance
    };
    let end = length.map(|n| start.saturating_add(n as usize));
    let mut start_byte = None;
    for (ordinal, (byte, character)) in text.char_indices().enumerate() {
        observe_char(character, work)?;
        if ordinal == start {
            start_byte = Some(byte);
        }
        if end == Some(ordinal) {
            return Ok(&text[start_byte.unwrap_or(byte)..byte]);
        }
    }
    Ok(start_byte.map_or("", |byte| &text[byte..]))
}

pub(super) fn evaluate_string_substring<'a>(
    _op: StringSubstringOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if !matches!(types.len(), 2 | 3) || types.len() != arguments.len() {
            return Err(invalid(
                "substring requires its exact two or three checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("substring requires value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index == 0 {
                        DataType::Utf8
                    } else {
                        DataType::Int32
                    }
            {
                return Err(invalid(
                    "substring differs from its exact installed argument profile",
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
                "substring differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("substring selected source is not Utf8"))?;
        let starts = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal("substring selected position is not Int32"))?;
        let lengths = if arguments.len() == 3 {
            Some(
                arguments[2]
                    .array()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| internal("substring selected length is not Int32"))?,
            )
        } else {
            None
        };
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<&str>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("substring selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("substring requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "substring non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            substring_span(
                strings.value(rows[0]),
                starts.value(rows[1]),
                lengths.map(|array| array.value(rows[2])),
                work,
            )
            .map(Some)
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some(span) = row_span(ordinal, batch_row, &mut work)? {
                total = total
                    .checked_add(span.len())
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
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
        for (ordinal, batch_row) in selection.iter().enumerate() {
            match row_span(ordinal, batch_row, &mut work)? {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(span) => {
                    if span.len() > total - bytes.len() {
                        return Err(internal("substring exceeded its measured output extent"));
                    }
                    for byte in span.bytes() {
                        bytes.push(byte);
                        work.step()?;
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "substring differs from its measured output extent",
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
#[path = "string_substring_tests.rs"]
mod tests;
