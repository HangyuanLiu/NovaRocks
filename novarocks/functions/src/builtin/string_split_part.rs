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

//! Selected split_part spans preserve the original directional search algorithm.
//! SQL NULL inputs produce a non-null empty string; no payload is read for NULL.
//! Layout checks cover requested representability, not formal host memory grants.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use arrow_buffer::{Buffer, OffsetBuffer};
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
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn find_subslice(
    haystack: &[u8],
    needle: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    if haystack.len() < needle.len() {
        return Ok(None);
    }
    for (ordinal, window) in haystack.windows(needle.len()).enumerate() {
        let mut equal = true;
        for (left, right) in window.iter().zip(needle) {
            let same = left == right;
            work.step()?;
            if !same {
                equal = false;
                break;
            }
        }
        work.step()?;
        if equal {
            return Ok(Some(ordinal));
        }
    }
    Ok(None)
}

fn positive_span<'a>(
    text: &'a str,
    delimiter: &str,
    target: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<&'a str>, KernelFailure> {
    // Utf8 source offsets already bound lengths to i32. Retain the original
    // signed sentinel and its exact final-part/no-delimiter correspondence.
    let delimiter_len = delimiter.len() as isize;
    let mut previous = -delimiter_len;
    let mut offset = -delimiter_len;
    let mut count = 0i64;
    while count < target {
        previous = offset;
        let start = (offset + delimiter_len) as usize;
        if start > text.len() {
            break;
        }
        if let Some(relative) =
            find_subslice(&text.as_bytes()[start..], delimiter.as_bytes(), work)?
        {
            offset = (start + relative) as isize;
            count += 1;
        } else {
            offset = text.len() as isize;
            count = if count == 0 { 0 } else { count + 1 };
            work.step()?;
            break;
        }
        work.step()?;
    }
    Ok((count == target).then(|| &text[(previous + delimiter_len) as usize..offset as usize]))
}

fn negative_span<'a>(
    text: &'a str,
    delimiter: &str,
    part: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<&'a str>, KernelFailure> {
    // The sole installed index is Int32, widened before negation.
    let target =
        usize::try_from(-part).map_err(|_| internal("split_part index is not representable"))?;
    let mut offset = text.len() as isize;
    let mut previous = offset;
    let mut count = 0usize;
    let mut scope = text;
    while count <= target && offset >= 0 {
        work.flush()?;
        let found = scope.rfind(delimiter);
        work.flush()?;
        if let Some(found) = found {
            offset = found as isize;
            count += 1;
            if count == target {
                work.step()?;
                break;
            }
            previous = offset;
            offset -= 1;
            scope = &text[..previous as usize];
        } else {
            offset = -1;
            work.step()?;
            break;
        }
        work.step()?;
    }
    if offset == -1 && count != 0 {
        count += 1;
    }
    work.step()?;
    Ok(if count == target {
        Some(if offset == -1 {
            &text[..previous as usize]
        } else {
            &text[offset as usize + delimiter.len()..previous as usize]
        })
    } else {
        None
    })
}

fn split_part_span<'a>(
    text: &'a str,
    delimiter: &str,
    index: i32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<&'a str, KernelFailure> {
    if index == 0 {
        return Ok("");
    }
    let index = i64::from(index);
    if delimiter.is_empty() {
        if index > text.len() as i64 {
            return Ok("");
        }
        let mut byte = 0usize;
        let mut count = 0i64;
        let target = index.saturating_sub(1);
        while byte < text.len() && count < target {
            let character = text[byte..]
                .chars()
                .next()
                .ok_or_else(|| internal("split_part character extent changed"))?;
            byte += character.len_utf8();
            count += 1;
            for _ in 0..character.len_utf8() {
                work.step()?;
            }
        }
        if byte >= text.len() {
            return Ok("");
        }
        let character = text[byte..]
            .chars()
            .next()
            .ok_or_else(|| internal("split_part character extent changed"))?;
        for _ in 0..character.len_utf8() {
            work.step()?;
        }
        return Ok(&text[byte..byte + character.len_utf8()]);
    }
    let selected = if index > 0 {
        positive_span(text, delimiter, index, work)?
    } else {
        negative_span(text, delimiter, index, work)?
    };
    Ok(selected.unwrap_or(if index == 1 || index == -1 { text } else { "" }))
}

pub(super) fn evaluate_string_split_part<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 3 || arguments.len() != 3 {
            return Err(invalid(
                "split_part requires its exact three checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("split_part requires value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int32
                    }
            {
                return Err(invalid(
                    "split_part differs from its exact installed argument profile",
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
                "split_part differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("split_part selected source is not Utf8"))?;
        let delimiters = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("split_part selected delimiter is not Utf8"))?;
        let indices = arguments[2]
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal("split_part selected index is not Int32"))?;
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<&str, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal(
                        "split_part selected argument row is out of bounds",
                    ));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("split_part requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "split_part non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok("");
            }
            split_part_span(
                strings.value(rows[0]),
                delimiters.value(rows[1]),
                indices.value(rows[2]),
                work,
            )
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let span = row_span(ordinal, batch_row, &mut work)?;
            total = total
                .checked_add(span.len())
                .ok_or(KernelFailure::ResourceExhausted)?;
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
        offsets.push(0i32);
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let span = row_span(ordinal, batch_row, &mut work)?;
            if span.len() > total - bytes.len() {
                return Err(internal("split_part exceeded its measured output extent"));
            }
            for byte in span.bytes() {
                bytes.push(byte);
                work.step()?;
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "split_part differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            None,
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
#[path = "string_split_part_tests.rs"]
mod tests;
