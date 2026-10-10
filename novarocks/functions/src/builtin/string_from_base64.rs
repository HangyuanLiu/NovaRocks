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

//! Selected FROM_BASE64 with the original STANDARD decoding grammar.
//! Fixed chunk scratch and direct Latin1-to-UTF8 output avoid per-row allocation.
//! Library decoding is opaque within each <=256-byte input chunk. Output Layout
//! checks are representation gates, not a formal host memory grant.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use base64::{
    DecodeSliceError, Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
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

fn visit_decoded(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(u8, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<bool, KernelFailure> {
    if text.is_empty() {
        return Ok(false);
    }
    work.flush()?;
    let mut scratch = [0u8; 192];
    work.flush()?;
    let input = text.as_bytes();
    let mut offset = 0;
    while offset < input.len() {
        let length = (input.len() - offset).min(256);
        let end = offset + length;
        let engine = if end == input.len() {
            &STANDARD
        } else {
            &STANDARD_NO_PAD
        };
        work.step()?;
        work.flush()?;
        // Full nonfinal quads cannot contain padding. The final chunk alone
        // keeps the original canonical padding and zero trailing-bit checks.
        // Both engines use the same library decoder, not a second parser.
        let decoded = engine.decode_slice(&input[offset..end], &mut scratch);
        work.flush()?;
        work.step()?;
        let length = match decoded {
            Ok(length) => length,
            Err(DecodeSliceError::DecodeError(_)) => return Ok(false),
            Err(DecodeSliceError::OutputSliceTooSmall) => {
                return Err(internal("from_base64 fixed scratch is too small"));
            }
        };
        for byte in &scratch[..length] {
            visit(*byte, work)?;
            work.step()?;
        }
        offset = end;
        work.step()?;
    }
    Ok(true)
}
fn measure_row(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    let mut length = 0usize;
    let valid = visit_decoded(text, work, |byte, work| {
        length = length
            .checked_add(if byte < 128 { 1 } else { 2 })
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.step()
    })?;
    // A partially decoded invalid row contributes no bytes to the output.
    Ok(valid.then_some(length))
}
fn emit_row(
    text: &str,
    bytes: &mut Vec<u8>,
    limit: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    let start = bytes.len();
    let mut exceeded = false;
    let valid = visit_decoded(text, work, |byte, work| {
        let width = if byte < 128 { 1 } else { 2 };
        let available = limit.checked_sub(bytes.len()).ok_or_else(|| {
            internal("from_base64 destination already exceeds its measured extent")
        })?;
        let fits = width <= available;
        work.step()?;
        if exceeded || !fits {
            exceeded = true;
            return Ok(());
        }
        if byte < 128 {
            bytes.push(byte);
            work.step()?;
        } else {
            // Latin1 code point U+0080..U+00FF, never lossy UTF8 decoding.
            bytes.push(0xc0 | (byte >> 6));
            work.step()?;
            bytes.push(0x80 | (byte & 63));
            work.step()?;
        }
        Ok(())
    })?;
    if !valid {
        // Invalid rows may have a valid prefix. Its speculative bytes stay
        // inside the admitted final extent and are never published as a value.
        bytes.truncate(start);
        work.step()?;
        return Ok(false);
    }
    if exceeded {
        return Err(internal("from_base64 exceeded its measured output extent"));
    }
    Ok(true)
}

pub(super) fn evaluate_string_from_base64<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 1 || arguments.len() != 1 {
            return Err(invalid(
                "from_base64 requires its exact one checked argument",
            ));
        }
        let FunctionArgumentType::Value(source) = &types[0] else {
            return Err(invalid("from_base64 requires a value argument"));
        };
        let exact =
            source.logical_type == ValueLogicalType::Physical && source.data_type == DataType::Utf8;
        work.step()?;
        if !exact {
            return Err(invalid(
                "from_base64 differs from its exact installed argument profile",
            ));
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "from_base64 differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("from_base64 selected source is not Utf8"))?;
        let selected = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<&str>, KernelFailure> {
            let row = arguments[0].value_row(ordinal, batch_row);
            work.step()?;
            if row >= strings.len() {
                return Err(internal(
                    "from_base64 selected argument row is out of bounds",
                ));
            }
            if strings.is_null(row) {
                if !source.nullable {
                    return Err(internal(
                        "from_base64 non-null argument contains selected SQL NULL",
                    ));
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
            if let Some(text) = selected(ordinal, row, &mut work)?
                && let Some(length) = measure_row(text, &mut work)?
            {
                total = total
                    .checked_add(length)
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
            let valid = match selected(ordinal, row, &mut work)? {
                Some(text) => emit_row(text, &mut bytes, total, &mut work)?,
                None => false,
            };
            validity.append(valid);
            has_null |= !valid;
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "from_base64 differs from its measured output extent",
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
#[path = "string_from_base64_tests.rs"]
mod tests;
