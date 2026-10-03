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

//! Borrowed selected Unicode-scalar lpad/rpad with both original size gates.
//! Two output passes measure and directly write without row String/char buffers.
//! Layout gates are representational facts, not formal host memory grants.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringPadOp {
    Left,
    Right,
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

// The original string::common owner applies this independently to target
// Unicode scalar count and resulting UTF-8 bytes. Oversize is successful NULL.
const MAX_PAD_LENGTH: usize = 1_048_576;

struct RowPlan<'a> {
    prefix: &'a str,
    pad: &'a str,
    needed: usize,
    bytes: usize,
}

fn row_plan<'a>(
    text: &'a str,
    pad: &'a str,
    count: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<RowPlan<'a>>, KernelFailure> {
    if count < 0 {
        return Ok(None);
    }
    // A target not representable on this platform remains oversize SQL NULL;
    // truncating Int64 to usize must not turn it into a small accepted target.
    let Ok(target) = usize::try_from(count) else {
        return Ok(None);
    };
    if target > MAX_PAD_LENGTH {
        return Ok(None);
    }
    let mut source_chars = 0usize;
    let mut end = 0usize;
    for (byte, ch) in text.char_indices().take(target) {
        source_chars += 1;
        end = byte + ch.len_utf8();
        work.step()?;
    }
    let prefix = &text[..end];
    if prefix.len() > MAX_PAD_LENGTH {
        return Ok(None);
    }
    let needed = if source_chars == target || pad.is_empty() {
        0
    } else {
        target - source_chars
    };
    let mut bytes = prefix.len();
    for ch in pad.chars().cycle().take(needed) {
        bytes = bytes
            .checked_add(ch.len_utf8())
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.step()?;
        if bytes > MAX_PAD_LENGTH {
            return Ok(None);
        }
    }
    Ok(Some(RowPlan {
        prefix,
        pad,
        needed,
        bytes,
    }))
}

fn write_plan(
    op: StringPadOp,
    plan: RowPlan<'_>,
    bytes: &mut Vec<u8>,
    total: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    if plan.bytes > total - bytes.len() {
        return Err(internal("lpad/rpad exceeded its measured output extent"));
    }
    let copy_source = |bytes: &mut Vec<u8>, work: &mut EvaluationCheckpoints<'_>| {
        for byte in plan.prefix.bytes() {
            bytes.push(byte);
            work.step()?;
        }
        Ok::<_, KernelFailure>(())
    };
    if op == StringPadOp::Right {
        copy_source(bytes, work)?;
    }
    for ch in plan.pad.chars().cycle().take(plan.needed) {
        let mut utf8 = [0u8; 4];
        let encoded = ch.encode_utf8(&mut utf8);
        for byte in encoded.bytes() {
            bytes.push(byte);
            work.step()?;
        }
    }
    if op == StringPadOp::Left {
        copy_source(bytes, work)?;
    }
    Ok(())
}

pub(super) fn evaluate_string_pad<'a>(
    op: StringPadOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 3 || types.len() != arguments.len() {
            return Err(invalid(
                "lpad/rpad requires its exact three checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("lpad/rpad requires value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index != 1 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    }
            {
                return Err(invalid(
                    "lpad/rpad differs from its exact installed argument profile",
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
                "lpad/rpad differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("lpad/rpad selected source is not Utf8"))?;
        let lengths = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("lpad/rpad selected length is not Int64"))?;
        let pads = arguments[2]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("lpad/rpad selected padding is not Utf8"))?;
        let selected_plan = |ordinal,
                             batch_row,
                             work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<RowPlan<'_>>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("lpad/rpad selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("lpad/rpad requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "lpad/rpad non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            row_plan(
                strings.value(rows[0]),
                pads.value(rows[2]),
                lengths.value(rows[1]),
                work,
            )
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some(plan) = selected_plan(ordinal, batch_row, &mut work)? {
                total = total
                    .checked_add(plan.bytes)
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
            match selected_plan(ordinal, batch_row, &mut work)? {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(plan) => {
                    write_plan(op, plan, &mut bytes, total, &mut work)?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "lpad/rpad differs from its measured output extent",
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
#[path = "string_pad_tests.rs"]
mod tests;
