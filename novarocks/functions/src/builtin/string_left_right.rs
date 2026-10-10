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

//! Selected Unicode scalar prefix/suffix over the original borrowed UTF-8 values.
//! Layout checks cover requested representability, not formal host memory grants.

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

pub(super) use crate::string_left_right_core::LeftRightOp as StringLeftRightOp;
use crate::string_left_right_core::{self, LeftRightProjection};

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

/// Directional view of the exact reversed characters yielded by the sole core.
struct ReversedSpan<'a>(&'a str);
struct SelectedProjection<'work, 'control> {
    work: &'work mut EvaluationCheckpoints<'control>,
}
impl<'a> LeftRightProjection<'a> for SelectedProjection<'_, '_> {
    type Error = KernelFailure;
    type Output = &'a str;
    type Reversed = ReversedSpan<'a>;
    fn empty(&mut self) -> Result<&'a str, KernelFailure> {
        Ok("")
    }
    fn forward(
        &mut self,
        source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<&'a str, KernelFailure> {
        let mut bytes = 0;
        for character in characters {
            observe_char(character, self.work)?;
            bytes += character.len_utf8();
        }
        // Preserve the old selected observer's first unselected left character.
        // This is observation only; the selection program remains in the core.
        if let Some(character) = source[bytes..].chars().next() {
            observe_char(character, self.work)?;
        }
        Ok(&source[..bytes])
    }
    fn reverse(
        &mut self,
        source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<ReversedSpan<'a>, KernelFailure> {
        let mut bytes = 0;
        for character in characters {
            observe_char(character, self.work)?;
            bytes += character.len_utf8();
        }
        Ok(ReversedSpan(&source[source.len() - bytes..]))
    }
    fn restore(&mut self, reversed: ReversedSpan<'a>) -> Result<&'a str, KernelFailure> {
        // Reversing the directional character view yields its original forward
        // source bytes. No allocation or second character traversal is needed.
        Ok(reversed.0)
    }
}
fn selected_span<'a>(
    op: StringLeftRightOp,
    text: &'a str,
    count: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<&'a str, KernelFailure> {
    string_left_right_core::project(text, count, op, SelectedProjection { work })
}

pub(super) fn evaluate_string_left_right<'a>(
    op: StringLeftRightOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 2 || types.len() != arguments.len() {
            return Err(invalid(
                "left/right requires its exact two checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("left/right requires value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index == 0 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    }
            {
                return Err(invalid(
                    "left/right differs from its exact installed argument profile",
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
                "left/right differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("left/right selected source is not Utf8"))?;
        let lengths = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("left/right selected length is not Int64"))?;
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<&str>, KernelFailure> {
            let mut rows = [0usize; 2];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal(
                        "left/right selected argument row is out of bounds",
                    ));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("left/right requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "left/right non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            selected_span(op, strings.value(rows[0]), lengths.value(rows[1]), work).map(Some)
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
                        return Err(internal("left/right exceeded its measured output extent"));
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
                "left/right differs from its measured output extent",
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
#[path = "string_left_right_tests.rs"]
mod tests;
