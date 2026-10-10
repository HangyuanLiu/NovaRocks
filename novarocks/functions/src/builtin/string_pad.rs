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
    string_repeat_pad_core::{self, CyclicCharacters, PadProjection, SourceCharacters},
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
#[cfg(test)]
const MAX_PAD_LENGTH: usize = string_repeat_pad_core::MAX_STRING_BYTES;

struct BorrowedCharacters<'a> {
    text: &'a str,
    len: usize,
}
impl SourceCharacters for BorrowedCharacters<'_> {
    fn len(&self) -> usize {
        self.len
    }
    fn iter(&self) -> impl Iterator<Item = char> {
        self.text.chars()
    }
}
struct BorrowedCycle<'a> {
    text: &'a str,
    len: usize,
    cursor: std::str::Chars<'a>,
    next: usize,
}
impl CyclicCharacters for BorrowedCycle<'_> {
    type Error = KernelFailure;
    fn len(&self) -> usize {
        self.len
    }
    fn get(&mut self, index: usize) -> Result<char, KernelFailure> {
        if index == 0 {
            self.cursor = self.text.chars();
            self.next = 0;
        }
        if index != self.next {
            return Err(internal(
                "lpad/rpad cyclic projection differs from its source order",
            ));
        }
        let ch = self
            .cursor
            .next()
            .ok_or_else(|| internal("lpad/rpad cyclic projection has no original character"))?;
        self.next += 1;
        Ok(ch)
    }
}
/// Inline storage projection: no per-row String, Vec<char>, or scratch grant.
/// The shared original program alone selects prefix, fill count, side, and caps.
struct CompactPadProjection<'a, 'control> {
    bytes: Option<&'a mut Vec<u8>>,
    total: usize,
    extent: usize,
    work: &'a mut EvaluationCheckpoints<'control>,
}
impl CompactPadProjection<'_, '_> {
    fn push(&mut self, byte: u8) -> Result<(), KernelFailure> {
        self.extent = self
            .extent
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        if let Some(bytes) = &mut self.bytes {
            if bytes.len() >= self.total {
                return Err(internal("lpad/rpad exceeded its measured output extent"));
            }
            bytes.push(byte);
        }
        self.work.step()
    }
    fn copy(&mut self, source: &str) -> Result<(), KernelFailure> {
        for byte in source.bytes() {
            self.push(byte)?;
        }
        Ok(())
    }
    fn characters(&mut self, text: &str) -> Result<usize, KernelFailure> {
        let mut count = 0;
        for _ in text.chars() {
            count += 1;
            self.work.step()?;
        }
        Ok(count)
    }
}
impl<'source> PadProjection<'source> for CompactPadProjection<'_, '_> {
    type Error = KernelFailure;
    type Source = BorrowedCharacters<'source>;
    type Pad = BorrowedCycle<'source>;
    type Fill = ();
    type Output = usize;
    type Emission = Option<usize>;
    fn source(&mut self, text: &'source str) -> Result<Self::Source, KernelFailure> {
        Ok(BorrowedCharacters {
            text,
            len: self.characters(text)?,
        })
    }
    fn pad(&mut self, text: &'source str) -> Result<Self::Pad, KernelFailure> {
        Ok(BorrowedCycle {
            text,
            len: self.characters(text)?,
            cursor: text.chars(),
            next: 0,
        })
    }
    fn prefix(&mut self, chars: impl Iterator<Item = char>) -> Result<usize, KernelFailure> {
        for ch in chars {
            self.push_fill(&mut (), ch)?;
        }
        Ok(self.extent)
    }
    fn new_fill(&mut self, source: &str, left: bool) -> Result<(), KernelFailure> {
        // Direct compact output stores the right-prefix before its fill; this
        // is storage projection only. The original owned projection is unchanged.
        if !left {
            self.copy(source)?;
        }
        Ok(())
    }
    fn push_fill(&mut self, _fill: &mut (), ch: char) -> Result<(), KernelFailure> {
        let mut utf8 = [0u8; 4];
        for byte in ch.encode_utf8(&mut utf8).bytes() {
            self.push(byte)?;
        }
        Ok(())
    }
    fn compose_left(&mut self, _fill: (), source: &str) -> Result<usize, KernelFailure> {
        self.copy(source)?;
        Ok(self.extent)
    }
    fn compose_right(&mut self, _source: &str, _fill: ()) -> Result<usize, KernelFailure> {
        Ok(self.extent)
    }
    fn result_len(&self, result: &usize) -> usize {
        *result
    }
    fn emit(&mut self, result: Option<usize>) -> Result<Option<usize>, KernelFailure> {
        Ok(result)
    }
}

/// Original static failures are shared without constructing CPU diagnostics during admission.
#[derive(Clone, Copy)]
pub(super) enum StaticProfileFailure {
    Count,
    Value,
    Source,
    Result,
}
impl StaticProfileFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Count => "lpad/rpad requires its exact three checked arguments",
            Self::Value => "lpad/rpad requires value arguments",
            Self::Source => "lpad/rpad differs from its exact installed argument profile",
            Self::Result => "lpad/rpad differs from its exact installed result profile",
        }
    }
}
pub(super) fn check_count(types: usize, arguments: usize) -> Result<(), StaticProfileFailure> {
    let arity = 3;
    if types != arity || types != arguments {
        return Err(StaticProfileFailure::Count);
    }
    Ok(())
}
pub(super) fn check_argument(
    index: usize,
    ty: &FunctionArgumentType,
) -> Result<(), StaticProfileFailure> {
    let FunctionArgumentType::Value(ty) = ty else {
        return Err(StaticProfileFailure::Value);
    };
    if ty.logical_type != ValueLogicalType::Physical
        || ty.data_type
            != if index != 1 {
                DataType::Utf8
            } else {
                DataType::Int64
            }
    {
        return Err(StaticProfileFailure::Source);
    }
    Ok(())
}
pub(super) fn check_result(target: &crate::FunctionValueType) -> Result<(), StaticProfileFailure> {
    if target.logical_type != ValueLogicalType::Physical
        || target.data_type != DataType::Utf8
        || !target.nullable
    {
        return Err(StaticProfileFailure::Result);
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
        check_count(types.len(), arguments.len()).map_err(|error| invalid(error.message()))?;
        for (index, ty) in types.iter().enumerate() {
            check_argument(index, ty).map_err(|error| invalid(error.message()))?;
            work.step()?;
        }
        let target = input.contract().result_type();
        check_result(target).map_err(|error| invalid(error.message()))?;
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
         -> Result<Option<(&str, i64, usize)>, KernelFailure> {
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
            Ok(Some((
                strings.value(rows[0]),
                lengths.value(rows[1]),
                rows[2],
            )))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some((text, count, pad_row)) = selected_plan(ordinal, batch_row, &mut work)? {
                if let Some(extent) = string_repeat_pad_core::pad_into(
                    text,
                    count,
                    || pads.value(pad_row),
                    op == StringPadOp::Left,
                    CompactPadProjection {
                        bytes: None,
                        total: 0,
                        extent: 0,
                        work: &mut work,
                    },
                )? {
                    total = total
                        .checked_add(extent)
                        .ok_or(KernelFailure::ResourceExhausted)?;
                }
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
            let measured = match selected_plan(ordinal, batch_row, &mut work)? {
                None => None,
                Some((text, count, pad_row)) => string_repeat_pad_core::pad_into(
                    text,
                    count,
                    || pads.value(pad_row),
                    op == StringPadOp::Left,
                    CompactPadProjection {
                        bytes: None,
                        total: 0,
                        extent: 0,
                        work: &mut work,
                    },
                )?
                .map(|extent| (text, count, pad_row, extent)),
            };
            match measured {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some((text, count, pad_row, extent)) => {
                    let written = string_repeat_pad_core::pad_into(
                        text,
                        count,
                        || pads.value(pad_row),
                        op == StringPadOp::Left,
                        CompactPadProjection {
                            bytes: Some(&mut bytes),
                            total,
                            extent: 0,
                            work: &mut work,
                        },
                    )?;
                    if written != Some(extent) {
                        return Err(internal(
                            "lpad/rpad differs from its measured output extent",
                        ));
                    }
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
