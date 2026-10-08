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

//! Selected original Rust Unicode case conversion, without runtime name lookup.
//! Numerical allocation checks prove requested-layout representability only.
//! Whole-string Unicode conversion remains an observed opaque library call;
//! it is not internally cooperative or a formal host memory grant.

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringCaseOp {
    Lower,
    Upper,
}

fn mapped_length(
    op: StringCaseOp,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    let mut length = 0usize;
    for ch in text.chars() {
        // UTF-8 decoding consumes at most four bytes per scalar. The Unicode
        // mapping iterators hold at most three chars and allocate no backing.
        work.step()?;
        match op {
            StringCaseOp::Lower => {
                for mapped in ch.to_lowercase() {
                    length = length
                        .checked_add(mapped.len_utf8())
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    work.step()?;
                }
            }
            StringCaseOp::Upper => {
                for mapped in ch.to_uppercase() {
                    length = length
                        .checked_add(mapped.len_utf8())
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    work.step()?;
                }
            }
        }
    }
    // Whole-string lowercase's contextual sigma chooses σ or ς. Both have
    // two UTF-8 bytes; measuring per-char mappings therefore preserves length
    // without replacing the original context-sensitive value algorithm.
    Ok(length)
}
fn temporary_layout(input_bytes: usize, output_bytes: usize) -> Result<(), KernelFailure> {
    if input_bytes == 0 && output_bytes == 0 {
        return Ok(());
    }
    // Locked Rust str::{to_lowercase,to_uppercase} starts one Vec<u8> at N;
    // String::push uses RawVec max(2*old, required, 8). For final length L,
    // requested capacity is <=max(N,2L,8), and cumulative requests are
    // <=N+2*max(2L,8). These are requests, not allocator-retained/RSS bytes.
    let doubled = output_bytes
        .checked_mul(2)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let next = doubled.max(8);
    let cumulative = input_bytes
        .checked_add(
            next.checked_mul(2)
                .ok_or(KernelFailure::ResourceExhausted)?,
        )
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(input_bytes.max(next)).map_err(|_| KernelFailure::ResourceExhausted)?;
    // Historical request totals are arithmetic, not one allocation layout.
    let _ = cumulative;
    Ok(())
}
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
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|value| value.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_string_case<'a>(
    op: StringCaseOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "string case conversion requires one checked value argument",
            ));
        };
        let target = input.contract().result_type();
        if source.logical_type != ValueLogicalType::Physical
            || source.data_type != DataType::Utf8
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || target.nullable != source.nullable
        {
            return Err(invalid(
                "string case conversion differs from its exact installed profile",
            ));
        }
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("string case conversion selected carrier is not Utf8"))?;
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut output_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= values.len() {
                return Err(internal(
                    "string case conversion selected row is outside its carrier",
                ));
            }
            if values.is_null(row) {
                if !source.nullable {
                    return Err(internal(
                        "string case conversion non-null source contains selected SQL NULL",
                    ));
                }
                continue;
            }
            let text = values.value(row);
            let length = mapped_length(op, text, &mut work)?;
            temporary_layout(text.len(), length)?;
            output_bytes = output_bytes
                .checked_add(length)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        output_capacity(selection.len(), output_bytes)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(output_bytes)
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
        walk_case_values(
            op,
            *argument,
            selection,
            source.nullable,
            &mut work,
            |converted, work| {
                if let Some(converted) = converted {
                    if converted.len() > output_bytes - bytes.len() {
                        return Err(internal(
                            "string case conversion exceeded its measured output extent",
                        ));
                    }
                    for byte in converted.bytes() {
                        bytes.push(byte);
                        work.step()?;
                    }
                    validity.append(true);
                } else {
                    has_null = true;
                    validity.append(false);
                }
                offsets.push(
                    i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?,
                );
                work.step()?;
                Ok(())
            },
        )?;
        if bytes.len() != output_bytes {
            return Err(internal(
                "string case conversion differs from its measured output extent",
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

/// One original whole-string calculation walk for both value boundaries.
/// The per-char pass above only measures output; final sigma still depends on
/// its complete input string in Rust's original Unicode implementation.
fn walk_case_values(
    op: StringCaseOp,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    source_nullable: bool,
    work: &mut EvaluationCheckpoints<'_>,
    mut append: impl FnMut(Option<String>, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("string case conversion selected carrier is not Utf8"))?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= values.len() {
            return Err(internal(
                "string case conversion selected row is outside its carrier",
            ));
        }
        let converted = if values.is_null(row) {
            if !source_nullable {
                return Err(internal(
                    "string case conversion non-null source contains selected SQL NULL",
                ));
            }
            None
        } else {
            work.flush()?;
            let converted = match op {
                StringCaseOp::Lower => values.value(row).to_lowercase(),
                StringCaseOp::Upper => values.value(row).to_uppercase(),
            };
            work.flush()?;
            Some(converted)
        };
        append(converted, work)?;
    }
    Ok(())
}
/// V1 owns child evaluation and calls the same calculation on all input rows.
/// Keep its original carrier diagnostics and original Arrow assembly sink;
/// unsupported carriers are not coerced and output metadata is not applied.
/// Its Vec/StringArray allocation and offset panic behavior remains unchanged.
pub fn evaluate_legacy_case(op: StringCaseOp, input: &ArrayRef) -> Result<ArrayRef, String> {
    let values = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            match op {
                StringCaseOp::Lower => "lower expects string",
                StringCaseOp::Upper => "upper: argument must be a string array",
            }
            .to_string()
        })?;
    let mut output = Vec::with_capacity(values.len());
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    walk_case_values(
        op,
        EvaluatedArgument::Column(input),
        Selection::all(values.len()),
        true,
        &mut work,
        |converted, _| {
            output.push(converted);
            Ok(())
        },
    )
    .map_err(|failure| match failure {
        KernelFailure::InvalidProgram(message)
        | KernelFailure::Internal(message)
        | KernelFailure::Operational(message) => message.message().to_string(),
        other => other.to_string(),
    })?;
    let output = match op {
        StringCaseOp::Lower => StringArray::from(output),
        StringCaseOp::Upper => StringArray::from_iter(output),
    };
    Ok(Arc::new(output))
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("pure scalar calculation never waits")
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "string_case_tests.rs"]
mod tests;
