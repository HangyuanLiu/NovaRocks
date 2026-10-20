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
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringCaseOp {
    Lower,
    Upper,
}

fn mapped_length(
    op: StringCaseOp,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u128, KernelFailure> {
    let mut length = 0u128;
    for ch in text.chars() {
        // UTF-8 decoding consumes at most four bytes per scalar. The Unicode
        // mapping iterators hold at most three chars and allocate no backing.
        work.step()?;
        match op {
            StringCaseOp::Lower => {
                for mapped in ch.to_lowercase() {
                    length += mapped.len_utf8() as u128;
                    work.step()?;
                }
            }
            StringCaseOp::Upper => {
                for mapped in ch.to_uppercase() {
                    length += mapped.len_utf8() as u128;
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
    super::string_failure::array_layout::<u8>(input_bytes.max(next))?;
    // Historical request totals are arithmetic, not one allocation layout.
    let _ = cumulative;
    Ok(())
}
fn output_layout(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let offsets = super::string_failure::array_layout::<i32>(offsets)?.size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    super::string_failure::array_layout::<u8>(bitmap)?;
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
        if source.logical_type() != ValueLogicalType::Physical
            || source.data_type() != &DataType::Utf8
            || target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
            || target.nullable() != source.nullable()
        {
            return Err(invalid(
                "string case conversion differs from its exact installed profile",
            ));
        }
        let selection = input.selection();
        let array = build_case_array(op, *argument, selection, source.nullable(), &mut work)?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            array,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

/// One original whole-string calculation walk for both value boundaries.
/// The per-char pass above only measures output; final sigma still depends on
/// its complete input string in Rust's original Unicode implementation.
fn build_case_array(
    op: StringCaseOp,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    source_nullable: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("string case conversion selected carrier is not Utf8"))?;
    output_layout(selection.len(), 0)?;
    let mut output_extent = 0i32;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        work.step()?;
        if row >= values.len() {
            return Err(internal(
                "string case conversion selected row is outside its carrier",
            ));
        }
        if values.is_null(row) {
            if !source_nullable {
                return Err(internal(
                    "string case conversion non-null source contains selected SQL NULL",
                ));
            }
            continue;
        }
        let text = values.value(row);
        let length = value_extent(mapped_length(op, text, work)?)?;
        temporary_layout(text.len(), length)?;
        output_extent = crate::aggregate_scalar::checked_utf8_output_offset(output_extent, length)?;
    }
    let output_bytes = output_extent as usize;
    output_layout(selection.len(), output_bytes)?;
    work.flush()?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(output_bytes)
        .map_err(|source| super::string_failure::reservation::<u8>(output_bytes, source))?;
    work.flush()?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(selection.len() + 1)
        .map_err(|source| super::string_failure::reservation::<i32>(selection.len() + 1, source))?;
    work.flush()?;
    let mut validity = BooleanBufferBuilder::new(selection.len());
    work.flush()?;
    offsets.push(0i32);
    let mut has_null = false;
    walk_case_values(
        op,
        argument,
        selection,
        source_nullable,
        work,
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
                i32::try_from(bytes.len())
                    .map_err(|_| internal("validated string case output extent drifted"))?,
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
    Ok(array)
}

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
/// Arguments are already evaluated. Both entry points share Unicode conversion,
/// per-value representation planning and the final Arrow output author.
pub fn evaluate_required_case(
    op: StringCaseOp,
    input: &ArrayRef,
) -> Result<ArrayRef, KernelFailure> {
    let values = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelFailure::from_domain(CaseFailure::Carrier(op)))?;
    let mut work = EvaluationCheckpoints::new(&DirectCaseControl);
    let result = build_case_array(
        op,
        EvaluatedArgument::Column(input),
        Selection::all(values.len()),
        true,
        &mut work,
    );
    work.finish_result(result)
}
struct DirectCaseControl;
impl KernelEvaluationControl for DirectCaseControl {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("direct string case calculation must not wait"))
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "string_case_tests.rs"]
mod tests;

#[derive(Debug)]
enum CaseFailure {
    Carrier(StringCaseOp),
    ValueExtent { required: u128 },
}
impl std::fmt::Display for CaseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(StringCaseOp::Lower) => f.write_str("lower expects string"),
            Self::Carrier(StringCaseOp::Upper) => {
                f.write_str("upper: argument must be a string array")
            }
            Self::ValueExtent { required } => write!(
                f,
                "Unicode case value requires {required} UTF-8 bytes, exceeding i32 offsets"
            ),
        }
    }
}
impl std::error::Error for CaseFailure {}
impl novarocks_failure_contract::FailureProjection for CaseFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let reason =
            |code| FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap());
        match self {
            Self::Carrier(_) => FailureFact::new(FailureClass::Invariant, reason(359)),
            Self::ValueExtent { required } => {
                FailureFact::new(FailureClass::Representation, reason(358)).with_representation(
                    RepresentationFacts {
                        boundary: RepresentationBoundary::MinimumUnit,
                        layout: RepresentationLayout {
                            domain: domain::KERNEL,
                            code: std::num::NonZeroU16::new(60).unwrap(),
                        },
                        required: u64::try_from(*required).ok(),
                        extent_limit: Some(i32::MAX as u64),
                        representable_prefix: None,
                    },
                )
            }
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CaseFailure {}
fn value_extent(required: u128) -> Result<usize, KernelFailure> {
    if required > i32::MAX as u128 {
        return Err(KernelFailure::from_domain(CaseFailure::ValueExtent {
            required,
        }));
    }
    Ok(required as usize)
}
