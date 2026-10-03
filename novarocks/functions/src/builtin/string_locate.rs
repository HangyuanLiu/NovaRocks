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

//! Selected original Unicode scalar positions using the borrowed std searcher.
//! Rust 1.92 str::find uses inline StrSearcher/TwoWaySearcher state, with no
//! dynamic scratch. The search remains opaque between original control checks;
//! this author does not promise a checkpoint within that standard-library call.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringLocateOp {
    Locate,
    Instr,
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
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
fn locate(
    haystack: &str,
    needle: &str,
    start: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<i32, KernelFailure> {
    if start <= 0 {
        work.step()?;
        return Ok(0);
    }
    let Ok(start_index) = usize::try_from(start) else {
        work.step()?;
        return Ok(0);
    };
    let target = start_index - 1;
    let mut characters = 0usize;
    let mut start_byte = None;
    for (ordinal, (byte, character)) in haystack.char_indices().enumerate() {
        if ordinal == target {
            start_byte = Some(byte);
        }
        characters += 1;
        observe_char(character, work)?;
    }
    if needle.is_empty() {
        let position = if start_index <= characters.max(1) {
            start
        } else {
            0
        };
        work.step()?;
        return i32::try_from(position)
            .map_err(|_| internal("locate empty-needle position exceeds Utf8 representation"));
    }
    if start_index > characters {
        work.step()?;
        return Ok(0);
    }
    let Some(start_byte) = start_byte else {
        work.step()?;
        return Ok(0);
    };
    // Same borrowed standard search as execution/string/locate_ops.rs. No
    // synthetic work loop substitutes for its opaque internal byte comparisons.
    work.flush()?;
    let relative = haystack[start_byte..].find(needle);
    work.flush()?;
    let Some(relative) = relative else {
        return Ok(0);
    };
    let absolute = start_byte + relative;
    let mut prefix_chars = 0usize;
    for character in haystack[..absolute].chars() {
        prefix_chars += 1;
        observe_char(character, work)?;
    }
    i32::try_from(prefix_chars + 1)
        .map_err(|_| internal("locate result exceeds Utf8 representation"))
}

pub(super) fn evaluate_string_locate<'a>(
    op: StringLocateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let args = input.arguments();
        let valid_arity = match op {
            StringLocateOp::Instr => types.len() == 2,
            StringLocateOp::Locate => matches!(types.len(), 2 | 3),
        };
        if !valid_arity || types.len() != args.len() {
            return Err(invalid(
                "locate/instr differs from its installed argument count",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("locate/instr requires checked values"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    }
            {
                return Err(invalid(
                    "locate/instr differs from its installed argument profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Int32
            || !target.nullable
        {
            return Err(invalid(
                "locate/instr differs from its installed result profile",
            ));
        }
        let (haystack_index, needle_index) = match op {
            StringLocateOp::Locate => (1, 0),
            StringLocateOp::Instr => (0, 1),
        };
        let haystacks = args[haystack_index]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("locate/instr haystack carrier is not Utf8"))?;
        let needles = args[needle_index]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("locate/instr needle carrier is not Utf8"))?;
        let starts = if args.len() == 3 {
            Some(
                args[2]
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| internal("locate start carrier is not Int64"))?,
            )
        } else {
            None
        };
        let selection = input.selection();
        output_capacity(selection.len())?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, arg) in args.iter().enumerate() {
                let row = arg.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= arg.array().len() {
                    return Err(internal("locate/instr selected row is out of bounds"));
                }
                if arg.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("locate/instr requires checked values"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "locate/instr non-null source contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                values.push(0);
                validity.append(false);
                has_null = true;
            } else {
                let start = starts.map_or(1, |array| array.value(rows[2]));
                values.push(locate(
                    haystacks.value(rows[haystack_index]),
                    needles.value(rows[needle_index]),
                    start,
                    &mut work,
                )?);
                validity.append(true);
            }
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Int32Array::new(
            values.into(),
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
#[path = "string_locate_tests.rs"]
mod tests;
