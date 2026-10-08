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
//! The original regexp_position row calculation, shared by v1 and selected calls.
use super::string_extended::{LegacyStringControl, StringCoreInput};
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    pattern_memo::PatternMemo,
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use regex::Regex;
use std::{alloc::Layout, sync::Arc};

fn strings(array: &ArrayRef) -> Result<Option<&StringArray>, String> {
    if array.data_type() == &DataType::Null {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(Some)
        .ok_or_else(|| "regexp_position expects string".to_string())
}
enum Positions<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Null,
}
impl<'a> Positions<'a> {
    fn from_array(array: &'a ArrayRef) -> Result<Self, String> {
        if array.data_type() == &DataType::Null {
            return Ok(Self::Null);
        }
        if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
            return Ok(Self::Int32(a));
        }
        if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(a));
        }
        Err("regexp_position expects int".to_string())
    }
    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Int32(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) as i64)
                }
            }
            Self::Int64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            Self::Null => None,
        }
    }
}
/// The v1 shell checks these two arguments before evaluating optional children.
pub fn validate_legacy_strings(source: &ArrayRef, pattern: &ArrayRef) -> Result<(), String> {
    strings(source)?;
    strings(pattern)?;
    Ok(())
}
/// Original optional raw Int32/Int64/Null admission, before the next child.
pub fn validate_legacy_position(array: &ArrayRef) -> Result<(), String> {
    Positions::from_array(array).map(|_| ())
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    if !(2..=4).contains(&types.len()) {
        return Err(invalid(
            "regexp_position requires its exact two, three or four checked arguments",
        ));
    }
    for (i, arg) in types.iter().enumerate() {
        step()?;
        let FunctionArgumentType::Value(ty) = arg else {
            return Err(invalid("regexp_position requires value arguments"));
        };
        let supported = if i < 2 {
            ty.data_type == DataType::Utf8 && ty.logical_type == ValueLogicalType::Physical
        } else {
            ty.data_type == DataType::Int64 && ty.logical_type == ValueLogicalType::Physical
        };
        if !supported {
            return Err(invalid(
                "regexp_position has no exact installed source profile",
            ));
        }
    }
    step()?;
    let target = contract.result_type();
    if target.data_type != DataType::Int32 || target.logical_type != ValueLogicalType::Physical {
        return Err(invalid(
            "regexp_position has no exact installed result profile",
        ));
    }
    Ok(())
}
fn chars_count(input: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<usize, KernelFailure> {
    let mut count = 0;
    for _ in input.chars() {
        work.step()?;
        count += 1;
    }
    Ok(count)
}
fn char_pos_to_byte_offset(
    input: &str,
    pos_1_based: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    if pos_1_based == 0 {
        return Ok(None);
    }
    if pos_1_based == 1 {
        return Ok(Some(0));
    }
    let target_zero_based = pos_1_based - 1;
    let len_chars = chars_count(input, work)?;
    if target_zero_based > len_chars {
        return Ok(None);
    }
    if target_zero_based == len_chars {
        return Ok(Some(input.len()));
    }
    for (ordinal, (idx, _)) in input.char_indices().enumerate() {
        work.step()?;
        if ordinal == target_zero_based {
            return Ok(Some(idx));
        }
    }
    Ok(None)
}
/// ONE original row body. A raw error is projected only at the caller boundary.
fn eval_row<'a>(
    input: &str,
    pattern: &'a str,
    start_pos: i64,
    occurrence: i64,
    patterns: &mut PatternMemo<'a, Regex, regex::Error>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<i64, String>, KernelFailure> {
    if start_pos <= 0 || occurrence <= 0 {
        return Ok(Ok(-1));
    }
    let start_pos = start_pos as usize;
    let occurrence = occurrence as usize;
    let len_chars = chars_count(input, work)?;
    if pattern.is_empty() {
        let max_pos = len_chars + 1;
        if start_pos > max_pos {
            return Ok(Ok(-1));
        }
        let target = start_pos + occurrence - 1;
        return Ok(Ok(if target <= max_pos { target as i64 } else { -1 }));
    }
    let start_byte = match char_pos_to_byte_offset(input, start_pos, work)? {
        Some(offset) => offset,
        None => return Ok(Ok(-1)),
    };
    for _ in pattern.as_bytes() {
        work.step()?;
    }
    work.flush()?;
    let compiled = patterns.get_or_compile(pattern, Regex::new);
    work.flush()?;
    let re = match compiled {
        Ok(re) => re,
        Err(e) => {
            work.flush()?;
            let message = format!("Invalid regex expression: {pattern}. Detail message: {e}");
            work.flush()?;
            return Ok(Err(message));
        }
    };
    let suffix = &input[start_byte..];
    // Regex searching remains the original opaque library operation.
    work.flush()?;
    let mut matches = re.find_iter(suffix);
    work.flush()?;
    let mut seen = 0usize;
    loop {
        work.flush()?;
        let next = matches.next();
        work.flush()?;
        let Some(matched) = next else {
            break;
        };
        seen += 1;
        work.step()?;
        if seen == occurrence {
            let byte_offset = start_byte + matched.start();
            return Ok(Ok((chars_count(&input[..byte_offset], work)? + 1) as i64));
        }
    }
    Ok(Ok(-1))
}
fn reserve<T>(
    len: usize,
    legacy: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<T>, KernelFailure> {
    work.flush()?;
    let values = if legacy {
        Vec::with_capacity(len)
    } else {
        Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut out = Vec::new();
        out.try_reserve_exact(len)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        out
    };
    work.flush()?;
    Ok(values)
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let checked = validate_profile(input.contract(), || work.step());
    work.finish_result(checked)?;
    evaluate_selected(
        StringCoreInput {
            arguments: input.arguments(),
            selection: input.selection(),
            error_boundary: None,
        },
        false,
        control,
    )
}
fn evaluate_selected<'a>(
    input: StringCoreInput<'_, 'a>,
    legacy: bool,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if !(2..=4).contains(&input.arguments.len()) {
            return Err(invalid(
                "regexp_position requires two, three or four evaluated arguments",
            ));
        }
        let source = strings(input.arguments[0].array()).map_err(|message| internal(&message))?;
        let pattern = strings(input.arguments[1].array()).map_err(|message| internal(&message))?;
        let start = input
            .arguments
            .get(2)
            .map(|a| Positions::from_array(a.array()))
            .transpose()
            .map_err(|message| internal(&message))?;
        let occurrence = input
            .arguments
            .get(3)
            .map(|a| Positions::from_array(a.array()))
            .transpose()
            .map_err(|message| internal(&message))?;
        let selection = input.selection;
        let mut patterns = PatternMemo::new();
        let mut out = reserve::<Option<i32>>(selection.len(), legacy, &mut work)?;
        let mut errors = reserve::<RowDataError>(0, legacy, &mut work)?;
        for (ordinal, batch) in selection.iter().enumerate() {
            let mut rows = [0usize; 4];
            for (i, arg) in input.arguments.iter().enumerate() {
                rows[i] = arg.value_row(ordinal, batch);
                work.step()?;
                if !legacy && rows[i] >= arg.array().len() {
                    return Err(internal(
                        "regexp_position selected argument row is out of bounds",
                    ));
                }
            }
            let Some(s) = source.filter(|s| !s.is_null(rows[0])) else {
                out.push(None);
                continue;
            };
            let Some(p) = pattern.filter(|p| !p.is_null(rows[1])) else {
                out.push(None);
                continue;
            };
            let Some(start) = start.as_ref().map_or(Some(1), |a| a.value(rows[2])) else {
                out.push(None);
                continue;
            };
            let Some(occurrence) = occurrence.as_ref().map_or(Some(1), |a| a.value(rows[3])) else {
                out.push(None);
                continue;
            };
            match eval_row(
                s.value(rows[0]),
                p.value(rows[1]),
                start,
                occurrence,
                &mut patterns,
                &mut work,
            )? {
                Ok(value) => out.push(Some(value as i32)),
                Err(message) => {
                    for _ in message.as_bytes() {
                        work.step()?;
                    }
                    if let Some(boundary) = input.error_boundary {
                        work.flush()?;
                        boundary(&message)?;
                        work.flush()?;
                    }
                    work.flush()?;
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(RowDataError::new(ordinal, &message));
                    work.flush()?;
                    out.push(None);
                }
            }
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Int32Array::from(out)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Int32,
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
/// The v1 shell has already evaluated and admitted arguments in original order.
pub fn evaluate_legacy(arrays: &[ArrayRef], rows: usize) -> Result<ArrayRef, String> {
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let raw = std::cell::RefCell::new(None);
    let boundary = |message: &str| {
        *raw.borrow_mut() = Some(message.to_string());
        Err(KernelFailure::InstanceFailed)
    };
    let result = evaluate_selected(
        StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(rows),
            error_boundary: Some(&boundary),
        },
        true,
        &LegacyStringControl,
    );
    if let Some(error) = raw.into_inner() {
        return Err(error);
    }
    result
        .map(|values| values.values().clone())
        .map_err(|e| e.to_string())
}
#[cfg(test)]
#[path = "regexp_position_tests.rs"]
mod tests;
