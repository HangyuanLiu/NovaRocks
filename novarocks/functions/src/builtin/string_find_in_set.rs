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

//! Selected literal comma-separated membership without temporary splits/strings.
//! Borrowed byte scans preserve empty entries and first exact match. Layout
//! bounds are representability checks rather than formal host allocation grants.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

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
fn equal_part(
    part: &[u8],
    target: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    let same_length = part.len() == target.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (left, right) in part.iter().zip(target) {
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn find_in_set(
    target: &str,
    set: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<i32, KernelFailure> {
    for byte in target.bytes() {
        let comma = byte == b',';
        work.step()?;
        if comma {
            return Ok(0);
        }
    }
    let mut start = 0usize;
    let mut position = 1usize;
    for (byte, value) in set.bytes().enumerate() {
        let comma = value == b',';
        work.step()?;
        if comma {
            if equal_part(&set.as_bytes()[start..byte], target.as_bytes(), work)? {
                return checked_position(position);
            }
            start = byte + 1;
            position += 1;
            work.step()?;
        }
    }
    if equal_part(&set.as_bytes()[start..], target.as_bytes(), work)? {
        checked_position(position)
    } else {
        Ok(0)
    }
}

fn checked_position(position: usize) -> Result<i32, KernelFailure> {
    // A nonempty matching target consumes at least one source byte, bounding
    // its ordinal by Utf8's i32 byte extent. For an empty target, preceding
    // nonempty entries require a byte plus a comma each; the first empty match
    // is therefore also representable. No legal source needs truncating as i32.
    i32::try_from(position)
        .map_err(|_| internal("find_in_set match exceeds the Utf8 position domain"))
}

pub(super) fn evaluate_string_find_in_set<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let args = input.arguments();
        if types.len() != 2 || args.len() != 2 {
            return Err(invalid(
                "find_in_set differs from its installed argument count",
            ));
        }
        for ty in types.iter() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("find_in_set requires checked values"));
            };
            if ty.logical_type != ValueLogicalType::Physical || ty.data_type != DataType::Utf8 {
                return Err(invalid(
                    "find_in_set differs from its installed argument profile",
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
                "find_in_set differs from its installed result profile",
            ));
        }
        let targets = args[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("find_in_set target is not Utf8"))?;
        let sets = args[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("find_in_set set is not Utf8"))?;
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
            let mut rows = [0usize; 2];
            let mut is_null = false;
            for (index, arg) in args.iter().enumerate() {
                let row = arg.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= arg.array().len() {
                    return Err(internal("find_in_set selected row is out of bounds"));
                }
                if arg.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("find_in_set requires checked values"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "find_in_set non-null source contains selected SQL NULL",
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
                values.push(find_in_set(
                    targets.value(rows[0]),
                    sets.value(rows[1]),
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
#[path = "string_find_in_set_tests.rs"]
mod tests;
