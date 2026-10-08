// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//! The original saturated TIME formatter and one selected row loop.
//! Formatting and Arrow construction are opaque allocation boundaries. Capacity
//! checks prove representation only; they do not establish a scratch grant.
use super::calendar_extended_shared::CalendarInput;
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc, time::Duration};
const SEC_TO_TIME_CAP_SECONDS: i64 = 839 * 3600 + 59 * 60 + 59;
pub fn format_sec_to_time(seconds: i64) -> String {
    let clamped = seconds.clamp(-SEC_TO_TIME_CAP_SECONDS, SEC_TO_TIME_CAP_SECONDS);
    let sign = if clamped < 0 { "-" } else { "" };
    let abs = clamped.abs();
    let hour = abs / 3600;
    let minute = (abs % 3600) / 60;
    let second = abs % 60;
    format!("{sign}{hour:02}:{minute:02}:{second:02}")
}

pub(super) fn evaluate_sec_to_time<'a>(
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (input.types, input.arguments())
        else {
            return Err(invalid(
                "sec_to_time requires its exact single value argument",
            ));
        };
        let target = input.target;
        if source.logical_type != ValueLogicalType::Physical
            || source.data_type != DataType::Int64
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "sec_to_time differs from its exact Int64 to nullable Utf8 profile",
            ));
        }
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("sec_to_time selected carrier is not Int64"))?;
        let selection = input.selection();
        work.flush()?;
        let mut out = if input.legacy {
            // Keep the original raw allocation and Arrow offset failure boundary.
            // Selected capacity checks must not impose a new v1 row limit.
            Vec::with_capacity(selection.len())
        } else {
            Layout::array::<Option<String>>(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            let bytes = selection
                .len()
                .checked_mul(10)
                .ok_or(KernelFailure::ResourceExhausted)?;
            i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut out = Vec::new();
            out.try_reserve_exact(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            out
        };
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= values.len() {
                return Err(internal("sec_to_time selected row is out of bounds"));
            }
            let value = if values.is_null(row) {
                if !source.nullable {
                    return Err(internal("sec_to_time non-null source contains SQL NULL"));
                }
                None
            } else {
                work.flush()?;
                let value = format_sec_to_time(values.value(row));
                work.flush()?;
                Some(value)
            };
            out.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(StringArray::from(out)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        unreachable!("sec_to_time never waits")
    }
}
/// Preserve the raw v1 admission diagnostic and actual output carrier.
/// The arena shell still owns child evaluation and its original indexing panic.
pub fn evaluate_legacy_sec_to_time(array: &ArrayRef) -> Result<ArrayRef, String> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "sec_to_time expects int".to_string())?;
    let types = [FunctionArgumentType::Value(FunctionValueType::new(
        DataType::Int64,
        true,
    ))];
    let args = [EvaluatedArgument::Column(array)];
    let target = FunctionValueType::new(DataType::Utf8, true);
    let input = CalendarInput {
        types: &types,
        carrier: &DataType::Utf8,
        target: &target,
        arguments: &args,
        selected: Selection::all(array.len()),
        row_error_boundary: None,
        carrier_error_boundary: None,
        legacy: true,
    };
    evaluate_sec_to_time(input, &LegacyControl)
        .map(|values| values.values().clone())
        .map_err(|error| error.to_string())
}
#[cfg(test)]
#[path = "calendar_sec_to_time_tests.rs"]
mod tests;
