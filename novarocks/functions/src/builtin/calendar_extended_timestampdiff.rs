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

//! The original timestampdiff calendar-field and duration algorithms.

use super::{
    calendar_extended::DateInput,
    calendar_extended_shared::{CalendarDurationUnit, CalendarInput, calendar_duration_difference},
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use chrono::{Datelike, NaiveDateTime};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

const UNIT_ERROR: &str =
    "unit of timestampdiff must be one of year/month/week/day/hour/minute/second/millisecond";
fn timestampdiff_value(
    start: NaiveDateTime,
    end: NaiveDateTime,
    unit: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<i64, &'static str>, KernelFailure> {
    work.flush()?;
    let unit = unit.to_lowercase();
    work.flush()?;
    let value = match unit.as_str() {
        "year" => (end.year() - start.year()) as i64,
        "month" => {
            ((end.year() - start.year()) * 12 + (end.month() as i32 - start.month() as i32)) as i64
        }
        "week" => calendar_duration_difference(end, start, CalendarDurationUnit::Weeks),
        "day" => calendar_duration_difference(end, start, CalendarDurationUnit::Days),
        "hour" => calendar_duration_difference(end, start, CalendarDurationUnit::Hours),
        "minute" => calendar_duration_difference(end, start, CalendarDurationUnit::Minutes),
        "second" => calendar_duration_difference(end, start, CalendarDurationUnit::Seconds),
        "millisecond" => {
            calendar_duration_difference(end, start, CalendarDurationUnit::Milliseconds)
        }
        _ => return Ok(Err(UNIT_ERROR)),
    };
    work.step()?;
    Ok(Ok(value))
}
pub(super) fn evaluate_timestampdiff<'a>(
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(unit_type),
                FunctionArgumentType::Value(start_type),
                FunctionArgumentType::Value(end_type),
            ],
            [unit_arg, start_arg, end_arg],
        ) = (input.types, input.arguments())
        else {
            return Err(invalid(
                "timestampdiff requires its exact three value arguments",
            ));
        };
        let target = input.target;
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Int64
            || !target.nullable
            || unit_type.logical_type != ValueLogicalType::Physical
            || unit_type.data_type != DataType::Utf8
            || unit_arg.array().data_type() != &DataType::Utf8
            || (!input.legacy && start_type.data_type != end_type.data_type)
        {
            return Err(invalid(
                "timestampdiff differs from its exact declared profile",
            ));
        }
        let units = unit_arg
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("timestampdiff expects unit string"))?;
        let start = DateInput::for_input(start_type, start_arg.array().as_ref(), input)?;
        let end = DateInput::for_input(end_type, end_arg.array().as_ref(), input)?;
        let selection = input.selection();
        Layout::array::<Option<i64>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        Layout::array::<RowDataError>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let arguments = [unit_arg, start_arg, end_arg];
            let types = [unit_type, start_type, end_type];
            let mut rows = [0; 3];
            let mut null = false;
            for index in 0..3 {
                rows[index] = arguments[index].value_row(ordinal, batch_row);
                work.step()?;
                if rows[index] >= arguments[index].array().len() {
                    return Err(internal("timestampdiff selected row is out of bounds"));
                }
                let is_null = arguments[index].array().is_null(rows[index]);
                if is_null && !types[index].nullable {
                    return Err(internal("timestampdiff non-null source contains SQL NULL"));
                }
                null |= is_null;
                work.step()?;
            }
            let value = if null {
                None
            } else {
                let start = start.read(rows[1], &mut work)?;
                let end = end.read(rows[2], &mut work)?;
                match (start, end) {
                    (Some(start), Some(end)) => {
                        match timestampdiff_value(start, end, units.value(rows[0]), &mut work)? {
                            Ok(value) => Some(value),
                            Err(message) => {
                                work.flush()?;
                                errors.push(input.row_error(ordinal, message)?);
                                work.flush()?;
                                None
                            }
                        }
                    }
                    _ => None,
                }
            };
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            input.carrier,
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[cfg(test)]
mod legacy_goldens {
    use super::super::calendar_extended_shared::{CalendarOperation, evaluate_legacy_calendar};
    use super::*;

    #[test]
    fn timestampdiff_calendar_fields_do_not_count_elapsed_full_periods() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["year", "month", "second"])),
            Arc::new(StringArray::from(vec![
                "2023-12-31 23:59:59",
                "2024-01-31",
                "1970-01-01 00:00:00.999999",
            ])),
            Arc::new(StringArray::from(vec![
                "2024-01-01",
                "2024-02-01",
                "1970-01-01",
            ])),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::TimestampDiff,
            &arguments,
            &DataType::Int64,
            3,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(0)]
        );
    }

    #[test]
    fn timestampdiff_invalid_temporal_value_suppresses_the_original_unit_error() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["bad", "bad"])),
            Arc::new(StringArray::from(vec![Some("invalid"), None])),
            Arc::new(StringArray::from(vec!["1970-01-01", "1970-01-01"])),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::TimestampDiff,
            &arguments,
            &DataType::Int64,
            2,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None]
        );
    }
}
