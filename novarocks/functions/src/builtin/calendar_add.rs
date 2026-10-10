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

//! The original day/week shift expression, including its existing extreme-value panic order.
use super::{
    calendar_extended::{DateInput, timestamp_value_for_type},
    calendar_extended_shared::CalendarInput,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues, datetime_value,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Date32Array, Int64Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDateTime};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

pub fn shift_calendar_days(
    date: Option<NaiveDateTime>,
    days: Option<i64>,
    factor: i64,
) -> Option<NaiveDateTime> {
    let days = days?;
    // Keep multiplication outside Option::map, and Duration construction inside it.
    // Changing either boundary changes the original SQL NULL/extreme input behavior.
    let delta = days * factor;
    date.map(|date| date + Duration::days(delta))
}
pub(super) fn evaluate_day_shift<'a>(
    factor: i64,
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(date_type),
                FunctionArgumentType::Value(interval_type),
            ],
            [date_arg, interval_arg],
        ) = (input.types, input.arguments())
        else {
            return Err(invalid("day shift requires its exact two value arguments"));
        };
        let target = input.target;
        let expected_result = if date_type.data_type == DataType::Date32 {
            DataType::Date32
        } else {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        };
        if target.logical_type != ValueLogicalType::Physical
            || !target.nullable
            || interval_type.logical_type != ValueLogicalType::Physical
            || interval_type.data_type != DataType::Int64
            || interval_arg.array().data_type() != &DataType::Int64
            || (!input.legacy && target.data_type != expected_result)
        {
            return Err(invalid("day shift differs from its exact declared profile"));
        }
        let dates = DateInput::for_input(date_type, date_arg.array().as_ref(), input)?;
        let intervals = interval_arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("date_add expects int"))?;
        let selection = input.selection();
        Layout::array::<Option<NaiveDateTime>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let date_row = date_arg.value_row(ordinal, batch_row);
            let interval_row = interval_arg.value_row(ordinal, batch_row);
            work.step()?;
            if date_row >= date_arg.array().len() || interval_row >= intervals.len() {
                return Err(internal("day shift selected row is out of bounds"));
            }
            let date_null = date_arg.array().is_null(date_row);
            let interval_null = intervals.is_null(interval_row);
            if (date_null && !date_type.nullable) || (interval_null && !interval_type.nullable) {
                return Err(internal("day shift non-null source contains SQL NULL"));
            }
            work.step()?;
            let date = if date_null {
                None
            } else {
                dates.read(date_row, &mut work)?
            };
            let interval = if interval_null {
                None
            } else {
                Some(intervals.value(interval_row))
            };
            work.flush()?;
            let shifted = shift_calendar_days(date, interval, factor);
            work.flush()?;
            values.push(shifted);
            work.step()?;
        }
        work.flush()?;
        let array = if *input.carrier == DataType::Date32 {
            let mut projected = Vec::new();
            projected
                .try_reserve_exact(values.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for value in values {
                projected.push(value.map(|date| {
                    date.date().num_days_from_ce() - datetime_value::UNIX_EPOCH_DAY_OFFSET
                }));
                work.step()?;
            }
            work.flush()?;
            Arc::new(Date32Array::from(projected)) as ArrayRef
        } else {
            let mut projected = Vec::new();
            projected
                .try_reserve_exact(values.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for value in values {
                projected.push(
                    value.and_then(|date| timestamp_value_for_type(date, &target.data_type).ok()),
                );
                work.step()?;
            }
            work.flush()?;
            Arc::new(TimestampMicrosecondArray::from(projected)) as ArrayRef
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            input.carrier,
            array,
            Box::new([]),
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
    fn legacy_days_null_boundary_keeps_duration_construction_inside_the_map() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![None])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::DaysShift(1),
            &arguments,
            &DataType::Date32,
            1,
        )
        .unwrap();
        assert!(output.is_null(0));
        // This original panic is a future semantic decision, not an owner no-panic claim.
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        ];
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| evaluate_legacy_calendar(
                CalendarOperation::DaysShift(1),
                &arguments,
                &DataType::Date32,
                1
            )))
            .is_err()
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    fn legacy_day_factor_overflow_occurs_even_when_the_date_is_null() {
        for (factor, interval) in [(7, i64::MAX), (-7, i64::MAX), (-1, i64::MIN)] {
            let arguments: Vec<ArrayRef> = vec![
                Arc::new(Date32Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![interval])),
            ];
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || evaluate_legacy_calendar(
                        CalendarOperation::DaysShift(factor),
                        &arguments,
                        &DataType::Date32,
                        1
                    )
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn legacy_day_family_broadcasts_each_one_row_child_and_keeps_date32() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(-1)])),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::DaysShift(1),
            &arguments,
            &DataType::Date32,
            3,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), None, Some(-1)]
        );
    }

    #[test]
    fn legacy_day_family_keeps_requested_seconds_in_the_microsecond_carrier() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(arrow_array::StringArray::from(vec!["1970-01-01 00:00:10"])),
            Arc::new(Int64Array::from(vec![1, -1])),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::DaysShift(1),
            &arguments,
            &DataType::Timestamp(TimeUnit::Second, None),
            2,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(86410), Some(-86390)]
        );
    }
}
