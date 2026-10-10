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

//! The original month/year shift expression, including narrowing, clipping and panic order.
use super::{
    calendar_extended::{DateInput, timestamp_value_for_type},
    calendar_extended_shared::CalendarInput,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn add_months_to_date(date: NaiveDate, months: i32) -> NaiveDate {
    let mut year = date.year();
    let mut month = date.month() as i32 - 1 + months;
    year += month.div_euclid(12);
    month = month.rem_euclid(12) + 1;
    let last_day = last_day_of_month(year, month as u32);
    let day = date.day().min(last_day);
    NaiveDate::from_ymd_opt(year, month as u32, day).unwrap()
}

pub(super) fn add_months_to_datetime(dt: NaiveDateTime, months: i32) -> NaiveDateTime {
    let date = add_months_to_date(dt.date(), months);
    date.and_time(dt.time())
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
    (first_next - Duration::days(1)).day()
}

fn shift_calendar_months(
    date: Option<NaiveDateTime>,
    months: Option<i64>,
    factor: i32,
) -> Option<NaiveDateTime> {
    let months = months?;
    // Original narrowing and factor multiplication precede the date Option::map.
    let months = months as i32 * factor;
    date.map(|date| add_months_to_datetime(date, months))
}
pub(super) fn evaluate_month_shift<'a>(
    factor: i32,
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
            return Err(invalid(
                "month shift requires its exact two value arguments",
            ));
        };
        let target = input.target;
        if target.logical_type != ValueLogicalType::Physical
            || !target.nullable
            || interval_type.logical_type != ValueLogicalType::Physical
            || interval_type.data_type != DataType::Int64
            || interval_arg.array().data_type() != &DataType::Int64
            || (!input.legacy
                && target.data_type != DataType::Timestamp(TimeUnit::Microsecond, None))
        {
            return Err(invalid(
                "month shift differs from its exact declared profile",
            ));
        }
        let dates = DateInput::for_input(date_type, date_arg.array().as_ref(), input)?;
        let intervals = interval_arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("add_months expects int"))?;
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
                return Err(internal("month shift selected row is out of bounds"));
            }
            let date_null = date_arg.array().is_null(date_row);
            let interval_null = intervals.is_null(interval_row);
            if (date_null && !date_type.nullable) || (interval_null && !interval_type.nullable) {
                return Err(internal("month shift non-null source contains SQL NULL"));
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
            let shifted = shift_calendar_months(date, interval, factor);
            work.flush()?;
            values.push(shifted);
            work.step()?;
        }
        work.flush()?;
        let mut projected = Vec::new();
        projected
            .try_reserve_exact(values.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for value in values {
            // A legacy Date32 requested result intentionally projects NULL into its
            // historical TimestampUS carrier; the owner admits only exact TimestampUS.
            projected.push(
                value.and_then(|date| timestamp_value_for_type(date, &target.data_type).ok()),
            );
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(TimestampMicrosecondArray::from(projected)) as ArrayRef;
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
    use super::*;
    #[test]
    fn month_core_clips_end_of_month_and_preserves_time() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 31)
            .unwrap()
            .and_hms_micro_opt(12, 34, 56, 123456)
            .unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap()
            .and_time(start.time());
        assert_eq!(
            shift_calendar_months(Some(start), Some(1), 1),
            Some(expected)
        );
        assert_eq!(
            shift_calendar_months(Some(start), Some(1_i64 << 32), 1),
            Some(start)
        );
        assert_eq!(shift_calendar_months(None, Some(i64::MAX), 12), None);
    }
    #[test]
    #[cfg(debug_assertions)]
    fn month_factor_overflow_precedes_null_date_mask() {
        assert!(
            std::panic::catch_unwind(|| shift_calendar_months(None, Some(i32::MIN as i64), -1))
                .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| shift_calendar_months(None, Some(i32::MAX as i64), 12))
                .is_err()
        );
    }
    #[test]
    fn month_calendar_range_unwrap_remains_the_original_panic() {
        let start = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert!(
            std::panic::catch_unwind(|| shift_calendar_months(Some(start), Some(10_000_000), 1))
                .is_err()
        );
    }
}

#[cfg(test)]
mod profile_rejection_tests {
    use super::super::calendar_extended_owner::prepared_for_test_with_policy;
    use crate::FunctionValueType;
    use arrow_schema::DataType;
    use novarocks_type_contract::DecimalOverflowPolicy;
    #[test]
    fn declared_date32_month_aliases_refuse_legacy_timestamp_carrier_drift() {
        for name in ["months_add", "months_sub", "years_add", "years_sub"] {
            let types = [
                FunctionValueType::new(DataType::Date32, true),
                FunctionValueType::new(DataType::Int64, true),
            ];
            let error = match prepared_for_test_with_policy(
                name,
                &types,
                DecimalOverflowPolicy::OutputNull,
            ) {
                Ok(_) => panic!("{name} must reject legacy carrier drift"),
                Err(error) => error.to_string(),
            };
            assert!(
                error.contains(&format!(
                    "builtin.scalar/{name}/v1 rejects declared Date32 result"
                )),
                "{error}"
            );
        }
    }
}
