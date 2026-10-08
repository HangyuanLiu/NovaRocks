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

//! The original duration shift expression, including construction before the date NULL mask.
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
use chrono::{Duration, NaiveDateTime};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

/// A frozen unit and direction; no clock, timezone or implicit session default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CalendarDurationShift {
    SecondsAdd,
    SecondsSub,
    MinutesAdd,
    MinutesSub,
    HoursAdd,
    HoursSub,
    MillisecondsAdd,
    MillisecondsSub,
    MicrosecondsAdd,
    MicrosecondsSub,
}
impl CalendarDurationShift {
    fn duration(self, interval: i64) -> Duration {
        match self {
            Self::SecondsAdd => Duration::seconds(interval),
            Self::SecondsSub => Duration::seconds(-interval),
            Self::MinutesAdd => Duration::minutes(interval),
            Self::MinutesSub => Duration::minutes(-interval),
            Self::HoursAdd => Duration::hours(interval),
            Self::HoursSub => Duration::hours(-interval),
            Self::MillisecondsAdd => Duration::milliseconds(interval),
            Self::MillisecondsSub => Duration::milliseconds(-interval),
            Self::MicrosecondsAdd => Duration::microseconds(interval),
            Self::MicrosecondsSub => Duration::microseconds(-interval),
        }
    }
}
fn shift_calendar_duration(
    date: Option<NaiveDateTime>,
    interval: Option<i64>,
    operation: CalendarDurationShift,
) -> Option<NaiveDateTime> {
    let interval = interval?;
    // Preserve construction/negation even when the date is NULL or cannot be parsed.
    let duration = operation.duration(interval);
    date.map(|date| date + duration)
}
pub(super) fn evaluate_duration_shift<'a>(
    operation: CalendarDurationShift,
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
                "duration shift requires its exact two value arguments",
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
                "duration shift differs from its exact declared profile",
            ));
        }
        let dates = DateInput::for_input(date_type, date_arg.array().as_ref(), input)?;
        let intervals = interval_arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("duration add expects int"))?;
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
                return Err(internal("duration shift selected row is out of bounds"));
            }
            let date_null = date_arg.array().is_null(date_row);
            let interval_null = intervals.is_null(interval_row);
            if (date_null && !date_type.nullable) || (interval_null && !interval_type.nullable) {
                return Err(internal("duration shift non-null source contains SQL NULL"));
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
            let shifted = shift_calendar_duration(date, interval, operation);
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
    fn duration_constructor_runs_before_the_date_null_boundary() {
        assert!(
            std::panic::catch_unwind(|| shift_calendar_duration(
                None,
                Some(i64::MAX),
                CalendarDurationShift::SecondsAdd
            ))
            .is_err()
        );
        assert_eq!(
            shift_calendar_duration(None, Some(i64::MAX), CalendarDurationShift::MicrosecondsAdd),
            None
        );
        assert_eq!(
            shift_calendar_duration(None, None, CalendarDurationShift::HoursAdd),
            None
        );
    }
    #[test]
    #[cfg(debug_assertions)]
    fn duration_subtraction_negation_runs_before_the_date_null_boundary() {
        assert!(
            std::panic::catch_unwind(|| shift_calendar_duration(
                None,
                Some(i64::MIN),
                CalendarDurationShift::MicrosecondsSub
            ))
            .is_err()
        );
    }
    #[test]
    fn duration_microseconds_keep_fractional_precision() {
        let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert_eq!(
            shift_calendar_duration(Some(date), Some(1), CalendarDurationShift::MicrosecondsSub)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
            -1
        );
    }
}
