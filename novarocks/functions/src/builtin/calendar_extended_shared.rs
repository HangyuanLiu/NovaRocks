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

//! One selected calendar computation used by canonical owners and legacy arena shells.
//! Legacy projection preserves its existing raw carrier and diagnostic surface.

use super::{
    calendar_extended::{self, CalendarExtendedOp},
    calendar_extended_parse::CalendarParseOp,
};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, Selection,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Copy)]
pub(super) struct CalendarInput<'call, 'batch> {
    pub types: &'call [FunctionArgumentType],
    pub carrier: &'call DataType,
    pub target: &'call FunctionValueType,
    pub arguments: &'batch [EvaluatedArgument<'batch>],
    pub selected: Selection<'batch>,
    pub row_error_boundary: Option<&'call dyn Fn(usize, &str) -> Result<(), KernelFailure>>,
    pub legacy: bool,
}
impl<'call, 'batch> CalendarInput<'call, 'batch> {
    pub fn arguments(self) -> &'batch [EvaluatedArgument<'batch>] {
        self.arguments
    }
    pub fn selection(self) -> Selection<'batch> {
        self.selected
    }
    pub fn row_error(
        self,
        ordinal: usize,
        message: &str,
    ) -> Result<crate::RowDataError, KernelFailure> {
        if let Some(boundary) = self.row_error_boundary {
            boundary(ordinal, message)?;
        }
        Ok(crate::RowDataError::new(ordinal, message))
    }
    pub fn owner(input: ScalarCallInput<'call, 'batch>) -> Self {
        Self {
            types: &input.contract().selected().argument_types,
            target: input.contract().result_type(),
            carrier: &input.contract().result_type().data_type,
            arguments: input.arguments(),
            selected: input.selection(),
            legacy: false,
            row_error_boundary: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum CalendarOperation {
    Trunc,
    Timestamp,
    WeeksDiff,
    HoursDiff,
    MinutesDiff,
    SecondsDiff,
    DateFormat,
    StrToDate,
    LastDay,
    NextDay,
    PreviousDay,
    FromDays,
}
impl CalendarOperation {
    fn operation(self) -> CalendarExtendedOp {
        match self {
            Self::Trunc => CalendarExtendedOp::Trunc,
            Self::Timestamp => CalendarExtendedOp::Timestamp,
            Self::WeeksDiff => CalendarExtendedOp::WeeksDiff,
            Self::HoursDiff => CalendarExtendedOp::HoursDiff,
            Self::MinutesDiff => CalendarExtendedOp::MinutesDiff,
            Self::SecondsDiff => CalendarExtendedOp::SecondsDiff,
            Self::DateFormat => CalendarExtendedOp::DateFormat,
            Self::StrToDate => CalendarExtendedOp::Parse(CalendarParseOp::StrToDate),
            Self::LastDay => CalendarExtendedOp::Parse(CalendarParseOp::LastDay),
            Self::NextDay => CalendarExtendedOp::Parse(CalendarParseOp::NextDay),
            Self::PreviousDay => CalendarExtendedOp::Parse(CalendarParseOp::PreviousDay),
            Self::FromDays => CalendarExtendedOp::Parse(CalendarParseOp::FromDays),
        }
    }
}
/// The original duration rounding is shared with the remaining legacy date_diff callers.
#[derive(Clone, Copy)]
pub enum CalendarDurationUnit {
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
}
pub fn calendar_duration_difference(
    left: chrono::NaiveDateTime,
    right: chrono::NaiveDateTime,
    unit: CalendarDurationUnit,
) -> i64 {
    let difference = left - right;
    match unit {
        CalendarDurationUnit::Milliseconds => difference.num_milliseconds(),
        CalendarDurationUnit::Seconds => difference.num_seconds(),
        CalendarDurationUnit::Minutes => difference.num_minutes(),
        CalendarDurationUnit::Hours => difference.num_hours(),
        CalendarDurationUnit::Days => difference.num_days(),
        CalendarDurationUnit::Weeks => difference.num_weeks(),
    }
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        unreachable!("calendar kernels do not wait")
    }
}
/// An arena shell evaluates children and explicitly selects all of its original rows.
/// No function lookup, overload election or session authority is performed here.
pub fn evaluate_legacy_calendar(
    operation: CalendarOperation,
    arguments: &[ArrayRef],
    output_type: &DataType,
    rows: usize,
) -> Result<ArrayRef, String> {
    let types: Vec<_> = arguments
        .iter()
        .map(|array| {
            FunctionArgumentType::Value(FunctionValueType::new(array.data_type().clone(), true))
        })
        .collect();
    let arguments_view: Vec<_> = arguments.iter().map(EvaluatedArgument::Column).collect();
    // A short legacy array is an invalid caller and keeps the old indexing panic
    // at the adapter boundary. The selected core retains its own bounds checks.
    for argument in arguments {
        assert!(argument.len() >= rows, "legacy calendar row out of bounds");
    }
    let target = FunctionValueType::new(output_type.clone(), true);
    let carrier = if matches!(operation, CalendarOperation::Timestamp)
        || (matches!(
            operation,
            CalendarOperation::Trunc | CalendarOperation::StrToDate
        ) && *output_type != DataType::Date32)
    {
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    } else {
        output_type.clone()
    };
    // Legacy errors terminate its batch before a later row can compute or panic.
    // This callback projects the same row failure without choosing another algorithm.
    let row_failure = std::cell::RefCell::new(None);
    let row_error_boundary = |ordinal: usize, message: &str| {
        let message = if matches!(
            operation,
            CalendarOperation::NextDay | CalendarOperation::PreviousDay
        ) {
            let tokens = arguments[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| crate::kernel_control::invalid("next_day expects string"))?;
            format!(
                "{} not supported in {} dow_string backend",
                tokens.value(ordinal),
                if matches!(operation, CalendarOperation::NextDay) {
                    "next_day"
                } else {
                    "previous_day"
                }
            )
        } else {
            message.to_string()
        };
        *row_failure.borrow_mut() = Some(message);
        Err(crate::kernel_control::internal(
            "legacy calendar row failure",
        ))
    };
    let output = calendar_extended::evaluate_calendar_input(
        operation.operation(),
        CalendarInput {
            types: &types,
            target: &target,
            carrier: &carrier,
            arguments: &arguments_view,
            selected: Selection::all(rows),
            legacy: true,
            row_error_boundary: Some(&row_error_boundary),
        },
        &LegacyControl,
    )
    .map_err(|error| {
        row_failure
            .take()
            .unwrap_or_else(|| legacy_kernel_error(error))
    })?;
    Ok(Arc::clone(output.values()))
}

fn legacy_kernel_error(error: KernelFailure) -> String {
    match error {
        KernelFailure::InvalidProgram(message)
        | KernelFailure::Internal(message)
        | KernelFailure::Operational(message) => message.message().to_string(),
        other => other.to_string(),
    }
}
pub fn legacy_extract_datetimes(
    array: &ArrayRef,
) -> Result<Vec<Option<chrono::NaiveDateTime>>, String> {
    let reader = calendar_extended::DateInput::raw(array.as_ref()).map_err(legacy_kernel_error)?;
    let mut work = crate::kernel_input::EvaluationCheckpoints::new(&LegacyControl);
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            None
        } else {
            reader.read(row, &mut work).map_err(legacy_kernel_error)?
        };
        values.push(value);
    }
    Ok(values)
}
pub fn legacy_extract_dates(array: &ArrayRef) -> Result<Vec<Option<chrono::NaiveDate>>, String> {
    if !matches!(
        array.data_type(),
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
    ) {
        return Err(format!(
            "unsupported date input type: {:?}",
            array.data_type()
        ));
    }
    Ok(legacy_extract_datetimes(array)?
        .into_iter()
        .map(|date| date.map(|date| date.date()))
        .collect())
}
pub fn legacy_to_timestamp_value(
    date: chrono::NaiveDateTime,
    output: &DataType,
) -> Result<i64, String> {
    calendar_extended::timestamp_value_for_type(date, output)
}
pub fn legacy_mysql_format_to_chrono(format: &str) -> String {
    super::calendar_extended_format::mysql_format_to_chrono(
        format,
        &mut crate::kernel_input::EvaluationCheckpoints::new(&LegacyControl),
    )
    .expect("legacy calendar control never refuses")
}

pub const FROM_DAYS_MAX_VALID: i64 = super::calendar_extended_parse::FROM_DAYS_MAX_VALID;
pub fn calendar_from_days_value(days: i64) -> Option<i32> {
    super::calendar_extended_parse::from_days_value(days)
}
pub fn calendar_zero_date_sentinel_date32() -> i32 {
    super::calendar_extended_parse::zero_date_sentinel_date32()
}
pub fn calendar_date_from_julian(julian: i32) -> Option<chrono::NaiveDate> {
    super::calendar_extended_parse::date_from_julian(julian)
}

#[cfg(test)]
mod legacy_projection_tests {
    use super::*;
    use arrow_array::{
        Date32Array, FixedSizeBinaryArray, Int64Array, TimestampMicrosecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::TimeUnit;
    use chrono::Datelike;

    #[test]
    fn raw_timestamp_units_keep_requested_values_in_the_legacy_microsecond_carrier() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["second", "second"])),
            Arc::new(
                TimestampNanosecondArray::from(vec![-1, 1_234_567_890]).with_timezone("+08:00"),
            ),
        ];
        let requested = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        let output =
            evaluate_legacy_calendar(CalendarOperation::Trunc, &inputs, &requested, 2).unwrap();
        assert_eq!(
            output.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-1), Some(1)]
        );
        let output =
            evaluate_legacy_calendar(CalendarOperation::Trunc, &inputs, &DataType::Int64, 2)
                .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None]
        );
    }

    #[test]
    fn raw_largeint_numeric_dates_and_mixed_duration_inputs_keep_the_original_domains() {
        let values = [20240102_i128, i128::MAX];
        let dates: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.to_be_bytes()))
                .unwrap(),
        );
        let units: ArrayRef = Arc::new(StringArray::from(vec!["day", "day"]));
        let output = evaluate_legacy_calendar(
            CalendarOperation::Trunc,
            &[units, dates],
            &DataType::Date32,
            2,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(19724), None]
        );
        let left: ArrayRef =
            Arc::new(TimestampSecondArray::from(vec![709_201, -1]).with_timezone("UTC"));
        let right: ArrayRef = Arc::new(StringArray::from(vec![
            "1970-01-01",
            "1970-01-01 00:00:00.999999",
        ]));
        for (op, expected) in [
            (CalendarOperation::WeeksDiff, vec![Some(1), Some(0)]),
            (CalendarOperation::HoursDiff, vec![Some(197), Some(0)]),
            (CalendarOperation::MinutesDiff, vec![Some(11820), Some(0)]),
            (CalendarOperation::SecondsDiff, vec![Some(709201), Some(-1)]),
        ] {
            let output = evaluate_legacy_calendar(
                op,
                &[Arc::clone(&left), Arc::clone(&right)],
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
                expected
            );
        }
    }

    #[test]
    fn str_to_date_projects_the_single_parse_result_with_its_original_clock_and_carrier() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["1970-01-01 00:01:30", "bad"])),
            Arc::new(StringArray::from(vec!["%Y-%m-%d %H:%i:%s", "%Y-%m-%d"])),
        ];
        let output =
            evaluate_legacy_calendar(CalendarOperation::StrToDate, &inputs, &DataType::Date32, 2)
                .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), None]
        );
        let output = evaluate_legacy_calendar(
            CalendarOperation::StrToDate,
            &inputs,
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
            vec![Some(90), None]
        );
    }

    #[test]
    fn named_day_and_format_keep_raw_timezone_input_and_diagnostics() {
        let dates: ArrayRef = Arc::new(TimestampSecondArray::from(vec![0]).with_timezone("+08:00"));
        let token: ArrayRef = Arc::new(StringArray::from(vec!["Monday"]));
        let output = evaluate_legacy_calendar(
            CalendarOperation::NextDay,
            &[Arc::clone(&dates), token],
            &DataType::Date32,
            1,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            4
        );
        let token: ArrayRef = Arc::new(StringArray::from(vec!["monday"]));
        assert_eq!(
            evaluate_legacy_calendar(
                CalendarOperation::PreviousDay,
                &[Arc::clone(&dates), token],
                &DataType::Date32,
                1
            )
            .unwrap_err(),
            "monday not supported in previous_day dow_string backend"
        );
        let format: ArrayRef = Arc::new(StringArray::from(vec!["%Y-%m-%d %H:%i:%s.%f%"]));
        let output = evaluate_legacy_calendar(
            CalendarOperation::DateFormat,
            &[dates, format],
            &DataType::Utf8,
            1,
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1970-01-01 00:00:00.000000"
        );
    }

    #[test]
    fn legacy_row_failure_precedes_a_later_existing_extreme_week_panic() {
        let minimum = chrono::NaiveDate::MIN.num_days_from_ce()
            - crate::datetime_value::UNIX_EPOCH_DAY_OFFSET;
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["bad", "week"])),
            Arc::new(Date32Array::from(vec![0, minimum])),
        ];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            evaluate_legacy_calendar(CalendarOperation::Trunc, &inputs, &DataType::Date32, 2)
        }));
        assert_eq!(
            result.unwrap().unwrap_err(),
            "format value must in {microsecond, millisecond, second, minute, hour, day, month, year, week, quarter}"
        );
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["week"])),
            Arc::new(Date32Array::from(vec![minimum])),
        ];
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| evaluate_legacy_calendar(
                CalendarOperation::Trunc,
                &inputs,
                &DataType::Date32,
                1
            )))
            .is_err()
        );
    }
}
