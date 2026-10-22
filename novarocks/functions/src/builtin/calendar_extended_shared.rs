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

pub use super::calendar_duration::CalendarDurationShift;

use super::{
    calendar_extended::{self, CalendarCarrierError, CalendarExtendedOp},
    calendar_extended_parse::CalendarParseOp,
};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, Selection,
};
#[cfg(test)]
use arrow_array::StringArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::{sync::Arc, time::Duration};

/// A raw required call may stop with its actual row error. Selected execution
/// instantiates the same computation with Infallible and keeps row errors in
/// SelectedValues, so a row error cannot enter its outer KernelFailure channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CalendarFailure<E> {
    Outer(KernelFailure),
    Row(E),
}
pub type RawCalendarFailure = CalendarFailure<crate::RowDataError>;
impl<E> From<KernelFailure> for CalendarFailure<E> {
    fn from(source: KernelFailure) -> Self {
        Self::Outer(source)
    }
}
impl CalendarFailure<std::convert::Infallible> {
    pub(super) fn into_kernel(self) -> KernelFailure {
        match self {
            Self::Outer(source) => source,
            Self::Row(never) => match never {},
        }
    }
}
impl<E: std::fmt::Display> std::fmt::Display for CalendarFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Outer(source) => source.fmt(f),
            Self::Row(source) => source.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for CalendarFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Outer(source) => Some(source),
            Self::Row(source) => Some(source),
        }
    }
}
impl<E: novarocks_failure_contract::FailureProjection> novarocks_failure_contract::FailureProjection
    for CalendarFailure<E>
{
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Outer(source) => source.failure_fact(),
            Self::Row(source) => source.failure_fact(),
        }
    }
}
pub(super) fn finish_calendar_result<T, E>(
    work: crate::kernel_input::EvaluationCheckpoints<'_>,
    result: Result<T, CalendarFailure<E>>,
) -> Result<T, CalendarFailure<E>> {
    match result {
        // The raw required row failed at this point. It has no diagnostic
        // journal/sentinel and no optional callback after its first cause.
        Err(CalendarFailure::Row(source)) => Err(CalendarFailure::Row(source)),
        Err(CalendarFailure::Outer(source)) => work
            .finish_result(Err(source))
            .map_err(CalendarFailure::Outer),
        Ok(output) => work
            .finish_result(Ok(output))
            .map_err(CalendarFailure::Outer),
    }
}
pub(super) type CalendarRowErrorPort<'call, E> =
    Option<&'call dyn Fn(usize, &str) -> Result<crate::RowDataError, E>>;

pub(super) struct CalendarInput<'call, 'batch, E = std::convert::Infallible> {
    pub types: &'call [FunctionArgumentType],
    pub carrier: &'call DataType,
    pub target: &'call FunctionValueType,
    pub arguments: &'batch [EvaluatedArgument<'batch>],
    pub selected: Selection<'batch>,
    pub row_error_port: CalendarRowErrorPort<'call, E>,
    pub legacy: bool,
}
impl<E> Copy for CalendarInput<'_, '_, E> {}
impl<E> Clone for CalendarInput<'_, '_, E> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'call, 'batch, E> CalendarInput<'call, 'batch, E> {
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
    ) -> Result<crate::RowDataError, CalendarFailure<E>> {
        match self.row_error_port {
            Some(port) => port(ordinal, message).map_err(CalendarFailure::Row),
            None => Ok(crate::RowDataError::new(ordinal, message)),
        }
    }
    pub(super) fn carrier_error(self, error: CalendarCarrierError<'_>) -> KernelFailure {
        error.into_kernel()
    }
}
impl<'call, 'batch> CalendarInput<'call, 'batch> {
    pub fn owner(input: ScalarCallInput<'call, 'batch>) -> Self {
        Self {
            types: &input.contract().selected().argument_types,
            target: input.contract().result_type(),
            carrier: input.contract().result_type().data_type(),
            arguments: input.arguments(),
            selected: input.selection(),
            legacy: false,
            row_error_port: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum CalendarOperation {
    Trunc,
    ConvertTz,
    UnixTimestamp,
    EpochNtz,
    TimestampDiff,
    DaysShift(i64),
    MonthsShift(i32),
    DurationShift(CalendarDurationShift),
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
            Self::ConvertTz => CalendarExtendedOp::ConvertTz,
            Self::UnixTimestamp => CalendarExtendedOp::UnixTimestamp,
            Self::EpochNtz => CalendarExtendedOp::EpochNtz,
            Self::TimestampDiff => CalendarExtendedOp::TimestampDiff,
            Self::DaysShift(factor) => CalendarExtendedOp::DaysShift(factor),
            Self::MonthsShift(factor) => CalendarExtendedOp::MonthsShift(factor),
            Self::DurationShift(operation) => CalendarExtendedOp::DurationShift(operation),
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
) -> Result<ArrayRef, RawCalendarFailure> {
    let types: Vec<_> = arguments
        .iter()
        .map(|array| {
            FunctionValueType::try_with_logical_type(
                array.data_type().clone(),
                true,
                novarocks_type_contract::ValueLogicalType::Physical,
            )
            .map(FunctionArgumentType::Value)
            .map_err(KernelFailure::from)
        })
        .collect::<Result<Vec<_>, KernelFailure>>()?;
    let arguments_view: Vec<_> = arguments
        .iter()
        .map(|argument| {
            // The original interval families explicitly broadcast a one-row child.
            if matches!(
                operation,
                CalendarOperation::DaysShift(_)
                    | CalendarOperation::MonthsShift(_)
                    | CalendarOperation::DurationShift(_)
            ) && argument.len() == 1
            {
                EvaluatedArgument::Scalar(argument)
            } else {
                EvaluatedArgument::Column(argument)
            }
        })
        .collect();
    // A short legacy array is an invalid caller and keeps the old indexing panic
    // at the adapter boundary. The selected core retains its own bounds checks.
    for argument in arguments {
        assert!(
            argument.len() >= rows
                || (matches!(
                    operation,
                    CalendarOperation::DaysShift(_)
                        | CalendarOperation::MonthsShift(_)
                        | CalendarOperation::DurationShift(_)
                ) && argument.len() == 1),
            "legacy calendar row out of bounds"
        );
    }
    let carrier = if matches!(
        operation,
        CalendarOperation::Timestamp
            | CalendarOperation::ConvertTz
            | CalendarOperation::EpochNtz
            | CalendarOperation::MonthsShift(_)
            | CalendarOperation::DurationShift(_)
    ) || (matches!(
        operation,
        CalendarOperation::Trunc | CalendarOperation::StrToDate | CalendarOperation::DaysShift(_)
    ) && *output_type != DataType::Date32)
    {
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    } else {
        output_type.clone()
    };
    if carrier != *output_type {
        return Err(super::calendar_parts_shared::part_contract(format_args!(
            "calendar operation requires normalized result carrier {carrier:?}, got {output_type:?}"
        ))
        .into());
    }
    let target = FunctionValueType::try_with_logical_type(
        output_type.clone(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
    )
    .map_err(KernelFailure::from)?;
    let row_error_port =
        |ordinal: usize, message: &str| -> Result<crate::RowDataError, crate::RowDataError> {
            Err(crate::RowDataError::new(ordinal, message))
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
            row_error_port: Some(&row_error_port),
        },
        &LegacyControl,
    )?;
    Ok(Arc::clone(output.values()))
}

pub fn legacy_extract_datetimes(
    array: &ArrayRef,
) -> Result<Vec<Option<chrono::NaiveDateTime>>, KernelFailure> {
    let reader = calendar_extended::DateInput::raw(array.as_ref())
        .map_err(CalendarCarrierError::into_kernel)?;
    let mut work = crate::kernel_input::EvaluationCheckpoints::new(&LegacyControl);
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let value = if array.is_null(row) {
            None
        } else {
            reader.read(row, &mut work)?
        };
        values.push(value);
    }
    Ok(values)
}
pub fn legacy_extract_dates(
    array: &ArrayRef,
) -> Result<Vec<Option<chrono::NaiveDate>>, KernelFailure> {
    if !matches!(
        array.data_type(),
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
    ) {
        return Err(CalendarCarrierError::UnsupportedDate(array.data_type()).into_kernel());
    }
    Ok(legacy_extract_datetimes(array)?
        .into_iter()
        .map(|date| date.map(|date| date.date()))
        .collect())
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
    fn raw_timestamp_inputs_keep_real_units_and_refuse_an_unnormalized_result() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["second", "second"])),
            Arc::new(
                TimestampNanosecondArray::from(vec![-1, 1_234_567_890]).with_timezone("+08:00"),
            ),
        ];
        let output = evaluate_legacy_calendar(
            CalendarOperation::Trunc,
            &inputs,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            2,
        )
        .unwrap();
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
            vec![Some(-1_000_000), Some(1_000_000)]
        );
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for requested in [
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            DataType::Int64,
        ] {
            let error = evaluate_legacy_calendar(CalendarOperation::Trunc, &inputs, &requested, 2)
                .unwrap_err();
            assert!(matches!(error, CalendarFailure::Outer(_)));
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        }
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
    fn str_to_date_projects_the_single_parse_result_into_its_frozen_carrier() {
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
            &DataType::Timestamp(TimeUnit::Microsecond, None),
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
            vec![Some(90_000_000), None]
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
            .unwrap_err()
            .to_string(),
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
        let CalendarFailure::Row(error) = result.unwrap().unwrap_err() else {
            panic!("the first actual row error must precede the later row's work");
        };
        assert_eq!(error.selected_ordinal(), 0);
        assert_eq!(
            error.message(),
            "format value must in {microsecond, millisecond, second, minute, hour, day, month, year, week, quarter}"
        );
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&error).class(),
            novarocks_failure_contract::FailureClass::Data,
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

/// Validate only the original raw input carrier before another child conversion.
pub fn legacy_validate_datetime_source(array: &ArrayRef) -> Result<(), KernelFailure> {
    calendar_extended::DateInput::raw(array.as_ref())
        .map(|_| ())
        .map_err(CalendarCarrierError::into_kernel)
}

#[cfg(test)]
mod carrier_diagnostic_tests {
    use super::*;
    use arrow_array::{Int64Array, StructArray};
    use arrow_schema::Field;

    fn assert_extraction_contract(error: KernelFailure, expected: &str) {
        use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.to_string(),
            BoundedDetail::truncating(expected).as_str()
        );
        assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
    }
    #[test]
    fn raw_carrier_admission_bounds_the_diagnostic_and_retains_its_contract_fact() {
        let fields = (0..16)
            .map(|index| {
                Arc::new(Field::new(
                    format!("raw_field_{index}_").repeat(24),
                    DataType::Int64,
                    true,
                ))
            })
            .collect::<Vec<_>>();
        let source: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            (0..16)
                .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
                .collect(),
            None,
        ));
        let expected = format!("unsupported datetime input type: {:?}", source.data_type());
        assert!(expected.len() > 4096);
        assert_extraction_contract(legacy_extract_datetimes(&source).unwrap_err(), &expected);
        assert_extraction_contract(
            legacy_validate_datetime_source(&source).unwrap_err(),
            &expected,
        );
        let units: ArrayRef = Arc::new(StringArray::from(vec!["day"]));
        let CalendarFailure::Outer(error) = evaluate_legacy_calendar(
            CalendarOperation::Trunc,
            &[units, Arc::clone(&source)],
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            1,
        )
        .unwrap_err() else {
            panic!("a frozen carrier failure cannot enter the row channel");
        };
        assert_extraction_contract(error, &expected);
        let expected_date = format!("unsupported date input type: {:?}", source.data_type());
        assert_extraction_contract(legacy_extract_dates(&source).unwrap_err(), &expected_date);
        let CalendarFailure::Outer(error) =
            evaluate_legacy_calendar(CalendarOperation::LastDay, &[source], &DataType::Date32, 1)
                .unwrap_err()
        else {
            panic!("a frozen date carrier cannot enter the row channel");
        };
        assert_extraction_contract(error, &expected_date);
    }
}

/// The legacy timestampadd month/year branch shares this same calendar computation.
pub fn legacy_add_months_to_datetime(
    date: chrono::NaiveDateTime,
    months: i32,
) -> chrono::NaiveDateTime {
    super::calendar_month::add_months_to_datetime(date, months)
}

/// The original argument-form Unix projection, also used by the existing clock shell.
pub fn calendar_unix_seconds(date: chrono::NaiveDateTime) -> i64 {
    date.and_utc().timestamp()
}
/// The original epoch scale split shared by NTZ and legacy timezone-aware shells.
pub fn legacy_split_epoch_value(value: i64, scale: i64) -> Option<(i64, u32)> {
    super::calendar_epoch_ntz::split_epoch_value(value, scale)
}
/// Pure UTC construction; any timezone projection remains explicit in its caller.
pub fn legacy_epoch_utc_datetime(
    seconds: i64,
    micros: u32,
) -> Option<chrono::DateTime<chrono::Utc>> {
    super::calendar_epoch_ntz::epoch_utc_datetime(seconds, micros)
}

#[cfg(test)]
mod frozen_result_tests {
    use super::*;
    use arrow_array::{Date32Array, Int64Array};
    use arrow_schema::TimeUnit;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn raw_calendar_cannot_return_a_different_carrier_or_relabel_timestamp_units() {
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![0]));
        let months: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let error = evaluate_legacy_calendar(
            CalendarOperation::MonthsShift(1),
            &[dates.clone(), months],
            &DataType::Date32,
            1,
        )
        .unwrap_err();
        assert!(matches!(error, CalendarFailure::Outer(_)));
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let error = evaluate_legacy_calendar(
            CalendarOperation::Timestamp,
            &[dates],
            &DataType::Timestamp(TimeUnit::Second, None),
            1,
        )
        .unwrap_err();
        assert!(matches!(error, CalendarFailure::Outer(_)));
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
