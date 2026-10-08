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

//! Selected calendar truncation, duration differences and timestamp projection.
//! These bodies preserve the original date helper algorithms and invalid-value NULLs.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallInput, SelectedValues,
    datetime_value::{self, DateParseObservation},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, FixedSizeBinaryArray, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CalendarExtendedOp {
    Trunc,
    UnixTimestamp,
    EpochNtz,
    TimestampDiff,
    DaysShift(i64),
    MonthsShift(i32),
    DurationShift(super::calendar_duration::CalendarDurationShift),
    DateFormat,
    Parse(super::calendar_extended_parse::CalendarParseOp),
    WeeksDiff,
    HoursDiff,
    MinutesDiff,
    SecondsDiff,
    Timestamp,
}

/// Carrier admission facts; the legacy boundary retains the complete raw type diagnostic.
pub(super) enum CalendarCarrierError<'a> {
    UnsupportedDatetime(&'a DataType),
    UnsupportedDate(&'a DataType),
    Downcast(&'static str),
}
impl CalendarCarrierError<'_> {
    pub(super) fn legacy_message(self) -> String {
        match self {
            Self::UnsupportedDatetime(data_type) => {
                format!("unsupported datetime input type: {data_type:?}")
            }
            Self::UnsupportedDate(data_type) => {
                format!("unsupported date input type: {data_type:?}")
            }
            Self::Downcast(message) => message.to_string(),
        }
    }
}

pub(super) enum DateInput<'a> {
    Date(&'a Date32Array),
    Timestamp(&'a TimestampMicrosecondArray),
    Text(&'a StringArray),
    RawTimestamp(&'a dyn Array, TimeUnit),
    RawLargeInt(&'a FixedSizeBinaryArray),
}
impl<'a> DateInput<'a> {
    pub(super) fn checked(
        source: &FunctionValueType,
        array: &'a dyn Array,
    ) -> Result<Self, KernelFailure> {
        if source.logical_type != ValueLogicalType::Physical
            || array.data_type() != &source.data_type
        {
            return Err(invalid(
                "extended calendar requires its exact Physical selected source",
            ));
        }
        match &source.data_type {
            DataType::Date32 => array.as_any().downcast_ref().map(Self::Date),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                array.as_any().downcast_ref().map(Self::Timestamp)
            }
            DataType::Utf8 => array.as_any().downcast_ref().map(Self::Text),
            _ => {
                return Err(invalid(
                    "extended calendar source is not an installed temporal profile",
                ));
            }
        }
        .ok_or_else(|| internal("extended calendar selected source cannot be downcast"))
    }
    pub(super) fn for_input(
        source: &FunctionValueType,
        array: &'a dyn Array,
        input: super::calendar_extended_shared::CalendarInput<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        if !input.legacy {
            return Self::checked(source, array);
        }
        Self::raw(array).map_err(|error| input.carrier_error(error))
    }
    pub(super) fn date_for_input(
        source: &FunctionValueType,
        array: &'a dyn Array,
        input: super::calendar_extended_shared::CalendarInput<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        if input.legacy
            && !matches!(
                array.data_type(),
                DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
            )
        {
            return Err(
                input.carrier_error(CalendarCarrierError::UnsupportedDate(array.data_type()))
            );
        }
        Self::for_input(source, array, input)
    }
    pub(super) fn raw(array: &'a dyn Array) -> Result<Self, CalendarCarrierError<'a>> {
        match array.data_type() {
            DataType::Date32 => {
                array
                    .as_any()
                    .downcast_ref()
                    .map(Self::Date)
                    .ok_or(CalendarCarrierError::Downcast(
                        "failed to downcast to Date32Array",
                    ))
            }
            DataType::Timestamp(unit, _) => Ok(Self::RawTimestamp(array, unit.clone())),
            DataType::Utf8 => {
                array
                    .as_any()
                    .downcast_ref()
                    .map(Self::Text)
                    .ok_or(CalendarCarrierError::Downcast(
                        "failed to downcast to StringArray",
                    ))
            }
            DataType::FixedSizeBinary(16) => {
                array.as_any().downcast_ref().map(Self::RawLargeInt).ok_or(
                    CalendarCarrierError::Downcast(
                        "datetime LARGEINT input: expected FixedSizeBinaryArray",
                    ),
                )
            }
            other => Err(CalendarCarrierError::UnsupportedDatetime(other)),
        }
    }
    pub(super) fn read(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDateTime>, KernelFailure> {
        work.step()?;
        match self {
            Self::Date(values) => {
                work.flush()?;
                let value = datetime_value::date32_to_naive(values.value(row))
                    .and_then(|d| d.and_hms_opt(0, 0, 0));
                work.flush()?;
                Ok(value)
            }
            Self::Timestamp(values) => {
                work.flush()?;
                let value =
                    datetime_value::timestamp_to_naive(&TimeUnit::Microsecond, values.value(row));
                work.flush()?;
                Ok(value)
            }
            Self::RawTimestamp(values, unit) => {
                macro_rules! value {
                    ($array:ty, $error:literal) => {
                        values
                            .as_any()
                            .downcast_ref::<$array>()
                            .ok_or_else(|| invalid($error))?
                            .value(row)
                    };
                }
                let value = match unit {
                    TimeUnit::Second => value!(
                        TimestampSecondArray,
                        "failed to downcast to TimestampSecondArray"
                    ),
                    TimeUnit::Millisecond => value!(
                        TimestampMillisecondArray,
                        "failed to downcast to TimestampMillisecondArray"
                    ),
                    TimeUnit::Microsecond => value!(
                        TimestampMicrosecondArray,
                        "failed to downcast to TimestampMicrosecondArray"
                    ),
                    TimeUnit::Nanosecond => value!(
                        TimestampNanosecondArray,
                        "failed to downcast to TimestampNanosecondArray"
                    ),
                };
                work.flush()?;
                let value = datetime_value::timestamp_to_naive(unit, value);
                work.flush()?;
                Ok(value)
            }
            Self::RawLargeInt(values) => {
                let value = i64::try_from(i128::from_be_bytes(
                    values
                        .value(row)
                        .try_into()
                        .map_err(|_| invalid("LARGEINT value must have sixteen bytes"))?,
                ))
                .ok();
                work.flush()?;
                let value =
                    value.and_then(crate::calendar_numeric::numeric_datetime_literal_to_naive);
                work.flush()?;
                Ok(value)
            }
            Self::Text(values) => {
                let mut observe = |event| match event {
                    DateParseObservation::Step => work.step(),
                    DateParseObservation::OpaqueBoundary => work.flush(),
                };
                let text = values.value(row);
                if let Some(value) = datetime_value::parse_datetime_observed(text, &mut observe)? {
                    return Ok(Some(value));
                }
                let date = datetime_value::parse_date_observed(text, &mut observe)?;
                observe(DateParseObservation::OpaqueBoundary)?;
                let value = date.and_then(|d| d.and_hms_opt(0, 0, 0));
                observe(DateParseObservation::OpaqueBoundary)?;
                Ok(value)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum TruncUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}
const TRUNC_ERROR: &str = "format value must in {microsecond, millisecond, second, minute, hour, day, month, year, week, quarter}";
fn trunc_unit(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<TruncUnit>, KernelFailure> {
    // Original Unicode lowercase is one opaque library operation, not an ASCII replacement.
    work.flush()?;
    let text = text.to_lowercase();
    work.flush()?;
    let unit = match text.as_str() {
        "microsecond" => Some(TruncUnit::Microsecond),
        "millisecond" => Some(TruncUnit::Millisecond),
        "second" => Some(TruncUnit::Second),
        "minute" => Some(TruncUnit::Minute),
        "hour" => Some(TruncUnit::Hour),
        "day" => Some(TruncUnit::Day),
        "week" => Some(TruncUnit::Week),
        "month" => Some(TruncUnit::Month),
        "quarter" => Some(TruncUnit::Quarter),
        "year" => Some(TruncUnit::Year),
        _ => None,
    };
    work.step()?;
    Ok(unit)
}
fn truncate(dt: NaiveDateTime, unit: TruncUnit) -> NaiveDateTime {
    match unit {
        TruncUnit::Microsecond => dt,
        TruncUnit::Millisecond => dt
            .with_nanosecond(dt.nanosecond() / 1_000_000 * 1_000_000)
            .unwrap(),
        TruncUnit::Second => dt.with_nanosecond(0).unwrap(),
        TruncUnit::Minute => dt
            .with_second(0)
            .and_then(|v| v.with_nanosecond(0))
            .unwrap(),
        TruncUnit::Hour => dt
            .with_minute(0)
            .and_then(|v| v.with_second(0))
            .and_then(|v| v.with_nanosecond(0))
            .unwrap(),
        TruncUnit::Day => dt.date().and_hms_opt(0, 0, 0).unwrap(),
        TruncUnit::Week => (dt - Duration::days(dt.weekday().num_days_from_monday() as i64))
            .date()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        TruncUnit::Month => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        TruncUnit::Quarter => NaiveDate::from_ymd_opt(dt.year(), (dt.month() - 1) / 3 * 3 + 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        TruncUnit::Year => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    }
}
pub(super) fn timestamp_value_for_type(
    dt: NaiveDateTime,
    output_type: &DataType,
) -> Result<i64, String> {
    if !(0..=9999).contains(&dt.year()) {
        return Err("timestamp out of StarRocks DATETIME year range".to_string());
    }
    let micros = dt.and_utc().timestamp_micros();
    let julian = 2_440_588_i64
        .checked_add(micros.div_euclid(86_400_000_000))
        .ok_or_else(|| "timestamp out of StarRocks DATETIME encoding range".to_string())?;
    if julian < 0 {
        return Err("timestamp out of StarRocks DATETIME encoding range".to_string());
    }
    let packed = ((julian as u64) << 40) | micros.rem_euclid(86_400_000_000) as u64;
    if i64::try_from(packed).is_err() {
        return Err("timestamp out of StarRocks DATETIME encoding range".to_string());
    }
    match output_type {
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(micros),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(dt.and_utc().timestamp_millis()),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(dt.and_utc().timestamp()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| "timestamp out of range".to_string()),
        _ => Err("expected timestamp output type".to_string()),
    }
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<Option<i64>>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let errors = Layout::array::<RowDataError>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    values
        .checked_add(errors)
        .and_then(|n| n.checked_add(rows.checked_mul(8)?))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_calendar_input<'a>(
    op: CalendarExtendedOp,
    input: super::calendar_extended_shared::CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if op == CalendarExtendedOp::TimestampDiff {
        return super::calendar_extended_timestampdiff::evaluate_timestampdiff(input, control);
    }
    if let CalendarExtendedOp::DaysShift(factor) = op {
        return super::calendar_add::evaluate_day_shift(factor, input, control);
    }
    if let CalendarExtendedOp::MonthsShift(factor) = op {
        return super::calendar_month::evaluate_month_shift(factor, input, control);
    }
    if let CalendarExtendedOp::DurationShift(operation) = op {
        return super::calendar_duration::evaluate_duration_shift(operation, input, control);
    }
    if op == CalendarExtendedOp::EpochNtz {
        return super::calendar_epoch_ntz::evaluate_epoch_ntz(input, control);
    }
    if let CalendarExtendedOp::Parse(operation) = op {
        return super::calendar_extended_parse::evaluate_calendar_parse(operation, input, control);
    }
    if op == CalendarExtendedOp::DateFormat {
        return super::calendar_extended_format::evaluate_date_format(input, control);
    }
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let arguments = input.arguments();
        let types = input.types;
        let target = input.target;
        if target.logical_type != ValueLogicalType::Physical || !target.nullable {
            return Err(invalid(
                "extended calendar requires its nullable Physical result",
            ));
        }
        let arity = if matches!(
            op,
            CalendarExtendedOp::Timestamp | CalendarExtendedOp::UnixTimestamp
        ) {
            1
        } else {
            2
        };
        if arguments.len() != arity || types.len() != arity {
            return Err(invalid(
                "extended calendar requires its exact value argument count",
            ));
        }
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(arity)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (ty, arg) in types.iter().zip(arguments) {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("extended calendar requires exact value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || &ty.data_type != arg.array().data_type()
            {
                return Err(invalid(
                    "extended calendar selected argument carrier mismatch",
                ));
            }
            sources.push(ty);
            work.step()?;
        }
        let date_index = usize::from(op == CalendarExtendedOp::Trunc);
        let date_reader = DateInput::for_input(
            sources[date_index],
            arguments[date_index].array().as_ref(),
            input,
        )?;
        let other_reader = if matches!(
            op,
            CalendarExtendedOp::WeeksDiff
                | CalendarExtendedOp::HoursDiff
                | CalendarExtendedOp::MinutesDiff
                | CalendarExtendedOp::SecondsDiff
        ) {
            if (!input.legacy && sources[0].data_type != sources[1].data_type)
                || target.data_type != DataType::Int64
            {
                return Err(invalid(
                    "calendar difference requires homogeneous sources and Int64 result",
                ));
            }
            Some(DateInput::for_input(
                sources[1],
                arguments[1].array().as_ref(),
                input,
            )?)
        } else {
            None
        };
        let units = if op == CalendarExtendedOp::Trunc {
            let units = arguments[0]
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("date_trunc expects string unit"))?;
            let expected = if sources[1].data_type == DataType::Date32 {
                DataType::Date32
            } else {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            };
            if !input.legacy && target.data_type != expected {
                return Err(invalid(
                    "date_trunc result differs from its exact source profile",
                ));
            }
            Some(units)
        } else {
            None
        };
        if op == CalendarExtendedOp::Timestamp
            && !input.legacy
            && target.data_type != DataType::Timestamp(TimeUnit::Microsecond, None)
        {
            return Err(invalid("timestamp requires microsecond output"));
        }
        if op == CalendarExtendedOp::UnixTimestamp && target.data_type != DataType::Int64 {
            return Err(invalid(
                "unix_timestamp argument form requires its exact Int64 result",
            ));
        }
        let selection = input.selection();
        output_capacity(selection.len())?;
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
            let mut rows = [0usize; 2];
            let mut nulls = [false; 2];
            for (index, argument) in arguments.iter().enumerate() {
                rows[index] = argument.value_row(ordinal, batch_row);
                work.step()?;
                if rows[index] >= argument.array().len() {
                    return Err(internal("extended calendar selected row is out of bounds"));
                }
                nulls[index] = argument.array().is_null(rows[index]);
                if nulls[index] && !sources[index].nullable {
                    return Err(internal(
                        "extended calendar non-null source contains SQL NULL",
                    ));
                }
                work.step()?;
            }
            let value = if let Some(units) = units {
                // The original unit error is required even for a NULL/invalid date.
                if nulls[0] {
                    None
                } else if let Some(unit) = trunc_unit(units.value(rows[0]), &mut work)? {
                    let date = if nulls[1] {
                        None
                    } else {
                        date_reader.read(rows[1], &mut work)?
                    };
                    work.flush()?;
                    let truncated = date.map(|date| truncate(date, unit));
                    work.flush()?;
                    if target.data_type == DataType::Date32 {
                        truncated.map(|d| {
                            (d.date().num_days_from_ce() - datetime_value::UNIX_EPOCH_DAY_OFFSET)
                                as i64
                        })
                    } else {
                        truncated
                            .and_then(|date| timestamp_value_for_type(date, &target.data_type).ok())
                    }
                } else {
                    work.flush()?;
                    errors.push(input.row_error(ordinal, TRUNC_ERROR)?);
                    work.flush()?;
                    None
                }
            } else if nulls[..arity].iter().any(|v| *v) {
                None
            } else {
                let left = date_reader.read(rows[0], &mut work)?;
                if let Some(other) = &other_reader {
                    let right = other.read(rows[1], &mut work)?;
                    work.flush()?;
                    let value = match (left, right) {
                        (Some(left), Some(right)) => {
                            use super::calendar_extended_shared::{
                                CalendarDurationUnit, calendar_duration_difference,
                            };
                            let unit = match op {
                                CalendarExtendedOp::WeeksDiff => CalendarDurationUnit::Weeks,
                                CalendarExtendedOp::HoursDiff => CalendarDurationUnit::Hours,
                                CalendarExtendedOp::MinutesDiff => CalendarDurationUnit::Minutes,
                                CalendarExtendedOp::SecondsDiff => CalendarDurationUnit::Seconds,
                                _ => unreachable!(),
                            };
                            Some(calendar_duration_difference(left, right, unit))
                        }
                        _ => None,
                    };
                    work.flush()?;
                    value
                } else {
                    if op == CalendarExtendedOp::UnixTimestamp {
                        left.map(super::calendar_extended_shared::calendar_unix_seconds)
                    } else {
                        left.and_then(|date| timestamp_value_for_type(date, &target.data_type).ok())
                    }
                }
            };
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = if *input.carrier == DataType::Date32 {
            let mut dates = Vec::new();
            dates
                .try_reserve_exact(values.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for value in values {
                dates.push(value.map(|value| value as i32));
                work.step()?;
            }
            work.flush()?;
            Arc::new(Date32Array::from(dates)) as ArrayRef
        } else if *input.carrier == DataType::Int64 {
            Arc::new(Int64Array::from(values)) as ArrayRef
        } else {
            Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            input.carrier,
            Arc::clone(&array),
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[cfg(test)]
#[path = "calendar_extended_tests.rs"]
mod tests;

pub(super) fn evaluate_calendar_extended<'a>(
    operation: CalendarExtendedOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    evaluate_calendar_input(
        operation,
        super::calendar_extended_shared::CalendarInput::owner(input),
        control,
    )
}
