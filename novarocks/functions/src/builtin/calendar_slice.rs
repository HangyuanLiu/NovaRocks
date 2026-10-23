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

//! One original slice computation, selected row protocol and temporal output projection.
use super::calendar_extended::DateInput;
use super::calendar_extended_shared::{
    CalendarFailure, RawCalendarFailure, finish_calendar_result,
};
use super::calendar_timestamp::{TimestampValueError, timestamp_value};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallInput, SelectedValues, Selection, ValueLogicalType,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use novarocks_failure_contract::BoundedDetail;
use std::{alloc::Layout, sync::Arc, time::Duration as WaitDuration};
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CalendarSliceDomain {
    Date,
    Time,
}
impl CalendarSliceDomain {
    fn name(self) -> &'static str {
        match self {
            Self::Date => "date_slice",
            Self::Time => "time_slice",
        }
    }
    fn data_type(self) -> DataType {
        match self {
            Self::Date => DataType::Date32,
            Self::Time => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }
}
#[derive(Debug)]
enum SliceRowError {
    NullInterval,
    NullUnit,
    PositiveInterval,
    UnsupportedUnit(BoundedDetail),
    DateTimeUnit,
    NullBoundary,
    UnsupportedBoundary(BoundedDetail),
    BeforeOrigin,
    Arithmetic,
    IndexOverflow,
    Timestamp(TimestampValueError),
}
struct SliceDiagnostic<'a> {
    error: &'a SliceRowError,
    domain: CalendarSliceDomain,
}
impl std::fmt::Display for SliceDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.domain.name();
        match self.error {
            SliceRowError::NullInterval => write!(f, "{name} requires non-null interval"),
            SliceRowError::NullUnit => write!(f, "{name} requires non-null unit"),
            SliceRowError::PositiveInterval => {
                write!(f, "{name} requires second parameter must be greater than 0")
            }
            SliceRowError::UnsupportedUnit(unit) => {
                write!(f, "time_slice unsupported unit: {}", unit.as_str())
            }
            SliceRowError::DateTimeUnit => {
                f.write_str("can't use time_slice for date with time(hour/minute/second)")
            }
            SliceRowError::NullBoundary => write!(f, "{name} requires non-null boundary"),
            SliceRowError::UnsupportedBoundary(boundary) => write!(
                f,
                "time_slice expects boundary floor/ceil, got {}",
                boundary.as_str()
            ),
            SliceRowError::BeforeOrigin => {
                f.write_str("time used with time_slice can't before 0001-01-01 00:00:00")
            }
            SliceRowError::Arithmetic => {
                write!(f, "{name} input exceeds supported slice arithmetic")
            }
            SliceRowError::IndexOverflow => write!(f, "{name} bucket index overflow"),
            SliceRowError::Timestamp(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl SliceRowError {
    fn reason(&self) -> crate::DataFailureReason {
        match self {
            Self::Arithmetic | Self::IndexOverflow | Self::Timestamp(_) => {
                crate::DataFailureReason::NumericOutOfRange
            }
            _ => crate::DataFailureReason::InvalidValue,
        }
    }
    fn raw_row_error(&self, domain: CalendarSliceDomain, ordinal: usize) -> RowDataError {
        let detail = BoundedDetail::truncating_display(SliceDiagnostic {
            error: self,
            domain,
        });
        RowDataError::with_reason(ordinal, self.reason(), detail.as_str())
    }
    fn row_error(&self, domain: CalendarSliceDomain, ordinal: usize) -> RowDataError {
        match self {
            Self::UnsupportedUnit(_) => RowDataError::new(ordinal, "time_slice unsupported unit"),
            Self::UnsupportedBoundary(_) => {
                RowDataError::new(ordinal, "time_slice expects boundary floor/ceil")
            }
            _ => self.raw_row_error(domain, ordinal),
        }
    }
}
fn bounded_lowercase(raw: &str) -> BoundedDetail {
    let bounded = BoundedDetail::truncating(raw);
    let mut text = bounded.as_str().to_owned();
    text.make_ascii_lowercase();
    BoundedDetail::truncating(&text)
}
enum SliceCalculationError {
    Row(SliceRowError),
    Kernel(KernelFailure),
}
impl From<SliceRowError> for SliceCalculationError {
    fn from(error: SliceRowError) -> Self {
        Self::Row(error)
    }
}
impl From<KernelFailure> for SliceCalculationError {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}
#[derive(Copy, Clone, Debug)]
enum TimeSliceUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

impl TimeSliceUnit {
    fn parse(raw: &str) -> Result<Self, SliceRowError> {
        // Length checks in eq_ignore_ascii_case reject oversized invalid tokens
        // without copying or scanning their complete source text.
        for (singular, plural, unit) in [
            ("year", "years", Self::Year),
            ("quarter", "quarters", Self::Quarter),
            ("month", "months", Self::Month),
            ("week", "weeks", Self::Week),
            ("day", "days", Self::Day),
            ("hour", "hours", Self::Hour),
            ("minute", "minutes", Self::Minute),
            ("second", "seconds", Self::Second),
            ("millisecond", "milliseconds", Self::Millisecond),
            ("microsecond", "microseconds", Self::Microsecond),
        ] {
            if raw.eq_ignore_ascii_case(singular) || raw.eq_ignore_ascii_case(plural) {
                return Ok(unit);
            }
        }
        Err(SliceRowError::UnsupportedUnit(bounded_lowercase(raw)))
    }

    fn index(self, dt: NaiveDateTime, start: NaiveDateTime) -> Option<i64> {
        let delta = dt.signed_duration_since(start);
        Some(match self {
            Self::Year => i64::from(dt.year() - start.year()),
            Self::Quarter => ((dt.year() - start.year()) as i64) * 4 + (dt.month0() / 3) as i64,
            Self::Month => ((dt.year() - start.year()) as i64) * 12 + dt.month0() as i64,
            Self::Week => delta.num_days() / 7,
            Self::Day => delta.num_days(),
            Self::Hour => delta.num_hours(),
            Self::Minute => delta.num_minutes(),
            Self::Second => delta.num_seconds(),
            Self::Millisecond => delta.num_milliseconds(),
            Self::Microsecond => delta.num_microseconds()?,
        })
    }

    fn sliced_datetime(
        self,
        start: NaiveDateTime,
        epoch: i64,
        interval: i64,
    ) -> Option<NaiveDateTime> {
        let units = epoch.checked_mul(interval)?;
        match self {
            Self::Year => {
                let year = i32::try_from(i64::from(start.year()).checked_add(units)?).ok()?;
                NaiveDate::from_ymd_opt(year, 1, 1)?.and_hms_opt(0, 0, 0)
            }
            Self::Quarter | Self::Month => {
                let months = if matches!(self, Self::Quarter) {
                    units.checked_mul(3)?
                } else {
                    units
                };
                let year =
                    i32::try_from(i64::from(start.year()).checked_add(months.div_euclid(12))?)
                        .ok()?;
                let month = u32::try_from(months.rem_euclid(12).checked_add(1)?).ok()?;
                NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)
            }
            // Checked durations/addition preserve legitimate overflow -> NULL
            // without a chrono panic for large positive INT32 intervals.
            Self::Week => start.checked_add_signed(Duration::try_days(units.checked_mul(7)?)?),
            Self::Day => start.checked_add_signed(Duration::try_days(units)?),
            Self::Hour => start.checked_add_signed(Duration::try_hours(units)?),
            Self::Minute => start.checked_add_signed(Duration::try_minutes(units)?),
            Self::Second => start.checked_add_signed(Duration::try_seconds(units)?),
            Self::Millisecond => start.checked_add_signed(Duration::try_milliseconds(units)?),
            Self::Microsecond => start.checked_add_signed(Duration::microseconds(units)),
        }
    }
}

fn calculate_row_lazy<'text>(
    domain: CalendarSliceDomain,
    interval: Option<i64>,
    unit: Option<&'text str>,
    boundary: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<Option<&'text str>>, KernelFailure>,
    datetime: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDateTime>, KernelFailure>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<NaiveDateTime>, SliceCalculationError> {
    let interval = interval.ok_or(SliceRowError::NullInterval)?;
    let unit_str = unit.ok_or(SliceRowError::NullUnit)?;
    if interval <= 0 {
        return Err(SliceRowError::PositiveInterval.into());
    }
    work.flush()?;
    let unit = TimeSliceUnit::parse(unit_str);
    work.flush()?;
    let unit = unit?;
    if domain == CalendarSliceDomain::Date
        && !matches!(
            unit,
            TimeSliceUnit::Year
                | TimeSliceUnit::Quarter
                | TimeSliceUnit::Month
                | TimeSliceUnit::Week
                | TimeSliceUnit::Day
        )
    {
        return Err(SliceRowError::DateTimeUnit.into());
    }
    let boundary = match boundary(work)? {
        Some(value) => value.ok_or(SliceRowError::NullBoundary)?,
        None => "floor",
    };
    work.flush()?;
    let use_ceil = if boundary.eq_ignore_ascii_case("floor") {
        false
    } else if boundary.eq_ignore_ascii_case("ceil") {
        true
    } else {
        return Err(SliceRowError::UnsupportedBoundary(bounded_lowercase(boundary)).into());
    };
    work.flush()?;
    let dt = match datetime(work)? {
        Some(value) => value,
        None => return Ok(None),
    };
    let start = NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    if dt < start {
        return Err(SliceRowError::BeforeOrigin.into());
    }
    work.flush()?;
    let duration = unit.index(dt, start).ok_or(SliceRowError::Arithmetic)?;
    let mut epoch = duration / interval;
    if use_ceil {
        epoch = epoch.checked_add(1).ok_or(SliceRowError::IndexOverflow)?;
    }
    let Some(sliced) = unit.sliced_datetime(start, epoch, interval) else {
        return Ok(None);
    };
    if !(1..=9999).contains(&sliced.year()) {
        return Ok(None);
    }
    work.flush()?;
    Ok(Some(sliced))
}
enum SliceTemporal<'a> {
    Decoded(&'a [Option<NaiveDateTime>]),
    Selected {
        reader: DateInput<'a>,
        argument: EvaluatedArgument<'a>,
    },
}
impl SliceTemporal<'_> {
    fn read(
        &self,
        ordinal: usize,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDateTime>, KernelFailure> {
        work.step()?;
        match self {
            Self::Decoded(values) => values
                .get(row)
                .copied()
                .ok_or_else(|| internal("legacy slice temporal address exceeds decoded rows")),
            Self::Selected { reader, argument } => {
                let at = argument.value_row(ordinal, row);
                if at >= argument.array().len() {
                    return Err(invalid(
                        "slice selected temporal address exceeds its carrier",
                    ));
                }
                if argument.array().is_null(at) {
                    return Ok(None);
                }
                reader.read(at, work)
            }
        }
    }
}
enum SliceCount<'a> {
    Decoded(&'a [Option<i64>]),
    Selected {
        values: &'a Int32Array,
        argument: EvaluatedArgument<'a>,
    },
}
impl SliceCount<'_> {
    fn read(
        &self,
        ordinal: usize,
        row: usize,
        rows: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<i64>, KernelFailure> {
        work.step()?;
        match self {
            Self::Decoded(values) => {
                let at = if values.len() == 1 && rows > 1 {
                    0
                } else {
                    row
                };
                Ok(values.get(at).copied().flatten())
            }
            Self::Selected { values, argument } => {
                let at = argument.value_row(ordinal, row);
                if at >= values.len() {
                    return Err(invalid(
                        "slice selected interval address exceeds its carrier",
                    ));
                }
                Ok(super::calendar_add_interval::legacy_int32_interval_at(
                    values, at,
                ))
            }
        }
    }
}
struct SliceText<'a> {
    values: &'a StringArray,
    argument: Option<EvaluatedArgument<'a>>,
}
impl SliceText<'_> {
    fn read(
        &self,
        ordinal: usize,
        row: usize,
        rows: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<&str>, KernelFailure> {
        work.step()?;
        let at = if let Some(argument) = self.argument {
            argument.value_row(ordinal, row)
        } else if self.values.len() == 1 && rows > 1 {
            0
        } else {
            row
        };
        if at >= self.values.len() {
            if self.argument.is_some() {
                return Err(invalid("slice selected text address exceeds its carrier"));
            }
            return Ok(None);
        }
        Ok((!self.values.is_null(at)).then(|| self.values.value(at)))
    }
}
enum SliceValues {
    Date(Vec<Option<i32>>),
    Datetime(Vec<Option<i64>>),
}
impl SliceValues {
    fn new(domain: CalendarSliceDomain, rows: usize) -> Result<Self, KernelFailure> {
        let bytes = match domain {
            CalendarSliceDomain::Date => Layout::array::<Option<i32>>(rows),
            CalendarSliceDomain::Time => Layout::array::<Option<i64>>(rows),
        };
        bytes.map_err(|_| KernelFailure::ResourceExhausted)?;
        match domain {
            CalendarSliceDomain::Date => {
                let mut values = Vec::new();
                values
                    .try_reserve_exact(rows)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                Ok(Self::Date(values))
            }
            CalendarSliceDomain::Time => {
                let mut values = Vec::new();
                values
                    .try_reserve_exact(rows)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                Ok(Self::Datetime(values))
            }
        }
    }
    fn push(&mut self, value: Option<NaiveDateTime>) -> Result<(), SliceRowError> {
        match self {
            Self::Date(values) => {
                values.push(value.map(|dt| crate::datetime_value::naive_to_date32(dt.date())))
            }
            Self::Datetime(values) => values.push(
                value
                    .map(|dt| timestamp_value(dt, TimeUnit::Microsecond))
                    .transpose()
                    .map_err(SliceRowError::Timestamp)?,
            ),
        }
        Ok(())
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::Date(values) => Arc::new(Date32Array::from(values)),
            Self::Datetime(values) => Arc::new(TimestampMicrosecondArray::from(values)),
        }
    }
}
type SliceRowErrorPort<'a, E> =
    Option<&'a dyn Fn(&SliceRowError, usize) -> Result<RowDataError, E>>;

struct SliceInput<'a, E = std::convert::Infallible> {
    domain: CalendarSliceDomain,
    selection: Selection<'a>,
    batch_rows: usize,
    datetime: SliceTemporal<'a>,
    count: SliceCount<'a>,
    unit: SliceText<'a>,
    boundary: Option<SliceText<'a>>,
    row_error: SliceRowErrorPort<'a, E>,
}
fn evaluate_input<'a, E>(
    input: SliceInput<'a, E>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, CalendarFailure<E>> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        work.flush()?;
        let mut output = SliceValues::new(input.domain, input.selection.len())?;
        work.flush()?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(input.selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, row) in input.selection.iter().enumerate() {
            work.step()?;
            let interval = input
                .count
                .read(ordinal, row, input.batch_rows, &mut work)?;
            // Read the unit before testing positivity, exactly as the original row order.
            let unit = input.unit.read(ordinal, row, input.batch_rows, &mut work)?;
            // Boundary indexing is lazy, after interval/unit validation, preserving old masks.
            let boundary = |w: &mut EvaluationCheckpoints<'_>| {
                input
                    .boundary
                    .as_ref()
                    .map(|b| b.read(ordinal, row, input.batch_rows, w))
                    .transpose()
            };
            // A closure boundary is evaluated by the row implementation after its prior checks.
            let value = calculate_row_lazy(
                input.domain,
                interval,
                unit,
                boundary,
                |w| input.datetime.read(ordinal, row, w),
                &mut work,
            );
            let failure = match value {
                Ok(value) => output.push(value).err(),
                Err(SliceCalculationError::Row(error)) => Some(error),
                Err(SliceCalculationError::Kernel(error)) => return Err(error.into()),
            };
            if let Some(error) = failure {
                let row = match input.row_error {
                    Some(port) => port(&error, ordinal).map_err(CalendarFailure::Row)?,
                    None => error.row_error(input.domain, ordinal),
                };
                errors.push(row);
                output
                    .push(None)
                    .map_err(|_| internal("slice NULL projection failed"))?;
            }
            work.step()?;
        }
        work.flush()?;
        let array = output.finish();
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            input.selection,
            &input.domain.data_type(),
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
        .map_err(CalendarFailure::Outer)
    })();
    finish_calendar_result(work, result)
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: WaitDuration) -> Result<(), KernelFailure> {
        unreachable!("slice kernels do not wait")
    }
}
/// All admission, eager whole-column decoding and length checks remain at the legacy boundary.
pub fn evaluate_legacy_slice(
    domain: CalendarSliceDomain,
    datetime: &[Option<NaiveDateTime>],
    count: &[Option<i64>],
    unit: &StringArray,
    boundary: Option<&StringArray>,
) -> Result<ArrayRef, RawCalendarFailure> {
    let row_error = |error: &SliceRowError, ordinal: usize| -> Result<RowDataError, RowDataError> {
        Err(error.raw_row_error(domain, ordinal))
    };
    let output = evaluate_input(
        SliceInput {
            domain,
            selection: Selection::all(datetime.len()),
            batch_rows: datetime.len(),
            datetime: SliceTemporal::Decoded(datetime),
            count: SliceCount::Decoded(count),
            unit: SliceText {
                values: unit,
                argument: None,
            },
            boundary: boundary.map(|values| SliceText {
                values,
                argument: None,
            }),
            row_error: Some(&row_error),
        },
        &LegacyControl,
    )?;
    Ok(Arc::clone(output.values()))
}
pub(super) fn exact_profile(
    domain: CalendarSliceDomain,
    contract: &crate::ScalarCallContract,
) -> Result<(), KernelFailure> {
    let target = contract.result_type();
    let types = &contract.selected().argument_types;
    let [
        FunctionArgumentType::Value(value),
        FunctionArgumentType::Value(count),
        FunctionArgumentType::Value(unit),
        rest @ ..,
    ] = types.as_ref()
    else {
        return Err(invalid(
            "slice requires three/four exact selected value arguments",
        ));
    };
    let physical = |t: &FunctionValueType, ty: &DataType| {
        t.logical_type() == ValueLogicalType::Physical && t.data_type() == ty
    };
    if !physical(target, &domain.data_type())
        || !physical(value, &domain.data_type())
        || !physical(count, &DataType::Int32)
        || !physical(unit, &DataType::Utf8)
        || !matches!(rest, [] | [FunctionArgumentType::Value(_)])
        || rest
            .iter()
            .any(|t| !matches!(t,FunctionArgumentType::Value(v)if physical(v,&DataType::Utf8)))
    {
        return Err(invalid(
            "slice differs from its exact original temporal/INT32/Utf8 domain",
        ));
    }
    Ok(())
}
pub(super) fn evaluate_selected<'a>(
    domain: CalendarSliceDomain,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    exact_profile(domain, input.contract())?;
    let args = input.arguments();
    let types = &input.contract().selected().argument_types;
    let FunctionArgumentType::Value(source) = &types[0] else {
        return Err(invalid("slice has no exact temporal source"));
    };
    let datetime = DateInput::checked(source, args[0].array().as_ref())?;
    let count = args[1]
        .array()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| invalid("slice expects its exact INT32 count carrier"))?;
    let text = |argument: EvaluatedArgument<'a>| -> Result<SliceText<'a>, KernelFailure> {
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("slice expects its exact Utf8 control carrier"))?;
        Ok(SliceText {
            values,
            argument: Some(argument),
        })
    };
    evaluate_input(
        SliceInput::<std::convert::Infallible> {
            domain,
            selection: input.selection(),
            batch_rows: input.selection().batch_rows(),
            datetime: SliceTemporal::Selected {
                reader: datetime,
                argument: args[0],
            },
            count: SliceCount::Selected {
                values: count,
                argument: args[1],
            },
            unit: text(args[2])?,
            boundary: if args.len() == 4 {
                Some(text(args[3])?)
            } else {
                None
            },
            row_error: None,
        },
        control,
    )
    .map_err(CalendarFailure::into_kernel)
}
#[cfg(test)]
#[path = "calendar_slice_tests.rs"]
mod tests;
