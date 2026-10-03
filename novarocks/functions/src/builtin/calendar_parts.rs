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

//! The installed calendar fields over exact canonical selected carriers.
//! Parsers and finite Chrono calls reuse their original value authors.
//! Output Layout checks are representation facts, not formal memory admission.

use crate::{
    FunctionArgumentType, FunctionValueType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    datetime_value::{self, DateParseObservation},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, FixedSizeBinaryArray, Int32Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDateTime, Timelike};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CalendarPartOp {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DayOfWeek,
    YearWeek,
    DayOfYear,
    WeekOfYear,
    Quarter,
}

impl CalendarPartOp {
    fn extract(self, value: NaiveDateTime) -> Result<i32, KernelFailure> {
        let result = match self {
            Self::Year => i64::from(value.year()),
            Self::Month => i64::from(value.month()),
            Self::Day => i64::from(value.day()),
            Self::Hour => i64::from(value.hour()),
            Self::Minute => i64::from(value.minute()),
            Self::Second => i64::from(value.second()),
            Self::DayOfWeek => i64::from(value.weekday().number_from_sunday()),
            Self::YearWeek => {
                let iso = value.iso_week();
                i64::from(iso.year()) * 100 + i64::from(iso.week())
            }
            Self::DayOfYear => i64::from(value.ordinal()),
            Self::WeekOfYear => i64::from(value.iso_week().week()),
            Self::Quarter => i64::from((value.month() - 1) / 3 + 1),
        };
        i32::try_from(result)
            .map_err(|_| internal("bounded calendar part exceeds its installed Int32 result"))
    }
}

enum CalendarInput<'a> {
    Timestamp(&'a TimestampMicrosecondArray),
    Date(&'a Date32Array),
    Text(&'a StringArray),
    LargeInt(&'a FixedSizeBinaryArray),
}
impl<'a> CalendarInput<'a> {
    fn checked(source: &FunctionValueType, array: &'a dyn Array) -> Result<Self, KernelFailure> {
        if array.data_type() != &source.data_type {
            return Err(internal(
                "calendar selected carrier differs from its exact source",
            ));
        }
        let physical = source.logical_type == ValueLogicalType::Physical;
        match &source.data_type {
            DataType::Timestamp(TimeUnit::Microsecond, None) if physical => {
                array.as_any().downcast_ref().map(Self::Timestamp)
            }
            DataType::Date32 if physical => array.as_any().downcast_ref().map(Self::Date),
            DataType::Utf8 if physical => array.as_any().downcast_ref().map(Self::Text),
            DataType::FixedSizeBinary(16) if source.logical_type == ValueLogicalType::LargeInt => {
                array.as_any().downcast_ref().map(Self::LargeInt)
            }
            _ => {
                return Err(invalid(
                    "calendar source differs from its four exact installed profiles",
                ));
            }
        }
        .ok_or_else(|| internal("calendar exact selected carrier cannot be downcast"))
    }

    fn read(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDateTime>, KernelFailure> {
        match self {
            Self::Text(values) => {
                let text = values.value(row);
                let mut observe = |event| match event {
                    DateParseObservation::Step => work.step(),
                    DateParseObservation::OpaqueBoundary => work.flush(),
                };
                let parsed = datetime_value::parse_datetime_observed(text, &mut observe)?;
                if parsed.is_some() {
                    return Ok(parsed);
                }
                let date = datetime_value::parse_date_observed(text, &mut observe)?;
                work.flush()?;
                let parsed = date.and_then(|date| date.and_hms_opt(0, 0, 0));
                work.flush()?;
                Ok(parsed)
            }
            Self::Timestamp(values) => {
                let value = values.value(row);
                work.step()?;
                work.flush()?;
                let parsed = datetime_value::timestamp_to_naive(&TimeUnit::Microsecond, value);
                work.flush()?;
                Ok(parsed)
            }
            Self::Date(values) => {
                let value = values.value(row);
                work.step()?;
                work.flush()?;
                let parsed = datetime_value::date32_to_naive(value)
                    .and_then(|date| date.and_hms_opt(0, 0, 0));
                work.flush()?;
                Ok(parsed)
            }
            Self::LargeInt(values) => {
                let bytes: [u8; 16] = values.value(row).try_into().map_err(|_| {
                    internal("calendar LARGEINT differs from its exact sixteen-byte width")
                })?;
                let value = i64::try_from(i128::from_be_bytes(bytes)).ok();
                work.step()?;
                work.flush()?;
                let parsed =
                    value.and_then(crate::calendar_numeric::numeric_datetime_literal_to_naive);
                work.flush()?;
                Ok(parsed)
            }
        }
    }
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

pub(super) fn evaluate_calendar_part<'a>(
    op: CalendarPartOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "calendar part requires one exact checked value argument",
            ));
        };
        let target = input.contract().result_type();
        let exact = target.logical_type == ValueLogicalType::Physical
            && target.data_type == DataType::Int32
            && target.nullable;
        work.step()?;
        if !exact {
            return Err(invalid(
                "calendar part differs from its exact Int32 nullable result",
            ));
        }
        let reader = CalendarInput::checked(source, argument.array().as_ref())?;
        work.step()?;
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
            let row = argument.value_row(ordinal, batch_row);
            let valid_address = row < argument.array().len();
            work.step()?;
            if !valid_address {
                return Err(internal("calendar selected argument row is out of bounds"));
            }
            let null = argument.array().is_null(row);
            work.step()?;
            if null && !source.nullable {
                return Err(internal(
                    "calendar non-null source contains selected SQL NULL",
                ));
            }
            let parsed = if null {
                None
            } else {
                reader.read(row, &mut work)?
            };
            let part = match parsed {
                Some(value) => {
                    work.flush()?;
                    let part = op.extract(value);
                    work.flush()?;
                    Some(part?)
                }
                None => None,
            };
            values.push(part.unwrap_or(0));
            validity.append(part.is_some());
            has_null |= part.is_none();
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
#[path = "calendar_parts_tests.rs"]
mod tests;
