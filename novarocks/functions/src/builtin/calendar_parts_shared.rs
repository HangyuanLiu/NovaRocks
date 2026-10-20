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

//! Shared calendar-field computation and explicit legacy input projections.
//! Name-bound calls use the general datetime reader. The historical Year tag
//! retains its distinct raw carrier/parse contract and result projection.

use crate::{KernelFailure, Selection, kernel_control::internal};
use arrow_array::{
    Array, ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CalendarPartOp {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DayOfWeek,
    DayOfWeekIso,
    WeekDay,
    DayName,
    MonthName,
    YearWeek,
    DayOfYear,
    WeekOfYear,
    Quarter,
}

impl CalendarPartOp {
    pub fn extract_i64(self, value: NaiveDateTime) -> i64 {
        match self {
            Self::Year => i64::from(value.year()),
            Self::Month => i64::from(value.month()),
            Self::Day => i64::from(value.day()),
            Self::Hour => i64::from(value.hour()),
            Self::Minute => i64::from(value.minute()),
            Self::Second => i64::from(value.second()),
            Self::DayOfWeek => i64::from(value.weekday().number_from_sunday()),
            Self::DayOfWeekIso => i64::from(value.weekday().number_from_monday()),
            Self::WeekDay => i64::from(value.weekday().num_days_from_monday()),
            Self::DayName | Self::MonthName => 0,
            Self::YearWeek => {
                let iso = value.iso_week();
                i64::from(iso.year()) * 100 + i64::from(iso.week())
            }
            Self::DayOfYear => i64::from(value.ordinal()),
            Self::WeekOfYear => i64::from(value.iso_week().week()),
            Self::Quarter => i64::from((value.month() - 1) / 3 + 1),
        }
    }
    pub fn extract(self, value: NaiveDateTime) -> Result<i32, KernelFailure> {
        i32::try_from(self.extract_i64(value))
            .map_err(|_| internal("bounded calendar part exceeds its installed Int32 result"))
    }
}

/// Compute over the supplied original rows using the sole general raw reader.
pub fn legacy_calendar_parts(
    op: CalendarPartOp,
    array: &ArrayRef,
    selection: Selection<'_>,
) -> Result<Vec<Option<i64>>, KernelFailure> {
    let values = super::calendar_extended_shared::legacy_extract_datetimes(array)?;
    Ok(selection
        .iter()
        .map(|row| values[row].map(|value| op.extract_i64(value)))
        .collect())
}

/// Preserve the historical English Chrono name formatting.
pub fn legacy_calendar_names(
    op: CalendarPartOp,
    array: &ArrayRef,
    selection: Selection<'_>,
) -> Result<Vec<Option<String>>, KernelFailure> {
    let values = super::calendar_extended_shared::legacy_extract_datetimes(array)?;
    Ok(selection
        .iter()
        .map(|row| {
            values[row].map(|value| match op {
                CalendarPartOp::DayName => value.format("%A").to_string(),
                CalendarPartOp::MonthName => value.format("%B").to_string(),
                _ => String::new(),
            })
        })
        .collect())
}

/// The explicit Year tag retains its original three text grammars and its
/// NULL result for values outside the calendar domain. Frozen carrier errors
/// are outer failures; they never manufacture an epoch or a default type.
pub fn legacy_year_array(
    array: &ArrayRef,
    expected_type: &DataType,
    selection: Selection<'_>,
) -> Result<ArrayRef, KernelFailure> {
    if !matches!(
        expected_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        return Err(part_contract(format_args!(
            "year: unsupported frozen output type: {expected_type:?}"
        )));
    }
    let mut values = Vec::with_capacity(selection.len());
    match array.data_type() {
        DataType::Date32 => {
            let source = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| part_contract("failed to downcast to Date32Array"))?;
            for row in selection.iter() {
                values.push(if source.is_null(row) {
                    None
                } else {
                    crate::datetime_value::date32_to_naive(source.value(row))
                        .map(|date| date.year())
                });
            }
        }
        DataType::Timestamp(unit, _) => {
            macro_rules! timestamp_years {
                ($array:ty) => {{
                    let source = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                        part_contract("year: frozen timestamp carrier cannot be downcast")
                    })?;
                    for row in selection.iter() {
                        values.push(if source.is_null(row) {
                            None
                        } else {
                            crate::datetime_value::timestamp_to_naive(unit, source.value(row))
                                .map(|date| date.year())
                        });
                    }
                }};
            }
            match unit {
                TimeUnit::Second => timestamp_years!(TimestampSecondArray),
                TimeUnit::Millisecond => timestamp_years!(TimestampMillisecondArray),
                TimeUnit::Microsecond => timestamp_years!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => timestamp_years!(TimestampNanosecondArray),
            }
        }
        DataType::Utf8 => {
            let source = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| part_contract("failed to downcast to StringArray"))?;
            for row in selection.iter() {
                values.push(if source.is_null(row) {
                    None
                } else {
                    let text = source.value(row);
                    NaiveDate::parse_from_str(text, "%Y-%m-%d")
                        .ok()
                        .map(|date| date.year())
                        .or_else(|| {
                            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                                .ok()
                                .map(|date| date.year())
                        })
                        .or_else(|| {
                            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                                .ok()
                                .map(|date| date.year())
                        })
                });
            }
        }
        other => {
            return Err(part_contract(format_args!(
                "year: unsupported input type: {other:?}"
            )));
        }
    }
    Ok(match expected_type {
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .into_iter()
                .map(|year| year.and_then(|year| i16::try_from(year).ok()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(values)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .into_iter()
                .map(|year| year.map(i64::from))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        _ => unreachable!("the frozen Year output was validated before reading input"),
    })
}

/// The raw host supplies the already frozen result carrier.
pub fn cast_calendar_array(out: ArrayRef, target: &DataType) -> Result<ArrayRef, KernelFailure> {
    if out.data_type() == target {
        return Ok(out);
    }
    arrow_cast::cast(&out, target).map_err(super::calendar_parts_failure::output)
}
pub fn part_contract(detail: impl std::fmt::Display) -> KernelFailure {
    super::calendar_parts_failure::contract(detail)
}

#[cfg(test)]
mod raw_year_tests {
    use super::*;
    #[test]
    fn raw_year_invalid_dates_and_negative_fractions_do_not_become_epoch_values() {
        let source: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            Some(0),
            Some(i32::MAX),
            None,
        ]));
        let output = legacy_year_array(&source, &DataType::Int32, Selection::all(5)).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![None, Some(1969), Some(1970), None, None])
        );
        for source in [
            Arc::new(TimestampSecondArray::from(vec![Some(-1), Some(0), None])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(-1),
                Some(0),
                None,
            ])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                Some(0),
                None,
            ])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                Some(0),
                None,
            ])) as ArrayRef,
        ] {
            let output = legacy_year_array(&source, &DataType::Int64, Selection::all(3)).unwrap();
            assert_eq!(
                output.as_any().downcast_ref::<Int64Array>().unwrap(),
                &Int64Array::from(vec![Some(1969), Some(1970), None])
            );
        }
        let source: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MIN, i64::MAX]));
        let output = legacy_year_array(&source, &DataType::Int16, Selection::all(2)).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int16Array>().unwrap(),
            &Int16Array::from(vec![None, None])
        );
    }
    #[test]
    fn raw_year_keeps_text_grammars_and_rejects_an_unrelated_frozen_output() {
        let source: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
            Some("2020-01-01"),
            Some("2021-02-03 04:05:06"),
            Some("2022-03-04 05:06:07.123"),
            Some("invalid"),
            None,
        ]));
        let output = legacy_year_array(&source, &DataType::Int16, Selection::all(5)).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int16Array>().unwrap(),
            &Int16Array::from(vec![Some(2020), Some(2021), Some(2022), None, None])
        );
        let error = legacy_year_array(&source, &DataType::Float64, Selection::all(5)).unwrap_err();
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
