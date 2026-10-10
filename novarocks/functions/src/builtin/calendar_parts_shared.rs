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
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
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
) -> Result<Vec<Option<i64>>, String> {
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
) -> Result<Vec<Option<String>>, String> {
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

fn year_from_datetime(value: NaiveDateTime) -> i32 {
    CalendarPartOp::Year.extract_i64(value) as i32
}
fn year_from_date(value: NaiveDate) -> i32 {
    year_from_datetime(value.and_hms_opt(0, 0, 0).unwrap())
}

/// The old explicit Year tag's carrier admission and output contract.
/// This input projection intentionally retains unchecked Date32 arithmetic,
/// truncating fractional timestamps and the original invalid-value epoch.
pub fn legacy_year_array(
    array: &ArrayRef,
    expected_type: &DataType,
    selection: Selection<'_>,
) -> Result<ArrayRef, String> {
    let result: ArrayRef = match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;

            let values: Vec<Option<i32>> = selection
                .iter()
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        let days_since_epoch = arr.value(i);
                        // Date32 represents days since 1970-01-01
                        // 719163 is the Julian day number for 1970-01-01
                        let date = NaiveDate::from_num_days_from_ce_opt(719163 + days_since_epoch)
                            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                        Some(year_from_date(date))
                    }
                })
                .collect();

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        DataType::Timestamp(unit, _tz) => {
            let values: Vec<Option<i32>> = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    selection
                        .iter()
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let seconds = arr.value(i);
                                let dt_utc = DateTime::from_timestamp(seconds, 0)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(year_from_datetime(dt_utc.naive_utc()))
                            }
                        })
                        .collect()
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    selection
                        .iter()
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let millis = arr.value(i);
                                let seconds = millis / 1000;
                                let nanos = ((millis % 1000) * 1_000_000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(year_from_datetime(dt_utc.naive_utc()))
                            }
                        })
                        .collect()
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    selection
                        .iter()
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let micros = arr.value(i);
                                let seconds = micros / 1_000_000;
                                let nanos = ((micros % 1_000_000) * 1000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(year_from_datetime(dt_utc.naive_utc()))
                            }
                        })
                        .collect()
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    selection
                        .iter()
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let nanos_total = arr.value(i);
                                let seconds = nanos_total / 1_000_000_000;
                                let nanos = (nanos_total % 1_000_000_000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(year_from_datetime(dt_utc.naive_utc()))
                            }
                        })
                        .collect()
                }
            };

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        DataType::Utf8 => {
            // Parse string dates/timestamps
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;

            let values: Vec<Option<i32>> = selection
                .iter()
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i);
                        // Try to parse as date or timestamp
                        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            Some(year_from_date(date))
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        {
                            Some(year_from_date(dt.date()))
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        {
                            Some(year_from_date(dt.date()))
                        } else {
                            None // Return null for unparseable strings
                        }
                    }
                })
                .collect();

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        _ => {
            return Err(format!(
                "year: unsupported input type: {:?}",
                array.data_type()
            ));
        }
    };

    Ok(result)
}
