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

//! Original finite calendar construction and named-day algorithms.

use super::{calendar_extended::DateInput, calendar_extended_format::mysql_format_to_chrono};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues,
    datetime_value::{self, DateParseObservation},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::DataType;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Weekday};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CalendarParseOp {
    StrToDate,
    FromDays,
    LastDay,
    NextDay,
    PreviousDay,
}

fn weekyear_sunday(
    input: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<NaiveDateTime>, KernelFailure> {
    work.flush()?;
    let mut parts = input.split_whitespace();
    let year_week = parts.next();
    let weekday_name = parts.next();
    let extra = parts.next();
    work.flush()?;
    let (Some(year_week), Some(weekday_name), None) = (year_week, weekday_name, extra) else {
        return Ok(None);
    };
    if year_week.len() != 6 {
        return Ok(None);
    }
    for c in year_week.chars() {
        work.step()?;
        if !c.is_ascii_digit() {
            return Ok(None);
        }
    }
    work.flush()?;
    let year = year_week[0..4].parse::<i32>().ok();
    let week = year_week[4..6].parse::<u32>().ok();
    let weekday_name = weekday_name.to_ascii_lowercase();
    work.flush()?;
    let weekday = match weekday_name.as_str() {
        "sunday" => Weekday::Sun,
        "monday" => Weekday::Mon,
        "tuesday" => Weekday::Tue,
        "wednesday" => Weekday::Wed,
        "thursday" => Weekday::Thu,
        "friday" => Weekday::Fri,
        "saturday" => Weekday::Sat,
        _ => return Ok(None),
    };
    let (Some(year), Some(week)) = (year, week) else {
        return Ok(None);
    };
    if !(1..=53).contains(&week) {
        return Ok(None);
    }
    work.flush()?;
    let value = NaiveDate::from_ymd_opt(year, 1, 1).and_then(|jan1| {
        let first_sunday =
            jan1 + Duration::days(((7 - jan1.weekday().num_days_from_sunday()) % 7) as i64);
        let date = first_sunday
            + Duration::days((week as i64 - 1) * 7 + weekday.num_days_from_sunday() as i64);
        date.and_hms_opt(0, 0, 0)
    });
    work.flush()?;
    Ok(value)
}
fn parse_date_with_format(
    text: &str,
    format: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<NaiveDateTime>, KernelFailure> {
    work.flush()?;
    let weekyear = format.eq_ignore_ascii_case("%X%V %W");
    work.flush()?;
    if weekyear {
        return weekyear_sunday(text, work);
    }
    let format = mysql_format_to_chrono(format, work)?;
    work.flush()?;
    let datetime = NaiveDateTime::parse_from_str(text, &format).ok();
    work.flush()?;
    if datetime.is_some() {
        return Ok(datetime);
    }
    work.flush()?;
    let date = NaiveDate::parse_from_str(text, &format)
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0));
    work.flush()?;
    if date.is_some() {
        return Ok(date);
    }
    let mut observe = |event| match event {
        DateParseObservation::Step => work.step(),
        DateParseObservation::OpaqueBoundary => work.flush(),
    };
    let date = datetime_value::parse_date_observed(text, &mut observe)?;
    observe(DateParseObservation::OpaqueBoundary)?;
    let date = date.and_then(|date| date.and_hms_opt(0, 0, 0));
    observe(DateParseObservation::OpaqueBoundary)?;
    Ok(date)
}
fn end_of_month(year: i32, month: u32) -> Option<NaiveDate> {
    let (year, month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(year, month, 1)?.checked_sub_signed(Duration::days(1))
}
fn last_day(
    date: NaiveDate,
    unit: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<Option<NaiveDate>, &'static str>, KernelFailure> {
    work.flush()?;
    let unit = unit.to_ascii_lowercase();
    work.flush()?;
    let date = match unit.as_str() {
        "month" => end_of_month(date.year(), date.month()),
        "quarter" => end_of_month(date.year(), ((date.month() - 1) / 3 + 1) * 3),
        "year" => NaiveDate::from_ymd_opt(date.year(), 12, 31),
        _ => return Ok(Err("avaiable data_part parameter is year/month/quarter")),
    };
    work.flush()?;
    Ok(Ok(date))
}
fn named_day(
    date: Option<NaiveDate>,
    token: &str,
    next: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<Option<NaiveDate>, String>, KernelFailure> {
    let target = match token {
        "Mo" | "Mon" | "Monday" => Weekday::Mon,
        "Tu" | "Tue" | "Tuesday" => Weekday::Tue,
        "We" | "Wed" | "Wednesday" => Weekday::Wed,
        "Th" | "Thu" | "Thursday" => Weekday::Thu,
        "Fr" | "Fri" | "Friday" => Weekday::Fri,
        "Sa" | "Sat" | "Saturday" => Weekday::Sat,
        "Su" | "Sun" | "Sunday" => Weekday::Sun,
        _ => {
            let size = token
                .len()
                .checked_add(64)
                .ok_or(KernelFailure::ResourceExhausted)?;
            Layout::array::<u8>(size).map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            let error = format!(
                "{} not supported in {} dow_string backend",
                token,
                if next { "next_day" } else { "previous_day" }
            );
            work.flush()?;
            return Ok(Err(error));
        }
    };
    work.step()?;
    let Some(date) = date else {
        return Ok(Ok(None));
    };
    let current = date.weekday().num_days_from_monday() as i64;
    let target = target.num_days_from_monday() as i64;
    let distance = if next {
        (target - current + 7) % 7
    } else {
        (current - target + 7) % 7
    };
    let distance = if distance == 0 { 7 } else { distance };
    work.flush()?;
    let date = date
        .checked_add_signed(Duration::days(if next { distance } else { -distance }))
        .filter(|date| (0..=9999).contains(&date.year()));
    work.flush()?;
    Ok(Ok(date))
}

pub(super) fn evaluate_calendar_parse<'a>(
    op: CalendarParseOp,
    input: super::calendar_extended_shared::CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if op == CalendarParseOp::FromDays {
        return evaluate_from_days(input, control);
    }
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let arguments = input.arguments();
        let types = input.types;
        let target = input.target;
        if target.logical_type != ValueLogicalType::Physical
            || (target.data_type != DataType::Date32
                && !(input.legacy && op == CalendarParseOp::StrToDate))
            || !target.nullable
        {
            return Err(invalid(
                "calendar constructor requires its nullable Physical Date32 result",
            ));
        }
        if arguments.len() != types.len()
            || arguments.is_empty()
            || arguments.len() > 2
            || (op != CalendarParseOp::LastDay && arguments.len() != 2)
        {
            return Err(invalid(
                "calendar constructor requires its exact declared argument count",
            ));
        }
        let mut sources = Vec::new();
        sources
            .try_reserve_exact(types.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (ty, argument) in types.iter().zip(arguments) {
            let FunctionArgumentType::Value(source) = ty else {
                return Err(invalid("calendar constructor requires value arguments"));
            };
            if source.logical_type != ValueLogicalType::Physical
                || &source.data_type != argument.array().data_type()
            {
                return Err(invalid(
                    "calendar constructor argument differs from selected profile",
                ));
            }
            sources.push(source);
            work.step()?;
        }
        let texts = if op == CalendarParseOp::StrToDate {
            Some(
                arguments[0]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("str_to_date expects string"))?,
            )
        } else {
            None
        };
        let dates = if op == CalendarParseOp::StrToDate {
            None
        } else {
            Some(DateInput::date_for_input(
                sources[0],
                arguments[0].array().as_ref(),
                input.legacy,
            )?)
        };
        let tokens = if arguments.len() == 2 {
            Some(
                arguments[1]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("calendar constructor expects string token"))?,
            )
        } else {
            None
        };
        let selection = input.selection();
        Layout::array::<Option<NaiveDateTime>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        Layout::array::<RowDataError>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
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
            let mut null = false;
            for (index, argument) in arguments.iter().enumerate() {
                rows[index] = argument.value_row(ordinal, batch_row);
                work.step()?;
                if rows[index] >= argument.array().len() {
                    return Err(internal("calendar constructor selected row out of bounds"));
                }
                let is_null = argument.array().is_null(rows[index]);
                if is_null && !sources[index].nullable {
                    return Err(internal(
                        "calendar constructor non-null source contains SQL NULL",
                    ));
                }
                null |= is_null;
                work.step()?;
            }
            let date = if null {
                None
            } else if let Some(texts) = texts {
                parse_date_with_format(
                    texts.value(rows[0]),
                    tokens.unwrap().value(rows[1]),
                    &mut work,
                )?
            } else {
                let date = dates
                    .as_ref()
                    .unwrap()
                    .read(rows[0], &mut work)?
                    .map(|date| date.date());
                let result = if op == CalendarParseOp::LastDay {
                    if let Some(date) = date {
                        last_day(
                            date,
                            tokens.map_or("month", |tokens| tokens.value(rows[1])),
                            &mut work,
                        )?
                        .map_err(str::to_string)
                    } else {
                        Ok(None)
                    }
                } else {
                    named_day(
                        date,
                        tokens.unwrap().value(rows[1]),
                        op == CalendarParseOp::NextDay,
                        &mut work,
                    )?
                };
                match result {
                    Ok(value) => value.and_then(|date| date.and_hms_opt(0, 0, 0)),
                    Err(message) => {
                        work.flush()?;
                        errors.push(input.row_error(ordinal, &message)?);
                        work.flush()?;
                        None
                    }
                }
            };
            work.flush()?;
            // Keep the parsed datetime once; Date32 and timestamp are output projections.
            values.push(date);
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
                projected.push(value.and_then(|date| {
                    super::calendar_extended::timestamp_value_for_type(date, &target.data_type).ok()
                }));
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
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

pub(super) fn date_from_julian(julian: i32) -> Option<NaiveDate> {
    let a = julian + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = 100 * b + d - 4800 + (m / 10);
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}
pub(super) fn from_days_value(days: i64) -> Option<i32> {
    if days < i32::MIN as i64 || days > i32::MAX as i64 {
        return None;
    }
    let julian = if (0..=FROM_DAYS_MAX_VALID).contains(&days) {
        i32::try_from(crate::calendar_julian::BC_EPOCH_JULIAN as i64 + days).ok()?
    } else {
        crate::calendar_julian::BC_EPOCH_JULIAN - 32
    };
    if (0..=FROM_DAYS_MAX_VALID).contains(&days) {
        date_from_julian(julian)
            .map(|date| date.num_days_from_ce() - datetime_value::UNIX_EPOCH_DAY_OFFSET)
    } else {
        Some(zero_date_sentinel_date32())
    }
}
pub(super) const FROM_DAYS_MAX_VALID: i64 = 3_652_424;
pub(super) fn zero_date_sentinel_date32() -> i32 {
    date_from_julian(crate::calendar_julian::BC_EPOCH_JULIAN - 32).map_or(0, |date| {
        date.num_days_from_ce() - datetime_value::UNIX_EPOCH_DAY_OFFSET
    })
}
fn evaluate_from_days<'a>(
    input: super::calendar_extended_shared::CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (input.types, input.arguments())
        else {
            return Err(invalid("from_days requires one exact integer argument"));
        };
        let target = input.target;
        if source.logical_type != ValueLogicalType::Physical
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Date32
            || !target.nullable
            || argument.array().data_type() != &source.data_type
        {
            return Err(invalid(
                "from_days differs from its exact installed profile",
            ));
        }
        let narrow = if source.data_type == DataType::Int32 {
            Some(
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        internal("from_days Int32 selected carrier cannot be downcast")
                    })?,
            )
        } else {
            None
        };
        let wide = if source.data_type == DataType::Int64 {
            Some(
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        internal("from_days Int64 selected carrier cannot be downcast")
                    })?,
            )
        } else {
            None
        };
        if narrow.is_none() && wide.is_none() {
            return Err(invalid("from_days expects int"));
        }
        let selection = input.selection();
        Layout::array::<Option<i32>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= argument.array().len() {
                return Err(internal("from_days selected row out of bounds"));
            }
            let is_null = argument.array().is_null(row);
            if is_null && !source.nullable {
                return Err(internal(
                    "from_days non-null source contains selected SQL NULL",
                ));
            }
            let value = if is_null {
                None
            } else {
                let days = narrow.map_or_else(
                    || wide.unwrap().value(row),
                    |values| values.value(row) as i64,
                );
                work.step()?;
                work.flush()?;
                let value = from_days_value(days);
                work.flush()?;
                value
            };
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(Date32Array::from(values)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
