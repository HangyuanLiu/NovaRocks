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
//! Original Unix epoch/format computation; Local authority stays in the v1 input shell.
use super::calendar_extended_shared::legacy_to_timestamp_value;
use crate::datetime_value::{naive_to_date32, parse_date, parse_datetime};
use crate::{KernelFailure, kernel_input::EvaluationCheckpoints};
use arrow_array::{
    ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Timelike, Utc};
use chrono_tz::Tz;
use std::{str::FromStr, sync::Arc};
pub(super) const DEFAULT_FROM_UNIXTIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const MAX_FROM_UNIXTIME_SECONDS: i64 = 253_402_243_199;
const MAX_FROM_UNIXTIME_FORMAT_LEN: usize = 128;

#[derive(Clone, Copy, Debug)]
pub enum TimeZoneSpec {
    Local,
    Fixed(FixedOffset),
    Named(Tz),
}

fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
    if let Ok(offset) = FixedOffset::from_str(s) {
        return Some(offset);
    }

    let sign = match s.as_bytes().first().copied() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let rest = &s[1..];
    let (hours, minutes) = rest.split_once(':')?;
    let hours = hours.parse::<i32>().ok()?;
    let minutes = minutes.parse::<i32>().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

pub fn parse_tz(s: &str) -> Option<TimeZoneSpec> {
    if s.eq_ignore_ascii_case("local") {
        return Some(TimeZoneSpec::Local);
    }
    if s.eq_ignore_ascii_case("utc") {
        return FixedOffset::east_opt(0).map(TimeZoneSpec::Fixed);
    }
    if let Some(offset) = parse_fixed_offset(s) {
        return Some(TimeZoneSpec::Fixed(offset));
    }
    Tz::from_str(s).ok().map(TimeZoneSpec::Named)
}

fn normalize_mysql_from_unixtime_format(fmt: &str) -> Option<String> {
    let mut out = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            if ch.is_ascii_alphabetic() {
                return None;
            }
            out.push(ch);
            continue;
        }

        let spec = chars.next()?;
        match spec {
            'Y' => out.push_str("%Y"),
            'm' => out.push_str("%m"),
            'd' => out.push_str("%d"),
            'H' => out.push_str("%H"),
            'i' => out.push_str("%M"),
            's' | 'S' => out.push_str("%S"),
            '%' => out.push('%'),
            _ => return None,
        }
    }
    Some(out)
}

pub(super) fn normalize_from_unixtime_format(fmt: &str) -> Option<String> {
    if fmt.is_empty() || fmt.len() > MAX_FROM_UNIXTIME_FORMAT_LEN {
        return None;
    }
    match fmt {
        "yyyy-MM-dd HH:mm:ss" => Some(DEFAULT_FROM_UNIXTIME_FORMAT.to_string()),
        "yyyy-MM-dd" => Some("%Y-%m-%d".to_string()),
        "yyyyMMdd" => Some("%Y%m%d".to_string()),
        _ => normalize_mysql_from_unixtime_format(fmt),
    }
}

fn epoch_value_to_datetime(
    value: i64,
    units_per_second: i64,
    timezone: TimeZoneSpec,
    project_local: Option<fn(DateTime<Utc>) -> NaiveDateTime>,
) -> Option<NaiveDateTime> {
    if value < 0 || units_per_second <= 0 || 1_000_000_000 % units_per_second != 0 {
        return None;
    }

    let seconds = value.div_euclid(units_per_second);
    if !(0..=MAX_FROM_UNIXTIME_SECONDS).contains(&seconds) {
        return None;
    }

    let remainder = value.rem_euclid(units_per_second) as u32;
    let nanos = remainder.checked_mul((1_000_000_000 / units_per_second) as u32)?;
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, nanos)?;
    Some(match timezone {
        TimeZoneSpec::Local => project_local?(dt_utc),
        TimeZoneSpec::Fixed(offset) => dt_utc.with_timezone(&offset).naive_local(),
        TimeZoneSpec::Named(tz) => dt_utc.with_timezone(&tz).naive_local(),
    })
}

fn build_timestamp_array(
    values: Vec<Option<i64>>,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (unit, tz) = match output_type {
        DataType::Timestamp(unit, tz) => (*unit, tz.as_deref().map(|s| s.to_string())),
        other => {
            return Err(format!(
                "from_unixtime unsupported output type: {:?}",
                other
            ));
        }
    };

    let array: ArrayRef = match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
    };
    Ok(array)
}

#[derive(Debug)]
enum UnixtimeValue {
    Direct(NaiveDateTime),
    Formatted(String),
}
fn from_unixtime_row<'a, F: FnOnce() -> Option<&'a str>, T: FnOnce() -> Option<&'a str>>(
    value: Option<i64>,
    format: Option<F>,
    timezone: Option<T>,
    default_zone: TimeZoneSpec,
    units: i64,
    local: Option<fn(DateTime<Utc>) -> NaiveDateTime>,
) -> Option<UnixtimeValue> {
    let value = value?;
    if let Some(format) = format {
        let format = normalize_from_unixtime_format(format()?)?;
        let zone = if let Some(timezone) = timezone {
            parse_tz(timezone()?)?
        } else {
            default_zone
        };
        let dt = epoch_value_to_datetime(value, units, zone, local)?;
        let rendered = render_datetime(dt, &format);
        if rendered.len() > MAX_FROM_UNIXTIME_FORMAT_LEN {
            return None;
        }
        Some(UnixtimeValue::Formatted(rendered))
    } else {
        epoch_value_to_datetime(value, units, default_zone, local).map(UnixtimeValue::Direct)
    }
}
fn render_datetime(date: NaiveDateTime, format: &str) -> String {
    date.format(format).to_string()
}
fn project_utf8(value: UnixtimeValue) -> String {
    match value {
        UnixtimeValue::Direct(dt) => render_datetime(dt, DEFAULT_FROM_UNIXTIME_FORMAT),
        UnixtimeValue::Formatted(s) => s,
    }
}
fn project_date(value: UnixtimeValue) -> Option<i32> {
    match value {
        UnixtimeValue::Direct(dt) => Some(naive_to_date32(dt.date())),
        UnixtimeValue::Formatted(s) => parse_datetime(&s)
            .map(|d| naive_to_date32(d.date()))
            .or_else(|| parse_date(&s).map(naive_to_date32)),
    }
}
fn project_timestamp(value: UnixtimeValue, ty: &DataType) -> Option<i64> {
    let dt = match value {
        UnixtimeValue::Direct(d) => Some(d),
        UnixtimeValue::Formatted(s) => {
            parse_datetime(&s).or_else(|| parse_date(&s).and_then(|d| d.and_hms_opt(0, 0, 0)))
        }
    };
    dt.and_then(|d| legacy_to_timestamp_value(d, ty).ok())
}
/// Raw admission/whole-array decoding has already been performed by the original shell.
pub fn evaluate_legacy_from_unixtime(
    values: &[Option<i64>],
    format: Option<&StringArray>,
    timezone: Option<&StringArray>,
    default_zone: TimeZoneSpec,
    units: i64,
    output: &DataType,
    project_local: fn(DateTime<Utc>) -> NaiveDateTime,
) -> Result<ArrayRef, String> {
    use arrow_array::Array;
    let rows = values
        .iter()
        .copied()
        .enumerate()
        .map(|(i, v)| {
            from_unixtime_row(
                v,
                format.map(|a| move || if a.is_null(i) { None } else { Some(a.value(i)) }),
                timezone.map(|a| move || if a.is_null(i) { None } else { Some(a.value(i)) }),
                default_zone,
                units,
                Some(project_local),
            )
        })
        .collect::<Vec<_>>();
    match output {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            rows.into_iter()
                .map(|r| r.map(project_utf8))
                .collect::<Vec<_>>(),
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            rows.into_iter()
                .map(|r| r.and_then(project_date))
                .collect::<Vec<_>>(),
        ))),
        DataType::Timestamp(_, _) => build_timestamp_array(
            rows.into_iter()
                .map(|r| r.and_then(|r| project_timestamp(r, output)))
                .collect(),
            output,
        ),
        other => Err(format!(
            "from_unixtime unsupported output type: {:?}",
            other
        )),
    }
}
fn hour_from_unixtime_row(
    value: Option<i64>,
    project_local: fn(i64) -> Option<NaiveDateTime>,
) -> Option<i64> {
    let seconds = value?;
    if !(0..=MAX_FROM_UNIXTIME_SECONDS).contains(&seconds) {
        return None;
    }
    project_local(seconds).map(|dt| dt.hour() as i64)
}
pub fn evaluate_legacy_hour_from_unixtime(
    values: &[Option<i64>],
    project_local: fn(i64) -> Option<NaiveDateTime>,
) -> ArrayRef {
    Arc::new(arrow_array::Int64Array::from(
        values
            .iter()
            .copied()
            .map(|v| hour_from_unixtime_row(v, project_local))
            .collect::<Vec<_>>(),
    ))
}

fn exact_format<'a>(
    actual: Option<&'a str>,
    constant: &crate::ConstantValue,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    use crate::kernel_control::invalid;
    use arrow_array::Array;
    let expected = constant
        .pool()
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("from_unixtime lost its exact prepared format carrier"))?;
    let row = constant.ordinal() as usize;
    let expected = if expected.is_null(row) {
        None
    } else {
        Some(expected.value(row))
    };
    let same = match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) if actual.len() == expected.len() => {
            let mut same = true;
            for (a, b) in actual
                .as_bytes()
                .chunks(256)
                .zip(expected.as_bytes().chunks(256))
            {
                work.flush()?;
                if a != b {
                    same = false;
                    break;
                }
                work.flush()?;
                for _ in a {
                    work.step()?;
                }
            }
            same
        }
        _ => false,
    };
    if !same {
        return Err(invalid(
            "from_unixtime format differs from its exact prepared constant",
        ));
    }
    Ok(())
}
/// A frozen, non-Local zone and exact constant-format proof are preparation facts.
/// No parameter table, wall clock, Local rule or AST is looked up here.
pub(super) fn evaluate_selected<'a>(
    input: crate::ScalarCallInput<'_, 'a>,
    zone: TimeZoneSpec,
    format_proof: Option<&crate::ConstantValue>,
    control: &dyn crate::KernelEvaluationControl,
) -> Result<crate::SelectedValues<'a>, KernelFailure> {
    use crate::{FunctionArgumentType, SelectedValues, ValueLogicalType, kernel_control::invalid};
    use arrow_array::{Array, Int64Array};
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if matches!(zone, TimeZoneSpec::Local) {
            return Err(invalid(
                "from_unixtime has no frozen ProcessLocal timezone rule author",
            ));
        }
        let types = &input.contract().selected().argument_types;
        let arguments = input.arguments();
        let target = input.contract().result_type();
        let [FunctionArgumentType::Value(source), rest @ ..] = types.as_ref() else {
            return Err(invalid("from_unixtime requires its exact value arguments"));
        };
        if source.logical_type != ValueLogicalType::Physical
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "from_unixtime differs from its exact declared profile",
            ));
        }
        let has_format = match (source.data_type.clone(), rest, format_proof) {
            (DataType::Int64, [], None) => false,
            (DataType::Utf8, [FunctionArgumentType::Value(format)], Some(_))
                if format.logical_type == ValueLogicalType::Physical
                    && format.data_type == DataType::Utf8 =>
            {
                true
            }
            _ => {
                return Err(invalid(
                    "from_unixtime refuses unsupported raw integer carrier or unproven format",
                ));
            }
        };
        let argument = arguments[0];
        let integer = argument.array().as_any().downcast_ref::<Int64Array>();
        let text = argument.array().as_any().downcast_ref::<StringArray>();
        let format = if has_format {
            Some(
                arguments[1]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("from_unixtime expects string format"))?,
            )
        } else {
            None
        };
        let selection = input.selection();
        std::alloc::Layout::array::<Option<String>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let source_row = argument.value_row(ordinal, row);
            if source_row >= argument.array().len() {
                return Err(invalid(
                    "from_unixtime selected epoch address is outside its array",
                ));
            }
            let value = if argument.array().is_null(source_row) {
                None
            } else if let Some(integer) = integer {
                Some(integer.value(source_row))
            } else if let Some(text) = text {
                work.flush()?;
                let value =
                    super::calendar_add_interval::parse_i64_from_utf8(text.value(source_row));
                work.flush()?;
                value
            } else {
                return Err(invalid("from_unixtime has no exact integer reader"));
            };
            let format_value = if let Some(format) = format {
                let at = arguments[1].value_row(ordinal, row);
                if at >= format.len() {
                    return Err(invalid(
                        "from_unixtime selected format address is outside its array",
                    ));
                }
                let value = if format.is_null(at) {
                    None
                } else {
                    Some(format.value(at))
                };
                exact_format(
                    value,
                    format_proof.expect("validated prepared format"),
                    &mut work,
                )?;
                Some(value)
            } else {
                None
            };
            work.flush()?;
            let value = from_unixtime_row(
                value,
                format_value.map(|f| move || f),
                None::<fn() -> Option<&'a str>>,
                zone,
                1,
                None,
            )
            .map(project_utf8);
            work.flush()?;
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let output = Arc::new(StringArray::from(values)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            output,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[cfg(test)]
#[path = "calendar_unixtime_tests.rs"]
mod tests;
