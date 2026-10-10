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
//! The original conversion between explicit offset/named timezone arguments.
use super::{
    calendar_extended::{DateInput, timestamp_value_for_type},
    calendar_extended_shared::CalendarInput,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{FixedOffset, TimeZone, Utc};
use chrono_tz::Tz;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, str::FromStr, sync::Arc};

#[derive(Clone, Copy)]
enum TimeZoneSpec {
    Fixed(FixedOffset),
    Named(Tz),
}

fn parse_tz(s: &str) -> Option<TimeZoneSpec> {
    if s.eq_ignore_ascii_case("utc") {
        return FixedOffset::east_opt(0).map(TimeZoneSpec::Fixed);
    }
    if let Ok(offset) = FixedOffset::from_str(s) {
        return Some(TimeZoneSpec::Fixed(offset));
    }
    Tz::from_str(s).ok().map(TimeZoneSpec::Named)
}

fn to_utc_datetime(dt: chrono::NaiveDateTime, from: TimeZoneSpec) -> Option<chrono::DateTime<Utc>> {
    match from {
        TimeZoneSpec::Fixed(offset) => offset
            .from_local_datetime(&dt)
            .single()
            .map(|v| v.with_timezone(&Utc)),
        TimeZoneSpec::Named(tz) => tz
            .from_local_datetime(&dt)
            .single()
            .or_else(|| tz.from_local_datetime(&dt).earliest())
            .or_else(|| tz.from_local_datetime(&dt).latest())
            .map(|v| v.with_timezone(&Utc)),
    }
}

fn convert_tz_with_zone(
    dt: chrono::NaiveDateTime,
    from: TimeZoneSpec,
    to: TimeZoneSpec,
) -> Option<chrono::NaiveDateTime> {
    let utc = to_utc_datetime(dt, from)?;
    let out = match to {
        TimeZoneSpec::Fixed(offset) => utc.with_timezone(&offset).naive_local(),
        TimeZoneSpec::Named(tz) => utc.with_timezone(&tz).naive_local(),
    };
    Some(out)
}

pub(super) fn evaluate_convert_tz<'a>(
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(date_type),
                FunctionArgumentType::Value(from_type),
                FunctionArgumentType::Value(to_type),
            ],
            [date_arg, from_arg, to_arg],
        ) = (input.types, input.arguments())
        else {
            return Err(invalid(
                "convert_tz requires its exact three value arguments",
            ));
        };
        let types = [date_type, from_type, to_type];
        let args = [date_arg, from_arg, to_arg];
        for (ty, arg) in types.iter().zip(args) {
            if ty.logical_type != ValueLogicalType::Physical
                || &ty.data_type != arg.array().data_type()
            {
                return Err(invalid(
                    "convert_tz input differs from its exact Physical profile",
                ));
            }
            work.step()?;
        }
        if input.target.logical_type != ValueLogicalType::Physical
            || !input.target.nullable
            || from_type.data_type != DataType::Utf8
            || to_type.data_type != DataType::Utf8
            || (!input.legacy
                && (date_type.data_type != DataType::Timestamp(TimeUnit::Microsecond, None)
                    || input.target.data_type != DataType::Timestamp(TimeUnit::Microsecond, None)))
        {
            return Err(invalid(
                "convert_tz differs from its exact declared profile",
            ));
        }
        let dates = DateInput::for_input(date_type, date_arg.array().as_ref(), input)?;
        let from = from_arg
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("convert_tz expects string"))?;
        let to = to_arg
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("convert_tz expects string"))?;
        let selection = input.selection();
        Layout::array::<Option<i64>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut values = Vec::new();
        work.flush()?;
        values
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let mut rows = [0; 3];
            let mut nulls = [false; 3];
            for index in 0..3 {
                rows[index] = args[index].value_row(ordinal, batch_row);
                work.step()?;
                if rows[index] >= args[index].array().len() {
                    return Err(internal("convert_tz selected row is out of bounds"));
                }
                nulls[index] = args[index].array().is_null(rows[index]);
                if nulls[index] && !types[index].nullable {
                    return Err(internal("convert_tz non-null input contains SQL NULL"));
                }
                work.step()?;
            }
            let value = if nulls[1] || nulls[2] {
                None
            } else {
                let date = if nulls[0] {
                    None
                } else {
                    dates.read(rows[0], &mut work)?
                };
                let from_text = from.value(rows[1]);
                let to_text = to.value(rows[2]);
                for _ in from_text
                    .as_bytes()
                    .chunks(32)
                    .chain(to_text.as_bytes().chunks(32))
                {
                    work.step()?;
                }
                work.flush()?;
                let from = parse_tz(from_text);
                let to = parse_tz(to_text);
                work.flush()?;
                let result = match (date, from, to) {
                    (Some(date), Some(from), Some(to)) => convert_tz_with_zone(date, from, to)
                        .and_then(|date| {
                            timestamp_value_for_type(date, &input.target.data_type).ok()
                        }),
                    _ => None,
                };
                work.flush()?;
                result
            };
            values.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
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
