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

//! The original clock-free UTC epoch conversion and selected NTZ projection.
use super::{calendar_extended::timestamp_value_for_type, calendar_extended_shared::CalendarInput};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

pub(super) fn split_epoch_value(value: i64, scale: i64) -> Option<(i64, u32)> {
    match scale {
        0 => Some((value, 0)),
        3 => {
            let secs = value.div_euclid(1_000);
            let micros = (value.rem_euclid(1_000) as u32) * 1_000;
            Some((secs, micros))
        }
        6 => {
            let secs = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            Some((secs, micros))
        }
        _ => None,
    }
}
pub(super) fn epoch_utc_datetime(seconds: i64, micros: u32) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(seconds, micros * 1_000)
}
pub(super) fn evaluate_epoch_ntz<'a>(
    input: CalendarInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let target = input.target;
        if target.logical_type != ValueLogicalType::Physical
            || !target.nullable
            || (!input.legacy
                && target.data_type != DataType::Timestamp(TimeUnit::Microsecond, None))
            || input.types.len() != input.arguments().len()
            || !(1..=2).contains(&input.types.len())
        {
            return Err(invalid(
                "to_datetime_ntz requires its exact one or two arguments and result",
            ));
        }
        let mut values = Vec::new();
        let mut types = Vec::new();
        values
            .try_reserve_exact(input.types.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        types
            .try_reserve_exact(input.types.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for (ty, argument) in input.types.iter().zip(input.arguments()) {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("to_datetime_ntz requires exact value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type != DataType::Int64
                || argument.array().data_type() != &DataType::Int64
            {
                return Err(invalid(
                    "to_datetime_ntz requires exact Int64 selected inputs",
                ));
            }
            values.push(
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("to_datetime expects int"))?,
            );
            types.push(ty);
            work.step()?;
        }
        let selection = input.selection();
        Layout::array::<Option<i64>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut projected = Vec::new();
        work.flush()?;
        projected
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let mut rows = [0; 2];
            let mut null = false;
            for (index, argument) in input.arguments().iter().enumerate() {
                rows[index] = argument.value_row(ordinal, batch_row);
                work.step()?;
                if rows[index] >= values[index].len() {
                    return Err(internal("to_datetime_ntz selected row is out of bounds"));
                }
                let is_null = values[index].is_null(rows[index]);
                if is_null && !types[index].nullable {
                    return Err(internal(
                        "to_datetime_ntz non-null selected input contains SQL NULL",
                    ));
                }
                null |= is_null;
                work.step()?;
            }
            let value = if null {
                None
            } else {
                let value = values[0].value(rows[0]);
                let scale = if values.len() == 1 {
                    0
                } else {
                    values[1].value(rows[1])
                };
                work.flush()?;
                let result = split_epoch_value(value, scale)
                    .and_then(|(seconds, micros)| epoch_utc_datetime(seconds, micros))
                    .and_then(|date| {
                        timestamp_value_for_type(date.naive_utc(), &target.data_type).ok()
                    });
                work.flush()?;
                result
            };
            projected.push(value);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(TimestampMicrosecondArray::from(projected)) as ArrayRef;
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
#[cfg(test)]
mod legacy_goldens {
    use super::*;
    #[test]
    fn epoch_ntz_split_keeps_euclidean_negative_fraction_and_exact_scale_domain() {
        assert_eq!(split_epoch_value(-1, 3), Some((-1, 999_000)));
        assert_eq!(split_epoch_value(-1, 6), Some((-1, 999_999)));
        assert_eq!(split_epoch_value(1, 1), None);
        assert_eq!(
            epoch_utc_datetime(-1, 999_999).unwrap().timestamp_micros(),
            -1
        );
    }
}
