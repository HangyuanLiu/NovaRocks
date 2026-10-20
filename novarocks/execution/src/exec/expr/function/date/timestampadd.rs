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
use super::common::{extract_datetime_array, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::Duration;
use std::sync::Arc;

pub fn eval_timestampadd<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 3 {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "timestampadd requires three arguments",
            )
            .into(),
        );
    }
    super::common::require_frozen_type(
        arena.arena(),
        expr,
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let unit_arr = arena.eval(args[0], chunk)?;
    let interval_arr = arena.eval(args[1], chunk)?;
    let dt_arr = arena.eval(args[2], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "timestampadd expects unit string",
            )
        })?;
    let interval_arr = interval_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "timestampadd expects int interval",
            )
        })?;
    let dts = extract_datetime_array(&dt_arr)?;
    if unit_arr.len() != dts.len() || interval_arr.len() != dts.len() {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "timestampadd argument lengths differ",
            )
            .into(),
        );
    }
    let mut out = Vec::with_capacity(dts.len());
    for (i, dt) in dts.iter().copied().enumerate() {
        if unit_arr.is_null(i) || interval_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let dt = match dt {
            Some(v) => v,
            None => {
                out.push(None);
                continue;
            }
        };
        let interval = interval_arr.value(i);
        let unit = unit_arr.value(i).to_lowercase();
        let new_dt = match unit.as_str() {
            "year" => super::add_months::add_months_to_datetime(dt, interval as i32 * 12),
            "month" => super::add_months::add_months_to_datetime(dt, interval as i32),
            "week" => dt + Duration::weeks(interval),
            "day" => dt + Duration::days(interval),
            "hour" => dt + Duration::hours(interval),
            "minute" => dt + Duration::minutes(interval),
            "second" => dt + Duration::seconds(interval),
            "millisecond" => dt + Duration::milliseconds(interval),
            "microsecond" => dt + Duration::microseconds(interval),
            _ => dt,
        };
        out.push(to_timestamp_value(new_dt, TimeUnit::Microsecond).ok());
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}
