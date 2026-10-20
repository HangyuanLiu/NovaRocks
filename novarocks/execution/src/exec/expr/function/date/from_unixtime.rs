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
use super::common::extract_i64_array;
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use novarocks_functions::builtin::calendar_unixtime::{
    TimeZoneSpec, evaluate_legacy_from_unixtime, parse_tz,
};
fn project_local(utc: DateTime<Utc>) -> NaiveDateTime {
    utc.with_timezone(&Local).naive_local()
}
fn eval_from_unixtime_inner<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    units_per_second: i64,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if !(1..=3).contains(&args.len()) {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "from_unixtime requires one to three arguments",
            )
            .into(),
        );
    }
    let output_type = super::common::frozen_type(arena.arena(), expr)?.clone();
    if !matches!(
        output_type,
        DataType::Utf8 | DataType::Date32 | DataType::Timestamp(_, _)
    ) {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "from_unixtime has an unsupported frozen result type",
            )
            .into(),
        );
    }
    let values = extract_i64_array(&arena.eval(args[0], chunk)?, "from_unixtime")?;
    let has_format_arg = args.len() > 1;
    let format_array = if has_format_arg {
        Some(arena.eval(args[1], chunk)?)
    } else {
        None
    };
    let format = format_array
        .as_ref()
        .map(|array| {
            array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                novarocks_functions::builtin::calendar_parts_shared::part_contract(
                    "from_unixtime expects string format",
                )
            })
        })
        .transpose()?;
    let timezone_array = if has_format_arg && args.len() > 2 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let timezone = timezone_array
        .as_ref()
        .map(|array| {
            array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                novarocks_functions::builtin::calendar_parts_shared::part_contract(
                    "from_unixtime expects string timezone",
                )
            })
        })
        .transpose()?;
    let default_zone = arena
        .session_time_zone()
        .and_then(parse_tz)
        .unwrap_or(TimeZoneSpec::Local);
    evaluate_legacy_from_unixtime(
        &values,
        format,
        timezone,
        default_zone,
        units_per_second,
        &output_type,
        project_local,
    )
    .map_err(Into::into)
}
#[inline]
pub fn eval_from_unixtime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1)
}
pub fn eval_from_unixtime_ms<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1000)
}
