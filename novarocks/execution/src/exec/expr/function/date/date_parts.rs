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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

use novarocks_functions::{
    Selection,
    builtin::calendar_parts_shared::{
        CalendarPartOp as DatePart, legacy_calendar_names, legacy_calendar_parts,
    },
};

#[inline]
fn eval_part_int(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let out = legacy_calendar_parts(part, &arr, Selection::all(arr.len()))?;
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    let expected_type = arena.data_type(expr).cloned().unwrap_or(DataType::Int64);
    if out.data_type() == &expected_type {
        return Ok(out);
    }
    cast(&out, &expected_type).map_err(|e| {
        format!(
            "date part output cast failed from {:?} to {:?}: {}",
            out.data_type(),
            expected_type,
            e
        )
    })
}

#[inline]
fn eval_part_string(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let out = legacy_calendar_names(part, &arr, Selection::all(arr.len()))?;
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_day(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Day)
}

pub fn eval_dayofmonth(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Day)
}

pub fn eval_dayofweek(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeek)
}

pub fn eval_dayofyear(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfYear)
}

pub fn eval_dayofweek_iso(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeekIso)
}

pub fn eval_weekday(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::WeekDay)
}

pub fn eval_dayname(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_string(arena, args, chunk, DatePart::DayName)
}

pub fn eval_hour(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Hour)
}

pub fn eval_minute(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Minute)
}

pub fn eval_month(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Month)
}

pub fn eval_monthname(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_string(arena, args, chunk, DatePart::MonthName)
}

pub fn eval_quarter(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Quarter)
}

pub fn eval_second(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Second)
}

pub fn eval_week(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::WeekOfYear)
}

pub fn eval_weekofyear(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::WeekOfYear)
}

pub fn eval_year(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::Year)
}

pub fn eval_yearweek(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_part_int(arena, expr, args, chunk, DatePart::YearWeek)
}
