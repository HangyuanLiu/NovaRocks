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
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarOperation, evaluate_legacy_calendar, legacy_validate_datetime_source,
};
use std::sync::Arc;

pub(super) fn add_months_to_datetime(date: NaiveDateTime, months: i32) -> NaiveDateTime {
    novarocks_functions::builtin::calendar_extended_shared::legacy_add_months_to_datetime(
        date, months,
    )
}
fn eval_with_factor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    factor: i32,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let output_type = super::common::frozen_type(arena.arena(), expr)?.clone();
    let date = arena.eval(args[0], chunk)?;
    let interval = arena.eval(args[1], chunk)?;
    legacy_validate_datetime_source(&date)?;
    let intervals = extract_i64_array(&interval, "add_months")?;
    let rows = date.len().max(intervals.len());
    let interval: ArrayRef = Arc::new(Int64Array::from(intervals));

    evaluate_legacy_calendar(
        CalendarOperation::MonthsShift(factor),
        &[date, interval],
        &output_type,
        rows,
    )
    .map_err(Into::into)
}

pub fn eval_add_months<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_months_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_months_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, -1)
}

pub fn eval_quarters_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, 3)
}

pub fn eval_quarters_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, -3)
}

pub fn eval_years_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, 12)
}

pub fn eval_years_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_with_factor(arena, expr, args, chunk, -12)
}
