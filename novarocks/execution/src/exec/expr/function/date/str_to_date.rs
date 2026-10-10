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
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarOperation, evaluate_legacy_calendar,
};

fn eval_str_to_date_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let text = arena.eval(args[0], chunk)?;
    let format = arena.eval(args[1], chunk)?;
    text.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_date expects string".to_string())?;
    format
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_date expects string".to_string())?;
    let rows = text.len();
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Date32);
    evaluate_legacy_calendar(
        CalendarOperation::StrToDate,
        &[text, format],
        &output_type,
        rows,
    )
}

pub fn eval_str_to_date(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_str_to_date_inner(arena, expr, args, chunk)
}

pub fn eval_str2date(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_str_to_date_inner(arena, expr, args, chunk)
}
