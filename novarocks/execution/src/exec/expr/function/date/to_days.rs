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
use super::common::extract_date_array;
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use arrow::array::{ArrayRef, Int64Array};
use novarocks_functions::calendar_julian::day_number_from_date;
use std::sync::Arc;

pub fn eval_to_days<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    super::common::require_frozen_type(arena.arena(), expr, &arrow::datatypes::DataType::Int64)?;
    if args.len() != 1 {
        return Err(
            novarocks_functions::builtin::calendar_parts_shared::part_contract(
                "to_days has an incompatible argument count",
            )
            .into(),
        );
    }
    let arr = arena.eval(args[0], chunk)?;
    let dates = extract_date_array(&arr)?;
    let mut out = Vec::with_capacity(dates.len());
    for date in dates {
        let v = date.map(day_number_from_date);
        out.push(v);
    }
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}
