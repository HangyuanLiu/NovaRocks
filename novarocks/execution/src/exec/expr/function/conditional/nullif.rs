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
use crate::exec::expr::ExprId;
use crate::runtime::fragment::ExecutionFailure;
use arrow::array::ArrayRef;

pub fn eval_nullif<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    let target = arena
        .data_type(expr)
        .ok_or_else(|| crate::exec::expr::expression_contract("nullif: missing output type"))?;
    let left_arr = arena.eval(left, chunk)?;
    let right_arr = arena.eval(right, chunk)?;
    novarocks_functions::builtin::nullif::evaluate_legacy(left_arr, right_arr, target)
        .map_err(ExecutionFailure::from)
}
