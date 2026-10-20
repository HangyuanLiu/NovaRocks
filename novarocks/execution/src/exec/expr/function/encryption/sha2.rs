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
use crate::exec::expr::expression_contract;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::ArrayRef;
use novarocks_functions::builtin::sha2_shared;

pub fn eval_sha2<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 2 {
        return Err(expression_contract(
            "sha2 requires exactly 2 prepared argument(s)",
        ));
    }
    let _ = expr;
    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "sha2", 0)?;
    let length = super::common::to_i64_array(&arena.eval(args[1], chunk)?, "sha2", 1)?;
    if input.len() < chunk.len() || length.len() < chunk.len() {
        return Err(expression_contract(
            "sha2 prepared input is shorter than its batch",
        ));
    }
    Ok(sha2_shared::evaluate_legacy(&input, &length, chunk.len()))
}
