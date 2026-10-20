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

use arrow::array::ArrayRef;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;

pub fn eval_json_object<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.is_empty() {
        return novarocks_functions::json_object_core::evaluate_original(
            &[],
            novarocks_functions::Selection::all(chunk.len()),
        )
        .map_err(crate::runtime::fragment::ExecutionFailure::from_domain);
    }
    let mut arg_arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for arg in args {
        arg_arrays.push(arena.eval(*arg, chunk)?);
    }
    novarocks_functions::json_object_core::evaluate_original(
        &arg_arrays,
        novarocks_functions::Selection::all(chunk.len()),
    )
    .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)
}
