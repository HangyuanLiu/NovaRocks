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
use crate::exec::expr::{ExprArena, ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::KernelFailure;
use novarocks_functions::builtin::array_append_core::{
    AppendFailure, AppendOutputFacts, ArrayAppendInputs, append_observed,
};
fn append_error(error: AppendFailure<KernelFailure>) -> ExecutionFailure {
    match error {
        AppendFailure::Operation(cause) | AppendFailure::Control(cause) => {
            ExecutionFailure::from(cause)
        }
    }
}
pub fn eval_array_append<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 2 {
        return Err(expression_contract(
            "array_append requires two prepared arguments",
        ));
    }
    let arr = arena.eval(args[0], chunk)?;
    let target = arena.eval(args[1], chunk)?;
    let output = match arena.data_type(expr) {
        Some(DataType::List(field)) => AppendOutputFacts::FrozenList(field),
        None => AppendOutputFacts::OriginalInputList,
        Some(other) => {
            return Err(expression_contract(format_args!(
                "array_append output type must be List, got {other:?}"
            )));
        }
    };
    let mut observer = |_| Ok::<(), KernelFailure>(());
    let inputs =
        ArrayAppendInputs::new(arr, target, output, &mut observer).map_err(append_error)?;
    append_observed(
        &inputs,
        novarocks_functions::Selection::all(chunk.len()),
        |_, row| Ok::<_, KernelFailure>(inputs.direct_rows(row)),
        |_, _, _, _| Ok::<(), KernelFailure>(()),
        &mut observer,
    )
    .map_err(append_error)
}
