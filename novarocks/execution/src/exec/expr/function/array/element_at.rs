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
use arrow::array::{Array, ArrayRef};
use novarocks_functions::KernelFailure;
use novarocks_functions::builtin::array_access_core::{
    AccessFailure, ArrayAccessInputs, lookup_observed,
};
fn access_error(error: AccessFailure<KernelFailure>) -> ExecutionFailure {
    match error {
        AccessFailure::Operation(cause) | AccessFailure::Control(cause) => {
            ExecutionFailure::from(cause)
        }
    }
}
pub fn eval_element_at<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if !matches!(args.len(), 2 | 3) {
        return Err(expression_contract(
            "element_at array requires 2 or 3 prepared arguments",
        ));
    }
    let arr = arena.eval(args[0], chunk)?;
    let subscript_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let mut observer = |_| Ok::<(), KernelFailure>(());
    let inputs = ArrayAccessInputs::new(arr, subscript_arr, check_arr, &mut observer)
        .map_err(access_error)?;
    inputs.validate_direct_lengths()?;
    lookup_observed(
        &inputs,
        novarocks_functions::Selection::all(inputs.list().len()),
        |_, row| Ok::<_, KernelFailure>(inputs.direct_rows(row)),
        arena.data_type(expr),
        |values, indices| {
            novarocks_functions::selected_copy::preflight_original_take(
                values,
                novarocks_functions::selected_copy::CopyIndexView::UInt32(indices),
                |_| Ok(()),
            )
            .map_err(novarocks_functions::selected_copy::CopyError::into_kernel)
        },
        &mut observer,
    )
    .map_err(access_error)
}
