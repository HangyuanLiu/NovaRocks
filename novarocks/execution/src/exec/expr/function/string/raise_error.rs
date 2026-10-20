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
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use std::sync::Arc;

pub fn eval_raise_error<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let _ = expr;
    let [argument] = args else {
        return Err(expression_contract(
            "raise_error requires exactly one prepared argument",
        ));
    };
    let input = arena.eval(*argument, chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("raise_error expects string"))?;
    // Any non-null value raises an error with that message.
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            return Err(ExecutionFailure::from_domain(RaisedByValue(
                novarocks_failure_contract::BoundedDetail::truncating(arr.value(i)),
            )));
        }
    }
    // All inputs are NULL: return a null boolean column of the same length.
    let nulls: Vec<Option<bool>> = vec![None; arr.len()];
    Ok(Arc::new(BooleanArray::from(nulls)) as ArrayRef)
}

#[derive(Debug)]
struct RaisedByValue(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for RaisedByValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RaisedByValue {}
impl novarocks_failure_contract::FailureProjection for RaisedByValue {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            ExecutionReason::StringRaisedByValue.reason(),
        )
    }
}
