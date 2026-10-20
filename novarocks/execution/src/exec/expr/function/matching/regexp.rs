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
use crate::exec::expr::function::pattern_memo::PatternMemo;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 2 {
        return Err(expression_contract(
            "regexp requires exactly two prepared arguments",
        ));
    }
    let _ = expr;
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;

    let left = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("regexp expects VARCHAR as first argument"))?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("regexp expects VARCHAR as second argument"))?;

    let mut patterns = PatternMemo::new();
    if left.len() != chunk.len() || right.len() != chunk.len() {
        return Err(expression_contract(
            "regexp prepared argument lengths differ from their batch",
        ));
    }
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
            continue;
        }

        let regex = patterns
            .get_or_compile(right.value(row), |pattern| {
                Regex::new(pattern).map_err(Arc::new)
            })
            .map_err(|source| {
                ExecutionFailure::from(
                    novarocks_functions::builtin::required_regex_compile_failure(
                        row,
                        Arc::clone(source),
                    ),
                )
            })?;
        out.push(Some(regex.is_match(left.value(row))));
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}
