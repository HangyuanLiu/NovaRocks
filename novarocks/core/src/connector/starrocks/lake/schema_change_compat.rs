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

use super::schema_change::RollupExprInput;

pub(super) fn eval_rollup_expr(
    expr: &crate::thrift::exprs::TExpr,
    eval_input: &RollupExprInput,
    expr_context: &str,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    let mut arena = crate::exec::expr::ExprArena::default();
    let layout = crate::protocol::starrocks::decode::layout::Layout {
        order: eval_input.layout.order.clone(),
        index: eval_input.layout.index.clone(),
    };
    let expr_id = crate::protocol::starrocks::decode::decode_expression_for_layout(
        expr,
        &mut arena,
        &layout,
    )
    .map_err(|e| {
                format!(
                    "rollup lower expression failed: rowset_idx={} target_index={} target_name={} context={} error={}",
                    rowset_idx, target_idx, target_name, expr_context, e
                )
            })?;
    arena.eval(expr_id, &eval_input.chunk).map_err(|e| {
        format!(
            "rollup evaluate expression failed: rowset_idx={} target_index={} target_name={} context={} error={}",
            rowset_idx, target_idx, target_name, expr_context, e
        )
    })
}
