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

//! CASE expression lowering.

use arrow::datatypes::DataType;

use super::{lower_proto_expr, lower_required_unboxed_child};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::expr;

use super::super::layout::Layout;

pub(crate) fn lower_case(
    case_expr: &expr::CaseExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let mut children = Vec::with_capacity(
        usize::from(case_expr.operand.is_some())
            + case_expr.when_then.len() * 2
            + usize::from(case_expr.else_expr.is_some()),
    );
    if let Some(operand) = &case_expr.operand {
        children.push(lower_proto_expr(operand, arena, input_layout)?);
    }
    for (idx, branch) in case_expr.when_then.iter().enumerate() {
        children.push(lower_required_unboxed_child(
            &branch.when,
            &format!("Case.when_then[{idx}].when"),
            arena,
            input_layout,
        )?);
        children.push(lower_required_unboxed_child(
            &branch.then,
            &format!("Case.when_then[{idx}].then"),
            arena,
            input_layout,
        )?);
    }
    if let Some(else_expr) = &case_expr.else_expr {
        children.push(lower_proto_expr(else_expr, arena, input_layout)?);
    }
    Ok(arena.push_typed(
        ExprNode::Case {
            has_case_expr: case_expr.operand.is_some(),
            has_else_expr: case_expr.else_expr.is_some(),
            children,
        },
        data_type,
    ))
}
