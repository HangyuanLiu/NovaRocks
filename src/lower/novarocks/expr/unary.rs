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

//! Unary operation expression lowering.

use arrow::datatypes::DataType;

use super::{function_call, literal, lower_required_child};
use crate::exec::expr::function::lookup_function;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::expr;

use super::super::layout::Layout;

pub(crate) fn lower_unary_op(
    unary: &expr::UnaryOpExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let op =
        expr::UnaryOp::try_from(unary.op).map_err(|_| format!("unknown UnaryOp {}", unary.op))?;
    let operand = lower_required_child(&unary.operand, "UnaryOp.operand", arena, input_layout)?;
    match op {
        expr::UnaryOp::Unspecified => Err("UnaryOp.op is unspecified".to_string()),
        expr::UnaryOp::Not => Ok(arena.push_typed(ExprNode::Not(operand), data_type)),
        expr::UnaryOp::Negate => {
            let zero_type = arena
                .data_type(operand)
                .cloned()
                .unwrap_or_else(|| data_type.clone());
            let zero = literal::push_zero_literal(arena, &zero_type)?;
            Ok(arena.push_typed(ExprNode::Sub(zero, operand), data_type))
        }
        expr::UnaryOp::BitwiseNot => {
            let kind = lookup_function("bitnot")
                .ok_or_else(|| "BITWISE_NOT requires bitnot function support".to_string())?;
            function_call::validate_function_arity("bitnot", kind, 1)?;
            Ok(arena.push_typed(
                ExprNode::FunctionCall {
                    kind,
                    args: vec![operand],
                },
                data_type,
            ))
        }
    }
}
