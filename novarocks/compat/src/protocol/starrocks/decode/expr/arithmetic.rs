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
use arrow::datatypes::DataType;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, function::FunctionKind};

use crate::thrift::exprs;
use crate::thrift::opcodes;

/// Lower ARITHMETIC_EXPR expression to arithmetic ExprNode.
pub(crate) fn lower_arithmetic(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let opcode = node
        .opcode
        .ok_or_else(|| "ARITHMETIC_EXPR missing opcode".to_string())?;

    let mut lower_bit_function = |name: &'static str, arity: usize| -> Result<ExprId, String> {
        if children.len() != arity {
            return Err(format!(
                "ARITHMETIC_EXPR {:?} expected {} children, got {}",
                opcode,
                arity,
                children.len()
            ));
        }
        Ok(arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: children.to_vec(),
            },
            data_type.clone(),
        ))
    };

    match opcode {
        o if o == opcodes::TExprOpcode::BITNOT => return lower_bit_function("bitnot", 1),
        o if o == opcodes::TExprOpcode::BITAND => return lower_bit_function("bitand", 2),
        o if o == opcodes::TExprOpcode::BITOR => return lower_bit_function("bitor", 2),
        o if o == opcodes::TExprOpcode::BITXOR => return lower_bit_function("bitxor", 2),
        o if o == opcodes::TExprOpcode::BIT_SHIFT_LEFT => {
            return lower_bit_function("bit_shift_left", 2);
        }
        o if o == opcodes::TExprOpcode::BIT_SHIFT_RIGHT => {
            return lower_bit_function("bit_shift_right", 2);
        }
        o if o == opcodes::TExprOpcode::BIT_SHIFT_RIGHT_LOGICAL => {
            return lower_bit_function("bit_shift_right_logical", 2);
        }
        _ => {}
    }

    if children.len() != 2 {
        return Err(format!(
            "ARITHMETIC_EXPR expected 2 children, got {}",
            children.len()
        ));
    }
    let left = children[0];
    let right = children[1];
    let id = match opcode {
        o if o == opcodes::TExprOpcode::ADD => {
            arena.push_typed(ExprNode::Add(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::SUBTRACT => {
            arena.push_typed(ExprNode::Sub(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::MULTIPLY => {
            arena.push_typed(ExprNode::Mul(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::DIVIDE => {
            arena.push_typed(ExprNode::Div(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::INT_DIVIDE => {
            arena.push_typed(ExprNode::Div(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::MOD => {
            arena.push_typed(ExprNode::Mod(left, right), data_type.clone())
        }
        _ => return Err(format!("unsupported ARITHMETIC_EXPR opcode: {:?}", opcode)),
    };
    Ok(id)
}
