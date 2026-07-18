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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::datatypes::DataType;

use crate::thrift::exprs;
use crate::thrift::opcodes;

/// Lower COMPOUND_PRED expression to logical ExprNode.
pub(crate) fn lower_compound_pred(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let opcode = node
        .opcode
        .ok_or_else(|| "COMPOUND_PRED missing opcode".to_string())?;
    let id = match opcode {
        o if o == opcodes::TExprOpcode::COMPOUND_NOT => {
            if children.len() != 1 {
                return Err(format!(
                    "COMPOUND_NOT expected 1 child, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::Not(children[0]), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::COMPOUND_AND => {
            if children.len() != 2 {
                return Err(format!(
                    "COMPOUND_AND expected 2 children, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::And(children[0], children[1]), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::COMPOUND_OR => {
            if children.len() != 2 {
                return Err(format!(
                    "COMPOUND_OR expected 2 children, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::Or(children[0], children[1]), data_type.clone())
        }
        _ => return Err(format!("unsupported COMPOUND_PRED opcode: {:?}", opcode)),
    };
    Ok(id)
}
