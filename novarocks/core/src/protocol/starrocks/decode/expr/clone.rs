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
/// Lower CLONE_EXPR from Thrift to novarocks ExprNode.
///
/// CloneExpr is used to create independent column copies when multiple operators
/// reference the same column data, preventing unintended data corruption when
/// expressions modify columns in-place.
///
/// Implementation aligns with StarRocks BE's CloneExpr (be/src/exprs/clone_expr.h):
/// - Evaluates child expression
/// - Returns a COW-protected reference to the column
/// - Arrow's Arc-based arrays provide natural COW semantics
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::datatypes::DataType;

/// Lower CLONE_EXPR node to Clone ExprNode.
///
/// # Arguments
/// * `children` - Child expression IDs (should contain exactly 1 child)
/// * `arena` - Expression arena to store the node
/// * `data_type` - Expected data type from Thrift descriptor
///
/// # Returns
/// ExprId of the lowered Clone node
///
/// # Errors
/// Returns error if:
/// - Number of children != 1
pub(crate) fn lower_clone(
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if children.len() != 1 {
        return Err(format!(
            "CLONE_EXPR expects exactly 1 child, got {}",
            children.len()
        ));
    }

    let child = children[0];
    let clone_node = ExprNode::Clone(child);
    Ok(arena.push_typed(clone_node, data_type))
}
