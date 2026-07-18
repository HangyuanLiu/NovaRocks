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

#[cfg(feature = "compat")]
use crate::common::ids::SlotId;
#[cfg(feature = "compat")]
use crate::thrift::plan_nodes;

#[cfg(feature = "compat")]
pub(crate) fn extract_change_op_from_hdfs_range_extended_columns(
    node_id: i32,
    hdfs_range: &plan_nodes::THdfsScanRange,
    change_op_slot: Option<SlotId>,
) -> Result<Option<i8>, String> {
    let Some(slot) = change_op_slot else {
        return Ok(None);
    };
    let slot_id = i32::try_from(slot.as_u32()).map_err(|_| {
        format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot} exceeds i32")
    })?;
    let context = || format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot_id}");
    let Some(expr) = hdfs_range
        .extended_columns
        .as_ref()
        .and_then(|extended_columns| extended_columns.get(&slot_id))
    else {
        return Ok(None);
    };
    if expr.nodes.len() != 1 {
        return Err(format!(
            "{} expects exactly one INT_LITERAL extended column node, got {}",
            context(),
            expr.nodes.len()
        ));
    }
    let node = &expr.nodes[0];
    if node.node_type != crate::thrift::exprs::TExprNodeType::INT_LITERAL {
        return Err(format!(
            "{} expects INT_LITERAL extended column, got {:?}",
            context(),
            node.node_type
        ));
    }
    if node.num_children != 0 {
        return Err(format!(
            "{} INT_LITERAL extended column expects 0 children, got {}",
            context(),
            node.num_children
        ));
    }
    let value = node
        .int_literal
        .as_ref()
        .ok_or_else(|| format!("{} INT_LITERAL missing int payload", context()))?
        .value;
    let value = i8::try_from(value)
        .map_err(|_| format!("{} value {} does not fit in int8", context(), value))?;
    crate::exec::change_op::validate_change_op_value(value)
        .map_err(|e| format!("{} invalid value: {e}", context()))?;
    Ok(Some(value))
}
