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

use crate::common::ids::SlotId;
use crate::thrift::plan_nodes;

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::common::ids::SlotId;
    use crate::thrift::{exprs, plan_nodes, types};

    fn int_expr(value: i64) -> exprs::TExpr {
        exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: crate::types::arrow_thrift::thrift_type_desc_from_primitive(
                types::TPrimitiveType::BIGINT,
            ),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: Some(exprs::TIntLiteral { value }),
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }])
    }

    #[test]
    fn extracts_change_op_from_hdfs_range_extended_columns() {
        let mut range = plan_nodes::THdfsScanRange::default();
        range.extended_columns = Some(BTreeMap::from([(9, int_expr(-1))]));

        let value = super::extract_change_op_from_hdfs_range_extended_columns(
            7,
            &range,
            Some(SlotId::new(9)),
        )
        .unwrap();

        assert_eq!(value, Some(-1));
    }

    #[test]
    fn rejects_invalid_hdfs_range_change_op_extended_column() {
        let mut range = plan_nodes::THdfsScanRange::default();
        range.extended_columns = Some(BTreeMap::from([(9, int_expr(0))]));

        let error = super::extract_change_op_from_hdfs_range_extended_columns(
            7,
            &range,
            Some(SlotId::new(9)),
        )
        .unwrap_err();

        assert!(error.contains("invalid value"));
        assert!(error.contains("__change_op"));
    }
}
