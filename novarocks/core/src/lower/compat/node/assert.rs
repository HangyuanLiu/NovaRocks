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
use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::compat::layout::Layout;
use crate::lower::compat::node::Lowered;
use crate::thrift::plan_nodes;

pub(crate) fn lower_assert_num_rows_node(
    mut children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    _out_layout: &mut Layout,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "ASSERT_NUM_ROWS_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let child = children.pop().expect("child");

    let t_assert = node
        .assert_num_rows_node
        .as_ref()
        .ok_or_else(|| "ASSERT_NUM_ROWS_NODE missing payload".to_string())?;

    let assertion = match t_assert.assertion {
        Some(plan_nodes::TAssertion::EQ) | None => Assertion::Eq,
        Some(plan_nodes::TAssertion::NE) => Assertion::Ne,
        Some(plan_nodes::TAssertion::LT) => Assertion::Lt,
        Some(plan_nodes::TAssertion::LE) => Assertion::Le,
        Some(plan_nodes::TAssertion::GT) => Assertion::Gt,
        Some(plan_nodes::TAssertion::GE) => Assertion::Ge,
        Some(_) => Assertion::Eq,
    };

    let mode = match t_assert.group_key_slots.as_ref() {
        Some(raw_key_slots) if raw_key_slots.is_empty() => {
            return Err(
                "ASSERT_NUM_ROWS_NODE group_key_slots must be non-empty when present".to_string(),
            );
        }
        Some(raw_key_slots) => {
            let key_slots = raw_key_slots
                .iter()
                .map(|slot| SlotId::try_from(*slot))
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(labels) = t_assert.group_key_labels.as_ref() {
                if labels.len() != key_slots.len() {
                    return Err(format!(
                        "ASSERT_NUM_ROWS_NODE group_key_labels length mismatch: key_slots={} labels={}",
                        key_slots.len(),
                        labels.len()
                    ));
                }
            }

            for slot in &key_slots {
                let slot_i32 = i32::try_from(slot.as_u32())
                    .map_err(|_| format!("ASSERT_NUM_ROWS_NODE key slot {} exceeds i32", slot))?;
                if !child.layout.order.iter().any(|(_, s)| *s == slot_i32) {
                    return Err(format!(
                        "ASSERT_NUM_ROWS_NODE key slot {} is not present in child layout",
                        slot
                    ));
                }
            }

            let key_labels = t_assert.group_key_labels.clone().unwrap_or_else(|| {
                key_slots
                    .iter()
                    .map(|slot| format!("slot_{}", slot))
                    .collect()
            });
            let message_prefix = t_assert
                .keyed_message_prefix
                .clone()
                .unwrap_or_else(|| "assert_num_rows failed".to_string());

            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            }
        }
        None => {
            if t_assert.group_key_labels.is_some() || t_assert.keyed_message_prefix.is_some() {
                return Err(
                    "ASSERT_NUM_ROWS_NODE group_key_slots is required when keyed metadata is present"
                        .to_string(),
                );
            }

            AssertNumRowsMode::Global {
                desired_num_rows: t_assert.desired_num_rows.map(|v| v as usize),
                assertion,
                subquery_string: t_assert.subquery_string.clone(),
            }
        }
    };

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                mode,
            }),
        },
        // AssertNumRows is a pass-through node, keep child's layout.
        layout: child.layout,
    })
}
