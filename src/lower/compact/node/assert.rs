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
use crate::lower::compact::layout::Layout;
use crate::lower::compact::node::Lowered;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::node::assert::AssertNumRowsMode;
    use crate::lower::compact::layout::Layout;
    use crate::thrift::plan_nodes::{TAssertNumRowsNode, TAssertion, TPlanNodeType};
    use std::collections::HashMap;

    fn values_child_with_layout(layout: Layout) -> Lowered {
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(crate::exec::node::values::ValuesNode {
                    chunk: crate::exec::chunk::Chunk::default(),
                    node_id: 0,
                }),
            },
            layout,
        }
    }

    #[test]
    fn lower_assert_num_rows_carries_config() {
        // Fake child Lowered with a simple empty layout
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let child = values_child_with_layout(layout);

        let mut t_node =
            crate::lower::compact::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            desired_num_rows: Some(1),
            subquery_string: Some("select c1 from test".to_string()),
            assertion: Some(TAssertion::EQ),
            ..Default::default()
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let lowered = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect("lower assert node");

        match lowered.node.kind {
            ExecNodeKind::AssertNumRows(n) => match n.mode {
                AssertNumRowsMode::Global {
                    desired_num_rows,
                    assertion,
                    subquery_string,
                } => {
                    assert_eq!(desired_num_rows, Some(1));
                    assert!(matches!(assertion, Assertion::Eq));
                    assert_eq!(subquery_string.as_deref(), Some("select c1 from test"));
                }
                AssertNumRowsMode::PerKeyAtMostOne { .. } => {
                    panic!("expected global assert mode")
                }
            },
            _ => panic!("expected AssertNumRows exec node"),
        }
    }

    #[test]
    fn lower_assert_num_rows_keyed_mode_carries_group_keys() {
        let layout = Layout {
            order: vec![(0, 7)],
            index: HashMap::from([((0, 7), 0)]),
        };
        let child = values_child_with_layout(layout);

        let mut t_node =
            crate::lower::compact::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            group_key_slots: Some(vec![7]),
            group_key_labels: Some(vec!["_row_id".to_string()]),
            keyed_message_prefix: Some("assert_num_rows failed".to_string()),
            ..Default::default()
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let lowered = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect("lower keyed assert node");

        match lowered.node.kind {
            ExecNodeKind::AssertNumRows(n) => match n.mode {
                AssertNumRowsMode::PerKeyAtMostOne {
                    key_slots,
                    key_labels,
                    message_prefix,
                } => {
                    assert_eq!(key_slots, vec![SlotId::new(7)]);
                    assert_eq!(key_labels, vec!["_row_id".to_string()]);
                    assert_eq!(message_prefix, "assert_num_rows failed");
                }
                AssertNumRowsMode::Global { .. } => panic!("expected keyed assert mode"),
            },
            _ => panic!("expected AssertNumRows exec node"),
        }
    }

    #[test]
    fn lower_assert_num_rows_keyed_mode_rejects_missing_group_slot() {
        let layout = Layout {
            order: vec![(0, 8)],
            index: HashMap::from([((0, 8), 0)]),
        };
        let child = values_child_with_layout(layout);

        let mut t_node =
            crate::lower::compact::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            group_key_slots: Some(vec![7]),
            group_key_labels: Some(vec!["_row_id".to_string()]),
            keyed_message_prefix: Some("assert_num_rows failed".to_string()),
            ..Default::default()
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let err = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect_err("missing keyed slot should fail");
        assert!(err.contains("slot 7"), "{err}");
    }

    #[test]
    fn lower_assert_num_rows_rejects_empty_group_key_slots() {
        let layout = Layout {
            order: vec![(0, 7)],
            index: HashMap::from([((0, 7), 0)]),
        };
        let child = values_child_with_layout(layout);

        let mut t_node =
            crate::lower::compact::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            group_key_slots: Some(vec![]),
            group_key_labels: Some(vec![]),
            keyed_message_prefix: Some("assert_num_rows failed".to_string()),
            ..Default::default()
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let err = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect_err("empty keyed slots should fail");
        assert!(err.contains("group_key_slots"), "{err}");
    }

    #[test]
    fn lower_assert_num_rows_rejects_keyed_metadata_without_group_key_slots() {
        let layout = Layout {
            order: vec![(0, 7)],
            index: HashMap::from([((0, 7), 0)]),
        };
        let child = values_child_with_layout(layout);

        let mut t_node =
            crate::lower::compact::node::test_plan_node(0, TPlanNodeType::ASSERT_NUM_ROWS_NODE, 1);
        t_node.assert_num_rows_node = Some(TAssertNumRowsNode {
            group_key_labels: Some(vec!["_row_id".to_string()]),
            keyed_message_prefix: Some("assert_num_rows failed".to_string()),
            ..Default::default()
        });

        let mut out_layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let err = lower_assert_num_rows_node(vec![child], &t_node, &mut out_layout)
            .expect_err("orphan keyed metadata should fail");
        assert!(err.contains("group_key_slots"), "{err}");
    }
}
