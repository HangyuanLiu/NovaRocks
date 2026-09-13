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

//! Pure Native fragment physical-node projections.
//!
//! This module owns wire DTO to immutable Execution program projection. It
//! receives already-lowered children and has no Backend runtime, task-context,
//! Connector, or runtime-filter authority.

use novarocks_execution::exec::chunk::{ChunkSchemaRef, SlotLayout};
use novarocks_execution::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::plan;

use crate::fragment_error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};

/// One fully lowered physical node and its immutable output contract.
#[derive(Clone, Debug)]
pub struct NativeLoweredPlanNode {
    pub node: ExecNode,
    pub layout: SlotLayout,
    pub output_schema: ChunkSchemaRef,
}

/// Lowers a Native `LimitNode` after Backend recursion has supplied its child.
pub fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    path: FieldPath,
    node_path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("validated LimitNode child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "limit", error)
                .into_native(path.clone())
        })?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")
        .map_err(|error| {
            NativeFragmentDecodeError::out_of_range(node_path.field("limit"), error)
        })?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "limit",
            error,
        )
        .into_native(path.clone())
    })?;
    let offset = parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "offset", error)
                .into_native(path)
        })?
        .unwrap_or(0);
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

/// Lowers a Native `AssertOneRowNode` after Backend recursion has supplied its child.
pub fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    path: FieldPath,
    mut children: Vec<NativeLoweredPlanNode>,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let decoded = (|| -> Result<NativeLoweredPlanNode, NativeFragmentLeafDecodeError> {
        let child = children.pop().expect("validated AssertOneRowNode child");
        let desired_num_rows = parse_optional_nonnegative_i64(
            assert.desired_num_rows,
            "AssertOneRowNode.desired_num_rows",
        )
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::OutOfRange,
                "desired_num_rows",
                error,
            )
        })?
        .or(Some(1));
        let assertion = lower_row_count_assertion(assert.assertion)?;
        let mode = if assert.group_key_column_ids.is_empty() {
            if !assert.group_key_labels.is_empty() || assert.keyed_message_prefix.is_some() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::MissingField,
                    "group_key_column_ids",
                    "AssertOneRowNode group_key_column_ids is required when keyed metadata is present",
                ));
            }
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string: Some(assert.subquery_text.clone()),
            }
        } else {
            if desired_num_rows != Some(1) || !matches!(assertion, Assertion::Le) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "assertion",
                    "AssertOneRowNode keyed assertions only support desired_num_rows <= 1",
                ));
            }
            if !assert.group_key_labels.is_empty()
                && assert.group_key_labels.len() != assert.group_key_column_ids.len()
            {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "group_key_labels",
                    format!(
                        "AssertOneRowNode group_key_labels length mismatch: key_columns={} labels={}",
                        assert.group_key_column_ids.len(),
                        assert.group_key_labels.len()
                    ),
                ));
            }
            let key_slots = assert
                .group_key_column_ids
                .iter()
                .enumerate()
                .map(|(index, column_id)| {
                    child
                        .layout
                        .resolve_column_id(*column_id)
                        .ok_or_else(|| {
                            format!("ColumnRef column_id={column_id} not found in input layout")
                        })
                        .map_err(|error| {
                            NativeFragmentLeafDecodeError::at_field(
                                ProtocolErrorKind::InvalidValue,
                                "group_key_column_ids",
                                format!("AssertOneRowNode group key: {error}"),
                            )
                            .append_index(index)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key_labels = if assert.group_key_labels.is_empty() {
                assert
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| format!("column_{column_id}"))
                    .collect()
            } else {
                assert.group_key_labels.clone()
            };
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix: assert
                    .keyed_message_prefix
                    .clone()
                    .unwrap_or_else(|| "assert_num_rows failed".to_string()),
            }
        };
        Ok(NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    mode,
                }),
            },
            layout: child.layout,
            output_schema: child.output_schema,
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn lower_row_count_assertion(value: i32) -> Result<Assertion, NativeFragmentLeafDecodeError> {
    match value {
        value if value == plan::RowCountAssertion::Unspecified as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Eq as i32 => Ok(Assertion::Eq),
        value if value == plan::RowCountAssertion::Ne as i32 => Ok(Assertion::Ne),
        value if value == plan::RowCountAssertion::Lt as i32 => Ok(Assertion::Lt),
        value if value == plan::RowCountAssertion::Le as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Gt as i32 => Ok(Assertion::Gt),
        value if value == plan::RowCountAssertion::Ge as i32 => Ok(Assertion::Ge),
        other => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "assertion",
            format!("AssertOneRowNode assertion {other} is not supported"),
        )),
    }
}

/// Parses an optional native non-negative integer into a platform index.
pub fn parse_optional_nonnegative_i64(
    value: Option<i64>,
    label: &str,
) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            if value < 0 {
                Err(format!("{label} must be >= 0, got {value}"))
            } else {
                Ok(value as usize)
            }
        })
        .transpose()
}

/// Parses the `DistributedNode.limit` sentinel form.
pub fn parse_distributed_limit(value: i64, label: &str) -> Result<Option<usize>, String> {
    if value == -1 {
        Ok(None)
    } else if value < 0 {
        Err(format!("{label} must be -1 or >= 0, got {value}"))
    } else {
        Ok(Some(value as usize))
    }
}

/// Combines physical and distributed limit declarations fail-closed.
pub fn merge_limits(
    node_kind: &str,
    payload_limit: Option<usize>,
    outer_limit: Option<usize>,
) -> Result<Option<usize>, String> {
    match (payload_limit, outer_limit) {
        (Some(left), Some(right)) if left != right => Err(format!(
            "{node_kind} payload limit {left} conflicts with DistributedNode.limit {right}"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        NativeLoweredPlanNode, lower_assert_one_row_node, lower_limit_node, merge_limits,
        parse_distributed_limit, parse_optional_nonnegative_i64,
    };
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema, SlotLayout};
    use novarocks_execution::exec::node::assert::AssertNumRowsMode;
    use novarocks_execution::exec::node::values::ValuesNode;
    use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
    use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
    use novarocks_proto_models::plan;
    use novarocks_types::SlotId;

    fn child() -> NativeLoweredPlanNode {
        let output_schema = Arc::new(ChunkSchema::empty());
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(Arc::clone(&output_schema), Vec::new())
                        .expect("empty values chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots(Vec::new()),
            output_schema,
        }
    }

    fn child_with_key() -> NativeLoweredPlanNode {
        let slot = SlotId::new(1);
        let output_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot,
                Field::new("id", DataType::Int64, false),
                None,
                None,
            )])
            .expect("keyed chunk schema"),
        );
        NativeLoweredPlanNode {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::try_new_with_columns(
                        Arc::clone(&output_schema),
                        vec![Arc::new(Int64Array::from(vec![10]))],
                    )
                    .expect("keyed values chunk"),
                    node_id: 1,
                }),
            },
            layout: SlotLayout::for_slots(vec![slot]),
            output_schema,
        }
    }

    #[test]
    fn limit_shapes_preserve_native_sentinel_and_conflict_rules() {
        assert_eq!(
            parse_optional_nonnegative_i64(Some(5), "limit"),
            Ok(Some(5))
        );
        assert_eq!(parse_optional_nonnegative_i64(None, "limit"), Ok(None));
        assert!(parse_optional_nonnegative_i64(Some(-1), "limit").is_err());
        assert_eq!(
            parse_distributed_limit(-1, "DistributedNode.limit"),
            Ok(None)
        );
        assert_eq!(
            parse_distributed_limit(0, "DistributedNode.limit"),
            Ok(Some(0))
        );
        assert!(parse_distributed_limit(-2, "DistributedNode.limit").is_err());
        assert_eq!(merge_limits("LimitNode", Some(2), Some(2)), Ok(Some(2)));
        assert!(merge_limits("LimitNode", Some(2), Some(3)).is_err());
    }

    #[test]
    fn lower_limit_preserves_native_shape_and_error_paths() {
        let root = FieldPath::root("plan_fragment").field("root");
        let node = plan::DistributedNode {
            node_id: 7,
            limit: -1,
            ..Default::default()
        };
        let lowered = lower_limit_node(
            &node,
            &plan::LimitNode {
                limit: Some(3),
                offset: Some(1),
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root.clone(),
            vec![child()],
        )
        .expect("valid limit");
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected LimitNode");
        };
        assert_eq!(limit.node_id, 7);
        assert_eq!(limit.limit, Some(3));
        assert_eq!(limit.offset, 1);

        let error = lower_limit_node(
            &node,
            &plan::LimitNode {
                limit: Some(-2),
                offset: None,
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root.clone(),
            vec![child()],
        )
        .expect_err("negative payload limit must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.limit.limit"
        );

        let outer_error = lower_limit_node(
            &plan::DistributedNode {
                node_id: 7,
                limit: -2,
                ..Default::default()
            },
            &plan::LimitNode::default(),
            root.clone()
                .field("payload")
                .field("physical")
                .field("limit"),
            root,
            vec![child()],
        )
        .expect_err("negative distributed limit must fail");
        assert_eq!(
            outer_error
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.limit"
        );
    }

    #[test]
    fn lower_assert_one_row_preserves_global_contract_and_error_path() {
        let root = FieldPath::root("plan_fragment").field("root");
        let lowered = lower_assert_one_row_node(
            &plan::DistributedNode {
                node_id: 9,
                ..Default::default()
            },
            &plan::AssertOneRowNode {
                subquery_text: "subquery".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                ..Default::default()
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("assert_one_row"),
            vec![child()],
        )
        .expect("valid global assertion");
        let ExecNodeKind::AssertNumRows(assertion) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        assert_eq!(assertion.node_id, 9);

        let error = lower_assert_one_row_node(
            &plan::DistributedNode {
                node_id: 9,
                ..Default::default()
            },
            &plan::AssertOneRowNode {
                assertion: 999,
                ..Default::default()
            },
            root.clone()
                .field("payload")
                .field("physical")
                .field("assert_one_row"),
            vec![child()],
        )
        .expect_err("unknown assertion must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.assertion"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn lower_assert_one_row_preserves_keyed_contract_and_paths() {
        let root = FieldPath::root("plan_fragment").field("root");
        let path = root
            .clone()
            .field("payload")
            .field("physical")
            .field("assert_one_row");
        let node = plan::DistributedNode {
            node_id: 9,
            ..Default::default()
        };
        let lowered = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![1],
                group_key_labels: vec!["_row_id".to_string()],
                keyed_message_prefix: Some("MOR UPDATE matched target row".to_string()),
                ..Default::default()
            },
            path.clone(),
            vec![child_with_key()],
        )
        .expect("valid keyed assertion");
        let ExecNodeKind::AssertNumRows(assertion) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        let AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix,
        } = assertion.mode
        else {
            panic!("expected keyed assertion");
        };
        assert_eq!(key_slots, vec![SlotId::new(1)]);
        assert_eq!(key_labels, vec!["_row_id"]);
        assert_eq!(message_prefix, "MOR UPDATE matched target row");

        let negative = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(-1),
                ..Default::default()
            },
            path.clone(),
            vec![child_with_key()],
        )
        .expect_err("negative desired rows must fail");
        assert_eq!(
            negative
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.desired_num_rows"
        );

        let missing_key = lower_assert_one_row_node(
            &node,
            &plan::AssertOneRowNode {
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![999],
                ..Default::default()
            },
            path,
            vec![child_with_key()],
        )
        .expect_err("unknown group key must fail");
        assert_eq!(
            missing_key
                .protocol()
                .expect("protocol error")
                .path()
                .to_string(),
            "plan_fragment.root.payload.physical.assert_one_row.group_key_column_ids[0]"
        );
    }
}
