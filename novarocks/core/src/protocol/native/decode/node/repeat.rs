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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::super::layout::Layout;
use super::DecodedNode;
use super::common::check_exact_arity;
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::protocol::common::error::FieldPath;

pub(super) fn lower_repeat_node(
    node: &plan::DistributedNode,
    repeat: &plan::RepeatNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, super::super::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<DecodedNode, String> {
        check_exact_arity("RepeatNode", 1, children.len())?;
        let child = children.pop().expect("child");
        let repeat_times = repeat.grouping_ids.len();
        if repeat_times == 0 {
            return Err("RepeatNode grouping_ids is empty".to_string());
        }
        if repeat.repeat_column_ref_ids.len() != repeat_times {
            return Err(format!(
                "RepeatNode repeat_column_ref_ids size mismatch: expected {}, got {}",
                repeat_times,
                repeat.repeat_column_ref_ids.len()
            ));
        }
        let all_slot_ids = repeat
            .all_rollup_column_ids
            .iter()
            .copied()
            .map(SlotId::new)
            .collect::<Vec<_>>();
        let all_slot_set = all_slot_ids.iter().copied().collect::<HashSet<_>>();
        let null_slot_ids = repeat
            .repeat_column_ref_ids
            .iter()
            .enumerate()
            .map(|(idx, keep_ids)| {
                let keep = keep_ids
                    .values
                    .iter()
                    .copied()
                    .map(SlotId::new)
                    .collect::<HashSet<_>>();
                for slot in &keep {
                    if !all_slot_set.contains(slot) {
                        return Err(format!(
                            "RepeatNode keep set {idx} contains unknown rollup slot {}",
                            slot
                        ));
                    }
                }
                let mut nulls = all_slot_ids
                    .iter()
                    .copied()
                    .filter(|slot| !keep.contains(slot))
                    .collect::<Vec<_>>();
                nulls.sort_by_key(|slot| slot.as_u32());
                Ok(nulls)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let grouping_slot_ids = repeat
            .grouping_fn_ids
            .iter()
            .map(|entry| SlotId::new(entry.value))
            .collect::<Vec<_>>();
        let grouping_list =
            repeat_grouping_values(repeat, path.clone()).map_err(|error| error.to_string())?;
        let (layout, output_schema) = repeat_output_layout_and_schema(
            &child,
            &repeat.grouping_fn_ids,
            &grouping_slot_ids,
            path.clone(),
        )
        .map_err(|error| error.to_string())?;

        Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Repeat(RepeatNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    null_slot_ids,
                    grouping_slot_ids,
                    grouping_list,
                    repeat_times,
                }),
            },
            layout,
            output_schema,
        })
    })();
    super::super::NativeFragmentDecodeError::map_invalid(path, decoded)
}

fn repeat_output_layout_and_schema(
    child: &DecodedNode,
    grouping_fn_ids: &[plan::NamedUInt32],
    grouping_slot_ids: &[SlotId],
    path: FieldPath,
) -> Result<(Layout, ChunkSchemaRef), super::super::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<(Layout, ChunkSchemaRef), String> {
        let mut slots = child.output_schema.slots().to_vec();
        let mut output_slot_ids = child.layout.order().to_vec();
        for (idx, slot_id) in grouping_slot_ids.iter().copied().enumerate() {
            if child.layout.contains_slot(slot_id) || output_slot_ids.contains(&slot_id) {
                return Err(format!(
                    "RepeatNode grouping slot {} duplicates input slot",
                    slot_id
                ));
            }
            let name = grouping_fn_ids
                .get(idx)
                .map(|entry| entry.name.as_str())
                .filter(|name| !name.is_empty())
                .unwrap_or("__grouping_fn");
            let field = Field::new(name, DataType::Int64, true);
            slots.push(ChunkSlotSchema::new_with_field(slot_id, field, None, None));
            output_slot_ids.push(slot_id);
        }
        let layout = Layout::for_slots(output_slot_ids);
        let output_schema = Arc::new(ChunkSchema::try_new(slots)?);
        Ok((layout, output_schema))
    })();
    super::super::NativeFragmentDecodeError::map_invalid(path, decoded)
}

fn repeat_grouping_values(
    repeat: &plan::RepeatNode,
    path: FieldPath,
) -> Result<Vec<Vec<i64>>, super::super::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<Vec<Vec<i64>>, String> {
        if repeat.grouping_fn_ids.len() != repeat.grouping_fn_arg_ids.len() {
            return Err(format!(
                "RepeatNode grouping fn length mismatch: ids={} arg_ids={}",
                repeat.grouping_fn_ids.len(),
                repeat.grouping_fn_arg_ids.len()
            ));
        }
        let repeat_times = repeat.grouping_ids.len();
        let keep_sets = repeat
            .repeat_column_ref_ids
            .iter()
            .map(|ids| ids.values.iter().copied().collect::<HashSet<_>>())
            .collect::<Vec<_>>();
        repeat
            .grouping_fn_arg_ids
            .iter()
            .enumerate()
            .map(|(idx, args)| {
                if args.values.len() > 63 {
                    return Err(format!(
                        "RepeatNode grouping_fn_arg_ids[{idx}] has too many arguments: {}",
                        args.values.len()
                    ));
                }
                let mut values = Vec::with_capacity(repeat_times);
                for (repeat_idx, keep) in keep_sets.iter().enumerate() {
                    let mut value = 0i64;
                    for (arg_idx, column_id) in args.values.iter().enumerate() {
                        if !keep.contains(column_id) {
                            let reverse_bit_pos = args.values.len() - 1 - arg_idx;
                            value |= 1i64 << reverse_bit_pos;
                        }
                    }
                    if repeat_idx >= repeat_times {
                        return Err("RepeatNode internal repeat index overflow".to_string());
                    }
                    values.push(value);
                }
                Ok(values)
            })
            .collect()
    })();
    super::super::NativeFragmentDecodeError::map_invalid(path, decoded)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{lower, physical_node, two_col_values_node};
    use crate::exec::node::ExecNodeKind;
    use crate::proto::plan;

    #[test]
    fn repeat_grouping_function_uses_sql_reverse_bit_order() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1, 2] },
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: vec![2] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1, 2, 3],
                all_rollup_columns: vec!["a".to_string(), "b".to_string()],
                all_rollup_column_ids: vec![1, 2],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1, 2] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.grouping_list, vec![vec![0, 1, 2, 3]]);
    }
}
