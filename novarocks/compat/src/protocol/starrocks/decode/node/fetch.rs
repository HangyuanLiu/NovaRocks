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
use crate::protocol::starrocks::decode::descriptor::decode_lookup_nodes_info;
use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::node::lookup::lower_row_pos_descs;
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;
use novarocks::common::ids::SlotId;
use novarocks::exec::node::fetch::FetchNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use std::collections::HashSet;

pub(crate) fn lower_fetch_node(
    mut children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "FETCH_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let fetch = node
        .fetch_node
        .as_ref()
        .ok_or_else(|| "FETCH_NODE missing fetch_node payload".to_string())?;
    let row_pos_descs = fetch
        .row_pos_descs
        .as_ref()
        .ok_or_else(|| "FETCH_NODE missing row_pos_descs".to_string())?;
    let row_pos_descs = lower_row_pos_descs(row_pos_descs)?;
    if row_pos_descs.is_empty() {
        return Err("FETCH_NODE row_pos_descs is empty".to_string());
    }
    let target_node_id = fetch
        .target_node_id
        .ok_or_else(|| "FETCH_NODE missing target_node_id".to_string())?;

    let input_child = children
        .pop()
        .ok_or_else(|| "FETCH_NODE missing input child".to_string())?;

    let mut row_pos_slots: HashSet<i32> = HashSet::new();
    for desc in row_pos_descs.values() {
        row_pos_slots.insert(desc.row_source_slot.as_u32() as i32);
        for slot in &desc.fetch_ref_slots {
            row_pos_slots.insert(slot.as_u32() as i32);
        }
        for slot in &desc.lookup_ref_slots {
            row_pos_slots.insert(slot.as_u32() as i32);
        }
    }

    let mut order = Vec::new();
    for (tuple_id, slot_id) in &out_layout.order {
        if row_pos_slots.contains(slot_id) {
            continue;
        }
        order.push((*tuple_id, *slot_id));
    }
    let index = order
        .iter()
        .enumerate()
        .map(|(idx, key)| (*key, idx))
        .collect();
    let fetch_layout = Layout { order, index };

    let desc_tbl = desc_tbl.ok_or_else(|| {
        "FETCH_NODE requires descriptor table for output chunk schema".to_string()
    })?;
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, &fetch_layout)?;
    let mut output_slots_by_tuple = std::collections::HashMap::<i32, Vec<SlotId>>::new();
    for (tuple_id, slot_id) in &fetch_layout.order {
        let slot_id = SlotId::try_from(*slot_id).map_err(|detail| {
            format!("FETCH_NODE output slot id {slot_id} is invalid: {detail}")
        })?;
        output_slots_by_tuple
            .entry(*tuple_id)
            .or_default()
            .push(slot_id);
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Fetch(FetchNode {
                input: Box::new(input_child.node),
                node_id: node.node_id,
                target_node_id,
                row_pos_descs,
                output_slots_by_tuple,
                nodes_info: fetch
                    .nodes_info
                    .as_ref()
                    .map(decode_lookup_nodes_info)
                    .transpose()?,
                output_chunk_schema,
            }),
        },
        layout: fetch_layout,
    })
}
