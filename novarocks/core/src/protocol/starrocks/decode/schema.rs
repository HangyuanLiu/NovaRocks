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
use crate::exec::chunk::{ChunkFieldSchema, ChunkSlotSchema};
use crate::thrift::types;

pub(crate) fn chunk_field_schema_from_type_desc(
    name: impl Into<String>,
    nullable: bool,
    desc: types::TTypeDesc,
) -> Result<ChunkFieldSchema, String> {
    let name = name.into();
    validate_type_desc_nodes(&desc, "field")?;
    let field = crate::protocol::starrocks::type_mapping::thrift_desc_to_arrow_field(
        &name, nullable, &desc,
    )
    .ok_or_else(|| "field type desc has unsupported arrow mapping".to_string())?;
    ChunkFieldSchema::from_field(&field)
}

pub(crate) fn chunk_slot_schema_from_type_desc(
    slot_id: SlotId,
    name: impl Into<String>,
    nullable: bool,
    desc: types::TTypeDesc,
    unique_id: Option<i32>,
) -> Result<ChunkSlotSchema, String> {
    let name = name.into();
    validate_type_desc_nodes(&desc, "slot")?;
    let field = crate::protocol::starrocks::type_mapping::thrift_desc_to_arrow_field(
        &name, nullable, &desc,
    )
    .ok_or_else(|| {
        format!(
            "chunk slot {} has unsupported type desc for arrow conversion",
            slot_id
        )
    })?;
    ChunkSlotSchema::try_new_with_field(slot_id, field, None, unique_id)
}

pub(crate) fn chunk_slot_schema_from_optional_type_desc(
    slot_id: SlotId,
    name: impl Into<String>,
    nullable: bool,
    desc: Option<types::TTypeDesc>,
    unique_id: Option<i32>,
) -> Result<ChunkSlotSchema, String> {
    let Some(desc) = desc else {
        return Err(format!(
            "chunk slot {} missing type_desc; use try_new_with_field for runtime fields",
            slot_id
        ));
    };
    chunk_slot_schema_from_type_desc(slot_id, name, nullable, desc, unique_id)
}

fn validate_type_desc_nodes(desc: &types::TTypeDesc, label: &str) -> Result<(), String> {
    let nodes = desc
        .types
        .as_ref()
        .ok_or_else(|| format!("{label} type desc missing nodes"))?;
    let next = type_desc_node_span(nodes, 0)?;
    if next != nodes.len() {
        return Err(format!(
            "{label} type desc has trailing nodes: consumed={} total={}",
            next,
            nodes.len()
        ));
    }
    Ok(())
}

fn type_desc_node_span(nodes: &[types::TTypeNode], start: usize) -> Result<usize, String> {
    let node = nodes
        .get(start)
        .ok_or_else(|| format!("field type desc ended unexpectedly at node {}", start))?;

    match node.type_ {
        t if t == types::TTypeNodeType::SCALAR => Ok(start + 1),
        t if t == types::TTypeNodeType::STRUCT => {
            let struct_fields = node
                .struct_fields
                .as_ref()
                .ok_or_else(|| "struct type desc missing struct_fields".to_string())?;
            let mut cursor = start + 1;
            for _ in struct_fields {
                cursor = type_desc_node_span(nodes, cursor)?;
            }
            Ok(cursor)
        }
        t if t == types::TTypeNodeType::ARRAY => type_desc_node_span(nodes, start + 1),
        t if t == types::TTypeNodeType::MAP => {
            let next = type_desc_node_span(nodes, start + 1)?;
            type_desc_node_span(nodes, next)
        }
        other => Err(format!("unsupported type desc node {:?}", other)),
    }
}
