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

//! Immutable output contracts decoded from a Native scan wire payload.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema, SlotLayout};
use novarocks_plan_codec::native_type::decode_field_type;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, plan};
use novarocks_types::SlotId;

use crate::fragment_error::NativeFragmentDecodeError;

#[derive(Clone, Debug)]
pub struct ProvenancedOutputColumn {
    column: common::OutputColumn,
    source_path: FieldPath,
    name_path: FieldPath,
    type_path: Option<FieldPath>,
    slot_schema: ChunkSlotSchema,
}

impl ProvenancedOutputColumn {
    fn decode(
        column: common::OutputColumn,
        source_path: FieldPath,
        name_path: FieldPath,
        type_path: FieldPath,
    ) -> Result<Self, NativeFragmentDecodeError> {
        let type_desc = column.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                type_path.clone(),
                format!("ScanNode column {} type missing", column.name),
            )
        })?;
        let field = decode_field_type(&column.name, column.nullable, type_desc)
            .map_err(|error| NativeFragmentDecodeError::invalid_value(type_path.clone(), error))?;
        let slot_schema = ChunkSlotSchema::from_field(SlotId::new(column.column_id), &field, None)
            .map_err(|error| NativeFragmentDecodeError::invalid_value(type_path.clone(), error))?;
        Ok(Self {
            column,
            source_path,
            name_path,
            type_path: Some(type_path),
            slot_schema,
        })
    }

    pub fn column(&self) -> &common::OutputColumn {
        &self.column
    }

    pub fn source_path(&self) -> FieldPath {
        self.source_path.clone()
    }

    pub fn type_path(&self) -> Option<FieldPath> {
        self.type_path.clone()
    }

    pub fn name_path(&self) -> FieldPath {
        self.name_path.clone()
    }

    pub fn slot_schema(&self) -> &ChunkSlotSchema {
        &self.slot_schema
    }
}

#[derive(Clone, Debug)]
pub struct DecodedScanOutputColumns {
    columns: Vec<common::OutputColumn>,
    provenanced: Vec<ProvenancedOutputColumn>,
    layout: SlotLayout,
    output_schema: ChunkSchemaRef,
}

impl DecodedScanOutputColumns {
    pub fn columns(&self) -> &[common::OutputColumn] {
        &self.columns
    }

    pub fn source_path(&self, selected_index: usize) -> FieldPath {
        self.provenanced[selected_index].source_path()
    }

    pub fn provenanced(&self) -> &[ProvenancedOutputColumn] {
        &self.provenanced
    }

    pub fn layout(&self) -> SlotLayout {
        self.layout.clone()
    }

    pub fn output_schema(&self) -> ChunkSchemaRef {
        Arc::clone(&self.output_schema)
    }
}

pub fn decode_scan_output_columns(
    scan: &plan::ScanNode,
    scan_path: FieldPath,
) -> Result<DecodedScanOutputColumns, NativeFragmentDecodeError> {
    if scan.columns.is_empty() {
        return Err(NativeFragmentDecodeError::missing(
            scan_path.field("columns"),
            "ScanNode columns are empty",
        ));
    }
    let required = (!scan.required_columns.is_empty()).then(|| {
        scan.required_columns
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    });
    let selected = scan
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            required
                .as_ref()
                .is_none_or(|required| required.contains(&column.name.to_ascii_lowercase()))
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            scan_path.field("required_columns"),
            format!(
                "ScanNode required_columns {:?} do not match any scan columns",
                scan.required_columns
            ),
        ));
    }
    let columns_path = scan_path.field("columns");
    let mut columns = Vec::with_capacity(selected.len());
    let mut provenanced = Vec::with_capacity(selected.len());
    let mut seen = HashMap::with_capacity(selected.len());
    for (wire_index, column) in selected {
        let source_path = columns_path.clone().index(wire_index);
        let slot_id = SlotId::new(column.column_id);
        if let Some(first_wire_index) = seen.insert(slot_id, wire_index) {
            return Err(NativeFragmentDecodeError::inconsistent(
                source_path.field("column_id"),
                format!(
                    "duplicate ScanNode column_id {} at wire index {} (first seen at wire index {})",
                    column.column_id, wire_index, first_wire_index
                ),
            ));
        }
        let decoded = ProvenancedOutputColumn::decode(
            column.clone(),
            source_path.clone(),
            source_path.clone().field("name"),
            source_path.field("type"),
        )?;
        columns.push(column.clone());
        provenanced.push(decoded);
    }
    let slot_schemas = provenanced
        .iter()
        .map(|column| column.slot_schema().clone())
        .collect::<Vec<_>>();
    let layout = SlotLayout::for_slots(slot_schemas.iter().map(ChunkSlotSchema::slot_id));
    let output_schema = ChunkSchema::try_new(slot_schemas)
        .map(Arc::new)
        .map_err(|error| NativeFragmentDecodeError::inconsistent(columns_path.clone(), error))?;
    Ok(DecodedScanOutputColumns {
        columns,
        provenanced,
        layout,
        output_schema,
    })
}
