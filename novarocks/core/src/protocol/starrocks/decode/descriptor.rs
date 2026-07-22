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

use std::collections::HashMap;

use arrow::datatypes::Field;

use crate::common::ids::SlotId;
use crate::runtime::descriptor_snapshot::{
    DescriptorIcebergSchema, DescriptorIcebergSchemaField, DescriptorLogicalType, DescriptorSlot,
    DescriptorSnapshot, DescriptorTable, DescriptorTableKind, LookupNodeInfo, LookupNodesInfo,
};
use crate::thrift::{descriptors, types};

pub(crate) fn decode_lookup_nodes_info(
    nodes_info: &descriptors::TNodesInfo,
) -> Result<LookupNodesInfo, String> {
    let nodes = nodes_info
        .nodes
        .iter()
        .map(|node| {
            let async_internal_port = u16::try_from(node.async_internal_port).map_err(|_| {
                format!(
                    "lookup async_internal_port {} for backend_id {} is out of u16 range",
                    node.async_internal_port, node.id
                )
            })?;
            Ok(LookupNodeInfo {
                id: node.id,
                option: node.option,
                host: node.host.clone(),
                async_internal_port,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(LookupNodesInfo {
        version: nodes_info.version,
        nodes,
    })
}

pub(crate) fn descriptor_snapshot_from_thrift(
    desc: &descriptors::TDescriptorTable,
) -> Result<DescriptorSnapshot, String> {
    let mut tuple_to_table = HashMap::new();
    for tuple in &desc.tuple_descriptors {
        if let (Some(tuple_id), Some(table_id)) = (tuple.id, tuple.table_id) {
            tuple_to_table.insert(tuple_id, table_id);
        }
    }

    let tables = desc
        .table_descriptors
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(descriptor_table_from_thrift)
        .collect::<Result<Vec<_>, _>>()?;

    let Some(slot_descs) = desc.slot_descriptors.as_ref() else {
        return DescriptorSnapshot::new_with_tables(Vec::new(), tuple_to_table, tables);
    };
    let mut slots = Vec::with_capacity(slot_descs.len());
    for slot in slot_descs {
        let (Some(tuple_id), Some(raw_slot_id), Some(type_desc)) =
            (slot.parent, slot.id, slot.slot_type.as_ref())
        else {
            continue;
        };
        let slot_id = SlotId::try_from(raw_slot_id)?;
        let data_type =
            crate::protocol::starrocks::type_mapping::thrift_desc_to_arrow_type(type_desc)
                .ok_or_else(|| {
                    format!(
                        "unsupported descriptor slot type for tuple_id={} slot_id={}",
                        tuple_id, raw_slot_id
                    )
                })?;
        let name = descriptor_slot_display_name(slot);
        let nullable = slot.is_nullable.unwrap_or(true);
        let logical = logical_type_from_desc(type_desc).unwrap_or(DescriptorLogicalType::Unknown);
        slots.push(DescriptorSlot {
            tuple_id,
            slot_id,
            name: name.clone(),
            field: Field::new(name, data_type, nullable),
            logical,
            unique_id: slot.col_unique_id.filter(|v| *v > 0),
        });
    }

    DescriptorSnapshot::new_with_tables(slots, tuple_to_table, tables)
}

fn descriptor_slot_display_name(desc: &descriptors::TSlotDescriptor) -> String {
    if let Some(name) = desc.col_name.as_ref().filter(|v| !v.is_empty()) {
        return name.clone();
    }
    if let Some(name) = desc.col_physical_name.as_ref().filter(|v| !v.is_empty()) {
        return name.clone();
    }
    match (desc.parent, desc.id) {
        (Some(parent), Some(id)) => format!("col_{parent}_{id}"),
        (_, Some(id)) => format!("col_{id}"),
        _ => "col_unknown".to_string(),
    }
}

fn descriptor_table_from_thrift(
    desc: &descriptors::TTableDescriptor,
) -> Result<DescriptorTable, String> {
    let kind = if desc.iceberg_table.is_some() {
        DescriptorTableKind::Iceberg
    } else if desc.paimon_table.is_some() {
        DescriptorTableKind::Paimon
    } else {
        DescriptorTableKind::Other
    };
    let iceberg_schema = desc
        .iceberg_table
        .as_ref()
        .and_then(|table| table.iceberg_schema.as_ref())
        .map(descriptor_iceberg_schema_from_thrift);
    let location = desc
        .iceberg_table
        .as_ref()
        .and_then(|table| table.location.as_ref())
        .map(|location| location.trim())
        .filter(|location| !location.is_empty())
        .map(str::to_string);
    Ok(DescriptorTable {
        id: desc.id,
        kind,
        location,
        iceberg_schema,
    })
}

fn descriptor_iceberg_schema_from_thrift(
    schema: &descriptors::TIcebergSchema,
) -> DescriptorIcebergSchema {
    DescriptorIcebergSchema {
        fields: schema.fields.as_ref().map(|fields| {
            fields
                .iter()
                .map(descriptor_iceberg_schema_field_from_thrift)
                .collect()
        }),
    }
}

fn descriptor_iceberg_schema_field_from_thrift(
    field: &descriptors::TIcebergSchemaField,
) -> DescriptorIcebergSchemaField {
    DescriptorIcebergSchemaField {
        field_id: field.field_id,
        name: field.name.clone(),
        initial_default_json: field.initial_default_json.clone(),
        children: field.children.as_ref().map(|children| {
            children
                .iter()
                .map(|child| descriptor_iceberg_schema_field_from_thrift(child.as_ref()))
                .collect()
        }),
    }
}

fn logical_type_from_desc(desc: &types::TTypeDesc) -> Option<DescriptorLogicalType> {
    let nodes = desc.types.as_ref()?;
    let scalar = nodes.first()?.scalar_type.as_ref()?;
    Some(match scalar.type_ {
        t if t == types::TPrimitiveType::NULL_TYPE => DescriptorLogicalType::Null,
        t if t == types::TPrimitiveType::BOOLEAN => DescriptorLogicalType::Boolean,
        t if t == types::TPrimitiveType::TINYINT => DescriptorLogicalType::Int8,
        t if t == types::TPrimitiveType::SMALLINT => DescriptorLogicalType::Int16,
        t if t == types::TPrimitiveType::INT => DescriptorLogicalType::Int32,
        t if t == types::TPrimitiveType::BIGINT => DescriptorLogicalType::Int64,
        t if t == types::TPrimitiveType::LARGEINT => DescriptorLogicalType::LargeInt,
        t if t == types::TPrimitiveType::FLOAT => DescriptorLogicalType::Float32,
        t if t == types::TPrimitiveType::DOUBLE => DescriptorLogicalType::Float64,
        t if t == types::TPrimitiveType::DATE => DescriptorLogicalType::Date,
        t if t == types::TPrimitiveType::DATETIME => DescriptorLogicalType::Timestamp,
        t if t == types::TPrimitiveType::TIME => DescriptorLogicalType::Time,
        t if t == types::TPrimitiveType::DECIMAL256 => DescriptorLogicalType::Decimal256 {
            precision: scalar
                .precision
                .and_then(|v| u8::try_from(v).ok())
                .unwrap_or(76),
            scale: scalar.scale.and_then(|v| i8::try_from(v).ok()).unwrap_or(0),
        },
        t if t == types::TPrimitiveType::DECIMAL
            || t == types::TPrimitiveType::DECIMAL32
            || t == types::TPrimitiveType::DECIMAL64
            || t == types::TPrimitiveType::DECIMAL128
            || t == types::TPrimitiveType::DECIMALV2 =>
        {
            let precision = scalar
                .precision
                .and_then(|v| u8::try_from(v).ok())
                .unwrap_or(38);
            let scale = scalar.scale.and_then(|v| i8::try_from(v).ok()).unwrap_or(0);
            if precision > 38 {
                DescriptorLogicalType::Decimal256 { precision, scale }
            } else {
                DescriptorLogicalType::Decimal128 { precision, scale }
            }
        }
        t if t == types::TPrimitiveType::CHAR || t == types::TPrimitiveType::VARCHAR => {
            DescriptorLogicalType::Utf8
        }
        t if t == types::TPrimitiveType::BINARY || t == types::TPrimitiveType::VARBINARY => {
            DescriptorLogicalType::Binary
        }
        t if t == types::TPrimitiveType::JSON => DescriptorLogicalType::Json,
        t if t == types::TPrimitiveType::VARIANT => DescriptorLogicalType::Variant,
        t if t == types::TPrimitiveType::HLL => DescriptorLogicalType::Hll,
        t if t == types::TPrimitiveType::OBJECT => DescriptorLogicalType::Object,
        t if t == types::TPrimitiveType::PERCENTILE => DescriptorLogicalType::Percentile,
        t if t == types::TPrimitiveType::FUNCTION => DescriptorLogicalType::Function,
        _ => DescriptorLogicalType::Unknown,
    })
}
