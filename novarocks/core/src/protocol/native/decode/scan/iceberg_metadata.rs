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
use crate::connector::ScanConfig;
use crate::connector::iceberg::metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanRange,
    IcebergMetadataTableType,
};
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::runtime::scan_range::{ScanRange, ScanRangeParams};

use super::super::node::{DecodedNode, NativePlanDecodeContext};
use super::common::{DecodedScanOutputColumns, lower_scan_predicate, parse_scan_limit};
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn lower_iceberg_metadata_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergMetadataTable,
    output_columns: &DecodedScanOutputColumns,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    let layout = output_columns.layout();
    let output_schema = output_columns.output_schema();
    let metadata_table_type = metadata_table_type(source.metadata_table_type).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "metadata_table_type",
            error,
        )
    })?;
    let ranges = decode_metadata_scan_ranges(ctx.scan_ranges(node.node_id)?).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "ranges", error)
    })?;
    let cfg = IcebergMetadataScanConfig {
        metadata_table_type,
        serialized_table: source.serialized_table.clone(),
        serialized_predicate: source.metadata_payload.clone().unwrap_or_default(),
        load_column_stats: false,
        ranges,
        batch_size: 4096,
        output_columns: metadata_output_columns(output_columns),
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("iceberg", ScanConfig::IcebergMetadata(cfg))
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "serialized_table",
                error,
            )
        })?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

fn decode_metadata_scan_ranges(
    ranges: &[ScanRangeParams],
) -> Result<Vec<IcebergMetadataScanRange>, String> {
    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(format!(
                    "IcebergMetadataTable range {idx} has_more is not supported by native lowering"
                ));
            }
            if range.empty.unwrap_or(false) {
                return Ok(None);
            }
            let ScanRange::File(file) = &range.range else {
                return Err(format!(
                    "IcebergMetadataTable range {idx} expected file range"
                ));
            };
            Ok(Some(IcebergMetadataScanRange {
                path: file.full_path.clone().unwrap_or_default(),
                serialized_split: file.serialized_split.clone().unwrap_or_default(),
            }))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|ranges| ranges.into_iter().flatten().collect())
}

fn metadata_output_columns(
    output_columns: &DecodedScanOutputColumns,
) -> Vec<IcebergMetadataOutputColumn> {
    let schema = output_columns.output_schema();
    output_columns
        .columns()
        .iter()
        .zip(schema.slots())
        .map(|(col, slot)| IcebergMetadataOutputColumn {
            name: col.name.clone(),
            slot_id: SlotId::new(col.column_id),
            data_type: slot.data_type().clone(),
            nullable: col.nullable,
        })
        .collect()
}

fn metadata_table_type(value: i32) -> Result<IcebergMetadataTableType, String> {
    match plan::IcebergMetadataTableType::try_from(value)
        .map_err(|_| format!("unknown Iceberg metadata table type {value}"))?
    {
        plan::IcebergMetadataTableType::Files => Ok(IcebergMetadataTableType::Files),
        plan::IcebergMetadataTableType::Manifests => Ok(IcebergMetadataTableType::Manifests),
        plan::IcebergMetadataTableType::LogicalIcebergMetadata => {
            Ok(IcebergMetadataTableType::LogicalIcebergMetadata)
        }
        plan::IcebergMetadataTableType::Snapshots => Ok(IcebergMetadataTableType::Snapshots),
        plan::IcebergMetadataTableType::History => Ok(IcebergMetadataTableType::History),
        plan::IcebergMetadataTableType::Refs => Ok(IcebergMetadataTableType::Refs),
        plan::IcebergMetadataTableType::Partitions => Ok(IcebergMetadataTableType::Partitions),
        plan::IcebergMetadataTableType::Unspecified => {
            Err("Iceberg metadata table type is unspecified".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::decode_metadata_scan_ranges;

    #[test]
    fn metadata_scan_empty_ranges_decode_to_empty_no_synthetic_morsel() {
        let decoded = decode_metadata_scan_ranges(&[]).expect("decode empty");
        assert!(decoded.is_empty());
    }
}
