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

//! Provider-private frozen Iceberg scan payloads.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_fs::{
    FileCancellation, FileError, FileErrorKind, FileIdentity, ParquetMetadataInspection,
    ParquetStatisticsSortOrder, ParquetStatisticsValue, inspect_parquet_metadata,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_spi::connector::{
    ConnectorPrepareSplitRequest, ConnectorScalarType, ConnectorScalarValue,
    ConnectorScanUnitColumn, ConnectorScanUnitColumnDomain, ConnectorScanUnitColumnFacts,
    ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsEvidence,
    ConnectorScanUnitFactsMissingReason,
};
use serde::{Deserialize, Serialize};

use crate::delta::IcebergDeltaSplitPayload;
use crate::metadata_batch_reader::MetadataTableType;
use crate::scan_model::{IcebergDataFileInfo, IcebergPhysicalPredicate};
use crate::{access_binding::IcebergReadBinding, schema_mapping::is_variant_struct_data_type};

pub const ICEBERG_SPLIT_V5: u16 = 5;
pub const ICEBERG_PREPARED_SPLIT_SHARED_V2: u16 = 2;
pub const ICEBERG_PREPARED_SCAN_UNIT_V1: u16 = 1;

#[derive(Deserialize, Serialize)]
pub struct SplitPayload {
    pub version: u16,
    pub owner_instance_id: String,
    pub incarnation: [u8; 16],
    pub namespace: String,
    pub table: String,
    pub snapshot_id: Option<i64>,
    #[serde(default)]
    pub table_uuid: Option<String>,
    #[serde(default)]
    pub schema_id: Option<i32>,
    pub units: Vec<IcebergFrozenScanUnitPayload>,
    pub projection: Vec<usize>,
    pub limit: Option<u64>,
    #[serde(default)]
    pub physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    pub fact_columns: Vec<IcebergScanFactColumnV1>,
    #[serde(default)]
    pub name_mapping: Option<String>,
    #[serde(default)]
    pub delta: Option<IcebergDeltaSplitPayload>,
    #[serde(default)]
    pub metadata: Option<IcebergMetadataSplitPayloadV1>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergMetadataSplitPayloadV1 {
    pub metadata_table_type: MetadataTableType,
    pub serialized_table: String,
    pub serialized_payload: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergFrozenScanUnitPayload {
    pub data_file: IcebergDataFileInfo,
    pub row_groups: Option<Vec<usize>>,
    pub estimated_bytes: Option<u64>,
}

#[derive(Deserialize, Serialize)]
pub struct IcebergPreparedSplitSharedPayload {
    pub version: u16,
    pub owner_instance_id: String,
    pub incarnation: [u8; 16],
    pub namespace: String,
    pub table: String,
    pub snapshot_id: Option<i64>,
    pub table_uuid: Option<String>,
    pub schema_id: Option<i32>,
    pub projection: Vec<usize>,
    pub limit: Option<u64>,
    pub physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    pub fact_columns: Vec<IcebergScanFactColumnV1>,
    pub name_mapping: Option<String>,
    pub delta: Option<IcebergDeltaSplitPayload>,
    pub metadata: Option<IcebergMetadataSplitPayloadV1>,
}

#[derive(Deserialize, Serialize)]
pub struct IcebergPreparedUnitPayload {
    pub version: u16,
    pub data_file: IcebergDataFileInfo,
    pub row_groups: Option<Vec<usize>>,
}

#[derive(Deserialize, Serialize)]
pub struct IcebergPreparedMetadataUnitPayloadV1 {
    pub version: u16,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergScanFactColumnV1 {
    pub field_ordinal: u32,
    pub field_id: i32,
    pub canonical_name: String,
    pub scalar_type: IcebergScanFactScalarTypeV1,
    pub nullable: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
pub enum IcebergScanFactScalarTypeV1 {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    TimestampMicros,
    TimestampNanos,
    Utf8,
    Binary,
    Unsupported,
}

pub fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("serialize Iceberg {subject}: {error}"),
            )
        })
        .and_then(|payload| {
            if payload.len() > max_payload_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("Iceberg {subject} exceeds the request payload budget"),
                ));
            }
            Ok(Bytes::from(payload))
        })
}

pub fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg {subject}: {error}"),
        )
    })
}

pub fn canonical_split_name_mapping(mapping: &str) -> Result<String, ConnectorError> {
    if mapping.len() > novarocks_spi::connector::MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg name mapping exceeds the provider-private split bound",
        ));
    }
    crate::schema_mapping::canonical_name_mapping(mapping)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))
}

pub fn validate_split_name_mapping(mapping: Option<&str>) -> Result<(), ConnectorError> {
    let Some(mapping) = mapping else {
        return Ok(());
    };
    if canonical_split_name_mapping(mapping)? != mapping {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg split name mapping is not canonical",
        ));
    }
    Ok(())
}

pub fn validate_split_payload(payload: &SplitPayload) -> Result<(), ConnectorError> {
    if payload.version != ICEBERG_SPLIT_V5 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "unsupported Iceberg composite split version {}",
                payload.version
            ),
        ));
    }
    validate_split_name_mapping(payload.name_mapping.as_deref())?;
    Ok(())
}

pub fn validate_prepared_payload(
    shared: &IcebergPreparedSplitSharedPayload,
    unit: &IcebergPreparedUnitPayload,
) -> Result<(), ConnectorError> {
    if shared.version != ICEBERG_PREPARED_SPLIT_SHARED_V2
        || unit.version != ICEBERG_PREPARED_SCAN_UNIT_V1
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "unsupported Iceberg prepared scan unit payload version",
        ));
    }
    validate_split_name_mapping(shared.name_mapping.as_deref())?;
    if let Some(row_groups) = unit.row_groups.as_ref()
        && (row_groups.is_empty() || row_groups.windows(2).any(|window| window[0] >= window[1]))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg prepared scan unit row groups must be non-empty and strictly ordered",
        ));
    }
    Ok(())
}

/// Refine one already-authorized file into sealed local row-group leaves.
pub fn materialize_local_scan_units(
    binding: &IcebergReadBinding,
    frozen_units: Vec<IcebergFrozenScanUnitPayload>,
    special_unit: bool,
    request: &ConnectorPrepareSplitRequest,
) -> Result<Vec<IcebergFrozenScanUnitPayload>, ConnectorError> {
    if special_unit {
        return Ok(frozen_units);
    }
    let mut result = Vec::with_capacity(frozen_units.len());
    for unit in frozen_units {
        request.check_active()?;
        if unit.row_groups.is_some() || !is_parquet_path(&unit.data_file.path) {
            result.push(unit);
            continue;
        }
        let file_size = u64::try_from(unit.data_file.size).map_err(|_| {
            corrupt(format!(
                "Iceberg data file {} has a negative size",
                unit.data_file.path
            ))
        })?;
        let access = binding.resolve_access(&unit.data_file.path)?;
        let file = access
            .bind_location(
                &unit.data_file.path,
                FileIdentity::new(&unit.data_file.path, file_size, None),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let context =
            binding.file_read_context(FileCancellation::new(), request.context.deadline())?;
        let inspection = inspect_parquet_metadata(file, None, context).map_err(map_footer_error)?;
        let layout = inspection.row_groups();
        request.check_active()?;
        if layout.len() <= 1 {
            result.push(unit);
            continue;
        }
        let total = unit
            .estimated_bytes
            .ok_or_else(|| corrupt("Iceberg Parquet split unit must carry a known frozen cost"))?;
        let costs = distribute_unit_cost(total, layout)?;
        for (group, estimated_bytes) in layout.iter().zip(costs) {
            result.push(IcebergFrozenScanUnitPayload {
                data_file: unit.data_file.clone(),
                row_groups: Some(vec![group.ordinal as usize]),
                estimated_bytes: Some(estimated_bytes),
            });
        }
    }
    Ok(result)
}

pub fn iceberg_unit_domain_facts(
    binding: &IcebergReadBinding,
    inspections: &mut HashMap<String, ParquetMetadataInspection>,
    unit: &IcebergFrozenScanUnitPayload,
    columns: &[IcebergScanFactColumnV1],
    conservative: bool,
    special_unit: bool,
    request: &ConnectorPrepareSplitRequest,
) -> Result<ConnectorScanUnitDomainFacts, ConnectorError> {
    if special_unit || !is_parquet_path(&unit.data_file.path) {
        return Ok(ConnectorScanUnitDomainFacts::missing(
            ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
        ));
    }
    if columns.is_empty()
        || columns
            .iter()
            .any(|column| matches!(column.scalar_type, IcebergScanFactScalarTypeV1::Unsupported))
    {
        return Ok(ConnectorScanUnitDomainFacts::missing(
            ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
        ));
    }
    if !inspections.contains_key(&unit.data_file.path) {
        let file_size = u64::try_from(unit.data_file.size).map_err(|_| {
            corrupt(format!(
                "Iceberg data file {} has a negative size",
                unit.data_file.path
            ))
        })?;
        let access = binding.resolve_access(&unit.data_file.path)?;
        let file = access
            .bind_location(
                &unit.data_file.path,
                FileIdentity::new(&unit.data_file.path, file_size, None),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let context =
            binding.file_read_context(FileCancellation::new(), request.context.deadline())?;
        let metadata = inspect_parquet_metadata(file, None, context).map_err(map_footer_error)?;
        inspections.insert(unit.data_file.path.clone(), metadata);
    }
    let inspection = inspections
        .get(&unit.data_file.path)
        .expect("inserted authorized Parquet inspection");
    request.check_active()?;
    let selected = selected_row_groups(inspection, unit.row_groups.as_deref())?;
    let mapped = map_fact_columns(inspection, columns)?;
    let rows = selected.iter().try_fold(0_u64, |total, group| {
        total.checked_add(group.row_count).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg selected Parquet row count overflows facts accounting",
            )
        })
    })?;
    let evidence = if conservative {
        ConnectorScanUnitFactsEvidence::Conservative
    } else {
        ConnectorScanUnitFactsEvidence::Exact
    };
    let facts = mapped
        .iter()
        .map(|(column, ordinal)| column_facts(inspection, &selected, *ordinal, column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    ConnectorScanUnitDomainFacts::available(rows, evidence, facts)
}

fn selected_row_groups<'a>(
    inspection: &'a ParquetMetadataInspection,
    selected: Option<&[usize]>,
) -> Result<Vec<&'a novarocks_fs::ParquetRowGroupLayout>, ConnectorError> {
    match selected {
        None => Ok(inspection.row_groups().iter().collect()),
        Some(selected) => selected.iter().map(|ordinal| inspection.row_groups().get(*ordinal).ok_or_else(|| corrupt("Iceberg prepared scan unit selects a Parquet row group outside the frozen footer"))).collect(),
    }
}

fn map_fact_columns<'a>(
    inspection: &ParquetMetadataInspection,
    columns: &'a [IcebergScanFactColumnV1],
) -> Result<Vec<(&'a IcebergScanFactColumnV1, Option<u32>)>, ConnectorError> {
    let physical = inspection.physical_columns();
    let identity = physical
        .iter()
        .filter(|column| {
            let Some(root) = column.path().first() else {
                return true;
            };
            !inspection.schema().fields().iter().any(|field| {
                field.name().eq_ignore_ascii_case(root)
                    && is_variant_struct_data_type(field.data_type())
            })
        })
        .collect::<Vec<_>>();
    let total = identity.len();
    let with_ids = identity
        .iter()
        .filter(|column| column.field_id().is_some())
        .count();
    if with_ids != 0 && with_ids != total {
        return Err(corrupt(
            "Iceberg Parquet footer has mixed field-ID coverage",
        ));
    }
    columns
        .iter()
        .map(|column| {
            let matches = physical
                .iter()
                .filter(|physical| {
                    if with_ids == total {
                        physical.field_id() == Some(column.field_id)
                    } else {
                        physical.path().len() == 1
                            && physical.path()[0].eq_ignore_ascii_case(&column.canonical_name)
                    }
                })
                .collect::<Vec<_>>();
            if matches.len() > 1 {
                return Err(corrupt(
                    "Iceberg Parquet footer maps a frozen field to multiple physical leaves",
                ));
            }
            Ok((column, matches.first().map(|value| value.ordinal())))
        })
        .collect()
}

fn column_facts(
    inspection: &ParquetMetadataInspection,
    selected: &[&novarocks_fs::ParquetRowGroupLayout],
    physical_ordinal: Option<u32>,
    frozen: &IcebergScanFactColumnV1,
    physical_row_count: u64,
) -> Result<ConnectorScanUnitColumnFacts, ConnectorError> {
    let scalar_type = fact_scalar_type(frozen.scalar_type).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unsupported Iceberg facts scalar reached the Parquet mapper",
        )
    })?;
    let column = ConnectorScanUnitColumn::new(frozen.field_ordinal, scalar_type, frozen.nullable);
    let Some(physical_ordinal) = physical_ordinal else {
        return Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::ValueUnavailable,
        ));
    };
    if physical_row_count == 0 {
        return Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::ValueUnavailable,
        ));
    }

    let mut null_count = 0_u64;
    let mut min: Option<ConnectorScalarValue> = None;
    let mut max: Option<ConnectorScalarValue> = None;
    for row_group in selected {
        let Some(statistics) = inspection.column_statistics(row_group.ordinal, physical_ordinal)
        else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        let Some(row_group_nulls) = statistics.null_count() else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        null_count = null_count.checked_add(row_group_nulls).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg Parquet null count overflows facts accounting",
            )
        })?;
        if row_group_nulls == row_group.row_count {
            continue;
        }
        if !statistics.min_is_exact()
            || !statistics.max_is_exact()
            || statistics.min_max_deprecated()
            || !matches!(
                statistics.sort_order(),
                ParquetStatisticsSortOrder::Signed | ParquetStatisticsSortOrder::Unsigned
            )
        {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        }
        let (Some(row_min), Some(row_max)) = (statistics.min(), statistics.max()) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        let Some(row_min) = statistic_scalar(row_min, frozen.scalar_type) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
            ));
        };
        let Some(row_max) = statistic_scalar(row_max, frozen.scalar_type) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
            ));
        };
        min = match min {
            Some(current)
                if current
                    .compare_same_type(&row_min)
                    .is_some_and(|order| order.is_gt()) =>
            {
                Some(row_min)
            }
            Some(current) => Some(current),
            None => Some(row_min),
        };
        max = match max {
            Some(current)
                if current
                    .compare_same_type(&row_max)
                    .is_some_and(|order| order.is_lt()) =>
            {
                Some(row_max)
            }
            Some(current) => Some(current),
            None => Some(row_max),
        };
    }
    if null_count > physical_row_count {
        return Err(corrupt(
            "Iceberg Parquet null count exceeds selected physical rows",
        ));
    }
    if null_count == physical_row_count {
        return ConnectorScanUnitColumnDomain::try_all_null(column, null_count, physical_row_count);
    }
    match (min, max) {
        (Some(min), Some(max)) => ConnectorScanUnitColumnDomain::try_range(
            column,
            min,
            max,
            null_count,
            physical_row_count,
        ),
        _ => Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
        )),
    }
}

fn fact_scalar_type(scalar_type: IcebergScanFactScalarTypeV1) -> Option<ConnectorScalarType> {
    Some(match scalar_type {
        IcebergScanFactScalarTypeV1::Boolean => ConnectorScalarType::Boolean,
        IcebergScanFactScalarTypeV1::Int8 => ConnectorScalarType::Int8,
        IcebergScanFactScalarTypeV1::Int16 => ConnectorScalarType::Int16,
        IcebergScanFactScalarTypeV1::Int32 => ConnectorScalarType::Int32,
        IcebergScanFactScalarTypeV1::Int64 => ConnectorScalarType::Int64,
        IcebergScanFactScalarTypeV1::Date32 => ConnectorScalarType::Date32,
        IcebergScanFactScalarTypeV1::TimestampMicros => ConnectorScalarType::TimestampMicros,
        IcebergScanFactScalarTypeV1::TimestampNanos => ConnectorScalarType::TimestampNanos,
        IcebergScanFactScalarTypeV1::Utf8 => ConnectorScalarType::Utf8,
        IcebergScanFactScalarTypeV1::Binary => ConnectorScalarType::Binary,
        IcebergScanFactScalarTypeV1::Unsupported => return None,
    })
}

fn statistic_scalar(
    value: &ParquetStatisticsValue,
    scalar_type: IcebergScanFactScalarTypeV1,
) -> Option<ConnectorScalarValue> {
    match (scalar_type, value) {
        (IcebergScanFactScalarTypeV1::Boolean, ParquetStatisticsValue::Boolean(value)) => {
            Some(ConnectorScalarValue::Boolean(*value))
        }
        (IcebergScanFactScalarTypeV1::Int32, ParquetStatisticsValue::Int32(value)) => {
            Some(ConnectorScalarValue::Int32(*value))
        }
        (IcebergScanFactScalarTypeV1::Date32, ParquetStatisticsValue::Int32(value)) => {
            Some(ConnectorScalarValue::Date32(*value))
        }
        (IcebergScanFactScalarTypeV1::Int64, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::Int64(*value))
        }
        (IcebergScanFactScalarTypeV1::TimestampMicros, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::TimestampMicros(*value))
        }
        (IcebergScanFactScalarTypeV1::TimestampNanos, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::TimestampNanos(*value))
        }
        (IcebergScanFactScalarTypeV1::Utf8, ParquetStatisticsValue::ByteArray(value)) => {
            std::str::from_utf8(value)
                .ok()
                .map(|value| ConnectorScalarValue::Utf8(value.to_string()))
        }
        (IcebergScanFactScalarTypeV1::Binary, ParquetStatisticsValue::ByteArray(value)) => {
            Some(ConnectorScalarValue::Binary(value.clone()))
        }
        _ => None,
    }
}

fn distribute_unit_cost(
    total: u64,
    layout: &[novarocks_fs::ParquetRowGroupLayout],
) -> Result<Vec<u64>, ConnectorError> {
    let weight_total = layout.iter().try_fold(0_u64, |sum, row_group| {
        sum.checked_add(row_group.compressed_bytes)
    });
    let mut costs = Vec::with_capacity(layout.len());
    if let Some(weight_total) = weight_total.filter(|total| *total > 0) {
        let mut assigned = 0_u64;
        for (index, row_group) in layout.iter().enumerate() {
            let cost = if index + 1 == layout.len() {
                total.checked_sub(assigned).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg row-group cost accounting underflowed",
                    )
                })?
            } else {
                total
                    .checked_mul(row_group.compressed_bytes)
                    .and_then(|value| value.checked_div(weight_total))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "Iceberg row-group cost accounting overflowed",
                        )
                    })?
            };
            assigned = assigned.checked_add(cost).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-group cost accounting overflowed",
                )
            })?;
            costs.push(cost);
        }
    } else {
        let count = u64::try_from(layout.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg row-group count overflows u64",
            )
        })?;
        if count == 0 {
            return Ok(costs);
        }
        let base = total / count;
        let mut remainder = total % count;
        for _ in layout {
            let extra = u64::from(remainder > 0);
            remainder = remainder.saturating_sub(extra);
            costs.push(base + extra);
        }
    }
    Ok(costs)
}

pub fn scan_fact_scalar_type(data_type: &DataType) -> IcebergScanFactScalarTypeV1 {
    match data_type {
        DataType::Boolean => IcebergScanFactScalarTypeV1::Boolean,
        DataType::Int8 => IcebergScanFactScalarTypeV1::Int8,
        DataType::Int16 => IcebergScanFactScalarTypeV1::Int16,
        DataType::Int32 => IcebergScanFactScalarTypeV1::Int32,
        DataType::Int64 => IcebergScanFactScalarTypeV1::Int64,
        DataType::Date32 => IcebergScanFactScalarTypeV1::Date32,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            IcebergScanFactScalarTypeV1::TimestampMicros
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            IcebergScanFactScalarTypeV1::TimestampNanos
        }
        DataType::Utf8 => IcebergScanFactScalarTypeV1::Utf8,
        DataType::Binary => IcebergScanFactScalarTypeV1::Binary,
        _ => IcebergScanFactScalarTypeV1::Unsupported,
    }
}

fn is_parquet_path(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path).to_ascii_lowercase();
    path.ends_with(".parquet") || path.ends_with(".parq")
}

fn map_footer_error(error: FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        FileErrorKind::NotFound | FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::AlreadyExists | FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

#[cfg(test)]
mod tests {
    use super::distribute_unit_cost;

    #[test]
    fn row_group_cost_distribution_is_deterministic() {
        let weighted = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 1,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 3,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 6,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("weighted costs"),
            [10, 30, 61]
        );
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("repeated weighted costs"),
            [10, 30, 61]
        );

        let zero_weight = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 0,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(8, &zero_weight).expect("zero-weight costs"),
            [3, 3, 2]
        );
    }
}
