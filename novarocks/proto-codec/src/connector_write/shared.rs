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

//! Validation shared by both write carriers.

use novarocks_proto_models::connector_write as dto;

use super::{
    MAX_COLUMN_STAT_BOUND_BYTES, MAX_COLUMN_STAT_ENTRIES, MAX_NAME_BYTES,
    MAX_PARTITION_VALUE_BYTES, MAX_PARTITION_VALUES, MAX_PATH_BYTES, MAX_SPLIT_OFFSETS,
    bounded_bytes, bounded_count, bounded_text, inconsistent, invalid_enum, missing,
    nonnegative_i64, out_of_range,
};
use crate::{FieldPath, ProtocolError};

pub(super) fn validate_file_format(
    value: i32,
    path: FieldPath,
) -> Result<dto::IcebergFileFormat, ProtocolError> {
    match dto::IcebergFileFormat::try_from(value) {
        Ok(dto::IcebergFileFormat::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "file format must be a named Iceberg file format",
        )),
        Ok(format) => Ok(format),
    }
}

pub(super) fn validate_file_content(
    value: i32,
    path: FieldPath,
) -> Result<dto::IcebergFileContent, ProtocolError> {
    match dto::IcebergFileContent::try_from(value) {
        Ok(dto::IcebergFileContent::Unspecified) | Err(_) => Err(invalid_enum(
            path,
            "file content must be a named Iceberg file content kind",
        )),
        Ok(content) => Ok(content),
    }
}

pub(super) fn validate_content_range(
    range: &dto::IcebergContentRange,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    nonnegative_i64(range.offset, path.clone().field("offset"), "content offset")?;
    let size = nonnegative_i64(
        range.size_in_bytes,
        path.clone().field("size_in_bytes"),
        "content size",
    )?;
    if size == 0 {
        return Err(out_of_range(
            path.field("size_in_bytes"),
            "a content range must cover at least one byte",
        ));
    }
    Ok(())
}

/// A partition descriptor value carries a payload exactly when it is not null.
/// Repairing a disagreement here would silently move a row into another
/// partition, so it is rejected instead.
pub(super) fn validate_partition_descriptor(
    descriptor: &dto::IcebergPartitionDescriptor,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    bounded_count(
        descriptor.values.len(),
        MAX_PARTITION_VALUES,
        path.clone().field("values"),
        "partition value",
    )?;
    for (index, value) in descriptor.values.iter().enumerate() {
        let value_path = path.clone().field("values").index(index);
        match (value.is_null, value.datum_bytes.as_ref()) {
            (true, Some(_)) => {
                return Err(inconsistent(
                    value_path,
                    "a null partition value must not carry a datum",
                ));
            }
            (false, None) => {
                return Err(inconsistent(
                    value_path,
                    "a non-null partition value requires a datum",
                ));
            }
            (false, Some(datum)) => {
                bounded_bytes(datum, MAX_PARTITION_VALUE_BYTES, value_path)?;
            }
            (true, None) => {}
        }
    }
    Ok(())
}

pub(super) fn validate_artifact_partition(
    partition: Option<&dto::IcebergArtifactPartition>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let partition =
        partition.ok_or_else(|| missing(path.clone(), "an artifact requires its partition"))?;
    bounded_text(
        &partition.partition_path,
        MAX_PATH_BYTES,
        path.clone().field("partition_path"),
        true,
    )?;
    bounded_text(
        &partition.null_fingerprint,
        MAX_NAME_BYTES,
        path.clone().field("null_fingerprint"),
        true,
    )?;
    if partition.partition_spec_id < 0 {
        return Err(out_of_range(
            path.clone().field("partition_spec_id"),
            "partition spec id must be nonnegative",
        ));
    }
    let descriptor = partition.descriptor.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("descriptor"),
            "an artifact partition requires its descriptor",
        )
    })?;
    validate_partition_descriptor(descriptor, path.field("descriptor"))
}

fn validate_stat_counts(
    entries: &std::collections::BTreeMap<i32, i64>,
    path: FieldPath,
    label: &'static str,
) -> Result<(), ProtocolError> {
    bounded_count(entries.len(), MAX_COLUMN_STAT_ENTRIES, path.clone(), label)?;
    for (field_id, value) in entries {
        let entry_path = path.clone().map_key(field_id.to_string());
        nonnegative_i64(*value, entry_path, label)?;
    }
    Ok(())
}

fn validate_stat_bounds(
    entries: &std::collections::BTreeMap<i32, Vec<u8>>,
    path: FieldPath,
    label: &'static str,
) -> Result<(), ProtocolError> {
    bounded_count(entries.len(), MAX_COLUMN_STAT_ENTRIES, path.clone(), label)?;
    for (field_id, value) in entries {
        bounded_bytes(
            value,
            MAX_COLUMN_STAT_BOUND_BYTES,
            path.clone().map_key(field_id.to_string()),
        )?;
    }
    Ok(())
}

pub(super) fn validate_artifact_metrics(
    metrics: Option<&dto::IcebergArtifactMetrics>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let metrics =
        metrics.ok_or_else(|| missing(path.clone(), "an artifact requires its metrics"))?;
    bounded_count(
        metrics.split_offsets.len(),
        MAX_SPLIT_OFFSETS,
        path.clone().field("split_offsets"),
        "split offset",
    )?;
    for (index, offset) in metrics.split_offsets.iter().enumerate() {
        nonnegative_i64(
            *offset,
            path.clone().field("split_offsets").index(index),
            "split offset",
        )?;
    }
    let Some(stats) = metrics.column_stats.as_ref() else {
        return Ok(());
    };
    let stats_path = path.field("column_stats");
    validate_stat_counts(
        &stats.column_sizes,
        stats_path.clone().field("column_sizes"),
        "column size",
    )?;
    validate_stat_counts(
        &stats.value_counts,
        stats_path.clone().field("value_counts"),
        "value count",
    )?;
    validate_stat_counts(
        &stats.null_value_counts,
        stats_path.clone().field("null_value_counts"),
        "null value count",
    )?;
    validate_stat_counts(
        &stats.nan_value_counts,
        stats_path.clone().field("nan_value_counts"),
        "nan value count",
    )?;
    validate_stat_bounds(
        &stats.lower_bounds,
        stats_path.clone().field("lower_bounds"),
        "lower bound",
    )?;
    validate_stat_bounds(
        &stats.upper_bounds,
        stats_path.field("upper_bounds"),
        "upper bound",
    )
}
