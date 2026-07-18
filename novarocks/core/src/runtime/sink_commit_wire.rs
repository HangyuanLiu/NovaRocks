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

#![cfg(feature = "compat")]

use std::collections::BTreeMap;

use crate::common::engine_error::EngineError;
use crate::connector::iceberg::delete_file::IcebergFileContent;
use crate::connector::iceberg::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
};
use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
};
use crate::proto::novarocks;
use crate::thrift::types;

pub(crate) fn partition_descriptor_to_thrift(
    desc: IcebergPartitionDescriptor,
) -> types::TIcebergPartitionDescriptor {
    types::TIcebergPartitionDescriptor {
        values: Some(
            desc.values
                .into_iter()
                .map(|value| types::TIcebergPartitionValue {
                    is_null: Some(value.is_null),
                    datum_bytes: value.datum_bytes,
                })
                .collect(),
        ),
    }
}

pub(crate) fn partition_descriptor_from_thrift(
    desc: Option<types::TIcebergPartitionDescriptor>,
) -> Result<Option<IcebergPartitionDescriptor>, IcebergWriteDescriptorError> {
    let Some(desc) = desc else {
        return Ok(None);
    };
    let values = desc
        .values
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            let is_null =
                value
                    .is_null
                    .ok_or_else(|| IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: "partition descriptor value is missing null marker".to_string(),
                    })?;
            Ok(IcebergPartitionValueDescriptor {
                is_null,
                datum_bytes: value.datum_bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(IcebergPartitionDescriptor { values }))
}

pub(crate) fn sink_commit_info_to_native(
    info: types::TSinkCommitInfo,
) -> Result<novarocks::IcebergCommitInfo, String> {
    let df = info
        .iceberg_data_file
        .ok_or_else(|| "TSinkCommitInfo missing iceberg_data_file".to_string())?;
    Ok(novarocks::IcebergCommitInfo {
        iceberg_data_file: Some(data_file_to_native(df)),
        is_overwrite: info.is_overwrite,
        is_rewrite: info.is_rewrite,
    })
}

pub(crate) fn sink_commit_info_from_native(
    info: novarocks::IcebergCommitInfo,
) -> Result<types::TSinkCommitInfo, String> {
    let df = info
        .iceberg_data_file
        .ok_or_else(|| "IcebergCommitInfo missing iceberg_data_file".to_string())?;
    Ok(types::TSinkCommitInfo {
        iceberg_data_file: Some(data_file_from_native(df)?),
        hive_file_info: None,
        is_overwrite: info.is_overwrite,
        staging_dir: None,
        is_rewrite: info.is_rewrite,
    })
}

fn data_file_to_native(df: types::TIcebergDataFile) -> novarocks::IcebergDataFile {
    novarocks::IcebergDataFile {
        path: df.path,
        format: df.format,
        record_count: df.record_count,
        file_size_in_bytes: df.file_size_in_bytes,
        partition_path: df.partition_path,
        split_offsets: df
            .split_offsets
            .map(|values| novarocks::Int64List { values }),
        column_stats: df.column_stats.map(column_stats_to_native),
        partition_null_fingerprint: df.partition_null_fingerprint,
        file_content: df
            .file_content
            .map(file_content_to_native)
            .unwrap_or(novarocks::IcebergFileContent::Unspecified) as i32,
        referenced_data_file: df.referenced_data_file,
        first_row_id: df.first_row_id,
        equality_ids: df
            .equality_ids
            .map(|values| novarocks::Int32List { values }),
        key_metadata: df.key_metadata,
        partition_spec_id: df.partition_spec_id,
        partition_values_descriptor: df
            .partition_values_descriptor
            .map(partition_descriptor_to_native),
        content_offset: df.content_offset,
        content_size_in_bytes: df.content_size_in_bytes,
        cardinality: df.cardinality,
    }
}

fn data_file_from_native(
    df: novarocks::IcebergDataFile,
) -> Result<types::TIcebergDataFile, String> {
    Ok(types::TIcebergDataFile {
        path: df.path,
        format: df.format,
        record_count: df.record_count,
        file_size_in_bytes: df.file_size_in_bytes,
        partition_path: df.partition_path,
        split_offsets: df.split_offsets.map(|values| values.values),
        column_stats: df.column_stats.map(column_stats_from_native),
        partition_null_fingerprint: df.partition_null_fingerprint,
        file_content: Some(file_content_from_native_proto(df.file_content)?),
        referenced_data_file: df.referenced_data_file,
        first_row_id: df.first_row_id,
        equality_ids: df.equality_ids.map(|values| values.values),
        key_metadata: df.key_metadata,
        partition_spec_id: df.partition_spec_id,
        partition_values_descriptor: df
            .partition_values_descriptor
            .map(partition_descriptor_from_native)
            .transpose()?,
        content_offset: df.content_offset,
        content_size_in_bytes: df.content_size_in_bytes,
        cardinality: df.cardinality,
    })
}

fn column_stats_to_native(stats: types::TIcebergColumnStats) -> novarocks::IcebergColumnStats {
    novarocks::IcebergColumnStats {
        column_sizes: stats.column_sizes.unwrap_or_default().into_iter().collect(),
        value_counts: stats.value_counts.unwrap_or_default().into_iter().collect(),
        null_value_counts: stats
            .null_value_counts
            .unwrap_or_default()
            .into_iter()
            .collect(),
        nan_value_counts: stats
            .nan_value_counts
            .unwrap_or_default()
            .into_iter()
            .collect(),
        lower_bounds: stats.lower_bounds.unwrap_or_default().into_iter().collect(),
        upper_bounds: stats.upper_bounds.unwrap_or_default().into_iter().collect(),
    }
}

fn column_stats_from_native(stats: novarocks::IcebergColumnStats) -> types::TIcebergColumnStats {
    types::TIcebergColumnStats {
        column_sizes: non_empty(stats.column_sizes.into_iter().collect()),
        value_counts: non_empty(stats.value_counts.into_iter().collect()),
        null_value_counts: non_empty(stats.null_value_counts.into_iter().collect()),
        nan_value_counts: non_empty(stats.nan_value_counts.into_iter().collect()),
        lower_bounds: non_empty(stats.lower_bounds.into_iter().collect()),
        upper_bounds: non_empty(stats.upper_bounds.into_iter().collect()),
    }
}

fn partition_descriptor_to_native(
    desc: types::TIcebergPartitionDescriptor,
) -> novarocks::IcebergPartitionDescriptor {
    novarocks::IcebergPartitionDescriptor {
        values: desc
            .values
            .unwrap_or_default()
            .into_iter()
            .map(|value| novarocks::IcebergPartitionValue {
                is_null: value.is_null,
                datum_bytes: value.datum_bytes,
            })
            .collect(),
    }
}

fn partition_descriptor_from_native(
    desc: novarocks::IcebergPartitionDescriptor,
) -> Result<types::TIcebergPartitionDescriptor, String> {
    Ok(types::TIcebergPartitionDescriptor {
        values: Some(
            desc.values
                .into_iter()
                .map(|value| types::TIcebergPartitionValue {
                    is_null: value.is_null,
                    datum_bytes: value.datum_bytes,
                })
                .collect(),
        ),
    })
}

fn file_content_to_native(content: types::TIcebergFileContent) -> novarocks::IcebergFileContent {
    match content {
        types::TIcebergFileContent::DATA => novarocks::IcebergFileContent::Data,
        types::TIcebergFileContent::POSITION_DELETES => {
            novarocks::IcebergFileContent::PositionDeletes
        }
        types::TIcebergFileContent::EQUALITY_DELETES => {
            novarocks::IcebergFileContent::EqualityDeletes
        }
        _ => novarocks::IcebergFileContent::Unspecified,
    }
}

fn file_content_from_native_proto(value: i32) -> Result<types::TIcebergFileContent, String> {
    match novarocks::IcebergFileContent::try_from(value) {
        Ok(novarocks::IcebergFileContent::Data) => Ok(types::TIcebergFileContent::DATA),
        Ok(novarocks::IcebergFileContent::PositionDeletes) => {
            Ok(types::TIcebergFileContent::POSITION_DELETES)
        }
        Ok(novarocks::IcebergFileContent::EqualityDeletes) => {
            Ok(types::TIcebergFileContent::EQUALITY_DELETES)
        }
        Ok(novarocks::IcebergFileContent::Unspecified) => {
            Err("IcebergDataFile missing file_content".to_string())
        }
        Err(_) => Err(format!(
            "unknown IcebergFileContent value {value} in native sink commit info"
        )),
    }
}

pub(crate) fn writer_report_to_sink_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<types::TSinkCommitInfo, String> {
    sink_commit_info_from_native(
        crate::runtime::sink_commit::writer_report_to_iceberg_commit_info(report, metadata)?,
    )
}

pub(crate) fn sink_commit_info_to_writer_report(
    info: types::TSinkCommitInfo,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<IcebergWriterReport, String> {
    let df = info
        .iceberg_data_file
        .ok_or_else(|| "sink_commit_info missing iceberg_data_file".to_string())?;
    let path = df
        .path
        .ok_or_else(|| "TIcebergDataFile missing path".to_string())?;
    let format = df
        .format
        .ok_or_else(|| "TIcebergDataFile missing format".to_string())?;
    let record_count = df
        .record_count
        .ok_or_else(|| "TIcebergDataFile missing record_count".to_string())?;
    let file_size_in_bytes = df
        .file_size_in_bytes
        .ok_or_else(|| "TIcebergDataFile missing file_size_in_bytes".to_string())?;
    let file_content = df
        .file_content
        .ok_or_else(|| "TIcebergDataFile missing file_content".to_string())?;
    let partition_spec_id = df.partition_spec_id.ok_or_else(|| {
        EngineError::iceberg_write_descriptor_mismatch("TIcebergDataFile missing partition_spec_id")
            .to_bracketed_user_message()
    })?;
    let partition_descriptor = partition_descriptor_from_thrift(df.partition_values_descriptor)
        .map_err(|e| EngineError::from(e).to_bracketed_user_message())?;
    let partition_values =
        crate::connector::iceberg::write_descriptor::decode_partition_descriptor(
            partition_descriptor,
            partition_spec_id,
            metadata,
        )
        .map_err(|e| EngineError::from(e).to_bracketed_user_message())?;

    Ok(IcebergWriterReport {
        file: IcebergWrittenFileReport {
            path,
            format,
            content: file_content_from_thrift(file_content)?,
            record_count,
            file_size_in_bytes,
            partition: IcebergPartitionReport {
                partition_path: df.partition_path.unwrap_or_default(),
                null_fingerprint: df.partition_null_fingerprint.unwrap_or_default(),
                partition_spec_id,
                partition_values,
            },
            split_offsets: df.split_offsets,
            column_stats: column_stats_from_thrift(df.column_stats),
            referenced_data_file: df.referenced_data_file,
            first_row_id: df.first_row_id,
            equality_ids: df.equality_ids,
            key_metadata: df.key_metadata,
            content_offset: df.content_offset,
            content_size_in_bytes: df.content_size_in_bytes,
            cardinality: df.cardinality,
        },
        is_overwrite: info.is_overwrite,
        is_rewrite: info.is_rewrite,
    })
}

pub(crate) fn sink_commit_infos_to_writer_reports<I>(
    infos: I,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String>
where
    I: IntoIterator<Item = types::TSinkCommitInfo>,
{
    infos
        .into_iter()
        .map(|info| sink_commit_info_to_writer_report(info, metadata))
        .collect()
}

fn column_stats_to_thrift(stats: IcebergColumnStats) -> Option<types::TIcebergColumnStats> {
    if stats.is_empty() {
        return None;
    }
    Some(types::TIcebergColumnStats {
        column_sizes: non_empty(stats.column_sizes),
        value_counts: non_empty(stats.value_counts),
        null_value_counts: non_empty(stats.null_value_counts),
        nan_value_counts: non_empty(stats.nan_value_counts),
        lower_bounds: non_empty(stats.lower_bounds),
        upper_bounds: non_empty(stats.upper_bounds),
    })
}

fn non_empty<K: Ord, V>(map: BTreeMap<K, V>) -> Option<BTreeMap<K, V>> {
    (!map.is_empty()).then_some(map)
}

fn column_stats_from_thrift(
    stats: Option<types::TIcebergColumnStats>,
) -> Option<IcebergColumnStats> {
    stats.map(|stats| IcebergColumnStats {
        column_sizes: stats.column_sizes.unwrap_or_default(),
        value_counts: stats.value_counts.unwrap_or_default(),
        null_value_counts: stats.null_value_counts.unwrap_or_default(),
        nan_value_counts: stats.nan_value_counts.unwrap_or_default(),
        lower_bounds: stats.lower_bounds.unwrap_or_default(),
        upper_bounds: stats.upper_bounds.unwrap_or_default(),
    })
}

fn file_content_to_thrift(content: IcebergFileContent) -> types::TIcebergFileContent {
    match content {
        IcebergFileContent::Data => types::TIcebergFileContent::DATA,
        IcebergFileContent::PositionDeletes => types::TIcebergFileContent::POSITION_DELETES,
        IcebergFileContent::EqualityDeletes => types::TIcebergFileContent::EQUALITY_DELETES,
    }
}

fn file_content_from_thrift(
    content: types::TIcebergFileContent,
) -> Result<IcebergFileContent, String> {
    match content {
        types::TIcebergFileContent::DATA => Ok(IcebergFileContent::Data),
        types::TIcebergFileContent::POSITION_DELETES => Ok(IcebergFileContent::PositionDeletes),
        types::TIcebergFileContent::EQUALITY_DELETES => Ok(IcebergFileContent::EqualityDeletes),
        other => Err(format!(
            "unexpected TIcebergFileContent variant {other:?} in sink_commit_info"
        )),
    }
}
