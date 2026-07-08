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

use std::collections::BTreeMap;

use crate::common::engine_error::EngineError;
use crate::connector::iceberg::delete_file::IcebergFileContent;
use crate::connector::iceberg::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
};
use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
    encode_partition_descriptor,
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

fn iceberg_partition_descriptor_to_native(
    desc: IcebergPartitionDescriptor,
) -> novarocks::IcebergPartitionDescriptor {
    novarocks::IcebergPartitionDescriptor {
        values: desc
            .values
            .into_iter()
            .map(|value| novarocks::IcebergPartitionValue {
                is_null: Some(value.is_null),
                datum_bytes: value.datum_bytes,
            })
            .collect(),
    }
}

fn iceberg_partition_descriptor_from_native(
    desc: Option<novarocks::IcebergPartitionDescriptor>,
) -> Result<Option<IcebergPartitionDescriptor>, IcebergWriteDescriptorError> {
    let Some(desc) = desc else {
        return Ok(None);
    };
    let values = desc
        .values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            let is_null =
                value
                    .is_null
                    .ok_or_else(|| IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: "native partition descriptor value is missing null marker"
                            .to_string(),
                    })?;
            Ok(IcebergPartitionValueDescriptor {
                is_null,
                datum_bytes: value.datum_bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(IcebergPartitionDescriptor { values }))
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

fn column_stats_to_native_report(stats: IcebergColumnStats) -> novarocks::IcebergColumnStats {
    novarocks::IcebergColumnStats {
        column_sizes: stats.column_sizes.into_iter().collect(),
        value_counts: stats.value_counts.into_iter().collect(),
        null_value_counts: stats.null_value_counts.into_iter().collect(),
        nan_value_counts: stats.nan_value_counts.into_iter().collect(),
        lower_bounds: stats.lower_bounds.into_iter().collect(),
        upper_bounds: stats.upper_bounds.into_iter().collect(),
    }
}

fn column_stats_from_native_report(stats: novarocks::IcebergColumnStats) -> IcebergColumnStats {
    IcebergColumnStats {
        column_sizes: stats.column_sizes.into_iter().collect(),
        value_counts: stats.value_counts.into_iter().collect(),
        null_value_counts: stats.null_value_counts.into_iter().collect(),
        nan_value_counts: stats.nan_value_counts.into_iter().collect(),
        lower_bounds: stats.lower_bounds.into_iter().collect(),
        upper_bounds: stats.upper_bounds.into_iter().collect(),
    }
}

fn file_content_to_native_report(content: IcebergFileContent) -> novarocks::IcebergFileContent {
    match content {
        IcebergFileContent::Data => novarocks::IcebergFileContent::Data,
        IcebergFileContent::PositionDeletes => novarocks::IcebergFileContent::PositionDeletes,
        IcebergFileContent::EqualityDeletes => novarocks::IcebergFileContent::EqualityDeletes,
    }
}

fn file_content_from_native_report(value: i32) -> Result<IcebergFileContent, String> {
    match novarocks::IcebergFileContent::try_from(value) {
        Ok(novarocks::IcebergFileContent::Data) => Ok(IcebergFileContent::Data),
        Ok(novarocks::IcebergFileContent::PositionDeletes) => {
            Ok(IcebergFileContent::PositionDeletes)
        }
        Ok(novarocks::IcebergFileContent::EqualityDeletes) => {
            Ok(IcebergFileContent::EqualityDeletes)
        }
        Ok(novarocks::IcebergFileContent::Unspecified) => {
            Err("IcebergDataFile missing file_content".to_string())
        }
        Err(_) => Err(format!(
            "unknown IcebergFileContent value {value} in native iceberg commit info"
        )),
    }
}

pub(crate) fn writer_report_to_sink_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<types::TSinkCommitInfo, String> {
    sink_commit_info_from_native(writer_report_to_iceberg_commit_info(report, metadata)?)
}

pub(crate) fn writer_report_to_iceberg_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<novarocks::IcebergCommitInfo, String> {
    let partition_values_descriptor = iceberg_partition_descriptor_to_native(
        encode_partition_descriptor(
            &report.file.partition.partition_values,
            report.file.partition.partition_spec_id,
            metadata,
        )
        .map_err(|e| EngineError::from(e).to_bracketed_user_message())?,
    );
    Ok(novarocks::IcebergCommitInfo {
        iceberg_data_file: Some(novarocks::IcebergDataFile {
            path: Some(report.file.path),
            format: Some(report.file.format),
            record_count: Some(report.file.record_count),
            file_size_in_bytes: Some(report.file.file_size_in_bytes),
            partition_path: Some(report.file.partition.partition_path),
            split_offsets: report
                .file
                .split_offsets
                .map(|values| novarocks::Int64List { values }),
            column_stats: report.file.column_stats.map(column_stats_to_native_report),
            partition_null_fingerprint: Some(report.file.partition.null_fingerprint),
            file_content: file_content_to_native_report(report.file.content) as i32,
            referenced_data_file: report.file.referenced_data_file,
            first_row_id: report.file.first_row_id,
            equality_ids: report
                .file
                .equality_ids
                .map(|values| novarocks::Int32List { values }),
            key_metadata: report.file.key_metadata,
            partition_spec_id: Some(report.file.partition.partition_spec_id),
            partition_values_descriptor: Some(partition_values_descriptor),
            content_offset: report.file.content_offset,
            content_size_in_bytes: report.file.content_size_in_bytes,
            cardinality: report.file.cardinality,
        }),
        is_overwrite: report.is_overwrite,
        is_rewrite: report.is_rewrite,
    })
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

pub(crate) fn iceberg_commit_info_to_writer_report(
    info: novarocks::IcebergCommitInfo,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<IcebergWriterReport, String> {
    let df = info
        .iceberg_data_file
        .ok_or_else(|| "IcebergCommitInfo missing iceberg_data_file".to_string())?;
    let path = df
        .path
        .ok_or_else(|| "IcebergDataFile missing path".to_string())?;
    let format = df
        .format
        .ok_or_else(|| "IcebergDataFile missing format".to_string())?;
    let record_count = df
        .record_count
        .ok_or_else(|| "IcebergDataFile missing record_count".to_string())?;
    let file_size_in_bytes = df
        .file_size_in_bytes
        .ok_or_else(|| "IcebergDataFile missing file_size_in_bytes".to_string())?;
    let partition_spec_id = df.partition_spec_id.ok_or_else(|| {
        EngineError::iceberg_write_descriptor_mismatch("IcebergDataFile missing partition_spec_id")
            .to_bracketed_user_message()
    })?;
    let partition_descriptor =
        iceberg_partition_descriptor_from_native(df.partition_values_descriptor)
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
            content: file_content_from_native_report(df.file_content)?,
            record_count,
            file_size_in_bytes,
            partition: IcebergPartitionReport {
                partition_path: df.partition_path.unwrap_or_default(),
                null_fingerprint: df.partition_null_fingerprint.unwrap_or_default(),
                partition_spec_id,
                partition_values,
            },
            split_offsets: df.split_offsets.map(|values| values.values),
            column_stats: df.column_stats.map(column_stats_from_native_report),
            referenced_data_file: df.referenced_data_file,
            first_row_id: df.first_row_id,
            equality_ids: df.equality_ids.map(|values| values.values),
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

pub(crate) fn iceberg_commit_infos_to_writer_reports<I>(
    infos: I,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String>
where
    I: IntoIterator<Item = novarocks::IcebergCommitInfo>,
{
    infos
        .into_iter()
        .map(|info| iceberg_commit_info_to_writer_report(info, metadata))
        .collect()
}

pub(crate) fn list_iceberg_writer_reports(
    finst_id: crate::common::types::UniqueId,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String> {
    iceberg_commit_infos_to_writer_reports(
        crate::runtime::sink_commit::list_iceberg_commits(finst_id),
        metadata,
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::delete_file::IcebergFileContent;
    use crate::connector::iceberg::report::{
        IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    };
    use iceberg::TableCreation;
    use iceberg::spec::{
        FormatVersion, Literal, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType,
        Schema, Struct, TableMetadata, TableMetadataBuilder, Transform, Type,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn test_unpartitioned_metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(PartitionSpec::unpartition_spec())
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn test_string_partition_metadata(spec_id: i32) -> TableMetadata {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .expect("schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema.as_ref().clone())
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn partitioned_writer_report(metadata: &TableMetadata, region: &str) -> IcebergWriterReport {
        IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/data-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: format!("region={region}"),
                    null_fingerprint: "0".to_string(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::from_iter([Some(Literal::Primitive(
                        PrimitiveLiteral::String(region.to_string()),
                    ))]),
                },
                split_offsets: None,
                column_stats: None,
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        }
    }

    #[test]
    fn partition_descriptor_from_thrift_rejects_missing_null_marker() {
        let desc = types::TIcebergPartitionDescriptor {
            values: Some(vec![types::TIcebergPartitionValue {
                is_null: None,
                datum_bytes: Some(b"west".to_vec()),
            }]),
        };

        let err =
            partition_descriptor_from_thrift(Some(desc)).expect_err("expected missing null marker");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("null marker"),
            "missing null marker should be rejected, got: {err}"
        );
    }

    #[test]
    fn writer_report_to_sink_commit_info_encodes_file_content() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/delete-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::PositionDeletes,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: Some(vec![4]),
                column_stats: None,
                referenced_data_file: Some("file:///warehouse/t/data-1.parquet".to_string()),
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        };

        let info = writer_report_to_sink_commit_info(report, &metadata).expect("wire encode");
        let df = info.iceberg_data_file.expect("iceberg data file");

        assert_eq!(
            df.file_content,
            Some(crate::thrift::types::TIcebergFileContent::POSITION_DELETES)
        );
        assert_eq!(
            df.referenced_data_file.as_deref(),
            Some("file:///warehouse/t/data-1.parquet")
        );
        assert_eq!(
            df.partition_values_descriptor
                .expect("descriptor")
                .values
                .expect("values")
                .len(),
            0
        );
    }

    #[test]
    fn writer_report_to_sink_commit_info_carries_partition_descriptor() {
        let metadata = test_string_partition_metadata(7);
        let info = writer_report_to_sink_commit_info(
            partitioned_writer_report(&metadata, "west"),
            &metadata,
        )
        .expect("wire encode");
        let df = info.iceberg_data_file.expect("iceberg data file");

        assert_eq!(
            df.partition_spec_id,
            Some(metadata.default_partition_spec_id())
        );
        let desc = df
            .partition_values_descriptor
            .expect("partition descriptor must be present");
        assert_eq!(desc.values.expect("values").len(), 1);
    }

    #[test]
    fn writer_report_to_sink_commit_info_descriptor_failure_uses_engine_code() {
        let metadata = test_string_partition_metadata(7);
        let mut report = partitioned_writer_report(&metadata, "west");
        report.file.partition.partition_values = Struct::from_iter([Some(Literal::int(7))]);

        let err = writer_report_to_sink_commit_info(report, &metadata)
            .expect_err("descriptor encode should fail");

        assert!(
            err.starts_with("[IcebergWriteDescriptorMismatch] "),
            "got: {err}"
        );
        assert!(err.contains("incompatible with type String"), "got: {err}");
    }

    #[test]
    fn writer_report_to_sink_commit_info_preserves_optional_file_metadata() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/data-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 10,
                file_size_in_bytes: 100,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: Some(vec![4]),
                column_stats: None,
                referenced_data_file: Some("file:///warehouse/t/base.parquet".to_string()),
                first_row_id: Some(42),
                equality_ids: Some(vec![1, 2]),
                key_metadata: Some(vec![1u8, 2, 3]),
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        };

        let df = writer_report_to_sink_commit_info(report, &metadata)
            .expect("wire encode")
            .iceberg_data_file
            .expect("iceberg data file");

        assert_eq!(df.split_offsets, Some(vec![4]));
        assert_eq!(
            df.referenced_data_file.as_deref(),
            Some("file:///warehouse/t/base.parquet")
        );
        assert_eq!(df.first_row_id, Some(42));
        assert_eq!(df.equality_ids, Some(vec![1, 2]));
        assert_eq!(df.key_metadata, Some(vec![1u8, 2, 3]));
    }

    #[test]
    fn writer_report_to_sink_commit_info_preserves_nan_value_counts() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/data-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: None,
                column_stats: Some(IcebergColumnStats {
                    nan_value_counts: BTreeMap::from([(1, 1)]),
                    ..Default::default()
                }),
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        };

        let stats = writer_report_to_sink_commit_info(report, &metadata)
            .expect("wire encode")
            .iceberg_data_file
            .expect("iceberg data file")
            .column_stats
            .expect("column stats");

        assert_eq!(
            stats.nan_value_counts.expect("nan value counts").get(&1),
            Some(&1)
        );
    }

    #[test]
    fn writer_report_to_sink_commit_info_preserves_puffin_dv_descriptor() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/dv-00000000.puffin".to_string(),
                format: "puffin".to_string(),
                content: IcebergFileContent::PositionDeletes,
                record_count: 3,
                file_size_in_bytes: 40,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: Some(vec![]),
                column_stats: None,
                referenced_data_file: Some("file:///warehouse/t/data-1.parquet".to_string()),
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: Some(4),
                content_size_in_bytes: Some(12),
                cardinality: Some(3),
            },
            is_overwrite: None,
            is_rewrite: None,
        };

        let df = writer_report_to_sink_commit_info(report, &metadata)
            .expect("wire encode")
            .iceberg_data_file
            .expect("iceberg data file");

        assert_eq!(df.format.as_deref(), Some("puffin"));
        assert_eq!(
            df.file_content,
            Some(types::TIcebergFileContent::POSITION_DELETES)
        );
        assert_eq!(df.content_offset, Some(4));
        assert_eq!(df.content_size_in_bytes, Some(12));
        assert_eq!(df.cardinality, Some(3));
    }

    #[test]
    fn native_sink_commit_roundtrip_preserves_iceberg_metadata() {
        let metadata = test_string_partition_metadata(7);
        let mut thrift = writer_report_to_sink_commit_info(
            partitioned_writer_report(&metadata, "west"),
            &metadata,
        )
        .expect("wire encode");
        let data_file = thrift
            .iceberg_data_file
            .as_mut()
            .expect("iceberg data file");
        data_file.split_offsets = Some(vec![4, 8]);
        data_file.column_stats = Some(types::TIcebergColumnStats {
            column_sizes: Some([(1, 100)].into_iter().collect()),
            value_counts: Some([(1, 2)].into_iter().collect()),
            null_value_counts: Some([(1, 0)].into_iter().collect()),
            nan_value_counts: Some([(1, 0)].into_iter().collect()),
            lower_bounds: Some([(1, b"a".to_vec())].into_iter().collect()),
            upper_bounds: Some([(1, b"z".to_vec())].into_iter().collect()),
        });
        data_file.referenced_data_file = Some("s3://warehouse/t/base.parquet".to_string());
        data_file.first_row_id = Some(17);
        data_file.equality_ids = Some(vec![1, 2]);
        data_file.key_metadata = Some(vec![0xaa, 0xbb]);
        data_file.content_offset = Some(5);
        data_file.content_size_in_bytes = Some(12);
        data_file.cardinality = Some(3);
        thrift.is_overwrite = Some(true);
        thrift.is_rewrite = Some(false);

        let native = sink_commit_info_to_native(thrift.clone()).expect("native encode");
        let decoded = sink_commit_info_from_native(native).expect("native decode");

        assert_eq!(decoded, thrift);
    }

    #[test]
    fn sink_commit_info_to_writer_report_uses_descriptor_not_partition_path() {
        let metadata = test_string_partition_metadata(7);
        let expected_values = Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::String("west".to_string()),
        ))]);
        let mut info = writer_report_to_sink_commit_info(
            partitioned_writer_report(&metadata, "west"),
            &metadata,
        )
        .expect("wire encode");
        info.iceberg_data_file
            .as_mut()
            .expect("iceberg data file")
            .partition_path = Some("region=east".to_string());

        let report = sink_commit_info_to_writer_report(info, &metadata).expect("wire decode");

        assert_eq!(report.file.partition.partition_values, expected_values);
        assert_eq!(report.file.partition.partition_path, "region=east");
    }

    #[test]
    fn sink_commit_info_to_writer_report_rejects_missing_data_file() {
        let metadata = test_unpartitioned_metadata();
        let err = sink_commit_info_to_writer_report(
            types::TSinkCommitInfo {
                iceberg_data_file: None,
                ..Default::default()
            },
            &metadata,
        )
        .expect_err("missing data file should fail");

        assert!(
            err.contains("sink_commit_info missing iceberg_data_file"),
            "got: {err}"
        );
    }

    #[test]
    fn sink_commit_info_to_writer_report_rejects_missing_partition_descriptor() {
        let metadata = test_string_partition_metadata(7);
        let mut info = writer_report_to_sink_commit_info(
            partitioned_writer_report(&metadata, "west"),
            &metadata,
        )
        .expect("wire encode");
        info.iceberg_data_file
            .as_mut()
            .expect("iceberg data file")
            .partition_values_descriptor = None;

        let err = sink_commit_info_to_writer_report(info, &metadata)
            .expect_err("missing descriptor should fail");

        assert!(
            err.starts_with("[IcebergWriteDescriptorMismatch] "),
            "got: {err}"
        );
        assert!(err.contains("missing partition descriptor"), "got: {err}");
    }

    #[test]
    fn sink_commit_info_to_writer_report_rejects_missing_partition_spec_id() {
        let metadata = test_string_partition_metadata(7);
        let mut info = writer_report_to_sink_commit_info(
            partitioned_writer_report(&metadata, "west"),
            &metadata,
        )
        .expect("wire encode");
        info.iceberg_data_file
            .as_mut()
            .expect("iceberg data file")
            .partition_spec_id = None;

        let err = sink_commit_info_to_writer_report(info, &metadata)
            .expect_err("missing partition spec id should fail");

        assert_eq!(
            err,
            "[IcebergWriteDescriptorMismatch] TIcebergDataFile missing partition_spec_id"
        );
    }

    #[test]
    fn sink_commit_info_to_writer_report_rejects_missing_required_data_file_fields() {
        let metadata = test_unpartitioned_metadata();
        let report = IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: "file:///warehouse/t/data-1.parquet".to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: None,
                column_stats: None,
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        };
        let info = writer_report_to_sink_commit_info(report, &metadata).expect("wire encode");

        let cases: [(&str, fn(&mut types::TIcebergDataFile)); 4] = [
            ("format", |df: &mut types::TIcebergDataFile| {
                df.format = None
            }),
            ("record_count", |df: &mut types::TIcebergDataFile| {
                df.record_count = None
            }),
            ("file_size_in_bytes", |df: &mut types::TIcebergDataFile| {
                df.file_size_in_bytes = None
            }),
            ("file_content", |df: &mut types::TIcebergDataFile| {
                df.file_content = None
            }),
        ];
        for (field, mutate) in cases {
            let mut missing = info.clone();
            mutate(missing.iceberg_data_file.as_mut().expect("data file"));

            let err = sink_commit_info_to_writer_report(missing, &metadata)
                .expect_err("missing required field should fail");

            assert!(
                err.contains(&format!("TIcebergDataFile missing {field}")),
                "field {field} got: {err}"
            );
        }
    }

    #[test]
    fn empty_column_stats_omit_thrift_stats() {
        assert!(column_stats_to_thrift(IcebergColumnStats::default()).is_none());
    }

    #[test]
    fn column_stats_to_thrift_filters_empty_maps() {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 10);

        let thrift = column_stats_to_thrift(stats).expect("thrift stats");

        assert_eq!(
            thrift.column_sizes.expect("column sizes").get(&1),
            Some(&10)
        );
        assert!(thrift.value_counts.is_none());
        assert!(thrift.null_value_counts.is_none());
        assert!(thrift.nan_value_counts.is_none());
        assert!(thrift.lower_bounds.is_none());
        assert!(thrift.upper_bounds.is_none());
    }

    #[test]
    fn column_stats_to_thrift_preserves_nan_value_counts() {
        let mut stats = IcebergColumnStats::default();
        stats.nan_value_counts.insert(1, 2);

        let thrift = column_stats_to_thrift(stats).expect("thrift stats");

        assert_eq!(
            thrift.nan_value_counts.expect("nan value counts").get(&1),
            Some(&2)
        );
        assert!(thrift.column_sizes.is_none());
        assert!(thrift.value_counts.is_none());
        assert!(thrift.null_value_counts.is_none());
        assert!(thrift.lower_bounds.is_none());
        assert!(thrift.upper_bounds.is_none());
    }

    #[test]
    fn file_content_to_thrift_maps_domain_content() {
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::Data),
            types::TIcebergFileContent::DATA
        );
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::PositionDeletes),
            types::TIcebergFileContent::POSITION_DELETES
        );
        assert_eq!(
            file_content_to_thrift(IcebergFileContent::EqualityDeletes),
            types::TIcebergFileContent::EQUALITY_DELETES
        );
    }
}
