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

//! Provider-owned canonical payloads for Iceberg write handles and reports.

use std::collections::BTreeMap;

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use novarocks_spi::connector::{ConnectorCommittedVersion, ConnectorWriteReceipt};

use crate::commit::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
};
use crate::delete_file::IcebergFileContent;
use crate::iceberg::spec::TableMetadata;
use crate::write_descriptor::{IcebergPartitionDescriptor, IcebergPartitionValueDescriptor};
use crate::write_descriptor::{decode_partition_descriptor, encode_partition_descriptor};

pub const ICEBERG_WRITE_PAYLOAD_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IcebergFileContentV1 {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

impl From<IcebergFileContent> for IcebergFileContentV1 {
    fn from(value: IcebergFileContent) -> Self {
        match value {
            IcebergFileContent::Data => Self::Data,
            IcebergFileContent::PositionDeletes => Self::PositionDeletes,
            IcebergFileContent::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

impl From<IcebergFileContentV1> for IcebergFileContent {
    fn from(value: IcebergFileContentV1) -> Self {
        match value {
            IcebergFileContentV1::Data => Self::Data,
            IcebergFileContentV1::PositionDeletes => Self::PositionDeletes,
            IcebergFileContentV1::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Bytes, String> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| format!("encode canonical Iceberg write payload failed: {error}"))
}
fn decode_json<T: for<'de> Deserialize<'de>>(payload: &[u8], subject: &str) -> Result<T, String> {
    serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg {subject} payload failed: {error}"))
}
fn ensure_canonical_json<T: Serialize>(
    payload: &[u8],
    value: &T,
    subject: &str,
) -> Result<(), String> {
    if canonical_json(value)?.as_ref() != payload {
        return Err(format!(
            "Iceberg {subject} payload is not canonical JSON v1"
        ));
    }
    Ok(())
}
fn base64_encode(value: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}
fn base64_decode(value: &str, subject: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| format!("decode Iceberg {subject} base64 failed: {error}"))
}
fn validate_secret_free_text(subject: &str, value: &str) -> Result<(), String> {
    if value.contains('\0') {
        return Err(format!("Iceberg {subject} contains a NUL byte"));
    }
    if let Ok(url) = url::Url::parse(value) {
        if !url.username().is_empty() || url.password().is_some() {
            return Err(format!("Iceberg {subject} must not embed credentials"));
        }
        for (key, _) in url.query_pairs() {
            if matches!(
                key.to_ascii_lowercase().as_str(),
                "access_key"
                    | "access_key_id"
                    | "secret"
                    | "secret_key"
                    | "session_token"
                    | "token"
            ) {
                return Err(format!("Iceberg {subject} must not embed credentials"));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergStagedReportsPayloadV1 {
    version: u32,
    reports: Vec<IcebergWriterReportV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriterReportV1 {
    file: IcebergWrittenFileReportV1,
    is_overwrite: Option<bool>,
    is_rewrite: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWrittenFileReportV1 {
    path: String,
    format: String,
    content: IcebergFileContentV1,
    record_count: i64,
    file_size_in_bytes: i64,
    partition: IcebergPartitionReportV1,
    split_offsets: Option<Vec<i64>>,
    column_stats: Option<IcebergColumnStatsV1>,
    referenced_data_file: Option<String>,
    first_row_id: Option<i64>,
    equality_ids: Option<Vec<i32>>,
    key_metadata_base64: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    cardinality: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPartitionReportV1 {
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    values: Vec<IcebergPartitionValueV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPartitionValueV1 {
    is_null: bool,
    datum_base64: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergColumnStatsV1 {
    column_sizes: BTreeMap<i32, i64>,
    value_counts: BTreeMap<i32, i64>,
    null_value_counts: BTreeMap<i32, i64>,
    nan_value_counts: BTreeMap<i32, i64>,
    lower_bounds_base64: BTreeMap<i32, String>,
    upper_bounds_base64: BTreeMap<i32, String>,
}

impl IcebergWriterReportV1 {
    fn from_report(report: &IcebergWriterReport, metadata: &TableMetadata) -> Result<Self, String> {
        let file = &report.file;
        validate_secret_free_text("staged file path", &file.path)?;
        if let Some(path) = &file.referenced_data_file {
            validate_secret_free_text("referenced data file", path)?;
        }
        let descriptor = encode_partition_descriptor(
            &file.partition.partition_values,
            file.partition.partition_spec_id,
            metadata,
        )
        .map_err(|error| format!("encode Iceberg partition descriptor failed: {error}"))?;
        Ok(Self {
            file: IcebergWrittenFileReportV1 {
                path: file.path.clone(),
                format: file.format.clone(),
                content: file.content.into(),
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                partition: IcebergPartitionReportV1 {
                    partition_path: file.partition.partition_path.clone(),
                    null_fingerprint: file.partition.null_fingerprint.clone(),
                    partition_spec_id: file.partition.partition_spec_id,
                    values: descriptor
                        .values
                        .into_iter()
                        .map(|value| IcebergPartitionValueV1 {
                            is_null: value.is_null,
                            datum_base64: value.datum_bytes.map(base64_encode),
                        })
                        .collect(),
                },
                split_offsets: file.split_offsets.clone(),
                column_stats: file
                    .column_stats
                    .as_ref()
                    .map(IcebergColumnStatsV1::from_stats),
                referenced_data_file: file.referenced_data_file.clone(),
                first_row_id: file.first_row_id,
                equality_ids: file.equality_ids.clone(),
                key_metadata_base64: file.key_metadata.as_ref().map(base64_encode),
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                cardinality: file.cardinality,
            },
            is_overwrite: report.is_overwrite,
            is_rewrite: report.is_rewrite,
        })
    }

    fn into_report(self, metadata: &TableMetadata) -> Result<IcebergWriterReport, String> {
        validate_secret_free_text("staged file path", &self.file.path)?;
        if let Some(path) = &self.file.referenced_data_file {
            validate_secret_free_text("referenced data file", path)?;
        }
        let values = self
            .file
            .partition
            .values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let datum_bytes = match (value.is_null, value.datum_base64) {
                    (true, None) => None,
                    (true, Some(_)) => {
                        return Err(format!(
                            "Iceberg partition descriptor value {index} is null but carries a payload"
                        ));
                    }
                    (false, Some(value)) => Some(base64_decode(&value, "partition datum")?),
                    (false, None) => {
                        return Err(format!(
                            "Iceberg partition descriptor value {index} is non-null but has no payload"
                        ));
                    }
                };
                Ok(IcebergPartitionValueDescriptor {
                    is_null: value.is_null,
                    datum_bytes,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let partition_spec_id = self.file.partition.partition_spec_id;
        let partition_values = decode_partition_descriptor(
            Some(IcebergPartitionDescriptor { values }),
            partition_spec_id,
            metadata,
        )
        .map_err(|error| format!("decode Iceberg partition descriptor failed: {error}"))?;
        Ok(IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: self.file.path,
                format: self.file.format,
                content: self.file.content.into(),
                record_count: self.file.record_count,
                file_size_in_bytes: self.file.file_size_in_bytes,
                partition: IcebergPartitionReport {
                    partition_path: self.file.partition.partition_path,
                    null_fingerprint: self.file.partition.null_fingerprint,
                    partition_spec_id,
                    partition_values,
                },
                split_offsets: self.file.split_offsets,
                column_stats: self
                    .file
                    .column_stats
                    .map(IcebergColumnStatsV1::into_stats)
                    .transpose()?,
                referenced_data_file: self.file.referenced_data_file,
                first_row_id: self.file.first_row_id,
                equality_ids: self.file.equality_ids,
                key_metadata: self
                    .file
                    .key_metadata_base64
                    .as_deref()
                    .map(|value| base64_decode(value, "key metadata"))
                    .transpose()?,
                content_offset: self.file.content_offset,
                content_size_in_bytes: self.file.content_size_in_bytes,
                cardinality: self.file.cardinality,
            },
            is_overwrite: self.is_overwrite,
            is_rewrite: self.is_rewrite,
        })
    }
}

impl IcebergColumnStatsV1 {
    fn from_stats(stats: &IcebergColumnStats) -> Self {
        Self {
            column_sizes: stats.column_sizes.clone(),
            value_counts: stats.value_counts.clone(),
            null_value_counts: stats.null_value_counts.clone(),
            nan_value_counts: stats.nan_value_counts.clone(),
            lower_bounds_base64: stats
                .lower_bounds
                .iter()
                .map(|(field_id, value)| (*field_id, base64_encode(value)))
                .collect(),
            upper_bounds_base64: stats
                .upper_bounds
                .iter()
                .map(|(field_id, value)| (*field_id, base64_encode(value)))
                .collect(),
        }
    }

    fn into_stats(self) -> Result<IcebergColumnStats, String> {
        Ok(IcebergColumnStats {
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            nan_value_counts: self.nan_value_counts,
            lower_bounds: self
                .lower_bounds_base64
                .into_iter()
                .map(|(field_id, value)| {
                    base64_decode(&value, "lower bound").map(|value| (field_id, value))
                })
                .collect::<Result<_, _>>()?,
            upper_bounds: self
                .upper_bounds_base64
                .into_iter()
                .map(|(field_id, value)| {
                    base64_decode(&value, "upper bound").map(|value| (field_id, value))
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Encode one logical writer's Iceberg file facts. Multiple files are kept in
/// one logical payload, which the write session carries as the receipt's
/// provider-private evidence.
pub fn encode_writer_reports(
    reports: &[IcebergWriterReport],
    metadata: &TableMetadata,
) -> Result<Bytes, String> {
    let payload = IcebergStagedReportsPayloadV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        reports: reports
            .iter()
            .map(|report| IcebergWriterReportV1::from_report(report, metadata))
            .collect::<Result<_, _>>()?,
    };
    canonical_json(&payload)
}

pub fn decode_writer_reports(
    payload: &[u8],
    metadata: &TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String> {
    let decoded: IcebergStagedReportsPayloadV1 = decode_json(payload, "staged report")?;
    if decoded.version != ICEBERG_WRITE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported Iceberg staged report payload version {}; expected {}",
            decoded.version, ICEBERG_WRITE_PAYLOAD_VERSION
        ));
    }
    ensure_canonical_json(payload, &decoded, "staged report")?;
    decoded
        .reports
        .into_iter()
        .map(|report| report.into_report(metadata))
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergWriteReceiptV1 {
    version: u32,
    snapshot_id: i64,
}

pub fn encode_write_receipt(snapshot_id: i64) -> Result<Bytes, String> {
    canonical_json(&IcebergWriteReceiptV1 {
        version: ICEBERG_WRITE_PAYLOAD_VERSION,
        snapshot_id,
    })
}

pub fn connector_write_receipt_with_partitioning(
    snapshot_id: i64,
    resulting_row_count: Option<u64>,
    committed_partitioning: Option<novarocks_spi::connector::ConnectorCommittedPartitioning>,
) -> Result<ConnectorWriteReceipt, String> {
    let payload = encode_write_receipt(snapshot_id)?;
    let committed_version = ConnectorCommittedVersion::try_new(payload.clone(), Some(snapshot_id))
        .map_err(|error| format!("build Iceberg connector committed version failed: {error}"))?;
    match committed_partitioning {
        Some(partitioning) => ConnectorWriteReceipt::try_new_with_committed_facts_and_partitioning(
            payload,
            committed_version,
            resulting_row_count,
            partitioning,
        ),
        None => ConnectorWriteReceipt::try_new_with_committed_facts(
            payload,
            committed_version,
            resulting_row_count,
        ),
    }
    .map_err(|error| format!("build Iceberg connector write receipt failed: {error}"))
}
