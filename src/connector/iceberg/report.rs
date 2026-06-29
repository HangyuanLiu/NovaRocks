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

use std::collections::{BTreeMap, HashMap};

use base64::Engine;
use iceberg::spec::{Literal, PartitionSpecRef, PrimitiveLiteral, Struct};

use crate::connector::iceberg::delete_file::IcebergFileContent;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IcebergColumnStats {
    pub(crate) column_sizes: BTreeMap<i32, i64>,
    pub(crate) value_counts: BTreeMap<i32, i64>,
    pub(crate) null_value_counts: BTreeMap<i32, i64>,
    pub(crate) lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub(crate) upper_bounds: BTreeMap<i32, Vec<u8>>,
}

impl IcebergColumnStats {
    pub(crate) fn is_empty(&self) -> bool {
        self.column_sizes.is_empty()
            && self.value_counts.is_empty()
            && self.null_value_counts.is_empty()
            && self.lower_bounds.is_empty()
            && self.upper_bounds.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergPartitionReport {
    pub(crate) partition_path: String,
    pub(crate) null_fingerprint: String,
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: Struct,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergWrittenFileReport {
    pub(crate) path: String,
    pub(crate) format: String,
    pub(crate) content: IcebergFileContent,
    pub(crate) record_count: i64,
    pub(crate) file_size_in_bytes: i64,
    pub(crate) partition: IcebergPartitionReport,
    pub(crate) split_offsets: Option<Vec<i64>>,
    pub(crate) column_stats: Option<IcebergColumnStats>,
    pub(crate) referenced_data_file: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) equality_ids: Option<Vec<i32>>,
    pub(crate) key_metadata: Option<Vec<u8>>,
    pub(crate) content_offset: Option<i64>,
    pub(crate) content_size_in_bytes: Option<i64>,
    pub(crate) cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergWriterReport {
    pub(crate) file: IcebergWrittenFileReport,
    pub(crate) is_overwrite: Option<bool>,
    pub(crate) is_rewrite: Option<bool>,
}

pub(crate) fn writer_report_from_written_file(
    file: &crate::connector::iceberg::commit::WrittenFile,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<IcebergWriterReport, String> {
    let partition_spec = metadata
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| {
            format!(
                "iceberg written file `{}` references unknown partition spec id {}",
                file.path, file.partition_spec_id
            )
        })?;
    let (partition_path, null_fingerprint) =
        partition_path_from_struct(&file.partition_values, &partition_spec)?;
    let content = match file.content {
        iceberg::spec::DataContentType::Data => IcebergFileContent::Data,
        iceberg::spec::DataContentType::PositionDeletes => IcebergFileContent::PositionDeletes,
        iceberg::spec::DataContentType::EqualityDeletes => IcebergFileContent::EqualityDeletes,
    };
    Ok(IcebergWriterReport {
        file: IcebergWrittenFileReport {
            path: file.path.clone(),
            format: file.format.to_string(),
            content,
            record_count: u64_to_i64(file.record_count, "record_count")?,
            file_size_in_bytes: u64_to_i64(file.file_size_in_bytes, "file_size_in_bytes")?,
            partition: IcebergPartitionReport {
                partition_path,
                null_fingerprint,
                partition_spec_id: file.partition_spec_id,
                partition_values: file.partition_values.clone(),
            },
            split_offsets: (!file.split_offsets.is_empty()).then_some(file.split_offsets.clone()),
            column_stats: column_stats_from_written_file(file)?,
            referenced_data_file: file.referenced_data_file.clone(),
            first_row_id: file.first_row_id,
            equality_ids: file.equality_ids.clone(),
            key_metadata: file.key_metadata.clone(),
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
            cardinality: file
                .cardinality
                .map(|value| u64_to_i64(value, "cardinality"))
                .transpose()?,
        },
        is_overwrite: None,
        is_rewrite: None,
    })
}

fn column_stats_from_written_file(
    file: &crate::connector::iceberg::commit::WrittenFile,
) -> Result<Option<IcebergColumnStats>, String> {
    let stats = IcebergColumnStats {
        column_sizes: u64_stats_to_i64(&file.column_sizes, "column_sizes")?,
        value_counts: u64_stats_to_i64(&file.value_counts, "value_counts")?,
        null_value_counts: u64_stats_to_i64(&file.null_value_counts, "null_value_counts")?,
        lower_bounds: datum_bounds_to_bytes(&file.lower_bounds, "lower_bounds")?,
        upper_bounds: datum_bounds_to_bytes(&file.upper_bounds, "upper_bounds")?,
    };
    Ok((!stats.is_empty()).then_some(stats))
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("iceberg {label} {value} overflows i64"))
}

fn u64_stats_to_i64(stats: &HashMap<i32, u64>, label: &str) -> Result<BTreeMap<i32, i64>, String> {
    stats
        .iter()
        .map(|(field_id, value)| {
            u64_to_i64(*value, &format!("{label}[{field_id}]")).map(|value| (*field_id, value))
        })
        .collect()
}

fn datum_bounds_to_bytes(
    bounds: &HashMap<i32, iceberg::spec::Datum>,
    label: &str,
) -> Result<BTreeMap<i32, Vec<u8>>, String> {
    bounds
        .iter()
        .map(|(field_id, datum)| {
            datum
                .to_bytes()
                .map(|bytes| (*field_id, bytes.to_vec()))
                .map_err(|e| {
                    format!("convert iceberg datum bound {label}[{field_id}] to bytes failed: {e}")
                })
        })
        .collect()
}

pub(crate) fn partition_path_from_struct(
    values: &Struct,
    partition_spec: &PartitionSpecRef,
) -> Result<(String, String), String> {
    if values.fields().len() != partition_spec.fields().len() {
        return Err(format!(
            "partition value count {} does not match partition spec field count {}",
            values.fields().len(),
            partition_spec.fields().len()
        ));
    }
    let mut path = String::new();
    let mut null_fingerprint = String::with_capacity(values.fields().len());
    for (value, field) in values.fields().iter().zip(partition_spec.fields().iter()) {
        null_fingerprint.push(if value.is_none() { '1' } else { '0' });
        path.push_str(&field.name);
        path.push('=');
        match value {
            Some(value) => path.push_str(&partition_literal_to_path_value(value)?),
            None => path.push_str("null"),
        }
        path.push('/');
    }
    Ok((path.trim_matches('/').to_string(), null_fingerprint))
}

fn partition_literal_to_path_value(value: &Literal) -> Result<String, String> {
    let Literal::Primitive(value) = value else {
        return Err("iceberg partition path only supports primitive literals".to_string());
    };
    Ok(match value {
        PrimitiveLiteral::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        PrimitiveLiteral::Int(value) => value.to_string(),
        PrimitiveLiteral::Long(value) => value.to_string(),
        PrimitiveLiteral::Float(value) => value.0.to_string(),
        PrimitiveLiteral::Double(value) => value.0.to_string(),
        PrimitiveLiteral::String(value) => url_encode_partition_value(value),
        PrimitiveLiteral::Binary(value) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(value);
            url_encode_partition_value(&encoded)
        }
        PrimitiveLiteral::Int128(value) => value.to_string(),
        PrimitiveLiteral::UInt128(value) => value.to_string(),
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return Err("iceberg partition path cannot encode sentinel bounds".to_string());
        }
    })
}

fn url_encode_partition_value(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_column_stats_reports_empty() {
        assert!(IcebergColumnStats::default().is_empty());
    }

    #[test]
    fn column_size_makes_stats_non_empty() {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 42);

        assert!(!stats.is_empty());
    }
}
