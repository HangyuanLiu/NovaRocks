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
use crate::connector::iceberg::report::{IcebergColumnStats, IcebergWriterReport};
use crate::connector::iceberg::write_descriptor::encode_partition_descriptor;
use crate::runtime::runtime_state::RuntimeState;
use crate::thrift::types;

pub(crate) fn emit_iceberg_writer_report(
    state: &RuntimeState,
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<(), String> {
    let commit_info = writer_report_to_sink_commit_info(report, metadata)?;
    state.add_sink_commit_info(commit_info);
    Ok(())
}

fn writer_report_to_sink_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<types::TSinkCommitInfo, String> {
    let partition_values_descriptor = encode_partition_descriptor(
        &report.file.partition.partition_values,
        report.file.partition.partition_spec_id,
        metadata,
    )
    .map_err(|e| EngineError::from(e).to_bracketed_user_message())?;
    Ok(types::TSinkCommitInfo {
        iceberg_data_file: Some(types::TIcebergDataFile {
            path: Some(report.file.path),
            format: Some(report.file.format),
            record_count: Some(report.file.record_count),
            file_size_in_bytes: Some(report.file.file_size_in_bytes),
            partition_path: Some(report.file.partition.partition_path),
            split_offsets: report.file.split_offsets,
            column_stats: report.file.column_stats.and_then(column_stats_to_thrift),
            partition_null_fingerprint: Some(report.file.partition.null_fingerprint),
            file_content: Some(file_content_to_thrift(report.file.content)),
            referenced_data_file: report.file.referenced_data_file,
            first_row_id: report.file.first_row_id,
            equality_ids: report.file.equality_ids,
            key_metadata: report.file.key_metadata,
            partition_values_descriptor: Some(partition_values_descriptor),
            partition_spec_id: Some(report.file.partition.partition_spec_id),
            content_offset: report.file.content_offset,
            content_size_in_bytes: report.file.content_size_in_bytes,
            cardinality: report.file.cardinality,
        }),
        hive_file_info: None,
        is_overwrite: report.is_overwrite,
        staging_dir: None,
        is_rewrite: report.is_rewrite,
    })
}

fn column_stats_to_thrift(stats: IcebergColumnStats) -> Option<types::TIcebergColumnStats> {
    if stats.is_empty() {
        return None;
    }
    Some(types::TIcebergColumnStats {
        column_sizes: non_empty(stats.column_sizes),
        value_counts: non_empty(stats.value_counts),
        null_value_counts: non_empty(stats.null_value_counts),
        nan_value_counts: None,
        lower_bounds: non_empty(stats.lower_bounds),
        upper_bounds: non_empty(stats.upper_bounds),
    })
}

fn non_empty<K: Ord, V>(map: BTreeMap<K, V>) -> Option<BTreeMap<K, V>> {
    (!map.is_empty()).then_some(map)
}

fn file_content_to_thrift(content: IcebergFileContent) -> types::TIcebergFileContent {
    match content {
        IcebergFileContent::Data => types::TIcebergFileContent::DATA,
        IcebergFileContent::PositionDeletes => types::TIcebergFileContent::POSITION_DELETES,
        IcebergFileContent::EqualityDeletes => types::TIcebergFileContent::EQUALITY_DELETES,
    }
}

#[cfg(test)]
pub(crate) fn expected_file_content_for_test(
    content: IcebergFileContent,
) -> types::TIcebergFileContent {
    file_content_to_thrift(content)
}

#[cfg(test)]
mod tests {
    use super::*;

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
