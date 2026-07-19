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

use crate::proto::novarocks;
use crate::runtime::sink_commit::{TabletCommitInfo, TabletFailInfo};
use crate::thrift::types;

pub(crate) fn tablet_commit_info_to_thrift(info: TabletCommitInfo) -> types::TTabletCommitInfo {
    types::TTabletCommitInfo::new(
        info.tablet_id,
        info.backend_id,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        Option::<Vec<i64>>::None,
    )
}

pub(crate) fn tablet_commit_infos_to_thrift(
    infos: impl IntoIterator<Item = TabletCommitInfo>,
) -> Vec<types::TTabletCommitInfo> {
    infos
        .into_iter()
        .map(tablet_commit_info_to_thrift)
        .collect()
}

pub(crate) fn tablet_fail_info_to_thrift(info: TabletFailInfo) -> types::TTabletFailInfo {
    types::TTabletFailInfo::new(Some(info.tablet_id), Some(info.backend_id))
}

pub(crate) fn tablet_fail_infos_to_thrift(
    infos: impl IntoIterator<Item = TabletFailInfo>,
) -> Vec<types::TTabletFailInfo> {
    infos.into_iter().map(tablet_fail_info_to_thrift).collect()
}

pub(crate) fn iceberg_commit_info_to_thrift(
    info: novarocks::IcebergCommitInfo,
) -> Result<types::TSinkCommitInfo, String> {
    let data_file = info
        .iceberg_data_file
        .ok_or_else(|| "IcebergCommitInfo missing iceberg_data_file".to_string())?;
    Ok(types::TSinkCommitInfo {
        iceberg_data_file: Some(iceberg_data_file_to_thrift(data_file)?),
        hive_file_info: None,
        is_overwrite: info.is_overwrite,
        staging_dir: None,
        is_rewrite: info.is_rewrite,
    })
}

fn iceberg_data_file_to_thrift(
    data_file: novarocks::IcebergDataFile,
) -> Result<types::TIcebergDataFile, String> {
    Ok(types::TIcebergDataFile {
        path: data_file.path,
        format: data_file.format,
        record_count: data_file.record_count,
        file_size_in_bytes: data_file.file_size_in_bytes,
        partition_path: data_file.partition_path,
        split_offsets: data_file.split_offsets.map(|values| values.values),
        column_stats: data_file.column_stats.map(column_stats_to_thrift),
        partition_null_fingerprint: data_file.partition_null_fingerprint,
        file_content: Some(file_content_to_thrift(data_file.file_content)?),
        referenced_data_file: data_file.referenced_data_file,
        first_row_id: data_file.first_row_id,
        equality_ids: data_file.equality_ids.map(|values| values.values),
        key_metadata: data_file.key_metadata,
        partition_spec_id: data_file.partition_spec_id,
        partition_values_descriptor: data_file
            .partition_values_descriptor
            .map(partition_descriptor_to_thrift),
        content_offset: data_file.content_offset,
        content_size_in_bytes: data_file.content_size_in_bytes,
        cardinality: data_file.cardinality,
    })
}

fn column_stats_to_thrift(stats: novarocks::IcebergColumnStats) -> types::TIcebergColumnStats {
    types::TIcebergColumnStats {
        column_sizes: non_empty(stats.column_sizes.into_iter().collect()),
        value_counts: non_empty(stats.value_counts.into_iter().collect()),
        null_value_counts: non_empty(stats.null_value_counts.into_iter().collect()),
        nan_value_counts: non_empty(stats.nan_value_counts.into_iter().collect()),
        lower_bounds: non_empty(stats.lower_bounds.into_iter().collect()),
        upper_bounds: non_empty(stats.upper_bounds.into_iter().collect()),
    }
}

fn partition_descriptor_to_thrift(
    descriptor: novarocks::IcebergPartitionDescriptor,
) -> types::TIcebergPartitionDescriptor {
    types::TIcebergPartitionDescriptor {
        values: Some(
            descriptor
                .values
                .into_iter()
                .map(|value| types::TIcebergPartitionValue {
                    is_null: value.is_null,
                    datum_bytes: value.datum_bytes,
                })
                .collect(),
        ),
    }
}

fn file_content_to_thrift(value: i32) -> Result<types::TIcebergFileContent, String> {
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

fn non_empty<K: Ord, V>(map: BTreeMap<K, V>) -> Option<BTreeMap<K, V>> {
    (!map.is_empty()).then_some(map)
}

#[cfg(test)]
mod tests {
    use crate::proto::novarocks::{IcebergCommitInfo, IcebergDataFile, IcebergFileContent};
    use crate::runtime::sink_commit::{TabletCommitInfo, TabletFailInfo};

    #[test]
    fn tablet_domain_records_encode_exact_thrift_fields() {
        let commit = super::tablet_commit_info_to_thrift(TabletCommitInfo {
            tablet_id: 101,
            backend_id: 202,
        });
        assert_eq!(commit.tablet_id, 101);
        assert_eq!(commit.backend_id, 202);
        assert_eq!(commit.invalid_dict_cache_columns, None);
        assert_eq!(commit.valid_dict_cache_columns, None);
        assert_eq!(commit.valid_dict_collected_versions, None);

        let fail = super::tablet_fail_info_to_thrift(TabletFailInfo {
            tablet_id: 303,
            backend_id: 404,
        });
        assert_eq!(fail.tablet_id, Some(303));
        assert_eq!(fail.backend_id, Some(404));
    }

    #[test]
    fn native_iceberg_commit_encodes_thrift_sink_commit_info() {
        let encoded = super::iceberg_commit_info_to_thrift(IcebergCommitInfo {
            iceberg_data_file: Some(IcebergDataFile {
                path: Some("s3://warehouse/data.parquet".to_string()),
                format: Some("parquet".to_string()),
                record_count: Some(11),
                file_size_in_bytes: Some(101),
                file_content: IcebergFileContent::Data as i32,
                partition_spec_id: Some(3),
                ..Default::default()
            }),
            is_overwrite: Some(true),
            is_rewrite: Some(false),
        })
        .expect("native commit to Thrift");

        let file = encoded.iceberg_data_file.expect("data file");
        assert_eq!(file.path.as_deref(), Some("s3://warehouse/data.parquet"));
        assert_eq!(file.record_count, Some(11));
        assert_eq!(
            file.file_content,
            Some(crate::thrift::types::TIcebergFileContent::DATA)
        );
        assert_eq!(file.partition_spec_id, Some(3));
        assert_eq!(encoded.is_overwrite, Some(true));
        assert_eq!(encoded.is_rewrite, Some(false));
    }
}
