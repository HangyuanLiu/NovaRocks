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
use std::sync::{Mutex, OnceLock};

use crate::common::engine_error::EngineError;
use crate::common::types::UniqueId;
use crate::connector::iceberg::delete_file::IcebergFileContent;
use crate::connector::iceberg::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
};
use crate::connector::iceberg::stats_assembler::FileSketchSet;
use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
    encode_partition_descriptor,
};
use crate::proto::novarocks::IcebergCommitInfo;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SinkLoadStats {
    pub loaded_rows: i64,
    pub loaded_bytes: i64,
    pub filtered_rows: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletCommitInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletFailInfo {
    pub tablet_id: i64,
    pub backend_id: i64,
}

/// Protocol-neutral final-report facts collected by fragment sinks.
///
/// The runtime owns this data; protocol adapters are responsible for encoding
/// it into their respective report wire formats.
#[derive(Clone, Debug, Default)]
pub struct SinkCommitReportSnapshot {
    pub iceberg_commits: Vec<IcebergCommitInfo>,
    pub tablet_commit_infos: Vec<TabletCommitInfo>,
    pub tablet_fail_infos: Vec<TabletFailInfo>,
    pub load_stats: SinkLoadStats,
}

struct SinkCommitStore {
    mu: Mutex<HashMap<UniqueId, SinkCommitEntry>>,
}

#[derive(Default)]
struct SinkCommitEntry {
    iceberg_commits: Vec<IcebergCommitInfo>,
    tablet_commit_infos: Vec<TabletCommitInfo>,
    tablet_fail_infos: Vec<TabletFailInfo>,
    /// Per-file Theta sketch sets produced by the Iceberg sink for Puffin
    /// NDV statistics. These are not Cloneable (the `ThetaSketchHandle`
    /// holds an underlying `ThetaSketch` that does not implement `Clone`),
    /// so callers consume them via `take_sketch_sets` — a destructive
    /// drain — rather than `list_sketch_sets`. The pattern mirrors
    /// `IcebergCommitCollector::take_sketch_sets`.
    sketch_sets: Vec<FileSketchSet>,
    loaded_rows: i64,
    loaded_bytes: i64,
    filtered_rows: i64,
}

static STORE: OnceLock<SinkCommitStore> = OnceLock::new();

fn store() -> &'static SinkCommitStore {
    STORE.get_or_init(|| SinkCommitStore {
        mu: Mutex::new(HashMap::new()),
    })
}

pub(crate) fn register(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.entry(finst_id).or_default();
}

pub(crate) fn try_register(finst_id: UniqueId) -> bool {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    if guard.contains_key(&finst_id) {
        return false;
    }
    guard.insert(finst_id, SinkCommitEntry::default());
    true
}

pub fn unregister(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.remove(&finst_id);
}

pub(crate) fn is_registered(finst_id: UniqueId) -> bool {
    store()
        .mu
        .lock()
        .expect("sink commit store lock")
        .contains_key(&finst_id)
}

pub(crate) fn add_iceberg_commit(finst_id: UniqueId, info: IcebergCommitInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard
        .entry(finst_id)
        .or_default()
        .iceberg_commits
        .push(info);
}

pub(crate) fn list_iceberg_commits(finst_id: UniqueId) -> Vec<IcebergCommitInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.iceberg_commits.clone())
        .unwrap_or_default()
}

/// Push a per-file Theta sketch set produced by the Iceberg sink. Used by
/// the pipeline-driven sink path; the standalone iceberg_writer path uses
/// [`IcebergCommitCollector::inject_sketch_set`] directly.
pub(crate) fn add_sketch_set(finst_id: UniqueId, set: FileSketchSet) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.entry(finst_id).or_default().sketch_sets.push(set);
}

/// Destructively drain the per-file sketch sets registered via
/// [`add_sketch_set`]. The sketches cannot be cloned (the underlying
/// `ThetaSketch` from the `datasketches` crate does not implement Clone),
/// so each finst_id can be drained exactly once.
pub(crate) fn take_sketch_sets(finst_id: UniqueId) -> Vec<FileSketchSet> {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get_mut(&finst_id)
        .map(|entry| std::mem::take(&mut entry.sketch_sets))
        .unwrap_or_default()
}

pub(crate) fn add_tablet_commit_info(finst_id: UniqueId, info: TabletCommitInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_commit_infos.contains(&info);
    if !already_exists {
        entry.tablet_commit_infos.push(info);
    }
}

pub(crate) fn list_tablet_commit_infos(finst_id: UniqueId) -> Vec<TabletCommitInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_commit_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_tablet_fail_info(finst_id: UniqueId, info: TabletFailInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_fail_infos.contains(&info);
    if !already_exists {
        entry.tablet_fail_infos.push(info);
    }
}

pub(crate) fn list_tablet_fail_infos(finst_id: UniqueId) -> Vec<TabletFailInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_fail_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_load_stats(
    finst_id: UniqueId,
    loaded_rows: i64,
    loaded_bytes: i64,
    filtered_rows: i64,
) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    entry.loaded_rows = entry.loaded_rows.saturating_add(loaded_rows.max(0));
    entry.loaded_bytes = entry.loaded_bytes.saturating_add(loaded_bytes.max(0));
    entry.filtered_rows = entry.filtered_rows.saturating_add(filtered_rows.max(0));
}

pub(crate) fn get_load_counters(finst_id: UniqueId) -> (i64, i64) {
    let stats = get_load_stats(finst_id);
    (stats.loaded_rows, stats.loaded_bytes)
}

pub(crate) fn get_load_stats(finst_id: UniqueId) -> SinkLoadStats {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| SinkLoadStats {
            loaded_rows: entry.loaded_rows,
            loaded_bytes: entry.loaded_bytes,
            filtered_rows: entry.filtered_rows,
        })
        .unwrap_or_default()
}

pub fn report_snapshot(finst_id: UniqueId) -> SinkCommitReportSnapshot {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    let Some(entry) = guard.get(&finst_id) else {
        return SinkCommitReportSnapshot::default();
    };
    SinkCommitReportSnapshot {
        iceberg_commits: entry.iceberg_commits.clone(),
        tablet_commit_infos: entry.tablet_commit_infos.clone(),
        tablet_fail_infos: entry.tablet_fail_infos.clone(),
        load_stats: SinkLoadStats {
            loaded_rows: entry.loaded_rows,
            loaded_bytes: entry.loaded_bytes,
            filtered_rows: entry.filtered_rows,
        },
    }
}

fn partition_descriptor_to_native(
    desc: IcebergPartitionDescriptor,
) -> crate::proto::novarocks::IcebergPartitionDescriptor {
    crate::proto::novarocks::IcebergPartitionDescriptor {
        values: desc
            .values
            .into_iter()
            .map(|value| crate::proto::novarocks::IcebergPartitionValue {
                is_null: Some(value.is_null),
                datum_bytes: value.datum_bytes,
            })
            .collect(),
    }
}

fn partition_descriptor_from_native(
    desc: Option<crate::proto::novarocks::IcebergPartitionDescriptor>,
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

fn column_stats_to_native(
    stats: IcebergColumnStats,
) -> crate::proto::novarocks::IcebergColumnStats {
    crate::proto::novarocks::IcebergColumnStats {
        column_sizes: stats.column_sizes.into_iter().collect(),
        value_counts: stats.value_counts.into_iter().collect(),
        null_value_counts: stats.null_value_counts.into_iter().collect(),
        nan_value_counts: stats.nan_value_counts.into_iter().collect(),
        lower_bounds: stats.lower_bounds.into_iter().collect(),
        upper_bounds: stats.upper_bounds.into_iter().collect(),
    }
}

fn column_stats_from_native(
    stats: crate::proto::novarocks::IcebergColumnStats,
) -> IcebergColumnStats {
    IcebergColumnStats {
        column_sizes: stats.column_sizes.into_iter().collect(),
        value_counts: stats.value_counts.into_iter().collect(),
        null_value_counts: stats.null_value_counts.into_iter().collect(),
        nan_value_counts: stats.nan_value_counts.into_iter().collect(),
        lower_bounds: stats.lower_bounds.into_iter().collect(),
        upper_bounds: stats.upper_bounds.into_iter().collect(),
    }
}

fn file_content_to_native(
    content: IcebergFileContent,
) -> crate::proto::novarocks::IcebergFileContent {
    match content {
        IcebergFileContent::Data => crate::proto::novarocks::IcebergFileContent::Data,
        IcebergFileContent::PositionDeletes => {
            crate::proto::novarocks::IcebergFileContent::PositionDeletes
        }
        IcebergFileContent::EqualityDeletes => {
            crate::proto::novarocks::IcebergFileContent::EqualityDeletes
        }
    }
}

fn file_content_from_native(value: i32) -> Result<IcebergFileContent, String> {
    match crate::proto::novarocks::IcebergFileContent::try_from(value) {
        Ok(crate::proto::novarocks::IcebergFileContent::Data) => Ok(IcebergFileContent::Data),
        Ok(crate::proto::novarocks::IcebergFileContent::PositionDeletes) => {
            Ok(IcebergFileContent::PositionDeletes)
        }
        Ok(crate::proto::novarocks::IcebergFileContent::EqualityDeletes) => {
            Ok(IcebergFileContent::EqualityDeletes)
        }
        Ok(crate::proto::novarocks::IcebergFileContent::Unspecified) => {
            Err("IcebergDataFile missing file_content".to_string())
        }
        Err(_) => Err(format!(
            "unknown IcebergFileContent value {value} in native iceberg commit info"
        )),
    }
}

pub(crate) fn writer_report_to_iceberg_commit_info(
    report: IcebergWriterReport,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<IcebergCommitInfo, String> {
    let partition_values_descriptor = partition_descriptor_to_native(
        encode_partition_descriptor(
            &report.file.partition.partition_values,
            report.file.partition.partition_spec_id,
            metadata,
        )
        .map_err(|e| EngineError::from(e).to_bracketed_user_message())?,
    );
    Ok(IcebergCommitInfo {
        iceberg_data_file: Some(crate::proto::novarocks::IcebergDataFile {
            path: Some(report.file.path),
            format: Some(report.file.format),
            record_count: Some(report.file.record_count),
            file_size_in_bytes: Some(report.file.file_size_in_bytes),
            partition_path: Some(report.file.partition.partition_path),
            split_offsets: report
                .file
                .split_offsets
                .map(|values| crate::proto::novarocks::Int64List { values }),
            column_stats: report.file.column_stats.map(column_stats_to_native),
            partition_null_fingerprint: Some(report.file.partition.null_fingerprint),
            file_content: file_content_to_native(report.file.content) as i32,
            referenced_data_file: report.file.referenced_data_file,
            first_row_id: report.file.first_row_id,
            equality_ids: report
                .file
                .equality_ids
                .map(|values| crate::proto::novarocks::Int32List { values }),
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

pub(crate) fn iceberg_commit_info_to_writer_report(
    info: IcebergCommitInfo,
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
    let partition_descriptor = partition_descriptor_from_native(df.partition_values_descriptor)
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
            content: file_content_from_native(df.file_content)?,
            record_count,
            file_size_in_bytes,
            partition: IcebergPartitionReport {
                partition_path: df.partition_path.unwrap_or_default(),
                null_fingerprint: df.partition_null_fingerprint.unwrap_or_default(),
                partition_spec_id,
                partition_values,
            },
            split_offsets: df.split_offsets.map(|values| values.values),
            column_stats: df.column_stats.map(column_stats_from_native),
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

pub(crate) fn iceberg_commit_infos_to_writer_reports<I>(
    infos: I,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String>
where
    I: IntoIterator<Item = IcebergCommitInfo>,
{
    infos
        .into_iter()
        .map(|info| iceberg_commit_info_to_writer_report(info, metadata))
        .collect()
}

pub(crate) fn list_iceberg_writer_reports(
    finst_id: UniqueId,
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<IcebergWriterReport>, String> {
    iceberg_commit_infos_to_writer_reports(list_iceberg_commits(finst_id), metadata)
}

#[cfg(all(test, feature = "compat"))]
mod tests {
    use super::{
        TabletCommitInfo, TabletFailInfo, add_tablet_commit_info, add_tablet_fail_info,
        list_tablet_commit_infos, list_tablet_fail_infos, unregister,
    };
    use crate::common::types::UniqueId;

    #[test]
    fn tablet_domain_records_deduplicate_by_tablet_and_backend() {
        let finst_id = UniqueId { hi: 41, lo: 42 };
        unregister(finst_id);

        let commit = TabletCommitInfo {
            tablet_id: 101,
            backend_id: 202,
        };
        add_tablet_commit_info(finst_id, commit);
        add_tablet_commit_info(finst_id, commit);
        add_tablet_commit_info(
            finst_id,
            TabletCommitInfo {
                tablet_id: 101,
                backend_id: 303,
            },
        );

        let fail = TabletFailInfo {
            tablet_id: 404,
            backend_id: 505,
        };
        add_tablet_fail_info(finst_id, fail);
        add_tablet_fail_info(finst_id, fail);

        assert_eq!(
            list_tablet_commit_infos(finst_id),
            vec![
                commit,
                TabletCommitInfo {
                    tablet_id: 101,
                    backend_id: 303,
                },
            ]
        );
        assert_eq!(list_tablet_fail_infos(finst_id), vec![fail]);

        unregister(finst_id);
    }
}
