use std::collections::BTreeMap;

use crate::thrift::{data_cache, frontend_service, runtime_profile, status, types};
use novarocks::common::types::UniqueId;
use novarocks::novarocks_logging::debug;
use novarocks::proto::novarocks as native_proto;
use novarocks::runtime::query_context::QueryId;
use novarocks::runtime::sink_commit;

pub(crate) struct ExecStatusReportInput {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) backend_num: i32,
    pub(crate) status: status::TStatus,
    pub(crate) done: bool,
    pub(crate) profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) tracking_url: Option<String>,
    pub(crate) load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
}

pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    let snapshot = sink_commit::report_snapshot(input.finst_id);
    let sink_commit_infos =
        thrift_sink_commit_infos_for_report(input.finst_id, &snapshot.iceberg_commits);
    let tablet_commit_infos = tablet_commit_infos_to_thrift(snapshot.tablet_commit_infos);
    let tablet_fail_infos = tablet_fail_infos_to_thrift(snapshot.tablet_fail_infos);
    let (normal_rows, loaded_bytes, filtered_rows) =
        load_stats_for_report(snapshot.load_stats, &snapshot.iceberg_commits);

    let load_counters = if normal_rows > 0 || loaded_bytes > 0 || filtered_rows > 0 {
        let mut counters = BTreeMap::new();
        counters.insert("dpp.norm.ALL".to_string(), normal_rows.to_string());
        counters.insert("dpp.abnorm.ALL".to_string(), filtered_rows.to_string());
        if loaded_bytes > 0 {
            counters.insert("loaded.bytes".to_string(), loaded_bytes.to_string());
        }
        Some(counters)
    } else {
        None
    };

    debug!(
        target: "novarocks::sink_commit",
        finst_id = %input.finst_id,
        backend_num = input.backend_num,
        query_id = %input.query_id,
        tablet_commit_info_len = tablet_commit_infos.len(),
        tablet_fail_info_len = tablet_fail_infos.len(),
        commit_info_len = sink_commit_infos.len(),
        done = input.done,
        "reportExecStatus sink/tablet commit infos"
    );

    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId {
            hi: input.query_id.hi(),
            lo: input.query_id.lo(),
        }),
        Some(input.backend_num),
        Some(types::TUniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        Some(input.status),
        Some(input.done),
        input.profile,
        None::<Vec<String>>,
        None::<Vec<String>>,
        load_counters,
        input.tracking_url,
        None::<Vec<String>>,
        (!tablet_commit_infos.is_empty()).then_some(tablet_commit_infos),
        (normal_rows > 0).then_some(normal_rows),
        None::<i64>,
        (loaded_bytes > 0).then_some(loaded_bytes),
        None::<i64>,
        None::<i64>,
        None::<crate::thrift::internal_service::TLoadJobType>,
        (!tablet_fail_infos.is_empty()).then_some(tablet_fail_infos),
        (filtered_rows > 0).then_some(filtered_rows),
        None::<i64>,
        None::<i64>,
        (!sink_commit_infos.is_empty()).then_some(sink_commit_infos),
        None::<String>,
        None,
        input.load_datacache_metrics,
    )
}

fn load_stats_for_report(
    stats: sink_commit::SinkLoadStats,
    iceberg_commits: &[native_proto::IcebergCommitInfo],
) -> (i64, i64, i64) {
    let mut normal_rows = stats.loaded_rows.max(0);
    let mut loaded_bytes = stats.loaded_bytes.max(0);
    let filtered_rows = stats.filtered_rows.max(0);
    for info in iceberg_commits {
        if let Some(file) = info.iceberg_data_file.as_ref() {
            if let Some(rows) = file.record_count {
                normal_rows = normal_rows.saturating_add(rows);
            }
            if let Some(bytes) = file.file_size_in_bytes {
                loaded_bytes = loaded_bytes.saturating_add(bytes);
            }
        }
    }
    (normal_rows, loaded_bytes, filtered_rows)
}

fn tablet_commit_infos_to_thrift(
    infos: Vec<sink_commit::TabletCommitInfo>,
) -> Vec<types::TTabletCommitInfo> {
    infos
        .into_iter()
        .map(|info| {
            types::TTabletCommitInfo::new(info.tablet_id, info.backend_id, None, None, None)
        })
        .collect()
}

fn tablet_fail_infos_to_thrift(
    infos: Vec<sink_commit::TabletFailInfo>,
) -> Vec<types::TTabletFailInfo> {
    infos
        .into_iter()
        .map(|info| types::TTabletFailInfo::new(Some(info.tablet_id), Some(info.backend_id)))
        .collect()
}

fn thrift_sink_commit_infos_for_report(
    finst_id: UniqueId,
    infos: &[native_proto::IcebergCommitInfo],
) -> Vec<types::TSinkCommitInfo> {
    infos.iter().cloned().filter_map(|info| match iceberg_commit_info_to_thrift(info) {
        Ok(info) => Some(info),
        Err(error) => {
            debug!(target: "novarocks::sink_commit", finst_id = %finst_id, error = %error, "skip invalid native iceberg commit in thrift report");
            None
        }
    }).collect()
}

fn iceberg_commit_info_to_thrift(
    info: native_proto::IcebergCommitInfo,
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
    data_file: native_proto::IcebergDataFile,
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

fn column_stats_to_thrift(stats: native_proto::IcebergColumnStats) -> types::TIcebergColumnStats {
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
    descriptor: native_proto::IcebergPartitionDescriptor,
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
    match native_proto::IcebergFileContent::try_from(value) {
        Ok(native_proto::IcebergFileContent::Data) => Ok(types::TIcebergFileContent::DATA),
        Ok(native_proto::IcebergFileContent::PositionDeletes) => {
            Ok(types::TIcebergFileContent::POSITION_DELETES)
        }
        Ok(native_proto::IcebergFileContent::EqualityDeletes) => {
            Ok(types::TIcebergFileContent::EQUALITY_DELETES)
        }
        Ok(native_proto::IcebergFileContent::Unspecified) => {
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
