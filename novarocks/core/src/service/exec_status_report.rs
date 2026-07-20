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

use crate::common::types::UniqueId;
use crate::novarocks_logging::debug;
use crate::proto::{common, novarocks};
use crate::runtime::query_context::QueryId;
use crate::runtime::sink_commit;
use crate::service::starrocks_sink_commit_wire;
use crate::thrift::{data_cache, frontend_service, runtime_profile, status, types};

pub(crate) struct ExecStatusReportInput {
    pub(crate) finst_id: UniqueId,
    pub(crate) query_id: QueryId,
    pub(crate) backend_num: i32,
    pub(crate) status: status::TStatus,
    pub(crate) done: bool,
    pub(crate) profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) tracking_url: Option<String>,
    pub(crate) load_channel_profile: Option<runtime_profile::TRuntimeProfileTree>,
    pub(crate) load_datacache_metrics: Option<data_cache::TLoadDataCacheMetrics>,
    pub(crate) native_profile: Option<novarocks::RuntimeProfileTree>,
}

pub(crate) fn build_report_params(
    input: ExecStatusReportInput,
) -> frontend_service::TReportExecStatusParams {
    let iceberg_commits = sink_commit::list_iceberg_commits(input.finst_id);
    let sink_commit_infos = thrift_sink_commit_infos_for_report(input.finst_id, &iceberg_commits);
    let tablet_commit_infos = starrocks_sink_commit_wire::tablet_commit_infos_to_thrift(
        sink_commit::list_tablet_commit_infos(input.finst_id),
    );
    let tablet_fail_infos = starrocks_sink_commit_wire::tablet_fail_infos_to_thrift(
        sink_commit::list_tablet_fail_infos(input.finst_id),
    );
    let (normal_rows, loaded_bytes, filtered_rows) =
        load_stats_for_report(input.finst_id, &iceberg_commits);

    // FE derives loaded rows from these LoadEtlTask-recognized counters.
    // Missing or mismatched keys make FE see loadedRows=0.
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

    let tablet_commit_infos = if tablet_commit_infos.is_empty() {
        None
    } else {
        Some(tablet_commit_infos)
    };
    let sink_commit_infos = if sink_commit_infos.is_empty() {
        None
    } else {
        Some(sink_commit_infos)
    };
    let tablet_fail_infos = if tablet_fail_infos.is_empty() {
        None
    } else {
        Some(tablet_fail_infos)
    };

    frontend_service::TReportExecStatusParams::new(
        frontend_service::FrontendServiceVersion::V1,
        Some(types::TUniqueId {
            hi: input.query_id.hi,
            lo: input.query_id.lo,
        }),
        Some(input.backend_num),
        Some(types::TUniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        Some(input.status),
        Some(input.done),
        input.profile,
        Option::<Vec<String>>::None,
        Option::<Vec<String>>::None,
        load_counters,
        input.tracking_url,
        Option::<Vec<String>>::None,
        tablet_commit_infos,
        (normal_rows > 0).then_some(normal_rows),
        Option::<i64>::None,
        (loaded_bytes > 0).then_some(loaded_bytes),
        Option::<i64>::None,
        Option::<i64>::None,
        Option::<crate::thrift::internal_service::TLoadJobType>::None,
        tablet_fail_infos,
        (filtered_rows > 0).then_some(filtered_rows),
        Option::<i64>::None,
        Option::<i64>::None,
        sink_commit_infos,
        Option::<String>::None,
        input.load_channel_profile,
        input.load_datacache_metrics,
    )
}

pub(crate) fn build_native_report(
    input: ExecStatusReportInput,
) -> Result<novarocks::ExecStatusReport, String> {
    let iceberg_commits = sink_commit::list_iceberg_commits(input.finst_id);
    let (loaded_rows, sink_load_bytes, filtered_rows) =
        load_stats_for_report(input.finst_id, &iceberg_commits);

    Ok(novarocks::ExecStatusReport {
        query_id: Some(common::UniqueId {
            hi: input.query_id.hi,
            lo: input.query_id.lo,
        }),
        fragment_instance_id: Some(common::UniqueId {
            hi: input.finst_id.hi,
            lo: input.finst_id.lo,
        }),
        backend_num: input.backend_num,
        status: Some(common::Status {
            code: input.status.status_code.0,
            message: input
                .status
                .error_msgs
                .as_ref()
                .map(|msgs| msgs.join("; "))
                .unwrap_or_default(),
        }),
        done: input.done,
        iceberg_commits,
        loaded_rows,
        sink_load_bytes,
        filtered_rows,
        profile: input.native_profile,
    })
}

fn load_stats_for_report(
    finst_id: UniqueId,
    iceberg_commits: &[novarocks::IcebergCommitInfo],
) -> (i64, i64, i64) {
    let state_stats = sink_commit::get_load_stats(finst_id);
    let mut normal_rows: i64 = state_stats.loaded_rows.max(0);
    let mut loaded_bytes: i64 = state_stats.loaded_bytes.max(0);
    let filtered_rows: i64 = state_stats.filtered_rows.max(0);

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

fn thrift_sink_commit_infos_for_report(
    finst_id: UniqueId,
    iceberg_commits: &[novarocks::IcebergCommitInfo],
) -> Vec<types::TSinkCommitInfo> {
    iceberg_commits
        .iter()
        .filter_map(|info| {
            match starrocks_sink_commit_wire::iceberg_commit_info_to_thrift(info.clone()) {
                Ok(info) => Some(info),
                Err(err) => {
                    debug!(
                        target: "novarocks::sink_commit",
                        finst_id = %finst_id,
                        error = %err,
                        "skip invalid native iceberg commit in thrift report"
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{ExecStatusReportInput, build_report_params};
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::sink_commit::{self, TabletCommitInfo, TabletFailInfo};
    use crate::thrift::status::TStatus;
    use crate::thrift::status_code::TStatusCode;

    #[test]
    fn thrift_report_caller_encodes_domain_tablet_results_at_service_boundary() {
        let finst_id = UniqueId {
            hi: 9_101,
            lo: 9_102,
        };
        sink_commit::register(finst_id);
        sink_commit::add_tablet_commit_info(
            finst_id,
            TabletCommitInfo {
                tablet_id: 101,
                backend_id: 201,
            },
        );
        sink_commit::add_tablet_fail_info(
            finst_id,
            TabletFailInfo {
                tablet_id: 102,
                backend_id: 202,
            },
        );

        let report = build_report_params(ExecStatusReportInput {
            finst_id,
            query_id: QueryId {
                hi: 9_001,
                lo: 9_002,
            },
            backend_num: 7,
            status: TStatus::new(TStatusCode::OK, None),
            done: true,
            profile: None,
            tracking_url: None,
            load_channel_profile: None,
            load_datacache_metrics: None,
            native_profile: None,
        });
        sink_commit::unregister(finst_id);

        let commits = report.commit_infos.expect("tablet commits");
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].tablet_id, 101);
        assert_eq!(commits[0].backend_id, 201);
        let failures = report.fail_infos.expect("tablet failures");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].tablet_id, Some(102));
        assert_eq!(failures[0].backend_id, Some(202));
    }
}
