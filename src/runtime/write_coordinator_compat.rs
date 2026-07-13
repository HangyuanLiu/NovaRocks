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

//! StarRocks-compatible thrift report adapters for distributed write reports.

#![cfg(feature = "compat")]

use crate::common::engine_error::EngineError;
use crate::common::types::UniqueId;
use crate::coordinator::write::report::FragmentExecStatusReport;
use crate::coordinator::write::{self as write_coordinator, ReportOutcome};
use crate::proto::common;
use crate::runtime::sink_commit_wire;
use crate::thrift::frontend_service;

pub(crate) fn report_from_thrift(
    params: frontend_service::TReportExecStatusParams,
) -> Result<FragmentExecStatusReport, String> {
    let query_id = unique_id_from_thrift(
        params
            .query_id
            .ok_or_else(|| "TReportExecStatusParams missing query_id".to_string())?,
    );
    let fragment_instance_id = unique_id_from_thrift(
        params
            .fragment_instance_id
            .ok_or_else(|| "TReportExecStatusParams missing fragment_instance_id".to_string())?,
    );
    let backend_num = params
        .backend_num
        .ok_or_else(|| "TReportExecStatusParams missing backend_num".to_string())?;
    let status = params
        .status
        .ok_or_else(|| "TReportExecStatusParams missing status".to_string())?;
    let done = params
        .done
        .ok_or_else(|| "TReportExecStatusParams missing done".to_string())?;
    let iceberg_commits = params
        .sink_commit_infos
        .unwrap_or_default()
        .into_iter()
        .map(sink_commit_wire::sink_commit_info_to_native)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FragmentExecStatusReport {
        query_id,
        fragment_instance_id,
        backend_num,
        done,
        status: status_from_thrift(status),
        iceberg_commits,
        load_counters: params.load_counters.unwrap_or_default(),
        loaded_rows: params.loaded_rows.unwrap_or_default(),
        loaded_bytes: params.sink_load_bytes.unwrap_or_default(),
        filtered_rows: params.filtered_rows.unwrap_or_default(),
    })
}

pub(crate) fn handle_report_exec_status(
    params: frontend_service::TReportExecStatusParams,
) -> Result<ReportOutcome, EngineError> {
    let report = report_from_thrift(params).map_err(EngineError::protocol_decode)?;
    write_coordinator::handle_fragment_report_exec_status(report)
}

fn unique_id_from_thrift(id: crate::thrift::types::TUniqueId) -> UniqueId {
    UniqueId {
        hi: id.hi,
        lo: id.lo,
    }
}

fn unique_id_to_thrift(id: UniqueId) -> crate::thrift::types::TUniqueId {
    crate::thrift::types::TUniqueId::new(id.hi, id.lo)
}

fn status_from_thrift(status: crate::thrift::status::TStatus) -> common::Status {
    common::Status {
        code: status.status_code.0,
        message: status
            .error_msgs
            .as_ref()
            .map(|msgs| msgs.join("; "))
            .unwrap_or_default(),
    }
}

fn status_to_thrift(status: common::Status) -> crate::thrift::status::TStatus {
    let error_msgs = if status.message.is_empty() {
        None
    } else {
        Some(vec![status.message])
    };
    crate::thrift::status::TStatus::new(
        crate::thrift::status_code::TStatusCode(status.code),
        error_msgs,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::coordinator::write::report::WriterKey;
    use crate::thrift::{status, status_code, types};

    fn id(hi: i64, lo: i64) -> UniqueId {
        UniqueId { hi, lo }
    }

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    fn thrift_params(
        report: FragmentExecStatusReport,
    ) -> frontend_service::TReportExecStatusParams {
        let sink_commit_infos = report
            .iceberg_commits
            .into_iter()
            .map(sink_commit_wire::sink_commit_info_from_native)
            .collect::<Result<Vec<_>, _>>()
            .expect("native commit to thrift");
        frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            Some(unique_id_to_thrift(report.query_id)),
            Some(report.backend_num),
            Some(unique_id_to_thrift(report.fragment_instance_id)),
            Some(status_to_thrift(report.status)),
            Some(report.done),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            Some(report.load_counters),
            None,
            Option::<Vec<String>>::None,
            None,
            Some(report.loaded_rows),
            None,
            Some(report.loaded_bytes),
            None,
            None,
            None,
            None,
            Some(report.filtered_rows),
            None,
            None,
            Some(sink_commit_infos),
            None,
            None,
            None,
        )
    }

    fn report(writer: &WriterKey, path: &str) -> FragmentExecStatusReport {
        FragmentExecStatusReport {
            query_id: writer.query_id.clone(),
            fragment_instance_id: writer.fragment_instance_id.clone(),
            backend_num: writer.backend_num,
            done: true,
            status: status_from_thrift(ok_status()),
            iceberg_commits: vec![crate::proto::novarocks::IcebergCommitInfo {
                iceberg_data_file: Some(crate::proto::novarocks::IcebergDataFile {
                    path: Some(path.to_string()),
                    record_count: Some(7),
                    file_size_in_bytes: Some(70),
                    file_content: crate::proto::novarocks::IcebergFileContent::Data as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            load_counters: BTreeMap::new(),
            loaded_rows: 7,
            loaded_bytes: 70,
            filtered_rows: 0,
        }
    }

    #[test]
    fn thrift_report_requires_identity_and_status() {
        let params = frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            None,
            Some(0),
            None,
            Some(ok_status()),
            Some(true),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            None,
            None,
            Option::<Vec<String>>::None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let err = report_from_thrift(params).expect_err("missing ids must fail");
        assert!(err.contains("query_id"), "{err}");
    }

    #[test]
    fn thrift_report_maps_commit_and_load_fields() {
        let query_id = id(21, 31);
        let finst_id = id(123, 223);
        let sink_commit = types::TSinkCommitInfo {
            iceberg_data_file: Some(types::TIcebergDataFile {
                path: Some("s3://w/from-thrift.parquet".to_string()),
                record_count: Some(123),
                file_size_in_bytes: Some(456),
                ..Default::default()
            }),
            ..Default::default()
        };
        let tablet_commit =
            types::TTabletCommitInfo::new(1001, 2002, None, Some(vec!["c1".to_string()]), None);
        let tablet_fail = types::TTabletFailInfo::new(Some(3003), Some(4004));
        let load_counters = BTreeMap::from([
            ("dpp.norm.ALL".to_string(), "123".to_string()),
            ("loaded.bytes".to_string(), "456".to_string()),
        ]);
        let params = frontend_service::TReportExecStatusParams::new(
            frontend_service::FrontendServiceVersion::V1,
            Some(unique_id_to_thrift(query_id)),
            Some(7),
            Some(unique_id_to_thrift(finst_id)),
            Some(ok_status()),
            Some(true),
            None,
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            Some(load_counters.clone()),
            None,
            Option::<Vec<String>>::None,
            Some(vec![tablet_commit.clone()]),
            Some(123),
            None,
            Some(456),
            None,
            None,
            None,
            Some(vec![tablet_fail.clone()]),
            Some(5),
            None,
            None,
            Some(vec![sink_commit.clone()]),
            None,
            None,
            None,
        );

        let report = report_from_thrift(params).expect("thrift report");
        assert_eq!(report.query_id, query_id);
        assert_eq!(report.fragment_instance_id, finst_id);
        assert_eq!(report.backend_num, 7);
        assert!(report.done);
        assert_eq!(report.iceberg_commits.len(), 1);
        assert_eq!(
            report.iceberg_commits[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|file| file.path.as_deref()),
            Some("s3://w/from-thrift.parquet")
        );
        assert_eq!(report.load_counters, load_counters);
        assert_eq!(report.loaded_rows, 123);
        assert_eq!(report.loaded_bytes, 456);
        assert_eq!(report.filtered_rows, 5);
    }

    #[test]
    fn thrift_report_handler_reuses_shared_write_coordinator() {
        let mut guard = write_coordinator::write_registry_test_guard();
        let query_id = id(31, 41);
        let writer = WriterKey {
            query_id: query_id.clone(),
            fragment_instance_id: id(131, 231),
            backend_num: 0,
        };
        let coord = guard
            .register_query(query_id.clone(), vec![writer.clone()])
            .expect("register write coordinator");

        assert_eq!(
            handle_report_exec_status(thrift_params(report(&writer, "s3://w/compat.parquet")))
                .expect("handle thrift report"),
            ReportOutcome::CommitReady
        );
        let commit = coord
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect("commit input");
        assert_eq!(commit.write_id, query_id);
        assert_eq!(
            commit.writers[0].iceberg_commits[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|f| f.path.as_deref()),
            Some("s3://w/compat.parquet")
        );
    }
}
