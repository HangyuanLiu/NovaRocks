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

//! Native distributed write report model.

use std::collections::BTreeMap;

use crate::common::types::UniqueId;
use crate::proto::{common, novarocks};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct WriterKey {
    pub(crate) query_id: UniqueId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FragmentExecStatusReport {
    pub(crate) query_id: UniqueId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
    pub(crate) done: bool,
    pub(crate) status: common::Status,
    pub(crate) iceberg_commits: Vec<novarocks::IcebergCommitInfo>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WriteCommitInput {
    pub(crate) write_id: UniqueId,
    pub(crate) writers: Vec<WriterCommitInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WriterCommitInput {
    pub(crate) writer_id: usize,
    pub(crate) writer_key: WriterKey,
    pub(crate) iceberg_commits: Vec<novarocks::IcebergCommitInfo>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WriteAbortInput {
    pub(crate) write_id: UniqueId,
    pub(crate) reason: String,
    pub(crate) completed_writer_outputs: Vec<WriterCommitInput>,
    pub(crate) incomplete_writers: Vec<WriterKey>,
}

pub(crate) fn unique_id_from_native(
    id: Option<common::UniqueId>,
    missing_message: &'static str,
) -> Result<UniqueId, String> {
    let id = id.ok_or_else(|| missing_message.to_string())?;
    Ok(UniqueId {
        hi: id.hi,
        lo: id.lo,
    })
}

pub(crate) fn report_from_native(
    report: novarocks::ExecStatusReport,
) -> Result<FragmentExecStatusReport, String> {
    Ok(FragmentExecStatusReport {
        query_id: unique_id_from_native(report.query_id, "ExecStatusReport missing query_id")?,
        fragment_instance_id: unique_id_from_native(
            report.fragment_instance_id,
            "ExecStatusReport missing fragment_instance_id",
        )?,
        backend_num: report.backend_num,
        done: report.done,
        status: report.status.unwrap_or(common::Status {
            code: 1,
            message: "ExecStatusReport missing status".to_string(),
        }),
        iceberg_commits: report.iceberg_commits,
        load_counters: BTreeMap::new(),
        loaded_rows: report.loaded_rows,
        loaded_bytes: report.sink_load_bytes,
        filtered_rows: report.filtered_rows,
    })
}
