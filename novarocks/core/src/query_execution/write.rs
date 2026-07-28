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
use crate::query_execution::artifact::WriterRegistrationSet;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::runtime::profile::RuntimeProfileTree;

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
pub struct WriteCommitInput {
    pub(crate) write_id: UniqueId,
    pub(crate) writers: Vec<WriterCommitInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterCommitInput {
    pub(crate) writer_id: usize,
    pub(crate) writer_key: WriterKey,
    pub(crate) iceberg_commits: Vec<novarocks::IcebergCommitInfo>,
    pub(crate) load_counters: BTreeMap<String, String>,
    pub(crate) loaded_rows: i64,
    pub(crate) loaded_bytes: i64,
    pub(crate) filtered_rows: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteAbortInput {
    pub(crate) write_id: UniqueId,
    pub(crate) reason: String,
    pub(crate) completed_writer_outputs: Vec<WriterCommitInput>,
    pub(crate) incomplete_writers: Vec<WriterKey>,
}

impl WriteAbortInput {
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Neutral frontend-facing report value decoded by core from native wire data.
/// Native protobuf fields and runtime cleanup capabilities are not exposed.
pub struct NativeExecutionReport {
    write: FragmentExecStatusReport,
    profile: Option<RuntimeProfileTree>,
}

impl NativeExecutionReport {
    pub fn query_id(&self) -> crate::query_execution::contract::QueryId {
        crate::query_execution::contract::QueryId::new(
            self.write.query_id.hi,
            self.write.query_id.lo,
        )
    }

    pub fn fragment_instance_id(&self) -> UniqueId {
        self.write.fragment_instance_id
    }

    pub fn backend_num(&self) -> i32 {
        self.write.backend_num
    }

    pub fn is_final(&self) -> bool {
        self.write.done
    }

    pub fn is_failed(&self) -> bool {
        self.write.status.code != 0
    }

    pub fn has_profile(&self) -> bool {
        self.profile.is_some()
    }

    pub fn has_write_metadata(&self) -> bool {
        !self.write.iceberg_commits.is_empty()
    }

    pub fn same_write_report(&self, other: &Self) -> bool {
        self.write == other.write
    }

    pub fn failure_message(&self) -> Option<&str> {
        self.is_failed()
            .then_some(if self.write.status.message.is_empty() {
                "native fragment execution failed"
            } else {
                self.write.status.message.as_str()
            })
    }

    pub(crate) fn into_parts(self) -> (FragmentExecStatusReport, Option<RuntimeProfileTree>) {
        (self.write, self.profile)
    }

    #[cfg(feature = "query-execution-contract-test-support")]
    pub(crate) fn for_contract_test(
        query_id: UniqueId,
        fragment_instance_id: UniqueId,
        backend_num: i32,
        status: common::Status,
        profile: Option<RuntimeProfileTree>,
    ) -> Self {
        Self {
            write: FragmentExecStatusReport {
                query_id,
                fragment_instance_id,
                backend_num,
                done: true,
                status,
                iceberg_commits: Vec::new(),
                load_counters: BTreeMap::new(),
                loaded_rows: 1,
                loaded_bytes: 8,
                filtered_rows: 0,
            },
            profile,
        }
    }

    #[cfg(feature = "query-execution-contract-test-support")]
    pub(crate) fn for_contract_test_with_write_metadata(
        query_id: UniqueId,
        fragment_instance_id: UniqueId,
        backend_num: i32,
        done: bool,
    ) -> Self {
        let mut report = Self::for_contract_test(
            query_id,
            fragment_instance_id,
            backend_num,
            common::Status {
                code: 0,
                message: String::new(),
            },
            None,
        );
        report
            .write
            .iceberg_commits
            .push(novarocks::IcebergCommitInfo::default());
        report.write.done = done;
        report
    }
}

pub struct WriteReportOutcome {
    commit: Option<WriteCommitInput>,
    abort: Option<WriteAbortInput>,
}

impl WriteReportOutcome {
    pub fn abort_reason(&self) -> Option<&str> {
        self.abort.as_ref().map(|abort| abort.reason.as_str())
    }

    pub fn into_payloads(self) -> (Option<WriteCommitInput>, Option<WriteAbortInput>) {
        (self.commit, self.abort)
    }
}

/// Pure consuming builder from neutral native reports to the intent-safe write
/// completion payload.
pub struct WriteReportBuilder {
    write_id: UniqueId,
    expected: BTreeMap<WriterKey, usize>,
    completed: BTreeMap<WriterKey, WriterCommitInput>,
    failure: Option<String>,
}

impl WriteReportBuilder {
    pub fn new(registrations: WriterRegistrationSet) -> Result<Self, DistributedQueryError> {
        let registrations = registrations.into_registrations();
        let write_id = registrations
            .first()
            .map(|registration| registration.query_id)
            .ok_or_else(|| contract_violation("write execution has no writer registrations"))?;
        let mut expected = BTreeMap::new();
        for (writer_id, registration) in registrations.into_iter().enumerate() {
            let key = WriterKey {
                query_id: registration.query_id,
                fragment_instance_id: registration.fragment_instance_id,
                backend_num: registration.backend_num,
            };
            if key.query_id != write_id {
                return Err(contract_violation(
                    "writer registrations contain multiple query ids",
                ));
            }
            if expected.insert(key, writer_id).is_some() {
                return Err(contract_violation(
                    "writer registrations contain duplicate writer identities",
                ));
            }
        }
        Ok(Self {
            write_id,
            expected,
            completed: BTreeMap::new(),
            failure: None,
        })
    }

    pub fn apply(&mut self, report: NativeExecutionReport) -> Result<(), DistributedQueryError> {
        let (report, _) = report.into_parts();
        let key = WriterKey {
            query_id: report.query_id,
            fragment_instance_id: report.fragment_instance_id,
            backend_num: report.backend_num,
        };
        let Some(writer_id) = self.expected.get(&key).copied() else {
            self.failure.get_or_insert_with(|| {
                format!(
                    "native report references an unregistered writer {}/{}",
                    key.fragment_instance_id.hi, key.fragment_instance_id.lo
                )
            });
            return Ok(());
        };
        if !report.done {
            self.failure
                .get_or_insert_with(|| "write report builder requires final native reports".into());
            return Ok(());
        }
        if report.status.code != 0 {
            self.failure.get_or_insert_with(|| {
                if report.status.message.is_empty() {
                    format!("native writer failed with status {}", report.status.code)
                } else {
                    report.status.message
                }
            });
            return Ok(());
        }
        let output = WriterCommitInput {
            writer_id,
            writer_key: key.clone(),
            iceberg_commits: report.iceberg_commits,
            load_counters: report.load_counters,
            loaded_rows: report.loaded_rows,
            loaded_bytes: report.loaded_bytes,
            filtered_rows: report.filtered_rows,
        };
        if let Some(existing) = self.completed.get(&key) {
            if existing == &output {
                return Ok(());
            }
            self.failure.get_or_insert_with(|| {
                "write report builder received conflicting final writer output".into()
            });
            return Ok(());
        }
        self.completed.insert(key, output);
        Ok(())
    }

    pub fn latch_failure(&mut self, message: impl Into<String>) {
        self.failure.get_or_insert_with(|| message.into());
    }

    pub fn finish(self) -> Result<WriteReportOutcome, DistributedQueryError> {
        let incomplete_writers = self
            .expected
            .keys()
            .filter(|key| !self.completed.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        let completed_writer_outputs = self.completed.into_values().collect::<Vec<_>>();
        if let Some(reason) = self.failure {
            return Ok(WriteReportOutcome {
                commit: None,
                abort: Some(WriteAbortInput {
                    write_id: self.write_id,
                    reason,
                    completed_writer_outputs,
                    incomplete_writers,
                }),
            });
        }
        if !incomplete_writers.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Failed,
                "write execution ended before all writer reports arrived",
            ));
        }
        Ok(WriteReportOutcome {
            commit: Some(WriteCommitInput {
                write_id: self.write_id,
                writers: completed_writer_outputs,
            }),
            abort: None,
        })
    }
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
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

/// Decode the native report wire DTO into the frontend-owned coordinator value.
///
/// The protobuf remains a transport concern while role crates receive the
/// capability-safe value used by the distributed-query contract.
pub fn decode_native_execution_report(
    report: novarocks::ExecStatusReport,
) -> Result<NativeExecutionReport, String> {
    let profile = report
        .profile
        .as_ref()
        .map(RuntimeProfileTree::from_proto)
        .transpose()?;
    Ok(NativeExecutionReport {
        write: report_from_native(report)?,
        profile,
    })
}
