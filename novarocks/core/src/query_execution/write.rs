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
use crate::proto::novarocks;
use crate::query_execution::artifact::WriterRegistrationSet;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle::{FragmentTerminalOutcome, FragmentTerminalSnapshot};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct WriterKey {
    pub(crate) query_id: UniqueId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteCommitInput {
    pub(crate) write_id: UniqueId,
    pub(crate) writers: Vec<WriterCommitInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterCommitInput {
    pub(crate) writer_id: usize,
    pub(crate) fragment_id: u32,
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
pub struct WriteTerminalBuilder {
    write_id: UniqueId,
    expected: BTreeMap<WriterKey, (usize, u32)>,
    completed: BTreeMap<WriterKey, WriterCommitInput>,
    failure: Option<String>,
}

impl WriteTerminalBuilder {
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
            if expected
                .insert(key, (writer_id, registration.fragment_id))
                .is_some()
            {
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

    /// Applies an immutable QLC-4 terminal fact.
    pub fn apply_terminal(
        &mut self,
        fragment: &FragmentTerminalSnapshot,
    ) -> Result<(), DistributedQueryError> {
        let key = WriterKey {
            query_id: self.write_id,
            fragment_instance_id: fragment.fragment_instance_id(),
            backend_num: fragment.backend_num(),
        };
        let Some((writer_id, fragment_id)) = self.expected.get(&key).copied() else {
            return Ok(());
        };
        if !matches!(fragment.outcome(), FragmentTerminalOutcome::Succeeded) {
            self.failure
                .get_or_insert_with(|| match fragment.outcome() {
                    FragmentTerminalOutcome::Failed { code, detail } => {
                        format!("native writer failed with {code}: {detail}")
                    }
                    FragmentTerminalOutcome::Cancelled { detail } => {
                        format!("native writer cancelled: {detail}")
                    }
                    FragmentTerminalOutcome::IncompleteDrain { detail } => {
                        format!("native writer drain was incomplete: {detail}")
                    }
                    FragmentTerminalOutcome::Succeeded => unreachable!(),
                });
            return Ok(());
        }
        let sink = fragment.sink();
        let output = WriterCommitInput {
            writer_id,
            fragment_id,
            writer_key: key.clone(),
            iceberg_commits: sink.iceberg_commits.clone(),
            load_counters: BTreeMap::new(),
            loaded_rows: sink.load_stats.loaded_rows,
            loaded_bytes: sink.load_stats.loaded_bytes,
            filtered_rows: sink.load_stats.filtered_rows,
        };
        if let Some(existing) = self.completed.get(&key) {
            if existing == &output {
                return Ok(());
            }
            self.failure.get_or_insert_with(|| {
                "write terminal set contains conflicting writer output".into()
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
