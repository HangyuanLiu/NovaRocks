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

pub const STATE_STORE_OPERATION_COUNT: usize = 6;
pub const STATE_STORE_OUTCOME_COUNT: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum StateStoreOperation {
    Begin = 0,
    Get = 1,
    Range = 2,
    Put = 3,
    Delete = 4,
    Commit = 5,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum StateStoreOutcome {
    Success = 0,
    Error = 1,
    Conflict = 2,
    TransientBeforeCommit = 3,
    DefiniteFailure = 4,
    CommitUnknown = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreMetricsSnapshot {
    pub provider: &'static str,
    pub begin_count: u64,
    pub get_count: u64,
    pub range_count: u64,
    pub put_count: u64,
    pub delete_count: u64,
    pub commit_count: u64,
    pub operation_outcomes: [[u64; STATE_STORE_OUTCOME_COUNT]; STATE_STORE_OPERATION_COUNT],
    pub operation_duration_micros: [u64; STATE_STORE_OPERATION_COUNT],
    pub operation_duration_observations: [u64; STATE_STORE_OPERATION_COUNT],
    pub retry_count: u64,
    pub deadline_count: u64,
    pub blocking_failure_count: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub page_records: u64,
    pub notification_lag_micros: u64,
    pub notification_lag_observations: u64,
}

impl StateStoreMetricsSnapshot {
    pub fn operation_outcome_count(
        &self,
        operation: StateStoreOperation,
        outcome: StateStoreOutcome,
    ) -> u64 {
        self.operation_outcomes[operation as usize][outcome as usize]
    }

    pub fn operation_duration_micros(&self, operation: StateStoreOperation) -> u64 {
        self.operation_duration_micros[operation as usize]
    }

    pub fn operation_duration_observations(&self, operation: StateStoreOperation) -> u64 {
        self.operation_duration_observations[operation as usize]
    }
}
