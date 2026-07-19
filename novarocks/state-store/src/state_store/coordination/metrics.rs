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

use std::sync::atomic::{AtomicU64, Ordering};

pub const COORDINATION_OPERATION_COUNT: usize = 8;
pub const COORDINATION_OUTCOME_COUNT: usize = 11;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CoordinationOperation {
    Bootstrap = 0,
    Load = 1,
    BeginRestore = 2,
    OpenWrites = 3,
    AdmitWrites = 4,
    Acquire = 5,
    Renew = 6,
    Release = 7,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CoordinationOutcome {
    Success = 0,
    Contended = 1,
    AwaitingTakeover = 2,
    ClockUnsafe = 3,
    FenceLost = 4,
    IncarnationChanged = 5,
    WriteClosed = 6,
    OperationNotCommitted = 7,
    CommitUncertain = 8,
    Corruption = 9,
    StoreUnavailable = 10,
}

#[derive(Debug)]
pub struct CoordinationMetrics {
    operation_outcomes: [[AtomicU64; COORDINATION_OUTCOME_COUNT]; COORDINATION_OPERATION_COUNT],
}

impl CoordinationMetrics {
    pub fn new() -> Self {
        Self {
            operation_outcomes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }

    pub fn record(&self, operation: CoordinationOperation, outcome: CoordinationOutcome) {
        self.operation_outcomes[operation as usize][outcome as usize]
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CoordinationMetricsSnapshot {
        CoordinationMetricsSnapshot {
            operation_outcomes: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| {
                    self.operation_outcomes[operation][outcome].load(Ordering::Relaxed)
                })
            }),
        }
    }
}

impl Default for CoordinationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinationMetricsSnapshot {
    operation_outcomes: [[u64; COORDINATION_OUTCOME_COUNT]; COORDINATION_OPERATION_COUNT],
}

impl CoordinationMetricsSnapshot {
    pub fn operation_outcome_count(
        &self,
        operation: CoordinationOperation,
        outcome: CoordinationOutcome,
    ) -> u64 {
        self.operation_outcomes[operation as usize][outcome as usize]
    }
}
