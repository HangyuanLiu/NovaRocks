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

use super::{CoordinationError, CoordinationErrorKind};

pub const COORDINATION_OPERATION_COUNT: usize = 9;
pub const COORDINATION_OUTCOME_COUNT: usize = 13;

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
    ValidateFence = 8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CoordinationOutcome {
    Success = 0,
    Contended = 1,
    AwaitingTakeover = 2,
    Takeover = 3,
    ClockUnsafe = 4,
    FenceLost = 5,
    IncarnationChanged = 6,
    WriteClosed = 7,
    OperationNotCommitted = 8,
    CommitUncertain = 9,
    Corruption = 10,
    StoreUnavailable = 11,
    NotBootstrapped = 12,
}

pub(crate) fn error_outcome(error: &CoordinationError) -> Option<CoordinationOutcome> {
    match error.kind() {
        CoordinationErrorKind::ClockUnsafe => Some(CoordinationOutcome::ClockUnsafe),
        CoordinationErrorKind::FenceLost => Some(CoordinationOutcome::FenceLost),
        CoordinationErrorKind::IncarnationChanged => Some(CoordinationOutcome::IncarnationChanged),
        CoordinationErrorKind::WriteClosed => Some(CoordinationOutcome::WriteClosed),
        CoordinationErrorKind::OperationNotCommitted => {
            Some(CoordinationOutcome::OperationNotCommitted)
        }
        CoordinationErrorKind::CommitUncertain => Some(CoordinationOutcome::CommitUncertain),
        CoordinationErrorKind::Corruption => Some(CoordinationOutcome::Corruption),
        CoordinationErrorKind::StoreUnavailable => Some(CoordinationOutcome::StoreUnavailable),
        CoordinationErrorKind::NotBootstrapped => Some(CoordinationOutcome::NotBootstrapped),
        CoordinationErrorKind::InvalidRequest
        | CoordinationErrorKind::LimitExceeded
        | CoordinationErrorKind::EpochExhausted
        | CoordinationErrorKind::IncarnationExhausted => None,
    }
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
