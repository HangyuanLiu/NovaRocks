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

//! The port a driver uses to deliver one task update.
//!
//! Keeping this a port rather than a concrete client is what lets the driver
//! be tested for ack, backpressure, terminal, and cancellation behavior without
//! a live backend, and keeps native transport ownership with the coordinator.

use std::fmt;

use super::driver::AssignmentTarget;
use crate::query_execution::connector_domain::TaskUpdateRequest;
use novarocks_proto_codec::lifecycle::QueryExecutionId;

/// What one backend reported for one plan node after an update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AcceptedPlanNode {
    pub(crate) plan_node_id: i32,
    pub(crate) accepted_through_sequence: u64,
    pub(crate) no_more_splits: bool,
    /// Splits still queued on the task. A driver uses this for backpressure;
    /// it is an observation, never an instruction.
    pub(crate) queued_splits: u64,
}

/// The outcome of one delivered task update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TaskUpdateOutcome {
    Accepted(Vec<AcceptedPlanNode>),
    /// The task refused the update. The driver must not retry the same content
    /// blindly: a rejection is a decision about this attempt, not a hiccup.
    Rejected {
        reason: String,
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaskUpdateTransportError {
    detail: String,
}

impl TaskUpdateTransportError {
    pub(crate) fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for TaskUpdateTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for TaskUpdateTransportError {}

pub(crate) trait TaskUpdateTransport: Send + Sync {
    fn send(
        &self,
        execution_id: QueryExecutionId,
        target: &AssignmentTarget,
        request: TaskUpdateRequest,
    ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError>;
}
