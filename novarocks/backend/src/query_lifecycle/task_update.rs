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

//! Backend-owned admission for runtime split assignment.
//!
//! A task update never creates a lifecycle entry, never revives one, and never
//! extends retention. It is admissible only for a task this backend already
//! admitted and staged, which is why it reuses the existing entry lookup rather
//! than establishing a second task registry.

use novarocks_proto::connector_read::SplitAssignment;
use novarocks_proto::lifecycle::{QueryExecutionId, decode_query_execution_id};
use novarocks_proto::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks as proto;
use novarocks_types::UniqueId;

use super::{QueryLifecycleError, QueryLifecycleErrorCode};

/// Why one task update was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskUpdateRejectionReason {
    UnknownExecution,
    UnknownTask,
    NotAdmitted,
    Terminated,
    SequenceConflict,
    AfterNoMoreSplits,
    UnknownPlanNode,
    InvalidAssignment,
    ResourceExhausted,
}

impl TaskUpdateRejectionReason {
    const fn to_proto(self) -> proto::TaskUpdateRejectionReason {
        match self {
            Self::UnknownExecution => proto::TaskUpdateRejectionReason::UnknownExecution,
            Self::UnknownTask => proto::TaskUpdateRejectionReason::UnknownTask,
            Self::NotAdmitted => proto::TaskUpdateRejectionReason::NotAdmitted,
            Self::Terminated => proto::TaskUpdateRejectionReason::Terminated,
            Self::SequenceConflict => proto::TaskUpdateRejectionReason::SequenceConflict,
            Self::AfterNoMoreSplits => proto::TaskUpdateRejectionReason::AfterNoMoreSplits,
            Self::UnknownPlanNode => proto::TaskUpdateRejectionReason::UnknownPlanNode,
            Self::InvalidAssignment => proto::TaskUpdateRejectionReason::InvalidAssignment,
            Self::ResourceExhausted => proto::TaskUpdateRejectionReason::ResourceExhausted,
        }
    }

    /// A lifecycle admission failure maps onto exactly one wire reason, so a
    /// sender can distinguish "retry later" from "this attempt is over".
    const fn from_lifecycle(code: QueryLifecycleErrorCode) -> Self {
        match code {
            QueryLifecycleErrorCode::Terminated => Self::Terminated,
            QueryLifecycleErrorCode::InvalidManifest => Self::UnknownTask,
            QueryLifecycleErrorCode::Conflict => Self::NotAdmitted,
            QueryLifecycleErrorCode::Capacity => Self::ResourceExhausted,
            QueryLifecycleErrorCode::StaleBackend
            | QueryLifecycleErrorCode::Transport
            | QueryLifecycleErrorCode::Internal => Self::NotAdmitted,
        }
    }
}

/// What one plan node has durably accepted after this update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaskUpdateAcceptedNode {
    pub(crate) plan_node_id: i32,
    pub(crate) accepted_through_sequence: u64,
    pub(crate) no_more_splits: bool,
    pub(crate) queued_splits: u64,
}

/// The typed acknowledgement returned for every task update, accepted or not.
#[derive(Clone, Debug)]
pub(crate) enum TaskUpdateAck {
    Accepted(Vec<TaskUpdateAcceptedNode>),
    Rejected {
        reason: TaskUpdateRejectionReason,
        detail: String,
        field_path: Option<String>,
    },
}

/// Longest safe detail carried back to the sender.
const MAX_SAFE_DETAIL_BYTES: usize = 512;
const MAX_SAFE_FIELD_PATH_BYTES: usize = 256;

impl TaskUpdateAck {
    pub(crate) fn rejected(reason: TaskUpdateRejectionReason, detail: impl Into<String>) -> Self {
        Self::Rejected {
            reason,
            detail: truncate_on_char_boundary(detail.into(), MAX_SAFE_DETAIL_BYTES),
            field_path: None,
        }
    }

    fn rejected_at(
        reason: TaskUpdateRejectionReason,
        detail: impl Into<String>,
        field_path: String,
    ) -> Self {
        Self::Rejected {
            reason,
            detail: truncate_on_char_boundary(detail.into(), MAX_SAFE_DETAIL_BYTES),
            field_path: Some(truncate_on_char_boundary(
                field_path,
                MAX_SAFE_FIELD_PATH_BYTES,
            )),
        }
    }

    pub(crate) fn to_proto(&self) -> proto::TaskUpdateResponse {
        let outcome = match self {
            Self::Accepted(nodes) => {
                proto::task_update_response::Outcome::Accepted(proto::TaskUpdateAccepted {
                    nodes: nodes
                        .iter()
                        .map(|node| proto::TaskUpdateAcceptedNode {
                            plan_node_id: node.plan_node_id,
                            accepted_through_sequence: node.accepted_through_sequence,
                            no_more_splits: node.no_more_splits,
                            queued_splits: node.queued_splits,
                        })
                        .collect(),
                })
            }
            Self::Rejected {
                reason,
                detail,
                field_path,
            } => proto::task_update_response::Outcome::Rejection(proto::TaskUpdateRejection {
                reason: reason.to_proto() as i32,
                safe_detail: detail.clone(),
                safe_field_path: field_path.clone(),
            }),
        };
        proto::TaskUpdateResponse {
            outcome: Some(outcome),
        }
    }
}

fn truncate_on_char_boundary(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

/// A structurally validated task update.
#[derive(Clone, Debug)]
pub(crate) struct TaskUpdateRequest {
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    assignments: Vec<SplitAssignment>,
}

impl TaskUpdateRequest {
    pub(crate) fn parse(raw: proto::TaskUpdateRequest) -> Result<Self, ProtocolError> {
        let root = FieldPath::root("task_update_request");
        let execution_id = raw.execution_id.as_ref().ok_or_else(|| {
            ProtocolError::new(
                root.clone().field("execution_id"),
                ProtocolErrorKind::MissingField,
                "task update requires an execution id",
            )
        })?;
        let execution_id = decode_query_execution_id(execution_id).map_err(|error| {
            ProtocolError::new(
                root.clone().field("execution_id"),
                error.kind(),
                error.detail().to_owned(),
            )
        })?;
        let fragment_instance_id = raw.fragment_instance_id.as_ref().ok_or_else(|| {
            ProtocolError::new(
                root.clone().field("fragment_instance_id"),
                ProtocolErrorKind::MissingField,
                "task update requires a fragment instance id",
            )
        })?;
        let fragment_instance_id = UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo);
        let assignments = novarocks_proto::connector_read::parse_task_update_assignments(
            &raw.assignments,
            root.field("assignments"),
        )?;
        Ok(Self {
            execution_id,
            fragment_instance_id,
            assignments,
        })
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn assignments(&self) -> &[SplitAssignment] {
        &self.assignments
    }
}

/// Turn a validation failure into a rejection rather than a transport error:
/// a malformed assignment is the sender's fault and must be reported before
/// any split reaches a queue.
pub(crate) fn rejection_from_contract_error(error: &ProtocolError) -> TaskUpdateAck {
    let reason = match error.kind() {
        ProtocolErrorKind::Capacity | ProtocolErrorKind::OutOfRange => {
            TaskUpdateRejectionReason::ResourceExhausted
        }
        ProtocolErrorKind::MissingField
        | ProtocolErrorKind::InvalidEnum
        | ProtocolErrorKind::InvalidValue
        | ProtocolErrorKind::DuplicateField
        | ProtocolErrorKind::InconsistentFields
        | ProtocolErrorKind::Unsupported
        | ProtocolErrorKind::Conflict
        | ProtocolErrorKind::VersionMismatch => TaskUpdateRejectionReason::InvalidAssignment,
    };
    TaskUpdateAck::rejected_at(reason, error.detail().to_owned(), error.path().to_string())
}

/// Turn a lifecycle admission failure into a typed rejection.
pub(crate) fn rejection_from_lifecycle_error(error: &QueryLifecycleError) -> TaskUpdateAck {
    TaskUpdateAck::rejected(
        TaskUpdateRejectionReason::from_lifecycle(error.code()),
        error.detail().to_owned(),
    )
}

/// The backend-local owner that actually enqueues admitted splits.
///
/// Admission and delivery are separate on purpose: the lifecycle owns whether
/// an attempt may receive work, and the fragment runtime owns the per-plan-node
/// queue that work lands in.
pub(crate) trait TaskSplitDelivery: Send + Sync + 'static {
    fn deliver(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        assignments: &[SplitAssignment],
    ) -> TaskUpdateAck;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_execution_id_is_a_contract_error() {
        let error = TaskUpdateRequest::parse(proto::TaskUpdateRequest {
            execution_id: None,
            fragment_instance_id: None,
            assignments: Vec::new(),
        })
        .expect_err("missing execution id");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "task_update_request.execution_id");
    }

    #[test]
    fn every_lifecycle_admission_failure_maps_to_a_typed_reason() {
        for (code, expected) in [
            (
                QueryLifecycleErrorCode::Terminated,
                TaskUpdateRejectionReason::Terminated,
            ),
            (
                QueryLifecycleErrorCode::InvalidManifest,
                TaskUpdateRejectionReason::UnknownTask,
            ),
            (
                QueryLifecycleErrorCode::Conflict,
                TaskUpdateRejectionReason::NotAdmitted,
            ),
            (
                QueryLifecycleErrorCode::Capacity,
                TaskUpdateRejectionReason::ResourceExhausted,
            ),
        ] {
            assert_eq!(TaskUpdateRejectionReason::from_lifecycle(code), expected);
        }
    }

    #[test]
    fn a_rejection_detail_is_bounded_on_a_character_boundary() {
        let ack = TaskUpdateAck::rejected(
            TaskUpdateRejectionReason::InvalidAssignment,
            "é".repeat(MAX_SAFE_DETAIL_BYTES),
        );
        let TaskUpdateAck::Rejected { detail, .. } = &ack else {
            panic!("rejected");
        };
        assert!(detail.len() <= MAX_SAFE_DETAIL_BYTES);
        assert!(detail.is_char_boundary(detail.len()));
    }

    #[test]
    fn an_accepted_ack_reports_each_plan_node_terminal_state() {
        let ack = TaskUpdateAck::Accepted(vec![TaskUpdateAcceptedNode {
            plan_node_id: 7,
            accepted_through_sequence: 12,
            no_more_splits: true,
            queued_splits: 0,
        }]);
        let encoded = ack.to_proto();
        let Some(proto::task_update_response::Outcome::Accepted(accepted)) = encoded.outcome else {
            panic!("accepted outcome");
        };
        assert_eq!(accepted.nodes.len(), 1);
        assert_eq!(accepted.nodes[0].plan_node_id, 7);
        assert_eq!(accepted.nodes[0].accepted_through_sequence, 12);
        assert!(accepted.nodes[0].no_more_splits);
    }
}
