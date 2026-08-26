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

//! Runtime split-assignment values owned by the coordinator.
//!
//! Sequence is the only scheduling identity: it is monotonic within one
//! (task attempt, plan node), an exact replay is idempotent, and the same
//! sequence carrying different content is a conflict. No split digest, content
//! id, or self-attestation exists here.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_proto::connector_read::canonical_scheduled_split_bytes;
use novarocks_proto_models::connector_read as dto;
use novarocks_types::UniqueId;

use super::handle::Split;

/// Largest number of splits one assignment may carry, matching the wire bound.
pub(crate) const MAX_SPLITS_PER_ASSIGNMENT: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SplitAssignmentError {
    /// A plan node already reached its terminal marker.
    AlreadyTerminal { plan_node_id: i32 },
    /// The batch would exceed the per-assignment split bound.
    BatchTooLarge { plan_node_id: i32, splits: usize },
}

impl fmt::Display for SplitAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyTerminal { plan_node_id } => write!(
                formatter,
                "plan node {plan_node_id} already reached no-more-splits"
            ),
            Self::BatchTooLarge {
                plan_node_id,
                splits,
            } => write!(
                formatter,
                "plan node {plan_node_id} assignment carries {splits} splits, above the bound"
            ),
        }
    }
}

impl std::error::Error for SplitAssignmentError {}

/// Allocates monotonic sequences for one task attempt.
///
/// A new attempt constructs a new allocator, so a sequence from a replaced
/// round can never be reused or resumed.
#[derive(Debug, Default)]
pub(crate) struct SplitSequenceAllocator {
    next: BTreeMap<i32, u64>,
}

impl SplitSequenceAllocator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn allocate(&mut self, plan_node_id: i32) -> u64 {
        let slot = self.next.entry(plan_node_id).or_insert(0);
        let sequence = *slot;
        *slot += 1;
        sequence
    }

    /// The next sequence a plan node will use; also the count already issued.
    pub(crate) fn issued(&self, plan_node_id: i32) -> u64 {
        self.next.get(&plan_node_id).copied().unwrap_or_default()
    }
}

/// One split placed at one sequence in one plan node's queue.
#[derive(Clone, Debug)]
pub(crate) struct ScheduledSplit {
    sequence_id: u64,
    plan_node_id: i32,
    split: Split,
    encoded: dto::ScheduledSplit,
}

impl ScheduledSplit {
    pub(crate) const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn split(&self) -> &Split {
        &self.split
    }

    pub(crate) const fn as_proto(&self) -> &dto::ScheduledSplit {
        &self.encoded
    }

    /// The bytes a retransmission must reproduce exactly.
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        canonical_scheduled_split_bytes(&self.encoded)
    }
}

/// A batch of splits for one plan node, plus its terminal marker.
#[derive(Clone, Debug)]
pub(crate) struct SplitAssignment {
    plan_node_id: i32,
    splits: Vec<ScheduledSplit>,
    no_more_splits: bool,
}

impl SplitAssignment {
    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) fn splits(&self) -> &[ScheduledSplit] {
        &self.splits
    }

    pub(crate) const fn no_more_splits(&self) -> bool {
        self.no_more_splits
    }

    pub(crate) fn to_proto(&self) -> dto::SplitAssignment {
        dto::SplitAssignment {
            plan_node_id: self.plan_node_id,
            splits: self
                .splits
                .iter()
                .map(|split| split.encoded.clone())
                .collect(),
            no_more_splits: self.no_more_splits,
        }
    }
}

/// Per-plan-node send state for one task attempt.
///
/// It records what has been issued and whether the terminal marker was already
/// sent, so a driver cannot append after no-more or reuse a sequence.
#[derive(Debug, Default)]
pub(crate) struct PlanNodeAssignmentState {
    sequences: SplitSequenceAllocator,
    terminal: BTreeMap<i32, bool>,
}

impl PlanNodeAssignmentState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_terminal(&self, plan_node_id: i32) -> bool {
        self.terminal
            .get(&plan_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn issued(&self, plan_node_id: i32) -> u64 {
        self.sequences.issued(plan_node_id)
    }

    /// Build the next assignment for one plan node.
    ///
    /// Zero splits with `no_more_splits` is the normal way a plan node with no
    /// work finishes; it never produces a synthetic empty split.
    pub(crate) fn assign(
        &mut self,
        plan_node_id: i32,
        splits: Vec<Split>,
        no_more_splits: bool,
    ) -> Result<SplitAssignment, SplitAssignmentError> {
        if self.is_terminal(plan_node_id) {
            return Err(SplitAssignmentError::AlreadyTerminal { plan_node_id });
        }
        if splits.len() > MAX_SPLITS_PER_ASSIGNMENT {
            return Err(SplitAssignmentError::BatchTooLarge {
                plan_node_id,
                splits: splits.len(),
            });
        }
        let scheduled = splits
            .into_iter()
            .map(|split| {
                let sequence_id = self.sequences.allocate(plan_node_id);
                let encoded = dto::ScheduledSplit {
                    sequence_id,
                    plan_node_id,
                    split: Some(split.split().as_proto().clone()),
                };
                ScheduledSplit {
                    sequence_id,
                    plan_node_id,
                    split,
                    encoded,
                }
            })
            .collect();
        if no_more_splits {
            self.terminal.insert(plan_node_id, true);
        }
        Ok(SplitAssignment {
            plan_node_id,
            splits: scheduled,
            no_more_splits,
        })
    }
}

/// One update delivered to one admitted task.
#[derive(Clone, Debug)]
pub(crate) struct TaskUpdateRequest {
    fragment_instance_id: UniqueId,
    assignments: Vec<SplitAssignment>,
}

impl TaskUpdateRequest {
    pub(crate) fn new(fragment_instance_id: UniqueId, assignments: Vec<SplitAssignment>) -> Self {
        Self {
            fragment_instance_id,
            assignments,
        }
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn assignments(&self) -> &[SplitAssignment] {
        &self.assignments
    }

    pub(crate) fn to_proto_assignments(&self) -> Vec<dto::SplitAssignment> {
        self.assignments
            .iter()
            .map(SplitAssignment::to_proto)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto::FieldPath;
    use novarocks_proto::connector_read::ValidatedConnectorSplit;

    use super::super::handle::CatalogHandle;
    use super::*;

    fn split() -> Split {
        let raw = dto::ConnectorSplit {
            split_weight_raw: 100,
            remotely_accessible: true,
            addresses: Vec::new(),
            affinity_key: None,
            retained_size_in_bytes: 64,
            category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                provider: Some(dto::data_split::Provider::Iceberg(iceberg_split())),
            })),
        };
        let validated = ValidatedConnectorSplit::parse(raw, FieldPath::root("connector_split"))
            .expect("valid split");
        Split::new(validated)
    }

    fn iceberg_split() -> dto::IcebergSplit {
        dto::IcebergSplit {
            path: "s3://bucket/table/data/0001.parquet".to_owned(),
            start: 0,
            length: 1024,
            file_size: 1024,
            file_record_count: 10,
            file_format: dto::IcebergFileFormat::Parquet as i32,
            partition_spec_id: 0,
            partition_data_json: "{}".to_owned(),
            deletes: Vec::new(),
            file_statistics_domain: Some(dto::TupleDomain {
                none: false,
                column_domains: Vec::new(),
            }),
            data_sequence_number: Some(1),
            file_first_row_id: None,
            decryption_data: None,
        }
    }

    #[test]
    fn sequences_are_monotonic_per_plan_node() {
        let mut state = PlanNodeAssignmentState::new();
        let first = state
            .assign(7, vec![split(), split()], false)
            .expect("assign");
        assert_eq!(
            first
                .splits()
                .iter()
                .map(ScheduledSplit::sequence_id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        let second = state.assign(7, vec![split()], true).expect("assign");
        assert_eq!(second.splits()[0].sequence_id(), 2);
        assert!(second.no_more_splits());
    }

    #[test]
    fn plan_nodes_have_independent_sequence_spaces() {
        let mut state = PlanNodeAssignmentState::new();
        state.assign(1, vec![split()], false).expect("assign");
        let other = state.assign(2, vec![split()], false).expect("assign");
        assert_eq!(other.splits()[0].sequence_id(), 0);
    }

    #[test]
    fn a_plan_node_cannot_be_assigned_after_its_terminal_marker() {
        let mut state = PlanNodeAssignmentState::new();
        state.assign(3, Vec::new(), true).expect("terminal");
        assert!(state.is_terminal(3));
        assert_eq!(
            state.assign(3, vec![split()], false).expect_err("terminal"),
            SplitAssignmentError::AlreadyTerminal { plan_node_id: 3 }
        );
    }

    #[test]
    fn a_plan_node_with_no_work_finishes_without_a_synthetic_split() {
        let mut state = PlanNodeAssignmentState::new();
        let assignment = state.assign(5, Vec::new(), true).expect("assign");
        assert!(assignment.splits().is_empty());
        assert!(assignment.no_more_splits());
        assert_eq!(state.issued(5), 0);
    }

    #[test]
    fn a_new_attempt_restarts_the_sequence_space() {
        let mut first = PlanNodeAssignmentState::new();
        first
            .assign(1, vec![split(), split()], false)
            .expect("assign");
        let mut replacement = PlanNodeAssignmentState::new();
        let assignment = replacement.assign(1, vec![split()], false).expect("assign");
        assert_eq!(assignment.splits()[0].sequence_id(), 0);
    }

    #[test]
    fn canonical_bytes_are_stable_for_the_same_scheduled_split() {
        let mut state = PlanNodeAssignmentState::new();
        let assignment = state.assign(1, vec![split()], false).expect("assign");
        let scheduled = &assignment.splits()[0];
        assert_eq!(scheduled.canonical_bytes(), scheduled.canonical_bytes());
    }
}
