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

//! The per-round split-assignment driver.
//!
//! It pulls bounded batches from one split source at a time, spreads them over
//! already admitted tasks by split weight, and keeps at most one update in
//! flight per task. Every admitted task — including one that receives no work
//! at all — is told no-more-splits so it can finish.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_types::UniqueId;

use novarocks_proto_codec::connector_read::ConnectorReadCodec;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilterSnapshot, ConnectorReadSplitSource,
};

use super::super::connector_domain::{PlanNodeAssignmentState, Split, SplitAssignmentError};
use super::transport::{TaskUpdateOutcome, TaskUpdateTransport, TaskUpdateTransportError};

/// Largest number of splits one update may carry, matching the wire bound.
pub(crate) const MAX_SPLITS_PER_UPDATE: usize = 4096;

/// One admitted task this driver may assign work to.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AssignmentTarget {
    pub(crate) backend_idx: usize,
    pub(crate) fragment_instance_id: UniqueId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SplitAssignmentDriverError {
    /// The driver was closed; a caller must not keep assigning.
    Closed,
    /// No admitted task exists for this plan node, so a split has nowhere to
    /// go. Failing closed is required: silently dropping it would produce a
    /// query that returns fewer rows than it should.
    NoAdmittedTask {
        plan_node_id: i32,
    },
    Assignment(SplitAssignmentError),
    Transport {
        target: AssignmentTarget,
        detail: String,
    },
    Rejected {
        target: AssignmentTarget,
        reason: String,
        detail: String,
    },
    /// The connector's own enumeration failed. It is never retried blindly:
    /// re-enumerating could produce a different split set for the same pinned
    /// snapshot only if something is wrong, and silently continuing would drop
    /// work.
    SplitSource {
        plan_node_id: i32,
        detail: String,
    },
}

impl fmt::Display for SplitAssignmentDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("split assignment driver is closed"),
            Self::NoAdmittedTask { plan_node_id } => write!(
                formatter,
                "plan node {plan_node_id} has no admitted task to receive splits"
            ),
            Self::Assignment(error) => write!(formatter, "{error}"),
            Self::Transport { target, detail } => write!(
                formatter,
                "task update to backend {} failed: {detail}",
                target.backend_idx
            ),
            Self::Rejected {
                target,
                reason,
                detail,
            } => write!(
                formatter,
                "backend {} rejected a task update ({reason}): {detail}",
                target.backend_idx
            ),
            Self::SplitSource {
                plan_node_id,
                detail,
            } => write!(
                formatter,
                "split enumeration for plan node {plan_node_id} failed: {detail}"
            ),
        }
    }
}

impl std::error::Error for SplitAssignmentDriverError {}

impl From<SplitAssignmentError> for SplitAssignmentDriverError {
    fn from(error: SplitAssignmentError) -> Self {
        Self::Assignment(error)
    }
}

/// A driver-owned split source, closed exactly once when the round ends.
pub(crate) struct SplitSourceHandle {
    plan_node_id: i32,
    finished: bool,
    closed: bool,
}

impl SplitSourceHandle {
    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }
}

/// Per-task send state.
#[derive(Debug, Default)]
struct TaskState {
    /// Assigned weight so far, used to spread work rather than to bound it.
    assigned_weight: u64,
    /// Splits the backend still had queued at the last acknowledgement.
    queued_splits: u64,
    /// One update at a time: a driver waits for the acknowledgement before it
    /// sends the next, so a slow task cannot accumulate unbounded work.
    in_flight: bool,
}

/// The coordinator-side driver for one execution round.
pub(crate) struct SplitAssignmentDriver {
    execution_id: QueryExecutionId,
    transport: std::sync::Arc<dyn TaskUpdateTransport>,
    /// Admitted tasks per plan node, frozen before the Init barrier.
    ///
    /// Keyed by plan node alone rather than by (fragment, plan node): the
    /// distributed planner allocates node ids from one counter spanning every
    /// fragment, so two fragments cannot share one. If that ever changes, this
    /// map would silently merge two scans' task sets.
    tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
    sequences: BTreeMap<AssignmentTarget, PlanNodeAssignmentState>,
    task_state: BTreeMap<AssignmentTarget, TaskState>,
    sources: Vec<SplitSourceHandle>,
    closed: bool,
    /// Splits the backends reported as still queued, above which the driver
    /// stops pulling new batches.
    max_queued_splits_per_task: u64,
    codecs: BTreeMap<i32, std::sync::Arc<dyn ConnectorReadCodec>>,
}

impl SplitAssignmentDriver {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        transport: std::sync::Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
        codecs: BTreeMap<i32, std::sync::Arc<dyn ConnectorReadCodec>>,
    ) -> Self {
        let mut sequences = BTreeMap::new();
        let mut task_state = BTreeMap::new();
        for targets in tasks.values() {
            for target in targets {
                sequences
                    .entry(target.clone())
                    .or_insert_with(PlanNodeAssignmentState::new);
                task_state.entry(target.clone()).or_default();
            }
        }
        let sources = tasks
            .keys()
            .map(|plan_node_id| SplitSourceHandle {
                plan_node_id: *plan_node_id,
                finished: false,
                closed: false,
            })
            .collect();
        Self {
            execution_id,
            transport,
            tasks,
            sequences,
            task_state,
            sources,
            closed: false,
            max_queued_splits_per_task,
            codecs,
        }
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn sources(&self) -> &[SplitSourceHandle] {
        &self.sources
    }

    /// Whether this plan node has already sent its terminal marker to every
    /// admitted task, so there is nothing left to pump.
    pub(crate) fn is_terminal_for(&self, plan_node_id: i32) -> bool {
        self.sources
            .iter()
            .find(|source| source.plan_node_id() == plan_node_id)
            .is_some_and(SplitSourceHandle::is_finished)
    }

    /// Whether every task is currently at or above its queue ceiling, so the
    /// driver should not pull another batch from a split source yet.
    pub(crate) fn is_backpressured(&self, plan_node_id: i32) -> bool {
        let Some(targets) = self.tasks.get(&plan_node_id) else {
            return false;
        };
        targets.iter().all(|target| {
            self.task_state.get(target).is_some_and(|state| {
                state.in_flight || state.queued_splits >= self.max_queued_splits_per_task
            })
        })
    }

    /// Distribute one batch over the admitted tasks of a plan node.
    ///
    /// Placement is by accumulated weight so a heavy split does not crowd a
    /// task the way an equal split count would.
    pub(crate) fn distribute(
        &mut self,
        plan_node_id: i32,
        splits: Vec<Split>,
    ) -> Result<BTreeMap<AssignmentTarget, Vec<Split>>, SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        let all_targets = self
            .tasks
            .get(&plan_node_id)
            .filter(|targets| !targets.is_empty())
            .ok_or(SplitAssignmentDriverError::NoAdmittedTask { plan_node_id })?
            .clone();
        // Prefer tasks that are not already at their queue ceiling. Weight
        // balancing alone would keep feeding a saturated task simply because it
        // has carried less weight so far, which is exactly the case where its
        // queue depth, not its history, is what matters. If every task is
        // saturated, fall back to all of them: the caller only reaches here
        // when it has splits in hand, and holding them back here would strand
        // work that `is_backpressured` already declined to pull.
        let ceiling = self.max_queued_splits_per_task;
        let targets = {
            let available = all_targets
                .iter()
                .filter(|target| {
                    self.task_state
                        .get(*target)
                        .is_none_or(|state| state.queued_splits < ceiling)
                })
                .cloned()
                .collect::<Vec<_>>();
            if available.is_empty() {
                all_targets
            } else {
                available
            }
        };

        let mut placement: BTreeMap<AssignmentTarget, Vec<Split>> = BTreeMap::new();
        for split in splits {
            // Heaviest-first would need the whole batch sorted; assigning each
            // split to the currently lightest task keeps the pass streaming and
            // still balances weight across a batch.
            let target = targets
                .iter()
                .min_by_key(|target| {
                    (
                        self.task_state
                            .get(*target)
                            .map(|state| state.assigned_weight)
                            .unwrap_or_default(),
                        (*target).clone(),
                    )
                })
                .expect("targets is non-empty")
                .clone();
            self.task_state
                .entry(target.clone())
                .or_default()
                .assigned_weight += split.weight_raw();
            placement.entry(target).or_default().push(split);
        }
        Ok(placement)
    }

    /// Send one plan node's batch, then wait for each acknowledgement.
    pub(crate) fn send(
        &mut self,
        plan_node_id: i32,
        placement: BTreeMap<AssignmentTarget, Vec<Split>>,
        no_more_splits: bool,
    ) -> Result<(), SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        // A task that received nothing still has to hear the terminal marker,
        // otherwise its scan would block forever waiting for work that will
        // never arrive.
        let mut recipients: BTreeMap<AssignmentTarget, Vec<Split>> = placement;
        if no_more_splits {
            for target in self.tasks.get(&plan_node_id).into_iter().flatten() {
                recipients.entry(target.clone()).or_default();
            }
        }

        for (target, splits) in recipients {
            if splits.len() > MAX_SPLITS_PER_UPDATE {
                return Err(SplitAssignmentDriverError::Assignment(
                    SplitAssignmentError::BatchTooLarge {
                        plan_node_id,
                        splits: splits.len(),
                    },
                ));
            }
            // Sequences are allocated before the send and are not rolled back
            // if it fails. That is deliberate: a failed send ends the round, so
            // the sequence space dies with it. Reusing a sequence after a
            // failure would be indistinguishable, on the receiving task, from a
            // conflicting retransmission of the one that failed.
            let assignment = self
                .sequences
                .entry(target.clone())
                .or_insert_with(PlanNodeAssignmentState::new)
                .assign(
                    plan_node_id,
                    splits,
                    no_more_splits,
                    self.codecs.get(&plan_node_id).cloned().ok_or_else(|| {
                        SplitAssignmentDriverError::SplitSource {
                            plan_node_id,
                            detail: "missing exact connector read codec".to_owned(),
                        }
                    })?,
                )?;
            let request = super::super::connector_domain::TaskUpdateRequest::new(
                target.fragment_instance_id,
                vec![assignment],
            );

            let state = self.task_state.entry(target.clone()).or_default();
            state.in_flight = true;
            let outcome = self.transport.send(self.execution_id, &target, request);
            let state = self.task_state.entry(target.clone()).or_default();
            state.in_flight = false;

            match outcome {
                Ok(TaskUpdateOutcome::Accepted(nodes)) => {
                    if let Some(node) = nodes.iter().find(|node| node.plan_node_id == plan_node_id)
                    {
                        state.queued_splits = node.queued_splits;
                    }
                }
                Ok(TaskUpdateOutcome::Rejected { reason, detail }) => {
                    return Err(SplitAssignmentDriverError::Rejected {
                        target,
                        reason,
                        detail,
                    });
                }
                Err(error) => {
                    return Err(SplitAssignmentDriverError::Transport {
                        target,
                        detail: error.detail().to_owned(),
                    });
                }
            }
        }

        if no_more_splits
            && let Some(source) = self
                .sources
                .iter_mut()
                .find(|source| source.plan_node_id == plan_node_id)
        {
            source.finished = true;
        }
        Ok(())
    }

    /// Pull one batch from a split source and deliver it.
    ///
    /// Exactly one batch per call, so a caller driving several sources can give
    /// each a turn: draining one source to exhaustion here would let a slow
    /// enumeration starve every other scan in the round.
    ///
    /// Returns `Ok(false)` when nothing was delivered — the tasks are
    /// saturated, or the source had nothing right now. An empty batch means
    /// "nothing right now", never the end of enumeration.
    pub(crate) fn pump(
        &mut self,
        plan_node_id: i32,
        source: &mut dyn ConnectorReadSplitSource,
        batch_size: usize,
    ) -> Result<bool, SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        if self.is_backpressured(plan_node_id) {
            return Ok(false);
        }
        // The frontend produces no runtime feedback today, so the snapshot it
        // offers is honestly unconstrained and final rather than a fabricated
        // pending filter.
        let batch = source
            .next_batch(
                batch_size.clamp(1, MAX_SPLITS_PER_UPDATE),
                &ConnectorReadDynamicFilterSnapshot::all_complete(),
            )
            .map_err(|error| SplitAssignmentDriverError::SplitSource {
                plan_node_id,
                detail: error.to_string(),
            })?;
        let no_more_splits = batch.no_more_splits();
        let splits = batch
            .into_splits()
            .into_iter()
            .map(Split::new)
            .collect::<Vec<_>>();
        let has_work = !splits.is_empty();
        if !has_work && !no_more_splits {
            return Ok(false);
        }
        let placement = self.distribute(plan_node_id, splits)?;
        self.send(plan_node_id, placement, no_more_splits)?;
        Ok(true)
    }

    /// Close the round.
    ///
    /// Idempotent, and after it no assignment may be produced or sent. A
    /// replacement round builds a new driver rather than reviving this one.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        for source in &mut self.sources {
            source.closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_source_error_names_its_plan_node() {
        assert!(
            SplitAssignmentDriverError::SplitSource {
                plan_node_id: 7,
                detail: "closed".to_owned()
            }
            .to_string()
            .contains("plan node 7")
        );
    }
}
