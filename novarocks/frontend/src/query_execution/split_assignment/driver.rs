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

use novarocks_proto::lifecycle::QueryExecutionId;
use novarocks_types::UniqueId;

use novarocks_proto::connector_read::{TypedConnectorSplitSource, WireDynamicFilterSnapshot};

use super::super::connector_domain::{
    CatalogHandle, PlanNodeAssignmentState, Split, SplitAssignmentError,
};
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
    /// The catalog generation every split of this round belongs to.
    catalog: CatalogHandle,
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
}

impl SplitAssignmentDriver {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        catalog: CatalogHandle,
        transport: std::sync::Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
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
            catalog,
            transport,
            tasks,
            sequences,
            task_state,
            sources,
            closed: false,
            max_queued_splits_per_task,
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
        let targets = self
            .tasks
            .get(&plan_node_id)
            .filter(|targets| !targets.is_empty())
            .ok_or(SplitAssignmentDriverError::NoAdmittedTask { plan_node_id })?
            .clone();

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
                .assign(plan_node_id, splits, no_more_splits)?;
            let encoded = vec![assignment.to_proto()];

            let state = self.task_state.entry(target.clone()).or_default();
            state.in_flight = true;
            let outcome = self.transport.send(
                self.execution_id,
                &target,
                target.fragment_instance_id,
                encoded,
            );
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
        source: &mut dyn TypedConnectorSplitSource,
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
                &WireDynamicFilterSnapshot::all_complete(),
            )
            .map_err(|error| SplitAssignmentDriverError::SplitSource {
                plan_node_id,
                detail: error.to_string(),
            })?;
        let no_more_splits = batch.no_more_splits();
        let splits = batch
            .into_splits()
            .into_iter()
            .map(|split| Split::new(self.catalog_for(plan_node_id), split))
            .collect::<Vec<_>>();
        let has_work = !splits.is_empty();
        if !has_work && !no_more_splits {
            return Ok(false);
        }
        let placement = self.distribute(plan_node_id, splits)?;
        self.send(plan_node_id, placement, no_more_splits)?;
        Ok(true)
    }

    fn catalog_for(&self, _plan_node_id: i32) -> CatalogHandle {
        self.catalog.clone()
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
    use std::sync::Arc;
    use std::sync::Mutex;

    use novarocks_proto::FieldPath;
    use novarocks_proto::connector_read::ValidatedConnectorSplit;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::ConnectorSplitBatch;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_types::{AttemptId, QueryId};

    use super::super::super::connector_domain::CatalogHandle;
    use super::super::transport::AcceptedPlanNode;
    use super::*;

    #[derive(Default)]
    struct RecordingTransport {
        sent: Mutex<Vec<(AssignmentTarget, Vec<dto::SplitAssignment>)>>,
        reject: Mutex<bool>,
        fail: Mutex<bool>,
    }

    impl TaskUpdateTransport for RecordingTransport {
        fn send(
            &self,
            _execution_id: QueryExecutionId,
            target: &AssignmentTarget,
            _fragment_instance_id: UniqueId,
            assignments: Vec<dto::SplitAssignment>,
        ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
            if *self.fail.lock().expect("transport lock") {
                return Err(TaskUpdateTransportError::new("backend unreachable"));
            }
            if *self.reject.lock().expect("transport lock") {
                return Ok(TaskUpdateOutcome::Rejected {
                    reason: "TERMINATED".to_owned(),
                    detail: "query lifecycle has terminated".to_owned(),
                });
            }
            let accepted = assignments
                .iter()
                .map(|assignment| AcceptedPlanNode {
                    plan_node_id: assignment.plan_node_id,
                    accepted_through_sequence: assignment
                        .splits
                        .last()
                        .map(|split| split.sequence_id)
                        .unwrap_or_default(),
                    no_more_splits: assignment.no_more_splits,
                    queued_splits: assignment.splits.len() as u64,
                })
                .collect();
            self.sent
                .lock()
                .expect("transport lock")
                .push((target.clone(), assignments));
            Ok(TaskUpdateOutcome::Accepted(accepted))
        }
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
            .expect("execution id")
    }

    fn validated_split(weight_raw: u64) -> ValidatedConnectorSplit {
        let raw = dto::ConnectorSplit {
            split_weight_raw: weight_raw,
            remotely_accessible: true,
            addresses: Vec::new(),
            affinity_key: None,
            retained_size_in_bytes: 64,
            category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                provider: Some(dto::data_split::Provider::Iceberg(dto::IcebergSplit {
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
                })),
            })),
        };
        ValidatedConnectorSplit::parse(raw, FieldPath::root("connector_split"))
            .expect("valid split")
    }

    fn split(weight_raw: u64) -> Split {
        Split::new(
            CatalogHandle::new("ice", [1; 16]),
            validated_split(weight_raw),
        )
    }

    /// A source that hands out pre-programmed batches in order.
    struct ScriptedSource {
        batches: std::collections::VecDeque<ConnectorSplitBatch<ValidatedConnectorSplit>>,
        fail: bool,
        closed: usize,
    }

    impl TypedConnectorSplitSource for ScriptedSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &WireDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ValidatedConnectorSplit>, ConnectorError> {
            if self.fail {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "manifest read failed",
                ));
            }
            Ok(self
                .batches
                .pop_front()
                .unwrap_or_else(ConnectorSplitBatch::finished))
        }

        fn is_finished(&self) -> bool {
            self.batches.is_empty()
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closed += 1;
            Ok(())
        }
    }

    fn target(backend_idx: usize) -> AssignmentTarget {
        AssignmentTarget {
            backend_idx,
            fragment_instance_id: UniqueId::new(9, backend_idx as i64),
        }
    }

    fn new_driver(transport: Arc<RecordingTransport>, backends: usize) -> SplitAssignmentDriver {
        let mut tasks = BTreeMap::new();
        tasks.insert(7_i32, (0..backends).map(target).collect::<Vec<_>>());
        SplitAssignmentDriver::new(
            execution_id(),
            CatalogHandle::new("ice", [1; 16]),
            transport,
            tasks,
            1024,
        )
    }

    #[test]
    fn splits_spread_across_tasks_by_weight() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(transport, 2);
        let placement = driver
            .distribute(7, vec![split(400), split(100), split(100)])
            .expect("distribute");
        let counts = placement
            .values()
            .map(Vec::len)
            .collect::<std::collections::BTreeSet<_>>();
        // The heavy split lands alone; the two light ones share the other task.
        assert_eq!(counts, [1_usize, 2].into_iter().collect());
    }

    #[test]
    fn a_task_with_no_work_still_receives_the_terminal_marker() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 3);
        let placement = driver.distribute(7, vec![split(100)]).expect("distribute");
        driver.send(7, placement, true).expect("send");
        let sent = transport.sent.lock().expect("transport lock");
        assert_eq!(sent.len(), 3);
        assert!(
            sent.iter()
                .all(|(_, assignments)| assignments[0].no_more_splits)
        );
        assert_eq!(
            sent.iter()
                .filter(|(_, assignments)| assignments[0].splits.is_empty())
                .count(),
            2
        );
    }

    #[test]
    fn zero_splits_finish_the_source_without_a_synthetic_split() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 2);
        driver.send(7, BTreeMap::new(), true).expect("send");
        assert!(driver.sources()[0].is_finished());
        let sent = transport.sent.lock().expect("transport lock");
        assert!(
            sent.iter()
                .all(|(_, assignments)| assignments[0].splits.is_empty())
        );
    }

    #[test]
    fn a_plan_node_cannot_be_assigned_after_its_terminal_marker() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(transport, 1);
        driver.send(7, BTreeMap::new(), true).expect("send");
        let placement = driver.distribute(7, vec![split(100)]).expect("distribute");
        let error = driver.send(7, placement, false).expect_err("terminal");
        assert!(matches!(
            error,
            SplitAssignmentDriverError::Assignment(SplitAssignmentError::AlreadyTerminal {
                plan_node_id: 7
            })
        ));
    }

    #[test]
    fn a_closed_driver_refuses_further_work() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(transport, 1);
        driver.close();
        driver.close();
        assert!(driver.is_closed());
        assert!(driver.sources().iter().all(SplitSourceHandle::is_closed));
        assert_eq!(
            driver.distribute(7, vec![split(100)]).expect_err("closed"),
            SplitAssignmentDriverError::Closed
        );
        assert_eq!(
            driver.send(7, BTreeMap::new(), true).expect_err("closed"),
            SplitAssignmentDriverError::Closed
        );
    }

    #[test]
    fn a_plan_node_without_an_admitted_task_fails_closed() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(transport, 1);
        assert_eq!(
            driver.distribute(9, vec![split(100)]).expect_err("no task"),
            SplitAssignmentDriverError::NoAdmittedTask { plan_node_id: 9 }
        );
    }

    #[test]
    fn a_rejection_and_a_transport_failure_both_surface_typed() {
        let transport = Arc::new(RecordingTransport::default());
        *transport.reject.lock().expect("lock") = true;
        let mut driver = new_driver(Arc::clone(&transport), 1);
        let placement = driver.distribute(7, vec![split(100)]).expect("distribute");
        assert!(matches!(
            driver.send(7, placement, false).expect_err("rejected"),
            SplitAssignmentDriverError::Rejected { .. }
        ));

        *transport.reject.lock().expect("lock") = false;
        *transport.fail.lock().expect("lock") = true;
        let mut driver = new_driver(Arc::clone(&transport), 1);
        let placement = driver.distribute(7, vec![split(100)]).expect("distribute");
        assert!(matches!(
            driver.send(7, placement, false).expect_err("transport"),
            SplitAssignmentDriverError::Transport { .. }
        ));
    }

    #[test]
    fn sequences_advance_across_batches_within_one_task() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 1);
        for _ in 0..2 {
            let placement = driver.distribute(7, vec![split(100)]).expect("distribute");
            driver.send(7, placement, false).expect("send");
        }
        let sent = transport.sent.lock().expect("transport lock");
        let sequences = sent
            .iter()
            .flat_map(|(_, assignments)| {
                assignments[0].splits.iter().map(|split| split.sequence_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![0, 1]);
    }

    #[test]
    fn pump_drains_a_source_and_delivers_the_terminal_marker() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 2);
        let mut source = ScriptedSource {
            batches: [
                ConnectorSplitBatch::new(vec![validated_split(100), validated_split(100)], false),
                ConnectorSplitBatch::new(vec![validated_split(100)], true),
            ]
            .into_iter()
            .collect(),
            fail: false,
            closed: 0,
        };
        while !driver.is_terminal_for(7) {
            driver.pump(7, &mut source, 16).expect("pump");
        }
        let sent = transport.sent.lock().expect("transport lock");
        let total_splits: usize = sent
            .iter()
            .map(|(_, assignments)| assignments[0].splits.len())
            .sum();
        assert_eq!(total_splits, 3);
        assert!(
            sent.iter()
                .filter(|(_, assignments)| assignments[0].no_more_splits)
                .count()
                >= 1
        );
    }

    #[test]
    fn an_empty_batch_yields_without_ending_enumeration() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 1);
        let mut source = ScriptedSource {
            batches: [ConnectorSplitBatch::empty()].into_iter().collect(),
            fail: false,
            closed: 0,
        };
        assert!(!driver.pump(7, &mut source, 16).expect("pump"));
        assert!(transport.sent.lock().expect("transport lock").is_empty());
        assert!(!driver.is_closed());
    }

    #[test]
    fn a_source_failure_surfaces_typed_and_sends_nothing() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(Arc::clone(&transport), 1);
        let mut source = ScriptedSource {
            batches: std::collections::VecDeque::new(),
            fail: true,
            closed: 0,
        };
        assert!(matches!(
            driver.pump(7, &mut source, 16).expect_err("source failure"),
            SplitAssignmentDriverError::SplitSource {
                plan_node_id: 7,
                ..
            }
        ));
        assert!(transport.sent.lock().expect("transport lock").is_empty());
    }

    #[test]
    fn a_closed_driver_refuses_to_pump() {
        let transport = Arc::new(RecordingTransport::default());
        let mut driver = new_driver(transport, 1);
        driver.close();
        let mut source = ScriptedSource {
            batches: std::collections::VecDeque::new(),
            fail: false,
            closed: 0,
        };
        assert_eq!(
            driver.pump(7, &mut source, 16).expect_err("closed"),
            SplitAssignmentDriverError::Closed
        );
    }
}
