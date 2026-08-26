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

//! The split-assignment resource one execution round owns.
//!
//! It exists for exactly one round: aborting the round closes every split
//! source and sender, and a replacement round builds a new one under a new
//! attempt id rather than resuming this. Nothing here survives a round, which
//! is what keeps a replaced attempt from inheriting a sequence space.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_proto::connector_read::TypedConnectorSplitSource;
use novarocks_proto::lifecycle::QueryExecutionId;

use super::super::connector_domain::CatalogHandle;
use super::driver::{AssignmentTarget, SplitAssignmentDriver, SplitAssignmentDriverError};
use super::transport::TaskUpdateTransport;

/// How many splits one batch pulls from a source.
///
/// Bounded well below the wire limit so a slow task's queue drains between
/// batches instead of filling in one shot.
pub(crate) const DEFAULT_PUMP_BATCH_SIZE: usize = 256;

/// How long the pump waits when no source could make progress.
const IDLE_PUMP_BACKOFF: std::time::Duration = std::time::Duration::from_millis(2);

/// One typed scan's split source, with the plan node it feeds.
pub(crate) struct RoundSplitSource {
    pub(crate) plan_node_id: i32,
    /// The catalog generation this scan was frozen against. It travels with
    /// the source because one query may read two catalogs, and every split
    /// this source produces must be stamped with its own generation.
    pub(crate) catalog: CatalogHandle,
    pub(crate) source: Box<dyn TypedConnectorSplitSource>,
}

/// The per-round owner of every split source and the driver that drains them.
pub(crate) struct RoundSplitAssignment {
    driver: SplitAssignmentDriver,
    sources: Vec<RoundSplitSource>,
    closed: Arc<AtomicBool>,
}

impl RoundSplitAssignment {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        transport: Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
        sources: Vec<RoundSplitSource>,
    ) -> Self {
        let catalogs = sources
            .iter()
            .map(|source| (source.plan_node_id, source.catalog.clone()))
            .collect();
        Self {
            driver: SplitAssignmentDriver::new(
                execution_id,
                catalogs,
                transport,
                tasks,
                max_queued_splits_per_task,
            ),
            sources,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A handle that closes this round's assignment from another thread.
    ///
    /// Cancellation reaches the coordinator on the statement thread while the
    /// pump may be blocked in a batch request, so the stop signal has to be
    /// observable without holding the round.
    pub(crate) fn stop_handle(&self) -> RoundSplitAssignmentStop {
        RoundSplitAssignmentStop {
            closed: Arc::clone(&self.closed),
        }
    }

    /// Drain every source until each plan node has sent its terminal marker.
    ///
    /// A source that yields nothing right now is retried after the other
    /// sources get a turn, so one slow enumeration cannot starve the rest.
    pub(crate) fn pump_to_completion(&mut self) -> Result<(), SplitAssignmentDriverError> {
        while !self.closed.load(Ordering::Acquire) {
            let mut progressed = false;
            let mut pending = false;
            for index in 0..self.sources.len() {
                if self.closed.load(Ordering::Acquire) {
                    break;
                }
                let plan_node_id = self.sources[index].plan_node_id;
                if self.driver.is_terminal_for(plan_node_id) {
                    continue;
                }
                pending = true;
                if self.driver.is_backpressured(plan_node_id) {
                    continue;
                }
                let source = self.sources[index].source.as_mut();
                if self
                    .driver
                    .pump(plan_node_id, source, DEFAULT_PUMP_BATCH_SIZE)?
                {
                    progressed = true;
                }
            }
            if !pending {
                return Ok(());
            }
            if !progressed {
                // Either every remaining task is at its queue ceiling, or every
                // source had nothing right now. Both resolve on their own, so
                // wait briefly rather than spin a core. The statement deadline
                // and the stop handle are what end a round that never
                // progresses; this loop deliberately has no deadline of its
                // own, because inventing one would cancel a legitimately slow
                // enumeration.
                std::thread::sleep(IDLE_PUMP_BACKOFF);
            }
        }
        Ok(())
    }

    /// Idempotent. Closes the driver and every source exactly once.
    pub(crate) fn close(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.driver.close();
        for entry in &mut self.sources {
            // A source close may race an outstanding batch; the connector
            // contract makes it idempotent, and a batch that already completed
            // normally may still be returned to whoever asked for it.
            let _ = entry.source.close();
        }
    }
}

impl Drop for RoundSplitAssignment {
    fn drop(&mut self) {
        self.close();
    }
}

/// Closes a round's split assignment from another thread.
#[derive(Clone)]
pub(crate) struct RoundSplitAssignmentStop {
    closed: Arc<AtomicBool>,
}

impl RoundSplitAssignmentStop {
    /// Ask the pump to stop. The round still closes its sources on drop; this
    /// only makes the pump notice without waiting for a batch to return.
    pub(crate) fn stop(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use novarocks_proto::FieldPath;
    use novarocks_proto::connector_read::ValidatedConnectorSplit;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::ConnectorSplitBatch;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_types::{AttemptId, QueryId, UniqueId};

    use super::super::transport::{AcceptedPlanNode, TaskUpdateOutcome, TaskUpdateTransportError};
    use super::*;

    #[derive(Default)]
    struct CountingTransport {
        delivered: Mutex<Vec<(i32, usize, bool)>>,
    }

    impl TaskUpdateTransport for CountingTransport {
        fn send(
            &self,
            _execution_id: QueryExecutionId,
            _target: &AssignmentTarget,
            _fragment_instance_id: UniqueId,
            assignments: Vec<dto::SplitAssignment>,
        ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
            let mut delivered = self.delivered.lock().expect("transport lock");
            let mut accepted = Vec::new();
            for assignment in &assignments {
                delivered.push((
                    assignment.plan_node_id,
                    assignment.splits.len(),
                    assignment.no_more_splits,
                ));
                accepted.push(AcceptedPlanNode {
                    plan_node_id: assignment.plan_node_id,
                    accepted_through_sequence: assignment
                        .splits
                        .last()
                        .map(|split| split.sequence_id)
                        .unwrap_or_default(),
                    no_more_splits: assignment.no_more_splits,
                    queued_splits: 0,
                });
            }
            Ok(TaskUpdateOutcome::Accepted(accepted))
        }
    }

    struct ScriptedSource {
        batches: std::collections::VecDeque<ConnectorSplitBatch<ValidatedConnectorSplit>>,
        closed: Arc<AtomicBool>,
    }

    impl TypedConnectorSplitSource for ScriptedSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &novarocks_proto::connector_read::WireDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ValidatedConnectorSplit>, ConnectorError> {
            if self.closed.load(Ordering::Acquire) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "split source is closed",
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
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn validated_split() -> ValidatedConnectorSplit {
        let raw = dto::ConnectorSplit {
            split_weight_raw: 100,
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

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(4, 4), AttemptId::new(1).expect("attempt"))
            .expect("execution id")
    }

    fn round(
        transport: Arc<CountingTransport>,
        sources: Vec<RoundSplitSource>,
        plan_nodes: &[i32],
    ) -> RoundSplitAssignment {
        let mut tasks = BTreeMap::new();
        for plan_node_id in plan_nodes {
            tasks.insert(
                *plan_node_id,
                vec![AssignmentTarget {
                    backend_idx: 0,
                    fragment_instance_id: UniqueId::new(1, 1),
                }],
            );
        }
        RoundSplitAssignment::new(execution_id(), transport, tasks, 1024, sources)
    }

    fn scripted(
        plan_node_id: i32,
        batches: Vec<ConnectorSplitBatch<ValidatedConnectorSplit>>,
    ) -> RoundSplitSource {
        RoundSplitSource {
            plan_node_id,
            catalog: CatalogHandle::new("ice", [1; 16]),
            source: Box::new(ScriptedSource {
                batches: batches.into_iter().collect(),
                closed: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    #[test]
    fn every_source_is_drained_and_every_plan_node_reaches_its_terminal_marker() {
        let transport = Arc::new(CountingTransport::default());
        let mut round = round(
            Arc::clone(&transport),
            vec![
                scripted(
                    1,
                    vec![
                        ConnectorSplitBatch::new(vec![validated_split()], false),
                        ConnectorSplitBatch::new(vec![validated_split()], true),
                    ],
                ),
                scripted(2, vec![ConnectorSplitBatch::new(Vec::new(), true)]),
            ],
            &[1, 2],
        );
        round.pump_to_completion().expect("pump");
        let delivered = transport.delivered.lock().expect("transport lock");
        assert_eq!(
            delivered
                .iter()
                .filter(|(node, _, terminal)| *node == 1 && *terminal)
                .count(),
            1
        );
        assert_eq!(
            delivered
                .iter()
                .filter(|(node, _, terminal)| *node == 2 && *terminal)
                .count(),
            1
        );
        assert_eq!(
            delivered
                .iter()
                .filter(|(node, splits, _)| *node == 1 && *splits > 0)
                .count(),
            2
        );
    }

    #[test]
    fn a_slow_source_does_not_starve_the_others() {
        // The first source needs three turns; the second finishes on its
        // first. Round-robin means the second is not left waiting for the
        // first to finish.
        let transport = Arc::new(CountingTransport::default());
        let mut round = round(
            Arc::clone(&transport),
            vec![
                scripted(
                    1,
                    vec![
                        ConnectorSplitBatch::new(vec![validated_split()], false),
                        ConnectorSplitBatch::new(vec![validated_split()], false),
                        ConnectorSplitBatch::new(Vec::new(), true),
                    ],
                ),
                scripted(
                    2,
                    vec![ConnectorSplitBatch::new(vec![validated_split()], true)],
                ),
            ],
            &[1, 2],
        );
        round.pump_to_completion().expect("pump");
        let delivered = transport.delivered.lock().expect("transport lock");
        let first_node_two = delivered
            .iter()
            .position(|(node, _, _)| *node == 2)
            .expect("node 2 delivered");
        assert!(
            first_node_two <= 1,
            "node 2 waited until index {first_node_two}"
        );
    }

    #[test]
    fn stopping_the_round_ends_the_pump_and_closes_every_source() {
        let transport = Arc::new(CountingTransport::default());
        let mut round = round(
            Arc::clone(&transport),
            vec![scripted(
                1,
                vec![ConnectorSplitBatch::new(vec![validated_split()], false)],
            )],
            &[1],
        );
        let stop = round.stop_handle();
        stop.stop();
        round.pump_to_completion().expect("stopped pump");
        assert!(stop.is_stopped());
        assert!(transport.delivered.lock().expect("lock").is_empty());
        round.close();
        round.close();
    }

    #[test]
    fn closing_is_idempotent_and_happens_on_drop() {
        let transport = Arc::new(CountingTransport::default());
        let closed = Arc::new(AtomicBool::new(false));
        let source = RoundSplitSource {
            plan_node_id: 1,
            catalog: CatalogHandle::new("ice", [1; 16]),
            source: Box::new(ScriptedSource {
                batches: std::collections::VecDeque::new(),
                closed: Arc::clone(&closed),
            }),
        };
        {
            let _round = round(transport, vec![source], &[1]);
        }
        assert!(closed.load(Ordering::Acquire));
    }
}
