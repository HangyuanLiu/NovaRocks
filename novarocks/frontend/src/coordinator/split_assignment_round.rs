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

//! The coordinator's per-round split-assignment worker.
//!
//! A scan blocks until splits arrive, and the statement thread blocks fetching
//! results, so the pump cannot share either. It runs on its own thread for the
//! life of one execution round, and the guard below closes it on every exit
//! path — success, failure, cancellation, or timeout — because a round that
//! left its sources open would leak a connector's enumeration state past the
//! attempt that owned it.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto::lifecycle::QueryExecutionId;

use crate::query_execution::connector_domain::CatalogHandle;
use crate::query_execution::schedule::SchedulingPlan;
use crate::query_execution::split_assignment::{
    AssignmentTarget, RoundSplitAssignment, RoundSplitAssignmentStop, RoundSplitSource,
    SplitAssignmentDriverError, TaskUpdateTransport,
};
use novarocks_sql::plan_read::FragmentId;

/// How many splits one task may hold before the driver stops pulling for it.
const DEFAULT_MAX_QUEUED_SPLITS_PER_TASK: u64 = 4096;

/// The admitted tasks that will read each typed scan node.
///
/// Derived from the schedule rather than from placed splits: with runtime
/// assignment every instance of a fragment that owns the scan node is a
/// legitimate destination, and deriving from placement would silently exclude
/// an instance that happened to receive nothing at planning time.
pub(crate) fn assignment_targets(
    schedule: &SchedulingPlan,
    scan_nodes: &[(FragmentId, i32)],
) -> BTreeMap<i32, Vec<AssignmentTarget>> {
    let mut targets: BTreeMap<i32, Vec<AssignmentTarget>> = BTreeMap::new();
    for &(fragment_id, plan_node_id) in scan_nodes {
        let Some(placements) = schedule.by_fragment.get(&fragment_id) else {
            continue;
        };
        let entry = targets.entry(plan_node_id).or_default();
        for placement in placements {
            entry.push(AssignmentTarget {
                backend_idx: placement.backend_idx,
                fragment_instance_id: placement.finst_id,
            });
        }
    }
    targets
}

/// Every backend this round may deliver a task update to.
pub(crate) fn assignment_endpoints(schedule: &SchedulingPlan) -> Vec<(usize, RuntimeEndpoint)> {
    let mut endpoints: BTreeMap<usize, RuntimeEndpoint> = BTreeMap::new();
    for placements in schedule.by_fragment.values() {
        for placement in placements {
            endpoints
                .entry(placement.backend_idx)
                .or_insert_with(|| placement.endpoint.clone());
        }
    }
    endpoints.into_iter().collect()
}

/// A running round's split-assignment worker.
///
/// Dropping it stops the pump and closes every source, so no exit path has to
/// remember to.
pub(crate) struct SplitAssignmentRoundGuard {
    stop: RoundSplitAssignmentStop,
    worker: Option<std::thread::JoinHandle<Result<(), SplitAssignmentDriverError>>>,
}

impl SplitAssignmentRoundGuard {
    /// Start pumping. Returns `None` when this round has no typed scan, so a
    /// query that reads nothing through a connector starts no thread.
    pub(crate) fn start(
        execution_id: QueryExecutionId,
        catalog: CatalogHandle,
        transport: Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        sources: Vec<RoundSplitSource>,
    ) -> Option<Self> {
        if sources.is_empty() {
            return None;
        }
        let mut assignment = RoundSplitAssignment::new(
            execution_id,
            catalog,
            transport,
            tasks,
            DEFAULT_MAX_QUEUED_SPLITS_PER_TASK,
            sources,
        );
        let stop = assignment.stop_handle();
        let worker = std::thread::Builder::new()
            .name(format!(
                "split-assign-{:x}-{:x}-{}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get()
            ))
            .spawn(move || {
                let result = assignment.pump_to_completion();
                // The round owns its sources; closing here rather than only on
                // drop means a failed pump releases them immediately instead of
                // at an unpredictable later moment.
                assignment.close();
                result
            })
            .ok()?;
        Some(Self {
            stop,
            worker: Some(worker),
        })
    }

    /// Stop the pump and wait for it, returning what it ended with.
    ///
    /// A pump that already finished normally returns `Ok`; one stopped early
    /// also returns `Ok`, because an interrupted round is not itself a fault.
    /// Only a real assignment failure surfaces as an error.
    pub(crate) fn finish(mut self) -> Result<(), SplitAssignmentDriverError> {
        self.stop.stop();
        match self.worker.take() {
            Some(worker) => worker.join().unwrap_or(Ok(())),
            None => Ok(()),
        }
    }
}

impl Drop for SplitAssignmentRoundGuard {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks_types::{AttemptId, QueryId, UniqueId};

    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(9, 9), AttemptId::new(1).expect("attempt"))
            .expect("execution id")
    }

    #[test]
    fn a_round_with_no_typed_scan_starts_no_worker() {
        struct NeverCalled;
        impl TaskUpdateTransport for NeverCalled {
            fn send(
                &self,
                _execution_id: QueryExecutionId,
                _target: &AssignmentTarget,
                _fragment_instance_id: UniqueId,
                _assignments: Vec<novarocks_proto_models::connector_read::SplitAssignment>,
            ) -> Result<
                crate::query_execution::split_assignment::TaskUpdateOutcome,
                crate::query_execution::split_assignment::TaskUpdateTransportError,
            > {
                panic!("a round with no source must never send");
            }
        }
        assert!(
            SplitAssignmentRoundGuard::start(
                execution_id(),
                CatalogHandle::new("ice", [1; 16]),
                Arc::new(NeverCalled),
                BTreeMap::new(),
                Vec::new(),
            )
            .is_none()
        );
    }
}
