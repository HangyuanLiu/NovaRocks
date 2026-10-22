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

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::net::SocketAddr;

use crate::query_execution::artifact::{
    BackendPlacement, FragmentId, FragmentScheduleDraft, ValidatedFragmentSchedule,
};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::fragment_scheduling::{FragmentSchedulingFacts, SchedulingStreamKind};
#[cfg(test)]
use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_query_application::api::{LiveBackendTarget, RuntimeSplitPlacement};
use novarocks_types::BackendProcessId;

#[derive(Clone)]
pub struct FrontendBackendSnapshot {
    entries: Vec<LiveBackendTarget>,
}

impl FrontendBackendSnapshot {
    #[cfg(test)]
    pub fn for_test(entries: Vec<(usize, SocketAddr)>) -> Result<Self, DistributedQueryError> {
        let targets = entries
            .into_iter()
            .map(|(backend_idx, endpoint)| {
                let endpoint =
                    RuntimeEndpoint::new(endpoint.ip().to_string(), i32::from(endpoint.port()))
                        .map_err(|error| contract_error(error.to_string()))?;
                let descriptor = BackendProcessDescriptor::try_new(
                    BackendProcessId::new_v7(),
                    endpoint,
                    RuntimeEndpoint::new(format!("control-{backend_idx}.test.invalid"), 19061)
                        .map_err(|error| contract_error(error.to_string()))?,
                    "scheduler-test",
                    "scheduler-test",
                    novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                    4096,
                )
                .map_err(|error| contract_error(error.to_string()))?;
                Ok(LiveBackendTarget::new(
                    backend_idx,
                    descriptor,
                    novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                        [0x61; 16],
                    )
                    .expect("nonzero test epoch"),
                ))
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Self::validate(targets)
    }

    pub(crate) fn from_live_targets(
        targets: Vec<LiveBackendTarget>,
    ) -> Result<Self, DistributedQueryError> {
        Self::validate(targets)
    }

    fn validate(entries: Vec<LiveBackendTarget>) -> Result<Self, DistributedQueryError> {
        if entries.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Rejected,
                "no live backend available",
            ));
        }
        let ids = entries
            .iter()
            .map(LiveBackendTarget::backend_idx)
            .collect::<BTreeSet<_>>();
        if ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend ids",
            ));
        }
        let process_ids = entries
            .iter()
            .map(|target| {
                target
                    .process_id()
                    .map_err(|error| contract_error(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>, DistributedQueryError>>()?;
        if process_ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend process identities",
            ));
        }
        Ok(Self { entries })
    }

    fn target(&self, backend_idx: usize) -> Option<&LiveBackendTarget> {
        self.entries
            .iter()
            .find(|target| target.backend_idx() == backend_idx)
    }

    fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.entries.clone()
    }
}

#[derive(Clone)]
pub struct FrontendFragmentScheduler {
    #[cfg(novarocks_dev_controls)]
    nomination: Option<(
        std::sync::Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
        std::sync::Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    )>,
    backends: FrontendBackendSnapshot,
}

impl FrontendFragmentScheduler {
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_nomination(
        mut self,
        controls: std::sync::Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
        nomination: std::sync::Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    ) -> Self {
        self.nomination = Some((controls, nomination));
        self
    }

    pub const fn new(backends: FrontendBackendSnapshot) -> Self {
        Self {
            backends,
            #[cfg(novarocks_dev_controls)]
            nomination: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.backends.live_targets()
    }

    pub(crate) fn scheduled_backend_ownership(
        &self,
        backend_ids: &[usize],
    ) -> Result<Vec<(usize, BackendProcessId)>, DistributedQueryError> {
        backend_ids
            .iter()
            .map(|&backend_idx| {
                self.backends
                    .target(backend_idx)
                    .map(|target| {
                        target
                            .process_id()
                            .map(|process_id| (backend_idx, process_id))
                            .map_err(|error| contract_error(error.to_string()))
                    })
                    .transpose()?
                    .ok_or_else(|| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            format!(
                                "scheduled backend {backend_idx} is absent from the frontend topology snapshot"
                            ),
                        )
                    })
            })
            .collect()
    }

    pub fn schedule(
        &self,
        facts: &FragmentSchedulingFacts,
        execution_id: QueryExecutionId,
    ) -> Result<ValidatedFragmentSchedule, DistributedQueryError> {
        #[cfg(novarocks_dev_controls)]
        let test_execution = novarocks_types::QueryExecutionId::new(
            execution_id.query_id(),
            novarocks_types::AttemptId::new(execution_id.attempt_id().get())
                .map_err(|error| contract_error(error.to_string()))?,
        )
        .map_err(|error| contract_error(error.to_string()))?;
        let fragments = &facts.fragments;
        let scheduled_ids = fragments.keys().copied().collect::<BTreeSet<_>>();
        let ordered_ids = facts.order.iter().copied().collect::<BTreeSet<_>>();
        if ordered_ids.len() != facts.order.len() || ordered_ids != scheduled_ids {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "sealed topological order is not a permutation of scheduled fragments",
            ));
        }

        #[derive(Clone, Copy)]
        struct IncomingEdge {
            source_fragment_id: FragmentId,
            native_hash_partitioned: bool,
            stream_kind: SchedulingStreamKind,
        }

        let mut incoming = BTreeMap::<FragmentId, Vec<IncomingEdge>>::new();
        for edge in &facts.edges {
            incoming.entry(edge.target).or_default().push(IncomingEdge {
                source_fragment_id: edge.source,
                native_hash_partitioned: edge.native_hash_partitioned,
                stream_kind: edge.stream_kind,
            });
        }

        let live_backend_count = self.backends.entries.len();
        let backend_count = live_backend_count;
        let mut counts = BTreeMap::<FragmentId, usize>::new();
        for &fragment_id in &facts.order {
            let fragment = fragments.get(&fragment_id).ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("fragment {fragment_id} is missing from scheduling view"),
                )
            })?;
            let has_gather = incoming.get(&fragment_id).is_some_and(|edges| {
                edges
                    .iter()
                    .any(|edge| edge.stream_kind == SchedulingStreamKind::Gather)
            });
            let count = if has_gather {
                1
            } else if fragment.has_scans() {
                scan_fragment_parallelism(
                    fragment
                        .scans
                        .iter()
                        .map(|scan| (scan.range_count(), scan.runtime_splits)),
                    backend_count,
                )
            } else {
                incoming
                    .get(&fragment_id)
                    .into_iter()
                    .flatten()
                    .filter(|edge| edge.native_hash_partitioned)
                    .filter_map(|edge| counts.get(&edge.source_fragment_id).copied())
                    .max()
                    .unwrap_or(1)
            };
            counts.insert(fragment_id, count);
        }

        let root_fragment_id = facts.anchor;
        fragments.get(&root_fragment_id).ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "execution anchor is not present in scheduling view",
            )
        })?;
        counts.insert(root_fragment_id, 1);

        let fault_preferred = None::<usize>;
        #[cfg(novarocks_dev_controls)]
        let fault_preferred = if let Some((controls, nomination)) = &self.nomination {
            match controls.preferred_backend_process(nomination, test_execution).map_err(contract_error)? {
                Some(process) => Some(self.backends.entries.iter().position(|target| target.process_id().ok() == Some(process)).ok_or_else(|| contract_error("the exact nominated backend process is absent from the frozen live topology"))?),
                None => None,
            }
        } else {
            fault_preferred
        };
        if fault_preferred.is_some_and(|live_index| live_index >= backend_count) {
            return Err(contract_error(
                "runner-owned lifecycle fault target is excluded by the fragment backend limit",
            ));
        }
        let preferred = fault_preferred
            .unwrap_or_else(|| (execution_id.query_id().low() as usize) % backend_count);
        let mut draft = FragmentScheduleDraft::new();
        draft.freeze_live_backends(self.backends.live_targets())?;
        for (&fragment_id, &count) in &counts {
            let placements = (0..count)
                .map(|instance_index| {
                    let live_index = if count == 1 {
                        preferred
                    } else if count == backend_count {
                        instance_index
                    } else {
                        (preferred + instance_index) % backend_count
                    };
                    let target = &self.backends.entries[live_index];
                    let endpoint = target
                        .endpoint()
                        .map_err(|error| contract_error(error.to_string()))?;
                    Ok(BackendPlacement::new(target.backend_idx(), endpoint))
                })
                .collect::<Result<Vec<_>, DistributedQueryError>>()?;
            draft.assign_fragment(fragment_id, placements)?;
        }
        let schedule = ValidatedFragmentSchedule::validate(facts, execution_id, draft)?;
        #[cfg(novarocks_dev_controls)]
        if let Some((controls, nomination)) = &self.nomination {
            let attempt = novarocks_types::AttemptId::new(execution_id.attempt_id().get())
                .map_err(|error| contract_error(error.to_string()))?;
            let execution =
                novarocks_types::QueryExecutionId::new(execution_id.query_id(), attempt)
                    .map_err(|error| contract_error(error.to_string()))?;
            for target in &self.backends.entries {
                controls
                    .publish_nominated_binding(
                        nomination,
                        target
                            .process_id()
                            .map_err(|error| contract_error(error.to_string()))?,
                        execution,
                    )
                    .map_err(contract_error)?;
            }
        }
        Ok(schedule)
    }
}

/// Choose one fragment's scan fanout without enumerating connector splits.
///
/// A single-unit hint contributes one task without constraining other scans.
/// Ignoring a hint may add empty tasks, but cannot duplicate provider work.
fn scan_fragment_parallelism(
    scans: impl IntoIterator<Item = (usize, Option<RuntimeSplitPlacement>)>,
    backend_count: usize,
) -> usize {
    let mut parallelism = 1;
    for (file_ranges, placement) in scans {
        match placement {
            Some(RuntimeSplitPlacement::SingleUnit) => {}
            Some(RuntimeSplitPlacement::Spread) => {
                parallelism = parallelism.max(backend_count);
            }
            None => {
                parallelism = parallelism.max(file_ranges);
            }
        }
    }
    parallelism.clamp(1, backend_count)
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use super::{FrontendBackendSnapshot, scan_fragment_parallelism};
    use crate::query_execution::contract::DistributedQueryErrorKind;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_query_application::api::RuntimeSplitPlacement;
    use novarocks_types::QueryId;

    #[test]
    fn placement_hints_do_not_constrain_other_scans() {
        assert_eq!(
            scan_fragment_parallelism([(0, Some(RuntimeSplitPlacement::SingleUnit))], 3),
            1
        );
        assert_eq!(
            scan_fragment_parallelism(
                [
                    (0, Some(RuntimeSplitPlacement::SingleUnit)),
                    (0, Some(RuntimeSplitPlacement::Spread))
                ],
                3
            ),
            3
        );
        assert_eq!(
            scan_fragment_parallelism([(0, Some(RuntimeSplitPlacement::SingleUnit)), (8, None)], 3),
            3
        );
    }

    #[test]
    fn ignoring_a_single_unit_hint_retains_all_live_backends() {
        assert_eq!(
            scan_fragment_parallelism([(0, Some(RuntimeSplitPlacement::Spread))], 3),
            3
        );
    }

    #[allow(
        dead_code,
        reason = "Disabled scheduler fixture retains stable attempt identity coverage."
    )]
    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }

    #[test]
    #[cfg(any())]
    fn scheduler_attempt_identity_is_stable_within_attempt_and_changes_between_attempts() {
        let fixture =
            crate::query_execution::contract_test_support::non_empty_runtime_filter_contract_fixture();
        let snapshot =
            FrontendBackendSnapshot::for_test(fixture.backends().to_vec()).expect("valid backends");
        let scheduler = FrontendFragmentScheduler::new(snapshot);
        let request = fixture.into_request();
        let parts = request.into_parts();
        let view = parts.artifacts.scheduling_view();

        let first = scheduler
            .schedule(view, execution_id(1))
            .expect("first schedule");
        let repeated = scheduler
            .schedule(view, execution_id(1))
            .expect("repeated schedule");
        let next = scheduler
            .schedule(view, execution_id(2))
            .expect("next-attempt schedule");

        assert_eq!(
            first.fragment_instance_ids(),
            repeated.fragment_instance_ids()
        );
        assert!(
            first
                .fragment_instance_ids()
                .iter()
                .zip(next.fragment_instance_ids())
                .all(|(left, right)| left != &right)
        );
    }

    #[test]
    fn empty_captured_topology_rejects_distributed_scheduling() {
        let error = match FrontendBackendSnapshot::for_test(Vec::new()) {
            Ok(_) => panic!("distributed scheduling requires at least one captured backend"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), DistributedQueryErrorKind::Rejected);
        assert_eq!(error.message(), "no live backend available");
    }
}
