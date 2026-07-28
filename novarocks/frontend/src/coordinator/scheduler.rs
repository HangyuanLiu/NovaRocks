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
use std::net::SocketAddr;

use novarocks::query_execution::artifact::{
    BackendPlacement, FragmentId, FragmentScheduleDraft, FragmentSchedulingView,
    SchedulingStreamKind, ValidatedFragmentSchedule,
};
use novarocks::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, QueryId,
};

pub struct FrontendBackendSnapshot {
    entries: Vec<(usize, SocketAddr)>,
}

impl FrontendBackendSnapshot {
    pub fn new(entries: Vec<(usize, SocketAddr)>) -> Result<Self, DistributedQueryError> {
        if entries.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Rejected,
                "no live backend available",
            ));
        }
        let ids = entries.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
        if ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend ids",
            ));
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[(usize, SocketAddr)] {
        &self.entries
    }
}

pub struct FrontendFragmentScheduler {
    backends: FrontendBackendSnapshot,
}

impl FrontendFragmentScheduler {
    pub const fn new(backends: FrontendBackendSnapshot) -> Self {
        Self { backends }
    }

    pub fn schedule(
        &self,
        view: FragmentSchedulingView<'_>,
        query_id: QueryId,
    ) -> Result<ValidatedFragmentSchedule, DistributedQueryError> {
        let fragments = view
            .fragments()
            .map(|fragment| (fragment.fragment_id(), fragment))
            .collect::<BTreeMap<_, _>>();
        let scheduled_ids = fragments.keys().copied().collect::<BTreeSet<_>>();
        let ordered_ids = view
            .topological_order()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if ordered_ids.len() != view.topological_order().len() || ordered_ids != scheduled_ids {
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
        for edge in view.edges() {
            incoming
                .entry(edge.target_fragment_id())
                .or_default()
                .push(IncomingEdge {
                    source_fragment_id: edge.source_fragment_id(),
                    native_hash_partitioned: edge.is_native_hash_partitioned(),
                    stream_kind: edge.stream_kind(),
                });
        }

        let backend_count = self.backends.entries.len();
        let mut counts = BTreeMap::<FragmentId, usize>::new();
        for &fragment_id in view.topological_order() {
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
            } else if fragment.has_scan_nodes() {
                fragment
                    .scan_node_ids()
                    .iter()
                    .filter_map(|&node_id| fragment.scan_range_count(node_id))
                    .max()
                    .unwrap_or_default()
                    .clamp(1, backend_count)
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

        let root_fragment_id = view.execution_anchor();
        let root = fragments.get(&root_fragment_id).ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "execution anchor is not present in scheduling view",
            )
        })?;
        if !root.is_terminal_write() {
            counts.insert(root_fragment_id, 1);
        }

        let preferred = (query_id.low() as usize) % backend_count;
        let mut draft = FragmentScheduleDraft::new();
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
                    let (backend_idx, endpoint) = self.backends.entries[live_index];
                    BackendPlacement::new(backend_idx, endpoint)
                })
                .collect();
            draft.assign_fragment(fragment_id, placements)?;
        }
        ValidatedFragmentSchedule::validate(view, query_id, draft)
    }
}
