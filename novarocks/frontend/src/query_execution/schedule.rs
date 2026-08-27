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

//! Role-neutral values produced by a query execution scheduler.
//!
//! Scheduling policy belongs to the frontend. Core only consumes this sealed
//! description while preparing protocol payloads and runtime-filter routes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use crate::common::backend_topology::LiveBackendTarget;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::QueryInitPlanHeader;
use novarocks_execution::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_proto_codec::lifecycle::{ExchangeRouteManifest, ScanRangeParams};
use novarocks_sql::plan_read::FragmentId;
use novarocks_types::UniqueId;

/// Placement information for one fragment instance.
#[derive(Clone, Debug)]
pub struct FragmentInstancePlacement {
    pub fragment_id: FragmentId,
    pub instance_index: usize,
    pub finst_id: UniqueId,
    pub backend_idx: usize,
    pub endpoint: RuntimeEndpoint,
    pub scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    pub destinations: Vec<FragmentDestination>,
    pub per_exch_num_senders: BTreeMap<i32, i32>,
}

/// A sealed, role-neutral scheduling result.
#[derive(Clone, Debug)]
pub struct SchedulingPlan {
    pub root_fragment_id: FragmentId,
    pub by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub root_finst_id: UniqueId,
    pub root_backend_idx: usize,
}

impl SchedulingPlan {
    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Scheduling test helper preserves per-fragment placement assertions."
    )]
    pub(crate) fn placements_for_fragment_for_test(
        &self,
        fragment_id: FragmentId,
    ) -> Option<&[FragmentInstancePlacement]> {
        self.by_fragment.get(&fragment_id).map(Vec::as_slice)
    }
}

/// Immutable lifecycle-only projection of a validated schedule.
///
/// It deliberately contains neither the mutable scheduling plan nor the
/// native plan tree.
#[derive(Clone, Debug)]
pub(crate) struct FragmentLifecycleProjection {
    pub(crate) instances_by_backend: BTreeMap<usize, BTreeSet<UniqueId>>,
    pub(crate) endpoints_by_backend: BTreeMap<usize, RuntimeEndpoint>,
    pub(crate) frozen_live_backends: BTreeMap<usize, LiveBackendTarget>,
    pub(crate) exchange_routes: Vec<ExchangeRouteManifest>,
    query_init_header: OnceLock<QueryInitPlanHeader>,
}

impl FragmentLifecycleProjection {
    pub(crate) fn new(
        instances_by_backend: BTreeMap<usize, BTreeSet<UniqueId>>,
        endpoints_by_backend: BTreeMap<usize, RuntimeEndpoint>,
        mut exchange_routes: Vec<ExchangeRouteManifest>,
    ) -> Self {
        exchange_routes.sort_by_key(|route| {
            let route = route.as_proto();
            let source = route.source_fragment_instance_id.as_ref();
            let destination = route.destination_fragment_instance_id.as_ref();
            (
                source.map_or(0, |id| id.hi),
                source.map_or(0, |id| id.lo),
                destination.map_or(0, |id| id.hi),
                destination.map_or(0, |id| id.lo),
                route.destination_node_id,
                route.sender_ordinal,
                route.sender_count,
            )
        });
        Self {
            instances_by_backend,
            endpoints_by_backend,
            frozen_live_backends: BTreeMap::new(),
            exchange_routes,
            query_init_header: OnceLock::new(),
        }
    }

    pub(crate) fn freeze_query_init_header(
        &self,
        candidate: QueryInitPlanHeader,
    ) -> Result<(), DistributedQueryError> {
        let frozen = self.query_init_header.get_or_init(|| candidate);
        if *frozen != candidate {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                format!(
                    "query initialization header differs from first construction for execution {:?}",
                    frozen.execution_id()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn with_frozen_live_backends(
        mut self,
        live_backends: Vec<LiveBackendTarget>,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "lifecycle projection requires a nonempty frozen live-backend topology",
            ));
        }
        let mut endpoints = BTreeSet::new();
        let mut process_ids = BTreeSet::new();
        for target in live_backends {
            let process_id = target.process_id().map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!(
                        "lifecycle projection backend {} has invalid process identity: {error}",
                        target.backend_idx(),
                    ),
                )
            })?;
            let endpoint = target.endpoint().map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!(
                        "lifecycle projection backend {} has invalid endpoint: {error}",
                        target.backend_idx(),
                    ),
                )
            })?;
            if !process_ids.insert(process_id) {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("lifecycle projection repeats backend process identity {process_id}"),
                ));
            }
            if !endpoints.insert(endpoint.clone()) {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("lifecycle projection repeats endpoint {}", endpoint),
                ));
            }
            let backend_idx = target.backend_idx();
            if self
                .frozen_live_backends
                .insert(backend_idx, target)
                .is_some()
            {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("lifecycle projection repeats backend {}", backend_idx),
                ));
            }
        }
        for (&backend_idx, endpoint) in &self.endpoints_by_backend {
            let target = self.frozen_live_backends.get(&backend_idx).ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("scheduled backend {backend_idx} is absent from frozen topology"),
                )
            })?;
            let target_endpoint = target.endpoint().map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("scheduled backend {backend_idx} has invalid frozen endpoint: {error}"),
                )
            })?;
            if target_endpoint != *endpoint {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!(
                        "scheduled backend {backend_idx} endpoint differs from frozen topology"
                    ),
                ));
            }
        }
        Ok(self)
    }
}
