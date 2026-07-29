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

//! Opaque owned handoffs and neutral scheduling projections.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::Field;
use sha2::{Digest, Sha256};

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::backend::LiveBackendTarget;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, QueryId, ResolvedQueryOptions,
};
use crate::query_execution::fragment_transport::{
    ExpectedOutputSchemaView, FetchedQueryBatch, NativeFragmentEnvelope,
};
use crate::query_execution::lifecycle::{
    ExchangeRouteManifest, QueryExecutionId, QueryInitBarrier, QueryInitOptions,
    QueryLifecycleLease,
};
use crate::query_execution::preparation::{
    PreparedFragment, PreparedFragmentSchedulingView, PreparedFragmentSet, PreparedOutputColumn,
};
use crate::query_execution::schedule::{
    FragmentInstancePlacement, FragmentLifecycleProjection, SchedulingPlan,
};
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    FragmentEdgeKind, FragmentId as PlannerFragmentId, FragmentStreamKind, PartitionKind,
};

pub use crate::query_execution::runtime_filter::{
    RuntimeFilterAbortEnvelope, RuntimeFilterDeploymentDispatcher, RuntimeFilterDeploymentEpoch,
    RuntimeFilterDeploymentOptions, RuntimeFilterInstallBarrier, RuntimeFilterInstallEnvelope,
    RuntimeFilterInstallLease, RuntimeFilterInstallLeaseGuard, RuntimeFilterInstallPlan,
    RuntimeFilterParticipantInstallPlan, new_grpc_runtime_filter_deployment_dispatcher,
};

pub type FragmentId = u32;
pub type PlanNodeId = i32;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

static NEXT_HANDOFF_ID: AtomicU64 = AtomicU64::new(1);

/// The owned prepared/native pair. It has no public constructor, `Clone`, or
/// inverse `from_parts`, so artifacts from different sealed plans cannot be
/// recombined by a role crate.
pub struct PreparedDistributedQuery {
    handoff_id: u64,
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
}

impl PreparedDistributedQuery {
    pub(super) fn new(prepared: PreparedFragmentSet, native_bundle: NativeFragmentBundle) -> Self {
        Self {
            handoff_id: NEXT_HANDOFF_ID.fetch_add(1, Ordering::Relaxed),
            prepared,
            native_bundle,
        }
    }

    pub fn scheduling_view(&self) -> FragmentSchedulingView<'_> {
        FragmentSchedulingView {
            handoff_id: self.handoff_id,
            inner: self.prepared.scheduling_view(),
        }
    }

    pub fn bind_schedule(
        self,
        schedule: ValidatedFragmentSchedule,
    ) -> Result<ScheduleBoundDistributedQuery, DistributedQueryError> {
        if self.handoff_id != schedule.handoff_id {
            return Err(contract_error(
                "validated fragment schedule belongs to a different prepared query handoff",
            ));
        }
        Ok(ScheduleBoundDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule,
        })
    }
}

/// A core artifact bound to one validated schedule. This type deliberately has
/// no `assemble` method: query initialization and the frontend-owned control
/// readiness barrier must first produce `ControlReadyDistributedQuery`.
///
/// ```compile_fail
/// use novarocks::query_execution::artifact::ScheduleBoundDistributedQuery;
///
/// fn query_control_typestate_prevents_submission_before_ready(
///     scheduled: ScheduleBoundDistributedQuery,
/// ) {
///     let _ = scheduled.assemble();
/// }
/// ```
pub struct ScheduleBoundDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
}

impl ScheduleBoundDistributedQuery {
    pub fn initialize_query(
        self,
        options: QueryInitOptions,
        barrier: &dyn QueryInitBarrier,
    ) -> Result<ControlReadyDistributedQuery, DistributedQueryError> {
        if options.execution_id() != self.schedule.execution_id {
            return Err(contract_error(
                "query initialization execution id does not match validated schedule",
            ));
        }
        let runtime_filters = crate::query_execution::runtime_filter::compile_contribution_plan(
            options.execution_id(),
            self.prepared.runtime_filter_graph(),
            self.prepared.runtime_filter_join_progress(),
            self.prepared.scheduling_view().edges(),
            &self.schedule.inner,
            options.live_backends(),
            options.runtime_filter_worker_count(),
            options.runtime_filter_lifecycle(),
        )?;
        let plan = crate::query_execution::lifecycle::init_plan::compile_query_init_plan(
            self.schedule.lifecycle_projection(),
            runtime_filters,
            &options,
        )?;
        let query_lifecycle_lease = barrier.initialize_all(plan)?;
        Ok(ControlReadyDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            options,
            query_lifecycle_lease,
        })
    }

    /// Transitional Task 7 dependency for the production coordinator.
    ///
    /// This legacy RF-only path is deliberately not a query-lifecycle owner.
    pub fn prepare_legacy_runtime_filters(
        self,
        options: RuntimeFilterDeploymentOptions,
        barrier: &dyn RuntimeFilterInstallBarrier,
    ) -> Result<LegacyRuntimeFilterReadyDistributedQuery, DistributedQueryError> {
        let plan = crate::query_execution::runtime_filter::compile_install_plan(
            self.schedule.execution_id.query_id(),
            self.prepared.runtime_filter_graph(),
            self.prepared.runtime_filter_join_progress(),
            self.prepared.scheduling_view().edges(),
            &self.schedule.inner,
            options,
        )?;
        let runtime_filter_lease = barrier.install_all(plan)?;
        Ok(LegacyRuntimeFilterReadyDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            runtime_filter_lease,
        })
    }
}

/// The only query-control typestate that can assemble native submissions.
pub struct ControlReadyDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
    options: QueryInitOptions,
    query_lifecycle_lease: QueryLifecycleLease,
}

impl ControlReadyDistributedQuery {
    pub fn assemble(self) -> Result<PreparedNativeExecution, DistributedQueryError> {
        let context = match self.options.native_submission_context() {
            Ok(context) => context,
            Err(error) => {
                let kind = error.kind();
                let message = self
                    .query_lifecycle_lease
                    .abort_preserving(error.message().to_string());
                return Err(DistributedQueryError::new(kind, message));
            }
        };
        let assembled = assemble_native_execution(
            self.prepared,
            self.native_bundle,
            self.schedule.inner,
            self.schedule.execution_id,
            context,
        );
        match assembled {
            Ok(assembled) => Ok(PreparedNativeExecution {
                submissions: assembled.submissions,
                root_fetch: assembled.root_fetch,
                writer_registrations: assembled.writer_registrations,
                expected_output: assembled.expected_output,
                query_lifecycle_lease: self.query_lifecycle_lease,
            }),
            Err(error) => {
                let kind = error.kind();
                let message = self
                    .query_lifecycle_lease
                    .abort_preserving(error.message().to_string());
                Err(DistributedQueryError::new(kind, message))
            }
        }
    }
}

/// Transitional RF-only production handoff pending the Task 7 cutover.
///
/// It owns only a legacy runtime-filter lease and must never be treated as
/// evidence that query-wide Init/ControlReady completed.
pub struct LegacyRuntimeFilterReadyDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
    runtime_filter_lease: RuntimeFilterInstallLease,
}

impl LegacyRuntimeFilterReadyDistributedQuery {
    pub fn assemble(
        self,
        context: NativeSubmissionContext,
    ) -> Result<LegacyPreparedNativeExecution, DistributedQueryError> {
        if context.query_id != self.schedule.execution_id.query_id() {
            let error = contract_error(
                "native submission context query id does not match validated schedule",
            );
            let kind = error.kind();
            let message = self
                .runtime_filter_lease
                .abort_preserving(error.message().to_string());
            return Err(DistributedQueryError::new(kind, message));
        }
        let assembled = assemble_native_execution(
            self.prepared,
            self.native_bundle,
            self.schedule.inner,
            self.schedule.execution_id,
            context,
        );
        match assembled {
            Ok(assembled) => Ok(LegacyPreparedNativeExecution {
                submissions: assembled.submissions,
                root_fetch: assembled.root_fetch,
                writer_registrations: assembled.writer_registrations,
                expected_output: assembled.expected_output,
                runtime_filter_lease: self.runtime_filter_lease,
            }),
            Err(error) => {
                let kind = error.kind();
                let message = self
                    .runtime_filter_lease
                    .abort_preserving(error.message().to_string());
                Err(DistributedQueryError::new(kind, message))
            }
        }
    }
}

/// Immutable, scalar-only frontend scheduling projection.
#[derive(Clone, Copy)]
pub struct FragmentSchedulingView<'a> {
    handoff_id: u64,
    inner: PreparedFragmentSchedulingView<'a>,
}

impl<'a> FragmentSchedulingView<'a> {
    pub fn fragment_ids(self) -> impl ExactSizeIterator<Item = FragmentId> + 'a {
        self.inner.fragment_ids()
    }

    pub fn fragments(self) -> impl ExactSizeIterator<Item = SchedulingFragmentView<'a>> + 'a {
        self.inner
            .fragments()
            .map(move |fragment| SchedulingFragmentView {
                fragment,
                view: self.inner,
            })
    }

    pub fn topological_order(self) -> &'a [FragmentId] {
        self.inner.topological_order()
    }

    pub fn execution_anchor(self) -> FragmentId {
        self.inner.execution_anchor()
    }

    pub fn edges(self) -> impl ExactSizeIterator<Item = SchedulingEdgeView<'a>> + 'a {
        self.inner
            .edges()
            .iter()
            .map(|edge| SchedulingEdgeView { edge })
    }
}

#[derive(Clone, Copy)]
pub struct SchedulingFragmentView<'a> {
    fragment: &'a PreparedFragment,
    view: PreparedFragmentSchedulingView<'a>,
}

impl<'a> SchedulingFragmentView<'a> {
    pub fn fragment_id(self) -> FragmentId {
        self.fragment.fragment_id()
    }

    pub fn has_scan_nodes(self) -> bool {
        self.fragment.has_scan_nodes()
    }

    pub fn scan_node_ids(self) -> &'a [PlanNodeId] {
        self.fragment.scan_node_ids()
    }

    pub fn scan_range_count(self, node_id: PlanNodeId) -> Option<usize> {
        self.view
            .scan_ranges(self.fragment.fragment_id(), node_id)
            .map(<[_]>::len)
    }

    pub fn is_terminal_write(self) -> bool {
        self.fragment.execution_role().is_terminal_write()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

#[derive(Clone, Copy)]
pub struct SchedulingEdgeView<'a> {
    edge: &'a crate::sql::planner::distributed::FragmentEdge,
}

impl SchedulingEdgeView<'_> {
    pub fn source_fragment_id(self) -> FragmentId {
        self.edge.source_fragment_id
    }

    pub fn target_fragment_id(self) -> FragmentId {
        self.edge.target_fragment_id
    }

    pub fn target_exchange_node_id(self) -> PlanNodeId {
        self.edge.target_exchange_node_id
    }

    pub fn is_native_hash_partitioned(self) -> bool {
        matches!(self.edge.output_partition.kind, PartitionKind::Hash)
    }

    pub fn stream_kind(self) -> SchedulingStreamKind {
        let kind = match self.edge.edge_kind {
            FragmentEdgeKind::Stream => self.edge.stream_kind,
            FragmentEdgeKind::CteMulticast { .. } => FragmentStreamKind::Broadcast,
            FragmentEdgeKind::IcebergChangeStreamRouter { .. } => self.edge.stream_kind,
        };
        match kind {
            FragmentStreamKind::Gather => SchedulingStreamKind::Gather,
            FragmentStreamKind::Broadcast => SchedulingStreamKind::Broadcast,
            FragmentStreamKind::Partitioned => SchedulingStreamKind::Partitioned,
            FragmentStreamKind::Other => SchedulingStreamKind::Other,
        }
    }
}

/// A frontend decision for one instance. The native endpoint representation
/// remains core-private.
pub struct BackendPlacement {
    backend_idx: usize,
    endpoint: SocketAddr,
}

impl BackendPlacement {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr) -> Self {
        Self {
            backend_idx,
            endpoint,
        }
    }
}

/// Unvalidated frontend policy output.
pub struct FragmentScheduleDraft {
    by_fragment: BTreeMap<FragmentId, Vec<BackendPlacement>>,
    frozen_live_backends: Option<BTreeMap<usize, LiveBackendTarget>>,
}

impl FragmentScheduleDraft {
    pub fn new() -> Self {
        Self {
            by_fragment: BTreeMap::new(),
            frozen_live_backends: None,
        }
    }

    pub fn freeze_live_backends(
        &mut self,
        live_backends: Vec<LiveBackendTarget>,
    ) -> Result<(), DistributedQueryError> {
        if self.frozen_live_backends.is_some() {
            return Err(contract_error(
                "frontend schedule live-backend topology was frozen more than once",
            ));
        }
        if live_backends.is_empty() {
            return Err(contract_error(
                "frontend schedule requires a nonempty live-backend topology",
            ));
        }
        let mut by_backend = BTreeMap::new();
        let mut endpoints = BTreeSet::new();
        for target in live_backends {
            if target.start_epoch() == 0 {
                return Err(contract_error(format!(
                    "frontend schedule live backend {} has zero start epoch",
                    target.backend_idx()
                )));
            }
            if !endpoints.insert(target.endpoint()) {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats endpoint {}",
                    target.endpoint()
                )));
            }
            if by_backend.insert(target.backend_idx(), target).is_some() {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats backend {}",
                    target.backend_idx()
                )));
            }
        }
        self.frozen_live_backends = Some(by_backend);
        Ok(())
    }

    pub fn assign_fragment(
        &mut self,
        fragment_id: FragmentId,
        placements: Vec<BackendPlacement>,
    ) -> Result<(), DistributedQueryError> {
        if self.by_fragment.insert(fragment_id, placements).is_some() {
            return Err(contract_error(format!(
                "frontend schedule assigned fragment {fragment_id} more than once"
            )));
        }
        Ok(())
    }
}

impl Default for FragmentScheduleDraft {
    fn default() -> Self {
        Self::new()
    }
}

/// Core-validated schedule. It cannot be cloned, deconstructed, or created
/// without the immutable view from the same prepared handoff.
pub struct ValidatedFragmentSchedule {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    inner: SchedulingPlan,
    lifecycle: FragmentLifecycleProjection,
}

impl ValidatedFragmentSchedule {
    pub fn validate(
        view: FragmentSchedulingView<'_>,
        execution_id: QueryExecutionId,
        draft: FragmentScheduleDraft,
    ) -> Result<Self, DistributedQueryError> {
        let FragmentScheduleDraft {
            by_fragment: draft_by_fragment,
            frozen_live_backends,
        } = draft;
        let frozen_live_backends = frozen_live_backends.ok_or_else(|| {
            contract_error("frontend schedule did not freeze its live-backend topology")
        })?;
        let expected = view.fragment_ids().collect::<BTreeSet<_>>();
        let received = draft_by_fragment.keys().copied().collect::<BTreeSet<_>>();
        if expected != received {
            return Err(contract_error(format!(
                "frontend schedule fragment set mismatch: expected={expected:?}, received={received:?}"
            )));
        }

        let mut by_fragment = BTreeMap::new();
        for (fragment_id, placements) in draft_by_fragment {
            if placements.is_empty() {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} has no placements"
                )));
            }
            if placements.len() >= 1 << 16 {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} has too many placements"
                )));
            }
            let mut backend_ids = BTreeSet::new();
            let mut instances = placements
                .into_iter()
                .enumerate()
                .map(|(instance_index, placement)| {
                    if !backend_ids.insert(placement.backend_idx) {
                        return Err(contract_error(format!(
                            "frontend schedule fragment {fragment_id} repeats backend {}",
                            placement.backend_idx
                        )));
                    }
                    let frozen = frozen_live_backends
                        .get(&placement.backend_idx)
                        .ok_or_else(|| {
                            contract_error(format!(
                                "frontend schedule placement backend {} is absent from frozen topology",
                                placement.backend_idx
                            ))
                        })?;
                    if frozen.endpoint() != placement.endpoint {
                        return Err(contract_error(format!(
                            "frontend schedule placement backend {} endpoint {} differs from frozen topology endpoint {}",
                            placement.backend_idx, placement.endpoint, frozen.endpoint()
                        )));
                    }
                    Ok(FragmentInstancePlacement {
                        fragment_id,
                        instance_index,
                        finst_id: derive_fragment_instance_id(
                            execution_id,
                            fragment_id,
                            instance_index,
                        )?,
                        backend_idx: placement.backend_idx,
                        endpoint: RuntimeEndpoint::from_socket_addr(placement.endpoint),
                        scan_ranges: BTreeMap::new(),
                        destinations: Vec::new(),
                        per_exch_num_senders: BTreeMap::new(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let fragment = view.inner.fragment(fragment_id).ok_or_else(|| {
                contract_error(format!("prepared fragment {fragment_id} is missing"))
            })?;
            let instance_count = instances.len();
            for &node_id in fragment.scan_node_ids() {
                let ranges = view
                    .inner
                    .scan_ranges(fragment_id, node_id)
                    .ok_or_else(|| {
                        contract_error(format!(
                            "prepared scan ranges missing for fragment {fragment_id} node {node_id}"
                        ))
                    })?;
                for instance in &mut instances {
                    instance.scan_ranges.entry(node_id).or_default();
                }
                for (index, range) in ranges.iter().enumerate() {
                    instances[index % instance_count]
                        .scan_ranges
                        .entry(node_id)
                        .or_default()
                        .push(range.clone());
                }
            }
            let total_ranges = instances
                .iter()
                .flat_map(|instance| instance.scan_ranges.values())
                .map(Vec::len)
                .sum::<usize>();
            if total_ranges > 0
                && instances
                    .iter()
                    .any(|instance| instance.scan_ranges.values().all(Vec::is_empty))
            {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} creates an empty scan instance"
                )));
            }
            by_fragment.insert(fragment_id, instances);
        }

        let root_fragment_id = view.execution_anchor();
        let root = by_fragment
            .get(&root_fragment_id)
            .and_then(|placements| placements.first())
            .ok_or_else(|| contract_error("frontend schedule root has no placement"))?;
        let root_finst_id = root.finst_id;
        let root_backend_idx = root.backend_idx;
        let mut inner = SchedulingPlan {
            root_fragment_id,
            by_fragment,
            root_finst_id,
            root_backend_idx,
        };
        populate_destinations(&mut inner, view.inner.edges());
        populate_sender_counts(&mut inner, view.inner.edges());
        let lifecycle =
            build_fragment_lifecycle_projection(&inner, view.inner.edges(), frozen_live_backends)?;
        Ok(Self {
            handoff_id: view.handoff_id,
            execution_id,
            inner,
            lifecycle,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn backend_ids(&self) -> Vec<usize> {
        self.inner
            .by_fragment
            .values()
            .flatten()
            .map(|placement| placement.backend_idx)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn fragment_instance_ids(&self) -> Vec<UniqueId> {
        self.inner
            .by_fragment
            .values()
            .flatten()
            .map(|placement| placement.finst_id)
            .collect()
    }

    pub(crate) const fn lifecycle_projection(&self) -> &FragmentLifecycleProjection {
        &self.lifecycle
    }
}

const FRAGMENT_INSTANCE_ID_DOMAIN: &[u8] = b"novarocks.query-lifecycle.fragment-instance.v1\0";

fn derive_fragment_instance_id(
    execution_id: QueryExecutionId,
    fragment_id: FragmentId,
    instance_index: usize,
) -> Result<UniqueId, DistributedQueryError> {
    let instance_index = u64::try_from(instance_index)
        .map_err(|_| contract_error("fragment instance index exceeds u64 width"))?;
    let mut digest = Sha256::new();
    digest.update(FRAGMENT_INSTANCE_ID_DOMAIN);
    digest.update(execution_id.query_id().high().to_be_bytes());
    digest.update(execution_id.query_id().low().to_be_bytes());
    digest.update(execution_id.attempt_id().get().to_be_bytes());
    digest.update(fragment_id.to_be_bytes());
    digest.update(instance_index.to_be_bytes());
    let bytes = digest.finalize();
    let hi = i64::from_be_bytes(
        bytes[0..8]
            .try_into()
            .expect("SHA-256 prefix contains eight high bytes"),
    );
    let mut lo = i64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("SHA-256 prefix contains eight low bytes"),
    );
    if hi == 0 && lo == 0 {
        lo = 1;
    }
    Ok(UniqueId { hi, lo })
}

#[cfg(feature = "query-execution-contract-test-support")]
pub fn fragment_instance_id_for_contract_test(
    query_id: QueryId,
    fragment_id: FragmentId,
    instance_index: usize,
) -> UniqueId {
    let execution_id = QueryExecutionId::new(
        query_id,
        crate::query_execution::lifecycle::AttemptId::new(1)
            .expect("contract fixtures use a nonzero initial attempt"),
    )
    .expect("contract fixtures use a nonzero query id");
    derive_fragment_instance_id(execution_id, fragment_id, instance_index)
        .expect("contract fixture fragment identity is representable")
}

fn build_fragment_lifecycle_projection(
    schedule: &SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
    frozen_live_backends: BTreeMap<usize, LiveBackendTarget>,
) -> Result<FragmentLifecycleProjection, DistributedQueryError> {
    let mut instances_by_backend = BTreeMap::<usize, BTreeSet<UniqueId>>::new();
    let mut endpoints_by_backend = BTreeMap::<usize, RuntimeEndpoint>::new();
    for placement in schedule.by_fragment.values().flatten() {
        instances_by_backend
            .entry(placement.backend_idx)
            .or_default()
            .insert(placement.finst_id);
        match endpoints_by_backend.entry(placement.backend_idx) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(placement.endpoint.clone());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get() != &placement.endpoint =>
            {
                return Err(contract_error(format!(
                    "scheduled backend {} has inconsistent endpoints",
                    placement.backend_idx
                )));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }

    let mut exchange_routes = Vec::new();
    for edge in edges {
        let sources = schedule
            .by_fragment
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                contract_error(format!(
                    "exchange source fragment {} is absent from schedule",
                    edge.source_fragment_id
                ))
            })?;
        let destinations = schedule
            .by_fragment
            .get(&edge.target_fragment_id)
            .ok_or_else(|| {
                contract_error(format!(
                    "exchange destination fragment {} is absent from schedule",
                    edge.target_fragment_id
                ))
            })?;
        let sender_count = u32::try_from(sources.len())
            .map_err(|_| contract_error("exchange sender count exceeds u32 width"))?;
        for (sender_ordinal, source) in sources.iter().enumerate() {
            let sender_ordinal = u32::try_from(sender_ordinal)
                .map_err(|_| contract_error("exchange sender ordinal exceeds u32 width"))?;
            for destination in destinations {
                exchange_routes.push(
                    ExchangeRouteManifest::new(
                        source.finst_id,
                        destination.finst_id,
                        edge.target_exchange_node_id,
                        sender_ordinal,
                        sender_count,
                    )
                    .map_err(|error| contract_error(error.to_string()))?,
                );
            }
        }
    }
    FragmentLifecycleProjection::new(instances_by_backend, endpoints_by_backend, exchange_routes)
        .with_frozen_live_backends(frozen_live_backends.into_values().collect())
}

fn populate_destinations(
    schedule: &mut SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
) {
    for edge in edges {
        let destinations = schedule
            .by_fragment
            .get(&edge.target_fragment_id)
            .into_iter()
            .flatten()
            .map(|placement| {
                FragmentDestination::new(placement.finst_id, placement.endpoint.clone())
            })
            .collect::<Vec<_>>();
        if let Some(sources) = schedule.by_fragment.get_mut(&edge.source_fragment_id) {
            for source in sources {
                source.destinations.extend(destinations.iter().cloned());
            }
        }
    }
}

fn populate_sender_counts(
    schedule: &mut SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
) {
    for edge in edges {
        let upstream = schedule
            .by_fragment
            .get(&edge.source_fragment_id)
            .map(Vec::len)
            .unwrap_or_default() as i32;
        if let Some(targets) = schedule.by_fragment.get_mut(&edge.target_fragment_id) {
            for target in targets {
                *target
                    .per_exch_num_senders
                    .entry(edge.target_exchange_node_id)
                    .or_insert(0) += upstream;
            }
        }
    }
}

/// Owned core input for per-placement native submission assembly.
pub struct NativeSubmissionContext {
    pub(crate) query_id: QueryId,
    pub(crate) options: crate::runtime::query_options::QueryOptions,
    pub(crate) report_endpoint: RuntimeEndpoint,
    pub(crate) needs_fragment_status_report: bool,
}

impl NativeSubmissionContext {
    pub fn new(
        query_id: QueryId,
        options: &ResolvedQueryOptions,
        report_endpoint: crate::query_execution::backend::CoordinatorReportEndpoint,
        needs_fragment_status_report: bool,
    ) -> Self {
        Self {
            query_id,
            options: options.runtime_options().clone(),
            report_endpoint: report_endpoint.into_runtime_endpoint(),
            needs_fragment_status_report,
        }
    }
}

pub struct ValidatedNativeSubmission {
    backend_idx: usize,
    finst_id: UniqueId,
    envelope: NativeFragmentEnvelope,
}

impl ValidatedNativeSubmission {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.finst_id
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.envelope.execution_id()
    }

    pub fn into_envelope(self) -> NativeFragmentEnvelope {
        self.envelope
    }
}

pub struct RootFetchMetadata {
    fragment_id: FragmentId,
    backend_idx: usize,
    finst_id: UniqueId,
    uses_result_buffer: bool,
}

impl RootFetchMetadata {
    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.finst_id
    }

    pub const fn uses_result_buffer(&self) -> bool {
        self.uses_result_buffer
    }
}

pub(crate) struct WriterRegistration {
    pub(crate) query_id: UniqueId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
}

pub struct WriterRegistrationSet {
    registrations: Vec<WriterRegistration>,
}

impl WriterRegistrationSet {
    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    pub fn fragment_instance_ids(&self) -> Vec<UniqueId> {
        self.registrations
            .iter()
            .map(|registration| registration.fragment_instance_id)
            .collect()
    }

    pub fn writer_identities(&self) -> Vec<(UniqueId, i32)> {
        self.registrations
            .iter()
            .map(|registration| (registration.fragment_instance_id, registration.backend_num))
            .collect()
    }

    pub(crate) fn into_registrations(self) -> Vec<WriterRegistration> {
        self.registrations
    }
}

pub struct ExpectedOutputSchema {
    output_columns: Vec<PreparedOutputColumn>,
    chunk_schema: ChunkSchemaRef,
}

impl ExpectedOutputSchema {
    pub fn fetch_view(&self) -> ExpectedOutputSchemaView<'_> {
        ExpectedOutputSchemaView::new(&self.chunk_schema)
    }

    pub fn into_query_result(
        self,
        batches: Vec<FetchedQueryBatch>,
    ) -> Result<QueryResult, DistributedQueryError> {
        let chunks = batches
            .into_iter()
            .map(FetchedQueryBatch::into_chunk)
            .collect();
        let chunks = crate::query_execution::assembly::align_fetch_chunks_to_output_columns(
            chunks,
            &self.output_columns,
        )
        .map_err(contract_error)?;
        Ok(QueryResult {
            columns: self
                .output_columns
                .into_iter()
                .map(|column| QueryResultColumn {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                    logical_type: None,
                })
                .collect(),
            chunks,
        })
    }
}

pub struct PreparedNativeExecution {
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
    query_lifecycle_lease: QueryLifecycleLease,
}

struct AssembledNativeExecution {
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
}

impl PreparedNativeExecution {
    pub fn into_parts(self) -> PreparedNativeExecutionParts {
        PreparedNativeExecutionParts {
            submissions: self.submissions,
            root_fetch: self.root_fetch,
            writer_registrations: self.writer_registrations,
            expected_output: self.expected_output,
            query_lifecycle_lease: self.query_lifecycle_lease,
        }
    }
}

/// Consuming assembly output. No public constructor or inverse recombination
/// API exists.
pub struct PreparedNativeExecutionParts {
    pub submissions: Vec<ValidatedNativeSubmission>,
    pub root_fetch: RootFetchMetadata,
    pub writer_registrations: WriterRegistrationSet,
    pub expected_output: ExpectedOutputSchema,
    pub query_lifecycle_lease: QueryLifecycleLease,
}

/// Transitional Task 7 production result. It is intentionally distinct from
/// `PreparedNativeExecution` and cannot carry a query-wide lifecycle lease.
pub struct LegacyPreparedNativeExecution {
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
    runtime_filter_lease: RuntimeFilterInstallLease,
}

impl LegacyPreparedNativeExecution {
    pub fn into_parts(self) -> LegacyPreparedNativeExecutionParts {
        LegacyPreparedNativeExecutionParts {
            submissions: self.submissions,
            root_fetch: self.root_fetch,
            writer_registrations: self.writer_registrations,
            expected_output: self.expected_output,
            runtime_filter_lease: self.runtime_filter_lease,
        }
    }
}

pub struct LegacyPreparedNativeExecutionParts {
    pub submissions: Vec<ValidatedNativeSubmission>,
    pub root_fetch: RootFetchMetadata,
    pub writer_registrations: WriterRegistrationSet,
    pub expected_output: ExpectedOutputSchema,
    pub runtime_filter_lease: RuntimeFilterInstallLease,
}

fn assemble_native_execution(
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: SchedulingPlan,
    execution_id: QueryExecutionId,
    context: NativeSubmissionContext,
) -> Result<AssembledNativeExecution, DistributedQueryError> {
    crate::query_execution::assembly::validate_prepared_native_payloads(&prepared, &native_bundle)
        .map_err(contract_error)?;
    crate::query_execution::assembly::validate_artifact_fragment_sets(
        &prepared,
        &native_bundle,
        &schedule,
    )
    .map_err(contract_error)?;
    crate::query_execution::assembly::validate_scheduling_placements(&schedule)
        .map_err(contract_error)?;

    let prepared_ids = prepared.fragment_ids();
    let native_ids = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    let scheduled_ids = schedule.fragment_ids().collect::<BTreeSet<_>>();
    if prepared_ids != native_ids || prepared_ids != scheduled_ids {
        return Err(contract_error(format!(
            "prepared/native/scheduled fragment sets differ: prepared={prepared_ids:?}, native={native_ids:?}, scheduled={scheduled_ids:?}"
        )));
    }

    let query_id = context.query_id.into_unique_id();
    let root_fragment_id = schedule.root_fragment_id;
    let root = prepared
        .fragment(root_fragment_id)
        .ok_or_else(|| contract_error("prepared execution root is missing"))?;
    let expected_output = build_expected_output_schema(root)?;
    let root_fetch = RootFetchMetadata {
        fragment_id: root_fragment_id,
        backend_idx: schedule.root_backend_idx,
        finst_id: schedule.root_finst_id,
        uses_result_buffer: !root.execution_role().is_terminal_write(),
    };

    let edges = prepared.scheduling_view().edges().to_vec();
    let stream_edge_by_source =
        crate::query_execution::assembly::build_stream_edge_by_source(&edges);
    let router_edges_by_source: BTreeMap<
        FragmentId,
        (i32, Vec<&crate::sql::planner::distributed::FragmentEdge>),
    > = crate::query_execution::assembly::group_router_edges_by_source(&edges)
        .into_iter()
        .map(|((source_fragment_id, router_group_id), branch_edges)| {
            (source_fragment_id, (router_group_id, branch_edges))
        })
        .collect();
    let mut cte_consumers: BTreeMap<
        CteId,
        Vec<(
            FragmentId,
            i32,
            crate::proto::plan::DataPartition,
            Vec<i32>,
            Vec<ColumnId>,
        )>,
    > = BTreeMap::new();
    for edge in &edges {
        if let FragmentEdgeKind::CteMulticast {
            cte_id,
            receive_producer_column_ids,
        } = &edge.edge_kind
        {
            let native_partition =
                crate::protocol::native::encode::encode_data_partition(&edge.output_partition)
                    .map_err(contract_error)?;
            cte_consumers.entry(*cte_id).or_default().push((
                edge.target_fragment_id,
                edge.target_exchange_node_id,
                native_partition,
                edge.output_slot_ids.clone(),
                receive_producer_column_ids.clone(),
            ));
        }
    }
    for fragment in prepared.scheduling_view().fragments() {
        for (cte_id, exchange_node_id, receive_producer_column_ids) in
            fragment.boundary_projection().cte_exchange_nodes()
        {
            let consumers = cte_consumers.entry(*cte_id).or_default();
            if !consumers.iter().any(|(fid, nid, _, _, _)| {
                *fid == fragment.fragment_id() && *nid == *exchange_node_id
            }) {
                consumers.push((
                    fragment.fragment_id(),
                    *exchange_node_id,
                    crate::proto::plan::DataPartition {
                        kind: crate::proto::plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    },
                    Vec::new(),
                    receive_producer_column_ids.clone(),
                ));
            }
        }
    }
    let consumer_destinations = schedule
        .by_fragment
        .iter()
        .map(|(fragment_id, placements)| {
            let destinations = placements
                .iter()
                .map(|placement| {
                    FragmentDestination::new(placement.finst_id, placement.endpoint.clone())
                })
                .collect();
            (*fragment_id, destinations)
        })
        .collect::<BTreeMap<_, _>>();

    let mut native_by_fragment = native_bundle
        .into_fragments()
        .collect::<BTreeMap<PlannerFragmentId, _>>();
    let mut submissions_by_fragment = BTreeMap::new();
    let mut writer_registrations = Vec::new();
    for (&fragment_id, placements) in &schedule.by_fragment {
        let fragment = prepared
            .fragment(fragment_id)
            .ok_or_else(|| contract_error(format!("prepared fragment {fragment_id} is missing")))?;
        let template = native_by_fragment.remove(&fragment_id).ok_or_else(|| {
            contract_error(format!("native fragment template {fragment_id} is missing"))
        })?;
        let is_root = fragment_id == root_fragment_id;
        let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
        let router_edges = router_edges_by_source.get(&fragment_id);
        let is_writer = stream_edge.is_none()
            && router_edges.is_none()
            && fragment.boundary_projection().cte_id().is_none()
            && fragment.execution_role().is_terminal_write();
        let is_producer = stream_edge.is_some()
            || router_edges.is_some()
            || fragment.boundary_projection().cte_id().is_some();
        crate::query_execution::assembly::validate_fragment_output_kind(
            fragment_id,
            is_root,
            is_writer,
            is_producer,
            fragment.execution_role(),
        )
        .map_err(contract_error)?;
        crate::query_execution::assembly::ensure_native_fragment_sink_supported(
            fragment_id,
            is_root,
            is_writer,
            stream_edge.is_some(),
            router_edges.is_some(),
            fragment.boundary_projection().cte_id().is_some(),
        )
        .map_err(contract_error)?;
        let report_endpoint =
            (is_writer || context.needs_fragment_status_report).then_some(&context.report_endpoint);
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                if is_writer {
                    writer_registrations.push(WriterRegistration {
                        query_id,
                        fragment_instance_id: placement.finst_id,
                        backend_num: placement.instance_index as i32,
                    });
                }
                let mut native_fragment = template.clone();
                if !is_root && !is_writer && stream_edge.is_none() {
                    if let Some((router_group_id, branch_edges)) = router_edges {
                        crate::query_execution::assembly::
                            patch_native_iceberg_change_stream_router_sink(
                                &mut native_fragment,
                                fragment_id,
                                *router_group_id,
                                branch_edges,
                                &schedule.by_fragment,
                            )
                            .map_err(contract_error)?;
                    } else if let Some(cte_id) = fragment.boundary_projection().cte_id() {
                        let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                        crate::query_execution::assembly::patch_native_cte_multicast_sink(
                            &mut native_fragment,
                            fragment_id,
                            cte_id,
                            &consumers,
                            &consumer_destinations,
                        )
                        .map_err(contract_error)?;
                    }
                }
                let instance_params = crate::protocol::native::encode::encode_instance_params(
                    &query_id,
                    placement,
                    &context.options,
                    placement.instance_index as i32,
                    report_endpoint,
                    fragment_id == root_fragment_id && context.needs_fragment_status_report,
                )
                .map_err(contract_error)?;
                Ok(ValidatedNativeSubmission {
                    backend_idx: placement.backend_idx,
                    finst_id: placement.finst_id,
                    envelope: NativeFragmentEnvelope::new(
                        execution_id,
                        native_fragment,
                        instance_params,
                    ),
                })
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        submissions_by_fragment.insert(fragment_id, fragment_submissions);
    }
    if !native_by_fragment.is_empty() {
        return Err(contract_error(format!(
            "native templates remained after assembly: {:?}",
            native_by_fragment.keys().collect::<Vec<_>>()
        )));
    }

    let mut submissions = Vec::new();
    for &fragment_id in prepared.scheduling_view().topological_order().iter().rev() {
        let mut fragment_submissions =
            submissions_by_fragment
                .remove(&fragment_id)
                .ok_or_else(|| {
                    contract_error(format!("assembled fragment {fragment_id} is missing"))
                })?;
        submissions.append(&mut fragment_submissions);
    }
    if !submissions_by_fragment.is_empty() {
        return Err(contract_error(
            "assembled submissions contain unknown fragments",
        ));
    }

    Ok(AssembledNativeExecution {
        submissions,
        root_fetch,
        writer_registrations: WriterRegistrationSet {
            registrations: writer_registrations,
        },
        expected_output,
    })
}

fn build_expected_output_schema(
    root: &PreparedFragment,
) -> Result<ExpectedOutputSchema, DistributedQueryError> {
    let output_columns = root.boundary_projection().output_columns().to_vec();
    let chunk_schema = if output_columns.is_empty() {
        Arc::new(ChunkSchema::empty())
    } else {
        let slots = output_columns
            .iter()
            .enumerate()
            .map(|(index, output)| {
                let slot = u32::try_from(index + 1)
                    .map(SlotId::new)
                    .map_err(|_| contract_error("too many root output columns"))?;
                Ok(ChunkSlotSchema::new_with_field(
                    slot,
                    Field::new(
                        output.name.clone(),
                        output.data_type.clone(),
                        output.nullable,
                    ),
                    None,
                    None,
                ))
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Arc::new(ChunkSchema::try_new(slots).map_err(contract_error)?)
    };
    Ok(ExpectedOutputSchema {
        output_columns,
        chunk_schema,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{build_fragment_lifecycle_projection, derive_fragment_instance_id};
    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{AttemptId, ExchangeRouteManifest, QueryExecutionId};
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
    };

    fn placement(
        fragment_id: u32,
        instance_index: usize,
        finst_id: UniqueId,
        backend_idx: usize,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            endpoint: RuntimeEndpoint::new("127.0.0.1", 19040 + backend_idx as i32)
                .expect("valid endpoint"),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn stream_edge(source_fragment_id: u32, target_fragment_id: u32, node_id: i32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id: node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    #[test]
    fn fragment_instance_identity_is_stable_and_attempt_bound() {
        let query_id = QueryId::new(41, 73);
        let first_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("valid attempt"))
                .expect("valid execution id");
        let second_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(2).expect("valid attempt"))
                .expect("valid execution id");

        let first =
            derive_fragment_instance_id(first_attempt, 9, 3).expect("first fragment instance id");
        assert_eq!(
            derive_fragment_instance_id(first_attempt, 9, 3)
                .expect("repeated fragment instance id"),
            first
        );
        assert_ne!(
            derive_fragment_instance_id(second_attempt, 9, 3).expect("second fragment instance id"),
            first
        );
    }

    #[test]
    fn exchange_route_projection_canonicalizes_out_of_order_edges_and_placements() {
        let schedule = SchedulingPlan {
            root_fragment_id: 40,
            by_fragment: BTreeMap::from([
                (
                    10,
                    vec![
                        placement(10, 0, UniqueId { hi: 9, lo: 1 }, 0),
                        placement(10, 1, UniqueId { hi: 1, lo: 1 }, 1),
                    ],
                ),
                (
                    20,
                    vec![
                        placement(20, 0, UniqueId { hi: 8, lo: 1 }, 0),
                        placement(20, 1, UniqueId { hi: 2, lo: 1 }, 1),
                    ],
                ),
                (30, vec![placement(30, 0, UniqueId { hi: 7, lo: 1 }, 0)]),
                (40, vec![placement(40, 0, UniqueId { hi: 6, lo: 1 }, 1)]),
            ]),
            root_finst_id: UniqueId { hi: 6, lo: 1 },
            root_backend_idx: 1,
        };
        let edges = vec![stream_edge(30, 40, 400), stream_edge(10, 20, 200)];
        let live_backends = BTreeMap::from([
            (
                0,
                LiveBackendTarget::new(0, "127.0.0.1:19040".parse().expect("valid endpoint"), 100),
            ),
            (
                1,
                LiveBackendTarget::new(1, "127.0.0.1:19041".parse().expect("valid endpoint"), 101),
            ),
        ]);

        let projection = build_fragment_lifecycle_projection(&schedule, &edges, live_backends)
            .expect("valid lifecycle projection");
        let expected = vec![
            ExchangeRouteManifest::new(
                UniqueId { hi: 1, lo: 1 },
                UniqueId { hi: 2, lo: 1 },
                200,
                1,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 1, lo: 1 },
                UniqueId { hi: 8, lo: 1 },
                200,
                1,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 7, lo: 1 },
                UniqueId { hi: 6, lo: 1 },
                400,
                0,
                1,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 9, lo: 1 },
                UniqueId { hi: 2, lo: 1 },
                200,
                0,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 9, lo: 1 },
                UniqueId { hi: 8, lo: 1 },
                200,
                0,
                2,
            )
            .expect("valid route"),
        ];

        assert_eq!(projection.exchange_routes, expected);
    }
}
