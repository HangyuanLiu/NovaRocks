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

//! Role-neutral runtime-filter deployment handoff.
//!
//! Core retains the graph and compiler. A role crate owns the deployment
//! epoch, transport, all-participant ACK barrier, rollback state, and lease.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
use crate::query_execution::backend::{LiveBackendSnapshot, LiveBackendTarget};
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, QueryId, RuntimeFilterLifecycleView,
};
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::query_execution::schedule::SchedulingPlan;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::{
    RuntimeFilterDeploymentPolicy, RuntimeFilterQueryDeploymentPolicy,
    RuntimeFilterQueryTransportPolicy,
};
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterParticipantInstall,
};
use crate::sql::planner::distributed::{FragmentEdge, JoinBuildProgressCatalog};

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

const BLOOM_BITS_PER_KEY: u64 = 8;
const BLOOM_HASH_COUNT: u32 = 5;
const BLOOM_SEED: u64 = 17;
const BLOOM_ALGORITHM_VERSION: u16 = 1;
const TRANSPORT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const MAX_PENDING_ENTRIES: usize = 1 << 16;
const MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

fn derive_deployment_policy(
    graph: &RuntimeFilterGraph,
    backends: &LiveBackendSnapshot,
    runtime_worker_count: usize,
) -> Result<RuntimeFilterQueryDeploymentPolicy, String> {
    if runtime_worker_count == 0 {
        return Err("runtime filter deployment worker count must be nonzero".to_string());
    }
    if backends.entries().is_empty() {
        return Err("runtime filter deployment requires at least one live backend".to_string());
    }
    let replica_redundancy = u32::try_from(backends.entries().len()).map_err(|_| {
        "runtime filter live backend count exceeds replica-redundancy width".to_string()
    })?;

    let mut channel_count = 0usize;
    let mut total_artifact_bytes = 0u64;
    let mut max_artifact_bytes = 0u64;
    let mut minimum_deadline_ms = u64::MAX;
    let mut minimum_max_retries = u32::MAX;
    for channel in graph.channels() {
        channel_count = channel_count
            .checked_add(1)
            .ok_or_else(|| "runtime filter channel count overflow".to_string())?;
        let channel_policy = channel.policy;
        if channel_policy.max_contribution_bytes == 0
            || channel_policy.max_artifact_bytes == 0
            || channel_policy.deadline_ms == 0
        {
            return Err(format!(
                "runtime filter channel {} has a zero resource or deadline limit",
                channel.channel_id.get()
            ));
        }
        total_artifact_bytes = total_artifact_bytes
            .checked_add(channel_policy.max_artifact_bytes)
            .ok_or_else(|| "runtime filter artifact budget overflow".to_string())?;
        max_artifact_bytes = max_artifact_bytes.max(channel_policy.max_artifact_bytes);
        minimum_deadline_ms = minimum_deadline_ms.min(channel_policy.deadline_ms);
        minimum_max_retries = minimum_max_retries.min(channel_policy.max_retries);
    }
    if channel_count == 0 {
        return Err("runtime filter deployment policy requires a nonempty graph".to_string());
    }
    let max_attempts = minimum_max_retries
        .checked_add(1)
        .ok_or_else(|| "runtime filter transport attempt count overflow".to_string())?;
    let materialization = MaterializationPolicy::new(
        BLOOM_BITS_PER_KEY,
        BLOOM_HASH_COUNT,
        BLOOM_SEED,
        BLOOM_ALGORITHM_VERSION,
        total_artifact_bytes,
        max_artifact_bytes,
        channel_count.min(runtime_worker_count),
    )
    .map_err(|error| format!("invalid runtime filter materialization policy: {error:?}"))?;
    let deadline = Duration::from_millis(minimum_deadline_ms);

    Ok(RuntimeFilterQueryDeploymentPolicy {
        compiler: RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(total_artifact_bytes),
            replica_redundancy,
            materialization,
        },
        transport: RuntimeFilterQueryTransportPolicy {
            retry_interval: TRANSPORT_RETRY_INTERVAL,
            max_attempts,
            deadline,
            max_pending_entries: MAX_PENDING_ENTRIES,
            max_pending_bytes: MAX_PENDING_BYTES,
        },
        install_rpc_deadline: deadline,
    })
}

/// Frontend-owned, nonzero deployment epoch represented without exposing the
/// runtime-filter compiler identity type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterDeploymentEpoch(u64);

impl RuntimeFilterDeploymentEpoch {
    pub fn new(value: u64) -> Result<Self, DistributedQueryError> {
        if value == 0 {
            return Err(contract_error(
                "runtime filter deployment epoch must be nonzero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    fn into_core(self) -> DeploymentEpoch {
        DeploymentEpoch::new(self.0)
    }
}

/// Explicit schedule-bound compiler input. There is no default constructor:
/// the owner must provide one immutable live-backend snapshot, epoch, worker
/// budget, and the lifecycle derived from the core-resolved query options.
pub struct RuntimeFilterDeploymentOptions {
    epoch: RuntimeFilterDeploymentEpoch,
    live_backends: Vec<(usize, SocketAddr)>,
    runtime_worker_count: usize,
    lifecycle: RuntimeFilterLifecycleView,
}

impl RuntimeFilterDeploymentOptions {
    pub fn new(
        epoch: RuntimeFilterDeploymentEpoch,
        live_backends: Vec<(usize, SocketAddr)>,
        runtime_worker_count: usize,
        lifecycle: RuntimeFilterLifecycleView,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(contract_error(
                "runtime filter deployment requires an explicit nonempty live-backend snapshot",
            ));
        }
        if runtime_worker_count == 0 {
            return Err(contract_error(
                "runtime filter deployment worker count must be nonzero",
            ));
        }
        let mut backend_ids = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for (backend_idx, endpoint) in &live_backends {
            if !backend_ids.insert(*backend_idx) {
                return Err(contract_error(format!(
                    "runtime filter live-backend snapshot repeats backend {backend_idx}"
                )));
            }
            if !endpoints.insert(*endpoint) {
                return Err(contract_error(format!(
                    "runtime filter live-backend snapshot repeats endpoint {endpoint}"
                )));
            }
        }
        Ok(Self {
            epoch,
            live_backends,
            runtime_worker_count,
            lifecycle,
        })
    }
}

/// Opaque install payload. Only a core transport adapter can recover the
/// internal compiler DTO.
pub struct RuntimeFilterInstallEnvelope {
    query_id: crate::common::types::UniqueId,
    lifecycle: RuntimeFilterQueryLifecycleOptions,
    install: RuntimeFilterParticipantInstall,
}

impl RuntimeFilterInstallEnvelope {
    pub(crate) fn into_native(
        self,
    ) -> (
        crate::common::types::UniqueId,
        RuntimeFilterQueryLifecycleOptions,
        RuntimeFilterParticipantInstall,
    ) {
        (self.query_id, self.lifecycle, self.install)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        query_id: crate::common::types::UniqueId,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        install: RuntimeFilterParticipantInstall,
    ) -> Self {
        Self {
            query_id,
            lifecycle,
            install,
        }
    }
}

/// Opaque rollback payload paired with one participant install.
pub struct RuntimeFilterAbortEnvelope {
    query_id: crate::common::types::UniqueId,
    epoch: DeploymentEpoch,
}

impl RuntimeFilterAbortEnvelope {
    pub(crate) fn into_native(self) -> (crate::common::types::UniqueId, DeploymentEpoch) {
        (self.query_id, self.epoch)
    }
}

/// Transport-only port. The frontend barrier owns ordering, ACK aggregation,
/// rollback, and query state; an adapter only sends the provided opaque
/// envelope over the selected endpoint.
pub trait RuntimeFilterDeploymentDispatcher: Send + Sync + 'static {
    fn install(
        &self,
        backend_idx: usize,
        endpoint: SocketAddr,
        participant_id: u32,
        deadline: Duration,
        envelope: RuntimeFilterInstallEnvelope,
    ) -> Result<(), String>;

    fn abort(
        &self,
        backend_idx: usize,
        endpoint: SocketAddr,
        participant_id: u32,
        deadline: Duration,
        envelope: RuntimeFilterAbortEnvelope,
    ) -> Result<(), String>;
}

/// Build the production gRPC transport adapter from one explicit immutable
/// backend snapshot. The returned port preserves the same wire path for every
/// deployment topology.
pub fn new_grpc_runtime_filter_deployment_dispatcher(
    backends: &[(usize, SocketAddr)],
) -> Result<Arc<dyn RuntimeFilterDeploymentDispatcher>, String> {
    Ok(Arc::new(
        crate::service::grpc_fragment_dispatcher::GrpcRuntimeFilterDeploymentControl::new(
            backends,
        )?,
    ))
}

/// One opaque participant install paired with its exact rollback target.
pub struct RuntimeFilterParticipantInstallPlan {
    backend_idx: usize,
    endpoint: SocketAddr,
    participant_id: u32,
    deadline: Duration,
    install: RuntimeFilterInstallEnvelope,
    abort: RuntimeFilterAbortEnvelope,
}

impl RuntimeFilterParticipantInstallPlan {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub const fn participant_id(&self) -> u32 {
        self.participant_id
    }

    pub const fn deadline(&self) -> Duration {
        self.deadline
    }

    pub fn into_envelopes(self) -> (RuntimeFilterInstallEnvelope, RuntimeFilterAbortEnvelope) {
        (self.install, self.abort)
    }
}

/// Core-compiled install plan. Graphs, schedules, join progress, compiler
/// policies, and native install DTOs never cross this boundary.
pub struct RuntimeFilterInstallPlan {
    query_id: QueryId,
    epoch: RuntimeFilterDeploymentEpoch,
    participants: Vec<RuntimeFilterParticipantInstallPlan>,
}

impl RuntimeFilterInstallPlan {
    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn epoch(&self) -> RuntimeFilterDeploymentEpoch {
        self.epoch
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    pub fn into_participants(self) -> Vec<RuntimeFilterParticipantInstallPlan> {
        self.participants
    }
}

/// Frontend-owned guard behind an opaque lease.
///
/// Dropping the guard without `release` must roll back any armed participant.
pub trait RuntimeFilterInstallLeaseGuard: Send + 'static {
    fn release(self: Box<Self>);

    fn abort_preserving(self: Box<Self>, primary_error: String) -> String;
}

/// The installed-deployment lease carried through native assembly.
#[must_use = "the installed runtime-filter deployment must be released after submission or aborted"]
pub struct RuntimeFilterInstallLease {
    guard: Option<Box<dyn RuntimeFilterInstallLeaseGuard>>,
}

impl RuntimeFilterInstallLease {
    pub fn new(guard: Box<dyn RuntimeFilterInstallLeaseGuard>) -> Self {
        Self { guard: Some(guard) }
    }

    pub fn release(mut self) {
        self.guard
            .take()
            .expect("runtime-filter install lease is consumed exactly once")
            .release();
    }

    pub fn abort_preserving(mut self, primary_error: String) -> String {
        self.guard
            .take()
            .expect("runtime-filter install lease is consumed exactly once")
            .abort_preserving(primary_error)
    }
}

/// Frontend-owned all-participant install/ACK barrier.
pub trait RuntimeFilterInstallBarrier: Send + Sync + 'static {
    fn install_all(
        &self,
        plan: RuntimeFilterInstallPlan,
    ) -> Result<RuntimeFilterInstallLease, DistributedQueryError>;
}

/// One participant-local runtime-filter contribution for `InitQuery`.
///
/// Transport deadlines, ACK aggregation, and rollback ownership belong to the
/// query lifecycle barrier rather than this compiler output.
pub(crate) struct RuntimeFilterContributionPlan {
    backend_idx: usize,
    participant_id: u32,
    lifecycle: RuntimeFilterQueryLifecycleOptions,
    install: RuntimeFilterParticipantInstall,
}

impl RuntimeFilterContributionPlan {
    pub(crate) fn new(
        backend_idx: usize,
        participant_id: u32,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<Self, DistributedQueryError> {
        if participant_id == 0 {
            return Err(contract_error(
                "runtime filter contribution participant id must be nonzero",
            ));
        }
        if install.local_participant_id().get() != participant_id {
            return Err(contract_error(
                "runtime filter contribution participant id does not match typed install",
            ));
        }
        Ok(Self {
            backend_idx,
            participant_id,
            lifecycle,
            install,
        })
    }

    pub(crate) const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub(crate) const fn participant_id(&self) -> u32 {
        self.participant_id
    }

    pub(crate) const fn lifecycle(&self) -> RuntimeFilterQueryLifecycleOptions {
        self.lifecycle
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        usize,
        u32,
        RuntimeFilterQueryLifecycleOptions,
        RuntimeFilterParticipantInstall,
    ) {
        (
            self.backend_idx,
            self.participant_id,
            self.lifecycle,
            self.install,
        )
    }
}

pub(crate) fn compile_contribution_plan(
    execution_id: QueryExecutionId,
    graph: &RuntimeFilterGraph,
    join_progress: &JoinBuildProgressCatalog,
    edges: &[FragmentEdge],
    schedule: &SchedulingPlan,
    live_backends: &[LiveBackendTarget],
    runtime_worker_count: usize,
    lifecycle: RuntimeFilterLifecycleView,
) -> Result<Vec<RuntimeFilterContributionPlan>, DistributedQueryError> {
    if runtime_worker_count == 0 {
        return Err(contract_error(
            "runtime filter deployment worker count must be nonzero",
        ));
    }
    if live_backends.is_empty() {
        return Err(contract_error(
            "runtime filter deployment requires an explicit nonempty live-backend snapshot",
        ));
    }
    let mut backend_by_id = BTreeMap::new();
    let mut endpoints = BTreeSet::new();
    let mut live_entries = Vec::with_capacity(live_backends.len());
    for target in live_backends {
        if target.start_epoch() == 0 {
            return Err(contract_error(format!(
                "runtime filter live backend {} has zero start epoch",
                target.backend_idx()
            )));
        }
        if backend_by_id
            .insert(target.backend_idx(), target.endpoint())
            .is_some()
        {
            return Err(contract_error(format!(
                "runtime filter live-backend snapshot repeats backend {}",
                target.backend_idx()
            )));
        }
        if !endpoints.insert(target.endpoint()) {
            return Err(contract_error(format!(
                "runtime filter live-backend snapshot repeats endpoint {}",
                target.endpoint()
            )));
        }
        live_entries.push((target.backend_idx(), target.endpoint()));
    }
    for placement in schedule.by_fragment.values().flatten() {
        let endpoint = backend_by_id.get(&placement.backend_idx).ok_or_else(|| {
            contract_error(format!(
                "scheduled backend {} is absent from the runtime filter live-backend snapshot",
                placement.backend_idx
            ))
        })?;
        if RuntimeEndpoint::from_socket_addr(*endpoint) != placement.endpoint {
            return Err(contract_error(format!(
                "scheduled backend {} endpoint {} differs from runtime filter snapshot endpoint {}",
                placement.backend_idx,
                placement.endpoint.as_host_port(),
                endpoint
            )));
        }
    }
    if graph.is_empty() {
        return Ok(Vec::new());
    }

    let live_snapshot = LiveBackendSnapshot::new(live_entries);
    let policy = derive_deployment_policy(graph, &live_snapshot, runtime_worker_count)
        .map_err(|error| failed(format!("runtime filter deployment policy failed: {error}")))?;
    let epoch = DeploymentEpoch::new(execution_id.attempt_id().get());
    let compiled = crate::runtime_filter::deployment::compiler::compile_with_join_progress(
        graph,
        schedule,
        edges,
        join_progress,
        &live_snapshot,
        &policy.compiler,
        epoch,
    )
    .map_err(|error| failed(format!("runtime filter deployment compile failed: {error}")))?;
    let installs =
        crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension::new()
            .participant_installs(&compiled)
            .map_err(|error| {
                failed(format!(
                    "runtime filter participant install projection failed: {error}"
                ))
            })?;
    let lifecycle = RuntimeFilterQueryLifecycleOptions {
        delivery_expire: lifecycle.delivery_expire(),
        query_expire: lifecycle.query_expire(),
        transport_retry_interval: policy.transport.retry_interval,
        transport_max_attempts: policy.transport.max_attempts,
        transport_deadline: policy.transport.deadline,
        transport_max_pending_entries: policy.transport.max_pending_entries,
        transport_max_pending_bytes: policy.transport.max_pending_bytes,
    };
    let mut participants = Vec::with_capacity(installs.len());
    for (participant, install) in installs {
        let (backend_idx, _) = participant_backend(participant, live_snapshot.entries())?;
        participants.push(RuntimeFilterContributionPlan::new(
            backend_idx,
            participant.get(),
            lifecycle,
            install,
        )?);
    }
    participants.sort_by_key(RuntimeFilterContributionPlan::participant_id);
    Ok(participants)
}

pub(crate) fn compile_install_plan(
    query_id: QueryId,
    graph: &RuntimeFilterGraph,
    join_progress: &JoinBuildProgressCatalog,
    edges: &[FragmentEdge],
    schedule: &SchedulingPlan,
    options: RuntimeFilterDeploymentOptions,
) -> Result<RuntimeFilterInstallPlan, DistributedQueryError> {
    let RuntimeFilterDeploymentOptions {
        epoch,
        live_backends,
        runtime_worker_count,
        lifecycle,
    } = options;
    let backend_by_id = live_backends.iter().copied().collect::<BTreeMap<_, _>>();
    for placement in schedule.by_fragment.values().flatten() {
        let endpoint = backend_by_id.get(&placement.backend_idx).ok_or_else(|| {
            contract_error(format!(
                "scheduled backend {} is absent from the runtime filter live-backend snapshot",
                placement.backend_idx
            ))
        })?;
        if crate::runtime::endpoint::RuntimeEndpoint::from_socket_addr(*endpoint)
            != placement.endpoint
        {
            return Err(contract_error(format!(
                "scheduled backend {} endpoint {} differs from runtime filter snapshot endpoint {}",
                placement.backend_idx,
                placement.endpoint.as_host_port(),
                endpoint
            )));
        }
    }

    // Empty graphs still produce a plan and must cross the frontend barrier.
    if graph.is_empty() {
        return Ok(RuntimeFilterInstallPlan {
            query_id,
            epoch,
            participants: Vec::new(),
        });
    }

    let live_snapshot = LiveBackendSnapshot::new(live_backends);
    let policy = derive_deployment_policy(graph, &live_snapshot, runtime_worker_count)
        .map_err(|error| failed(format!("runtime filter deployment policy failed: {error}")))?;
    let core_epoch = epoch.into_core();
    let compiled = crate::runtime_filter::deployment::compiler::compile_with_join_progress(
        graph,
        schedule,
        edges,
        join_progress,
        &live_snapshot,
        &policy.compiler,
        core_epoch,
    )
    .map_err(|error| failed(format!("runtime filter deployment compile failed: {error}")))?;
    let installs =
        crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension::new()
            .participant_installs(&compiled)
            .map_err(|error| {
                failed(format!(
                    "runtime filter participant install projection failed: {error}"
                ))
            })?;
    let native_lifecycle = RuntimeFilterQueryLifecycleOptions {
        delivery_expire: lifecycle.delivery_expire(),
        query_expire: lifecycle.query_expire(),
        transport_retry_interval: policy.transport.retry_interval,
        transport_max_attempts: policy.transport.max_attempts,
        transport_deadline: policy.transport.deadline,
        transport_max_pending_entries: policy.transport.max_pending_entries,
        transport_max_pending_bytes: policy.transport.max_pending_bytes,
    };
    let query_native = query_id.into_unique_id();
    let mut participants = Vec::with_capacity(installs.len());
    for (participant, install) in installs {
        let (backend_idx, endpoint) = participant_backend(participant, live_snapshot.entries())?;
        participants.push(RuntimeFilterParticipantInstallPlan {
            backend_idx,
            endpoint,
            participant_id: participant.get(),
            deadline: policy.install_rpc_deadline,
            install: RuntimeFilterInstallEnvelope {
                query_id: query_native,
                lifecycle: native_lifecycle,
                install,
            },
            abort: RuntimeFilterAbortEnvelope {
                query_id: query_native,
                epoch: core_epoch,
            },
        });
    }
    participants.sort_by_key(RuntimeFilterParticipantInstallPlan::participant_id);
    Ok(RuntimeFilterInstallPlan {
        query_id,
        epoch,
        participants,
    })
}

fn participant_backend(
    participant: RuntimeFilterParticipantId,
    live_backends: &[(usize, SocketAddr)],
) -> Result<(usize, SocketAddr), DistributedQueryError> {
    let backend_idx = usize::try_from(participant.get() - 1)
        .map_err(|_| contract_error("runtime filter participant id exceeds backend index width"))?;
    let endpoint = live_backends
        .iter()
        .find_map(|(candidate, endpoint)| (*candidate == backend_idx).then_some(*endpoint))
        .ok_or_else(|| {
            contract_error(format!(
                "runtime filter participant {} has no live-backend endpoint",
                participant.get()
            ))
        })?;
    Ok((backend_idx, endpoint))
}
