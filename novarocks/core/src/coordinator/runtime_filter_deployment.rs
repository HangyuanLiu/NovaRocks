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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::join_all;

use crate::common::types::UniqueId;
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::ports::{
    RuntimeFilterDeploymentControlPort, RuntimeFilterDeploymentPolicyProvider,
};
use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime_filter::deployment::{
    RuntimeFilterDeploymentPolicy, RuntimeFilterQueryDeploymentPolicy,
    RuntimeFilterQueryTransportPolicy,
};
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::runtime_filter::port::identity::DeploymentEpoch;
use crate::runtime_filter::port::identity::RuntimeFilterParticipantId;
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterParticipantInstall,
};

const BLOOM_BITS_PER_KEY: u64 = 8;
const BLOOM_HASH_COUNT: u32 = 5;
const BLOOM_SEED: u64 = 17;
const BLOOM_ALGORITHM_VERSION: u16 = 1;
const TRANSPORT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const MAX_PENDING_ENTRIES: usize = 1 << 16;
const MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeRuntimeFilterDeploymentPolicyProvider {
    runtime_worker_count: usize,
}

impl NativeRuntimeFilterDeploymentPolicyProvider {
    pub(crate) const fn new(runtime_worker_count: usize) -> Self {
        Self {
            runtime_worker_count,
        }
    }
}

impl RuntimeFilterDeploymentPolicyProvider for NativeRuntimeFilterDeploymentPolicyProvider {
    fn policy_for(
        &self,
        graph: &RuntimeFilterGraph,
        backends: &LiveBackendSnapshot,
    ) -> Result<RuntimeFilterQueryDeploymentPolicy, String> {
        if self.runtime_worker_count == 0 {
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
        let max_concurrent_jobs = channel_count.min(self.runtime_worker_count);
        let materialization = MaterializationPolicy::new(
            BLOOM_BITS_PER_KEY,
            BLOOM_HASH_COUNT,
            BLOOM_SEED,
            BLOOM_ALGORITHM_VERSION,
            total_artifact_bytes,
            max_artifact_bytes,
            max_concurrent_jobs,
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
}

static NEXT_DEPLOYMENT_EPOCH: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DeploymentEpochAllocator;

impl DeploymentEpochAllocator {
    pub(crate) fn allocate(&self) -> Result<DeploymentEpoch, String> {
        let epoch = NEXT_DEPLOYMENT_EPOCH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| "runtime filter deployment epoch exhausted".to_string())?;
        if epoch == 0 {
            return Err("runtime filter deployment epoch allocator returned zero".to_string());
        }
        Ok(DeploymentEpoch::new(epoch))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedRuntimeFilterDeployment {
    pub(crate) epoch: DeploymentEpoch,
    pub(crate) policy: RuntimeFilterQueryDeploymentPolicy,
}

pub(crate) fn prepare_runtime_filter_deployment(
    graph: &RuntimeFilterGraph,
    backends: &LiveBackendSnapshot,
    policy_provider: &dyn RuntimeFilterDeploymentPolicyProvider,
    epoch_allocator: &DeploymentEpochAllocator,
) -> Result<Option<PreparedRuntimeFilterDeployment>, String> {
    if graph.is_empty() {
        return Ok(None);
    }
    let epoch = epoch_allocator.allocate()?;
    let policy = policy_provider.policy_for(graph, backends)?;
    Ok(Some(PreparedRuntimeFilterDeployment { epoch, policy }))
}

pub(crate) struct RuntimeFilterInstallBarrier {
    control: Arc<dyn RuntimeFilterDeploymentControlPort>,
}

impl RuntimeFilterInstallBarrier {
    pub(crate) fn new(control: Arc<dyn RuntimeFilterDeploymentControlPort>) -> Self {
        Self { control }
    }

    pub(crate) fn install_all_or_rollback(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        mut installs: Vec<(RuntimeFilterParticipantId, RuntimeFilterParticipantInstall)>,
    ) -> Result<(), String> {
        installs.sort_by_key(|(participant, _)| *participant);
        let mut seen = BTreeSet::new();
        for (participant, install) in &installs {
            if !seen.insert(*participant) {
                return Err(format!(
                    "duplicate runtime filter install participant {}",
                    participant.get()
                ));
            }
            if install.epoch() != epoch || install.local_participant_id() != *participant {
                return Err(format!(
                    "runtime filter install identity mismatch for participant {} under epoch {}",
                    participant.get(),
                    epoch.get()
                ));
            }
        }

        let install_results = data_block_on(async {
            join_all(installs.into_iter().map(|(participant, install)| {
                let control = Arc::clone(&self.control);
                async move {
                    let result = control
                        .install(query_id, lifecycle, deadline, participant, install)
                        .await;
                    (participant, result)
                }
            }))
            .await
        })
        .map_err(|error| format!("runtime filter install runtime failed: {error}"))?;

        let mut acknowledged = Vec::new();
        let mut install_failures = Vec::new();
        for (participant, result) in install_results {
            match result {
                Ok(()) => acknowledged.push(participant),
                Err(error) => install_failures.push((participant, error)),
            }
        }
        if install_failures.is_empty() {
            return Ok(());
        }

        let rollback_results = data_block_on(async {
            join_all(acknowledged.into_iter().map(|participant| {
                let control = Arc::clone(&self.control);
                async move {
                    let result = control.abort(query_id, epoch, deadline, participant).await;
                    (participant, result)
                }
            }))
            .await
        });
        let mut rollback_runtime_failure = None;
        let rollback_failures = match rollback_results {
            Ok(results) => results
                .into_iter()
                .filter_map(|(participant, result)| result.err().map(|error| (participant, error)))
                .collect::<Vec<_>>(),
            Err(error) => {
                rollback_runtime_failure = Some(error);
                Vec::new()
            }
        };

        let (primary_participant, primary_error) = &install_failures[0];
        let mut message = format!(
            "runtime filter install failed for participant {}: {}",
            primary_participant.get(),
            primary_error
        );
        if install_failures.len() > 1 {
            let additional = install_failures[1..]
                .iter()
                .map(|(participant, error)| format!("participant {}: {error}", participant.get()))
                .collect::<Vec<_>>()
                .join("; ");
            message.push_str(&format!("; additional install failures: [{additional}]"));
        }
        if !rollback_failures.is_empty() {
            let rollback = rollback_failures
                .iter()
                .map(|(participant, error)| format!("participant {}: {error}", participant.get()))
                .collect::<Vec<_>>()
                .join("; ");
            message.push_str(&format!("; rollback failures: [{rollback}]"));
        }
        if let Some(error) = rollback_runtime_failure {
            message.push_str(&format!("; rollback runtime failure: {error}"));
        }
        Err(message)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{
        DeploymentEpochAllocator, NativeRuntimeFilterDeploymentPolicyProvider,
        prepare_runtime_filter_deployment,
    };
    use crate::coordinator::cluster::LiveBackendSnapshot;
    use crate::coordinator::ports::RuntimeFilterDeploymentPolicyProvider;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ChannelId, ContributionKind, CoverageWitnessId, NullSemantics,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{RuntimeFilterChannelSpec, RuntimeFilterGraph};

    fn channel(
        channel_id: u32,
        max_contribution_bytes: u64,
        max_artifact_bytes: u64,
        deadline_ms: u64,
        max_retries: u32,
    ) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id: ChannelId::new(channel_id),
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: arrow::datatypes::DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(channel_id)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(channel_id)),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes,
                max_artifact_bytes,
                deadline_ms,
                max_retries,
            },
        }
    }

    fn graph(channels: impl IntoIterator<Item = RuntimeFilterChannelSpec>) -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        for channel in channels {
            graph.insert_channel(channel).expect("unique test channel");
        }
        graph
    }

    fn three_backends() -> LiveBackendSnapshot {
        LiveBackendSnapshot::new(
            (0..3)
                .map(|backend_idx| {
                    (
                        backend_idx,
                        SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::LOCALHOST),
                            9060 + u16::try_from(backend_idx).unwrap(),
                        ),
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn native_runtime_filter_policy_uses_graph_limits_and_live_snapshot() {
        let graph = graph([channel(1, 32, 100, 900, 4), channel(2, 64, 300, 400, 2)]);
        let backends = three_backends();

        let worker_limited = NativeRuntimeFilterDeploymentPolicyProvider::new(1)
            .policy_for(&graph, &backends)
            .expect("valid worker-limited policy");
        assert_eq!(worker_limited.compiler.core_budget.max_reducer_bytes(), 400);
        assert_eq!(
            worker_limited
                .compiler
                .materialization
                .max_total_retained_bytes(),
            400
        );
        assert_eq!(
            worker_limited
                .compiler
                .materialization
                .max_scratch_bytes_per_job(),
            300
        );
        assert_eq!(
            worker_limited
                .compiler
                .materialization
                .max_concurrent_jobs(),
            1
        );
        assert_eq!(worker_limited.compiler.replica_redundancy, 3);
        assert_eq!(
            (
                worker_limited.compiler.materialization.bloom_bits_per_key(),
                worker_limited.compiler.materialization.bloom_hash_count(),
                worker_limited.compiler.materialization.bloom_seed(),
                worker_limited
                    .compiler
                    .materialization
                    .bloom_algorithm_version(),
            ),
            (8, 5, 17, 1)
        );
        assert_eq!(
            worker_limited.transport.deadline,
            Duration::from_millis(400)
        );
        assert_eq!(
            worker_limited.transport.retry_interval,
            Duration::from_millis(200)
        );
        assert_eq!(worker_limited.transport.max_attempts, 3);
        assert_eq!(worker_limited.transport.max_pending_entries, 65_536);
        assert_eq!(
            worker_limited.transport.max_pending_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(
            worker_limited.install_rpc_deadline,
            Duration::from_millis(400)
        );

        let channel_limited = NativeRuntimeFilterDeploymentPolicyProvider::new(8)
            .policy_for(&graph, &backends)
            .expect("valid channel-limited policy");
        assert_eq!(
            channel_limited
                .compiler
                .materialization
                .max_concurrent_jobs(),
            2
        );
    }

    #[test]
    fn native_runtime_filter_policy_rejects_zero_overflow_and_empty_snapshot() {
        let backends = three_backends();
        let valid_graph = graph([channel(1, 1, 1, 1, 0)]);
        assert!(
            NativeRuntimeFilterDeploymentPolicyProvider::new(0)
                .policy_for(&valid_graph, &backends)
                .unwrap_err()
                .contains("worker")
        );

        for zero_graph in [
            graph([channel(1, 0, 1, 1, 0)]),
            graph([channel(1, 1, 0, 1, 0)]),
            graph([channel(1, 1, 1, 0, 0)]),
        ] {
            assert!(
                NativeRuntimeFilterDeploymentPolicyProvider::new(1)
                    .policy_for(&zero_graph, &backends)
                    .unwrap_err()
                    .contains("zero")
            );
        }

        let overflow_graph = graph([channel(1, 1, u64::MAX, 1, 0), channel(2, 1, 1, 1, 0)]);
        assert!(
            NativeRuntimeFilterDeploymentPolicyProvider::new(1)
                .policy_for(&overflow_graph, &backends)
                .unwrap_err()
                .contains("overflow")
        );
        assert!(
            NativeRuntimeFilterDeploymentPolicyProvider::new(1)
                .policy_for(&valid_graph, &LiveBackendSnapshot::new(Vec::new()))
                .unwrap_err()
                .contains("backend")
        );
    }

    #[test]
    fn deployment_epoch_allocator_never_returns_zero_and_is_monotonic() {
        let allocator = DeploymentEpochAllocator::default();
        let first = allocator.allocate().expect("first epoch");
        let second = allocator.allocate().expect("second epoch");

        assert_ne!(first.get(), 0);
        assert_ne!(second.get(), 0);
        assert!(second.get() > first.get());
    }

    #[test]
    fn empty_graph_is_typed_noop_without_calling_policy_provider() {
        struct CountingProvider(AtomicUsize);

        impl RuntimeFilterDeploymentPolicyProvider for CountingProvider {
            fn policy_for(
                &self,
                _graph: &RuntimeFilterGraph,
                _backends: &LiveBackendSnapshot,
            ) -> Result<crate::runtime_filter::deployment::RuntimeFilterQueryDeploymentPolicy, String>
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                panic!("empty graph must not invoke policy provider")
            }
        }

        let provider = CountingProvider(AtomicUsize::new(0));
        let prepared = prepare_runtime_filter_deployment(
            &RuntimeFilterGraph::default(),
            &LiveBackendSnapshot::new(Vec::new()),
            &provider,
            &DeploymentEpochAllocator::default(),
        )
        .expect("empty graph is a coordinator no-op");

        assert!(prepared.is_none());
        assert_eq!(provider.0.load(Ordering::Relaxed), 0);
    }
}
