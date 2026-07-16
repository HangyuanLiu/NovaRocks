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
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::RuntimeFilterChannel;
use crate::runtime_filter::materializer::bloom::BloomHashContract;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, CoverageWitnessId, ReductionRequirement,
    RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile, ConsumerProfileId,
};
use crate::runtime_filter::port::events::{
    RouteEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::final_domain::{
    CompletionFenceAuthority, RuntimeCompletionFenceContract,
};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RouteEdgeId};
use crate::runtime_filter::port::install::{
    RuntimeFilterChannelDeployment, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
use crate::runtime_filter::port::producer::{
    InstallContractError, InstallContractErrorKind, InstallOutcome, ProducerPortKind,
    RuntimeContractViolation, RuntimeContractViolationKind,
};
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteRole, RuntimeFilterRoutingShard,
};
use crate::runtime_filter::port::subscription::{SubscriptionHandle, SubscriptionKind};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactScratchBudget, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;
use crate::runtime_filter::port::value_domain::MembershipValues;
use crate::runtime_filter::router::loopback::LoopbackRouter;

use super::materialization::{ArtifactPublishGate, ArtifactPublishKey};
use super::subscription::SubscriptionGroup;
use super::{EventBatchCompletion, EventEmitter};

#[derive(Debug)]
pub(super) struct RegistryInstallResult {
    outcome: InstallOutcome,
    committed_at: Option<Instant>,
    events: Vec<RuntimeFilterEvent>,
}

impl RegistryInstallResult {
    pub(super) const fn outcome(&self) -> InstallOutcome {
        self.outcome
    }

    pub(super) const fn committed_at(&self) -> Option<Instant> {
        self.committed_at
    }

    pub(super) fn events(&self) -> &[RuntimeFilterEvent] {
        &self.events
    }
}

pub(super) struct ProducerRoute {
    pub(super) channel_id: ChannelId,
    pub(super) channel: Arc<RuntimeFilterChannel>,
    pub(super) expected_instances: BTreeSet<UniqueId>,
    pub(super) kind: ProducerPortKind,
    pub(super) final_domain_seed: Option<FinalDomainAuthoritySeed>,
}

#[derive(Clone)]
pub(super) struct FinalDomainAuthoritySeed {
    contract: Arc<RuntimeCompletionFenceContract>,
}

impl FinalDomainAuthoritySeed {
    fn new(contract: Arc<RuntimeCompletionFenceContract>) -> Self {
        Self { contract }
    }

    pub(super) fn derive(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<CompletionFenceAuthority, RuntimeContractViolation> {
        CompletionFenceAuthority::try_new(self.contract.clone(), binding_id, fragment_instance_id)
            .map_err(|_| {
                RuntimeContractViolation::new(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "completion-fence authority could not be derived from the installed seed",
                )
            })
    }
}

#[derive(Clone)]
pub(super) struct CapabilityGroup {
    key: ArtifactPublishKey,
    common: RuntimeFilterEventIdentity,
    profile: ConsumerArtifactProfile,
    route_edges: Arc<[RouteEdgeId]>,
}

impl CapabilityGroup {
    pub(super) const fn key(&self) -> ArtifactPublishKey {
        self.key
    }

    pub(super) const fn common(&self) -> RuntimeFilterEventIdentity {
        self.common
    }

    pub(super) const fn profile(&self) -> &ConsumerArtifactProfile {
        &self.profile
    }

    pub(super) fn route_edges(&self) -> &[RouteEdgeId] {
        &self.route_edges
    }
}

pub(super) struct ChannelArtifactPlan {
    schema: Option<ArtifactMembershipSchema>,
    policy: crate::runtime_filter::port::install::MaterializationPolicy,
    max_artifact_bytes: usize,
    max_concurrent_jobs: usize,
    retained_budget: Arc<ArtifactRetainedBudget>,
    scratch_budget: Arc<ArtifactScratchBudget>,
    groups: Arc<[CapabilityGroup]>,
}

impl ChannelArtifactPlan {
    pub(super) const fn schema(&self) -> Option<&ArtifactMembershipSchema> {
        self.schema.as_ref()
    }
    pub(super) const fn policy(
        &self,
    ) -> crate::runtime_filter::port::install::MaterializationPolicy {
        self.policy
    }
    pub(super) const fn max_artifact_bytes(&self) -> usize {
        self.max_artifact_bytes
    }
    pub(super) const fn max_concurrent_jobs(&self) -> usize {
        self.max_concurrent_jobs
    }
    pub(super) fn retained_budget(&self) -> Arc<ArtifactRetainedBudget> {
        self.retained_budget.clone()
    }
    pub(super) fn scratch_budget(&self) -> Arc<ArtifactScratchBudget> {
        self.scratch_budget.clone()
    }
    pub(super) fn groups(&self) -> &[CapabilityGroup] {
        &self.groups
    }
}

pub(super) struct InstalledDeployment {
    install: RuntimeFilterParticipantInstall,
    committed_at: Instant,
    channels: BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>,
    deadlines: BTreeMap<ChannelId, Instant>,
    producers: BTreeMap<BindingId, ProducerRoute>,
    consumer_activations: BTreeMap<BindingId, ConsumerActivation>,
    subscriptions: BTreeMap<BindingId, Arc<SubscriptionGroup>>,
    router: Arc<LoopbackRouter>,
    channel_routes: BTreeMap<ChannelId, Vec<RouteEdgeId>>,
    route_event_identities: BTreeMap<RouteEdgeId, Vec<RouteEventIdentity>>,
    artifact_channels: BTreeMap<ChannelId, ChannelArtifactPlan>,
    publish_gate: ArtifactPublishGate,
}

pub(super) struct CancelledDeployment {
    installed: Arc<InstalledDeployment>,
    cancelled_routes: BTreeMap<ChannelId, Vec<RouteEdgeId>>,
}

impl CancelledDeployment {
    pub(super) fn installed(&self) -> &Arc<InstalledDeployment> {
        &self.installed
    }

    pub(super) fn deliver_artifact_cancellation(&self) {
        for routes in self.cancelled_routes.values() {
            self.installed.router.route(
                routes,
                &crate::runtime_filter::port::subscription::ArtifactDeliveryOutcome::Cancelled,
            );
        }
    }

    pub(super) fn arm_artifact_cancellation(
        &self,
        channel_id: ChannelId,
        barrier: Arc<EventBatchCompletion>,
    ) {
        let Some(routes) = self.cancelled_routes.get(&channel_id) else {
            return;
        };
        for route in routes {
            for subscription in self.installed.subscriptions.values() {
                subscription.arm_cancellation_event(*route, barrier.clone());
            }
        }
    }
}

impl InstalledDeployment {
    pub(super) fn producer(&self, binding_id: BindingId) -> Option<&ProducerRoute> {
        self.producers.get(&binding_id)
    }

    pub(super) fn subscription(
        &self,
        binding_id: BindingId,
        instance: UniqueId,
        requested: SubscriptionKind,
    ) -> Option<SubscriptionHandle> {
        self.subscriptions
            .get(&binding_id)
            .and_then(|group| group.handle(instance, requested))
    }

    pub(super) fn consumer_activation(&self, binding_id: BindingId) -> Option<ConsumerActivation> {
        self.consumer_activations.get(&binding_id).copied()
    }

    pub(super) fn has_consumer(&self, binding_id: BindingId) -> bool {
        self.consumer_activations.contains_key(&binding_id)
    }

    #[cfg(test)]
    pub(super) fn set_subscription_delivery_hook(
        &self,
        binding_id: BindingId,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        self.subscriptions
            .get(&binding_id)
            .expect("test consumer binding is installed")
            .set_before_deliver_hook(hook);
    }

    #[cfg(test)]
    pub(super) fn subscription_delivery_call_count(&self, binding_id: BindingId) -> usize {
        self.subscriptions
            .get(&binding_id)
            .expect("test consumer binding is installed")
            .delivery_call_count()
    }

    pub(super) fn channels(
        &self,
    ) -> impl Iterator<Item = (ChannelId, Arc<RuntimeFilterChannel>)> + '_ {
        self.channels
            .iter()
            .map(|(channel_id, channel)| (*channel_id, channel.clone()))
    }

    pub(super) fn router(&self) -> &LoopbackRouter {
        &self.router
    }

    pub(super) fn routes_for_channel(&self, channel_id: ChannelId) -> &[RouteEdgeId] {
        self.channel_routes
            .get(&channel_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(super) fn artifact_plan(&self, channel_id: ChannelId) -> Option<&ChannelArtifactPlan> {
        self.artifact_channels.get(&channel_id)
    }

    pub(super) const fn publish_gate(&self) -> &ArtifactPublishGate {
        &self.publish_gate
    }

    pub(super) fn routes_for_profile(
        &self,
        channel_id: ChannelId,
        profile_id: ConsumerProfileId,
    ) -> &[RouteEdgeId] {
        self.artifact_channels
            .get(&channel_id)
            .and_then(|plan| {
                plan.groups()
                    .iter()
                    .find(|group| group.profile().id() == profile_id)
            })
            .map(CapabilityGroup::route_edges)
            .unwrap_or(&[])
    }

    fn invalidate_artifact_publication(&self) -> BTreeMap<ChannelId, Vec<RouteEdgeId>> {
        let keys = self
            .artifact_channels
            .values()
            .flat_map(|plan| plan.groups().iter().map(CapabilityGroup::key))
            .collect::<Vec<_>>();
        let mut routes = BTreeMap::<ChannelId, Vec<RouteEdgeId>>::new();
        for key in self.publish_gate.cancel_all(keys) {
            routes
                .entry(key.channel_id())
                .or_default()
                .extend_from_slice(self.routes_for_profile(key.channel_id(), key.profile_id()));
        }
        let live_routes = self
            .subscriptions
            .values()
            .filter_map(|subscription| subscription.live_route_edge_id())
            .collect::<BTreeSet<_>>();
        for (channel_id, plan) in &self.artifact_channels {
            for group in plan.groups() {
                routes.entry(*channel_id).or_default().extend(
                    group
                        .route_edges()
                        .iter()
                        .copied()
                        .filter(|route| live_routes.contains(route)),
                );
            }
        }
        for channel_routes in routes.values_mut() {
            channel_routes.sort_unstable();
            channel_routes.dedup();
        }
        routes
    }

    pub(super) fn route_event_identities(
        &self,
        route_edge_id: RouteEdgeId,
    ) -> &[RouteEventIdentity] {
        self.route_event_identities
            .get(&route_edge_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

struct InstallFlight {
    install: RuntimeFilterParticipantInstall,
    result: Mutex<Option<Result<Arc<InstalledDeployment>, InstallContractError>>>,
    completed: Condvar,
}

impl InstallFlight {
    fn new(install: RuntimeFilterParticipantInstall) -> Self {
        Self {
            install,
            result: Mutex::new(None),
            completed: Condvar::new(),
        }
    }

    fn wait(&self) -> Result<Arc<InstalledDeployment>, InstallContractError> {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while result.is_none() {
            result = self
                .completed
                .wait(result)
                .unwrap_or_else(|error| error.into_inner());
        }
        result
            .as_ref()
            .expect("completed install flight has a result")
            .clone()
    }

    fn complete(&self, completed: Result<Arc<InstalledDeployment>, InstallContractError>) {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if result.is_none() {
            *result = Some(completed);
            self.completed.notify_all();
        }
    }
}

enum RegistryState {
    Uninstalled,
    Installing(Arc<InstallFlight>),
    Publishing {
        installed: Arc<InstalledDeployment>,
        flight: Arc<InstallFlight>,
    },
    Installed(Arc<InstalledDeployment>),
    Cancelled(Option<Arc<InstalledDeployment>>),
}

pub(super) struct DeploymentRegistry {
    query_id: UniqueId,
    clock: Arc<dyn RuntimeFilterClock>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    events: Arc<EventEmitter>,
    state: Mutex<RegistryState>,
    #[cfg(test)]
    before_commit_clock: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_commit_before_publish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl DeploymentRegistry {
    pub(super) fn new(
        query_id: UniqueId,
        clock: Arc<dyn RuntimeFilterClock>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        events: Arc<EventEmitter>,
    ) -> Self {
        Self {
            query_id,
            clock,
            memory_account,
            events,
            state: Mutex::new(RegistryState::Uninstalled),
            #[cfg(test)]
            before_commit_clock: Mutex::new(None),
            #[cfg(test)]
            after_commit_before_publish: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn set_after_commit_before_publish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .after_commit_before_publish
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(super) fn set_before_commit_clock_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .before_commit_clock
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    pub(super) fn install(
        &self,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<RegistryInstallResult, InstallContractError> {
        validate_install_identity(&install)?;
        let view = install.core_view();
        if view.is_empty() && install.routing_shard().channels().is_empty() {
            return Ok(RegistryInstallResult {
                outcome: InstallOutcome::IgnoredEmpty,
                committed_at: None,
                events: Vec::new(),
            });
        }
        if view.is_empty() {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "empty core view with nonempty routing shard is not a valid participant install",
            ));
        }
        validate_install(&install)?;

        let (flight, leader) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &*state {
                RegistryState::Cancelled(_) => return Err(cancelled_install()),
                RegistryState::Publishing { installed, .. } => {
                    // Publishing begins after the logical commit. New install calls compare
                    // against that committed deployment and return immediately; only callers
                    // that already observed Installing wait for event publication to finish.
                    return compare_installed(installed, &install);
                }
                RegistryState::Installed(installed) => {
                    return compare_installed(installed, &install);
                }
                RegistryState::Installing(flight) => {
                    compare_installing(flight, &install)?;
                    (flight.clone(), false)
                }
                RegistryState::Uninstalled => {
                    let flight = Arc::new(InstallFlight::new(install.clone()));
                    *state = RegistryState::Installing(flight.clone());
                    (flight, true)
                }
            }
        };
        if !leader {
            let installed = flight.wait()?;
            return compare_installed(&installed, &install);
        }

        let candidate = (|| {
            let built = build_channels(self.query_id, &view, self.memory_account.clone())?;
            let routing = build_routing(
                self.query_id,
                &view,
                &built.channels,
                &built.final_domain_seeds,
                self.events.clone(),
            )?;
            Ok::<_, InstallContractError>((built.channels, routing))
        })();
        let (channels, routing) = match candidate {
            Ok(candidate) => candidate,
            Err(error) => {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                let active = matches!(&*state, RegistryState::Installing(active) if Arc::ptr_eq(active, &flight));
                if active {
                    *state = RegistryState::Uninstalled;
                }
                drop(state);
                if !active {
                    let error = cancelled_install();
                    flight.complete(Err(error.clone()));
                    return Err(error);
                }
                flight.complete(Err(error.clone()));
                return Err(error);
            }
        };

        #[cfg(test)]
        if let Some(hook) = self
            .before_commit_clock
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            hook();
        }
        let mut events = vec![RuntimeFilterEvent::DeploymentInstalled {
            query_id: self.query_id,
            participant_id: view.local_participant_id(),
            epoch: view.epoch(),
        }];
        events.extend(view.channels().keys().map(|channel_id| {
            RuntimeFilterEvent::ChannelPlanned {
                identity: RuntimeFilterEventIdentity::new(
                    self.query_id,
                    view.local_participant_id(),
                    *channel_id,
                    view.epoch(),
                ),
            }
        }));
        let install_batch = self.events.prequeue(events.clone());
        // This is the install's logical commit timestamp; later initialization and publication
        // time counts toward the configured deadline.
        let committed_at = self.clock.now();
        let deadlines = match compute_deadlines(&view, committed_at) {
            Ok(deadlines) => deadlines,
            Err(error) => {
                self.events.abort(install_batch);
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if matches!(&*state, RegistryState::Installing(active) if Arc::ptr_eq(active, &flight))
                {
                    *state = RegistryState::Uninstalled;
                }
                drop(state);
                flight.complete(Err(error.clone()));
                return Err(error);
            }
        };
        for (channel_id, channel) in &channels {
            channel
                .initialize_deadline(
                    *deadlines
                        .get(channel_id)
                        .expect("computed deadline exists for every channel"),
                )
                .expect("unanchored candidate deadline initializes exactly once");
        }
        let publish_gate = ArtifactPublishGate::default();
        for plan in routing.artifact_channels.values() {
            for group in plan.groups() {
                publish_gate.generation(group.key());
            }
        }
        let candidate = Arc::new(InstalledDeployment {
            install: install.clone(),
            committed_at,
            channels,
            deadlines,
            producers: routing.producers,
            consumer_activations: routing.consumer_activations,
            subscriptions: routing.subscriptions,
            router: Arc::new(LoopbackRouter::new(routing.routes)),
            channel_routes: routing.channel_routes,
            route_event_identities: routing.route_event_identities,
            artifact_channels: routing.artifact_channels,
            publish_gate,
        });

        let committed = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(&*state, RegistryState::Installing(active) if Arc::ptr_eq(active, &flight))
            {
                *state = RegistryState::Publishing {
                    installed: candidate.clone(),
                    flight: flight.clone(),
                };
                true
            } else {
                false
            }
        };
        if !committed {
            let error = cancelled_install();
            self.events.abort(install_batch);
            flight.complete(Err(error.clone()));
            return Err(error);
        }
        #[cfg(test)]
        if let Some(hook) = self
            .after_commit_before_publish
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            hook();
        }
        self.events.publish(install_batch);
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(&*state, RegistryState::Publishing { flight: active, .. } if Arc::ptr_eq(active, &flight))
            {
                *state = RegistryState::Installed(candidate.clone());
            }
        }
        flight.complete(Ok(candidate));
        Ok(RegistryInstallResult {
            outcome: InstallOutcome::Installed,
            committed_at: Some(committed_at),
            events,
        })
    }

    pub(super) fn cancel(&self) -> Option<CancelledDeployment> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let (installed, flight) = match &*state {
            RegistryState::Installed(installed) => (Some(installed.clone()), None),
            RegistryState::Publishing { installed, .. } => (Some(installed.clone()), None),
            RegistryState::Installing(flight) => (None, Some(flight.clone())),
            RegistryState::Cancelled(_) => return None,
            RegistryState::Uninstalled => (None, None),
        };
        let cancelled_routes = installed
            .as_ref()
            .map(|installed| installed.invalidate_artifact_publication())
            .unwrap_or_default();
        *state = RegistryState::Cancelled(installed.clone());
        drop(state);
        if let Some(flight) = flight {
            flight.complete(Err(cancelled_install()));
        }
        installed.map(|installed| CancelledDeployment {
            cancelled_routes,
            installed,
        })
    }

    pub(super) fn active_installation(&self) -> Option<Arc<InstalledDeployment>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                Some(installed.clone())
            }
            RegistryState::Uninstalled
            | RegistryState::Installing(_)
            | RegistryState::Cancelled(_) => None,
        }
    }

    pub(super) fn installation_for_dispatch(&self) -> Option<Arc<InstalledDeployment>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                Some(installed.clone())
            }
            RegistryState::Cancelled(installed) => installed.clone(),
            RegistryState::Uninstalled | RegistryState::Installing(_) => None,
        }
    }

    pub(super) fn installed_epoch(&self) -> Option<DeploymentEpoch> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                Some(installed.install.core_view().epoch())
            }
            RegistryState::Uninstalled
            | RegistryState::Installing(_)
            | RegistryState::Cancelled(_) => None,
        }
    }

    pub(super) fn channel_count(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                installed.channels.len()
            }
            RegistryState::Uninstalled
            | RegistryState::Installing(_)
            | RegistryState::Cancelled(_) => 0,
        }
    }

    pub(super) fn channel(&self, channel_id: ChannelId) -> Option<Arc<RuntimeFilterChannel>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                installed.channels.get(&channel_id).cloned()
            }
            RegistryState::Uninstalled
            | RegistryState::Installing(_)
            | RegistryState::Cancelled(_) => None,
        }
    }

    pub(super) fn deadline(&self, channel_id: ChannelId) -> Option<Instant> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RegistryState::Publishing { installed, .. } | RegistryState::Installed(installed) => {
                installed.deadlines.get(&channel_id).copied()
            }
            RegistryState::Uninstalled
            | RegistryState::Installing(_)
            | RegistryState::Cancelled(_) => None,
        }
    }
}

fn compare_installing(
    flight: &InstallFlight,
    incoming: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    if flight.install.epoch() != incoming.epoch() {
        return Err(install_error(
            InstallContractErrorKind::EpochMismatch,
            "runtime filter deployment epoch differs from the installing epoch",
        ));
    }
    if !participant_installs_equivalent(&flight.install, incoming) {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "same deployment epoch carried a different in-flight composite install",
        ));
    }
    Ok(())
}

fn compare_installed(
    installed: &InstalledDeployment,
    incoming: &RuntimeFilterParticipantInstall,
) -> Result<RegistryInstallResult, InstallContractError> {
    if installed.install.epoch() != incoming.epoch() {
        return Err(install_error(
            InstallContractErrorKind::EpochMismatch,
            "runtime filter deployment epoch differs from the installed epoch",
        ));
    }
    if !participant_installs_equivalent(&installed.install, incoming) {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "same deployment epoch carried a different installed composite",
        ));
    }
    Ok(RegistryInstallResult {
        outcome: InstallOutcome::AlreadyInstalled,
        committed_at: Some(installed.committed_at),
        events: Vec::new(),
    })
}

fn validate_install_identity(
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    if install.core_view().epoch() != install.routing_shard().deployment_epoch() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing epochs differ",
        ));
    }
    if install.core_view().local_participant_id() != install.routing_shard().local_participant_id()
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing participants differ",
        ));
    }
    Ok(())
}

fn validate_install(install: &RuntimeFilterParticipantInstall) -> Result<(), InstallContractError> {
    validate_view_with_routing(install.core_view(), Some(install.routing_shard()))
}

fn validate_view<'a>(view: &'a RuntimeFilterInstallView) -> Result<(), InstallContractError> {
    validate_view_with_routing(view, None)
}

fn validate_view_with_routing<'a>(
    view: &'a RuntimeFilterInstallView,
    routing_shard: Option<&RuntimeFilterRoutingShard>,
) -> Result<(), InstallContractError> {
    if view.epoch().get() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidEpoch,
            "deployment epoch must be non-zero",
        ));
    }

    let mut channel_ids = BTreeSet::new();
    let mut binding_ids = BTreeSet::new();
    let mut route_ids = BTreeSet::new();
    for (map_channel_id, channel) in view.channels() {
        if *map_channel_id != channel.channel_id() || !channel_ids.insert(channel.channel_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "channel map key and channel identity must match and be unique",
            ));
        }
        for binding_id in channel.producers().keys() {
            if !binding_ids.insert(*binding_id) {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "producer binding identities must be unique across the install view",
                ));
            }
        }
        for (binding_id, consumer) in channel.consumers() {
            if !binding_ids.insert(*binding_id)
                || !route_ids.insert(consumer.loopback_route_edge_id())
            {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "consumer binding and loopback route identities must be unique",
                ));
            }
        }
    }

    let mut profile_encodings = BTreeMap::<ConsumerProfileId, &'a [u8]>::new();
    for channel in view.channels().values() {
        let allow_consumerless_aggregator = match routing_shard {
            Some(shard) => {
                let routing = shard.channel(channel.channel_id()).ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "core channel {} is missing from routing shard",
                            channel.channel_id().get()
                        ),
                    )
                })?;
                validate_channel_routing_contract(view.local_participant_id(), channel, routing)?
            }
            None => false,
        };
        validate_channel(
            channel,
            &mut profile_encodings,
            allow_consumerless_aggregator,
        )?;
    }
    Ok(())
}

fn validate_channel_routing_contract(
    local_participant_id: crate::runtime_filter::port::identity::RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelDeployment,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<bool, InstallContractError> {
    let is_local_aggregator = routing
        .local_roles()
        .contains(&RuntimeFilterRouteRole::Aggregator);
    for (binding_id, producer) in channel.producers() {
        for fragment_instance_id in producer.expected_fragment_instances() {
            let participant_id = routing
                .producer_participant(*binding_id, *fragment_instance_id)
                .ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "producer binding {} instance {:?} is missing from routing producer index",
                            binding_id.get(),
                            fragment_instance_id
                        ),
                    )
                })?;
            if !is_local_aggregator && participant_id != local_participant_id {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "non-aggregator producer binding {} instance {:?} maps to remote participant {:?}",
                        binding_id.get(),
                        fragment_instance_id,
                        participant_id
                    ),
                ));
            }
            if participant_id == local_participant_id
                && !routing
                    .local_roles()
                    .contains(&RuntimeFilterRouteRole::Producer(*binding_id))
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "local producer binding {} has no matching local Producer role",
                        binding_id.get()
                    ),
                ));
            }
        }
    }

    if is_local_aggregator {
        for ((binding_id, fragment_instance_id), _) in routing.producer_instances() {
            let installed = channel.producers().get(binding_id).is_some_and(|producer| {
                producer
                    .expected_fragment_instances()
                    .contains(fragment_instance_id)
            });
            if !installed {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "aggregator core is missing routing-authorized producer binding {} instance {:?}",
                        binding_id.get(),
                        fragment_instance_id
                    ),
                ));
            }
        }

        let authorized_sources = routing
            .producer_instances()
            .iter()
            .map(|((binding_id, _), participant_id)| (*binding_id, *participant_id))
            .collect::<BTreeSet<_>>();
        for edge in routing.inbound_edges().iter().filter(|edge| {
            matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
                && edge.target().participant_id() == local_participant_id
                && edge.target().role() == RuntimeFilterRouteRole::Aggregator
        }) {
            let RuntimeFilterRouteRole::Producer(binding_id) = edge.source().role() else {
                unreachable!("inbound producer edges were filtered by source role")
            };
            if !authorized_sources.contains(&(binding_id, edge.source().participant_id())) {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "aggregator inbound producer edge source has no authorized producer instance",
                ));
            }
        }
        for (binding_id, source_participant_id) in authorized_sources {
            let matching_edges = routing
                .inbound_edges()
                .iter()
                .filter(|edge| {
                    edge.source().participant_id() == source_participant_id
                        && edge.source().role() == RuntimeFilterRouteRole::Producer(binding_id)
                        && edge.target().participant_id() == local_participant_id
                        && edge.target().role() == RuntimeFilterRouteRole::Aggregator
                })
                .collect::<Vec<_>>();
            if matching_edges.len() != 1 {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "aggregator producer binding {} source participant {:?} requires exactly one inbound Producer-to-Aggregator edge",
                        binding_id.get(),
                        source_participant_id
                    ),
                ));
            }
            let allowed_kinds = matching_edges[0].allowed_kinds();
            if !allowed_kinds.contains(&RuntimeFilterEnvelopeKind::Contribution)
                || !allowed_kinds.contains(&RuntimeFilterEnvelopeKind::ProducerClosed)
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "aggregator producer binding {} source participant {:?} inbound edge must allow Contribution and ProducerClosed",
                        binding_id.get(),
                        source_participant_id
                    ),
                ));
            }
        }
    }

    if !channel.consumers().is_empty() {
        return Ok(false);
    }
    if !is_local_aggregator {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "consumerless channel requires local Aggregator authority",
        ));
    }
    Ok(true)
}

/// Test-only shim so RFD-2 deployment-compiler tests (a different module) can
/// prove their projected `RuntimeFilterInstallView` satisfies this BE-side
/// install contract without duplicating `validate_view`'s logic.
#[cfg(test)]
pub(crate) fn validate_view_for_test(view: &RuntimeFilterInstallView) -> Result<(), String> {
    validate_view(view).map_err(|e| format!("{e}"))
}

struct RoutingBuild {
    producers: BTreeMap<BindingId, ProducerRoute>,
    consumer_activations: BTreeMap<BindingId, ConsumerActivation>,
    subscriptions: BTreeMap<BindingId, Arc<SubscriptionGroup>>,
    routes:
        BTreeMap<RouteEdgeId, Arc<dyn crate::runtime_filter::port::subscription::ArtifactDelivery>>,
    channel_routes: BTreeMap<ChannelId, Vec<RouteEdgeId>>,
    route_event_identities: BTreeMap<RouteEdgeId, Vec<RouteEventIdentity>>,
    artifact_channels: BTreeMap<ChannelId, ChannelArtifactPlan>,
}

fn build_routing(
    query_id: UniqueId,
    view: &RuntimeFilterInstallView,
    channels: &BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>,
    final_domain_seeds: &BTreeMap<ChannelId, FinalDomainAuthoritySeed>,
    events: Arc<dyn RuntimeFilterEventSink>,
) -> Result<RoutingBuild, InstallContractError> {
    let mut build = RoutingBuild {
        producers: BTreeMap::new(),
        consumer_activations: BTreeMap::new(),
        subscriptions: BTreeMap::new(),
        routes: BTreeMap::new(),
        channel_routes: BTreeMap::new(),
        route_event_identities: BTreeMap::new(),
        artifact_channels: BTreeMap::new(),
    };
    for (channel_id, deployment) in view.channels() {
        let common = RuntimeFilterEventIdentity::new(
            query_id,
            view.local_participant_id(),
            *channel_id,
            view.epoch(),
        );
        let channel = channels.get(channel_id).cloned().ok_or_else(|| {
            install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "temporary installed graph is missing a validated channel",
            )
        })?;
        let mut capability_routes =
            BTreeMap::<ConsumerProfileId, (ConsumerArtifactProfile, Vec<RouteEdgeId>)>::new();
        for (binding_id, producer) in deployment.producers() {
            build.producers.insert(
                *binding_id,
                ProducerRoute {
                    channel_id: *channel_id,
                    channel: channel.clone(),
                    expected_instances: producer.expected_fragment_instances().clone(),
                    kind: match deployment.reduction_requirement() {
                        ReductionRequirement::SetUnion
                            if matches!(
                                deployment.completion_requirement(),
                                CompletionRequirement::FencedFinalDomain(
                                    CompletionFenceKind::CommittedDomainFrozen
                                )
                            ) =>
                        {
                            ProducerPortKind::FinalDomain
                        }
                        ReductionRequirement::SetUnion => ProducerPortKind::Membership,
                        ReductionRequirement::TightenOrderedBound => ProducerPortKind::OrderedBound,
                        ReductionRequirement::MergeTopKSummary(_) => ProducerPortKind::TopKSummary,
                    },
                    final_domain_seed: final_domain_seeds.get(channel_id).cloned(),
                },
            );
        }
        for (binding_id, consumer) in deployment.consumers() {
            build
                .consumer_activations
                .insert(*binding_id, consumer.activation());
            let entry = capability_routes
                .entry(consumer.artifact_profile().id())
                .or_insert_with(|| (consumer.artifact_profile().clone(), Vec::new()));
            if entry.0.canonical_bytes() != consumer.artifact_profile().canonical_bytes() {
                return Err(install_error(
                    InstallContractErrorKind::ConflictingDeployment,
                    "consumer profile digest collision carried different canonical bytes",
                ));
            }
            let route_edge_id = consumer.loopback_route_edge_id();
            let group = Arc::new(SubscriptionGroup::new(
                common,
                *binding_id,
                consumer.activation(),
                route_edge_id,
                consumer.expected_fragment_instances().iter().copied(),
                events.clone(),
            ));
            build.subscriptions.insert(*binding_id, group.clone());
            build.routes.insert(route_edge_id, group);
            build
                .channel_routes
                .entry(*channel_id)
                .or_default()
                .push(route_edge_id);
            build.route_event_identities.insert(
                route_edge_id,
                consumer
                    .expected_fragment_instances()
                    .iter()
                    .copied()
                    .map(|instance| {
                        RouteEventIdentity::new(common, *binding_id, instance, route_edge_id)
                    })
                    .collect(),
            );
            entry.1.push(route_edge_id);
        }
        let schema = match deployment.logical_domain() {
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics,
            } => Some(
                ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
                    install_error(
                        InstallContractErrorKind::UnsupportedMembershipType,
                        "membership schema has no canonical artifact encoding",
                    )
                })?,
            ),
            RuntimeFilterLogicalDomain::OrderedBound(_) => None,
        };
        let policy = deployment.materialization_policy();
        let retained_bytes = usize::try_from(policy.max_total_retained_bytes()).map_err(|_| {
            install_error(
                InstallContractErrorKind::InvalidBudget,
                "materialization retained budget does not fit this platform",
            )
        })?;
        let scratch_per_job =
            usize::try_from(policy.max_scratch_bytes_per_job()).map_err(|_| {
                install_error(
                    InstallContractErrorKind::InvalidBudget,
                    "materialization scratch budget does not fit this platform",
                )
            })?;
        let scratch_total = policy.aggregate_scratch_bytes().map_err(|_| {
            install_error(
                InstallContractErrorKind::InvalidBudget,
                "materialization aggregate scratch budget overflows",
            )
        })?;
        let groups = capability_routes
            .into_iter()
            .map(|(profile_id, (profile, mut route_edges))| {
                route_edges.sort_unstable();
                route_edges.dedup();
                CapabilityGroup {
                    key: ArtifactPublishKey::new(*channel_id, view.epoch(), profile_id),
                    common,
                    profile,
                    route_edges: route_edges.into(),
                }
            })
            .collect::<Vec<_>>();
        build.artifact_channels.insert(
            *channel_id,
            ChannelArtifactPlan {
                schema,
                policy,
                max_artifact_bytes: usize::try_from(deployment.policy().max_artifact_bytes)
                    .map_err(|_| {
                        install_error(
                            InstallContractErrorKind::InvalidBudget,
                            "artifact byte budget does not fit this platform",
                        )
                    })?,
                max_concurrent_jobs: policy.max_concurrent_jobs(),
                retained_budget: Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
                scratch_budget: Arc::new(
                    ArtifactScratchBudget::new(scratch_per_job, scratch_total).map_err(|_| {
                        install_error(
                            InstallContractErrorKind::InvalidBudget,
                            "materialization scratch budget is inconsistent",
                        )
                    })?,
                ),
                groups: groups.into(),
            },
        );
    }
    Ok(build)
}

fn validate_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    allow_consumerless_aggregator: bool,
) -> Result<(), InstallContractError> {
    if matches!(
        channel.logical_domain(),
        RuntimeFilterLogicalDomain::OrderedBound(_)
    ) {
        return validate_ordered_channel(channel, profile_encodings, allow_consumerless_aggregator);
    }
    let RuntimeFilterLogicalDomain::Membership {
        value_type,
        null_semantics,
    } = channel.logical_domain()
    else {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M1 supports only Membership logical domains",
        ));
    };
    let ordinary = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement() == CompletionRequirement::ProducerClosed;
    let fenced_final = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement()
            == CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
        && *null_semantics == crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual
        && channel.availability_coverage().is_all_of_only()
        && channel.terminal_coverage().is_all_of_only();
    if !ordinary && !fenced_final {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "channel does not match the CompleteOnce Membership SetUnion matrix",
        ));
    }
    if MembershipValues::empty_for_data_type(value_type).is_none() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership data type is not supported by the runtime filter port",
        ));
    }
    if channel.producers().is_empty()
        || (channel.consumers().is_empty() && !allow_consumerless_aggregator)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M1 channel requires at least one producer and one consumer binding",
        ));
    }
    if channel.policy().max_contribution_bytes == 0
        || channel.policy().max_artifact_bytes == 0
        || channel.policy().deadline_ms == 0
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max contribution bytes, max artifact bytes, and completion deadline must be non-zero",
        ));
    }
    if channel.core_budget().max_reducer_bytes() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidBudget,
            "max reducer bytes must be non-zero",
        ));
    }
    let materialization_policy = channel.materialization_policy();
    usize::try_from(materialization_policy.max_total_retained_bytes()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization retained budget does not fit this platform",
        )
    })?;
    usize::try_from(materialization_policy.max_scratch_bytes_per_job()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization scratch budget does not fit this platform",
        )
    })?;
    materialization_policy
        .aggregate_scratch_bytes()
        .map_err(|_| {
            install_error(
                InstallContractErrorKind::InvalidBudget,
                "materialization aggregate scratch budget overflows",
            )
        })?;
    let schema = ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
        install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership schema has no canonical artifact encoding",
        )
    })?;
    let mut producer_witnesses = BTreeSet::new();
    if channel
        .producers()
        .values()
        .any(|producer| !producer_witnesses.insert(producer.coverage_witness_id()))
    {
        return Err(install_error(
            InstallContractErrorKind::DuplicateCoverageWitness,
            "producer witness identities must be unique within a channel",
        ));
    }
    if !channel
        .availability_coverage()
        .is_canonically_equivalent_to(channel.terminal_coverage())
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidCoverage,
            "CompleteOnce availability and terminal coverage must be canonically equivalent",
        ));
    }
    validate_coverage(channel.availability_coverage(), channel)?;
    validate_coverage(channel.terminal_coverage(), channel)?;

    for producer in channel.producers().values() {
        if producer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer expected fragment instance set must be non-empty",
            ));
        }
    }
    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if consumer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "consumer expected fragment instance set must be non-empty",
            ));
        }
        if ordinary && consumer.activation() != ConsumerActivation::BlockingSnapshot {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "M1 consumers must use BlockingSnapshot activation",
            ));
        }
        if fenced_final
            && !matches!(
                consumer.activation(),
                ConsumerActivation::NonBlockingLive { .. }
            )
        {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "fenced-final consumers must use NonBlockingLive activation",
            ));
        }
        let capabilities = consumer.capabilities();
        let profile = consumer.artifact_profile();
        unique_profiles.insert((profile.id(), profile.canonical_bytes()));
        if !capabilities.contains(&ArtifactCapability::Membership)
            || !capabilities.contains(&ArtifactCapability::EmptyDomain)
        {
            return Err(install_error(
                InstallContractErrorKind::MissingMembershipCapability,
                "M2 Membership consumers must declare Membership and EmptyDomain semantics",
            ));
        }
        if fenced_final
            && (capabilities
                != &BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ])
                || profile.accepted_kinds()
                    != &BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]))
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "fenced-final consumers require exact Membership and EmptyDomain semantics",
            ));
        }
        if !profile.accepts(ArtifactKind::EmptyDomain) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "M2 Membership profile must accept EmptyDomain",
            ));
        }
        let value_set = profile.accepts(ArtifactKind::ValueSet);
        let bitset = profile.accepts(ArtifactKind::Bitset) && bitset_schema_is_feasible(value_type);
        let bloom = profile.accepts(ArtifactKind::Bloom);
        if !value_set && !bitset && !bloom {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "M2 Membership profile has no statically feasible membership representation",
            ));
        }
        if matches!(
            null_semantics,
            crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual
        ) && !value_set
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "NullSafeEqual Membership profile must accept ValueSet",
            ));
        }
        if profile.accepts(ArtifactKind::Range)
            && !capabilities.contains(&ArtifactCapability::OrderedRange)
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Range physical kind requires OrderedRange semantic capability",
            ));
        }
        if profile.accepted_kinds().iter().any(|kind| {
            matches!(
                kind,
                ArtifactKind::ValueSet | ArtifactKind::Bloom | ArtifactKind::Bitset
            )
        }) && !capabilities.contains(&ArtifactCapability::Membership)
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "membership physical kinds require Membership semantic capability",
            ));
        }
        if let Some(existing) = profile_encodings.insert(profile.id(), profile.canonical_bytes())
            && existing != profile.canonical_bytes()
        {
            return Err(install_error(
                InstallContractErrorKind::ConflictingDeployment,
                "consumer profile digest collision carried different canonical bytes",
            ));
        }
        if profile.accepts(ArtifactKind::Bloom) {
            let expected = BloomHashContract::new(&schema, materialization_policy)
                .map_err(|_| {
                    install_error(
                        InstallContractErrorKind::InvalidPolicy,
                        "materialization Bloom policy is not supported",
                    )
                })?
                .digest();
            if profile.bloom_hash_contract() != Some(expected) {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "Bloom profile hash contract does not match channel schema and policy",
                ));
            }
        }
    }
    if !allow_consumerless_aggregator
        && materialization_policy.max_concurrent_jobs() > unique_profiles.len()
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max concurrent materialization jobs exceeds normalized unique profile count",
        ));
    }
    Ok(())
}

fn validate_ordered_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    allow_consumerless_aggregator: bool,
) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::OrderedBound(plan) = channel.logical_domain() else {
        unreachable!("ordered validator is called only for ordered channels")
    };
    let contract = RuntimeOrderContract::try_from_plan(plan).map_err(|error| {
        install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            format!("ordered channel has an invalid order contract: {error:?}"),
        )
    })?;
    match channel.reduction_requirement() {
        ReductionRequirement::TightenOrderedBound => {
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound M3A matrix",
                ));
            }
        }
        ReductionRequirement::MergeTopKSummary(requirement) => {
            RuntimeTopKSummaryContract::try_from_plan(plan, requirement).map_err(|error| {
                install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!("ordered channel has an invalid top-k summary contract: {error:?}"),
                )
            })?;
            if !channel
                .availability_coverage()
                .is_canonically_equivalent_to(channel.terminal_coverage())
            {
                return Err(install_error(
                    InstallContractErrorKind::InvalidCoverage,
                    "top-k summary availability and terminal coverage must be canonically equivalent",
                ));
            }
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
                || !channel.availability_coverage().is_all_of_only()
                || !channel.terminal_coverage().is_all_of_only()
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound TopKSummary M3B matrix",
                ));
            }
        }
        ReductionRequirement::SetUnion => {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered channel cannot use SetUnion reduction",
            ));
        }
    }
    if channel.producers().is_empty()
        || (channel.consumers().is_empty() && !allow_consumerless_aggregator)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "ordered channel requires at least one producer and one consumer binding",
        ));
    }
    if channel.policy().max_contribution_bytes == 0
        || channel.policy().max_artifact_bytes == 0
        || channel.policy().deadline_ms == 0
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max contribution bytes, max artifact bytes, and completion deadline must be non-zero",
        ));
    }
    if channel.core_budget().max_reducer_bytes() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidBudget,
            "max reducer bytes must be non-zero",
        ));
    }
    let materialization_policy = channel.materialization_policy();
    usize::try_from(materialization_policy.max_total_retained_bytes()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization retained budget does not fit this platform",
        )
    })?;
    usize::try_from(materialization_policy.max_scratch_bytes_per_job()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization scratch budget does not fit this platform",
        )
    })?;
    materialization_policy
        .aggregate_scratch_bytes()
        .map_err(|_| {
            install_error(
                InstallContractErrorKind::InvalidBudget,
                "materialization aggregate scratch budget overflows",
            )
        })?;

    let mut producer_witnesses = BTreeSet::new();
    for producer in channel.producers().values() {
        if !producer_witnesses.insert(producer.coverage_witness_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateCoverageWitness,
                "producer witness identities must be unique within a channel",
            ));
        }
        if producer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer expected fragment instance set must be non-empty",
            ));
        }
    }
    validate_coverage(channel.availability_coverage(), channel)?;
    validate_coverage(channel.terminal_coverage(), channel)?;

    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if consumer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "consumer expected fragment instance set must be non-empty",
            ));
        }
        if !matches!(
            consumer.activation(),
            ConsumerActivation::NonBlockingLive { .. }
        ) {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "ordered consumers must use NonBlockingLive activation",
            ));
        }
        if consumer.capabilities() != &BTreeSet::from([ArtifactCapability::OrderedRange]) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumers must declare exactly OrderedRange capability",
            ));
        }
        let profile = consumer.artifact_profile();
        if profile.accepted_kinds() != &BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(contract.digest())
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumer profile must accept only Range with the channel order digest",
            ));
        }
        unique_profiles.insert((profile.id(), profile.canonical_bytes()));
        if let Some(existing) = profile_encodings.insert(profile.id(), profile.canonical_bytes())
            && existing != profile.canonical_bytes()
        {
            return Err(install_error(
                InstallContractErrorKind::ConflictingDeployment,
                "consumer profile digest collision carried different canonical bytes",
            ));
        }
    }
    if !allow_consumerless_aggregator
        && materialization_policy.max_concurrent_jobs() > unique_profiles.len()
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max concurrent materialization jobs exceeds normalized unique profile count",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn validate_channel_contract(
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    validate_channel(channel, &mut BTreeMap::new(), false)
}

fn bitset_schema_is_feasible(data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Decimal128(1..=18, _)
    )
}

fn validate_coverage(
    coverage: &Coverage,
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    coverage.validate_shape().map_err(|error| {
        install_error(
            InstallContractErrorKind::InvalidCoverage,
            format!("invalid coverage shape: {error:?}"),
        )
    })?;
    let expected = channel
        .producers()
        .values()
        .map(|producer| producer.coverage_witness_id())
        .collect::<BTreeSet<_>>();
    let mut counts = BTreeMap::new();
    count_witnesses(coverage, &mut counts);
    if counts.keys().any(|witness| !expected.contains(witness)) {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage references a witness without an installed producer",
        ));
    }
    if counts.values().any(|count| *count != 1) {
        return Err(install_error(
            InstallContractErrorKind::DuplicateCoverageWitness,
            "coverage must reference each producer witness exactly once",
        ));
    }
    if counts.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage must reference every installed producer witness",
        ));
    }
    Ok(())
}

fn count_witnesses(coverage: &Coverage, counts: &mut BTreeMap<CoverageWitnessId, usize>) {
    match coverage {
        Coverage::Leaf(witness) => *counts.entry(*witness).or_default() += 1,
        Coverage::AllOf(children) | Coverage::AnyOf(children) => {
            for child in children {
                count_witnesses(child, counts);
            }
        }
    }
}

fn compute_deadlines(
    view: &RuntimeFilterInstallView,
    committed_at: Instant,
) -> Result<BTreeMap<ChannelId, Instant>, InstallContractError> {
    view.channels()
        .iter()
        .map(|(channel_id, channel)| {
            committed_at
                .checked_add(Duration::from_millis(channel.policy().deadline_ms))
                .map(|deadline| (*channel_id, deadline))
                .ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::InvalidPolicy,
                        "completion deadline overflows the monotonic clock",
                    )
                })
        })
        .collect()
}

struct BuiltChannels {
    channels: BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>,
    final_domain_seeds: BTreeMap<ChannelId, FinalDomainAuthoritySeed>,
}

fn build_channels(
    query_id: UniqueId,
    view: &RuntimeFilterInstallView,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<BuiltChannels, InstallContractError> {
    let mut channels = BTreeMap::new();
    let mut final_domain_seeds = BTreeMap::new();
    for (channel_id, deployment) in view.channels() {
        let final_domain_contract = match (
            deployment.logical_domain(),
            deployment.completion_requirement(),
        ) {
            (
                RuntimeFilterLogicalDomain::Membership {
                    value_type,
                    null_semantics,
                },
                CompletionRequirement::FencedFinalDomain(fence_kind),
            ) => {
                let schema =
                    ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
                        install_error(
                            InstallContractErrorKind::UnsupportedChannelContract,
                            "fenced-final channel has an unsupported membership schema",
                        )
                    })?;
                Some(Arc::new(
                    RuntimeCompletionFenceContract::try_from_install(
                        query_id,
                        view.epoch(),
                        *channel_id,
                        fence_kind,
                        &schema,
                    )
                    .map_err(|error| {
                        install_error(
                            InstallContractErrorKind::UnsupportedChannelContract,
                            error.to_string(),
                        )
                    })?,
                ))
            }
            _ => None,
        };
        let channel = RuntimeFilterChannel::new_unanchored_with_final_domain_contract(
            query_id,
            view.local_participant_id(),
            view.epoch(),
            deployment,
            memory_account.clone(),
            final_domain_contract.clone(),
        )
        .map_err(|error| {
            install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                error.to_string(),
            )
        })?;
        channels.insert(*channel_id, Arc::new(channel));
        if let Some(contract) = final_domain_contract {
            final_domain_seeds.insert(*channel_id, FinalDomainAuthoritySeed::new(contract));
        }
    }
    Ok(BuiltChannels {
        channels,
        final_domain_seeds,
    })
}

fn install_views_equivalent(
    left: &RuntimeFilterInstallView,
    right: &RuntimeFilterInstallView,
) -> bool {
    left.epoch() == right.epoch()
        && left.local_participant_id() == right.local_participant_id()
        && left.channels().len() == right.channels().len()
        && left.channels().iter().all(|(channel_id, left_channel)| {
            right
                .channels()
                .get(channel_id)
                .is_some_and(|right_channel| channels_equivalent(left_channel, right_channel))
        })
}

fn participant_installs_equivalent(
    left: &RuntimeFilterParticipantInstall,
    right: &RuntimeFilterParticipantInstall,
) -> bool {
    install_views_equivalent(left.core_view(), right.core_view())
        && left.routing_shard() == right.routing_shard()
}

fn channels_equivalent(
    left: &RuntimeFilterChannelDeployment,
    right: &RuntimeFilterChannelDeployment,
) -> bool {
    left.channel_id() == right.channel_id()
        && left.logical_domain() == right.logical_domain()
        && left.lifecycle() == right.lifecycle()
        && left
            .availability_coverage()
            .is_canonically_equivalent_to(right.availability_coverage())
        && left
            .terminal_coverage()
            .is_canonically_equivalent_to(right.terminal_coverage())
        && left.reduction_requirement() == right.reduction_requirement()
        && left.allowed_contribution_kinds() == right.allowed_contribution_kinds()
        && left.completion_requirement() == right.completion_requirement()
        && left.policy() == right.policy()
        && left.core_budget() == right.core_budget()
        && left.materialization_policy() == right.materialization_policy()
        && left.producers() == right.producers()
        && left.consumers() == right.consumers()
}

fn cancelled_install() -> InstallContractError {
    install_error(
        InstallContractErrorKind::ServiceClosed,
        "runtime filter deployment registry is cancelled",
    )
}

fn install_error(
    kind: InstallContractErrorKind,
    detail: impl Into<String>,
) -> InstallContractError {
    InstallContractError::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::materializer::bloom::BloomHashContract;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::{
        ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile, ConsumerProfileId,
    };
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::runtime_filter::port::producer::{
        InstallContractError, InstallContractErrorKind, InstallOutcome,
    };
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
        RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

    use super::{
        DeploymentRegistry, EventEmitter, RuntimeOrderContract, validate_channel_contract,
        validate_view,
    };

    #[derive(Default)]
    struct Account;

    impl RuntimeFilterMemoryAccount for Account {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _bytes: usize) {}
    }

    struct Clock(Instant);

    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            self.0
        }
    }

    struct CountingClock {
        now: Instant,
        calls: AtomicUsize,
    }

    impl RuntimeFilterClock for CountingClock {
        fn now(&self) -> Instant {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.now
        }
    }

    struct ReentrantClock {
        registry: Mutex<Weak<DeploymentRegistry>>,
        now: Instant,
    }

    impl RuntimeFilterClock for ReentrantClock {
        fn now(&self) -> Instant {
            let registry = self
                .registry
                .lock()
                .unwrap()
                .upgrade()
                .expect("registry installed before clock use");
            assert_eq!(registry.installed_epoch(), None);
            assert_eq!(registry.channel_count(), 0);
            self.now
        }
    }

    struct NoopEvents;

    impl RuntimeFilterEventSink for NoopEvents {
        fn record(&self, _event: RuntimeFilterEvent) {}
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId { hi: 7, lo }
    }

    fn channel(
        channel: u32,
        producer: u32,
        witness: u32,
        consumer: u32,
        route: u32,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(channel),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(CoverageWitnessId::new(witness)),
            Coverage::Leaf(CoverageWitnessId::new(witness)),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 3,
            },
            RuntimeFilterCoreBudget::new(4096),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(producer),
                ProducerDeployment::new(
                    CoverageWitnessId::new(witness),
                    BTreeSet::from([uid(producer.into())]),
                ),
            )]),
            BTreeMap::from([(
                BindingId::new(consumer),
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    RouteEdgeId::new(route),
                    BTreeSet::from([uid(consumer.into())]),
                ),
            )]),
        )
    }

    fn fenced_final_channel() -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(20);
        let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 3,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::from([(
                BindingId::new(30),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    ConsumerArtifactProfile::new(
                        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                        None,
                    )
                    .unwrap(),
                    RouteEdgeId::new(40),
                    BTreeSet::from([uid(30)]),
                ),
            )]),
        )
    }

    #[test]
    fn fenced_final_install_accepts_only_the_exact_aggregate_matrix_and_routes_typed_port() {
        let deployment = fenced_final_channel();
        assert!(validate_channel_contract(&deployment).is_ok());

        let registry = registry();
        registry.install(view([(1, deployment)])).unwrap();
        assert_eq!(
            registry
                .active_installation()
                .unwrap()
                .producer(BindingId::new(10))
                .unwrap()
                .kind,
            crate::runtime_filter::port::producer::ProducerPortKind::FinalDomain
        );
    }

    #[test]
    fn fenced_final_install_rejects_non_allof_or_blocking_consumer_contracts() {
        let valid = fenced_final_channel();
        let not_all_of = RuntimeFilterChannelDeployment::new(
            valid.channel_id(),
            valid.logical_domain().clone(),
            valid.lifecycle(),
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(20))]),
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(20))]),
            valid.reduction_requirement(),
            valid.allowed_contribution_kinds().clone(),
            valid.completion_requirement(),
            valid.policy(),
            valid.core_budget(),
            valid.materialization_policy(),
            valid.producers().clone(),
            valid.consumers().clone(),
        );
        assert!(validate_channel_contract(&not_all_of).is_err());

        let mut consumers = valid.consumers().clone();
        let consumer = consumers.get(&BindingId::new(30)).unwrap();
        consumers.insert(
            BindingId::new(30),
            ConsumerDeployment::with_profile(
                ConsumerActivation::BlockingSnapshot,
                consumer.capabilities().clone(),
                consumer.artifact_profile().clone(),
                consumer.loopback_route_edge_id(),
                consumer.expected_fragment_instances().clone(),
            ),
        );
        let blocking = RuntimeFilterChannelDeployment::new(
            valid.channel_id(),
            valid.logical_domain().clone(),
            valid.lifecycle(),
            valid.availability_coverage().clone(),
            valid.terminal_coverage().clone(),
            valid.reduction_requirement(),
            valid.allowed_contribution_kinds().clone(),
            valid.completion_requirement(),
            valid.policy(),
            valid.core_budget(),
            valid.materialization_policy(),
            valid.producers().clone(),
            consumers,
        );
        assert!(validate_channel_contract(&blocking).is_err());
    }

    struct OrderedDeploymentFixture(RuntimeFilterChannelDeployment);

    impl std::ops::Deref for OrderedDeploymentFixture {
        type Target = RuntimeFilterChannelDeployment;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl OrderedDeploymentFixture {
        fn with_consumer(
            &self,
            activation: ConsumerActivation,
            capabilities: BTreeSet<ArtifactCapability>,
            profile: ConsumerArtifactProfile,
        ) -> RuntimeFilterChannelDeployment {
            let (binding, consumer) = self.0.consumers().iter().next().unwrap();
            RuntimeFilterChannelDeployment::new(
                self.0.channel_id(),
                self.0.logical_domain().clone(),
                self.0.lifecycle(),
                self.0.availability_coverage().clone(),
                self.0.terminal_coverage().clone(),
                self.0.reduction_requirement(),
                self.0.allowed_contribution_kinds().clone(),
                self.0.completion_requirement(),
                self.0.policy(),
                self.0.core_budget(),
                self.0.materialization_policy(),
                self.0.producers().clone(),
                BTreeMap::from([(
                    *binding,
                    ConsumerDeployment::with_profile(
                        activation,
                        capabilities,
                        profile,
                        consumer.loopback_route_edge_id(),
                        consumer.expected_fragment_instances().clone(),
                    ),
                )]),
            )
        }

        fn with_blocking_consumer(&self) -> RuntimeFilterChannelDeployment {
            let consumer = self.0.consumers().values().next().unwrap();
            self.with_consumer(
                ConsumerActivation::BlockingSnapshot,
                consumer.capabilities().clone(),
                consumer.artifact_profile().clone(),
            )
        }

        fn without_range_profile(&self) -> RuntimeFilterChannelDeployment {
            let consumer = self.0.consumers().values().next().unwrap();
            self.with_consumer(
                consumer.activation(),
                consumer.capabilities().clone(),
                ConsumerArtifactProfile::m1_test_default(),
            )
        }
    }

    fn ordered_deployment_fixture() -> OrderedDeploymentFixture {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        let order_digest = RuntimeOrderContract::try_from_plan(&plan).unwrap().digest();
        let witness = CoverageWitnessId::new(20);
        OrderedDeploymentFixture(RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(plan),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 3,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::from([(
                BindingId::new(30),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                    ConsumerArtifactProfile::new_ordered_range(order_digest).unwrap(),
                    RouteEdgeId::new(40),
                    BTreeSet::from([uid(30)]),
                ),
            )]),
        ))
    }

    struct TopKDeploymentFixture(RuntimeFilterChannelDeployment);

    impl std::ops::Deref for TopKDeploymentFixture {
        type Target = RuntimeFilterChannelDeployment;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl TopKDeploymentFixture {
        fn rebuild(
            &self,
            availability: Coverage,
            reduction: ReductionRequirement,
            contributions: BTreeSet<ContributionKind>,
            consumer: ConsumerDeployment,
        ) -> RuntimeFilterChannelDeployment {
            let consumer_binding = *self.0.consumers().keys().next().unwrap();
            RuntimeFilterChannelDeployment::new(
                self.0.channel_id(),
                self.0.logical_domain().clone(),
                self.0.lifecycle(),
                availability,
                self.0.terminal_coverage().clone(),
                reduction,
                contributions,
                self.0.completion_requirement(),
                self.0.policy(),
                self.0.core_budget(),
                self.0.materialization_policy(),
                self.0.producers().clone(),
                BTreeMap::from([(consumer_binding, consumer)]),
            )
        }

        fn consumer(&self) -> &ConsumerDeployment {
            self.0.consumers().values().next().unwrap()
        }
    }

    fn topk_deployment_fixture() -> TopKDeploymentFixture {
        let direct = ordered_deployment_fixture();
        let witness = direct
            .producers()
            .values()
            .next()
            .unwrap()
            .coverage_witness_id();
        TopKDeploymentFixture(RuntimeFilterChannelDeployment::new(
            direct.channel_id(),
            direct.logical_domain().clone(),
            direct.lifecycle(),
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(4).unwrap()),
            BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]),
            direct.completion_requirement(),
            direct.policy(),
            direct.core_budget(),
            direct.materialization_policy(),
            direct.producers().clone(),
            direct.consumers().clone(),
        ))
    }

    #[test]
    fn topk_install_accepts_exact_k4_summary_matrix_and_preserves_direct_matrix() {
        assert!(validate_channel_contract(&topk_deployment_fixture()).is_ok());
        assert!(validate_channel_contract(&ordered_deployment_fixture()).is_ok());
    }

    #[test]
    fn topk_install_rejects_non_equivalent_allof_coverages() {
        let fixture = topk_deployment_fixture();
        let mismatched = fixture.rebuild(
            Coverage::AllOf(vec![fixture.availability_coverage().clone()]),
            fixture.reduction_requirement(),
            fixture.allowed_contribution_kinds().clone(),
            fixture.consumer().clone(),
        );

        assert_eq!(
            validate_channel_contract(&mismatched).unwrap_err().kind(),
            InstallContractErrorKind::InvalidCoverage
        );
    }

    #[test]
    fn topk_producer_route_kind_follows_summary_reduction_not_ordered_domain() {
        let registry = registry();
        registry
            .install(view([(1, topk_deployment_fixture().0)]))
            .unwrap();
        let installed = registry.active_installation().unwrap();

        assert_eq!(
            installed.producer(BindingId::new(10)).unwrap().kind,
            crate::runtime_filter::port::producer::ProducerPortKind::TopKSummary
        );
    }

    #[test]
    fn topk_install_rejects_wrong_reduction_or_mixed_direct_contribution_matrix() {
        let fixture = topk_deployment_fixture();
        let consumer = fixture.consumer().clone();
        let wrong_reduction = fixture.rebuild(
            fixture.availability_coverage().clone(),
            ReductionRequirement::TightenOrderedBound,
            fixture.allowed_contribution_kinds().clone(),
            consumer.clone(),
        );
        assert!(validate_channel_contract(&wrong_reduction).is_err());

        let mixed = fixture.rebuild(
            fixture.availability_coverage().clone(),
            fixture.reduction_requirement(),
            BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            consumer,
        );
        assert!(validate_channel_contract(&mixed).is_err());
    }

    #[test]
    fn topk_install_rejects_anyof_blocking_consumer_and_wrong_range_digest() {
        let fixture = topk_deployment_fixture();
        let consumer = fixture.consumer();
        let any_of = fixture.rebuild(
            Coverage::AnyOf(vec![fixture.availability_coverage().clone()]),
            fixture.reduction_requirement(),
            fixture.allowed_contribution_kinds().clone(),
            consumer.clone(),
        );
        assert!(validate_channel_contract(&any_of).is_err());

        let blocking = fixture.rebuild(
            fixture.availability_coverage().clone(),
            fixture.reduction_requirement(),
            fixture.allowed_contribution_kinds().clone(),
            ConsumerDeployment::with_profile(
                ConsumerActivation::BlockingSnapshot,
                consumer.capabilities().clone(),
                consumer.artifact_profile().clone(),
                consumer.loopback_route_edge_id(),
                consumer.expected_fragment_instances().clone(),
            ),
        );
        assert!(validate_channel_contract(&blocking).is_err());

        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        let wrong_plan = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        let wrong_digest = RuntimeOrderContract::try_from_plan(&wrong_plan)
            .unwrap()
            .digest();
        let wrong_range = fixture.rebuild(
            fixture.availability_coverage().clone(),
            fixture.reduction_requirement(),
            fixture.allowed_contribution_kinds().clone(),
            ConsumerDeployment::with_profile(
                consumer.activation(),
                consumer.capabilities().clone(),
                ConsumerArtifactProfile::new_ordered_range(wrong_digest).unwrap(),
                consumer.loopback_route_edge_id(),
                consumer.expected_fragment_instances().clone(),
            ),
        );
        assert!(validate_channel_contract(&wrong_range).is_err());
    }

    #[test]
    fn ordered_install_requires_live_range_profile_and_exact_matrix() {
        let valid = ordered_deployment_fixture();
        assert!(validate_channel_contract(&valid).is_ok());
        assert!(validate_channel_contract(&valid.with_blocking_consumer()).is_err());
        assert!(validate_channel_contract(&valid.without_range_profile()).is_err());
    }

    #[derive(Clone, Copy)]
    enum InvalidDeployment {
        Lifecycle,
        Reduction,
        Contributions,
        Completion,
        UnsupportedType,
        ZeroContributionLimit,
        ZeroArtifactLimit,
        ZeroDeadline,
        ZeroBudget,
        CoverageMismatch,
        CoverageShape,
        UnknownWitness,
        DuplicateCoverageWitness,
        EmptyProducerInstances,
        EmptyConsumerInstances,
        NonBlockingConsumer,
        MissingMembershipCapability,
        MissingProducer,
        MissingConsumer,
        DuplicateProducerWitness,
    }

    fn invalid_deployment(case: InvalidDeployment) -> RuntimeFilterChannelDeployment {
        let base = channel(1, 10, 20, 30, 40);
        let mut logical_domain = base.logical_domain().clone();
        let mut lifecycle = base.lifecycle();
        let mut availability = base.availability_coverage().clone();
        let mut terminal = base.terminal_coverage().clone();
        let mut reduction = base.reduction_requirement();
        let mut contributions = base.allowed_contribution_kinds().clone();
        let mut completion = base.completion_requirement();
        let mut policy = base.policy();
        let mut budget = base.core_budget();
        let mut producers = base.producers().clone();
        let mut consumers = base.consumers().clone();
        match case {
            InvalidDeployment::Lifecycle => lifecycle = RuntimeFilterLifecycle::MonotonicUpdates,
            InvalidDeployment::Reduction => reduction = ReductionRequirement::TightenOrderedBound,
            InvalidDeployment::Contributions => {
                contributions = BTreeSet::from([ContributionKind::ValueDomainDelta]);
            }
            InvalidDeployment::Completion => {
                completion = CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                );
            }
            InvalidDeployment::UnsupportedType => {
                logical_domain = RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "x",
                        DataType::Int64,
                        false,
                    ))),
                    null_semantics: NullSemantics::NeverMatches,
                };
            }
            InvalidDeployment::ZeroContributionLimit => policy.max_contribution_bytes = 0,
            InvalidDeployment::ZeroArtifactLimit => policy.max_artifact_bytes = 0,
            InvalidDeployment::ZeroDeadline => policy.deadline_ms = 0,
            InvalidDeployment::ZeroBudget => budget = RuntimeFilterCoreBudget::new(0),
            InvalidDeployment::CoverageMismatch => {
                terminal = Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(20))]);
            }
            InvalidDeployment::CoverageShape => {
                availability = Coverage::AllOf(Vec::new());
                terminal = availability.clone();
            }
            InvalidDeployment::UnknownWitness => {
                availability = Coverage::Leaf(CoverageWitnessId::new(99));
                terminal = availability.clone();
            }
            InvalidDeployment::DuplicateCoverageWitness => {
                availability = Coverage::AllOf(vec![
                    Coverage::Leaf(CoverageWitnessId::new(20)),
                    Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(20))]),
                ]);
                terminal = availability.clone();
            }
            InvalidDeployment::EmptyProducerInstances => {
                producers.insert(
                    BindingId::new(10),
                    ProducerDeployment::new(CoverageWitnessId::new(20), BTreeSet::new()),
                );
            }
            InvalidDeployment::EmptyConsumerInstances => {
                consumers.insert(
                    BindingId::new(30),
                    ConsumerDeployment::new(
                        ConsumerActivation::BlockingSnapshot,
                        BTreeSet::from([ArtifactCapability::Membership]),
                        RouteEdgeId::new(40),
                        BTreeSet::new(),
                    ),
                );
            }
            InvalidDeployment::NonBlockingConsumer => {
                consumers.insert(
                    BindingId::new(30),
                    ConsumerDeployment::new(
                        ConsumerActivation::NonBlockingLive {
                            late_apply: LateApplyGranularity::Batch,
                        },
                        BTreeSet::from([ArtifactCapability::Membership]),
                        RouteEdgeId::new(40),
                        BTreeSet::from([uid(30)]),
                    ),
                );
            }
            InvalidDeployment::MissingMembershipCapability => {
                consumers.insert(
                    BindingId::new(30),
                    ConsumerDeployment::new(
                        ConsumerActivation::BlockingSnapshot,
                        BTreeSet::from([ArtifactCapability::EmptyDomain]),
                        RouteEdgeId::new(40),
                        BTreeSet::from([uid(30)]),
                    ),
                );
            }
            InvalidDeployment::MissingProducer => producers.clear(),
            InvalidDeployment::MissingConsumer => consumers.clear(),
            InvalidDeployment::DuplicateProducerWitness => {
                producers.insert(
                    BindingId::new(11),
                    ProducerDeployment::new(CoverageWitnessId::new(20), BTreeSet::from([uid(11)])),
                );
            }
        }
        RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            logical_domain,
            lifecycle,
            availability,
            terminal,
            reduction,
            contributions,
            completion,
            policy,
            budget,
            base.materialization_policy(),
            producers,
            consumers,
        )
    }

    fn core_view(
        channels: impl IntoIterator<Item = (u32, RuntimeFilterChannelDeployment)>,
    ) -> RuntimeFilterInstallView {
        RuntimeFilterInstallView::new(
            DeploymentEpoch::new(9),
            RuntimeFilterParticipantId::new(3),
            channels
                .into_iter()
                .map(|(key, value)| (ChannelId::new(key), value))
                .collect(),
        )
    }

    fn view(
        channels: impl IntoIterator<Item = (u32, RuntimeFilterChannelDeployment)>,
    ) -> RuntimeFilterParticipantInstall {
        local_install(core_view(channels))
    }

    fn participant_install(
        core_view: RuntimeFilterInstallView,
        routing_shard: RuntimeFilterRoutingShard,
    ) -> RuntimeFilterParticipantInstall {
        RuntimeFilterParticipantInstall::new(core_view, routing_shard)
    }

    fn local_install(core_view: RuntimeFilterInstallView) -> RuntimeFilterParticipantInstall {
        local_participant_install_for_test(core_view)
    }

    fn inbound_to_aggregator(
        channel_id: ChannelId,
        binding_id: BindingId,
        source_participant: RuntimeFilterParticipantId,
        target_participant: RuntimeFilterParticipantId,
    ) -> RuntimeFilterRoutingEdgeView {
        inbound_to_aggregator_with(
            channel_id,
            RouteEdgeId::new(901),
            binding_id,
            source_participant,
            target_participant,
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::Unavailable,
            ]),
        )
    }

    fn inbound_to_aggregator_with(
        channel_id: ChannelId,
        route_edge_id: RouteEdgeId,
        binding_id: BindingId,
        source_participant: RuntimeFilterParticipantId,
        target_participant: RuntimeFilterParticipantId,
        allowed_kinds: BTreeSet<RuntimeFilterEnvelopeKind>,
    ) -> RuntimeFilterRoutingEdgeView {
        RuntimeFilterRoutingEdgeView::new(
            channel_id,
            route_edge_id,
            RuntimeFilterRouteEndpointView::new(
                source_participant,
                RuntimeFilterRouteRole::Producer(binding_id),
            ),
            RuntimeFilterRouteEndpointView::new(
                target_participant,
                RuntimeFilterRouteRole::Aggregator,
            ),
            RuntimeFilterRoutePeer::Remote {
                participant_id: source_participant,
                endpoint: RuntimeEndpoint::new("remote-producer", 9060).unwrap(),
            },
            allowed_kinds,
        )
        .unwrap()
    }

    fn routing_shard(
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
        channel_id: ChannelId,
        local_roles: BTreeSet<RuntimeFilterRouteRole>,
        producer_instances: BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId>,
        inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    ) -> RuntimeFilterRoutingShard {
        RuntimeFilterRoutingShard::new(
            epoch,
            participant,
            BTreeMap::from([(
                channel_id,
                RuntimeFilterChannelRoutingView::new(
                    channel_id,
                    local_roles,
                    producer_instances,
                    inbound_edges,
                    Vec::new(),
                )
                .unwrap(),
            )]),
        )
        .unwrap()
    }

    fn without_consumers(
        channel: &RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            BTreeMap::new(),
        )
    }

    fn consumerless_channels() -> [RuntimeFilterChannelDeployment; 2] {
        [
            without_consumers(&channel(1, 10, 20, 30, 40)),
            without_consumers(&ordered_deployment_fixture()),
        ]
    }

    fn assert_install_error(
        error: InstallContractError,
        kind: InstallContractErrorKind,
        detail: &str,
    ) {
        assert_eq!(error.kind(), kind);
        assert!(
            error.detail().contains(detail),
            "expected detail containing {detail:?}, got {:?}",
            error.detail()
        );
    }

    #[test]
    fn install_rejects_core_and_routing_epoch_mismatch() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let mut install = local_install(core.clone());
        install = participant_install(
            core,
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(10),
                install.local_participant_id(),
                install.routing_shard().channels().clone(),
            )
            .unwrap(),
        );

        assert_install_error(
            registry().install(install).unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "core and routing epochs differ",
        );
    }

    #[test]
    fn install_rejects_core_and_routing_participant_mismatch() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let install = local_install(core.clone());
        let mismatched = RuntimeFilterRoutingShard::new(
            install.epoch(),
            RuntimeFilterParticipantId::new(4),
            BTreeMap::new(),
        )
        .unwrap();

        assert_install_error(
            registry()
                .install(participant_install(core, mismatched))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "core and routing participants differ",
        );
    }

    #[test]
    fn same_epoch_changed_routing_shard_is_conflicting_deployment() {
        let registry = registry();
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let first = local_install(core.clone());
        registry.install(first.clone()).unwrap();
        let channel = first.routing_shard().channel(ChannelId::new(1)).unwrap();
        let mut roles = channel.local_roles().clone();
        roles.insert(RuntimeFilterRouteRole::Relay);
        let changed = routing_shard(
            first.epoch(),
            first.local_participant_id(),
            ChannelId::new(1),
            roles,
            channel.producer_instances().clone(),
            Vec::new(),
        );

        assert_install_error(
            registry
                .install(participant_install(core, changed))
                .unwrap_err(),
            InstallContractErrorKind::ConflictingDeployment,
            "different installed composite",
        );
    }

    #[test]
    fn install_rejects_core_channel_missing_from_routing_shard() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let routing = RuntimeFilterRoutingShard::new(
            core.epoch(),
            core.local_participant_id(),
            BTreeMap::new(),
        )
        .unwrap();

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "core channel 1 is missing from routing shard",
        );
    }

    #[test]
    fn install_rejects_expected_producer_instance_missing_from_routing_index() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let routing = routing_shard(
            core.epoch(),
            core.local_participant_id(),
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Consumer(BindingId::new(30)),
            ]),
            BTreeMap::new(),
            Vec::new(),
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "producer binding 10 instance",
        );
    }

    #[test]
    fn non_aggregator_rejects_core_producer_instance_mapped_to_remote_participant() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let participant = core.local_participant_id();
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Consumer(BindingId::new(30)),
            ]),
            BTreeMap::from([(
                (BindingId::new(10), uid(10)),
                RuntimeFilterParticipantId::new(4),
            )]),
            Vec::new(),
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "non-aggregator producer binding 10 instance",
        );
    }

    #[test]
    fn local_mapped_core_producer_requires_matching_local_role() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let participant = core.local_participant_id();
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Consumer(BindingId::new(30)),
                RuntimeFilterRouteRole::Aggregator,
            ]),
            BTreeMap::from([((BindingId::new(10), uid(10)), participant)]),
            Vec::new(),
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "local producer binding 10 has no matching local Producer role",
        );
    }

    #[test]
    fn install_rejects_aggregator_core_missing_authorized_remote_instance() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let participant = core.local_participant_id();
        let remote = RuntimeFilterParticipantId::new(4);
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Consumer(BindingId::new(30)),
                RuntimeFilterRouteRole::Aggregator,
            ]),
            BTreeMap::from([
                ((BindingId::new(10), uid(10)), participant),
                ((BindingId::new(10), uid(11)), remote),
            ]),
            vec![inbound_to_aggregator(
                ChannelId::new(1),
                BindingId::new(10),
                remote,
                participant,
            )],
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "aggregator core is missing routing-authorized producer",
        );
    }

    #[test]
    fn aggregator_rejects_missing_inbound_edge_for_one_authorized_source() {
        let base = channel(1, 10, 20, 30, 40);
        let channel = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            base.core_budget(),
            base.materialization_policy(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(
                    CoverageWitnessId::new(20),
                    BTreeSet::from([uid(10), uid(11)]),
                ),
            )]),
            BTreeMap::new(),
        );
        let core = core_view([(1, channel)]);
        let participant = core.local_participant_id();
        let source_a = RuntimeFilterParticipantId::new(4);
        let source_b = RuntimeFilterParticipantId::new(5);
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::from([
                ((BindingId::new(10), uid(10)), source_a),
                ((BindingId::new(10), uid(11)), source_b),
            ]),
            vec![inbound_to_aggregator(
                ChannelId::new(1),
                BindingId::new(10),
                source_a,
                participant,
            )],
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "producer binding 10 source participant",
        );
    }

    #[test]
    fn aggregator_rejects_contribution_only_inbound_edge() {
        let core = core_view([(1, without_consumers(&channel(1, 10, 20, 30, 40)))]);
        let participant = core.local_participant_id();
        let remote = RuntimeFilterParticipantId::new(4);
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::from([((BindingId::new(10), uid(10)), remote)]),
            vec![inbound_to_aggregator_with(
                ChannelId::new(1),
                RouteEdgeId::new(901),
                BindingId::new(10),
                remote,
                participant,
                BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
            )],
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "must allow Contribution and ProducerClosed",
        );
    }

    #[test]
    fn consumerful_aggregator_without_inbound_producer_edge_is_rejected() {
        let core = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let participant = core.local_participant_id();
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Consumer(BindingId::new(30)),
                RuntimeFilterRouteRole::Aggregator,
            ]),
            BTreeMap::from([((BindingId::new(10), uid(10)), participant)]),
            Vec::new(),
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "requires exactly one inbound Producer-to-Aggregator edge",
        );
    }

    #[test]
    fn aggregator_rejects_duplicate_inbound_edges_for_authorized_source() {
        let core = core_view([(1, without_consumers(&channel(1, 10, 20, 30, 40)))]);
        let participant = core.local_participant_id();
        let remote = RuntimeFilterParticipantId::new(4);
        let routing = routing_shard(
            core.epoch(),
            participant,
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::from([((BindingId::new(10), uid(10)), remote)]),
            vec![
                inbound_to_aggregator_with(
                    ChannelId::new(1),
                    RouteEdgeId::new(901),
                    BindingId::new(10),
                    remote,
                    participant,
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Contribution,
                        RuntimeFilterEnvelopeKind::ProducerClosed,
                    ]),
                ),
                inbound_to_aggregator_with(
                    ChannelId::new(1),
                    RouteEdgeId::new(902),
                    BindingId::new(10),
                    remote,
                    participant,
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Contribution,
                        RuntimeFilterEnvelopeKind::ProducerClosed,
                    ]),
                ),
            ],
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "requires exactly one inbound Producer-to-Aggregator edge",
        );
    }

    #[test]
    fn empty_core_and_nonempty_routing_is_not_ignored() {
        let core = core_view([]);
        let routing = routing_shard(
            core.epoch(),
            core.local_participant_id(),
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Relay]),
            BTreeMap::new(),
            Vec::new(),
        );

        assert_install_error(
            registry()
                .install(participant_install(core, routing))
                .unwrap_err(),
            InstallContractErrorKind::UnsupportedChannelContract,
            "empty core view with nonempty routing shard",
        );
    }

    #[test]
    fn consumerless_aggregator_channel_installs_when_routing_proves_inbound_authority() {
        for channel in consumerless_channels() {
            let core = core_view([(1, channel)]);
            let participant = core.local_participant_id();
            let remote = RuntimeFilterParticipantId::new(4);
            let routing = routing_shard(
                core.epoch(),
                participant,
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
                BTreeMap::from([((BindingId::new(10), uid(10)), remote)]),
                vec![inbound_to_aggregator(
                    ChannelId::new(1),
                    BindingId::new(10),
                    remote,
                    participant,
                )],
            );

            assert_eq!(
                registry()
                    .install(participant_install(core, routing))
                    .unwrap()
                    .outcome(),
                InstallOutcome::Installed
            );
        }
    }

    #[test]
    fn consumerless_non_aggregator_channel_is_rejected() {
        for channel in consumerless_channels() {
            let core = core_view([(1, channel)]);
            let participant = core.local_participant_id();
            let routing = routing_shard(
                core.epoch(),
                participant,
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Producer(BindingId::new(10))]),
                BTreeMap::from([((BindingId::new(10), uid(10)), participant)]),
                Vec::new(),
            );

            assert_install_error(
                registry()
                    .install(participant_install(core, routing))
                    .unwrap_err(),
                InstallContractErrorKind::UnsupportedChannelContract,
                "consumerless channel requires local Aggregator authority",
            );
        }
    }

    #[test]
    fn consumerless_aggregator_without_inbound_producer_edge_is_rejected() {
        for channel in consumerless_channels() {
            let core = core_view([(1, channel)]);
            let participant = core.local_participant_id();
            let remote = RuntimeFilterParticipantId::new(4);
            let routing = routing_shard(
                core.epoch(),
                participant,
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
                BTreeMap::from([((BindingId::new(10), uid(10)), remote)]),
                Vec::new(),
            );

            assert_install_error(
                registry()
                    .install(participant_install(core, routing))
                    .unwrap_err(),
                InstallContractErrorKind::UnsupportedChannelContract,
                "requires exactly one inbound Producer-to-Aggregator edge",
            );
        }
    }

    #[test]
    fn consumerless_aggregator_rejects_inbound_edge_from_unindexed_source() {
        for channel in consumerless_channels() {
            let core = core_view([(1, channel)]);
            let participant = core.local_participant_id();
            let edge_source = RuntimeFilterParticipantId::new(4);
            let indexed_source = RuntimeFilterParticipantId::new(5);
            let routing = routing_shard(
                core.epoch(),
                participant,
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
                BTreeMap::from([((BindingId::new(10), uid(10)), indexed_source)]),
                vec![inbound_to_aggregator(
                    ChannelId::new(1),
                    BindingId::new(10),
                    edge_source,
                    participant,
                )],
            );

            assert_install_error(
                registry()
                    .install(participant_install(core, routing))
                    .unwrap_err(),
                InstallContractErrorKind::UnsupportedChannelContract,
                "aggregator inbound producer edge source has no authorized producer instance",
            );
        }
    }

    fn with_consumer_contract(
        base: RuntimeFilterChannelDeployment,
        logical_domain: RuntimeFilterLogicalDomain,
        capabilities: BTreeSet<ArtifactCapability>,
        profile: ConsumerArtifactProfile,
    ) -> RuntimeFilterChannelDeployment {
        let consumer = base.consumers().values().next().unwrap();
        RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            logical_domain,
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            base.core_budget(),
            base.materialization_policy(),
            base.producers().clone(),
            BTreeMap::from([(
                *base.consumers().keys().next().unwrap(),
                ConsumerDeployment::with_profile(
                    consumer.activation(),
                    capabilities,
                    profile,
                    consumer.loopback_route_edge_id(),
                    consumer.expected_fragment_instances().clone(),
                ),
            )]),
        )
    }

    fn registry() -> DeploymentRegistry {
        let started = Instant::now();
        DeploymentRegistry::new(
            uid(0),
            Arc::new(Clock(started)),
            Arc::new(Account),
            Arc::new(EventEmitter::new(Arc::new(NoopEvents))),
        )
    }

    #[test]
    fn empty_install_does_not_initialize_epoch_or_emit_events() {
        let registry = registry();
        let result = registry.install(view([])).unwrap();
        assert_eq!(result.outcome(), InstallOutcome::IgnoredEmpty);
        assert!(result.events().is_empty());
        assert_eq!(registry.installed_epoch(), None);

        let result = registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap();
        assert_eq!(result.outcome(), InstallOutcome::Installed);
        assert_eq!(registry.installed_epoch(), Some(DeploymentEpoch::new(9)));
    }

    #[test]
    fn install_rejects_membership_without_empty_domain_semantics_or_kind() {
        let base = channel(1, 10, 20, 30, 40);
        let logical_domain = base.logical_domain().clone();
        let missing_semantic = with_consumer_contract(
            base,
            logical_domain,
            BTreeSet::from([ArtifactCapability::Membership]),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, missing_semantic)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::MissingMembershipCapability
        );

        let base = channel(1, 10, 20, 30, 40);
        let logical_domain = base.logical_domain().clone();
        let missing_kind = with_consumer_contract(
            base,
            logical_domain,
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, missing_kind)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::UnsupportedChannelContract
        );
    }

    #[test]
    fn install_rejects_schema_and_null_incompatible_profiles() {
        let base = channel(1, 10, 20, 30, 40);
        let bitset_only = with_consumer_contract(
            base,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Utf8,
                null_semantics: NullSemantics::NeverMatches,
            },
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, bitset_only)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::UnsupportedChannelContract
        );

        let base = channel(1, 10, 20, 30, 40);
        let null_without_value_set = with_consumer_contract(
            base,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, null_without_value_set)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::UnsupportedChannelContract
        );
    }

    #[test]
    fn install_recomputes_bloom_contract_and_bounds_jobs_by_unique_profiles() {
        let base = channel(1, 10, 20, 30, 40);
        let too_many_jobs = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            base.core_budget(),
            crate::runtime_filter::port::install::MaterializationPolicy::new(
                8,
                5,
                17,
                1,
                1 << 20,
                1 << 16,
                2,
            )
            .unwrap(),
            base.producers().clone(),
            base.consumers().clone(),
        );
        assert_eq!(
            registry()
                .install(view([(1, too_many_jobs)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::InvalidPolicy
        );

        let wrong_bloom = with_consumer_contract(
            base.clone(),
            base.logical_domain().clone(),
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerArtifactProfile::new(
                BTreeSet::from([
                    ArtifactKind::Bloom,
                    ArtifactKind::ValueSet,
                    ArtifactKind::EmptyDomain,
                ]),
                Some(crate::runtime_filter::port::artifact::HashContractDigest::new([7; 32])),
            )
            .unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, wrong_bloom)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::UnsupportedChannelContract
        );

        let RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } = base.logical_domain()
        else {
            unreachable!()
        };
        let schema = ArtifactMembershipSchema::new(value_type, *null_semantics).unwrap();
        let digest = BloomHashContract::new(&schema, base.materialization_policy())
            .unwrap()
            .digest();
        let valid_bloom = with_consumer_contract(
            base.clone(),
            base.logical_domain().clone(),
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerArtifactProfile::new(
                BTreeSet::from([
                    ArtifactKind::Bloom,
                    ArtifactKind::ValueSet,
                    ArtifactKind::EmptyDomain,
                ]),
                Some(digest),
            )
            .unwrap(),
        );
        assert_eq!(
            registry()
                .install(view([(1, valid_bloom)]))
                .unwrap()
                .outcome(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn install_rejects_profile_digest_collision_across_channels() {
        let collision_id = ConsumerProfileId::for_test([9; 32]);
        let semantics = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::EmptyDomain,
        ]);
        let first_base = channel(1, 10, 20, 30, 40);
        let first = with_consumer_contract(
            first_base.clone(),
            first_base.logical_domain().clone(),
            semantics.clone(),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap()
            .with_test_identity(collision_id),
        );
        let second_base = channel(2, 11, 21, 31, 41);
        let second = with_consumer_contract(
            second_base.clone(),
            second_base.logical_domain().clone(),
            semantics,
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap()
            .with_test_identity(collision_id),
        );

        assert_eq!(
            registry()
                .install(view([(1, first), (2, second)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::ConflictingDeployment
        );
    }

    #[test]
    fn empty_install_remains_ignored_after_install_and_cancel() {
        let registry = registry();
        registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap();
        assert_eq!(
            registry.install(view([])).unwrap().outcome(),
            InstallOutcome::IgnoredEmpty
        );
        registry.cancel();
        assert_eq!(
            registry.install(view([])).unwrap().outcome(),
            InstallOutcome::IgnoredEmpty
        );
    }

    #[test]
    fn concurrent_first_install_reads_commit_clock_exactly_once() {
        let clock = Arc::new(CountingClock {
            now: Instant::now(),
            calls: AtomicUsize::new(0),
        });
        let registry = Arc::new(DeploymentRegistry::new(
            uid(0),
            clock.clone(),
            Arc::new(Account),
            Arc::new(EventEmitter::new(Arc::new(NoopEvents))),
        ));
        let barrier = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let registry = registry.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    registry
                        .install(view([(1, channel(1, 10, 20, 30, 40))]))
                        .unwrap()
                        .outcome()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == InstallOutcome::Installed)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == InstallOutcome::AlreadyInstalled)
                .count(),
            1
        );
        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn install_clock_may_reenter_registry_reads_without_deadlock() {
        let clock = Arc::new(ReentrantClock {
            registry: Mutex::new(Weak::new()),
            now: Instant::now(),
        });
        let registry = Arc::new(DeploymentRegistry::new(
            uid(0),
            clock.clone(),
            Arc::new(Account),
            Arc::new(EventEmitter::new(Arc::new(NoopEvents))),
        ));
        *clock.registry.lock().unwrap() = Arc::downgrade(&registry);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(
                registry
                    .install(view([(1, channel(1, 10, 20, 30, 40))]))
                    .map(|result| result.outcome()),
            )
            .unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("reentrant clock deadlocked install")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn valid_nonempty_install_commits_all_channels_atomically() {
        let registry = registry();
        let result = registry
            .install(view([
                (1, channel(1, 10, 20, 30, 40)),
                (2, channel(2, 11, 21, 31, 41)),
            ]))
            .unwrap();
        assert_eq!(result.outcome(), InstallOutcome::Installed);
        assert_eq!(registry.channel_count(), 2);
        assert_eq!(result.events().len(), 3);
        assert_eq!(
            registry.deadline(ChannelId::new(1)),
            Some(result.committed_at().unwrap() + Duration::from_millis(100))
        );
    }

    #[test]
    fn producer_witness_identity_is_owned_per_channel() {
        let registry = registry();
        let first = channel(1, 10, 20, 30, 40);
        let mut second = channel(2, 11, 21, 31, 41);
        second = RuntimeFilterChannelDeployment::new(
            second.channel_id(),
            second.logical_domain().clone(),
            second.lifecycle(),
            Coverage::Leaf(CoverageWitnessId::new(20)),
            Coverage::Leaf(CoverageWitnessId::new(20)),
            second.reduction_requirement(),
            second.allowed_contribution_kinds().clone(),
            second.completion_requirement(),
            second.policy(),
            second.core_budget(),
            second.materialization_policy(),
            BTreeMap::from([(
                BindingId::new(11),
                ProducerDeployment::new(CoverageWitnessId::new(20), BTreeSet::from([uid(11)])),
            )]),
            second.consumers().clone(),
        );
        assert_eq!(
            registry
                .install(view([(1, first), (2, second)]))
                .unwrap()
                .outcome(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn identical_nonempty_install_is_idempotent_without_resetting_commit_time() {
        let clock = Arc::new(CountingClock {
            now: Instant::now(),
            calls: AtomicUsize::new(0),
        });
        let registry = DeploymentRegistry::new(
            uid(0),
            clock.clone(),
            Arc::new(Account),
            Arc::new(EventEmitter::new(Arc::new(NoopEvents))),
        );
        let install = view([(1, channel(1, 10, 20, 30, 40))]);
        let first = registry.install(install.clone()).unwrap();
        let second = registry.install(install).unwrap();
        assert_eq!(second.outcome(), InstallOutcome::AlreadyInstalled);
        assert!(second.events().is_empty());
        assert_eq!(second.committed_at(), first.committed_at());
        assert_eq!(
            registry.deadline(ChannelId::new(1)),
            Some(first.committed_at().unwrap() + Duration::from_millis(100))
        );
        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn duplicate_value_set_install_rejects_materialization_policy_mismatch() {
        let registry = registry();
        let base = channel(1, 10, 20, 30, 40);
        registry.install(view([(1, base.clone())])).unwrap();
        let changed = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            base.core_budget(),
            MaterializationPolicy::new(8, 5, 19, 1, 1 << 20, 1 << 16, 1).unwrap(),
            base.producers().clone(),
            base.consumers().clone(),
        );

        assert_eq!(
            registry.install(view([(1, changed)])).unwrap_err().kind(),
            InstallContractErrorKind::ConflictingDeployment
        );
    }

    #[test]
    fn equivalent_install_is_order_independent() {
        let registry = registry();
        let base = channel(1, 10, 20, 30, 40);
        let producers = BTreeMap::from([
            (
                BindingId::new(10),
                ProducerDeployment::new(CoverageWitnessId::new(20), BTreeSet::from([uid(10)])),
            ),
            (
                BindingId::new(11),
                ProducerDeployment::new(CoverageWitnessId::new(21), BTreeSet::from([uid(11)])),
            ),
        ]);
        let make_channel = |coverage: Coverage| {
            RuntimeFilterChannelDeployment::new(
                base.channel_id(),
                base.logical_domain().clone(),
                base.lifecycle(),
                coverage.clone(),
                coverage,
                base.reduction_requirement(),
                base.allowed_contribution_kinds().clone(),
                base.completion_requirement(),
                base.policy(),
                base.core_budget(),
                base.materialization_policy(),
                producers.clone(),
                base.consumers().clone(),
            )
        };
        let first_channel = make_channel(Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(20)),
            Coverage::Leaf(CoverageWitnessId::new(21)),
        ]));
        let second_channel = make_channel(Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(21)),
            Coverage::Leaf(CoverageWitnessId::new(20)),
        ]));
        assert_eq!(
            registry
                .install(view([(1, first_channel)]))
                .unwrap()
                .outcome(),
            InstallOutcome::Installed
        );
        assert_eq!(
            registry
                .install(view([(1, second_channel)]))
                .unwrap()
                .outcome(),
            InstallOutcome::AlreadyInstalled
        );
    }

    #[test]
    fn deadline_overflow_is_typed_and_does_not_commit() {
        let base = channel(1, 10, 20, 30, 40);
        let mut policy = base.policy();
        policy.deadline_ms = u64::MAX;
        let invalid = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            policy,
            base.core_budget(),
            base.materialization_policy(),
            base.producers().clone(),
            base.consumers().clone(),
        );
        let origin = Instant::now();
        let mut low = 0_u64;
        let mut high = u64::MAX;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if origin.checked_add(Duration::from_secs(middle)).is_some() {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let near_max = origin.checked_add(Duration::from_secs(low)).unwrap();
        let registry = DeploymentRegistry::new(
            uid(0),
            Arc::new(Clock(near_max)),
            Arc::new(Account),
            Arc::new(EventEmitter::new(Arc::new(NoopEvents))),
        );
        assert_eq!(
            registry.install(view([(1, invalid)])).unwrap_err().kind(),
            InstallContractErrorKind::InvalidPolicy
        );
        assert_eq!(registry.channel_count(), 0);
        assert_eq!(registry.installed_epoch(), None);
    }

    #[test]
    fn conflicting_same_epoch_install_fails_and_preserves_original() {
        let registry = registry();
        registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap();
        let error = registry
            .install(view([(2, channel(2, 11, 21, 31, 41))]))
            .unwrap_err();
        assert_eq!(
            error.kind(),
            InstallContractErrorKind::ConflictingDeployment
        );
        assert_eq!(registry.channel_count(), 1);
        assert!(registry.channel(ChannelId::new(1)).is_some());
    }

    #[test]
    fn malformed_nonempty_install_is_validated_before_installed_state_comparison() {
        let registry = registry();
        registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap();
        assert_eq!(
            registry
                .install(view([(
                    1,
                    invalid_deployment(InvalidDeployment::ZeroContributionLimit),
                )]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::InvalidPolicy
        );
        assert_eq!(registry.channel_count(), 1);
    }

    #[test]
    fn different_epoch_install_is_rejected() {
        let registry = registry();
        registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap();
        let other = core_view([(1, channel(1, 10, 20, 30, 40))]);
        let other = local_install(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(10),
            other.local_participant_id(),
            other.channels().clone(),
        ));
        assert_eq!(
            registry.install(other).unwrap_err().kind(),
            InstallContractErrorKind::EpochMismatch
        );
    }

    #[test]
    fn invalid_channel_causes_zero_partial_install() {
        let registry = registry();
        let mut invalid = channel(2, 11, 21, 31, 41);
        invalid = RuntimeFilterChannelDeployment::new(
            invalid.channel_id(),
            invalid.logical_domain().clone(),
            invalid.lifecycle(),
            Coverage::AllOf(vec![]),
            invalid.terminal_coverage().clone(),
            invalid.reduction_requirement(),
            invalid.allowed_contribution_kinds().clone(),
            invalid.completion_requirement(),
            invalid.policy(),
            invalid.core_budget(),
            invalid.materialization_policy(),
            invalid.producers().clone(),
            invalid.consumers().clone(),
        );
        let error = registry
            .install(view([(1, channel(1, 10, 20, 30, 40)), (2, invalid)]))
            .unwrap_err();
        assert_eq!(error.kind(), InstallContractErrorKind::InvalidCoverage);
        assert_eq!(registry.channel_count(), 0);
        assert_eq!(registry.installed_epoch(), None);
    }

    #[test]
    fn install_after_service_cancel_is_rejected_without_recreation() {
        let registry = registry();
        registry.cancel();
        let error = registry
            .install(view([(1, channel(1, 10, 20, 30, 40))]))
            .unwrap_err();
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
        assert_eq!(registry.channel_count(), 0);
    }

    #[test]
    fn validation_error_order_is_stable_and_complete_once_matrix_is_strict() {
        let registry = registry();
        let base = channel(1, 10, 20, 30, 40);
        let invalid_epoch = RuntimeFilterInstallView::new(
            DeploymentEpoch::new(0),
            RuntimeFilterParticipantId::new(3),
            BTreeMap::from([(ChannelId::new(2), base.clone())]),
        );
        assert_eq!(
            validate_view(&invalid_epoch).unwrap_err().kind(),
            InstallContractErrorKind::InvalidEpoch
        );

        let cases = [
            (
                view([(2, base.clone())]),
                InstallContractErrorKind::DuplicateIdentity,
            ),
            (
                view([(
                    1,
                    RuntimeFilterChannelDeployment::new(
                        base.channel_id(),
                        RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                            keys: vec![],
                            inclusive: true,
                            comparator_digest: ComparatorDigest::new([0; 32]),
                        }),
                        base.lifecycle(),
                        base.availability_coverage().clone(),
                        base.terminal_coverage().clone(),
                        base.reduction_requirement(),
                        base.allowed_contribution_kinds().clone(),
                        base.completion_requirement(),
                        base.policy(),
                        base.core_budget(),
                        base.materialization_policy(),
                        base.producers().clone(),
                        base.consumers().clone(),
                    ),
                )]),
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                view([(
                    1,
                    RuntimeFilterChannelDeployment::new(
                        base.channel_id(),
                        RuntimeFilterLogicalDomain::Membership {
                            value_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "x",
                                DataType::Int64,
                                false,
                            ))),
                            null_semantics: NullSemantics::NeverMatches,
                        },
                        base.lifecycle(),
                        base.availability_coverage().clone(),
                        base.terminal_coverage().clone(),
                        base.reduction_requirement(),
                        base.allowed_contribution_kinds().clone(),
                        base.completion_requirement(),
                        base.policy(),
                        base.core_budget(),
                        base.materialization_policy(),
                        base.producers().clone(),
                        base.consumers().clone(),
                    ),
                )]),
                InstallContractErrorKind::UnsupportedMembershipType,
            ),
        ];

        for (view, expected) in cases {
            assert_eq!(registry.install(view).unwrap_err().kind(), expected);
            assert_eq!(registry.channel_count(), 0);
        }
    }

    #[test]
    fn complete_validation_table_returns_stable_typed_errors_without_partial_install() {
        let cases = [
            (
                InvalidDeployment::Lifecycle,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::Reduction,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::Contributions,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::Completion,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::UnsupportedType,
                InstallContractErrorKind::UnsupportedMembershipType,
            ),
            (
                InvalidDeployment::ZeroContributionLimit,
                InstallContractErrorKind::InvalidPolicy,
            ),
            (
                InvalidDeployment::ZeroArtifactLimit,
                InstallContractErrorKind::InvalidPolicy,
            ),
            (
                InvalidDeployment::ZeroDeadline,
                InstallContractErrorKind::InvalidPolicy,
            ),
            (
                InvalidDeployment::ZeroBudget,
                InstallContractErrorKind::InvalidBudget,
            ),
            (
                InvalidDeployment::CoverageMismatch,
                InstallContractErrorKind::InvalidCoverage,
            ),
            (
                InvalidDeployment::CoverageShape,
                InstallContractErrorKind::InvalidCoverage,
            ),
            (
                InvalidDeployment::UnknownWitness,
                InstallContractErrorKind::UnknownCoverageWitness,
            ),
            (
                InvalidDeployment::DuplicateCoverageWitness,
                InstallContractErrorKind::DuplicateCoverageWitness,
            ),
            (
                InvalidDeployment::EmptyProducerInstances,
                InstallContractErrorKind::EmptyExpectedInstances,
            ),
            (
                InvalidDeployment::EmptyConsumerInstances,
                InstallContractErrorKind::EmptyExpectedInstances,
            ),
            (
                InvalidDeployment::NonBlockingConsumer,
                InstallContractErrorKind::InvalidConsumerActivation,
            ),
            (
                InvalidDeployment::MissingMembershipCapability,
                InstallContractErrorKind::MissingMembershipCapability,
            ),
            (
                InvalidDeployment::MissingProducer,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::MissingConsumer,
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                InvalidDeployment::DuplicateProducerWitness,
                InstallContractErrorKind::DuplicateCoverageWitness,
            ),
        ];
        for (case, expected) in cases {
            let empty_registry = registry();
            assert_eq!(
                empty_registry
                    .install(view([(1, invalid_deployment(case))]))
                    .unwrap_err()
                    .kind(),
                expected
            );
            assert_eq!(empty_registry.channel_count(), 0);
            assert_eq!(empty_registry.installed_epoch(), None);

            let installed_registry = registry();
            installed_registry
                .install(view([(1, channel(1, 10, 20, 30, 40))]))
                .unwrap();
            assert_eq!(
                installed_registry
                    .install(view([(1, invalid_deployment(case))]))
                    .unwrap_err()
                    .kind(),
                expected
            );
            assert_eq!(installed_registry.channel_count(), 1);
            assert_eq!(
                installed_registry.installed_epoch(),
                Some(DeploymentEpoch::new(9))
            );
        }
    }

    #[test]
    fn policy_and_budget_validation_precede_coverage_validation() {
        let base = invalid_deployment(InvalidDeployment::CoverageShape);
        let mut policy = base.policy();
        policy.max_contribution_bytes = 0;
        let invalid_policy = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            policy,
            base.core_budget(),
            base.materialization_policy(),
            base.producers().clone(),
            base.consumers().clone(),
        );
        assert_eq!(
            registry()
                .install(view([(1, invalid_policy)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::InvalidPolicy
        );

        let invalid_budget = RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            RuntimeFilterCoreBudget::new(0),
            base.materialization_policy(),
            base.producers().clone(),
            base.consumers().clone(),
        );
        assert_eq!(
            registry()
                .install(view([(1, invalid_budget)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::InvalidBudget
        );

        let duplicate = invalid_deployment(InvalidDeployment::DuplicateProducerWitness);
        let mut duplicate_policy = duplicate.policy();
        duplicate_policy.max_contribution_bytes = 0;
        let invalid_policy_and_duplicate = RuntimeFilterChannelDeployment::new(
            duplicate.channel_id(),
            duplicate.logical_domain().clone(),
            duplicate.lifecycle(),
            duplicate.availability_coverage().clone(),
            duplicate.terminal_coverage().clone(),
            duplicate.reduction_requirement(),
            duplicate.allowed_contribution_kinds().clone(),
            duplicate.completion_requirement(),
            duplicate_policy,
            duplicate.core_budget(),
            duplicate.materialization_policy(),
            duplicate.producers().clone(),
            duplicate.consumers().clone(),
        );
        assert_eq!(
            registry()
                .install(view([(1, invalid_policy_and_duplicate)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::InvalidPolicy
        );
    }

    #[test]
    fn binding_and_route_identities_are_unique_across_the_full_view() {
        let first = channel(1, 10, 20, 30, 40);
        let duplicate_binding = channel(2, 10, 21, 31, 41);
        assert_eq!(
            registry()
                .install(view([(1, first.clone()), (2, duplicate_binding)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::DuplicateIdentity
        );

        let duplicate_route = channel(2, 11, 21, 31, 40);
        assert_eq!(
            registry()
                .install(view([(1, first), (2, duplicate_route)]))
                .unwrap_err()
                .kind(),
            InstallContractErrorKind::DuplicateIdentity
        );
    }
}
