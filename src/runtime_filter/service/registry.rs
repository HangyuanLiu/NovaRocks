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
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::events::{
    RouteEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RouteEdgeId};
use crate::runtime_filter::port::install::{
    CompleteOnceChannelDeployment, RuntimeFilterInstallView,
};
use crate::runtime_filter::port::producer::{
    InstallContractError, InstallContractErrorKind, InstallOutcome,
};
use crate::runtime_filter::port::support::{RuntimeFilterClock, RuntimeFilterMemoryAccount};
use crate::runtime_filter::port::value_domain::MembershipValues;
use crate::runtime_filter::router::loopback::LoopbackRouter;

use super::EventEmitter;
use super::subscription::{SubscriptionGroup, SubscriptionSlot};

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
}

pub(super) struct InstalledDeployment {
    view: RuntimeFilterInstallView,
    committed_at: Instant,
    channels: BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>,
    deadlines: BTreeMap<ChannelId, Instant>,
    producers: BTreeMap<BindingId, ProducerRoute>,
    subscriptions: BTreeMap<BindingId, Arc<SubscriptionGroup>>,
    router: Arc<LoopbackRouter>,
    channel_routes: BTreeMap<ChannelId, Vec<RouteEdgeId>>,
    route_event_identities: BTreeMap<RouteEdgeId, Vec<RouteEventIdentity>>,
}

impl InstalledDeployment {
    pub(super) fn producer(&self, binding_id: BindingId) -> Option<&ProducerRoute> {
        self.producers.get(&binding_id)
    }

    pub(super) fn subscription(
        &self,
        binding_id: BindingId,
        instance: UniqueId,
    ) -> Option<Arc<SubscriptionSlot>> {
        self.subscriptions
            .get(&binding_id)
            .and_then(|group| group.slot(instance))
    }

    pub(super) fn has_consumer(&self, binding_id: BindingId) -> bool {
        self.subscriptions.contains_key(&binding_id)
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
    view: RuntimeFilterInstallView,
    result: Mutex<Option<Result<Arc<InstalledDeployment>, InstallContractError>>>,
    completed: Condvar,
}

impl InstallFlight {
    fn new(view: RuntimeFilterInstallView) -> Self {
        Self {
            view,
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
        view: RuntimeFilterInstallView,
    ) -> Result<RegistryInstallResult, InstallContractError> {
        if view.is_empty() {
            return Ok(RegistryInstallResult {
                outcome: InstallOutcome::IgnoredEmpty,
                committed_at: None,
                events: Vec::new(),
            });
        }
        validate_view(&view)?;

        let (flight, leader) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &*state {
                RegistryState::Cancelled(_) => return Err(cancelled_install()),
                RegistryState::Publishing { installed, .. } => {
                    // Publishing begins after the logical commit. New install calls compare
                    // against that committed deployment and return immediately; only callers
                    // that already observed Installing wait for event publication to finish.
                    return compare_installed(installed, &view);
                }
                RegistryState::Installed(installed) => {
                    return compare_installed(installed, &view);
                }
                RegistryState::Installing(flight) => {
                    compare_installing(flight, &view)?;
                    (flight.clone(), false)
                }
                RegistryState::Uninstalled => {
                    let flight = Arc::new(InstallFlight::new(view.clone()));
                    *state = RegistryState::Installing(flight.clone());
                    (flight, true)
                }
            }
        };
        if !leader {
            let installed = flight.wait()?;
            return compare_installed(&installed, &view);
        }

        let candidate = (|| {
            let channels = build_channels(self.query_id, &view, self.memory_account.clone())?;
            let routing = build_routing(self.query_id, &view, &channels, self.events.clone())?;
            Ok::<_, InstallContractError>((channels, routing))
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
        let candidate = Arc::new(InstalledDeployment {
            view: view.clone(),
            committed_at,
            channels,
            deadlines,
            producers: routing.producers,
            subscriptions: routing.subscriptions,
            router: Arc::new(LoopbackRouter::new(routing.routes)),
            channel_routes: routing.channel_routes,
            route_event_identities: routing.route_event_identities,
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

    pub(super) fn cancel(&self) -> Option<Arc<InstalledDeployment>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let (installed, flight) = match &*state {
            RegistryState::Installed(installed) => (Some(installed.clone()), None),
            RegistryState::Publishing { installed, .. } => (Some(installed.clone()), None),
            RegistryState::Installing(flight) => (None, Some(flight.clone())),
            RegistryState::Cancelled(installed) => return installed.clone(),
            RegistryState::Uninstalled => (None, None),
        };
        *state = RegistryState::Cancelled(installed.clone());
        drop(state);
        if let Some(flight) = flight {
            flight.complete(Err(cancelled_install()));
        }
        installed
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
                Some(installed.view.epoch())
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
    incoming: &RuntimeFilterInstallView,
) -> Result<(), InstallContractError> {
    if flight.view.epoch() != incoming.epoch() {
        return Err(install_error(
            InstallContractErrorKind::EpochMismatch,
            "runtime filter deployment epoch differs from the installing epoch",
        ));
    }
    if !install_views_equivalent(&flight.view, incoming) {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "same deployment epoch carried a different in-flight install view",
        ));
    }
    Ok(())
}

fn compare_installed(
    installed: &InstalledDeployment,
    incoming: &RuntimeFilterInstallView,
) -> Result<RegistryInstallResult, InstallContractError> {
    if installed.view.epoch() != incoming.epoch() {
        return Err(install_error(
            InstallContractErrorKind::EpochMismatch,
            "runtime filter deployment epoch differs from the installed epoch",
        ));
    }
    if !install_views_equivalent(&installed.view, incoming) {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "same deployment epoch carried a different install view",
        ));
    }
    Ok(RegistryInstallResult {
        outcome: InstallOutcome::AlreadyInstalled,
        committed_at: Some(installed.committed_at),
        events: Vec::new(),
    })
}

fn validate_view(view: &RuntimeFilterInstallView) -> Result<(), InstallContractError> {
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

    for channel in view.channels().values() {
        validate_channel(channel)?;
    }
    Ok(())
}

struct RoutingBuild {
    producers: BTreeMap<BindingId, ProducerRoute>,
    subscriptions: BTreeMap<BindingId, Arc<SubscriptionGroup>>,
    routes:
        BTreeMap<RouteEdgeId, Arc<dyn crate::runtime_filter::port::subscription::SnapshotDelivery>>,
    channel_routes: BTreeMap<ChannelId, Vec<RouteEdgeId>>,
    route_event_identities: BTreeMap<RouteEdgeId, Vec<RouteEventIdentity>>,
}

fn build_routing(
    query_id: UniqueId,
    view: &RuntimeFilterInstallView,
    channels: &BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>,
    events: Arc<dyn RuntimeFilterEventSink>,
) -> Result<RoutingBuild, InstallContractError> {
    let mut build = RoutingBuild {
        producers: BTreeMap::new(),
        subscriptions: BTreeMap::new(),
        routes: BTreeMap::new(),
        channel_routes: BTreeMap::new(),
        route_event_identities: BTreeMap::new(),
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
        for (binding_id, producer) in deployment.producers() {
            build.producers.insert(
                *binding_id,
                ProducerRoute {
                    channel_id: *channel_id,
                    channel: channel.clone(),
                    expected_instances: producer.expected_fragment_instances().clone(),
                },
            );
        }
        for (binding_id, consumer) in deployment.consumers() {
            let route_edge_id = consumer.loopback_route_edge_id();
            let group = Arc::new(SubscriptionGroup::new(
                common,
                *binding_id,
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
        }
    }
    Ok(build)
}

fn validate_channel(channel: &CompleteOnceChannelDeployment) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::Membership { value_type, .. } = channel.logical_domain() else {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M1 supports only Membership logical domains",
        ));
    };
    if channel.lifecycle() != RuntimeFilterLifecycle::CompleteOnce
        || channel.reduction_requirement() != ReductionRequirement::SetUnion
        || channel.allowed_contribution_kinds()
            != &BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ])
        || channel.completion_requirement() != CompletionRequirement::ProducerClosed
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "channel does not match the CompleteOnce Membership SetUnion M1 matrix",
        ));
    }
    if MembershipValues::empty_for_data_type(value_type).is_none() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership data type is not supported by the runtime filter port",
        ));
    }
    if channel.producers().is_empty() || channel.consumers().is_empty() {
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
    for consumer in channel.consumers().values() {
        if consumer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "consumer expected fragment instance set must be non-empty",
            ));
        }
        if consumer.activation() != ConsumerActivation::BlockingSnapshot {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "M1 consumers must use BlockingSnapshot activation",
            ));
        }
        if !consumer
            .capabilities()
            .contains(&ArtifactCapability::Membership)
        {
            return Err(install_error(
                InstallContractErrorKind::MissingMembershipCapability,
                "M1 consumers must support Membership artifacts",
            ));
        }
    }
    Ok(())
}

fn validate_coverage(
    coverage: &Coverage,
    channel: &CompleteOnceChannelDeployment,
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

fn build_channels(
    query_id: UniqueId,
    view: &RuntimeFilterInstallView,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<BTreeMap<ChannelId, Arc<RuntimeFilterChannel>>, InstallContractError> {
    view.channels()
        .iter()
        .map(|(channel_id, deployment)| {
            RuntimeFilterChannel::new_unanchored(
                query_id,
                view.local_participant_id(),
                view.epoch(),
                deployment,
                memory_account.clone(),
            )
            .map(|channel| (*channel_id, Arc::new(channel)))
            .map_err(|error| {
                install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    error.to_string(),
                )
            })
        })
        .collect()
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

fn channels_equivalent(
    left: &CompleteOnceChannelDeployment,
    right: &CompleteOnceChannelDeployment,
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
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::runtime_filter::port::producer::{InstallContractErrorKind, InstallOutcome};
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };

    use super::{DeploymentRegistry, EventEmitter};

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
    ) -> CompleteOnceChannelDeployment {
        CompleteOnceChannelDeployment::new(
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

    fn invalid_deployment(case: InvalidDeployment) -> CompleteOnceChannelDeployment {
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
        CompleteOnceChannelDeployment::new(
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
            producers,
            consumers,
        )
    }

    fn view(
        channels: impl IntoIterator<Item = (u32, CompleteOnceChannelDeployment)>,
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
        second = CompleteOnceChannelDeployment::new(
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
            CompleteOnceChannelDeployment::new(
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
        let invalid = CompleteOnceChannelDeployment::new(
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
        let mut other = view([(1, channel(1, 10, 20, 30, 40))]);
        other = RuntimeFilterInstallView::new(
            DeploymentEpoch::new(10),
            other.local_participant_id(),
            other.channels().clone(),
        );
        assert_eq!(
            registry.install(other).unwrap_err().kind(),
            InstallContractErrorKind::EpochMismatch
        );
    }

    #[test]
    fn invalid_channel_causes_zero_partial_install() {
        let registry = registry();
        let mut invalid = channel(2, 11, 21, 31, 41);
        invalid = CompleteOnceChannelDeployment::new(
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

        let cases = [
            (
                RuntimeFilterInstallView::new(
                    DeploymentEpoch::new(0),
                    RuntimeFilterParticipantId::new(3),
                    BTreeMap::from([(ChannelId::new(2), base.clone())]),
                ),
                InstallContractErrorKind::InvalidEpoch,
            ),
            (
                view([(2, base.clone())]),
                InstallContractErrorKind::DuplicateIdentity,
            ),
            (
                view([(
                    1,
                    CompleteOnceChannelDeployment::new(
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
                        base.producers().clone(),
                        base.consumers().clone(),
                    ),
                )]),
                InstallContractErrorKind::UnsupportedChannelContract,
            ),
            (
                view([(
                    1,
                    CompleteOnceChannelDeployment::new(
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
        let invalid_policy = CompleteOnceChannelDeployment::new(
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

        let invalid_budget = CompleteOnceChannelDeployment::new(
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
        let invalid_policy_and_duplicate = CompleteOnceChannelDeployment::new(
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
