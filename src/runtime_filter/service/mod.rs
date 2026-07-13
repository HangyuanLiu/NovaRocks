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

mod memory;
mod producer;
mod registry;
mod subscription;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::ThreadId;
use std::time::Instant;

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::install::RuntimeFilterInstallView;
use crate::runtime_filter::port::producer::{
    InstallContractError, InstallOutcome, ProducerAdapter, RuntimeContractViolation,
    RuntimeContractViolationKind,
};
use crate::runtime_filter::port::subscription::BlockingSnapshotSubscription;
use crate::runtime_filter::port::support::{RuntimeFilterClock, RuntimeFilterMemoryAccount};

use self::producer::ServiceProducerAdapter;
use self::registry::DeploymentRegistry;

struct EventQueueState {
    draining: bool,
    draining_thread: Option<ThreadId>,
    next_batch_id: u64,
    batches: VecDeque<EventBatch>,
}

struct EventBatch {
    id: u64,
    ready: bool,
    events: VecDeque<RuntimeFilterEvent>,
    completion: Arc<EventBatchCompletion>,
}

struct EventBatchHandle {
    id: u64,
    completion: Arc<EventBatchCompletion>,
}

#[derive(Default)]
struct EventBatchCompletion {
    completed: Mutex<bool>,
    changed: Condvar,
}

impl EventBatchCompletion {
    fn wait(&self) {
        let mut completed = self
            .completed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !*completed {
            completed = self
                .changed
                .wait(completed)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn complete(&self) {
        let mut completed = self
            .completed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !*completed {
            *completed = true;
            self.changed.notify_all();
        }
    }
}

struct EventEmitter {
    sink: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<EventQueueState>,
    #[cfg(test)]
    after_publish_ready: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl EventEmitter {
    fn new(sink: Arc<dyn RuntimeFilterEventSink>) -> Self {
        Self {
            sink,
            state: Mutex::new(EventQueueState {
                draining: false,
                draining_thread: None,
                next_batch_id: 0,
                batches: VecDeque::new(),
            }),
            #[cfg(test)]
            after_publish_ready: Mutex::new(None),
        }
    }

    fn prequeue(
        &self,
        events: impl IntoIterator<Item = RuntimeFilterEvent>,
    ) -> Option<EventBatchHandle> {
        let events = events.into_iter().collect::<VecDeque<_>>();
        if events.is_empty() {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_batch_id;
        state.next_batch_id = state
            .next_batch_id
            .checked_add(1)
            .expect("runtime filter event batch identity exhausted");
        let completion = Arc::new(EventBatchCompletion::default());
        state.batches.push_back(EventBatch {
            id,
            ready: false,
            events,
            completion: completion.clone(),
        });
        drop(state);
        self.drain();
        Some(EventBatchHandle { id, completion })
    }

    fn publish(&self, batch: Option<EventBatchHandle>) {
        let Some(batch) = batch else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let queued = state
            .batches
            .iter_mut()
            .find(|queued| queued.id == batch.id)
            .expect("prequeued runtime filter event batch must remain pending");
        queued.ready = true;
        let reentrant = state.draining_thread == Some(std::thread::current().id());
        drop(state);
        #[cfg(test)]
        if let Some(hook) = self
            .after_publish_ready
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            hook();
        }
        self.drain();
        if !reentrant {
            batch.completion.wait();
        }
    }

    fn abort(&self, batch: Option<EventBatchHandle>) {
        let Some(batch) = batch else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let removed = state
            .batches
            .iter()
            .position(|queued| queued.id == batch.id)
            .and_then(|index| state.batches.remove(index));
        drop(state);
        if let Some(removed) = removed {
            debug_assert!(!removed.ready, "published event batches cannot be aborted");
            removed.completion.complete();
        }
        self.drain();
    }

    fn record_all(&self, events: impl IntoIterator<Item = RuntimeFilterEvent>) {
        let events = events.into_iter().collect::<VecDeque<_>>();
        if events.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_batch_id;
        state.next_batch_id = state
            .next_batch_id
            .checked_add(1)
            .expect("runtime filter event batch identity exhausted");
        let completion = Arc::new(EventBatchCompletion::default());
        state.batches.push_back(EventBatch {
            id,
            ready: true,
            events,
            completion: completion.clone(),
        });
        drop(state);
        self.drain();
    }

    fn drain(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.draining {
            return;
        }
        state.draining = true;
        state.draining_thread = Some(std::thread::current().id());
        loop {
            let next = match state.batches.front_mut() {
                Some(batch) if batch.ready => {
                    let event = batch.events.pop_front();
                    let completion = batch.events.is_empty().then(|| batch.completion.clone());
                    event.map(|event| (event, completion))
                }
                Some(_) | None => None,
            };
            let Some((event, completion)) = next else {
                state.draining = false;
                state.draining_thread = None;
                return;
            };
            if state
                .batches
                .front()
                .is_some_and(|batch| batch.events.is_empty())
            {
                state.batches.pop_front();
            }
            drop(state);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.sink.record(event);
            }));
            if let Some(completion) = completion {
                completion.complete();
            }
            state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        }
    }
}

impl RuntimeFilterEventSink for EventEmitter {
    fn record(&self, event: RuntimeFilterEvent) {
        self.record_all([event]);
    }
}

struct ActionDispatcher {
    registry: Arc<DeploymentRegistry>,
    events: Arc<EventEmitter>,
    channels: Mutex<BTreeMap<ChannelId, Arc<ChannelDispatchFlight>>>,
    #[cfg(test)]
    after_claim: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

#[derive(Default)]
struct ChannelDispatchState {
    next_order: u64,
    draining: bool,
    pending: BTreeMap<u64, ChannelAction>,
}

#[derive(Default)]
struct ChannelDispatchFlight {
    state: Mutex<ChannelDispatchState>,
    changed: Condvar,
}

impl ActionDispatcher {
    #[cfg(test)]
    fn pending_action_count(&self, channel_id: ChannelId) -> usize {
        self.channels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&channel_id)
            .map(|flight| {
                flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .pending
                    .len()
            })
            .unwrap_or(0)
    }

    fn dispatch(&self, channel_id: ChannelId, action: ChannelAction) {
        let Some(order) = action.dispatch_order() else {
            return;
        };
        let flight = self
            .channels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(channel_id)
            .or_default()
            .clone();
        let mut incoming = Some(action);
        loop {
            let mut state = flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if order < state.next_order {
                return;
            }
            if state.draining && order == state.next_order {
                state = flight
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                drop(state);
                continue;
            }
            if let Some(action) = incoming.take() {
                state.pending.entry(order).or_insert(action);
            }
            if !state.draining
                && order == state.next_order
                && state.pending.contains_key(&state.next_order)
            {
                let next_order = state.next_order;
                let action = state
                    .pending
                    .remove(&next_order)
                    .expect("next ordered runtime filter action is pending");
                state.draining = true;
                drop(state);
                #[cfg(test)]
                if let Some(hook) = self
                    .after_claim
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                {
                    hook();
                }
                let batch = self.route_and_prequeue(channel_id, &action);
                let mut state = flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                state.next_order = state
                    .next_order
                    .checked_add(1)
                    .expect("runtime filter dispatch order exhausted");
                state.draining = false;
                flight.changed.notify_all();
                drop(state);
                self.events.publish(batch);
                return;
            }
            state = flight
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
            drop(state);
        }
    }

    fn route_and_prequeue(
        &self,
        channel_id: ChannelId,
        action: &ChannelAction,
    ) -> Option<EventBatchHandle> {
        let Some(installed) = self.registry.installation_for_dispatch() else {
            return None;
        };
        if matches!(action, ChannelAction::Progress { .. }) {
            return self.events.prequeue(action.events().iter().cloned());
        }
        let mut events = action.events().to_vec();
        if let ChannelAction::Completed { snapshot, .. } = &action {
            for route_edge_id in installed.routes_for_channel(channel_id) {
                if installed.router().contains_route(*route_edge_id) {
                    events.extend(
                        installed
                            .route_event_identities(*route_edge_id)
                            .iter()
                            .copied()
                            .map(|identity| RuntimeFilterEvent::LoopbackDelivered {
                                identity,
                                version: snapshot.version(),
                            }),
                    );
                }
            }
        }
        let batch = self.events.prequeue(events);
        installed
            .router()
            .route(installed.routes_for_channel(channel_id), action);
        batch
    }
}

pub(crate) struct RuntimeFilterService {
    _query_id: UniqueId,
    _clock: Arc<dyn RuntimeFilterClock>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    registry: Arc<DeploymentRegistry>,
    dispatcher: Arc<ActionDispatcher>,
    producer_handles: Mutex<BTreeMap<(BindingId, UniqueId), Weak<ServiceProducerAdapter>>>,
    operation: Mutex<()>,
}

impl RuntimeFilterService {
    fn new_with_dependencies(
        query_id: UniqueId,
        clock: Arc<dyn RuntimeFilterClock>,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Self {
        let events = Arc::new(EventEmitter::new(event_sink));
        let registry = Arc::new(DeploymentRegistry::new(
            query_id,
            clock.clone(),
            memory_account.clone(),
            events.clone(),
        ));
        let dispatcher = Arc::new(ActionDispatcher {
            registry: registry.clone(),
            events: events.clone(),
            channels: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            after_claim: Mutex::new(None),
        });
        Self {
            _query_id: query_id,
            _clock: clock,
            memory_account,
            registry,
            dispatcher,
            producer_handles: Mutex::new(BTreeMap::new()),
            operation: Mutex::new(()),
        }
    }

    pub(crate) fn install(
        &self,
        view: RuntimeFilterInstallView,
    ) -> Result<InstallOutcome, InstallContractError> {
        let result = self.registry.install(view)?;
        let outcome = result.outcome();
        Ok(outcome)
    }

    pub(crate) fn open_producer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Result<Arc<dyn ProducerAdapter>, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = self
            .registry
            .active_installation()
            .ok_or_else(service_cancelled)?;
        let route = installed.producer(binding_id).ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed on this participant",
            )
        })?;
        if !route.expected_instances.contains(&fragment_instance_id) {
            return Err(violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            ));
        }
        route
            .channel
            .open_producer(binding_id, fragment_instance_id, local_partition_count)?;
        let key = (binding_id, fragment_instance_id);
        let mut handles = self
            .producer_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(handle) = handles.get(&key).and_then(Weak::upgrade) {
            return Ok(handle);
        }
        let handle = Arc::new(ServiceProducerAdapter::new(
            route.channel_id,
            route.channel.clone(),
            binding_id,
            fragment_instance_id,
            self.memory_account.clone(),
            self.dispatcher.clone(),
        ));
        handles.insert(key, Arc::downgrade(&handle));
        Ok(handle)
    }

    pub(crate) fn subscribe(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<Arc<dyn BlockingSnapshotSubscription>, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = self
            .registry
            .active_installation()
            .ok_or_else(service_cancelled)?;
        if !installed.has_consumer(binding_id) {
            return Err(violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed on this participant",
            ));
        }
        installed
            .subscription(binding_id, fragment_instance_id)
            .map(|subscription| subscription as Arc<dyn BlockingSnapshotSubscription>)
            .ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                    "consumer fragment instance is not installed for this binding",
                )
            })
    }

    pub(crate) fn expire_deadlines(&self, now: Instant) {
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.active_installation()
        };
        if let Some(installed) = installed {
            for (channel_id, channel) in installed.channels() {
                let action = channel.expire_deadline(now);
                if !matches!(action, ChannelAction::None) {
                    self.dispatcher.dispatch(channel_id, action);
                }
            }
        }
    }

    pub(crate) fn cancel(&self) {
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.cancel()
        };
        if let Some(installed) = installed {
            for (channel_id, channel) in installed.channels() {
                let action = channel.cancel();
                let action = if matches!(action, ChannelAction::None) {
                    channel.terminal_action()
                } else {
                    action
                };
                if !matches!(action, ChannelAction::None) {
                    self.dispatcher.dispatch(channel_id, action);
                }
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.cancel();
    }

    #[cfg(test)]
    fn set_producer_before_dispatch_hook(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        if let Some(handle) = self
            .producer_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .and_then(Weak::upgrade)
        {
            handle.set_before_dispatch(hook);
        }
    }

    #[cfg(test)]
    fn set_dispatcher_after_claim_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .after_claim
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn dispatcher_pending_action_count(&self, channel_id: ChannelId) -> usize {
        self.dispatcher.pending_action_count(channel_id)
    }

    #[cfg(test)]
    fn set_before_commit_clock_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.registry.set_before_commit_clock_hook(hook);
    }

    #[cfg(test)]
    fn set_after_commit_before_publish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.registry.set_after_commit_before_publish_hook(hook);
    }
}

impl Drop for RuntimeFilterService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn service_cancelled() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ServiceUnavailable,
        "runtime filter service is uninstalled or cancelled",
    )
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::{
        ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
        RuntimeFilterEventSink,
    };
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::runtime_filter::port::producer::{
        InstallOutcome, ProducerAdapter, RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::subscription::{AcquireOutcome, BlockingSnapshotSubscription};
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    use super::memory::MemTrackerMemoryAccount;
    use super::{ChannelAction, EventEmitter, RuntimeFilterService};

    #[derive(Default)]
    struct Events(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for Events {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    struct PanicOnceEvents {
        panicked: AtomicBool,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for PanicOnceEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if !self.panicked.swap(true, Ordering::SeqCst) {
                panic!("intentional event sink panic");
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    struct Clock(Instant);

    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            self.0
        }
    }

    struct DynamicClock;

    impl RuntimeFilterClock for DynamicClock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    #[derive(Default)]
    struct RejectingMemoryAccount {
        calls: AtomicUsize,
    }

    struct BlockingFirstRejectingMemoryAccount {
        calls: AtomicUsize,
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl RuntimeFilterMemoryAccount for BlockingFirstRejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                return Err(MemoryAccountError::CapacityExceeded);
            }
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    struct BlockingInstallEvents {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    struct BlockingLastInstallEvent {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for BlockingLastInstallEvent {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::ChannelPlanned { .. }) {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    impl RuntimeFilterEventSink for BlockingInstallEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    struct ReentrantServiceClock {
        service: Mutex<Weak<RuntimeFilterService>>,
        now: Instant,
    }

    impl RuntimeFilterClock for ReentrantServiceClock {
        fn now(&self) -> Instant {
            let service = self
                .service
                .lock()
                .unwrap()
                .upgrade()
                .expect("service installed before clock use");
            assert_eq!(
                service
                    .subscribe(BindingId::new(30), uid(30))
                    .err()
                    .expect("service must remain unavailable during installation")
                    .kind(),
                RuntimeContractViolationKind::ServiceUnavailable
            );
            self.now
        }
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId { hi: 70, lo }
    }

    fn deployment(
        channel_id: u32,
        producer_binding: u32,
        consumer_binding: u32,
        route_edge: u32,
        producer_instances: impl IntoIterator<Item = i64>,
        consumer_instances: impl IntoIterator<Item = i64>,
        deadline_ms: u64,
    ) -> CompleteOnceChannelDeployment {
        let witness = CoverageWitnessId::new(channel_id + 100);
        CompleteOnceChannelDeployment::new(
            ChannelId::new(channel_id),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(8192),
            BTreeMap::from([(
                BindingId::new(producer_binding),
                ProducerDeployment::new(witness, producer_instances.into_iter().map(uid).collect()),
            )]),
            BTreeMap::from([(
                BindingId::new(consumer_binding),
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    RouteEdgeId::new(route_edge),
                    consumer_instances.into_iter().map(uid).collect(),
                ),
            )]),
        )
    }

    fn view(
        channels: impl IntoIterator<Item = CompleteOnceChannelDeployment>,
    ) -> RuntimeFilterInstallView {
        RuntimeFilterInstallView::new(
            DeploymentEpoch::new(9),
            RuntimeFilterParticipantId::new(3),
            channels
                .into_iter()
                .map(|channel| (channel.channel_id(), channel))
                .collect(),
        )
    }

    struct Fixture {
        service: Arc<RuntimeFilterService>,
        events: Arc<Events>,
        started: Instant,
        tracker: Arc<MemTrackerMemoryAccount>,
    }

    fn fixture() -> Fixture {
        let events = Arc::new(Events::default());
        let started = Instant::now();
        let tracker = MemTrackerMemoryAccount::new_root_for_test("runtime-filter-test-query");
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events.clone(),
            tracker.clone(),
        ));
        Fixture {
            service,
            events,
            started,
            tracker,
        }
    }

    fn install_one(fixture: &Fixture) {
        assert_eq!(
            fixture
                .service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    fn open_and_subscribe(
        fixture: &Fixture,
    ) -> (
        Arc<dyn ProducerAdapter>,
        Arc<dyn BlockingSnapshotSubscription>,
    ) {
        let subscription = fixture
            .service
            .subscribe(BindingId::new(30), uid(30))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        (producer, subscription)
    }

    fn complete(producer: &Arc<dyn ProducerAdapter>, value: i64) {
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([value]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
    }

    #[test]
    fn unpublished_causal_batch_blocks_later_subscription_event_until_publish() {
        let events = Arc::new(Events::default());
        let emitter = EventEmitter::new(events.clone());
        let common = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
        );
        let batch = emitter.prequeue([RuntimeFilterEvent::ChannelCompleted {
            identity: common,
            version: LogicalVersion::FIRST,
        }]);
        emitter.record(RuntimeFilterEvent::SubscriptionAcquired {
            identity: ConsumerEventIdentity::new(common, BindingId::new(30), uid(30)),
            version: LogicalVersion::FIRST,
        });
        assert!(events.0.lock().unwrap().is_empty());
        emitter.publish(batch);
        assert!(matches!(
            events.0.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::ChannelCompleted { .. },
                RuntimeFilterEvent::SubscriptionAcquired { .. }
            ]
        ));
    }

    #[test]
    fn dispatcher_owner_does_not_return_after_claiming_an_earlier_pending_order() {
        fn action(order: u64) -> ChannelAction {
            ChannelAction::Progress {
                order: Some(order),
                outcome: SubmitOutcome::Applied,
                events: vec![RuntimeFilterEvent::ChannelPlanned {
                    identity: RuntimeFilterEventIdentity::new(
                        uid(0),
                        RuntimeFilterParticipantId::new(3),
                        ChannelId::new(100 + u32::try_from(order).unwrap()),
                        DeploymentEpoch::new(9),
                    ),
                }],
            }
        }

        let fixture = fixture();
        install_one(&fixture);
        fixture.events.0.lock().unwrap().clear();
        let channel_id = ChannelId::new(1);
        let flight = fixture
            .service
            .dispatcher
            .channels
            .lock()
            .unwrap()
            .entry(channel_id)
            .or_default()
            .clone();
        {
            let mut state = flight.state.lock().unwrap();
            state.pending.insert(1, action(1));
            state.pending.insert(2, action(2));
        }
        fixture.service.dispatcher.dispatch(channel_id, action(0));

        let dispatcher = fixture.service.dispatcher.clone();
        let (second_tx, second_rx) = mpsc::channel();
        std::thread::spawn(move || {
            dispatcher.dispatch(channel_id, action(2));
            second_tx.send(()).unwrap();
        });
        assert!(second_rx.recv_timeout(Duration::from_millis(50)).is_err());

        let dispatcher = fixture.service.dispatcher.clone();
        let (first_tx, first_rx) = mpsc::channel();
        std::thread::spawn(move || {
            dispatcher.dispatch(channel_id, action(1));
            first_tx.send(()).unwrap();
        });
        first_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let state = flight.state.lock().unwrap();
        assert_eq!(state.next_order, 3);
        assert!(state.pending.is_empty());
        drop(state);
        let channel_ids = fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                RuntimeFilterEvent::ChannelPlanned { identity } => identity.channel_id().get(),
                other => panic!("unexpected event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(channel_ids, [100, 101, 102]);
    }

    #[test]
    fn install_waits_when_another_drainer_owns_its_published_batch() {
        let (sink_entered_tx, sink_entered_rx) = mpsc::channel();
        let (sink_release_tx, sink_release_rx) = mpsc::channel();
        let sink = Arc::new(BlockingLastInstallEvent {
            entered: sink_entered_tx,
            release: Mutex::new(sink_release_rx),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("publish-completion-race"),
        ));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (publish_release_tx, publish_release_rx) = mpsc::channel();
        let publish_release_rx = Mutex::new(publish_release_rx);
        *service
            .dispatcher
            .events
            .after_publish_ready
            .lock()
            .unwrap() = Some(Arc::new(move || {
            ready_tx.send(()).unwrap();
            publish_release_rx.lock().unwrap().recv().unwrap();
        }));

        let install_view = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let install_service = service.clone();
        let (install_tx, install_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_tx
                .send(install_service.install(install_view))
                .unwrap()
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let emitter = service.dispatcher.events.clone();
        let later_identity = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(2),
            DeploymentEpoch::new(9),
        );
        let (drainer_tx, drainer_rx) = mpsc::channel();
        std::thread::spawn(move || {
            emitter.record(RuntimeFilterEvent::ChannelCancelled {
                identity: later_identity,
            });
            drainer_tx.send(()).unwrap();
        });
        sink_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        publish_release_tx.send(()).unwrap();

        let early_install = install_rx.recv_timeout(Duration::from_millis(50));
        sink_release_tx.send(()).unwrap();
        drainer_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        match early_install {
            Ok(result) => panic!("install returned before its last sink callback: {result:?}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => panic!("install result channel disconnected: {error}"),
        }
        assert_eq!(
            install_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert!(matches!(
            sink.recorded.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::DeploymentInstalled { .. },
                RuntimeFilterEvent::ChannelPlanned { .. },
                RuntimeFilterEvent::ChannelCancelled { .. }
            ]
        ));
    }

    #[test]
    fn panicking_event_sink_is_contained_and_queue_keeps_draining() {
        let sink = Arc::new(PanicOnceEvents {
            panicked: AtomicBool::new(false),
            recorded: Mutex::new(Vec::new()),
        });
        let emitter = EventEmitter::new(sink.clone());
        let identity = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
        );
        emitter.record_all([
            RuntimeFilterEvent::ChannelPlanned { identity },
            RuntimeFilterEvent::ChannelCancelled { identity },
        ]);
        emitter.record(RuntimeFilterEvent::ChannelPlanned { identity });
        assert_eq!(sink.recorded.lock().unwrap().len(), 2);
    }

    #[test]
    fn install_batch_is_reserved_before_installed_state_is_observable() {
        let fixture = fixture();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_commit_before_publish_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let service = fixture.service.clone();
        let (installed_tx, installed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            installed_tx
                .send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let (submit_tx, submit_rx) = mpsc::channel();
        std::thread::spawn(move || {
            submit_tx
                .send(producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([1]), false),
                ))
                .unwrap();
        });
        assert!(submit_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(fixture.events.0.lock().unwrap().is_empty());
        release_tx.send(()).unwrap();
        assert_eq!(
            installed_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        let events = fixture.events.0.lock().unwrap();
        assert!(matches!(
            events[0],
            RuntimeFilterEvent::DeploymentInstalled { .. }
        ));
        assert!(matches!(
            events[1],
            RuntimeFilterEvent::ChannelPlanned { .. }
        ));
        assert!(matches!(
            events[2],
            RuntimeFilterEvent::DeltaAccepted { .. }
        ));
    }

    #[test]
    fn deadline_is_anchored_after_delayed_build_and_idempotent_install_does_not_reset_it() {
        let events = Arc::new(Events::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            events,
            MemTrackerMemoryAccount::new_root_for_test("delayed-build-clock"),
        ));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        service.set_before_commit_clock_hook(Arc::new(move || {
            ready_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let install_thread = service.clone();
        let first_view = install.clone();
        let handle = std::thread::spawn(move || install_thread.install(first_view));
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let released_at = Instant::now();
        release_tx.send(()).unwrap();
        assert_eq!(handle.join().unwrap().unwrap(), InstallOutcome::Installed);
        let deadline = service.registry.deadline(ChannelId::new(1)).unwrap();
        assert!(deadline >= released_at + Duration::from_millis(90));
        assert_eq!(
            service.install(install).unwrap(),
            InstallOutcome::AlreadyInstalled
        );
        assert_eq!(service.registry.deadline(ChannelId::new(1)), Some(deadline));
    }

    #[test]
    fn install_clock_may_reenter_service_reads_without_deadlock() {
        let events = Arc::new(Events::default());
        let clock = Arc::new(ReentrantServiceClock {
            service: Mutex::new(Weak::new()),
            now: Instant::now(),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            clock.clone(),
            events,
            MemTrackerMemoryAccount::new_root_for_test("reentrant-service-clock"),
        ));
        *clock.service.lock().unwrap() = Arc::downgrade(&service);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("reentrant clock deadlocked service install")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn loopback_never_bypasses_complete_once_publish_gate() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::TimedOut
        ));
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false)
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::TimedOut
        ));
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
    }

    #[test]
    fn loopback_delivers_completed_logical_snapshot_through_real_ports() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        complete(&producer, 7);
        let AcquireOutcome::Completed(snapshot) = subscription.acquire(Duration::ZERO) else {
            panic!("expected completed snapshot");
        };
        assert_eq!(snapshot.channel_id(), ChannelId::new(1));
        assert_eq!(snapshot.version(), LogicalVersion::FIRST);
        assert_eq!(snapshot.domain().values(), &MembershipValues::int64([7]));
    }

    #[test]
    fn blocking_acquire_returns_same_immutable_version_to_all_instances() {
        let fixture = fixture();
        fixture
            .service
            .install(view([deployment(1, 10, 30, 40, [10], [30, 31], 100)]))
            .unwrap();
        let first = fixture
            .service
            .subscribe(BindingId::new(30), uid(30))
            .unwrap();
        let second = fixture
            .service
            .subscribe(BindingId::new(30), uid(31))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        complete(&producer, 11);
        let (AcquireOutcome::Completed(first), AcquireOutcome::Completed(second)) = (
            first.acquire(Duration::ZERO),
            second.acquire(Duration::ZERO),
        ) else {
            panic!("expected both subscriptions completed");
        };
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.version(), LogicalVersion::FIRST);
    }

    #[test]
    fn subscription_timeout_does_not_mark_channel_unavailable() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::TimedOut
        ));
        complete(&producer, 5);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
    }

    #[test]
    fn expire_deadlines_marks_only_incomplete_channels_unavailable() {
        let fixture = fixture();
        fixture
            .service
            .install(view([
                deployment(1, 10, 30, 40, [10], [30], 100),
                deployment(2, 11, 31, 41, [11], [31], 100),
            ]))
            .unwrap();
        let completed_subscription = fixture
            .service
            .subscribe(BindingId::new(30), uid(30))
            .unwrap();
        let incomplete_subscription = fixture
            .service
            .subscribe(BindingId::new(31), uid(31))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        complete(&producer, 1);
        fixture
            .service
            .expire_deadlines(fixture.started + Duration::from_millis(100));
        assert!(matches!(
            completed_subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
        assert!(matches!(
            incomplete_subscription.acquire(Duration::ZERO),
            AcquireOutcome::Unavailable(_)
        ));
    }

    #[test]
    fn unauthorized_binding_instance_or_partition_fails_before_mutation() {
        let fixture = fixture();
        install_one(&fixture);
        assert_eq!(
            fixture
                .service
                .open_producer(BindingId::new(99), uid(10), 1)
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(99), 1)
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        assert_eq!(
            fixture
                .service
                .subscribe(BindingId::new(99), uid(30))
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            fixture
                .service
                .subscribe(BindingId::new(30), uid(99))
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert!(
            producer
                .submit(
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false)
                )
                .is_err()
        );
        assert_eq!(fixture.tracker.current(), 0);
        assert_eq!(fixture.tracker.peak(), 0);
    }

    #[test]
    fn invalid_partition_precedes_rejecting_temporary_memory_account() {
        let account = Arc::new(RejectingMemoryAccount::default());
        let service = RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            Arc::new(Events::default()),
            account.clone(),
        );
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.calls.load(Ordering::SeqCst), 0);
        assert!(
            !service
                .registry
                .channel(ChannelId::new(1))
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn temporary_reservation_failure_revalidates_concurrent_duplicate() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let account = Arc::new(BlockingFirstRejectingMemoryAccount {
            calls: AtomicUsize::new(0),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            Arc::new(Events::default()),
            account,
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let first = producer.clone();
        let (first_tx, first_rx) = mpsc::channel();
        std::thread::spawn(move || {
            first_tx
                .send(first.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                ))
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            first_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert!(
            !service
                .registry
                .channel(ChannelId::new(1))
                .unwrap()
                .is_terminal()
        );
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ValueDomainDelta::new(MembershipValues::int64([8]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
    }

    #[test]
    fn install_follower_waits_for_reserved_batch_sink_completion() {
        let (sink_entered_tx, sink_entered_rx) = mpsc::channel();
        let (sink_release_tx, sink_release_rx) = mpsc::channel();
        let events = Arc::new(BlockingInstallEvents {
            entered: sink_entered_tx,
            release: Mutex::new(sink_release_rx),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("install-follower-publish"),
        ));
        let (commit_ready_tx, commit_ready_rx) = mpsc::channel();
        let (commit_release_tx, commit_release_rx) = mpsc::channel();
        let commit_release_rx = Mutex::new(commit_release_rx);
        service.set_before_commit_clock_hook(Arc::new(move || {
            commit_ready_tx.send(()).unwrap();
            commit_release_rx.lock().unwrap().recv().unwrap();
        }));
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let leader_service = service.clone();
        let leader_view = install.clone();
        let (leader_tx, leader_rx) = mpsc::channel();
        std::thread::spawn(move || leader_tx.send(leader_service.install(leader_view)).unwrap());
        commit_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let follower_service = service.clone();
        let (follower_tx, follower_rx) = mpsc::channel();
        std::thread::spawn(move || follower_tx.send(follower_service.install(install)).unwrap());
        assert!(follower_rx.recv_timeout(Duration::from_millis(50)).is_err());
        commit_release_tx.send(()).unwrap();
        sink_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let follower_before_publish = follower_rx.recv_timeout(Duration::from_millis(50));
        sink_release_tx.send(()).unwrap();
        assert!(follower_before_publish.is_err());
        assert_eq!(
            leader_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert_eq!(
            follower_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::AlreadyInstalled
        );
        assert!(matches!(
            events.recorded.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::DeploymentInstalled { .. },
                RuntimeFilterEvent::ChannelPlanned { .. }
            ]
        ));
    }

    #[test]
    fn duplicate_open_same_partition_count_is_idempotent() {
        let fixture = fixture();
        install_one(&fixture);
        let first = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 2)
            .unwrap();
        let second = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 2)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn conflicting_open_partition_count_fails() {
        let fixture = fixture();
        install_one(&fixture);
        fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        assert!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(10), 2)
                .is_err()
        );
    }

    #[test]
    fn service_cancel_wakes_all_subscriptions_and_rejects_late_handles() {
        let fixture = fixture();
        install_one(&fixture);
        let subscription = fixture
            .service
            .subscribe(BindingId::new(30), uid(30))
            .unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(subscription.acquire(Duration::from_secs(5)))
                .unwrap()
        });
        fixture.service.cancel();
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            AcquireOutcome::Cancelled
        ));
        assert!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(10), 1)
                .is_err()
        );
        assert!(
            fixture
                .service
                .subscribe(BindingId::new(30), uid(30))
                .is_err()
        );
        assert!(
            fixture
                .service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .is_err()
        );
    }

    #[test]
    fn cancel_routes_completed_winner_even_before_producer_dispatch() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([17]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let (outcome_tx, outcome_rx) = mpsc::channel();
        std::thread::spawn(move || {
            outcome_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        fixture.service.cancel();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            outcome_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        let events = fixture.events.0.lock().unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            1
        );
        let position = |predicate: fn(&RuntimeFilterEvent) -> bool| {
            events
                .iter()
                .position(predicate)
                .expect("expected causal runtime-filter event")
        };
        let producer_closed =
            position(|event| matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. }));
        let channel_completed =
            position(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }));
        let loopback_delivered =
            position(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }));
        let subscription_acquired =
            position(|event| matches!(event, RuntimeFilterEvent::SubscriptionAcquired { .. }));
        assert!(producer_closed < channel_completed);
        assert!(channel_completed < loopback_delivered);
        assert!(loopback_delivered < subscription_acquired);
        for predicate in [
            (|event: &RuntimeFilterEvent| {
                matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. })
            }) as fn(&RuntimeFilterEvent) -> bool,
            |event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }),
            |event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }),
        ] {
            assert_eq!(events.iter().filter(|event| predicate(event)).count(), 1);
        }
    }

    #[test]
    fn paused_progress_is_emitted_before_later_cancel_terminal() {
        let fixture = fixture();
        install_one(&fixture);
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let (submit_tx, submit_rx) = mpsc::channel();
        std::thread::spawn(move || {
            submit_tx
                .send(producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([9]), false),
                ))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let service = fixture.service.clone();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            cancel_tx.send(()).unwrap();
        });
        assert!(cancel_rx.recv_timeout(Duration::from_millis(50)).is_err());
        release_tx.send(()).unwrap();
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let events = fixture.events.0.lock().unwrap();
        let delta = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::DeltaAccepted { .. }))
            .unwrap();
        let cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .unwrap();
        assert!(delta < cancelled);
    }

    #[test]
    fn duplicate_terminal_dispatch_waits_for_claimed_route_and_notify() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([12]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_dispatcher_after_claim_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let (close_tx, close_rx) = mpsc::channel();
        std::thread::spawn(move || {
            close_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let service = fixture.service.clone();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            cancel_tx.send(()).unwrap();
        });
        assert!(cancel_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::TimedOut
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            close_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
        assert_eq!(
            fixture
                .service
                .dispatcher_pending_action_count(ChannelId::new(1)),
            0
        );
    }

    #[test]
    fn service_emits_stable_control_contribution_route_and_outcome_events() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        complete(&producer, 3);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
        let events = fixture.events.0.lock().unwrap();
        assert!(
            matches!(events.first(), Some(RuntimeFilterEvent::DeploymentInstalled { query_id, participant_id, epoch }) if *query_id == uid(0) && participant_id.get() == 3 && epoch.get() == 9)
        );
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::DeltaAccepted { identity } if identity.query_id() == uid(0) && identity.participant_id().get() == 3 && identity.channel_id().get() == 1 && identity.epoch().get() == 9 && identity.stream().binding_id().get() == 10 && identity.stream().fragment_instance_id() == uid(10) && identity.stream().partition_id().get() == 0 && identity.sequence().get() == 0)));
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { identity, version } if identity.common().query_id() == uid(0) && identity.consumer_binding_id().get() == 30 && identity.fragment_instance_id() == uid(30) && identity.route_edge_id().get() == 40 && *version == LogicalVersion::FIRST)));
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::SubscriptionAcquired { identity, version } if identity.consumer_binding_id().get() == 30 && *version == LogicalVersion::FIRST)));
    }

    struct ReentrantSink {
        service: Mutex<Weak<RuntimeFilterService>>,
    }

    impl RuntimeFilterEventSink for ReentrantSink {
        fn record(&self, _event: RuntimeFilterEvent) {
            if let Some(service) = self.service.lock().unwrap().upgrade() {
                let _ = service.install(view([]));
            }
        }
    }

    struct NonemptyReentrantInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        view: Mutex<Option<RuntimeFilterInstallView>>,
        outcome: mpsc::Sender<InstallOutcome>,
    }

    impl RuntimeFilterEventSink for NonemptyReentrantInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(view) = self.view.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            self.outcome.send(service.install(view).unwrap()).unwrap();
        }
    }

    struct CrossThreadReentrantInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        view: Mutex<Option<RuntimeFilterInstallView>>,
        outcome: mpsc::Sender<Option<InstallOutcome>>,
    }

    impl RuntimeFilterEventSink for CrossThreadReentrantInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(view) = self.view.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            let (worker_tx, worker_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = worker_tx.send(service.install(view));
            });
            self.outcome
                .send(
                    worker_rx
                        .recv_timeout(Duration::from_secs(1))
                        .ok()
                        .and_then(Result::ok),
                )
                .unwrap();
        }
    }

    #[test]
    fn nonempty_reentrant_install_from_event_sink_does_not_wait_on_its_own_batch() {
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let (reentrant_tx, reentrant_rx) = mpsc::channel();
        let sink = Arc::new(NonemptyReentrantInstallSink {
            service: Mutex::new(Weak::new()),
            view: Mutex::new(Some(install.clone())),
            outcome: reentrant_tx,
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("nonempty-reentrant-install"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        let (outer_tx, outer_rx) = mpsc::channel();
        std::thread::spawn(move || outer_tx.send(service.install(install)).unwrap());
        assert_eq!(
            reentrant_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("event-sink install reentry deadlocked"),
            InstallOutcome::AlreadyInstalled
        );
        assert_eq!(
            outer_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer install did not finish")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn cross_thread_reentrant_install_from_event_sink_observes_logical_commit() {
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let (reentrant_tx, reentrant_rx) = mpsc::channel();
        let sink = Arc::new(CrossThreadReentrantInstallSink {
            service: Mutex::new(Weak::new()),
            view: Mutex::new(Some(install.clone())),
            outcome: reentrant_tx,
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("cross-thread-reentrant-install"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        let (outer_tx, outer_rx) = mpsc::channel();
        std::thread::spawn(move || outer_tx.send(service.install(install)).unwrap());
        assert_eq!(
            reentrant_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("event-sink did not report cross-thread install outcome"),
            Some(InstallOutcome::AlreadyInstalled)
        );
        assert_eq!(
            outer_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer install did not finish")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn reentrant_event_sink_does_not_deadlock_registry_or_channel_lock() {
        let sink = Arc::new(ReentrantSink {
            service: Mutex::new(Weak::new()),
        });
        let started = Instant::now();
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("reentrant-query"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        assert_eq!(
            service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .unwrap(),
            InstallOutcome::Installed
        );
        let subscription = service.subscribe(BindingId::new(30), uid(30)).unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1)
            .unwrap();
        complete(&producer, 7);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            AcquireOutcome::Completed(_)
        ));
    }
}
