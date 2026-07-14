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

use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::port::events::{
    ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::identity::RouteEdgeId;
use crate::runtime_filter::port::subscription::{
    ArtifactAcquireOutcome, ArtifactDelivery, ArtifactDeliveryOutcome, BlockingSnapshotSubscription,
};

use super::EventBatchCompletion;

enum SubscriptionState {
    Pending,
    Terminal(ArtifactDeliveryOutcome),
}

pub(super) struct SubscriptionSlot {
    identity: ConsumerEventIdentity,
    events: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<SubscriptionState>,
    cancellation_event_barrier: Mutex<Option<Arc<EventBatchCompletion>>>,
    changed: Condvar,
}

impl SubscriptionSlot {
    pub(super) fn new(
        identity: ConsumerEventIdentity,
        events: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        Self {
            identity,
            events,
            state: Mutex::new(SubscriptionState::Pending),
            cancellation_event_barrier: Mutex::new(None),
            changed: Condvar::new(),
        }
    }

    fn deliver(&self, outcome: ArtifactDeliveryOutcome) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(*state, SubscriptionState::Pending) {
            *state = SubscriptionState::Terminal(outcome);
        }
        drop(state);
        self.changed.notify_all();
    }

    fn current_outcome(state: &SubscriptionState) -> Option<ArtifactAcquireOutcome> {
        match state {
            SubscriptionState::Pending => None,
            SubscriptionState::Terminal(outcome) => Some(outcome.acquire_outcome()),
        }
    }

    fn arm_cancellation_event(&self, barrier: Arc<EventBatchCompletion>) {
        *self
            .cancellation_event_barrier
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(barrier);
    }

    fn emit_outcome(&self, outcome: &ArtifactAcquireOutcome) {
        let event = match outcome {
            ArtifactAcquireOutcome::Published(bundle) => RuntimeFilterEvent::SubscriptionAcquired {
                identity: self.identity,
                version: bundle.version(),
            },
            ArtifactAcquireOutcome::Unsupported(reason) => {
                RuntimeFilterEvent::SubscriptionUnsupported {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            ArtifactAcquireOutcome::Unavailable(reason) => {
                RuntimeFilterEvent::SubscriptionUnavailable {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            ArtifactAcquireOutcome::Cancelled => RuntimeFilterEvent::SubscriptionCancelled {
                identity: self.identity,
            },
            ArtifactAcquireOutcome::TimedOut => RuntimeFilterEvent::SubscriptionTimedOut {
                identity: self.identity,
            },
        };
        if matches!(outcome, ArtifactAcquireOutcome::Cancelled) {
            let barrier = self
                .cancellation_event_barrier
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            if let Some(barrier) = barrier {
                let events = self.events.clone();
                barrier.on_complete(move || events.record(event));
                return;
            }
        }
        self.events.record(event);
    }
}

impl BlockingSnapshotSubscription for SubscriptionSlot {
    fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                matches!(state, SubscriptionState::Pending)
            })
            .unwrap_or_else(|error| error.into_inner());
        let outcome = Self::current_outcome(&state).unwrap_or(ArtifactAcquireOutcome::TimedOut);
        drop(state);
        self.emit_outcome(&outcome);
        outcome
    }

    fn snapshot(&self) -> Option<Arc<crate::runtime_filter::port::artifact::ArtifactBundle>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            SubscriptionState::Terminal(ArtifactDeliveryOutcome::Published(bundle)) => {
                Some(bundle.clone())
            }
            SubscriptionState::Pending | SubscriptionState::Terminal(_) => None,
        }
    }
}

pub(super) struct SubscriptionGroup {
    route_edge_id: RouteEdgeId,
    slots: BTreeMap<UniqueId, Arc<SubscriptionSlot>>,
    #[cfg(test)]
    before_deliver: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    delivery_call_count: AtomicUsize,
}

impl SubscriptionGroup {
    pub(super) fn new(
        common: RuntimeFilterEventIdentity,
        binding_id: crate::runtime_filter::model::contract::BindingId,
        route_edge_id: RouteEdgeId,
        instances: impl IntoIterator<Item = UniqueId>,
        events: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        let slots = instances
            .into_iter()
            .map(|instance| {
                (
                    instance,
                    Arc::new(SubscriptionSlot::new(
                        ConsumerEventIdentity::new(common, binding_id, instance),
                        events.clone(),
                    )),
                )
            })
            .collect();
        Self {
            route_edge_id,
            slots,
            #[cfg(test)]
            before_deliver: Mutex::new(None),
            #[cfg(test)]
            delivery_call_count: AtomicUsize::new(0),
        }
    }

    pub(super) fn slot(&self, instance: UniqueId) -> Option<Arc<SubscriptionSlot>> {
        self.slots.get(&instance).cloned()
    }

    pub(super) fn arm_cancellation_event(
        &self,
        route_edge_id: RouteEdgeId,
        barrier: Arc<EventBatchCompletion>,
    ) {
        if route_edge_id != self.route_edge_id {
            return;
        }
        for slot in self.slots.values() {
            slot.arm_cancellation_event(barrier.clone());
        }
    }

    #[cfg(test)]
    pub(super) fn set_before_deliver_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_deliver.lock().unwrap() = Some(hook);
    }

    #[cfg(test)]
    pub(super) fn delivery_call_count(&self) -> usize {
        self.delivery_call_count.load(Ordering::SeqCst)
    }
}

impl ArtifactDelivery for SubscriptionGroup {
    fn deliver(&self, route_edge_id: RouteEdgeId, outcome: ArtifactDeliveryOutcome) {
        if route_edge_id != self.route_edge_id {
            return;
        }
        #[cfg(test)]
        self.delivery_call_count.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        if let Some(hook) = self.before_deliver.lock().unwrap().take() {
            hook();
        }
        for slot in self.slots.values() {
            slot.deliver(outcome.clone());
        }
    }
}
