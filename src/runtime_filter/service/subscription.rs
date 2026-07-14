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
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::port::events::{
    ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::identity::RouteEdgeId;
use crate::runtime_filter::port::subscription::{
    AcquireOutcome, BlockingSnapshotSubscription, DeliveryTerminal, SnapshotDelivery,
};
use crate::runtime_filter::port::value_domain::LogicalSnapshot;

enum SubscriptionState {
    Pending,
    Completed(Arc<LogicalSnapshot>),
    Unavailable(crate::runtime_filter::port::subscription::UnavailableReason),
    Cancelled,
}

pub(super) struct SubscriptionSlot {
    identity: ConsumerEventIdentity,
    events: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<SubscriptionState>,
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
            changed: Condvar::new(),
        }
    }

    fn deliver(&self, snapshot: Arc<LogicalSnapshot>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(*state, SubscriptionState::Pending) {
            *state = SubscriptionState::Completed(snapshot);
        }
        drop(state);
        self.changed.notify_all();
    }

    fn terminal(&self, terminal: DeliveryTerminal) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(*state, SubscriptionState::Pending) {
            *state = match terminal {
                DeliveryTerminal::Unavailable(reason) => SubscriptionState::Unavailable(reason),
                DeliveryTerminal::Cancelled => SubscriptionState::Cancelled,
            };
        }
        drop(state);
        self.changed.notify_all();
    }

    fn current_outcome(state: &SubscriptionState) -> Option<AcquireOutcome> {
        match state {
            SubscriptionState::Pending => None,
            SubscriptionState::Completed(snapshot) => {
                Some(AcquireOutcome::Completed(snapshot.clone()))
            }
            SubscriptionState::Unavailable(reason) => Some(AcquireOutcome::Unavailable(*reason)),
            SubscriptionState::Cancelled => Some(AcquireOutcome::Cancelled),
        }
    }

    fn emit_outcome(&self, outcome: &AcquireOutcome) {
        let event = match outcome {
            AcquireOutcome::Completed(snapshot) => RuntimeFilterEvent::SubscriptionAcquired {
                identity: self.identity,
                version: snapshot.version(),
            },
            AcquireOutcome::Unavailable(reason) => RuntimeFilterEvent::SubscriptionUnavailable {
                identity: self.identity,
                reason: *reason,
            },
            AcquireOutcome::Cancelled => RuntimeFilterEvent::SubscriptionCancelled {
                identity: self.identity,
            },
            AcquireOutcome::TimedOut => RuntimeFilterEvent::SubscriptionTimedOut {
                identity: self.identity,
            },
        };
        self.events.record(event);
    }
}

impl BlockingSnapshotSubscription for SubscriptionSlot {
    fn acquire(&self, timeout: Duration) -> AcquireOutcome {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                matches!(state, SubscriptionState::Pending)
            })
            .unwrap_or_else(|error| error.into_inner());
        let outcome = Self::current_outcome(&state).unwrap_or(AcquireOutcome::TimedOut);
        drop(state);
        self.emit_outcome(&outcome);
        outcome
    }
}

pub(super) struct SubscriptionGroup {
    route_edge_id: RouteEdgeId,
    slots: BTreeMap<UniqueId, Arc<SubscriptionSlot>>,
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
        }
    }

    pub(super) fn slot(&self, instance: UniqueId) -> Option<Arc<SubscriptionSlot>> {
        self.slots.get(&instance).cloned()
    }
}

impl SnapshotDelivery for SubscriptionGroup {
    fn deliver(&self, route_edge_id: RouteEdgeId, snapshot: Arc<LogicalSnapshot>) {
        if route_edge_id != self.route_edge_id {
            return;
        }
        for slot in self.slots.values() {
            slot.deliver(snapshot.clone());
        }
    }

    fn terminal(&self, route_edge_id: RouteEdgeId, outcome: DeliveryTerminal) {
        if route_edge_id != self.route_edge_id {
            return;
        }
        for slot in self.slots.values() {
            slot.terminal(outcome);
        }
    }
}
