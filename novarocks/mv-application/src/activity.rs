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

//! Process-local serialization for activity against one materialized view.
//!
//! The gate deliberately does not model scheduler or maintenance capacity.
//! Callers first enqueue a ticket and acquire their own capacity only after a
//! ticket obtains its lease, so waiting does not consume either worker's
//! independent concurrency budget.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};

use novarocks_query_application::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};

/// A stable, provider-neutral identity whose input has already passed
/// SQL/catalog canonicalization. Quoted identifiers intentionally stay exact.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanonicalMvTarget {
    catalog: Option<String>,
    database: String,
    name: String,
}

impl CanonicalMvTarget {
    pub fn from_parts(catalog: Option<&str>, database: &str, name: &str) -> Self {
        Self {
            catalog: catalog.map(str::to_owned),
            database: database.to_owned(),
            name: name.to_owned(),
        }
    }
}

/// The application path currently holding, or waiting to hold, an MV gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvActivityOwner {
    Create,
    Alter,
    Drop,
    ManualRefresh,
    ScheduledRefresh,
    Repartition,
    AutomaticMaintenance,
}

impl MvActivityOwner {
    fn is_worker_owned(self) -> bool {
        matches!(self, Self::ScheduledRefresh | Self::AutomaticMaintenance)
    }
}

/// Admission can no longer be granted because process shutdown has started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvActivityGateError {
    Stopping,
}

/// A process-local FIFO gate shared by DDL, foreground refresh, the scheduler,
/// and automatic maintenance.
#[derive(Clone, Default)]
pub struct MvActivityGate {
    inner: Arc<Mutex<GateState>>,
}

impl MvActivityGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one attempt without taking an execution permit.
    pub fn request(
        &self,
        target: CanonicalMvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, MvActivityGateError> {
        let mut state = lock(&self.inner);
        if state.stopping {
            return Err(MvActivityGateError::Stopping);
        }
        let ticket_id = state.next_ticket_id;
        state.next_ticket_id = state
            .next_ticket_id
            .checked_add(1)
            .unwrap_or_else(|| panic!("MV activity ticket ID overflow"));
        state
            .entries
            .entry(target.clone())
            .or_default()
            .waiters
            .push_back(Waiter { ticket_id, owner });
        Ok(MvActivityTicket {
            inner: Arc::downgrade(&self.inner),
            target,
            ticket_id,
            claimed: false,
        })
    }

    /// Stops new admission and asks only worker-owned attempts to cancel.
    /// Foreground work retains its statement-owned cancellation lifecycle.
    pub fn begin_stopping(&self) {
        let mut state = lock(&self.inner);
        state.stopping = true;
        for entry in state.entries.values_mut() {
            if let Some(active) = &entry.active
                && let Some(source) = &active.cancellation
            {
                let _ = source.request(QueryCancellationReason::ServerShutdown);
            }
        }
    }

    #[cfg(test)]
    fn tracked_target_count(&self) -> usize {
        lock(&self.inner).entries.len()
    }
}

/// A queued request. Dropping an unclaimed ticket removes it from the FIFO
/// queue, preventing cancelled pre-dispatch work from stranding later work.
pub struct MvActivityTicket {
    inner: Weak<Mutex<GateState>>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    claimed: bool,
}

impl MvActivityTicket {
    /// Acquires only when this ticket is the head of its target's FIFO queue.
    pub fn try_acquire(&mut self) -> Result<Option<MvActivityLease>, MvActivityGateError> {
        if self.claimed {
            return Ok(None);
        }
        let Some(inner) = self.inner.upgrade() else {
            return Err(MvActivityGateError::Stopping);
        };
        let mut state = lock(&inner);
        if state.stopping {
            remove_waiter(&mut state, &self.target, self.ticket_id);
            return Err(MvActivityGateError::Stopping);
        }
        let Some(entry) = state.entries.get_mut(&self.target) else {
            return Ok(None);
        };
        if entry.active.is_some()
            || entry
                .waiters
                .front()
                .is_none_or(|waiter| waiter.ticket_id != self.ticket_id)
        {
            return Ok(None);
        }
        let waiter = entry
            .waiters
            .pop_front()
            .expect("front waiter exists after FIFO check");
        debug_assert_eq!(waiter.ticket_id, self.ticket_id);
        let cancellation = waiter
            .owner
            .is_worker_owned()
            .then(QueryCancellationSource::new);
        entry.active = Some(ActiveAttempt {
            ticket_id: self.ticket_id,
            cancellation: cancellation.clone(),
        });
        self.claimed = true;
        Ok(Some(MvActivityLease {
            inner: Arc::downgrade(&inner),
            target: self.target.clone(),
            ticket_id: self.ticket_id,
            cancellation: cancellation.map(|source| source.view()),
        }))
    }
}

impl Drop for MvActivityTicket {
    fn drop(&mut self) {
        if self.claimed {
            return;
        }
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        remove_waiter(&mut lock(&inner), &self.target, self.ticket_id);
    }
}

/// Exclusive ownership of one MV activity slot. Dropping it releases the slot.
pub struct MvActivityLease {
    inner: Weak<Mutex<GateState>>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    cancellation: Option<QueryCancellationView>,
}

impl MvActivityLease {
    pub fn cancellation(&self) -> Option<QueryCancellationView> {
        self.cancellation.clone()
    }
}

impl Drop for MvActivityLease {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = lock(&inner);
        let mut remove_entry = false;
        if let Some(entry) = state.entries.get_mut(&self.target) {
            if entry
                .active
                .as_ref()
                .is_some_and(|active| active.ticket_id == self.ticket_id)
            {
                entry.active = None;
            }
            remove_entry = entry.active.is_none() && entry.waiters.is_empty();
        }
        if remove_entry {
            state.entries.remove(&self.target);
        }
    }
}

#[derive(Default)]
struct GateState {
    stopping: bool,
    next_ticket_id: u64,
    entries: BTreeMap<CanonicalMvTarget, TargetEntry>,
}

#[derive(Default)]
struct TargetEntry {
    waiters: VecDeque<Waiter>,
    active: Option<ActiveAttempt>,
}

struct Waiter {
    ticket_id: u64,
    owner: MvActivityOwner,
}

struct ActiveAttempt {
    ticket_id: u64,
    cancellation: Option<QueryCancellationSource>,
}

fn remove_waiter(state: &mut GateState, target: &CanonicalMvTarget, ticket_id: u64) {
    let mut remove_entry = false;
    if let Some(entry) = state.entries.get_mut(target) {
        if let Some(position) = entry
            .waiters
            .iter()
            .position(|waiter| waiter.ticket_id == ticket_id)
        {
            entry.waiters.remove(position);
        }
        remove_entry = entry.active.is_none() && entry.waiters.is_empty();
    }
    if remove_entry {
        state.entries.remove(target);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str) -> CanonicalMvTarget {
        CanonicalMvTarget::from_parts(Some("iceberg"), "db", name)
    }

    #[test]
    fn shared_target_is_fifo_across_foreground_and_worker_owners() {
        let gate = MvActivityGate::new();
        let mut manual = gate
            .request(target("mv"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let mut worker = gate
            .request(target("mv"), MvActivityOwner::AutomaticMaintenance)
            .unwrap();
        let lease = manual.try_acquire().unwrap().unwrap();
        assert!(worker.try_acquire().unwrap().is_none());
        drop(lease);
        assert!(worker.try_acquire().unwrap().is_some());
    }

    #[test]
    fn shutdown_cancels_worker_but_not_manual_activity() {
        let gate = MvActivityGate::new();
        let mut worker = gate
            .request(target("worker"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        let worker_lease = worker.try_acquire().unwrap().unwrap();
        let worker_cancellation = worker_lease.cancellation().unwrap();
        let mut manual = gate
            .request(target("manual"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let manual_lease = manual.try_acquire().unwrap().unwrap();
        assert!(manual_lease.cancellation().is_none());
        gate.begin_stopping();
        assert_eq!(
            worker_cancellation.reason(),
            Some(QueryCancellationReason::ServerShutdown)
        );
    }

    #[test]
    fn cancelled_and_finished_activity_reaps_target() {
        let gate = MvActivityGate::new();
        let ticket = gate
            .request(target("cancelled"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert_eq!(gate.tracked_target_count(), 1);
        drop(ticket);
        assert_eq!(gate.tracked_target_count(), 0);

        let mut terminal = gate
            .request(target("terminal"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        let lease = terminal.try_acquire().unwrap().unwrap();
        assert_eq!(gate.tracked_target_count(), 1);
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[test]
    fn ddl_refresh_and_repartition_share_one_fifo_target_queue() {
        let gate = MvActivityGate::new();
        let mut create = gate.request(target("mv"), MvActivityOwner::Create).unwrap();
        let mut refresh = gate
            .request(target("mv"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let mut repartition = gate
            .request(target("mv"), MvActivityOwner::Repartition)
            .unwrap();
        let mut drop_ticket = gate.request(target("mv"), MvActivityOwner::Drop).unwrap();
        let create_lease = create.try_acquire().unwrap().unwrap();
        assert!(refresh.try_acquire().unwrap().is_none());
        assert!(repartition.try_acquire().unwrap().is_none());
        assert!(drop_ticket.try_acquire().unwrap().is_none());
        drop(create_lease);
        let refresh_lease = refresh.try_acquire().unwrap().unwrap();
        assert!(repartition.try_acquire().unwrap().is_none());
        drop(refresh_lease);
        let repartition_lease = repartition.try_acquire().unwrap().unwrap();
        assert!(drop_ticket.try_acquire().unwrap().is_none());
        drop(repartition_lease);
        assert!(drop_ticket.try_acquire().unwrap().is_some());
    }

    /// The process-local gate is a fairness and shutdown mechanism, not a
    /// durable MV ownership fence.
    #[test]
    fn separate_process_runtimes_do_not_arbitrate_publications() {
        let target = CanonicalMvTarget::from_parts(Some("ice"), "sales", "daily");
        let first = MvActivityGate::new();
        let mut first_ticket = first
            .request(target.clone(), MvActivityOwner::ManualRefresh)
            .unwrap();
        let first_lease = first_ticket.try_acquire().unwrap().unwrap();
        let mut same_process = first
            .request(target.clone(), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert!(same_process.try_acquire().unwrap().is_none());

        let second = MvActivityGate::new();
        let mut other_process = second
            .request(target, MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert!(other_process.try_acquire().unwrap().is_some());
        drop(first_lease);
    }
}
