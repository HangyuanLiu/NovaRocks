// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Custody of the same active Native Box under its original actor registration.
//! No new WorkOwner, governance obligation, actor, admission, or dispatch policy.

use super::{LogicalExecutionRuntimeRegistryHandle, StatementNativeResidualRegistry};
use crate::api::{
    ActiveNativeAttemptOwner, NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
    NativeAttemptConvergence, QueryExecutionError, QueryExecutionErrorKind,
    StatementNativeHealthFailure,
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use novarocks_workload_control::CancellationView;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NativeActiveCustodyKey {
    pub(crate) initial: QueryExecutionId,
    pub(crate) generation: u64,
    pub(crate) execution: QueryExecutionId,
}
impl NativeActiveCustodyKey {
    fn registration_lower_bound(initial: QueryExecutionId, generation: u64) -> Self {
        // QueryExecutionId orders (i64 query high, i64 query low, AttemptId).
        // Both signed-min halves are valid; AttemptId forbids only zero. This
        // is the lowest valid execution value, used solely as a search bound.
        let execution = QueryExecutionId::new(
            QueryId::new(i64::MIN, i64::MIN),
            AttemptId::new(1).expect("one is the lowest nonzero attempt"),
        )
        .expect("signed-min query halves are nonzero");
        Self {
            initial,
            generation,
            execution,
        }
    }
}
struct RetainedNativeActiveOwner {
    _owner: Box<dyn ActiveNativeAttemptOwner>,
    failure: StatementNativeHealthFailure,
}
struct ActiveSlot {
    contexts: BTreeSet<QueryContextRef>,
    owner: Mutex<Option<RetainedNativeActiveOwner>>,
}
/// This typed storage is a member of the ORIGINAL RuntimeRegistry. It cannot
/// create a query, conclusion, WorkOwner, obligation, or scheduling authority.
#[derive(Clone)]
pub(crate) struct NativeActiveCustodyRegistry {
    slots: Arc<Mutex<BTreeMap<NativeActiveCustodyKey, Arc<ActiveSlot>>>>,
    health: StatementNativeResidualRegistry,
    progress: Arc<Notify>,
}
impl NativeActiveCustodyRegistry {
    pub(crate) fn new(health: StatementNativeResidualRegistry, progress: Arc<Notify>) -> Self {
        Self {
            slots: Arc::new(Mutex::new(BTreeMap::new())),
            health,
            progress,
        }
    }
    pub(crate) fn reserve(
        &self,
        key: NativeActiveCustodyKey,
        contexts: &[QueryContextRef],
        registry: LogicalExecutionRuntimeRegistryHandle,
    ) -> Option<NativeActiveCustodyReservation> {
        let exact = contexts.iter().copied().collect::<BTreeSet<_>>();
        if exact.is_empty()
            || exact.len() != contexts.len()
            || exact
                .iter()
                .any(|c| c.query_execution_id() != key.execution)
        {
            return None;
        }
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if slots.contains_key(&key) {
            return None;
        }
        let slot = Arc::new(ActiveSlot {
            contexts: exact,
            owner: Mutex::new(None),
        });
        slots.insert(key, Arc::clone(&slot));
        Some(NativeActiveCustodyReservation {
            custody: Some(NativeActiveCustodyHandle {
                storage: self.clone(),
                registry,
                key,
                slot,
            }),
        })
    }
    pub(crate) fn contains_registration(&self, initial: QueryExecutionId, generation: u64) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .next()
            .is_some_and(|(key, _)| key.initial == initial && key.generation == generation)
    }
    pub(crate) fn contexts(&self, key: NativeActiveCustodyKey) -> Option<Vec<QueryContextRef>> {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .map(|s| s.contexts.iter().copied().collect())
    }
    pub(crate) fn len(&self) -> usize {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub(crate) fn abandon_for_process_exit(&self) {
        let slots = std::mem::take(&mut *self.slots.lock().unwrap_or_else(|e| e.into_inner()));
        drop(slots);
    }
    fn remove(&self, key: NativeActiveCustodyKey) {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
        self.progress.notify_waiters();
    }
    pub(crate) fn registration_health(
        &self,
        initial: QueryExecutionId,
        generation: u64,
    ) -> Option<QueryExecutionError> {
        let slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .take_while(|(key, _)| key.initial == initial && key.generation == generation)
            .find_map(|(_, slot)| {
                slot.owner
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|held| {
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            held.failure.detail(),
                        )
                    })
            })
    }
}
struct NativeActiveCustodyHandle {
    storage: NativeActiveCustodyRegistry,
    registry: LogicalExecutionRuntimeRegistryHandle,
    key: NativeActiveCustodyKey,
    slot: Arc<ActiveSlot>,
}
impl NativeActiveCustodyHandle {
    fn retain(
        &self,
        owner: Box<dyn ActiveNativeAttemptOwner>,
        failure: StatementNativeHealthFailure,
    ) {
        // The same pre-reserved slot is held directly even during explicit
        // process-exit teardown. Normal retirement cannot remove its map entry.
        let mut held = self.slot.owner.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            held.is_none(),
            "original active owner returns to custody once"
        );
        *held = Some(RetainedNativeActiveOwner {
            _owner: owner,
            failure: failure.clone(),
        });
        drop(held);
        self.storage.health.record_unproven_exit(failure);
    }
}
/// Reserve BEFORE activate. On an activation failure, dropping this unbound
/// reservation just removes its empty custody record, not any live Native owner.
pub(crate) struct NativeActiveCustodyReservation {
    custody: Option<NativeActiveCustodyHandle>,
}
impl NativeActiveCustodyReservation {
    pub(crate) fn bind(
        mut self,
        original: Box<dyn ActiveNativeAttemptOwner>,
    ) -> SupervisedActiveNativeOwner {
        SupervisedActiveNativeOwner {
            owner: Some(original),
            custody: self
                .custody
                .take()
                .expect("original reservation binds once"),
            terminal_cut: None,
            remote_deadline: None,
            closed: false,
        }
    }
}
impl Drop for NativeActiveCustodyReservation {
    fn drop(&mut self) {
        if let Some(custody) = self.custody.take() {
            custody.storage.remove(custody.key);
        }
    }
}
/// One move-only wrapper around the ORIGINAL Box. Every run/converge operation
/// borrows it. An unpolled residual task or any enclosing unwind returns this
/// same Box to its pre-reserved original registry record.
pub(crate) struct SupervisedActiveNativeOwner {
    owner: Option<Box<dyn ActiveNativeAttemptOwner>>,
    custody: NativeActiveCustodyHandle,
    terminal_cut: Option<Instant>,
    remote_deadline: Option<Instant>,
    closed: bool,
}
impl fmt::Debug for SupervisedActiveNativeOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisedActiveNativeOwner")
            .field("key", &self.custody.key)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
impl SupervisedActiveNativeOwner {
    pub(crate) fn as_mut(&mut self) -> &mut dyn ActiveNativeAttemptOwner {
        self.owner
            .as_deref_mut()
            .expect("same active owner has not entered unresolved custody")
    }
    pub(crate) fn begin_terminal_cut(&mut self, cut: Instant) {
        self.terminal_cut.get_or_insert(cut);
    }
    pub(crate) fn request(
        &mut self,
        cancellation: CancellationView,
        budget: Duration,
    ) -> NativeActiveConvergenceRequest {
        // This fallback is for terminal actor refusals before run was entered.
        // Run/row terminals freeze their cut immediately, before replacement
        // preparation or client/cleanup waits can lend a new observation budget.
        let cut = *self.terminal_cut.get_or_insert_with(Instant::now);
        let deadline = *self.remote_deadline.get_or_insert_with(|| cut + budget);
        NativeActiveConvergenceRequest::new(cancellation, deadline)
    }
    pub(crate) fn shorten_remote_deadline(&mut self, deadline: Instant) {
        self.remote_deadline = Some(
            self.remote_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }
    pub(crate) fn retain_unresolved(
        &mut self,
        failure: StatementNativeHealthFailure,
    ) -> QueryExecutionError {
        let error = QueryExecutionError::new(QueryExecutionErrorKind::Failed, failure.detail());
        if let Some(owner) = self.owner.take() {
            self.custody.retain(owner, failure);
        }
        error
    }
    pub(crate) async fn record_and_close(
        &mut self,
        closure: NativeActiveConvergenceClosure,
    ) -> Result<(), QueryExecutionError> {
        let expected = self.custody.slot.contexts.clone();
        let (observed, count) = match &closure {
            NativeActiveConvergenceClosure::Evidence(
                NativeAttemptConvergence::AllWorkersStoppedAndContextsFenced,
            ) => (expected.clone(), expected.len()),
            NativeActiveConvergenceClosure::Evidence(NativeAttemptConvergence::Contexts(facts)) => {
                (facts.iter().map(|f| f.context()).collect(), facts.len())
            }
            NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts } => {
                if self.remote_deadline.is_none_or(|d| Instant::now() < d) {
                    return Err(self.retain_unresolved(
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "active Native remote tracking ended before its original deadline",
                            Arc::new(ActiveCustodyInvariant),
                        ),
                    ));
                }
                (
                    exact_contexts.iter().copied().collect(),
                    exact_contexts.len(),
                )
            }
        };
        if expected != observed || observed.len() != count {
            return Err(self.retain_unresolved(
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native closure differs from its original exact context set",
                    Arc::new(ActiveCustodyInvariant),
                ),
            ));
        }
        // Only the original registry/actor receives these closure facts. Local
        // custody stays held until every original observation was accepted.
        if let Err(error) = self
            .custody
            .registry
            .record_native_custody_closure(self.custody.key, &closure)
            .await
        {
            return Err(self.retain_unresolved(
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native closure could not settle its original registry",
                    Arc::new(error),
                ),
            ));
        }
        self.custody.storage.remove(self.custody.key);
        self.closed = true;
        Ok(())
    }
}
#[derive(Debug)]
struct ActiveCustodyInvariant;
impl fmt::Display for ActiveCustodyInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original active Native custody invariant failed")
    }
}
impl std::error::Error for ActiveCustodyInvariant {}
impl Drop for SupervisedActiveNativeOwner {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Some(owner) = self.owner.take() {
            self.custody.retain(
                owner,
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native owner left supervision before actual local closure",
                    Arc::new(ActiveCustodyInvariant),
                ),
            );
        }
    }
}

struct NativeConvergencePanic {
    _payload: Mutex<Box<dyn std::any::Any + Send>>,
}
impl fmt::Debug for NativeConvergencePanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeConvergencePanic")
    }
}
impl fmt::Display for NativeConvergencePanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native convergence future panicked")
    }
}
impl std::error::Error for NativeConvergencePanic {}

pub(crate) async fn catch_native_convergence_panic<F: std::future::Future>(
    future: F,
) -> Result<F::Output, StatementNativeHealthFailure> {
    let mut future = Box::pin(future);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => {
                std::task::Poll::Ready(Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native convergence panicked before actual local closure",
                    Arc::new(NativeConvergencePanic {
                        _payload: Mutex::new(payload),
                    }),
                )))
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{BackendProcessId, FrontendProcessId};

    fn execution(high: i64, low: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(high, low), AttemptId::new(attempt).unwrap()).unwrap()
    }

    #[derive(Debug)]
    struct MetadataOnlyOwner;
    impl ActiveNativeAttemptOwner for MetadataOnlyOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a crate::coordination::NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> crate::api::NativeAttemptRunFuture<'a> {
            panic!("prefix metadata fixture must not dispatch work")
        }
        fn converge<'a>(
            &'a mut self,
            _request: NativeActiveConvergenceRequest,
        ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
            panic!("prefix metadata fixture must not converge work")
        }
    }
    #[derive(Debug)]
    struct MetadataHealthCause;
    impl fmt::Display for MetadataHealthCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original prefix metadata cause")
        }
    }
    impl std::error::Error for MetadataHealthCause {}

    #[test]
    fn health_search_stays_in_exact_prefix_and_keeps_first_held_source_order() {
        let registry = NativeActiveCustodyRegistry::new(
            StatementNativeResidualRegistry::new(316),
            Arc::new(Notify::new()),
        );
        let initial = execution(91, 71, 3);
        let key = |generation, attempt| NativeActiveCustodyKey {
            initial,
            generation,
            execution: execution(91, 71, attempt),
        };
        let before = key(6, 3);
        let unheld = key(7, 3);
        let first_held = key(7, 4);
        let later_held = key(7, 5);
        let after = key(8, 3);
        let cause: Arc<dyn std::error::Error + Send + Sync> = Arc::new(MetadataHealthCause);
        {
            let mut slots = registry.slots.lock().unwrap();
            for (key, detail) in [
                (before, Some("foreign earlier prefix health")),
                (unheld, None),
                (first_held, Some("first exact prefix health")),
                (later_held, Some("later exact prefix health")),
                (after, Some("foreign later prefix health")),
            ] {
                let held = detail.map(|detail| RetainedNativeActiveOwner {
                    _owner: Box::new(MetadataOnlyOwner),
                    failure: StatementNativeHealthFailure::actual_exit_unproven(
                        detail,
                        Arc::clone(&cause),
                    ),
                });
                slots.insert(
                    key,
                    Arc::new(ActiveSlot {
                        contexts: BTreeSet::from([QueryContextRef::new(
                            key.execution,
                            FrontendProcessId::new_v7(),
                            BackendProcessId::new_v7(),
                        )]),
                        owner: Mutex::new(held),
                    }),
                );
            }
        }
        assert!(
            registry.registration_health(initial, 5).is_none(),
            "the next prefix cannot supply missing-prefix health"
        );
        assert!(
            registry
                .registration_health(execution(91, 71, 2), 7)
                .is_none(),
            "the same QueryId with another initial attempt is a different registration"
        );
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "first exact prefix health"
        );
        let retained = registry
            .slots
            .lock()
            .unwrap()
            .get(&first_held)
            .cloned()
            .unwrap();
        let retained_cause = retained
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .failure
            .clone()
            .into_parts()
            .1;
        assert!(
            Arc::ptr_eq(&retained_cause, &cause),
            "read-only health lookup retains the original typed source Arc"
        );
        registry.remove(first_held);
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "later exact prefix health"
        );
        registry.remove(later_held);
        assert!(
            registry.contains_registration(initial, 7),
            "the same unheld reservation remains"
        );
        assert!(
            registry.registration_health(initial, 7).is_none(),
            "an unheld own slot cannot leak the next prefix's health"
        );
    }

    #[test]
    fn exact_registration_prefix_membership_covers_adjacent_and_extreme_keys() {
        let registry = NativeActiveCustodyRegistry::new(
            StatementNativeResidualRegistry::new(315),
            Arc::new(Notify::new()),
        );
        let low = execution(i64::MIN, i64::MIN, 1);
        let initial = execution(91, 71, 3);
        let same_query_other_initial = execution(91, 71, 4);
        let high = execution(i64::MAX, i64::MAX, u64::MAX);
        assert!(!registry.contains_registration(initial, 7));
        let keys = [
            NativeActiveCustodyKey {
                initial: low,
                generation: 1,
                execution: low,
            },
            NativeActiveCustodyKey {
                initial,
                generation: 7,
                execution: execution(91, 71, 3),
            },
            NativeActiveCustodyKey {
                initial,
                generation: 7,
                execution: execution(91, 71, u64::MAX),
            },
            NativeActiveCustodyKey {
                initial,
                generation: 9,
                execution: execution(91, 71, 4),
            },
            NativeActiveCustodyKey {
                initial: same_query_other_initial,
                generation: 7,
                execution: same_query_other_initial,
            },
            NativeActiveCustodyKey {
                initial: high,
                generation: u64::MAX,
                execution: high,
            },
        ];
        // These are the same private empty pre-reservation slots: valid exact
        // contexts, no live owner, no WorkOwner/obligation, no exit evidence.
        {
            let mut slots = registry.slots.lock().unwrap();
            for key in keys {
                slots.insert(
                    key,
                    Arc::new(ActiveSlot {
                        contexts: BTreeSet::from([QueryContextRef::new(
                            key.execution,
                            FrontendProcessId::new_v7(),
                            BackendProcessId::new_v7(),
                        )]),
                        owner: Mutex::new(None),
                    }),
                );
            }
        }
        let probes = [
            (low, 0),
            (low, 1),
            (low, u64::MAX),
            (execution(91, 71, 2), 7),
            (initial, 0),
            (initial, 6),
            (initial, 7),
            (initial, 8),
            (initial, 9),
            (initial, 10),
            (same_query_other_initial, 7),
            (same_query_other_initial, 8),
            (execution(91, 72, 1), 7),
            (high, u64::MAX - 1),
            (high, u64::MAX),
        ];
        for (initial, generation) in probes {
            let expected = keys
                .iter()
                .any(|key| key.initial == initial && key.generation == generation);
            assert_eq!(
                registry.contains_registration(initial, generation),
                expected,
                "membership must retain the exact initial execution and generation prefix"
            );
        }
        let mut slots = registry.slots.lock().unwrap();
        slots.remove(&keys[1]);
        drop(slots);
        assert!(
            registry.contains_registration(initial, 7),
            "another exact attempt still holds original custody"
        );
        registry.remove(keys[2]);
        assert!(
            !registry.contains_registration(initial, 7),
            "an adjacent generation cannot keep the removed prefix live"
        );
    }
}
