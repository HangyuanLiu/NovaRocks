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

//! Post-commit cleanup handoff for `DROP TABLE ... PURGE`.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)
//!
//! Catalog visibility and object deletion are separate authorities, and this
//! module is the seam between them. It is deliberately weak:
//!
//! - **Nothing enters without proof.** A request is only constructible from a
//!   [`KnownCommitted`] witness, which only a committed catalog outcome hands
//!   out. A drop whose outcome is unknown therefore cannot enqueue anything —
//!   not by convention, by type.
//! - **Nothing leaves early, and this module does not decide when.** A request
//!   carries the dropped table's own lake timestamp as *evidence*. Eligibility
//!   is decided by the same caller-supplied `older_than_ms` cutoff the rest of
//!   this provider's collection already obeys, which the frontend owns through
//!   its durable age observation. Defining a second age policy here would be a
//!   second authority, and two authorities disagree.
//! - **The queue is an accelerator, not a record.** It is bounded and lives
//!   only as long as the control generation. Overflow, retirement, and process
//!   death all mean the same thing: the objects leak and independent garbage
//!   collection reclaims them later by re-proving they are dead. Leaking is the
//!   accepted cost; deleting without proof is not.
//!
//! What this module never does is delete by path prefix. Cleanup acts on the
//! exact object identity captured before the drop, because a prefix can be
//! shared — a second table created at the same location, or a location the
//! catalog reported but never owned — and a prefix delete cannot tell the
//! difference.

// The queue is filled by the drop path and drained by the collection pass. The
// drain side is not wired yet, so its accessors have no production caller.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::catalog::CatalogDropTableReceipt;
use crate::catalog::error::KnownCommitted;

/// Upper bound on queued cleanups per control generation.
///
/// Chosen to bound memory, not to bound correctness: passing it drops the
/// oldest request, which leaks its objects for independent collection instead
/// of blocking the statement that produced it.
pub(crate) const MAX_QUEUED_DROP_CLEANUPS: usize = 1024;

/// One table's worth of objects, eligible for deletion after a proven drop.
///
/// Construction requires the witness, so this type cannot exist for a drop
/// whose outcome was unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PostCommitCleanupRequest {
    /// Canonical `namespace.table` at drop time, for diagnostics only.
    pub(crate) target: Arc<str>,
    /// The exact table UUID observed before the drop. Cleanup refuses to act
    /// without it: an object set that cannot be attributed to this exact table
    /// might belong to another.
    pub(crate) table_uuid: Arc<str>,
    /// Root the dropped table occupied.
    pub(crate) table_location: Arc<str>,
    /// Metadata file the table was anchored at when it was dropped.
    pub(crate) metadata_location: Option<Arc<str>>,
    /// The dropped table's own last-updated timestamp, in lake time.
    ///
    /// Evidence, not policy. It comes from the table metadata rather than a
    /// local clock so that eligibility is decided against a fact every
    /// participant can observe, the same way owned-ref collection compares a
    /// snapshot's `timestamp_ms` against a caller-supplied cutoff.
    pub(crate) created_at_ms: i64,
}

impl PostCommitCleanupRequest {
    /// Build a cleanup request from a drop that is proven committed.
    ///
    /// Returns `None` when the pre-drop capture did not yield exact identity.
    /// That is a real outcome, not a defect: a table the catalog could not load
    /// before the drop has no attributable object set, and the correct response
    /// is to leak it for identity-aware collection rather than to guess.
    pub(crate) fn from_committed_drop(
        target: Arc<str>,
        receipt: &CatalogDropTableReceipt,
        _committed: &KnownCommitted,
    ) -> Option<Self> {
        let table_uuid = receipt.table_uuid.clone()?;
        let table_location = receipt.table_location.clone()?;
        // A non-positive timestamp is an unreadable age, and an unreadable age
        // cannot clear any cutoff. Refusing here keeps that fail-closed instead
        // of letting a zero sort below every threshold.
        if receipt.last_updated_ms <= 0 {
            return None;
        }
        Some(Self {
            target,
            table_uuid,
            table_location,
            metadata_location: receipt.metadata_location.clone(),
            created_at_ms: receipt.last_updated_ms,
        })
    }

    /// Whether this drop is old enough for the caller-supplied cutoff.
    pub(crate) fn is_eligible(&self, older_than_ms: i64) -> bool {
        self.created_at_ms > 0 && self.created_at_ms < older_than_ms
    }
}

/// Why a request did not stay in the queue.
///
/// Both variants mean the same thing operationally — the objects leak — and
/// both are recorded so the leak is visible rather than silent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupAdmission {
    Queued,
    /// The queue was full, so the oldest request was evicted to make room.
    EvictedOldest,
    /// The generation is retired; a newer generation owns this catalog now and
    /// this one must not act on its warehouse.
    Retired,
}

/// Generation-local queue of proven-committed drops awaiting their age window.
///
/// Not durable and not authoritative. Losing it loses only the acceleration.
#[derive(Debug, Default)]
pub(crate) struct DropCleanupQueue {
    inner: Mutex<DropCleanupQueueState>,
}

#[derive(Debug, Default)]
struct DropCleanupQueueState {
    pending: VecDeque<PostCommitCleanupRequest>,
    retired: bool,
    /// Count of requests whose objects were left for independent collection.
    leaked: u64,
}

impl DropCleanupQueue {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Admit a proven-committed drop.
    pub(crate) fn enqueue(&self, request: PostCommitCleanupRequest) -> CleanupAdmission {
        let mut state = match self.inner.lock() {
            Ok(state) => state,
            // A poisoned queue is an accelerator that can no longer be trusted
            // to be complete. Refusing is safe; the objects leak.
            Err(_) => return CleanupAdmission::Retired,
        };
        if state.retired {
            state.leaked = state.leaked.saturating_add(1);
            return CleanupAdmission::Retired;
        }
        let mut admission = CleanupAdmission::Queued;
        if state.pending.len() >= MAX_QUEUED_DROP_CLEANUPS {
            state.pending.pop_front();
            state.leaked = state.leaked.saturating_add(1);
            admission = CleanupAdmission::EvictedOldest;
        }
        state.pending.push_back(request);
        admission
    }

    /// Take every request older than the caller-supplied cutoff.
    ///
    /// The cutoff is the frontend's durable age observation. This module never
    /// computes one.
    pub(crate) fn take_eligible(&self, older_than_ms: i64) -> Vec<PostCommitCleanupRequest> {
        let Ok(mut state) = self.inner.lock() else {
            return Vec::new();
        };
        if state.retired {
            return Vec::new();
        }
        let mut due = Vec::new();
        let mut remaining = VecDeque::with_capacity(state.pending.len());
        while let Some(request) = state.pending.pop_front() {
            if request.is_eligible(older_than_ms) {
                due.push(request);
            } else {
                remaining.push_back(request);
            }
        }
        state.pending = remaining;
        due
    }

    /// Retire the queue when its control generation is replaced.
    ///
    /// Everything still queued leaks. A retired generation may hold stale
    /// credentials or point at a warehouse the new generation no longer owns,
    /// and deleting from it would be worse than leaking.
    pub(crate) fn retire(&self) -> u64 {
        let Ok(mut state) = self.inner.lock() else {
            return 0;
        };
        state.retired = true;
        let abandoned = state.pending.len() as u64;
        state.pending.clear();
        state.leaked = state.leaked.saturating_add(abandoned);
        abandoned
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.inner.lock().map(|s| s.pending.len()).unwrap_or(0)
    }

    pub(crate) fn leaked_count(&self) -> u64 {
        self.inner.lock().map(|s| s.leaked).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogOutcome;
    use novarocks_spi::connector::{ConnectorMutationFailureKind, ExternalMutationEffect};

    /// A fixed lake timestamp; the provider never reads a local clock here.
    const DROPPED_AT_MS: i64 = 1_700_000_000_000;

    fn receipt() -> CatalogDropTableReceipt {
        CatalogDropTableReceipt {
            table_uuid: Some(Arc::from("uuid-1")),
            table_location: Some(Arc::from("s3://w/db/t")),
            metadata_location: Some(Arc::from("s3://w/db/t/metadata/v3.metadata.json")),
            last_updated_ms: DROPPED_AT_MS,
        }
    }

    fn committed_request() -> PostCommitCleanupRequest {
        let outcome = CatalogOutcome::committed(receipt(), ExternalMutationEffect::Applied);
        let (receipt, _effect, witness) = outcome.into_known_committed().expect("committed");
        PostCommitCleanupRequest::from_committed_drop(Arc::from("db.t"), &receipt, &witness)
            .expect("exact identity and a readable age were captured")
    }

    /// The rule the whole module exists for: an unknown drop cannot even build
    /// the value that deletion consumes.
    #[test]
    fn an_unknown_drop_cannot_produce_a_cleanup_request() {
        let unknown: CatalogOutcome<CatalogDropTableReceipt> = CatalogOutcome::unknown(
            "connection reset",
            crate::catalog::error::CatalogCommitEvidence::for_target("db.t"),
        );
        assert!(unknown.into_known_committed().is_none());

        let uncommitted: CatalogOutcome<CatalogDropTableReceipt> =
            CatalogOutcome::uncommitted(ConnectorMutationFailureKind::NotFound, "absent");
        assert!(uncommitted.into_known_committed().is_none());

        let unsupported: CatalogOutcome<CatalogDropTableReceipt> =
            CatalogOutcome::unsupported("no");
        assert!(unsupported.into_known_committed().is_none());
    }

    #[test]
    fn a_drop_without_exact_identity_leaks_instead_of_guessing() {
        let outcome = CatalogOutcome::committed(
            CatalogDropTableReceipt {
                table_uuid: None,
                table_location: None,
                metadata_location: None,
                last_updated_ms: DROPPED_AT_MS,
            },
            ExternalMutationEffect::Applied,
        );
        let (receipt, _effect, witness) = outcome.into_known_committed().expect("committed");
        assert!(
            PostCommitCleanupRequest::from_committed_drop(Arc::from("db.t"), &receipt, &witness)
                .is_none(),
            "no exact identity means nothing is attributable, so nothing is eligible"
        );
    }

    #[test]
    fn an_unreadable_age_fails_closed_rather_than_sorting_below_every_cutoff() {
        let outcome = CatalogOutcome::committed(
            CatalogDropTableReceipt {
                last_updated_ms: 0,
                ..receipt()
            },
            ExternalMutationEffect::Applied,
        );
        let (receipt, _effect, witness) = outcome.into_known_committed().expect("committed");
        assert!(
            PostCommitCleanupRequest::from_committed_drop(Arc::from("db.t"), &receipt, &witness)
                .is_none()
        );
    }

    /// Eligibility is the caller's cutoff, never a policy this module owns.
    #[test]
    fn nothing_is_eligible_until_the_callers_cutoff_passes_it() {
        let queue = DropCleanupQueue::new();
        assert_eq!(queue.enqueue(committed_request()), CleanupAdmission::Queued);

        assert!(
            queue.take_eligible(DROPPED_AT_MS).is_empty(),
            "a cutoff at the drop's own timestamp does not clear it"
        );
        assert_eq!(queue.pending_len(), 1);

        let eligible = queue.take_eligible(DROPPED_AT_MS + 1);
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].table_uuid.as_ref(), "uuid-1");
        assert_eq!(queue.pending_len(), 0);
    }

    #[test]
    fn overflow_leaks_the_oldest_rather_than_blocking_or_deleting_early() {
        let queue = DropCleanupQueue::new();
        for _ in 0..MAX_QUEUED_DROP_CLEANUPS {
            assert_eq!(queue.enqueue(committed_request()), CleanupAdmission::Queued);
        }
        assert_eq!(
            queue.enqueue(committed_request()),
            CleanupAdmission::EvictedOldest
        );
        assert_eq!(queue.pending_len(), MAX_QUEUED_DROP_CLEANUPS);
        assert_eq!(queue.leaked_count(), 1);
    }

    #[test]
    fn a_retired_generation_leaks_everything_and_admits_nothing() {
        let queue = DropCleanupQueue::new();
        queue.enqueue(committed_request());
        queue.enqueue(committed_request());

        assert_eq!(queue.retire(), 2);
        assert_eq!(queue.leaked_count(), 2);
        assert_eq!(
            queue.enqueue(committed_request()),
            CleanupAdmission::Retired
        );
        assert!(
            queue.take_eligible(DROPPED_AT_MS + 1).is_empty(),
            "a retired generation must not delete from a warehouse it no longer owns"
        );
    }
}
