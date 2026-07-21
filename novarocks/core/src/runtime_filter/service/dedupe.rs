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

//! Query-scoped, per-channel ingress dedupe + `(query, epoch)` tombstone.
//!
//! RFD-4/M3 "bounded at-least-once transport" retries wire envelopes, so both
//! ingress directions must idempotently absorb a duplicate/out-of-order envelope
//! instead of re-applying or re-delivering it. This component is the single home
//! for that state; the two ingress paths (`inbound.rs`, `consumer_ingress.rs`)
//! consult it, and the service teardown (`mod.rs`) populates the tombstone.
//!
//! It holds three logically-distinct structures, all keyed per channel so a later
//! milestone (Task 4) can bound each set naturally:
//!
//! 1. **Transport-identity dedupe** — one index per direction, keyed on the wire
//!    route identity *including the transport sequence*. It absorbs a re-arrival of
//!    the *same wire message* (an at-least-once retry).
//!    - Producer contributions carry a content guard: the Core downstream is
//!      content-aware (it distinguishes a byte-identical retry from a conflicting
//!      replay). A same-identity arrival whose content differs is therefore **not**
//!      a valid retry — it must flow to the Core so its `ConflictingReplay`
//!      detection still fires — so the producer gate short-circuits only when the
//!      recorded content digest matches.
//!    - Consumer deliveries have no downstream content-conflict check, so their
//!      gate is identity-only: any re-arrival of a delivery identity is absorbed.
//! 2. **Logical delivery idempotency** (consumer only) — the stable
//!    `(route_edge, version)` identity already delivered into a subscription,
//!    absorbed from the former `RuntimeFilterService::delivered_versions` (M2C
//!    spec §7.7). This catches "the same logical version re-delivered via a
//!    *distinct* transport sequence", which the transport-identity index cannot
//!    see. Both indices must hold on the consumer side.
//! 3. **`(query, epoch)` tombstone** — the set of `(query_id, deployment_epoch)`
//!    pairs retired by cancel/completion, plus a stale-epoch check. A late envelope
//!    for a retired epoch is rejected without rebuilding context (M2B3 lookup-only);
//!    an envelope older than a retired epoch is rejected as stale.
//!
//! Task 4 adds the actual per-channel count ceilings and the resource-limit
//! rejection; this component deliberately stops at the structure and semantics.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
};
use crate::runtime_filter::port::transport::{ContributionRouteIdentity, DeliveryRouteIdentity};

/// Per-channel key for a producer contribution transport identity. It is exactly
/// the `ContributionRouteIdentity` (binding + finst + partition + sequence).
type ContributionKey = (BindingId, UniqueId, PartitionId, ProducerSequence);

/// Per-channel key for a consumer delivery transport identity. It is exactly the
/// `DeliveryRouteIdentity` (route edge + sequence).
type DeliveryKey = (RouteEdgeId, ProducerSequence);

/// Result of admitting a producer contribution transport identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContributionAdmission {
    /// First arrival of this identity: proceed to the Core.
    Fresh,
    /// A byte-identical at-least-once retry: short-circuit to `Duplicate` without
    /// touching the Core.
    DuplicateRetry,
    /// The identity was seen before but with different content: this is not a valid
    /// retry. Proceed to the Core, which rejects it as a `ConflictingReplay`.
    Conflict,
}

/// Result of admitting a consumer delivery transport / logical identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeliveryAdmission {
    /// First arrival of this identity: proceed to deliver.
    Fresh,
    /// Already recorded: answer `Duplicate` and do not re-deliver.
    Duplicate,
}

/// Verdict from consulting the `(query, epoch)` tombstone for an inbound envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TombstoneVerdict {
    /// Not retired and not older than a retired epoch: dispatch may proceed to the
    /// normal admission/authorization pipeline.
    Live,
    /// The envelope's `(query, epoch)` is retired (its deployment was cancelled or
    /// completed): reject without rebuilding context.
    Retired,
    /// The envelope's epoch is older than a retired epoch for this query: reject as
    /// a stale epoch.
    StaleEpoch,
}

pub(super) struct IngressDedupe {
    /// The query this dedupe is scoped to; the tombstone is keyed
    /// `(query_id, deployment_epoch)`, so it is recorded and consulted against this
    /// identity.
    query_id: UniqueId,
    state: Mutex<DedupeState>,
}

#[derive(Default)]
struct DedupeState {
    /// (1) producer transport-identity index, per channel, each identity carrying
    /// the content digest of its first arrival for the retry-vs-conflict guard.
    contributions: BTreeMap<ChannelId, BTreeMap<ContributionKey, [u8; 32]>>,
    /// (1) consumer transport-identity index, per channel.
    deliveries: BTreeMap<ChannelId, BTreeSet<DeliveryKey>>,
    /// (2) absorbed logical delivery idempotency, per channel.
    delivered_versions: BTreeMap<ChannelId, BTreeSet<(RouteEdgeId, LogicalVersion)>>,
    /// (3) `(query, epoch)` tombstone set.
    retired: BTreeSet<(UniqueId, DeploymentEpoch)>,
}

impl IngressDedupe {
    pub(super) fn new(query_id: UniqueId) -> Self {
        Self {
            query_id,
            state: Mutex::new(DedupeState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DedupeState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Producer transport-identity gate. Records the identity on first arrival with
    /// its content digest; a repeat with the same digest is a genuine retry
    /// (`DuplicateRetry`), a repeat with a different digest is a `Conflict` that the
    /// caller must forward to the Core (never silently absorbed).
    pub(super) fn admit_contribution(
        &self,
        channel_id: ChannelId,
        route: &ContributionRouteIdentity,
        content_digest: [u8; 32],
    ) -> ContributionAdmission {
        let key = (
            route.producer_binding_id(),
            route.fragment_instance_id(),
            route.partition_id(),
            route.sequence(),
        );
        let mut state = self.lock();
        match state
            .contributions
            .entry(channel_id)
            .or_default()
            .entry(key)
        {
            Entry::Vacant(slot) => {
                slot.insert(content_digest);
                ContributionAdmission::Fresh
            }
            Entry::Occupied(slot) => {
                if *slot.get() == content_digest {
                    ContributionAdmission::DuplicateRetry
                } else {
                    ContributionAdmission::Conflict
                }
            }
        }
    }

    /// Consumer transport-identity gate: absorbs a re-arrival of the same delivery
    /// identity (route edge + transport sequence) regardless of its content.
    pub(super) fn admit_delivery(
        &self,
        channel_id: ChannelId,
        route: &DeliveryRouteIdentity,
    ) -> DeliveryAdmission {
        let key = (route.route_edge_id(), route.sequence());
        let mut state = self.lock();
        if state.deliveries.entry(channel_id).or_default().insert(key) {
            DeliveryAdmission::Fresh
        } else {
            DeliveryAdmission::Duplicate
        }
    }

    /// Consumer logical idempotency (absorbed from `delivered_versions`): the stable
    /// `(route_edge, version)` identity delivered into a subscription. Distinct from
    /// the transport gate — it catches the same logical version re-delivered via a
    /// distinct transport sequence.
    pub(super) fn admit_delivered_version(
        &self,
        channel_id: ChannelId,
        route_edge_id: RouteEdgeId,
        version: LogicalVersion,
    ) -> DeliveryAdmission {
        let mut state = self.lock();
        if state
            .delivered_versions
            .entry(channel_id)
            .or_default()
            .insert((route_edge_id, version))
        {
            DeliveryAdmission::Fresh
        } else {
            DeliveryAdmission::Duplicate
        }
    }

    /// Tombstone this service's `(query, epoch)` at cancel/completion so a late or
    /// duplicate envelope arriving after teardown is rejected without rebuilding
    /// context.
    pub(super) fn retire_epoch(&self, epoch: DeploymentEpoch) {
        self.lock().retired.insert((self.query_id, epoch));
    }

    /// Consult the tombstone for an inbound envelope. Never rebuilds or revives any
    /// query state — a retired or stale envelope is rejected outright.
    pub(super) fn tombstone_verdict(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
    ) -> TombstoneVerdict {
        let state = self.lock();
        if state.retired.contains(&(query_id, epoch)) {
            return TombstoneVerdict::Retired;
        }
        // The highest epoch retired for this query bounds staleness: an envelope
        // older than a retired epoch belongs to a superseded deployment generation.
        if let Some((_, max_retired)) = state
            .retired
            .range(
                (query_id, DeploymentEpoch::new(u64::MIN))
                    ..=(query_id, DeploymentEpoch::new(u64::MAX)),
            )
            .next_back()
        {
            if epoch < *max_retired {
                return TombstoneVerdict::StaleEpoch;
            }
        }
        TombstoneVerdict::Live
    }
}

#[cfg(test)]
mod tests {
    use super::{ContributionAdmission, DeliveryAdmission, IngressDedupe, TombstoneVerdict};
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, DeliveryRouteIdentity,
    };

    const QID: UniqueId = UniqueId { hi: 5, lo: 6 };

    fn dedupe() -> IngressDedupe {
        IngressDedupe::new(QID)
    }

    fn contribution(sequence: u64) -> ContributionRouteIdentity {
        ContributionRouteIdentity::try_new(
            BindingId::new(1),
            UniqueId { hi: 1, lo: 2 },
            PartitionId::new(0),
            ProducerSequence::new(sequence),
        )
        .unwrap()
    }

    fn delivery(sequence: u64) -> DeliveryRouteIdentity {
        DeliveryRouteIdentity::try_new(RouteEdgeId::new(40), ProducerSequence::new(sequence))
            .unwrap()
    }

    #[test]
    fn ingress_dedupe_component_contribution_retry_vs_conflict() {
        let dedupe = dedupe();
        let channel = ChannelId::new(1);
        let route = contribution(0);
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
        // Same identity + same digest = a genuine retry.
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [1; 32]),
            ContributionAdmission::DuplicateRetry,
        );
        // Same identity + different digest = a conflict that must reach the Core.
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [2; 32]),
            ContributionAdmission::Conflict,
        );
    }

    #[test]
    fn ingress_dedupe_component_contribution_is_scoped_per_channel() {
        let dedupe = dedupe();
        let route = contribution(0);
        assert_eq!(
            dedupe.admit_contribution(ChannelId::new(1), &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
        // The same identity on a different channel is independent.
        assert_eq!(
            dedupe.admit_contribution(ChannelId::new(2), &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
    }

    #[test]
    fn ingress_dedupe_component_delivery_transport_and_logical_are_independent() {
        let dedupe = dedupe();
        let channel = ChannelId::new(1);
        let edge = RouteEdgeId::new(40);
        let version = LogicalVersion::new(5);

        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Fresh,
        );
        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version),
            DeliveryAdmission::Fresh,
        );

        // A distinct transport sequence is fresh at the transport gate ...
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(2)),
            DeliveryAdmission::Fresh,
        );
        // ... but the same logical version is a duplicate at the logical gate.
        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version),
            DeliveryAdmission::Duplicate,
        );
        // An exact transport retry is a duplicate at the transport gate.
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Duplicate,
        );
    }

    #[test]
    fn ingress_dedupe_component_tombstone_retired_and_stale() {
        let dedupe = dedupe();
        assert_eq!(
            dedupe.tombstone_verdict(QID, DeploymentEpoch::new(9)),
            TombstoneVerdict::Live,
        );
        dedupe.retire_epoch(DeploymentEpoch::new(9));
        assert_eq!(
            dedupe.tombstone_verdict(QID, DeploymentEpoch::new(9)),
            TombstoneVerdict::Retired,
        );
        assert_eq!(
            dedupe.tombstone_verdict(QID, DeploymentEpoch::new(8)),
            TombstoneVerdict::StaleEpoch,
        );
        // A newer epoch is neither retired nor stale (a still-live generation).
        assert_eq!(
            dedupe.tombstone_verdict(QID, DeploymentEpoch::new(10)),
            TombstoneVerdict::Live,
        );
    }

    #[test]
    fn ingress_dedupe_component_tombstone_is_query_scoped() {
        let dedupe = dedupe();
        dedupe.retire_epoch(DeploymentEpoch::new(9));
        // A different query's epoch is never tombstoned here.
        let other = UniqueId { hi: 7, lo: 8 };
        assert_eq!(
            dedupe.tombstone_verdict(other, DeploymentEpoch::new(9)),
            TombstoneVerdict::Live,
        );
        assert_eq!(
            dedupe.tombstone_verdict(other, DeploymentEpoch::new(1)),
            TombstoneVerdict::Live,
        );
    }
}
