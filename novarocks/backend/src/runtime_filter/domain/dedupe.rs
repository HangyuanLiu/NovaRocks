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

//! Backend ingress dedupe for at-least-once envelopes.

use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};

use novarocks_execution::runtime_filter::LogicalVersion;

use super::{
    BackendChannelIdentity, BackendContributionRouteIdentity, BackendDeliveryRouteIdentity,
    BackendRouteEdgeId,
};

type ContributionDigests = BTreeMap<BackendContributionRouteIdentity, [u8; 32]>;
type DeliveryDigests = BTreeMap<BackendDeliveryRouteIdentity, [u8; 32]>;
type RouteVersionState = BTreeMap<(BackendRouteEdgeId, LogicalVersion), ([u8; 32], bool)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub(crate) enum BackendContributionAdmission {
    Fresh,
    DuplicateRetry,
    Conflict,
    ResourceLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendDeliveryAdmission {
    Fresh,
    Duplicate,
    Conflict,
    ResourceLimit,
}

/// Query-scoped bounded identity state. A repeated contribution is only a
/// retry when its exact content digest agrees; otherwise the caller must
/// surface the conflict instead of silently swallowing it.
pub(crate) struct BackendIngressDedupe {
    max_identities_per_channel: usize,
    state: Mutex<BackendIngressDedupeState>,
    changed: Condvar,
}

#[derive(Default)]
struct BackendIngressDedupeState {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    contributions: BTreeMap<BackendChannelIdentity, ContributionDigests>,
    deliveries: BTreeMap<BackendChannelIdentity, DeliveryDigests>,
    versions: BTreeMap<BackendChannelIdentity, RouteVersionState>,
    pending_deliveries: BTreeMap<BackendChannelIdentity, DeliveryDigests>,
    pending_versions: BTreeMap<BackendChannelIdentity, RouteVersionState>,
}

impl BackendIngressDedupe {
    pub(crate) fn new(max_identities_per_channel: usize) -> Self {
        assert!(
            max_identities_per_channel > 0,
            "dedupe must admit one identity"
        );
        Self {
            max_identities_per_channel,
            state: Mutex::new(BackendIngressDedupeState::default()),
            changed: Condvar::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn admit_contribution(
        &self,
        route: BackendContributionRouteIdentity,
        digest: [u8; 32],
    ) -> BackendContributionAdmission {
        let channel = route.stream().channel();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let identities = state.contributions.entry(channel).or_default();
        match identities.get(&route) {
            Some(existing) if *existing == digest => BackendContributionAdmission::DuplicateRetry,
            Some(_) => BackendContributionAdmission::Conflict,
            None if identities.len() >= self.max_identities_per_channel => {
                BackendContributionAdmission::ResourceLimit
            }
            None => {
                identities.insert(route, digest);
                BackendContributionAdmission::Fresh
            }
        }
    }

    pub(crate) fn reserve_delivery(
        &self,
        route: BackendDeliveryRouteIdentity,
        version: Option<LogicalVersion>,
        final_artifact: bool,
        exact_digest: [u8; 32],
        content_digest: [u8; 32],
    ) -> BackendDeliveryAdmission {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let channel = route.channel();
        loop {
            if let Some(existing) = state
                .deliveries
                .get(&channel)
                .and_then(|delivery| delivery.get(&route))
            {
                return if *existing == exact_digest {
                    BackendDeliveryAdmission::Duplicate
                } else {
                    BackendDeliveryAdmission::Conflict
                };
            }
            if let Some(existing) = state
                .pending_deliveries
                .get(&channel)
                .and_then(|delivery| delivery.get(&route))
            {
                if *existing != exact_digest {
                    return BackendDeliveryAdmission::Conflict;
                }
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                continue;
            }
            let retained = state
                .deliveries
                .get(&channel)
                .map_or(0, BTreeMap::len)
                .saturating_add(
                    state
                        .pending_deliveries
                        .get(&channel)
                        .map_or(0, BTreeMap::len),
                );
            if retained >= self.max_identities_per_channel {
                return BackendDeliveryAdmission::ResourceLimit;
            }
            if let Some(version) = version {
                let key = (route.route_edge_id(), version);
                if let Some((existing_digest, was_final)) = state
                    .versions
                    .get(&channel)
                    .and_then(|values| values.get(&key))
                {
                    if *existing_digest != content_digest {
                        return BackendDeliveryAdmission::Conflict;
                    }
                    if *was_final || !final_artifact {
                        state
                            .deliveries
                            .entry(channel)
                            .or_default()
                            .insert(route, exact_digest);
                        return BackendDeliveryAdmission::Duplicate;
                    }
                }
                if let Some((pending_digest, _)) = state
                    .pending_versions
                    .get(&channel)
                    .and_then(|values| values.get(&key))
                {
                    if *pending_digest != content_digest {
                        return BackendDeliveryAdmission::Conflict;
                    }
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(|error| error.into_inner());
                    continue;
                }
                state
                    .pending_versions
                    .entry(channel)
                    .or_default()
                    .insert(key, (content_digest, final_artifact));
            }
            state
                .pending_deliveries
                .entry(channel)
                .or_default()
                .insert(route, exact_digest);
            return BackendDeliveryAdmission::Fresh;
        }
    }

    pub(crate) fn commit_delivery(
        &self,
        route: BackendDeliveryRouteIdentity,
        version: Option<LogicalVersion>,
        final_artifact: bool,
        exact_digest: [u8; 32],
        content_digest: [u8; 32],
    ) {
        let channel = route.channel();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        remove_pending_delivery(&mut state, route, version);
        state
            .deliveries
            .entry(channel)
            .or_default()
            .insert(route, exact_digest);
        if let Some(version) = version {
            let entry = state
                .versions
                .entry(channel)
                .or_default()
                .entry((route.route_edge_id(), version))
                .or_insert((content_digest, false));
            debug_assert_eq!(entry.0, content_digest);
            entry.1 |= final_artifact;
        }
        self.changed.notify_all();
    }

    pub(crate) fn abort_delivery(
        &self,
        route: BackendDeliveryRouteIdentity,
        version: Option<LogicalVersion>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        remove_pending_delivery(&mut state, route, version);
        self.changed.notify_all();
    }
}

fn remove_pending_delivery(
    state: &mut BackendIngressDedupeState,
    route: BackendDeliveryRouteIdentity,
    version: Option<LogicalVersion>,
) {
    let channel = route.channel();
    if let Some(deliveries) = state.pending_deliveries.get_mut(&channel) {
        deliveries.remove(&route);
        if deliveries.is_empty() {
            state.pending_deliveries.remove(&channel);
        }
    }
    if let Some(version) = version
        && let Some(versions) = state.pending_versions.get_mut(&channel)
    {
        versions.remove(&(route.route_edge_id(), version));
        if versions.is_empty() {
            state.pending_versions.remove(&channel);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};

    use novarocks_execution::runtime_filter::{
        PartitionId, RuntimeFilterBindingId, RuntimeFilterChannelId,
    };
    use novarocks_types::UniqueId;

    use super::*;
    use crate::runtime_filter::domain::{
        BackendParticipantIdentity, BackendProducerStreamIdentity, BackendTransportSequence,
    };

    fn channel() -> BackendChannelIdentity {
        BackendChannelIdentity::new(
            BackendParticipantIdentity::new(UniqueId::new(1, 2), 3),
            RuntimeFilterBindingId::new(4),
            RuntimeFilterChannelId::new(5),
        )
    }

    #[test]
    fn contribution_retry_and_conflict_remain_distinct() {
        let dedupe = BackendIngressDedupe::new(2);
        let route = BackendContributionRouteIdentity::new(
            BackendProducerStreamIdentity::new(channel(), UniqueId::new(6, 7), PartitionId::new(8)),
            BackendTransportSequence::new(9),
        );
        assert_eq!(
            dedupe.admit_contribution(route, [1; 32]),
            BackendContributionAdmission::Fresh
        );
        assert_eq!(
            dedupe.admit_contribution(route, [1; 32]),
            BackendContributionAdmission::DuplicateRetry
        );
        assert_eq!(
            dedupe.admit_contribution(route, [2; 32]),
            BackendContributionAdmission::Conflict
        );
    }

    #[test]
    fn final_artifact_is_one_allowed_logical_upgrade() {
        let dedupe = BackendIngressDedupe::new(5);
        let make = |sequence| {
            BackendDeliveryRouteIdentity::new(
                channel(),
                BackendRouteEdgeId::new(10),
                BackendTransportSequence::new(sequence),
            )
        };
        assert_eq!(
            dedupe.reserve_delivery(
                make(1),
                Some(LogicalVersion::FIRST),
                false,
                [1; 32],
                [9; 32]
            ),
            BackendDeliveryAdmission::Fresh
        );
        dedupe.commit_delivery(
            make(1),
            Some(LogicalVersion::FIRST),
            false,
            [1; 32],
            [9; 32],
        );
        assert_eq!(
            dedupe.reserve_delivery(
                make(2),
                Some(LogicalVersion::FIRST),
                false,
                [2; 32],
                [9; 32]
            ),
            BackendDeliveryAdmission::Duplicate
        );
        assert_eq!(
            dedupe.reserve_delivery(make(3), Some(LogicalVersion::FIRST), true, [3; 32], [9; 32]),
            BackendDeliveryAdmission::Fresh
        );
        assert_eq!(
            dedupe.reserve_delivery(make(3), Some(LogicalVersion::FIRST), true, [4; 32], [9; 32]),
            BackendDeliveryAdmission::Conflict
        );
        dedupe.commit_delivery(make(3), Some(LogicalVersion::FIRST), true, [3; 32], [9; 32]);
        assert_eq!(
            dedupe.reserve_delivery(make(4), Some(LogicalVersion::FIRST), true, [5; 32], [8; 32]),
            BackendDeliveryAdmission::Conflict
        );
    }

    #[test]
    fn retry_waits_for_reservation_and_retries_after_abort() {
        let dedupe = Arc::new(BackendIngressDedupe::new(2));
        let route = BackendDeliveryRouteIdentity::new(
            channel(),
            BackendRouteEdgeId::new(10),
            BackendTransportSequence::new(1),
        );
        assert_eq!(
            dedupe.reserve_delivery(route, Some(LogicalVersion::FIRST), false, [1; 32], [9; 32],),
            BackendDeliveryAdmission::Fresh
        );

        let (sent, received) = mpsc::channel();
        let retry = Arc::clone(&dedupe);
        let waiter = std::thread::spawn(move || {
            let admission =
                retry.reserve_delivery(route, Some(LogicalVersion::FIRST), false, [1; 32], [9; 32]);
            sent.send(admission).expect("report retry admission");
        });
        assert!(matches!(
            received.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        dedupe.abort_delivery(route, Some(LogicalVersion::FIRST));
        assert_eq!(
            received.recv().expect("retry admission after abort"),
            BackendDeliveryAdmission::Fresh
        );
        dedupe.abort_delivery(route, Some(LogicalVersion::FIRST));
        waiter.join().expect("retry waiter");
    }
}
