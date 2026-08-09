// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership.  The Apache License, Version 2.0 applies.

//! Backend ingress dedupe for at-least-once envelopes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use novarocks_execution::runtime_filter::LogicalVersion;

use super::{
    BackendChannelIdentity, BackendContributionRouteIdentity, BackendDeliveryRouteIdentity,
    BackendRouteEdgeId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    ResourceLimit,
}

/// Query-scoped bounded identity state. A repeated contribution is only a
/// retry when its exact content digest agrees; otherwise the caller must
/// surface the conflict instead of silently swallowing it.
pub(crate) struct BackendIngressDedupe {
    max_identities_per_channel: usize,
    state: Mutex<BackendIngressDedupeState>,
}

#[derive(Default)]
struct BackendIngressDedupeState {
    contributions:
        BTreeMap<BackendChannelIdentity, BTreeMap<BackendContributionRouteIdentity, [u8; 32]>>,
    deliveries: BTreeMap<BackendChannelIdentity, BTreeSet<BackendDeliveryRouteIdentity>>,
    versions:
        BTreeMap<BackendChannelIdentity, BTreeMap<(BackendRouteEdgeId, LogicalVersion), bool>>,
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
        }
    }

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

    pub(crate) fn admit_delivery(
        &self,
        route: BackendDeliveryRouteIdentity,
        version: LogicalVersion,
        final_artifact: bool,
    ) -> BackendDeliveryAdmission {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let channel = route.channel();
        if state
            .deliveries
            .get(&channel)
            .is_some_and(|delivery| delivery.contains(&route))
        {
            return BackendDeliveryAdmission::Duplicate;
        }
        if state
            .deliveries
            .get(&channel)
            .is_some_and(|delivery| delivery.len() >= self.max_identities_per_channel)
        {
            return BackendDeliveryAdmission::ResourceLimit;
        }
        let versions = state.versions.entry(channel).or_default();
        if let Some(was_final) = versions.get(&(route.route_edge_id(), version)) {
            // A final artifact is a strict upgrade of the same logical version;
            // a non-final replay stays duplicate. The distinct transport identity
            // is still retained to make future retries idempotent.
            if *was_final || !final_artifact {
                state.deliveries.entry(channel).or_default().insert(route);
                return BackendDeliveryAdmission::Duplicate;
            }
        }
        versions.insert((route.route_edge_id(), version), final_artifact);
        state.deliveries.entry(channel).or_default().insert(route);
        BackendDeliveryAdmission::Fresh
    }
}

#[cfg(test)]
mod tests {
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
        let dedupe = BackendIngressDedupe::new(3);
        let make = |sequence| {
            BackendDeliveryRouteIdentity::new(
                channel(),
                BackendRouteEdgeId::new(10),
                BackendTransportSequence::new(sequence),
            )
        };
        assert_eq!(
            dedupe.admit_delivery(make(1), LogicalVersion::FIRST, false),
            BackendDeliveryAdmission::Fresh
        );
        assert_eq!(
            dedupe.admit_delivery(make(2), LogicalVersion::FIRST, false),
            BackendDeliveryAdmission::Duplicate
        );
        assert_eq!(
            dedupe.admit_delivery(make(3), LogicalVersion::FIRST, true),
            BackendDeliveryAdmission::Fresh
        );
    }
}
