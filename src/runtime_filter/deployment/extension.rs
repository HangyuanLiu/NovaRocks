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
use std::fmt;
use std::sync::Mutex;

use crate::common::types::UniqueId;
use crate::runtime_filter::deployment::RuntimeFilterDeploymentPlan;
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::install::RuntimeFilterInstallView;

/// Install-port failures surfaced once a real coordinator starts issuing
/// installs (RFD-6). Distinct from RFD-2's compile-time `DeploymentError`:
/// these are runtime phase-contract violations, not static plan defects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentInstallError {
    /// The participant already has a view installed under a different epoch.
    EpochConflict { installed: u64, incoming: u64 },
    /// Same epoch, but the incoming view differs from what is installed.
    ConflictingView { participant: u32 },
}

impl fmt::Display for DeploymentInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EpochConflict {
                installed,
                incoming,
            } => write!(
                f,
                "runtime filter install epoch conflict: installed {installed}, incoming {incoming}"
            ),
            Self::ConflictingView { participant } => write!(
                f,
                "runtime filter install conflict: participant {participant} received a \
                 different view for the same epoch"
            ),
        }
    }
}

impl std::error::Error for DeploymentInstallError {}

/// Coordinator-side install port. `RuntimeFilterInstallPort` is RFD-2's own
/// abstraction; RFD-6 later provides the real adapter that wraps
/// `RuntimeFilterService::install` on each participant BE. Defining the
/// contract here lets the pre-submit phase contract (participant-only
/// install, idempotent retries, epoch-conflict rejection) be proven against a
/// fake ahead of any live wiring.
pub(crate) trait RuntimeFilterInstallPort: Send + Sync {
    fn install(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
        view: RuntimeFilterInstallView,
    ) -> Result<(), DeploymentInstallError>;
}

/// RF pre-submit extension. Turns a compiled [`RuntimeFilterDeploymentPlan`]
/// into per-participant install requests. It does not own query lifecycle —
/// RFD-6 wires this into `ExecutionCoordinator`'s pre-submit phase
/// (`coordinator/execution.rs:195`).
#[derive(Debug, Default)]
pub(crate) struct RuntimeFilterDeploymentExtension;

impl RuntimeFilterDeploymentExtension {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Install requests for the participants the compiler assigned an install
    /// view to (participant-only fan-out — a strict subset of the plan's live
    /// `participants`; role-less backends get no install request).
    pub(crate) fn participant_installs(
        &self,
        plan: &RuntimeFilterDeploymentPlan,
    ) -> Vec<(RuntimeFilterParticipantId, RuntimeFilterInstallView)> {
        plan.install_views
            .iter()
            .map(|(participant, view)| (*participant, view.clone()))
            .collect()
    }
}

/// Recording fake [`RuntimeFilterInstallPort`]: idempotent on an identical
/// `(epoch, view)` retry for a participant, rejects a differing epoch for a
/// participant that already has a view installed. Used to prove the
/// pre-submit phase contract ahead of RFD-6's real adapter.
#[derive(Default)]
pub(crate) struct RecordingInstallPort {
    installed:
        Mutex<BTreeMap<RuntimeFilterParticipantId, (DeploymentEpoch, RuntimeFilterInstallView)>>,
}

impl RecordingInstallPort {
    /// True iff every participant in `participants` has a recorded install.
    pub(crate) fn all_installed(
        &self,
        participants: &BTreeSet<RuntimeFilterParticipantId>,
    ) -> bool {
        let guard = self
            .installed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        participants.iter().all(|p| guard.contains_key(p))
    }
}

impl RuntimeFilterInstallPort for RecordingInstallPort {
    fn install(
        &self,
        _query_id: UniqueId,
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
        view: RuntimeFilterInstallView,
    ) -> Result<(), DeploymentInstallError> {
        let mut guard = self
            .installed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((existing_epoch, existing_view)) = guard.get(&participant) {
            if existing_epoch.get() != epoch.get() {
                return Err(DeploymentInstallError::EpochConflict {
                    installed: existing_epoch.get(),
                    incoming: epoch.get(),
                });
            }
            if existing_view != &view {
                return Err(DeploymentInstallError::ConflictingView {
                    participant: participant.get(),
                });
            }
            return Ok(()); // idempotent retry
        }
        guard.insert(participant, (epoch, view));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::common::types::UniqueId;
    use crate::runtime_filter::deployment::role_graph::RoleGraph;
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::RuntimeFilterInstallView;

    const QUERY: UniqueId = UniqueId { hi: 1, lo: 1 };

    fn pid(x: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(x)
    }

    fn sample_plan(epoch: u64) -> RuntimeFilterDeploymentPlan {
        let e = DeploymentEpoch::new(epoch);
        let mut install_views = BTreeMap::new();
        for p in [pid(0), pid(1)] {
            install_views.insert(p, RuntimeFilterInstallView::new(e, p, BTreeMap::new()));
        }
        RuntimeFilterDeploymentPlan {
            epoch: e,
            participants: BTreeSet::from([pid(0), pid(1)]),
            install_views,
            routing_shards: BTreeMap::new(),
            role_graph: RoleGraph::default(),
        }
    }

    fn sample_plan_with_roleless_participant(epoch: u64) -> RuntimeFilterDeploymentPlan {
        let e = DeploymentEpoch::new(epoch);
        let mut install_views = BTreeMap::new();
        for p in [pid(0), pid(1)] {
            install_views.insert(p, RuntimeFilterInstallView::new(e, p, BTreeMap::new()));
        }
        RuntimeFilterDeploymentPlan {
            epoch: e,
            // pid(2) is a live backend with no RF role; it must NOT be installed.
            participants: BTreeSet::from([pid(0), pid(1), pid(2)]),
            install_views,
            routing_shards: BTreeMap::new(),
            role_graph: RoleGraph::default(),
        }
    }

    #[test]
    fn participant_installs_cover_only_backends_with_views() {
        let plan = sample_plan_with_roleless_participant(7);
        let ext = RuntimeFilterDeploymentExtension::new();
        let installs = ext.participant_installs(&plan);
        // Only the backends the compiler assigned a view to, never the role-less pid(2).
        assert_eq!(installs.len(), plan.install_views.len());
        assert!(installs.iter().all(|(p, _)| *p != pid(2)));
        let port = RecordingInstallPort::default();
        for (participant, view) in installs {
            port.install(QUERY, plan.epoch, participant, view).unwrap();
        }
        assert!(port.all_installed(&BTreeSet::from([pid(0), pid(1)])));
        assert!(!port.all_installed(&plan.participants)); // pid(2) never installed
    }

    #[test]
    fn duplicate_identical_install_is_idempotent_and_epoch_conflict_rejected() {
        let plan = sample_plan(7);
        let port = RecordingInstallPort::default();
        let (participant, view) = plan
            .install_views
            .iter()
            .next()
            .map(|(p, v)| (*p, v.clone()))
            .unwrap();
        port.install(QUERY, plan.epoch, participant, view.clone())
            .unwrap();
        port.install(QUERY, plan.epoch, participant, view.clone())
            .unwrap(); // idempotent
        let other_epoch = DeploymentEpoch::new(plan.epoch.get() + 1);
        let err = port
            .install(QUERY, other_epoch, participant, view)
            .unwrap_err();
        assert!(matches!(err, DeploymentInstallError::EpochConflict { .. }));
    }

    #[test]
    fn conflicting_view_same_epoch_is_rejected() {
        let e = DeploymentEpoch::new(7);
        let port = RecordingInstallPort::default();
        let p = pid(0);
        port.install(
            QUERY,
            e,
            p,
            RuntimeFilterInstallView::new(e, pid(0), BTreeMap::new()),
        )
        .unwrap();
        // Same participant + same epoch, but a different view (different local id inside).
        let err = port
            .install(
                QUERY,
                e,
                p,
                RuntimeFilterInstallView::new(e, pid(1), BTreeMap::new()),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentInstallError::ConflictingView { .. }
        ));
    }
}
