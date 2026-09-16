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

//! What an operator can ask and say about management continuation.
//!
//! A management entrance that has lost the outcome of an effect stays closed,
//! and nothing inside this process can reopen it: only evidence that the old
//! dispatch can no longer land is admissible, and that evidence comes from
//! outside. This module is the one place that turns such evidence into a
//! readmission, and the one place that reports why a target is closed.
//!
//! It deliberately owns no durable state. The challenge it issues binds a
//! declaration to this process, this object and this admission state so a
//! stale statement cannot be replayed against a target that has since moved;
//! it is not a lease, a fence, or a takeover token, and it does not survive a
//! restart or any change to the state it was issued against.

use std::sync::Mutex;

use novarocks_spi::connector::{ConnectorTableIdentity, ConnectorTableObjectId};

use super::{
    DeploymentOwner, EffectScope, IsolationEvidence, ManagementClock, ManagementEntrance,
    ManagementTimestamp, ManualReadmissionDeclaration, MvManagementPhase, ProcessIncarnation,
    ReadmissionChallenge, ReadmissionError, ReadmissionEvaluator, ReadmissionMode,
    ReadmissionPermit, RemoteEffectLifetimeGuarantee, UnsettledEffect,
};

/// The remote-effect lifetime guarantees this deployment actually has.
///
/// Having none is the default and is not a gap to be filled with a number: a
/// configured duration is a claim about when an external system stops being
/// able to apply a request, and a deployment that cannot make that claim has
/// only an operator's declaration to continue on.
#[derive(Clone, Debug, Default)]
pub struct RemoteEffectPolicy {
    catalog_commit: Option<RemoteEffectLifetimeGuarantee>,
    object_deletion: Option<RemoteEffectLifetimeGuarantee>,
}

impl RemoteEffectPolicy {
    /// Declare the guarantees this deployment has, each covering its own path.
    ///
    /// A guarantee whose scope does not cover the path it is declared for is
    /// refused rather than narrowed: a catalog-commit bound says nothing about
    /// when an object deletion stops being possible.
    pub fn try_new(
        catalog_commit: Option<RemoteEffectLifetimeGuarantee>,
        object_deletion: Option<RemoteEffectLifetimeGuarantee>,
    ) -> Result<Self, ReadmissionError> {
        if catalog_commit
            .as_ref()
            .is_some_and(|guarantee| !guarantee.scope().covers(EffectScope::CATALOG_COMMIT))
            || object_deletion
                .as_ref()
                .is_some_and(|guarantee| !guarantee.scope().covers(EffectScope::OBJECT_DELETION))
        {
            return Err(ReadmissionError::GuaranteeScopeMismatch);
        }
        Ok(Self {
            catalog_commit,
            object_deletion,
        })
    }

    /// The one guarantee that covers an effect's whole scope, if there is one.
    ///
    /// An effect that spans both paths needs both to be guaranteed, and the
    /// narrower of the two decides, because the window is only elapsed once
    /// neither path can still land.
    pub fn guarantee_for(&self, scope: EffectScope) -> Option<&RemoteEffectLifetimeGuarantee> {
        let needed: Vec<&RemoteEffectLifetimeGuarantee> = [
            (EffectScope::CATALOG_COMMIT, self.catalog_commit.as_ref()),
            (EffectScope::OBJECT_DELETION, self.object_deletion.as_ref()),
        ]
        .into_iter()
        .filter(|(path, _)| scope.covers(*path))
        .map(|(_, guarantee)| guarantee)
        .collect::<Option<Vec<_>>>()?;
        needed
            .into_iter()
            .max_by_key(|guarantee| guarantee.lifetime().saturating_add(guarantee.margin()))
    }

    /// Whether an effect of this scope could ever continue without an operator.
    pub fn mode_for(&self, scope: EffectScope) -> ReadmissionMode {
        match self.guarantee_for(scope) {
            Some(_) => ReadmissionMode::AutomaticWhenGuaranteed,
            None => ReadmissionMode::OperatorDeclarationOnly,
        }
    }
}

/// One unresolved effect, as an operator needs to see it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvUnsettledEffectStatus {
    pub effect: super::EffectIdentity,
    pub dispatching_incarnation: ProcessIncarnation,
    pub scope: EffectScope,
    pub last_possible_dispatch_at: ManagementTimestamp,
    pub mode: ReadmissionMode,
}

/// Everything this process can say about one target's management.
#[derive(Clone, Debug)]
pub struct MvManagementStatus {
    pub table: ConnectorTableIdentity,
    pub object_id: Option<ConnectorTableObjectId>,
    pub local_owner: DeploymentOwner,
    pub local_incarnation: ProcessIncarnation,
    pub phase: MvManagementPhase,
    pub unsettled: Vec<MvUnsettledEffectStatus>,
    /// Issued only when a declaration could actually be used. A phase that no
    /// operator statement can change carries none, so a challenge never
    /// suggests an action that does not exist.
    pub challenge: Option<ReadmissionChallenge>,
    pub required_evidence: Option<String>,
}

/// One operator's statement that a target's previous writer is isolated.
///
/// `declared_at` is when the operator says the isolation was true, and it must
/// not precede the isolation itself; the evidence text is what a later reader
/// has to judge the statement by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvResumeDeclaration {
    pub challenge: ReadmissionChallenge,
    pub old_incarnation: ProcessIncarnation,
    pub operator: String,
    pub evidence: String,
    pub declared_at: ManagementTimestamp,
}

/// The process-local owner of management continuation.
pub struct ManagementContinuationService {
    entrance: ManagementEntrance,
    policy: RemoteEffectPolicy,
    evaluator: Mutex<ReadmissionEvaluator>,
}

impl ManagementContinuationService {
    pub fn new(entrance: ManagementEntrance, policy: RemoteEffectPolicy) -> Self {
        Self {
            entrance,
            policy,
            evaluator: Mutex::new(ReadmissionEvaluator::default()),
        }
    }

    pub const fn policy(&self) -> &RemoteEffectPolicy {
        &self.policy
    }

    pub fn local_owner(&self) -> &DeploymentOwner {
        self.entrance.owner()
    }

    pub fn local_incarnation(&self) -> &ProcessIncarnation {
        self.entrance.incarnation()
    }

    /// What this process can currently do with one target.
    pub fn management_phase(&self, table: &ConnectorTableIdentity) -> MvManagementPhase {
        self.entrance.management_phase(table)
    }

    /// Report one target's management state, and issue the challenge a
    /// declaration about it would have to carry.
    pub fn status(
        &self,
        table: &ConnectorTableIdentity,
        object_id: Option<ConnectorTableObjectId>,
        challenge: ReadmissionChallenge,
    ) -> Result<MvManagementStatus, ReadmissionError> {
        let phase = self.entrance.management_phase(table);
        let unsettled = self
            .entrance
            .unsettled_effects(table)
            .iter()
            .map(|effect| {
                let responsibility = effect.responsibility();
                MvUnsettledEffectStatus {
                    effect: responsibility.identity(),
                    dispatching_incarnation: responsibility.dispatching_incarnation().clone(),
                    scope: responsibility.scope(),
                    last_possible_dispatch_at: responsibility.last_possible_dispatch_at(),
                    mode: self.policy.mode_for(responsibility.scope()),
                }
            })
            .collect::<Vec<_>>();
        let issued = if unsettled.is_empty() {
            None
        } else {
            self.evaluator()?.issue_challenge(challenge)?;
            Some(challenge)
        };
        Ok(MvManagementStatus {
            table: table.clone(),
            object_id,
            local_owner: self.entrance.owner().clone(),
            local_incarnation: self.entrance.incarnation().clone(),
            phase,
            required_evidence: issued.map(|_| required_evidence(&unsettled)),
            unsettled,
            challenge: issued,
        })
    }

    /// Continue every unresolved effect of one target on an operator's single
    /// statement that its previous writer is isolated.
    ///
    /// One statement, one challenge, and every effect it covers: an operator
    /// declares a fact about a writer, not about an effect identity they have
    /// no way to know. An effect dispatched by some other incarnation is not
    /// covered by that fact, so the whole statement is refused rather than
    /// partially applied -- half a resumed target is a target whose remaining
    /// barrier nobody knows about.
    ///
    /// The permits say only that a fresh observation may now happen. What that
    /// observation finds still decides what the effects actually did.
    pub fn resume_target_on_declaration(
        &self,
        table: &ConnectorTableIdentity,
        declaration: &MvResumeDeclaration,
    ) -> Result<Vec<ReadmissionPermit>, ReadmissionError> {
        let unsettled = self.entrance.unsettled_effects(table);
        if unsettled.is_empty() {
            return Err(ReadmissionError::ManualMode);
        }
        if unsettled.iter().any(|effect| {
            *effect.responsibility().dispatching_incarnation() != declaration.old_incarnation
        }) {
            return Err(ReadmissionError::IncarnationMismatch);
        }
        let mut evaluator = self.evaluator()?;
        evaluator.consume_challenge(declaration.challenge)?;
        unsettled
            .iter()
            .map(|effect| {
                let responsibility = effect.responsibility();
                let isolation = IsolationEvidence::try_new(
                    responsibility.target().clone(),
                    declaration.old_incarnation.clone(),
                    declaration.declared_at,
                    declaration.evidence.as_str(),
                )?;
                let declared = ManualReadmissionDeclaration::try_new(
                    declaration.challenge,
                    responsibility.identity(),
                    responsibility.target().clone(),
                    declaration.old_incarnation.clone(),
                    responsibility.scope(),
                    declaration.operator.as_str(),
                    declaration.evidence.as_str(),
                    declaration.declared_at,
                )?;
                ReadmissionEvaluator::permit_declared(effect, &isolation, &declared)
            })
            .collect()
    }

    /// Continue one unresolved effect on an operator's declaration that the
    /// old dispatch has been isolated and can no longer land.
    ///
    /// The declaration does not decide what the effect did. It only permits
    /// the exact re-observation that will find out, which is why a permit that
    /// came from an unknown disposition keeps saying so.
    pub fn resume_on_declaration(
        &self,
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        declaration: &ManualReadmissionDeclaration,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        self.evaluator()?
            .from_manual_declaration(effect, isolation, declaration)
    }

    /// Continue one unresolved effect because its guaranteed remote lifetime
    /// has demonstrably elapsed. Without a guarantee for the effect's scope
    /// this refuses rather than falling back to a timeout.
    pub fn resume_on_policy_window(
        &self,
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        clock: &dyn ManagementClock,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        let scope = effect.responsibility().scope();
        let guarantee = self
            .policy
            .guarantee_for(scope)
            .ok_or(ReadmissionError::ManualMode)?
            .clone();
        self.evaluator()?.from_policy_window(
            effect,
            isolation,
            &guarantee,
            self.policy.mode_for(scope),
            clock,
        )
    }

    fn evaluator(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ReadmissionEvaluator>, ReadmissionError> {
        self.evaluator
            .lock()
            .map_err(|_| ReadmissionError::EvaluatorUnavailable)
    }
}

/// What an operator must be able to state before a declaration is accepted.
fn required_evidence(unsettled: &[MvUnsettledEffectStatus]) -> String {
    let operator_only = unsettled
        .iter()
        .filter(|effect| effect.mode == ReadmissionMode::OperatorDeclarationOnly)
        .count();
    if operator_only == 0 {
        return "the guaranteed remote lifetime of each unresolved effect has elapsed".to_string();
    }
    format!(
        "an operator statement that the dispatching incarnation is isolated and its {operator_only} \
         unresolved effect(s) can no longer land"
    )
}
