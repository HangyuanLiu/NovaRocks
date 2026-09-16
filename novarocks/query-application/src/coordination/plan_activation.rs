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

//! Which plan one logical execution runs, and when that stops being a choice.
//!
//! A statement can be planned more than once before anything is dispatched -
//! a better plan arrives, a candidate is superseded - and that is a plan
//! replacement. Once the first task has been asked for, it is no longer a
//! choice: every later attempt of the same statement runs the plan that was
//! already dispatched. Two attempts of one statement disagreeing about plan
//! shape is how a query comes to read two different tables, and it is exactly
//! what re-planning between attempts produces.
//!
//! The right to dispatch is therefore a value, held once and spent once. It is
//! spent when the intent is handed to the dispatcher, not when the dispatcher
//! reports back: an outcome nobody learned still means a task may exist, and a
//! plan that may already be running is not replaceable. Closing the choice on
//! submission rather than on success is what makes an unknown outcome safe.
//!
//! Nothing here can reach a compiler, an observation, or a negotiation. An
//! attempt replacement asks this type for the plan and gets the same one back;
//! there is no path from here to planning the statement again.

use std::sync::Arc;

use novarocks_physical_plan::{PhysicalPlan, PlanVersionId};

use crate::preparation::CompletedPlanWithAccess;

/// The one-time right to dispatch a logical execution's first task.
///
/// It cannot be copied, cloned or rebuilt: holding it is the proof that no
/// task has been asked for yet, and spending it is what makes that
/// irreversible.
#[derive(Debug)]
pub struct DispatchSeal {
    version: PlanVersionId,
}

impl DispatchSeal {
    /// The plan version this seal authorizes, so a dispatcher cannot be handed
    /// a seal for one version and a plan for another.
    pub const fn version(&self) -> PlanVersionId {
        self.version
    }
}

/// The plan one logical execution runs.
#[derive(Debug)]
pub struct ActiveLogicalPlan<A> {
    candidate: CompletedPlanWithAccess<A>,
    seal: Option<DispatchSeal>,
}

impl<A> ActiveLogicalPlan<A> {
    /// Activate a completed plan, minting the one right to dispatch it.
    ///
    /// The candidate is already validated and already paired with the
    /// capabilities its scans were frozen with, so activation adds no check of
    /// its own - there is no unchecked plan for it to accept.
    pub fn activate(candidate: CompletedPlanWithAccess<A>) -> Self {
        let version = candidate.candidate().plan().version();
        Self {
            candidate,
            seal: Some(DispatchSeal { version }),
        }
    }

    pub fn version(&self) -> PlanVersionId {
        self.candidate.candidate().plan().version()
    }

    /// The plan every attempt of this execution runs.
    ///
    /// A replacement attempt calls this and gets what the first attempt ran.
    /// That is the whole mechanism: there is nothing else here to call.
    pub fn plan(&self) -> &Arc<PhysicalPlan> {
        self.candidate.candidate().plan()
    }

    pub const fn candidate(&self) -> &CompletedPlanWithAccess<A> {
        &self.candidate
    }

    /// Whether dispatching has closed the choice of plan.
    pub const fn is_dispatched(&self) -> bool {
        self.seal.is_none()
    }

    /// Replace the plan this execution will run, before anything is dispatched.
    ///
    /// The displaced plan comes back rather than being dropped: its scans were
    /// frozen, and the capabilities that freeze produced still have an owner
    /// waiting to release them.
    pub fn replace(
        &mut self,
        candidate: CompletedPlanWithAccess<A>,
    ) -> Result<CompletedPlanWithAccess<A>, PlanActivationRefused<A>> {
        let version = candidate.candidate().plan().version();
        if self.seal.is_none() {
            return Err(PlanActivationRefused {
                error: PlanActivationError::AlreadyDispatched {
                    active: self.version(),
                },
                candidate,
            });
        }
        if version == self.version() {
            return Err(PlanActivationRefused {
                error: PlanActivationError::SameVersion { version },
                candidate,
            });
        }
        self.seal = Some(DispatchSeal { version });
        Ok(std::mem::replace(&mut self.candidate, candidate))
    }

    /// Spend the right to dispatch.
    ///
    /// Called with the intent, before the dispatcher answers. From here on the
    /// plan is what this execution runs, whatever the dispatcher reports and
    /// whatever it fails to report.
    pub fn take_dispatch_seal(&mut self) -> Result<DispatchSeal, PlanActivationError> {
        self.seal
            .take()
            .ok_or(PlanActivationError::AlreadyDispatched {
                active: self.version(),
            })
    }

    /// Give up the plan and its capabilities, for their owner to release.
    pub fn into_candidate(self) -> CompletedPlanWithAccess<A> {
        self.candidate
    }
}

/// A refused replacement, with the candidate that was not adopted.
///
/// The candidate comes back for the same reason a displaced one does: its
/// capabilities are real whether or not the plan they belong to ever runs.
pub struct PlanActivationRefused<A> {
    error: PlanActivationError,
    candidate: CompletedPlanWithAccess<A>,
}

impl<A> PlanActivationRefused<A> {
    pub const fn error(&self) -> PlanActivationError {
        self.error
    }

    /// The candidate that was not adopted, for its owner to release.
    pub fn into_candidate(self) -> CompletedPlanWithAccess<A> {
        self.candidate
    }

    pub fn into_parts(self) -> (PlanActivationError, CompletedPlanWithAccess<A>) {
        (self.error, self.candidate)
    }
}

/// Written without asking the capability to be printable.
impl<A> std::fmt::Debug for PlanActivationRefused<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlanActivationRefused")
            .field("error", &self.error)
            .field(
                "candidate_version",
                &self.candidate.candidate().plan().version(),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanActivationError {
    /// A task has already been asked for, so the plan is no longer a choice.
    AlreadyDispatched { active: PlanVersionId },
    /// A replacement must be a different plan; adopting the same version again
    /// would mint a second right to dispatch what is already active.
    SameVersion { version: PlanVersionId },
}

impl std::fmt::Display for PlanActivationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyDispatched { active } => write!(
                formatter,
                "plan version {active:?} has been dispatched and cannot be replaced"
            ),
            Self::SameVersion { version } => write!(
                formatter,
                "plan version {version:?} is already the active plan"
            ),
        }
    }
}

impl std::error::Error for PlanActivationError {}

#[cfg(test)]
mod tests {
    use crate::completed_plan_fixture::completed_values_plan;

    use super::*;

    const FIRST: [u8; 16] = [1; 16];
    const SECOND: [u8; 16] = [2; 16];

    /// Before anything is dispatched, a better plan may take over - and the one
    /// it displaces comes back, because its scans were frozen and something has
    /// to release what that produced.
    #[tokio::test]
    async fn a_plan_can_be_replaced_until_the_first_task_is_asked_for() {
        let mut active = ActiveLogicalPlan::activate(completed_values_plan(FIRST).await);
        assert_eq!(active.version(), PlanVersionId::try_new(FIRST).unwrap());
        let displaced = active
            .replace(completed_values_plan(SECOND).await)
            .expect("nothing has been dispatched");
        assert_eq!(
            displaced.candidate().plan().version(),
            PlanVersionId::try_new(FIRST).unwrap()
        );
        assert_eq!(active.version(), PlanVersionId::try_new(SECOND).unwrap());
    }

    /// The right to dispatch is spent when the intent is handed over, not when
    /// the dispatcher answers. A task may exist from that moment on, so the
    /// plan stops being a choice from that moment on.
    #[tokio::test]
    async fn asking_for_a_task_closes_the_choice_of_plan() {
        let mut active = ActiveLogicalPlan::activate(completed_values_plan(FIRST).await);
        let seal = active.take_dispatch_seal().expect("first dispatch");
        assert_eq!(seal.version(), PlanVersionId::try_new(FIRST).unwrap());
        assert!(active.is_dispatched());

        let refused = active
            .replace(completed_values_plan(SECOND).await)
            .expect_err("a plan that may be running cannot be replaced");
        assert!(matches!(
            refused.error(),
            PlanActivationError::AlreadyDispatched { .. }
        ));
        // The candidate nobody adopted still has capabilities to release.
        assert_eq!(
            refused.into_candidate().candidate().plan().version(),
            PlanVersionId::try_new(SECOND).unwrap()
        );
        // And the plan every later attempt runs is the one that was dispatched.
        assert_eq!(active.version(), PlanVersionId::try_new(FIRST).unwrap());
    }

    /// There is one right to dispatch, not one per caller.
    #[tokio::test]
    async fn the_right_to_dispatch_is_spent_once() {
        let mut active = ActiveLogicalPlan::activate(completed_values_plan(FIRST).await);
        active.take_dispatch_seal().expect("first dispatch");
        assert!(matches!(
            active.take_dispatch_seal(),
            Err(PlanActivationError::AlreadyDispatched { .. })
        ));
    }

    /// Adopting the active plan again would mint a second right to dispatch
    /// what is already active, which is the one thing the seal exists to
    /// prevent.
    #[tokio::test]
    async fn replacing_a_plan_with_itself_is_refused() {
        let mut active = ActiveLogicalPlan::activate(completed_values_plan(FIRST).await);
        let refused = active
            .replace(completed_values_plan(FIRST).await)
            .expect_err("the same version is not a replacement");
        assert!(matches!(
            refused.error(),
            PlanActivationError::SameVersion { .. }
        ));
    }
}
