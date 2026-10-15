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

//! One Task conclusion acceptance domain; stopping and physical exit are separate.

use crate::{TaskIdentity, TerminationDetail};
use std::num::NonZeroU64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskConclusion {
    Succeeded,
    Terminated(TerminationDetail),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConclusionDecision {
    Accept,
    Keep,
    RefineDerived,
}

/// Called by the Task owner under its acceptance lock before terminal
/// publication. An already published terminal retains its receipt; late facts
/// are diagnostics. No timestamp ordering,
/// lifecycle observer, or physical-stop escalation can replace this decision.
pub fn decide_conclusion(
    current: Option<&TaskConclusion>,
    proposed: &TaskConclusion,
) -> ConclusionDecision {
    match current {
        None => ConclusionDecision::Accept,
        Some(TaskConclusion::Terminated(existing))
            if existing.is_derived()
                && matches!(
                    proposed,
                    TaskConclusion::Terminated(TerminationDetail::Failed(_))
                ) =>
        {
            ConclusionDecision::RefineDerived
        }
        Some(_) => ConclusionDecision::Keep,
    }
}

/// The immutable accepted fact. A refined derived cause gets a new generation;
/// it is never a second Task or a license to reopen completed work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedConclusion {
    identity: TaskIdentity,
    generation: NonZeroU64,
    conclusion: TaskConclusion,
}
impl AcceptedConclusion {
    pub const fn new(
        identity: TaskIdentity,
        generation: NonZeroU64,
        conclusion: TaskConclusion,
    ) -> Self {
        Self {
            identity,
            generation,
            conclusion,
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    pub const fn conclusion(&self) -> &TaskConclusion {
        &self.conclusion
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConclusionAcceptance {
    pub decision: ConclusionDecision,
    pub accepted: AcceptedConclusion,
}

/// Bound to one exact Task by Worker from Accepted through preparation and
/// execution. Runtime/completion publishers submit here instead of latching.
pub trait TaskConclusionPort: Send + Sync {
    fn identity(&self) -> TaskIdentity;
    fn submit(&self, proposed: TaskConclusion) -> ConclusionAcceptance;
    fn accepted(&self) -> Option<AcceptedConclusion>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory};
    fn failed() -> TaskConclusion {
        TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::truncating("originating failure"),
        )))
    }
    #[test]
    fn accepted_cancellation_cannot_be_replaced_by_failure_abort_or_success() {
        let current = TaskConclusion::Terminated(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        for later in [
            failed(),
            TaskConclusion::Succeeded,
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::LeaseExpired)),
        ] {
            assert_eq!(
                decide_conclusion(Some(&current), &later),
                ConclusionDecision::Keep
            );
        }
    }
    #[test]
    fn only_an_originating_failure_refines_the_derived_placeholder() {
        let derived =
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed));
        assert_eq!(
            decide_conclusion(Some(&derived), &failed()),
            ConclusionDecision::RefineDerived
        );
        assert_eq!(
            decide_conclusion(Some(&failed()), &derived),
            ConclusionDecision::Keep
        );
        assert_eq!(
            decide_conclusion(Some(&derived), &TaskConclusion::Succeeded),
            ConclusionDecision::Keep
        );
        assert_eq!(
            decide_conclusion(None, &failed()),
            ConclusionDecision::Accept
        );
    }
    #[test]
    fn successful_seal_keeps_late_cleanup_as_diagnostics() {
        assert_eq!(
            decide_conclusion(Some(&TaskConclusion::Succeeded), &failed()),
            ConclusionDecision::Keep
        );
    }
}
