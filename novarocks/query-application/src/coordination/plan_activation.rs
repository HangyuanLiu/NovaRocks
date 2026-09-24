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

//! Which plan one logical execution runs.
//!
//! Preparation and completion choose a logical execution's plan, once. The
//! activation hands that completed plan to the execution, and from then on it
//! is the plan every attempt of the execution runs. There is no second
//! candidate to swap in and nothing to spend: nothing produces one, so a
//! replacement mechanism would be a state machine for a source that does not
//! exist. Two attempts of one statement disagreeing about plan shape is how a
//! query comes to read two different tables, and a fixed activation is what
//! rules it out.
//!
//! What still guards dispatch is not here. The logical-execution actor
//! authorizes each admission ticket and each establish, and its cancellation
//! and terminal gates stop new work; an activated plan grants none of that.
//!
//! Nothing here can reach a compiler, an observation, or a negotiation. An
//! attempt replacement the business allows asks this type for the plan and
//! gets the one the execution was activated with; there is no path from here
//! to planning the statement again.

use std::sync::Arc;

use novarocks_physical_plan::PhysicalPlan;

use crate::preparation::CompletedPhysicalPlanCandidate;

/// The completed semantic candidate one logical execution runs.
// Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)
#[derive(Debug)]
pub(crate) struct ActiveLogicalPlan {
    candidate: CompletedPhysicalPlanCandidate,
}

impl ActiveLogicalPlan {
    /// Activate a completed plan, once and for good.
    ///
    /// The completed description retains the candidate, while its Native
    /// session retains the access template checked during encoding. Only the
    /// supervisor can activate their accepted request.
    pub(crate) fn activate(candidate: CompletedPhysicalPlanCandidate) -> Self {
        Self { candidate }
    }

    /// The plan every attempt of this execution runs.
    ///
    /// A replacement attempt calls this and gets what the first attempt ran.
    /// That is the whole mechanism: there is nothing else here to call.
    pub(crate) fn plan(&self) -> &Arc<PhysicalPlan> {
        self.candidate.plan()
    }
}

#[cfg(test)]
mod tests {
    use novarocks_physical_plan::PlanVersionId;

    use crate::completed_plan_fixture::completed_values_plan;

    use super::*;

    const FIRST: [u8; 16] = [1; 16];

    /// Activation fixes the plan: every later question about which plan this
    /// execution runs answers with the very plan it was activated with.
    #[tokio::test]
    async fn activation_fixes_the_plan_every_attempt_runs() {
        let completed = completed_values_plan(FIRST).await;
        let candidate = completed.candidate().clone();
        let expected = Arc::clone(candidate.plan());
        let active = ActiveLogicalPlan::activate(candidate);
        assert_eq!(
            active.plan().version(),
            PlanVersionId::try_new(FIRST).unwrap()
        );
        assert!(
            Arc::ptr_eq(active.plan(), &expected),
            "a later attempt reads the activated plan itself, not an equal copy"
        );
    }
}
