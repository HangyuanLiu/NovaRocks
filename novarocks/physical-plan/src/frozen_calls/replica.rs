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

use super::*;
use crate::Distribution;
use novarocks_type_contract::{FunctionInstanceState, FunctionVolatility};

impl FrozenFragmentCalls {
    /// Check the complete occurrence prerequisite for claimed broadcast
    /// equivalence, retaining each invocation's own control domain.
    ///
    /// This rechecks exact definition/root/call correspondence first. It never
    /// chooses one invocation to represent a shared expression definition, and
    /// never consults legacy binding effect bits. All runtime occurrences of a
    /// broadcast-producing node participate, including guarded children and
    /// relational calls. The property author must relinquish an unsupported
    /// broadcast guarantee; this check does not rewrite the plan.
    ///
    /// Row errors remain observable and are not erased. This prerequisite
    /// compares the same invocation on identical replica inputs; it grants no
    /// movement across guards, earlier evaluation, or installed-owner proof.
    /// Admission must still recompute the complete facts with the exact owner.
    pub fn validate_replica_equivalence(
        &self,
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenCallError> {
        self.validate_fragment(fragment, uses, control)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = visit_calls(fragment, uses, &mut work, |site, binding, work| {
            work.step()?;
            let node = match site {
                PhysicalCallSite::Expression(id) => {
                    let invocation = uses
                        .flow()
                        .uses()
                        .get(&id)
                        .ok_or(FrozenCallError::InvalidSite)?;
                    fragment
                        .expressions()
                        .get(invocation.definition)
                        .ok_or(FrozenCallError::InvalidSite)?
                        .owner
                }
                PhysicalCallSite::Aggregate { node, .. }
                | PhysicalCallSite::TopNState { node, .. }
                | PhysicalCallSite::WriterPartial { node, .. }
                | PhysicalCallSite::WriterFinal { node, .. }
                | PhysicalCallSite::Table { node } => node,
            };
            let node = fragment
                .nodes()
                .get(&node)
                .ok_or(FrozenCallError::InvalidSite)?;
            if node.output_properties.distribution != Distribution::Broadcast {
                return Ok(());
            }
            let call = self
                .entries
                .get(&site)
                .ok_or(FrozenCallError::MissingSite(site))?;
            // Relational lifecycle state is required even for deterministic
            // iteration. Its exact kind was checked above; it does not by
            // itself mean the scalar-instance state that forbids replication.
            // Existing relational property proofs remain mandatory.
            if call.effects.value_stability != FunctionVolatility::Immutable
                || (binding.kind() == FunctionKind::Scalar
                    && call.effects.instance_state != FunctionInstanceState::None)
                || !call.effects.observable_effects.is_empty()
            {
                return Err(FrozenCallError::ReplicaEquivalence(site));
            }
            Ok(())
        });
        if matches!(result, Err(FrozenCallError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}
