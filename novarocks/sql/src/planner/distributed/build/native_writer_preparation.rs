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

//! Native-host Writer preparation from original materialized channel sources.
//! Ordinary calls keep the already-published neutral request namespace and
//! their actual default runtime actors; no Candidate expression gate is added.

use super::{
    expression_occurrences::author_native_writer_occurrences_observed,
    lowered_draft::SqlAuthoredPhysicalPlan,
    package_semantics::PackageSemanticsError,
    physical_fragment_effects::writer_source,
    physical_relational_effects::relational_writer_context_observed,
    physical_writer_occurrences::{
        PhysicalWriterOccurrenceError, WriterOccurrenceCallInput,
        prepare_native_writer_occurrence_observed,
    },
    physical_writer_requests::{
        PhysicalWriterRequestError, author_physical_writer_request_observed,
    },
};
use novarocks_physical_plan::{ConstantPolicy, FragmentId, FrozenPhysicalCall, PhysicalCallSite};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompilePhase, PureCompileControl,
};
use std::collections::BTreeMap;

/// Only actual Writer sites are included. This is neither a complete call table
/// nor an expression-flow certificate and has no conversion into either.
#[derive(Debug)]
pub struct NativeWriterPreparation {
    fragment: FragmentId,
    calls: BTreeMap<PhysicalCallSite, FrozenPhysicalCall>,
}
impl NativeWriterPreparation {
    pub fn fragment(&self) -> FragmentId {
        self.fragment
    }

    pub fn calls(&self) -> &BTreeMap<PhysicalCallSite, FrozenPhysicalCall> {
        &self.calls
    }
}

pub fn author_native_writer_preparation(
    owner: &SqlAuthoredPhysicalPlan,
    statement_policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, NativeWriterPreparation>, PackageSemanticsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let mut output = BTreeMap::new();
        for (id, fragment) in owner.plan().fragments() {
            work.step()?;
            // The sole finish-draft author already paired every definition with
            // its true request. Observe the actual statement policy without
            // resolving the call again or promoting metadata into capability.
            for (_, request) in fragment.call_requests().entries() {
                work.step()?;
                if request.constant_policy != statement_policy {
                    return Err(PackageSemanticsError::InvalidSource(
                        "native original request changes its captured statement constant policy",
                    ));
                }
            }
            work.flush()?;
            let occurrences = author_native_writer_occurrences_observed(fragment, control)?;
            let contexts = occurrences.contexts();
            let mut calls = BTreeMap::new();
            for &(site, _) in contexts.relational_contexts {
                work.step()?;
                let node_id = match site {
                    PhysicalCallSite::WriterPartial { node, .. }
                    | PhysicalCallSite::WriterFinal { node, .. } => node,
                    _ => {
                        return Err(PackageSemanticsError::InvalidSource(
                            "native Writer source contains another lifecycle",
                        ));
                    }
                };
                let node =
                    fragment
                        .nodes()
                        .get(&node_id)
                        .ok_or(PackageSemanticsError::InvalidSource(
                            "native Writer source node is absent",
                        ))?;
                let source = writer_source(node, site)?;
                let entry = owner.checked_writer_aggregate_source_observed(
                    fragment, node, site, source, &mut work,
                )?;
                if entry.captured().constant_policy() != statement_policy {
                    return Err(PackageSemanticsError::PolicyMismatch(site));
                }
                let request = author_physical_writer_request_observed(&entry, &mut work).map_err(
                    |cause| match cause {
                        PhysicalWriterRequestError::Control(cause) => {
                            PackageSemanticsError::Control(cause)
                        }
                        other => PackageSemanticsError::Journal(format!("{other:?}")),
                    },
                )?;
                let context = relational_writer_context_observed(&contexts, site, &mut work).map_err(|cause| match cause {
                    super::physical_relational_effects::PhysicalRelationalEffectsError::Control(cause) => PackageSemanticsError::Control(cause),
                    other => PackageSemanticsError::Effects(format!("{other:?}")),
                })?;
                // These references come only from the original captured lexical
                // author. An empty environment is the current no-dependency
                // Writer role, not a guessed default for an absent dependency.
                let environment = entry
                    .captured()
                    .binding()
                    .group_concat_source()
                    .map(|facts| facts.environment());
                let environment = environment.as_ref().map_or(&[][..], |refs| refs.as_slice());
                for reference in environment {
                    owner.plan().parameters().require(*reference).map_err(|_| {
                        PackageSemanticsError::InvalidSource(
                            "native Writer admitted parameter is absent",
                        )
                    })?;
                    work.step()?;
                }
                let fresh = prepare_native_writer_occurrence_observed(
                    WriterOccurrenceCallInput {
                        fragment,
                        node,
                        source,
                        request: &request,
                        parameters: owner.plan().parameters(),
                        environment,
                        decimal_overflow_policy: entry
                            .captured()
                            .binding()
                            .decimal_overflow_policy(),
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    &occurrences,
                    owner.function_catalog().as_ref(),
                    &mut work,
                )
                .map_err(|cause| match cause {
                    PhysicalWriterOccurrenceError::Control(cause) => {
                        PackageSemanticsError::Control(cause)
                    }
                    other => PackageSemanticsError::Effects(format!("{other:?}")),
                })?;
                work.flush()?;
                if calls.insert(site, fresh.frozen).is_some() {
                    return Err(PackageSemanticsError::InvalidSource(
                        "native Writer source repeats an actual call site",
                    ));
                }
                work.step()?;
            }
            work.flush()?;
            output.insert(
                *id,
                NativeWriterPreparation {
                    fragment: fragment.id(),
                    calls,
                },
            );
            work.step()?;
        }
        Ok(output)
    })();
    if matches!(result, Err(PackageSemanticsError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests {
    use super::super::contract_lowering::lowered_writer_state_tests::authored;
    use super::*;
    use novarocks_type_contract::{CompileControlError, EvaluationDemand};
    use std::sync::Mutex;
    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            if let Some((stop, _)) = self.refuse {
                assert!(at <= stop, "callback after original control refusal");
            }
            trace.push((phase, units));
            match self.refuse {
                Some((stop, cause)) if at == stop => Err(cause),
                _ => Ok(()),
            }
        }
    }
    #[test]
    fn native_writer_source_preserves_actual_partial_final_and_disjoint_value_receipts() {
        let owner = authored(&[1, 1], &[false, false]);
        let control = Control::default();
        let source = author_native_writer_preparation(
            &owner,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        assert_eq!(
            source.keys().collect::<Vec<_>>(),
            owner.plan().fragments().keys().collect::<Vec<_>>()
        );
        let mut partial = 0;
        let mut final_count = 0;
        for (&id, calls) in &source {
            let fragment = &owner.plan().fragments()[&id];
            assert_eq!(calls.fragment(), fragment.id());
            let mut contexts = std::collections::BTreeSet::new();
            let mut domains = std::collections::BTreeSet::new();
            for (&site, call) in calls.calls() {
                assert_eq!(call.site, site);
                match site {
                    PhysicalCallSite::WriterPartial { .. } => partial += 1,
                    PhysicalCallSite::WriterFinal { .. } => final_count += 1,
                    _ => panic!(
                        "Native source must not claim ordinary expression or other call effects"
                    ),
                }
                let value = call
                    .materialized_writer_input
                    .expect("real materialized Value context");
                assert_eq!(
                    (call.context.demand, value.demand),
                    (EvaluationDemand::Value, EvaluationDemand::Value)
                );
                assert!(contexts.insert(call.context.use_id));
                assert!(contexts.insert(value.use_id));
                assert!(domains.insert(call.context.domain));
                assert!(domains.insert(value.domain));
                assert_eq!(
                    call.effects.proof_scope,
                    CallProofScope::Domain(call.context.domain)
                );
                let node = match site {
                    PhysicalCallSite::WriterPartial { node, .. }
                    | PhysicalCallSite::WriterFinal { node, .. } => &fragment.nodes()[&node],
                    _ => unreachable!(),
                };
                let declared = writer_source(node, site).unwrap();
                assert!(fragment.values().contains_key(&declared.input));
                assert_eq!(
                    call.decimal_overflow_policy,
                    owner
                        .checked_writer_aggregate_source_observed(
                            fragment,
                            node,
                            site,
                            declared,
                            &mut CompileCheckpoints::try_new(
                                &control,
                                CompilePhase::FunctionSpecialization
                            )
                            .unwrap()
                        )
                        .unwrap()
                        .captured()
                        .binding()
                        .decimal_overflow_policy()
                );
            }
        }
        assert_eq!((partial, final_count), (2, 1));
    }
    #[test]
    fn native_writer_source_policy_refusal_and_each_original_control_prefix_are_preserved() {
        let owner = authored(&[1], &[false]);
        let mut wrong = crate::constant::test_constant_policy();
        wrong.max_rows = wrong.max_rows.checked_add(1).unwrap();
        let policy_control = Control::default();
        assert!(matches!(
            author_native_writer_preparation(&owner, wrong, &policy_control),
            Err(PackageSemanticsError::InvalidSource(
                "native original request changes its captured statement constant policy"
            ))
        ));
        let success = Control::default();
        author_native_writer_preparation(&owner, crate::constant::test_constant_policy(), &success)
            .unwrap();
        let trace = success.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let refused = Control {
                    trace: Mutex::default(),
                    refuse: Some((at, cause)),
                };
                assert!(
                    matches!(author_native_writer_preparation(&owner, crate::constant::test_constant_policy(), &refused), Err(PackageSemanticsError::Control(actual)) if actual == cause)
                );
                assert_eq!(
                    *refused.trace.lock().unwrap(),
                    trace[..=at],
                    "primary control cause has no callback or publication tail"
                );
            }
        }
    }
}
