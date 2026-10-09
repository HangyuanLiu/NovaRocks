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

//! Test-support loans through the actual original aggregate request/state author.
use super::physical_aggregate_requests::{
    author_physical_aggregate_merge_request_observed,
    author_physical_aggregate_update_request_from_journal_observed,
};
use crate::compiler::SqlAuthoredPhysicalPlan;
use arrow::array::{Array, Int64Array};
use novarocks_functions::FunctionArgument;
use novarocks_physical_plan::{AggregatePhase, NodeKind, PhysicalCallSite};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

#[derive(Debug, Eq, PartialEq)]
pub struct OrdinaryStateObservation {
    pub partials: usize,
    pub intermediates: usize,
    pub finals: usize,
    pub final_consumer_constant: Option<i64>,
    pub final_producer_constants: Vec<Option<i64>>,
    pub final_independent_lineages: usize,
}
fn constant(arguments: &[FunctionArgument]) -> Option<i64> {
    match arguments.first() {
        Some(FunctionArgument::Value {
            constant: Some(value),
            ..
        }) => {
            let array = value
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the actual SUM fixture authors Int64 constants");
            let row = usize::try_from(value.ordinal()).unwrap();
            assert!(!array.is_null(row));
            Some(array.value(row))
        }
        _ => None,
    }
}
/// This is an unbounded fixture observation, not a typed control-fault adapter.
/// Every phase is checked by its original request author. The final's actual
/// source iterator supplies order and independent source identity unchanged.
pub fn ordinary_state_observe_for_test(
    owner: &SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> Result<OrdinaryStateObservation, String> {
    let mut observed = OrdinaryStateObservation {
        partials: 0,
        intermediates: 0,
        finals: 0,
        final_consumer_constant: None,
        final_producer_constants: Vec::new(),
        final_independent_lineages: 0,
    };
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(|e| format!("{e:?}"))?;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let site = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: u32::try_from(ordinal).unwrap(),
                };
                let entry = owner
                    .checked_aggregate_source_observed(fragment, node, site, call, &mut work)
                    .map_err(|e| format!("{e:?}"))?;
                if call.binding.phase.consumes_logical_arguments() {
                    author_physical_aggregate_update_request_from_journal_observed(
                        &entry, &mut work,
                    )
                    .map_err(|e| format!("original update author: {e:?}"))?;
                    observed.partials += 1;
                    continue;
                }
                let request = author_physical_aggregate_merge_request_observed(&entry, &mut work)
                    .map_err(|e| format!("original merge author: {e:?}"))?;
                if matches!(call.binding.phase, AggregatePhase::Intermediate { .. }) {
                    observed.intermediates += 1;
                    continue;
                }
                assert!(matches!(call.binding.phase, AggregatePhase::Final { .. }));
                observed.finals += 1;
                observed.final_consumer_constant = constant(request.request().arguments);
                request
                    .state_inputs()
                    .visit_observed(
                        &mut work,
                        |producer, _, work| {
                            observed
                                .final_producer_constants
                                .push(constant(producer.captured().request().arguments));
                            if !producer
                                .captured()
                                .logical_identity()
                                .same_lineage(entry.captured().logical_identity())
                            {
                                observed.final_independent_lineages += 1;
                            }
                            work.step()?;
                            Ok(())
                        },
                        |_, _, _, _| Ok(()),
                    )
                    .map_err(|e| format!("{e:?}"))?;
            }
        }
    }
    work.finish().map_err(|e| format!("{e:?}"))?;
    Ok(observed)
}
