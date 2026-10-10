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

//! Genuine original Writer DML source loans through the completed journal.
use super::*;
use novarocks_sql::compiler::{
    AggregateRuntimeDemand, SqlCallDependencyProvenance, SqlCallDependencySite,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
#[test]
fn sql_call_dependency_loan_actual_writer_statistics_retains_value_channel_and_phase() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let (owner, _catalogue) = source(mode, true, DataType::Int64, "BIGINT");
        let control = SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let mut count = 0;
        for fragment in owner.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let (partial, calls) = match &node.kind {
                    NodeKind::TableWriter { target } => (true, &target.partial_aggregates),
                    NodeKind::TableFinish(finish) => (false, &finish.final_aggregates),
                    _ => continue,
                };
                for (ordinal, actual) in calls.iter().enumerate() {
                    let site = if partial {
                        novarocks_physical_plan::PhysicalCallSite::WriterPartial {
                            node: node.id,
                            call: ordinal as u32,
                        }
                    } else {
                        novarocks_physical_plan::PhysicalCallSite::WriterFinal {
                            node: node.id,
                            call: ordinal as u32,
                        }
                    };
                    let loan = owner
                        .borrow_call_dependency_observed(
                            SqlCallDependencySite::WriterAggregate {
                                fragment,
                                node,
                                site,
                                source: actual,
                            },
                            &mut work,
                        )
                        .unwrap();
                    assert_eq!(loan.original_logical_argument_count(), 1);
                    assert_eq!(loan.original_arguments().len(), 1);
                    match loan.provenance() {
                        SqlCallDependencyProvenance::WriterAggregate {
                            source,
                            phase,
                            runtime,
                            ..
                        } => {
                            assert!(std::ptr::eq(source, actual));
                            assert_eq!(phase, actual.binding.phase);
                            if partial {
                                assert_eq!(runtime, AggregateRuntimeDemand::Update);
                            } else {
                                assert_eq!(
                                    runtime,
                                    AggregateRuntimeDemand::WriterState(actual.input)
                                );
                            }
                        }
                        _ => panic!("Writer cannot masquerade as expression state"),
                    }
                    count += 1;
                }
            }
        }
        assert!(count > 0);
        work.finish().unwrap();
    }
}
