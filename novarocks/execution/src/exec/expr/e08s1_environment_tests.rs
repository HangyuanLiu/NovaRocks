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
//! Same-journal static environment admission; no preparation or data evaluation.
use super::e08s1_environment_original_tests::{concat_source, unix_source};
use novarocks_functions::FunctionBindingError;
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    SqlCompileControl, SqlCompileError, SqlPhysicalEmissionMode, builtin_sql_function_catalog,
    check_pure_call_definitions_observed,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, SemanticParameterRef,
    SemanticParameterValue,
};
use std::sync::atomic::{AtomicUsize, Ordering};
/// Borrow the actual emitted aggregate's original logical request, including separator.
fn original_concat_binding(
    owner: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> novarocks_functions::ResolvedFunctionBinding {
    use novarocks_physical_plan::{NodeKind, PhysicalCallSite};
    use novarocks_sql::compiler::SqlCallDependencySite;
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(control, CompilePhase::Validate)
            .unwrap();
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind {
                for (ordinal, source) in calls.iter().enumerate() {
                    let loan = owner
                        .borrow_call_dependency_observed(
                            SqlCallDependencySite::Aggregate {
                                fragment,
                                node,
                                site: PhysicalCallSite::Aggregate {
                                    node: node.id,
                                    call: ordinal as u32,
                                },
                                source,
                            },
                            &mut work,
                        )
                        .unwrap();
                    let binding = loan.original_binding().resolved().clone();
                    work.finish().unwrap();
                    return binding;
                }
            }
        }
    }
    panic!("original SQL source has no aggregate declaration")
}
#[test]
fn e08s1_environment_same_original_concat_source_and_parameters_pass_without_prepare() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let source = concat_source(mode);
        let c = SqlCompileControl::unbounded();
        check_pure_call_definitions_observed(&source, &c).unwrap();
        let original = builtin_sql_function_catalog().snapshot();
        let scoped = original.snapshot_for_scalar_presence();
        let binding = original_concat_binding(&source, &c);
        let parameters = source.plan().parameters();
        let refs = parameters
            .entries()
            .iter()
            .filter(|(_, value)| {
                matches!(
                    value,
                    SemanticParameterValue::GroupConcatLegacy(_)
                        | SemanticParameterValue::GroupConcatMaxLen(_)
                )
            })
            .map(|(id, value)| SemanticParameterRef {
                id: *id,
                expected_key: value.key(),
            })
            .collect::<Vec<_>>();
        assert_eq!(refs.len(), 2);
        scoped
            .admit_bound_environment_observed(&binding, &refs, parameters, &c)
            .unwrap();
        for invalid in [Vec::new(), vec![refs[0]], vec![refs[0], refs[0]]] {
            assert!(
                matches!(scoped.admit_bound_environment_observed(&binding,&invalid,parameters,&c),Err(FunctionBindingError::UnavailableImplementation(id)) if id==binding.selected.overload)
            );
            original
                .admit_bound_environment_observed(&binding, &invalid, parameters, &c)
                .unwrap();
        }
    }
}
#[test]
fn e08s1_environment_same_journal_absent_original_unixtime_zone_is_named_before_task() {
    let source = unix_source();
    let error =
        check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap_err();
    let SqlCompileError::Analyze(error) = error else {
        panic!("typed environment support refusal: {error}")
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
    assert!(error.message().contains("builtin.scalar/from_unixtime/"));
    assert_eq!(
        source.emission_mode(),
        SqlPhysicalEmissionMode::OriginalNativeV1,
        "inspection does not alter original source or execute a task"
    );
}
struct Refuse {
    cause: CompileControlError,
    calls: AtomicUsize,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "no callback after first cause"
        );
        Err(self.cause)
    }
}
#[test]
fn e08s1_environment_contract_frontier_preserves_three_compile_causes_and_original_no_work() {
    let source = concat_source(SqlPhysicalEmissionMode::OriginalNativeV1);
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    let binding = original_concat_binding(&source, &c);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let c = Refuse {
            cause,
            calls: AtomicUsize::new(0),
        };
        original
            .admit_bound_environment_observed(&binding, &[], source.plan().parameters(), &c)
            .unwrap();
        assert_eq!(c.calls.load(Ordering::SeqCst), 0);
        assert!(
            matches!(scoped.admit_bound_environment_observed(&binding,&[],source.plan().parameters(),&c),Err(FunctionBindingError::Control(actual)) if actual==cause)
        );
        assert_eq!(c.calls.load(Ordering::SeqCst), 1);
    }
}
