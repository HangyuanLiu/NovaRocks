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
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Mutex};

struct Control {
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<CompileControlError>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.observations.lock().unwrap().push((phase, units));
        if units > 0
            && let Some(failure) = self.fail
        {
            return Err(failure);
        }
        Ok(())
    }
}
fn control() -> Control {
    Control {
        observations: Mutex::default(),
        fail: None,
    }
}
fn expr() -> ExprId {
    ExprId::new(u32::MAX)
}
fn alternate() -> ExprId {
    ExprId::new(1000)
}
fn node() -> NodeId {
    NodeId::new(91)
}
// This fixture isolates the root-field projection. Full relational validity
// is exercised by the real FragmentBuilder and plan-contract suites.
fn fragment(kind: NodeKind) -> Fragment {
    let mut expressions = ExprArena::default();
    for id in [expr(), alternate()] {
        expressions.insert(ExprNode {
            id,
            owner: node(),
            lambda_scope: None,
            ty: ty(DataType::Boolean, false),
            kind: ExprKind::Literal(LiteralValue::Boolean(true)),
        });
    }
    FragmentParts {
        id: FragmentId::new(19),
        root: node(),
        values: BTreeMap::new(),
        expressions,
        nodes: BTreeMap::from([(
            node(),
            PhysicalNode {
                id: node(),
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton(),
                output: OutputPort {
                    node: node(),
                    columns: Box::default(),
                },
                kind,
            },
        )]),
        sink: FragmentSink::Noop,
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        runtime_filters: Box::default(),
    }
    .into()
}
fn site(role: ExpressionRootRole) -> ExpressionRootSite {
    ExpressionRootSite { node: node(), role }
}
fn uses(fragment: &Fragment) -> PhysicalExpressionRoots {
    PhysicalExpressionRoots::try_new(fragment, &control()).unwrap()
}
fn graph(
    fragment: &Fragment,
    definitions: &[(u32, ExprId, EvaluationDemand)],
) -> ExpressionControlFlow<ExprId> {
    let domain = EvaluationDomainId::new(u32::MAX);
    ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        definitions
            .iter()
            .map(|(id, definition, demand)| ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(*id),
                    domain,
                    demand: *demand,
                },
                definition: *definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect(),
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap()
}

#[test]
fn exact_field_sites_keep_repeated_definitions_and_join_demands() {
    use EvaluationDemand::{TruthOnly, Value};
    use ExpressionRootRole::*;
    let filter = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    assert_eq!(
        uses(&filter).sites(),
        &BTreeMap::from([
            (
                site(FilterPredicate { predicate: 0 }),
                ExprUse {
                    expr: expr(),
                    demand: TruthOnly
                }
            ),
            (
                site(FilterPredicate { predicate: 1 }),
                ExprUse {
                    expr: expr(),
                    demand: TruthOnly
                }
            )
        ])
    );
    let project = fragment(NodeKind::Project {
        expressions: Box::from([(expr(), ValueId::new(0)), (expr(), ValueId::new(1))]),
    });
    assert_eq!(uses(&project).sites().len(), 2);
    assert!(
        uses(&project)
            .sites()
            .values()
            .all(|root| root.demand == Value)
    );
    for (kind, demand) in [
        (JoinKind::Inner, TruthOnly),
        (JoinKind::NullAwareLeftAnti, Value),
    ] {
        let join = fragment(NodeKind::HashJoin {
            kind,
            keys: Box::from([crate::JoinKey {
                left: expr(),
                right: expr(),
                null_safe: false,
            }]),
            build_side: JoinSide::Right,
            distribution: JoinDistribution::BroadcastBuild,
            residual: Some(expr()),
            null_extended: Box::default(),
        });
        assert_eq!(
            uses(&join).sites(),
            &BTreeMap::from([
                (
                    site(JoinKey {
                        key: 0,
                        side: JoinSide::Left
                    }),
                    ExprUse {
                        expr: expr(),
                        demand: Value
                    }
                ),
                (
                    site(JoinKey {
                        key: 0,
                        side: JoinSide::Right
                    }),
                    ExprUse {
                        expr: expr(),
                        demand: Value
                    }
                ),
                (
                    site(HashJoinResidual),
                    ExprUse {
                        expr: expr(),
                        demand
                    }
                )
            ])
        );
    }
}

#[test]
fn optional_and_non_scalar_fields_keep_original_multidimensional_positions() {
    use ExpressionRootRole::*;
    let changes = fragment(NodeKind::ChangeEventExpand {
        events: Box::from([
            ChangeEventSpec {
                predicate: None,
                effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                assignments: Box::from([(ValueId::new(0), None), (ValueId::new(1), Some(expr()))]),
            },
            ChangeEventSpec {
                predicate: Some(alternate()),
                effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                assignments: Box::default(),
            },
        ]),
        effect_output: ValueId::new(9),
    });
    let roots = uses(&changes);
    assert_eq!(roots.sites().len(), 2);
    assert!(roots.sites().contains_key(&site(ChangeAssignment {
        event: 0,
        assignment: 1
    })));
    assert_eq!(
        roots.sites()[&site(ChangePredicate { event: 1 })].expr,
        alternate()
    );
    let unpivot = fragment(NodeKind::Unpivot {
        spec: UnpivotSpec {
            passthrough: Box::default(),
            value_output: ValueId::new(0),
            literal_outputs: Box::default(),
            mappings: Box::from([UnpivotValueMapping {
                input: ValueId::new(1),
                constants: Box::from([
                    crate::UnpivotConstant::Int32List(Box::from([1])),
                    crate::UnpivotConstant::Scalar(expr()),
                ]),
            }]),
            max_output_rows: 64,
            max_output_bytes: 1024,
        },
    });
    assert_eq!(
        uses(&unpivot).sites().keys().copied().collect::<Vec<_>>(),
        vec![site(UnpivotConstant {
            mapping: 0,
            constant: 1
        })]
    );
    let cells = fragment(NodeKind::Values {
        rows: Box::from([Box::default(), Box::from([expr(), alternate()])]),
    });
    assert_eq!(
        uses(&cells).sites().keys().copied().collect::<Vec<_>>(),
        vec![
            site(ValuesCell { row: 1, column: 0 }),
            site(ValuesCell { row: 1, column: 1 })
        ]
    );
}

#[test]
fn root_binding_requires_exact_complete_coverage_definition_and_demand() {
    use EvaluationDemand::{TruthOnly, Value};
    use ExpressionRootRole::*;
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let roots = uses(&fragment);
    let bindings = vec![
        (
            site(FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(1000),
        ),
        (
            site(FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    let flow = graph(
        &fragment,
        &[(1000, expr(), TruthOnly), (u32::MAX, expr(), TruthOnly)],
    );
    let bound =
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), bindings.clone(), &control())
            .unwrap();
    assert_eq!(bound.bindings().len(), 2);
    assert_eq!(bound.flow(), &flow);
    assert_eq!(
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), vec![], &control()).unwrap_err(),
        RootUseBindingError::IncompleteCoverage
    );
    let mut shared = bindings.clone();
    shared[1].1 = shared[0].1;
    assert_eq!(
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), shared, &control()).unwrap_err(),
        RootUseBindingError::SharedUse
    );
    let mut duplicate = bindings.clone();
    duplicate[1].0 = duplicate[0].0;
    assert_eq!(
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), duplicate, &control()).unwrap_err(),
        RootUseBindingError::DuplicateSite
    );
    let mut wrong_site = bindings.clone();
    wrong_site[0].0.role = ProjectOutput { expression: 0 };
    assert_eq!(
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), wrong_site, &control()).unwrap_err(),
        RootUseBindingError::InvalidSite
    );
    let mut wrong_use = bindings.clone();
    wrong_use[0].1 = ExpressionUseId::new(91);
    assert_eq!(
        PhysicalRootUses::try_new(roots.clone(), flow.clone(), wrong_use, &control()).unwrap_err(),
        RootUseBindingError::InvalidUse
    );
    let wrong_definition = graph(
        &fragment,
        &[
            (1000, alternate(), TruthOnly),
            (u32::MAX, expr(), TruthOnly),
        ],
    );
    assert_eq!(
        PhysicalRootUses::try_new(
            roots.clone(),
            wrong_definition,
            bindings.clone(),
            &control()
        )
        .unwrap_err(),
        RootUseBindingError::WrongDefinition
    );
    let wrong_demand = graph(
        &fragment,
        &[(1000, expr(), Value), (u32::MAX, expr(), TruthOnly)],
    );
    assert_eq!(
        PhysicalRootUses::try_new(roots, wrong_demand, bindings, &control()).unwrap_err(),
        RootUseBindingError::WrongDemand
    );
}

#[test]
fn empty_fields_observe_typed_control_failure_before_unbounded_traversal() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let fragment = fragment(NodeKind::Values {
            rows: vec![Box::default(); 1000].into_boxed_slice(),
        });
        let owner = Control {
            observations: Mutex::default(),
            fail: Some(failure),
        };
        assert_eq!(
            PhysicalExpressionRoots::try_new(&fragment, &owner).unwrap_err(),
            ExpressionRootError::Control(failure)
        );
        assert_eq!(
            *owner.observations.lock().unwrap(),
            vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 256)]
        );
    }
}

#[test]
fn scan_derived_roots_cover_real_value_origins_and_exclude_provider_proofs() {
    let proof_only = finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 0).unwrap();
    assert!(uses(&proof_only).sites().is_empty());
    let rechecked = finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 1).unwrap();
    let roots = uses(&rechecked);
    assert_eq!(roots.sites().len(), 1);
    assert_eq!(
        roots.sites().keys().next().unwrap().role,
        ExpressionRootRole::ScanResidual { predicate: 0 }
    );
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 9),
    };
    let make = |origin: ValueOrigin| {
        let source = fragment(NodeKind::Scan {
            occurrence: ProviderReadOccurrenceId::new(1),
            relation: Box::new(metadata_relation(&binding, column.clone())),
            read_budget: scan_budget(),
            provider_outputs: Box::default(),
            residuals: Box::from([expr()]),
            derived_values: Box::from([ValueId::new(5)]),
        });
        Fragment::from(FragmentParts {
            id: source.id(),
            root: source.root(),
            values: BTreeMap::from([(
                ValueId::new(5),
                ValueDef {
                    id: ValueId::new(5),
                    ty: ty(DataType::Boolean, false),
                    origin,
                },
            )]),
            expressions: source.expressions().clone(),
            nodes: source.nodes().clone(),
            sink: source.sink().clone(),
            dop_domain: source.dop_domain(),
            runtime_filters: source.runtime_filters().into(),
        })
    };
    let scan = make(ValueOrigin::Expr {
        node: node(),
        expr: alternate(),
    });
    assert_eq!(uses(&scan).sites().len(), 2);
    assert_eq!(
        uses(&scan).sites()[&site(ExpressionRootRole::ScanDerived { derived: 0 })],
        ExprUse {
            expr: alternate(),
            demand: EvaluationDemand::Value
        }
    );
    let wrong_origin = make(ValueOrigin::NodeOutput {
        node: node(),
        output_ordinal: 0,
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&wrong_origin, &control()).unwrap_err(),
        ExpressionRootError::InvalidDerivedValue
    );
    let wrong_owner = make(ValueOrigin::Expr {
        node: NodeId::new(92),
        expr: alternate(),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&wrong_owner, &control()).unwrap_err(),
        ExpressionRootError::InvalidDerivedValue
    );
    let absent_expression = make(ValueOrigin::Expr {
        node: node(),
        expr: ExprId::new(2),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&absent_expression, &control()).unwrap_err(),
        ExpressionRootError::InvalidExpressionOwner
    );
}

#[test]
fn root_count_has_the_shared_near_and_over_occurrence_bounds() {
    let count = novarocks_type_contract::MAX_CONTROL_USE_REFERENCES;
    let near = fragment(NodeKind::Values {
        rows: Box::from([vec![expr(); count].into_boxed_slice()]),
    });
    assert_eq!(uses(&near).sites().len(), count);
    let over = fragment(NodeKind::Values {
        rows: Box::from([vec![expr(); count + 1].into_boxed_slice()]),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&over, &control()).unwrap_err(),
        ExpressionRootError::TooManyRoots
    );
}

#[test]
fn an_argument_use_cannot_replace_an_operator_root() {
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let domain = EvaluationDomainId::new(0);
    let invoke = |id, arguments| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::TruthOnly,
        },
        definition: expr(),
        control: ControlShape::Conjunction,
        arguments,
    };
    let mut child = invoke(91, Box::default());
    child.control = ControlShape::Eager;
    let mut other = invoke(u32::MAX, Box::default());
    other.control = ControlShape::Eager;
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invoke(1000, Box::from([ExpressionUseId::new(91)])),
            child,
            other,
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let bindings = vec![
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(91),
        ),
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    assert_eq!(
        PhysicalRootUses::try_new(uses(&fragment), flow, bindings, &control()).unwrap_err(),
        RootUseBindingError::InvalidUse
    );
}

#[test]
fn operator_roots_cannot_escape_from_a_lambda_lexical_scope() {
    let source = fragment(NodeKind::Project {
        expressions: Box::from([(expr(), ValueId::new(0))]),
    });
    let mut definitions = source.expressions().clone();
    let mut nested = definitions.get(expr()).unwrap().clone();
    nested.lambda_scope = Some(alternate());
    definitions.insert(nested);
    let fragment = Fragment::from(FragmentParts {
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions: definitions,
        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().into(),
    });
    assert_eq!(
        PhysicalExpressionRoots::try_new(&fragment, &control()).unwrap_err(),
        ExpressionRootError::InvalidExpressionScope
    );
}

#[test]
fn a_guarded_disconnected_use_cannot_become_an_operator_root() {
    use novarocks_type_contract::{DomainGuard, GuardKind};
    let fragment = fragment(NodeKind::Filter {
        predicates: Box::from([expr(), expr()]),
    });
    let domain = EvaluationDomainId::new(0);
    let branch = EvaluationDomainId::new(1);
    let otherwise = EvaluationDomainId::new(2);
    let invoke = |id, domain| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::TruthOnly,
        },
        definition: expr(),
        control: ControlShape::Eager,
        arguments: Box::default(),
    };
    let mut owner = invoke(1000, domain);
    owner.control = ControlShape::If;
    owner.arguments = Box::from([
        ExpressionUseId::new(91),
        ExpressionUseId::new(92),
        ExpressionUseId::new(93),
    ]);
    let guarded = |id, kind| ExpressionEvaluationDomain {
        id,
        parent: Some(domain),
        guard: Some(DomainGuard {
            owner: owner.context.use_id,
            kind,
        }),
    };
    let flow = ExpressionControlFlow::try_new(
        vec![
            ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            },
            guarded(branch, GuardKind::IfThen),
            guarded(otherwise, GuardKind::IfElse),
        ],
        vec![
            owner,
            invoke(91, domain),
            invoke(92, branch),
            invoke(93, otherwise),
            invoke(u32::MAX, branch),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let bindings = vec![
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 0 }),
            ExpressionUseId::new(1000),
        ),
        (
            site(ExpressionRootRole::FilterPredicate { predicate: 1 }),
            ExpressionUseId::new(u32::MAX),
        ),
    ];
    assert_eq!(
        PhysicalRootUses::try_new(uses(&fragment), flow, bindings, &control()).unwrap_err(),
        RootUseBindingError::GuardedRoot
    );
}
