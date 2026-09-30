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

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use novarocks_connector_contract::{
    ConnectorReadRelationRecipeDraft, ConnectorValueType, FrozenConnectorRead, FrozenConnectorScan,
    StaticScanAssignment, TupleDomain,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileControlError, CompilePhase, ControlShape,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, PureCompileControl, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
};

use super::*;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn fixture_expression_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let definition = fragment.expressions().get(root.expr).unwrap();
        match &definition.kind {
            ExprKind::Literal(_) | ExprKind::Value(_) => {}
            ExprKind::FunctionCall { function, args } => {
                // This fixture declares only the exact zero-argument Eager
                // call below; legacy bits are never a general control fallback.
                assert_eq!(function.function_id.as_str(), "test.parameter");
                assert_eq!(function.overload.as_str(), "test.parameter.zero");
                assert_eq!(
                    function.argument_evaluation,
                    FunctionArgumentEvaluation::Eager
                );
                assert!(function.argument_types.is_empty());
                assert!(args.is_empty());
            }
            other => panic!("fixture requires explicit control for {other:?}"),
        }
        let id = ExpressionUseId::new(ordinal as u32);
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control).unwrap()
}

fn fixture_controls(plan: &PhysicalPlan) -> BTreeMap<FragmentId, PhysicalRootUses> {
    plan.fragments()
        .iter()
        .map(|(id, fragment)| (*id, fixture_expression_uses(fragment)))
        .collect()
}

// The selected test.parameter.zero fixture reads its exact frozen timezone
// reference and returns its bounded byte length as a non-NULL Int64. These
// are explicit fixture-owner
// facts, not effects inferred from a general function's legacy four fields.
fn parameter_fixture_effects(reference: SemanticParameterRef) -> CallEffects {
    assert_eq!(reference.expected_key, SemanticParameterKey::TimeZone);
    CallEffects {
        value_stability: FunctionVolatility::Stable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment: Box::from([reference]),
        proof_scope: CallProofScope::Unconditional,
    }
}

fn fixture_calls(fragment: &Fragment, uses: &PhysicalRootUses) -> FrozenFragmentCalls {
    let mut calls = Vec::new();
    for (id, invocation) in uses.flow().uses() {
        if let ExprKind::FunctionCall { function, args } = &fragment
            .expressions()
            .get(invocation.definition)
            .unwrap()
            .kind
        {
            assert_eq!(function.function_id.as_str(), "test.parameter");
            assert_eq!(function.overload.as_str(), "test.parameter.zero");
            assert_eq!(function.kind, FunctionKind::Scalar);
            assert_eq!(function.result_type, ty(DataType::Int64, false));
            assert!(args.is_empty());
            assert!(function.argument_types.is_empty());
            let [reference] = function.semantic_parameters.as_ref() else {
                panic!("the parameter fixture requires its selected timezone reference");
            };
            calls.push(FrozenPhysicalCall {
                site: PhysicalCallSite::Expression(*id),
                context: invocation.context,
                effects: parameter_fixture_effects(*reference),
            });
        }
    }
    FrozenFragmentCalls::try_new(fragment, uses, calls, &Control).unwrap()
}

fn fixture_call_tables(
    plan: &PhysicalPlan,
    controls: &BTreeMap<FragmentId, PhysicalRootUses>,
) -> BTreeMap<FragmentId, FrozenFragmentCalls> {
    plan.fragments()
        .iter()
        .map(|(id, fragment)| (*id, fixture_calls(fragment, &controls[id])))
        .collect()
}

fn extract(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    parameters: &SemanticParameters,
    writes: &BTreeMap<WriteTargetOrdinal, novarocks_connector_contract::ConnectorWriteRecipeDraft>,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    let controls = fixture_controls(plan);
    let calls = fixture_call_tables(plan, &controls);
    extract_fragment_packages(plan, scans, parameters, writes, &controls, &calls, &Control)
}

fn package_input(fragment: Fragment) -> FragmentPackageInput {
    let expression_uses = fixture_expression_uses(&fragment);
    let calls = fixture_calls(&fragment, &expression_uses);
    package_input_with_controls(fragment, expression_uses, calls)
}

fn package_input_with_controls(
    fragment: Fragment,
    expression_uses: PhysicalRootUses,
    calls: FrozenFragmentCalls,
) -> FragmentPackageInput {
    FragmentPackageInput {
        version: version(),
        required: RequiredContracts::default(),
        expression_uses,
        calls,
        fragment,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}

#[test]
fn extraction_requires_exact_expression_control_fragment_coverage() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = builder.finish().unwrap();
    let controls = fixture_controls(&plan);
    let calls = fixture_call_tables(&plan, &controls);
    let parameters = SemanticParameters::default();
    let extract_with = |controls: &BTreeMap<FragmentId, PhysicalRootUses>| {
        extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &parameters,
            &BTreeMap::new(),
            controls,
            &calls,
            &Control,
        )
    };
    let packages = extract_with(&controls).unwrap();
    assert_eq!(
        packages[&fragment.id()].expression_uses(),
        &controls[&fragment.id()]
    );
    assert_eq!(
        extract_with(&BTreeMap::new()).unwrap_err(),
        FragmentPackageExtractionError::MissingExpressionUses(fragment.id())
    );
    let (extra_fragment, _) = literal_fragment(FragmentId::new(82), FragmentSink::Noop, false);
    let mut extra = controls;
    extra.insert(
        extra_fragment.id(),
        fixture_expression_uses(&extra_fragment),
    );
    assert_eq!(
        extract_with(&extra).unwrap_err(),
        FragmentPackageExtractionError::UnusedExpressionUses
    );
}

fn binary_control_fixture() -> (Fragment, PhysicalRootUses, ExprId, ExprId, ExprId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(83));
    let node = builder.reserve_node_id().unwrap();
    let left = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(11)),
        )
        .unwrap();
    let right = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(22)),
        )
        .unwrap();
    let root = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Binary {
                left,
                op: BinaryOperator::Subtract,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        )
        .unwrap();
    let outputs = (0..2)
        .map(|ordinal| {
            builder
                .add_value(
                    ty(DataType::Int64, false),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: outputs.into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([root, left])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap();
    let domain = EvaluationDomainId::new(0);
    let invocation = |id, definition, arguments| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::Value,
        },
        definition,
        control: ControlShape::Eager,
        arguments,
    };
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invocation(
                0,
                root,
                Box::from([ExpressionUseId::new(1), ExpressionUseId::new(2)]),
            ),
            invocation(1, left, Box::default()),
            invocation(2, right, Box::default()),
            invocation(3, left, Box::default()),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                ExpressionUseId::new(0),
            ),
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                ExpressionUseId::new(3),
            ),
        ],
        &Control,
    )
    .unwrap();
    (fragment, uses, root, left, right)
}

#[test]
fn package_rechecks_ordered_children_intrinsic_control_and_roots_with_reused_ids() {
    let (original, uses, root, left, right) = binary_control_fixture();
    let calls = fixture_calls(&original, &uses);
    FragmentPackage::try_new(
        package_input_with_controls(original.clone(), uses.clone(), calls.clone()),
        &Control,
    )
    .unwrap();
    for mutation in 0..3 {
        let mut expressions = original.expressions().clone();
        let mut nodes = original.nodes().clone();
        let mut values = original.values().clone();
        let expected = match mutation {
            0 => {
                let mut root_definition = expressions.get(root).unwrap().clone();
                root_definition.kind = ExprKind::Binary {
                    left: right,
                    op: BinaryOperator::Subtract,
                    right: left,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                };
                expressions.insert(root_definition);
                RootUseBindingError::WrongArguments
            }
            1 => {
                let mut condition = expressions.get(left).unwrap().clone();
                condition.ty = ty(DataType::Boolean, false);
                condition.kind = ExprKind::Literal(LiteralValue::Boolean(true));
                expressions.insert(condition);
                let mut root_definition = expressions.get(root).unwrap().clone();
                root_definition.ty = ty(DataType::Int64, true);
                root_definition.kind = ExprKind::Case {
                    operand: None,
                    when_then: Box::from([(left, right)]),
                    else_expr: None,
                };
                expressions.insert(root_definition);
                let outputs = &nodes[&original.root()].output.columns;
                values.get_mut(&outputs[0]).unwrap().ty = ty(DataType::Int64, true);
                values.get_mut(&outputs[1]).unwrap().ty = ty(DataType::Boolean, false);
                RootUseBindingError::WrongControl
            }
            _ => {
                let NodeKind::Values { rows } = &mut nodes.get_mut(&original.root()).unwrap().kind
                else {
                    unreachable!();
                };
                rows[0].swap(0, 1);
                RootUseBindingError::ChangedRoots
            }
        };
        let changed = Fragment::from(FragmentParts {
            id: original.id(),
            root: original.root(),
            values,
            expressions,
            nodes,
            sink: original.sink().clone(),
            dop_domain: original.dop_domain(),
            runtime_filters: original.runtime_filters().into(),
        });
        validate_fragment_definition(&changed).unwrap();
        assert_eq!(changed.id(), original.id());
        assert_eq!(changed.expressions().len(), original.expressions().len());
        assert_eq!(
            FragmentPackage::try_new(
                package_input_with_controls(changed, uses.clone(), calls.clone()),
                &Control,
            )
            .unwrap_err(),
            FragmentPackageError::ExpressionUses(expected)
        );
    }
}

struct FailureControl {
    failure: CompileControlError,
    positive_only: bool,
    work: std::sync::Mutex<Vec<u32>>,
}
impl PureCompileControl for FailureControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        if !self.positive_only || units > 0 {
            Err(self.failure)
        } else {
            Ok(())
        }
    }
}

#[test]
fn package_and_extraction_keep_typed_control_failures_before_and_during_validation() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let input = package_input(fragment.clone());
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish().unwrap();
    let controls = fixture_controls(&plan);
    let calls = fixture_call_tables(&plan, &controls);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = FailureControl {
                failure,
                positive_only,
                work: std::sync::Mutex::default(),
            };
            assert_eq!(
                FragmentPackage::try_new(input.clone(), &control).unwrap_err(),
                FragmentPackageError::Control(failure)
            );
            if positive_only {
                assert!(control.work.lock().unwrap().iter().any(|units| *units > 0));
            }
            let control = FailureControl {
                failure,
                positive_only,
                work: std::sync::Mutex::default(),
            };
            let expected = FragmentPackageExtractionError::Control(failure);
            assert_eq!(
                extract_fragment_packages(
                    &plan,
                    &BTreeMap::new(),
                    &SemanticParameters::default(),
                    &BTreeMap::new(),
                    &controls,
                    &calls,
                    &control
                )
                .unwrap_err(),
                expected
            );
            if positive_only {
                assert!(control.work.lock().unwrap().iter().any(|units| *units > 0));
            }
        }
    }
}

#[test]
fn package_control_checks_correspondence_without_claiming_literal_content_identity() {
    let (original, _) = literal_fragment(FragmentId::new(84), FragmentSink::Noop, false);
    let checked = fixture_expression_uses(&original);
    let calls = fixture_calls(&original, &checked);
    let mut expressions = original.expressions().clone();
    let NodeKind::Values { rows } = &original.nodes()[&original.root()].kind else {
        unreachable!();
    };
    let mut literal = expressions.get(rows[0][0]).unwrap().clone();
    assert!(matches!(
        literal.kind,
        ExprKind::Literal(LiteralValue::Int64(11))
    ));
    literal.kind = ExprKind::Literal(LiteralValue::Int64(22));
    expressions.insert(literal);
    let changed = Fragment::from(FragmentParts {
        id: original.id(),
        root: original.root(),
        values: original.values().clone(),
        expressions,
        nodes: original.nodes().clone(),
        sink: original.sink().clone(),
        dop_domain: original.dop_domain(),
        runtime_filters: original.runtime_filters().into(),
    });
    validate_fragment_definition(&changed).unwrap();
    let package = FragmentPackage::try_new(
        package_input_with_controls(changed.clone(), checked.clone(), calls.clone()),
        &Control,
    )
    .unwrap();
    assert_eq!(package.fragment(), &changed);
    assert_eq!(package.expression_uses(), &checked);

    let (other, _) = literal_fragment(FragmentId::new(85), FragmentSink::Noop, false);
    assert_eq!(
        FragmentPackage::try_new(package_input_with_controls(other, checked, calls), &Control)
            .unwrap_err(),
        FragmentPackageError::ExpressionUses(RootUseBindingError::WrongFragment)
    );
}

#[test]
fn package_preserves_actual_case_branch_guards_and_root_occurrences() {
    use novarocks_type_contract::{DomainGuard, GuardKind};
    let (original, _, root, left, right) = binary_control_fixture();
    let mut expressions = original.expressions().clone();
    let mut condition = expressions.get(left).unwrap().clone();
    condition.ty = ty(DataType::Boolean, false);
    condition.kind = ExprKind::Literal(LiteralValue::Boolean(true));
    expressions.insert(condition);
    let mut result = expressions.get(root).unwrap().clone();
    result.ty = ty(DataType::Int64, true);
    result.kind = ExprKind::Case {
        operand: None,
        when_then: Box::from([(left, right)]),
        else_expr: None,
    };
    expressions.insert(result);
    let mut values = original.values().clone();
    let node = original.root();
    let outputs = &original.nodes()[&node].output.columns;
    values.get_mut(&outputs[0]).unwrap().ty = ty(DataType::Int64, true);
    values.get_mut(&outputs[1]).unwrap().ty = ty(DataType::Boolean, false);
    let fragment = Fragment::from(FragmentParts {
        id: original.id(),
        root: node,
        values,
        expressions,
        nodes: original.nodes().clone(),
        sink: original.sink().clone(),
        dop_domain: original.dop_domain(),
        runtime_filters: original.runtime_filters().into(),
    });
    validate_fragment_definition(&fragment).unwrap();
    let domain = EvaluationDomainId::new(0);
    let when = EvaluationDomainId::new(1);
    let then = EvaluationDomainId::new(2);
    let invocation = |id, definition, domain, demand, control, arguments| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand,
        },
        definition,
        control,
        arguments,
    };
    let flow = ExpressionControlFlow::try_new(
        vec![
            ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            },
            ExpressionEvaluationDomain {
                id: when,
                parent: Some(domain),
                guard: Some(DomainGuard {
                    owner: ExpressionUseId::new(0),
                    kind: GuardKind::CaseWhen { arm: 0 },
                }),
            },
            ExpressionEvaluationDomain {
                id: then,
                parent: Some(domain),
                guard: Some(DomainGuard {
                    owner: ExpressionUseId::new(0),
                    kind: GuardKind::CaseThen { arm: 0 },
                }),
            },
        ],
        vec![
            invocation(
                0,
                root,
                domain,
                EvaluationDemand::Value,
                ControlShape::Case {
                    simple: false,
                    arms: 1,
                    has_else: false,
                },
                Box::from([ExpressionUseId::new(1), ExpressionUseId::new(2)]),
            ),
            invocation(
                1,
                left,
                when,
                EvaluationDemand::TruthOnly,
                ControlShape::Eager,
                Box::default(),
            ),
            invocation(
                2,
                right,
                then,
                EvaluationDemand::Value,
                ControlShape::Eager,
                Box::default(),
            ),
            invocation(
                3,
                left,
                domain,
                EvaluationDemand::Value,
                ControlShape::Eager,
                Box::default(),
            ),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(
        &fragment,
        flow.clone(),
        vec![
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                ExpressionUseId::new(0),
            ),
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                ExpressionUseId::new(3),
            ),
        ],
        &Control,
    )
    .unwrap();
    let calls = fixture_calls(&fragment, &uses);
    let package = FragmentPackage::try_new(
        package_input_with_controls(fragment, uses.clone(), calls),
        &Control,
    )
    .unwrap();
    assert_eq!(package.expression_uses(), &uses);
    assert_eq!(package.expression_uses().flow(), &flow);
    assert_eq!(
        package.expression_uses().flow().domains()[&when]
            .guard
            .unwrap()
            .kind,
        GuardKind::CaseWhen { arm: 0 }
    );
    assert_eq!(
        package.expression_uses().flow().domains()[&then]
            .guard
            .unwrap()
            .kind,
        GuardKind::CaseThen { arm: 0 }
    );
}

fn frozen_scan(fragment: &Fragment) -> FrozenConnectorRead {
    let NodeKind::Scan {
        relation,
        read_budget,
        ..
    } = &fragment.nodes()[&fragment.root()].kind
    else {
        unreachable!()
    };
    let read = relation.read();
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        vec![StaticScanAssignment::new(
            Arc::from("v"),
            ConnectorValueType::BigInt,
        )],
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
        NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
        relation.work_source(),
    )
    .unwrap();
    public_read(fragment, fragment.root(), scan).unwrap()
}

fn public_read(
    fragment: &Fragment,
    scan_id: NodeId,
    scan: FrozenConnectorScan,
) -> Result<FrozenConnectorRead, novarocks_connector_contract::ConnectorError> {
    use novarocks_connector_contract::*;
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &fragment.nodes()[&scan_id].kind
    else {
        unreachable!()
    };
    let properties = relation.provided_properties();
    let distribution = match properties.distribution {
        Distribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
        Distribution::Singleton => ConnectorReadDistribution::Singleton,
        Distribution::RoundRobin => ConnectorReadDistribution::RoundRobin,
        _ => unreachable!("fixture uses no provider partitioned guarantee"),
    };
    let ordering = properties
        .ordering
        .iter()
        .map(|key| {
            ConnectorReadOrderingKey::new(
                ScanColumnId::new(
                    provider_outputs
                        .iter()
                        .position(|(_, value)| *value == key.value)
                        .unwrap(),
                ),
                match key.direction {
                    SortDirection::Ascending => ConnectorReadSortDirection::Ascending,
                    SortDirection::Descending => ConnectorReadSortDirection::Descending,
                },
                match key.null_ordering {
                    NullOrdering::First => ConnectorReadNullOrdering::First,
                    NullOrdering::Last => ConnectorReadNullOrdering::Last,
                },
            )
        })
        .collect::<Vec<_>>();
    let (kind, coverage) = match relation.as_ref() {
        Relation::Data(_) => (None, vec![]),
        Relation::Metadata(metadata) => (
            Some(ConnectorReadMetadataKind::try_new(metadata.kind.as_str())?),
            metadata.coverage_evidence.to_vec(),
        ),
    };
    let facts = ConnectorReadStaticFacts::try_new(
        relation.read().input_version.clone(),
        relation.selection_digest(),
        ConnectorReadProperties::try_new(distribution, ordering)?,
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        coverage,
    )?;
    let schema = arrow_schema::Schema::new(
        relation
            .schema()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                arrow_schema::Field::new(
                    format!("v{ordinal}"),
                    field.ty.data_type.clone(),
                    field.ty.nullable,
                )
            })
            .collect::<Vec<_>>(),
    );
    let public = ConnectorReadPublicFacts::try_new(
        facts,
        kind,
        schema,
        relation
            .schema()
            .iter()
            .map(|field| field.ty.logical_type)
            .collect(),
    )?;
    FrozenConnectorRead::try_new(scan, public)
}

#[test]
fn package_extracts_metadata_without_losing_public_scan_facts() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let scan = frozen_scan(&fragment);
    let fragment_id = fragment.id();
    let node_id = fragment.root();
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Node(fragment_id, node_id),
        key: "statistics.row_count".into(),
        value: "17".into(),
    });
    let plan = builder.finish().unwrap();
    let parameters = SemanticParameters::try_new([(
        SemanticParameterId::new(u32::MAX),
        SemanticParameterValue::StatementStartUtc(-17),
    )])
    .unwrap();
    let scans = BTreeMap::from([(ProviderReadOccurrenceId::new(0), scan.clone())]);
    let packages = extract(&plan, &scans, &parameters, &BTreeMap::new()).unwrap();
    let package = &packages[&fragment_id];
    assert_eq!(package.fragment(), &fragment);
    assert!(package.parameters().entries().is_empty());
    assert_eq!(package.scans()[&node_id], scan);
    assert_eq!(package.annotations(), plan.annotations());
    assert!(
        matches!(&package.fragment().nodes()[&node_id].kind, NodeKind::Scan { relation, .. } if matches!(relation.as_ref(), Relation::Metadata(_)))
    );
    assert!(matches!(
        extract(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::MissingScan(_))
    ));
    let mut extra = scans;
    extra.insert(ProviderReadOccurrenceId::new(1), scan);
    assert!(matches!(
        extract(&plan, &extra, &parameters, &BTreeMap::new()),
        Err(FragmentPackageExtractionError::UnusedScan)
    ));
}

#[test]
fn package_refuses_missing_or_wrong_scan_node_facts() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let mut input = package_input(fragment.clone());
    assert!(
        FragmentPackage::try_new(input.clone(), &Control)
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.scans.insert(fragment.root(), frozen_scan(&fragment));
    FragmentPackage::try_new(input.clone(), &Control).unwrap();
    input
        .scans
        .insert(NodeId::new(u32::MAX), frozen_scan(&fragment));
    assert!(
        FragmentPackage::try_new(input, &Control)
            .unwrap_err()
            .to_string()
            .contains("missing or non-scan node")
    );
}

#[test]
fn package_refuses_complete_public_source_drift() {
    use novarocks_connector_contract::*;
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let original = frozen_scan(&fragment);
    for mutation in 0..7 {
        let public = original.public_facts();
        let source = public.source();
        let properties = if mutation == 4 {
            ConnectorReadProperties::try_new(ConnectorReadDistribution::RoundRobin, vec![]).unwrap()
        } else if mutation == 5 {
            ConnectorReadProperties::try_new(
                source.properties().distribution().clone(),
                vec![ConnectorReadOrderingKey::new(
                    ScanColumnId::new(0),
                    ConnectorReadSortDirection::Ascending,
                    ConnectorReadNullOrdering::First,
                )],
            )
            .unwrap()
        } else {
            source.properties().clone()
        };
        let source = ConnectorReadStaticFacts::try_new(
            if mutation == 0 {
                {
                    let mut bytes = source.input_version().as_bytes().to_vec();
                    bytes[0] ^= 1;
                    ConnectorReadInputVersion::try_new(bytes).unwrap()
                }
            } else {
                source.input_version().clone()
            },
            if mutation == 1 {
                let mut digest = source.selection_digest();
                digest[0] ^= 1;
                digest
            } else {
                source.selection_digest()
            },
            properties,
            source.artifact_coverage().clone(),
            if mutation == 3 {
                let mut bytes = source.coverage_evidence().to_vec();
                bytes[0] ^= 1;
                bytes
            } else {
                source.coverage_evidence().to_vec()
            },
        )
        .unwrap();
        let schema = if mutation == 6 {
            arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "v0",
                DataType::Int64,
                !public.schema().field(0).is_nullable(),
            )])
        } else {
            public.schema().clone()
        };
        let public = ConnectorReadPublicFacts::try_new(
            source,
            if mutation == 2 {
                Some(ConnectorReadMetadataKind::try_new("other").unwrap())
            } else {
                public.metadata_kind().cloned()
            },
            schema,
            public.logical_types().to_vec(),
        )
        .unwrap();
        let scan = FrozenConnectorRead::try_new(original.scan().clone(), public).unwrap();
        let mut input = package_input(fragment.clone());
        input.scans.insert(fragment.root(), scan);
        assert!(
            FragmentPackage::try_new(input, &Control).is_err(),
            "accepted public source drift {mutation}"
        );
    }
}

#[test]
fn package_refuses_exact_relation_and_batch_contract_drift() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
    };
    let fragment = finish_scan_relation(metadata_relation(&binding, column)).unwrap();
    let frozen = frozen_scan(&fragment);
    let scan = frozen.scan();
    let recipe = scan.recipe();
    for mutation in 0..4 {
        let mut columns = recipe.columns().to_vec();
        if mutation == 0 {
            columns[0] = encoded(&binding, ConnectorCodecCategory::ReadColumn, 8);
        }
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.relation().clone(),
            columns,
        )
        .unwrap();
        let malformed = FrozenConnectorScan::try_new(
            draft,
            if mutation == 3 {
                vec![StaticScanAssignment::new(
                    Arc::from("v"),
                    ConnectorValueType::Varchar,
                )]
            } else {
                scan.assignments().to_vec()
            },
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
            if mutation == 1 {
                NonZeroU64::new(scan.max_batch_rows().get() + 1).unwrap()
            } else {
                scan.max_batch_rows()
            },
            scan.max_batch_bytes(),
            if mutation == 2 {
                ConnectorReadWorkSource::WholeRelation
            } else {
                scan.work_source()
            },
        )
        .unwrap();
        let malformed = public_read(&fragment, fragment.root(), malformed);
        if mutation == 3 {
            assert!(malformed.is_err());
            continue;
        }
        let mut input = package_input(fragment.clone());
        input.scans.insert(fragment.root(), malformed.unwrap());
        assert!(
            FragmentPackage::try_new(input, &Control).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn result_and_annotation_facts_are_checked_at_the_local_boundary() {
    let id = FragmentId::new(81);
    let (fragment, value) = literal_fragment(id, FragmentSink::Result, false);
    let field = ResultField {
        name: "v".into(),
        alias: None,
        value,
        ty: fragment.values()[&value].ty.clone(),
    };
    let mut input = package_input(fragment.clone());
    assert!(FragmentPackage::try_new(input.clone(), &Control).is_err());
    input.result = Some(ResultPort {
        fragment: id,
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::from([field]),
    });
    FragmentPackage::try_new(input.clone(), &Control).unwrap();
    let mut wrong_result = input.clone();
    wrong_result.result.as_mut().unwrap().fields[0].ty.nullable = true;
    assert!(
        FragmentPackage::try_new(wrong_result, &Control)
            .unwrap_err()
            .to_string()
            .contains("result type differs")
    );
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Node(FragmentId::new(82), fragment.root()),
        key: "statistics.row_count".into(),
        value: "17".into(),
    }]);
    assert!(
        FragmentPackage::try_new(input, &Control)
            .unwrap_err()
            .to_string()
            .contains("subject this plan does not have")
    );
}

#[test]
fn remote_plan_statistics_do_not_grow_a_fragment_package() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    let before = builder.finish().unwrap();
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    builder.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "remote table statistics".repeat(100).into(),
    });
    let after = builder.finish().unwrap();
    let parameters = SemanticParameters::default();
    assert_eq!(
        extract(&before, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap(),
        extract(&after, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap()
    );
    assert_eq!(after.annotations().len(), 1);
}

#[test]
fn duplicate_scan_payloads_keep_runtime_filter_assignment_occurrences() {
    let (fragment, filter, second) =
        super::contract_regressions::scan_lineage_filter(false, false, false, true);
    let scan_id = filter.consumers[0].endpoint.node;
    let second = second.unwrap();
    let mut nodes = fragment.nodes().clone();
    let NodeKind::Scan {
        relation,
        provider_outputs,
        ..
    } = &mut nodes.get_mut(&scan_id).unwrap().kind
    else {
        unreachable!()
    };
    let first_field = provider_outputs[0].0.clone();
    provider_outputs[1].0 = first_field.clone();
    let schema = Box::from([relation.schema()[0].clone(), relation.schema()[0].clone()]);
    match relation.as_mut() {
        Relation::Data(relation) => relation.schema = schema,
        Relation::Metadata(relation) => relation.schema = schema,
    }
    let mut values = fragment.values().clone();
    values.get_mut(&second).unwrap().origin = ValueOrigin::ProviderField {
        scan_node: scan_id,
        field: first_field,
    };
    let fragment = Fragment::from(crate::plan::FragmentParts {
        id: fragment.id(),
        root: fragment.root(),
        nodes,
        values,
        expressions: fragment.expressions().clone(),
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
    });
    let NodeKind::Scan {
        relation,
        read_budget,
        ..
    } = &fragment.nodes()[&scan_id].kind
    else {
        unreachable!()
    };
    let read = relation.read();
    let recipe = ConnectorReadRelationRecipeDraft::try_new(
        read.binding.clone(),
        read.relation.clone(),
        relation
            .schema()
            .iter()
            .map(|field| field.column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let mut input = package_input(fragment.clone());
    input.cuts = FragmentCuts {
        runtime_filters: Box::from([filter]),
        ..FragmentCuts::default()
    };
    // The consumer targets the first ValueId. The second assignment has an
    // identical private payload but is a different output occurrence.
    for (variable, accepted) in [("v0", true), ("v1", false)] {
        let scan = FrozenConnectorScan::try_new(
            recipe.clone(),
            vec![
                StaticScanAssignment::new(Arc::from("v0"), ConnectorValueType::BigInt),
                StaticScanAssignment::new(Arc::from("v1"), ConnectorValueType::BigInt),
            ],
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![novarocks_connector_contract::StaticScanDynamicFilter::new(
                101,
                Arc::from(variable),
            )],
            NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
            NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
            relation.work_source(),
        )
        .unwrap();
        input
            .scans
            .insert(scan_id, public_read(&fragment, scan_id, scan).unwrap());
        let result = FragmentPackage::try_new(input.clone(), &Control);
        assert_eq!(result.is_ok(), accepted, "{variable}: {result:?}");
    }
}

fn parameter_fragment(reference: novarocks_type_contract::SemanticParameterRef) -> Fragment {
    parameter_occurrences_fragment(reference, 1)
}

fn parameter_occurrences_fragment(reference: SemanticParameterRef, occurrences: u32) -> Fragment {
    assert!(occurrences > 0);
    let mut builder = FragmentBuilder::new(FragmentId::new(81));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expr = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: FunctionId::try_new("test.parameter").unwrap(),
                    overload: FunctionOverloadId::try_new("test.parameter.zero").unwrap(),
                    kind: FunctionKind::Scalar,
                    argument_types: Box::default(),
                    result_type: value_type.clone(),
                    volatility: FunctionVolatility::Stable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
                    semantic_parameters: Box::from([reference]),
                },
                args: Box::default(),
            },
        )
        .unwrap();
    let values = (0..occurrences)
        .map(|output_ordinal| {
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: values.into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([vec![expr; occurrences as usize].into_boxed_slice()]),
            },
        })
        .unwrap();
    builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap()
}

#[test]
fn extraction_requires_frozen_call_table_for_every_fragment_even_without_calls() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment.clone()).unwrap();
    let plan = builder.finish().unwrap();
    let controls = fixture_controls(&plan);
    let calls = fixture_call_tables(&plan, &controls);
    let parameters = SemanticParameters::default();
    let extract_with = |calls: &BTreeMap<FragmentId, FrozenFragmentCalls>| {
        extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &parameters,
            &BTreeMap::new(),
            &controls,
            calls,
            &Control,
        )
    };
    let packages = extract_with(&calls).unwrap();
    assert!(packages[&fragment.id()].calls().entries().is_empty());
    assert_eq!(packages[&fragment.id()].calls(), &calls[&fragment.id()]);
    assert_eq!(
        extract_with(&BTreeMap::new()).unwrap_err(),
        FragmentPackageExtractionError::MissingCalls(fragment.id())
    );
    let (extra_fragment, _) = literal_fragment(FragmentId::new(82), FragmentSink::Noop, false);
    let extra_uses = fixture_expression_uses(&extra_fragment);
    let mut extra = calls;
    extra.insert(
        extra_fragment.id(),
        fixture_calls(&extra_fragment, &extra_uses),
    );
    assert_eq!(
        extract_with(&extra).unwrap_err(),
        FragmentPackageExtractionError::UnusedCalls
    );
}

#[test]
fn package_empty_noncall_table_cannot_substitute_for_an_actual_call_occurrence() {
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment(reference);
    let uses = fixture_expression_uses(&fragment);
    let (literal, _) = literal_fragment(fragment.id(), FragmentSink::Noop, false);
    let literal_uses = fixture_expression_uses(&literal);
    let empty_calls = fixture_calls(&literal, &literal_uses);
    let empty = FragmentPackage::try_new(
        package_input_with_controls(literal, literal_uses, empty_calls.clone()),
        &Control,
    )
    .unwrap();
    assert!(empty.calls().entries().is_empty());
    assert_eq!(
        FragmentPackage::try_new(
            package_input_with_controls(fragment, uses, empty_calls),
            &Control
        )
        .unwrap_err(),
        FragmentPackageError::Calls(FrozenCallError::MissingSite(PhysicalCallSite::Expression(
            ExpressionUseId::new(0)
        )))
    );
}

fn parameter_calls_with_references(
    fragment: &Fragment,
    uses: &PhysicalRootUses,
    references: &[SemanticParameterRef],
) -> FrozenFragmentCalls {
    assert_eq!(uses.flow().uses().len(), references.len());
    let calls = uses
        .flow()
        .uses()
        .iter()
        .zip(references)
        .map(|((id, invocation), reference)| {
            let ExprKind::FunctionCall { function, args } = &fragment
                .expressions()
                .get(invocation.definition)
                .unwrap()
                .kind
            else {
                panic!("explicit parameter-call fixture requires its actual function definition");
            };
            assert_eq!(function.function_id.as_str(), "test.parameter");
            assert_eq!(function.overload.as_str(), "test.parameter.zero");
            assert_eq!(function.kind, FunctionKind::Scalar);
            assert_eq!(function.result_type, ty(DataType::Int64, false));
            assert!(function.argument_types.is_empty());
            assert!(args.is_empty());
            FrozenPhysicalCall {
                site: PhysicalCallSite::Expression(*id),
                context: invocation.context,
                effects: parameter_fixture_effects(*reference),
            }
        })
        .collect();
    FrozenFragmentCalls::try_new(fragment, uses, calls, &Control).unwrap()
}

#[test]
fn package_preserves_same_key_different_lexical_references_for_shared_definition_uses() {
    let first = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let second = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_occurrences_fragment(first, 2);
    let uses = fixture_expression_uses(&fragment);
    let contexts = uses.flow().uses().values().collect::<Vec<_>>();
    assert_eq!(contexts[0].definition, contexts[1].definition);
    assert_ne!(contexts[0].context.use_id, contexts[1].context.use_id);
    let calls = parameter_calls_with_references(&fragment, &uses, &[first, second]);
    let mut input = package_input_with_controls(fragment.clone(), uses.clone(), calls.clone());
    input.parameters = SemanticParameters::try_new([
        (first.id, SemanticParameterValue::TimeZone("UTC".into())),
        (
            second.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
    ])
    .unwrap();
    let checked = FragmentPackage::try_new(input.clone(), &Control).unwrap();
    assert_eq!(checked.parameters().entries().len(), 2);
    for (id, reference) in [first, second].into_iter().enumerate() {
        assert_eq!(
            checked.calls().entries()
                [&PhysicalCallSite::Expression(ExpressionUseId::new(id as u32))]
                .effects
                .environment
                .as_ref(),
            &[reference]
        );
    }
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish().unwrap();
    let packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &input.parameters,
        &BTreeMap::new(),
        &BTreeMap::from([(FragmentId::new(81), uses)]),
        &BTreeMap::from([(FragmentId::new(81), calls)]),
        &Control,
    )
    .unwrap();
    assert_eq!(
        packages[&FragmentId::new(81)].parameters(),
        checked.parameters()
    );

    let mut missing = input.clone();
    missing.parameters =
        SemanticParameters::try_new([(first.id, SemanticParameterValue::TimeZone("UTC".into()))])
            .unwrap();
    assert!(
        FragmentPackage::try_new(missing, &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    let mut wrong_key = input.clone();
    wrong_key.parameters = SemanticParameters::try_new([
        (first.id, SemanticParameterValue::TimeZone("UTC".into())),
        (second.id, SemanticParameterValue::AllowThrowException(true)),
    ])
    .unwrap();
    assert!(
        FragmentPackage::try_new(wrong_key, &Control)
            .unwrap_err()
            .to_string()
            .contains("expected key")
    );
    input.parameters = SemanticParameters::try_new([
        (first.id, SemanticParameterValue::TimeZone("UTC".into())),
        (
            second.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
        (
            SemanticParameterId::new(8),
            SemanticParameterValue::TimeZone("Europe/London".into()),
        ),
    ])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input, &Control)
            .unwrap_err()
            .to_string()
            .contains("unused definitions")
    );
}

#[test]
fn frozen_call_environment_is_the_only_package_dependency_authority() {
    let legacy = SemanticParameterRef {
        id: SemanticParameterId::new(7),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let active = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment(legacy);
    let uses = fixture_expression_uses(&fragment);
    let calls = parameter_calls_with_references(&fragment, &uses, &[active]);
    let mut input = package_input_with_controls(fragment.clone(), uses.clone(), calls.clone());
    input.parameters = SemanticParameters::try_new([(
        active.id,
        SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
    )])
    .unwrap();
    let checked = FragmentPackage::try_new(input.clone(), &Control).unwrap();
    assert!(checked.parameters().entries().get(&legacy.id).is_none());
    let definition = &checked
        .fragment()
        .expressions()
        .get(checked.expression_uses().flow().uses()[&ExpressionUseId::new(0)].definition)
        .unwrap()
        .kind;
    let ExprKind::FunctionCall { function, .. } = definition else {
        unreachable!();
    };
    assert_eq!(function.semantic_parameters.as_ref(), &[legacy]);
    assert_eq!(
        checked.calls().entries()[&PhysicalCallSite::Expression(ExpressionUseId::new(0))]
            .effects
            .environment
            .as_ref(),
        &[active]
    );

    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish().unwrap();
    let snapshot = SemanticParameters::try_new([
        (legacy.id, SemanticParameterValue::TimeZone("UTC".into())),
        (
            active.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
    ])
    .unwrap();
    let controls = BTreeMap::from([(FragmentId::new(81), uses)]);
    let tables = BTreeMap::from([(FragmentId::new(81), calls)]);
    let packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &snapshot,
        &BTreeMap::new(),
        &controls,
        &tables,
        &Control,
    )
    .unwrap();
    assert_eq!(
        packages[&FragmentId::new(81)].parameters(),
        checked.parameters()
    );
    input.parameters =
        SemanticParameters::try_new([(legacy.id, SemanticParameterValue::TimeZone("UTC".into()))])
            .unwrap();
    assert!(
        FragmentPackage::try_new(input.clone(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    assert!(matches!(
        extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &input.parameters,
            &BTreeMap::new(),
            &controls,
            &tables,
            &Control
        ),
        Err(FragmentPackageExtractionError::Parameter(_))
    ));
}

#[test]
fn package_parameters_are_the_exact_call_dependency_closure() {
    use novarocks_type_contract::{SemanticParameterKey, SemanticParameterRef};
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let fragment = parameter_fragment(reference);
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    let plan = builder.finish().unwrap();
    let parameters = SemanticParameters::try_new([
        (
            reference.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
        (
            SemanticParameterId::new(1),
            SemanticParameterValue::TimeZone("UTC".into()),
        ),
    ])
    .unwrap();
    let packages = extract(&plan, &BTreeMap::new(), &parameters, &BTreeMap::new()).unwrap();
    let mut input = packages[&FragmentId::new(81)].clone().into_input();
    assert_eq!(input.parameters.entries().len(), 1);
    assert_eq!(
        input.parameters.require(reference).unwrap(),
        &SemanticParameterValue::TimeZone("Asia/Shanghai".into())
    );
    input.parameters = parameters;
    assert!(
        FragmentPackage::try_new(input.clone(), &Control)
            .unwrap_err()
            .to_string()
            .contains("unused definitions")
    );
    input.parameters = SemanticParameters::default();
    assert!(
        FragmentPackage::try_new(input.clone(), &Control)
            .unwrap_err()
            .to_string()
            .contains("missing semantic parameter ID")
    );
    input.parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    assert!(
        FragmentPackage::try_new(input, &Control)
            .unwrap_err()
            .to_string()
            .contains("expected key")
    );
}

#[test]
fn package_cannot_carry_whole_plan_display_annotations() {
    let (fragment, _) = literal_fragment(FragmentId::new(81), FragmentSink::Noop, false);
    let mut input = package_input(fragment);
    input.annotations = Box::from([PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "optimizer.table_statistics".into(),
        value: "peer facts".into(),
    }]);
    assert!(FragmentPackage::try_new(input, &Control).is_err());
}

#[test]
fn writer_package_requires_the_exact_public_input_recipe() {
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
    )
    .unwrap();
    let fragment = plan.fragments()[&FragmentId::new(722)].clone();
    let writer_id = fragment.root();
    let NodeKind::TableWriter { target } = &fragment.nodes()[&writer_id].kind else {
        unreachable!()
    };
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    let fields = target
        .target_fields
        .iter()
        .map(|field| {
            ConnectorWriteFieldBinding::new(
                field.token,
                arrow_schema::Field::new(
                    field.provider_name.as_ref(),
                    field.ty.data_type.clone(),
                    field.ty.nullable,
                ),
            )
        })
        .collect::<Vec<_>>();
    let write = ConnectorWriteRecipeDraft::try_new(
        binding.clone(),
        target.handle.clone(),
        ConnectorWriteInputShape::Data {
            fields: fields.clone(),
        },
    )
    .unwrap();
    let mut input = package_input(fragment.clone());
    input.cuts = fragment_cuts(&plan, fragment.id()).unwrap();
    assert!(
        FragmentPackage::try_new(input.clone(), &Control)
            .unwrap_err()
            .to_string()
            .contains("no complete frozen public facts")
    );
    input.writes.insert(writer_id, write.clone());
    FragmentPackage::try_new(input.clone(), &Control).unwrap();
    let mut wrong_node = input.clone();
    wrong_node.writes.insert(NodeId::new(u32::MAX), write);
    assert!(
        FragmentPackage::try_new(wrong_node, &Control)
            .unwrap_err()
            .to_string()
            .contains("missing or non-writer node")
    );
    for mutation in 0..3 {
        let field = &fields[0];
        let malformed = match mutation {
            0 => ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([8; 32]),
                field.field().clone(),
            ),
            1 => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("different", DataType::Int64, false),
            ),
            _ => ConnectorWriteFieldBinding::new(
                field.token(),
                arrow_schema::Field::new("v", DataType::Utf8, false),
            ),
        };
        let write = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data {
                fields: vec![malformed],
            },
        )
        .unwrap();
        input.writes.insert(writer_id, write);
        assert!(
            FragmentPackage::try_new(input.clone(), &Control)
                .unwrap_err()
                .to_string()
                .contains("input field occurrence")
        );
    }
}

#[test]
fn writer_package_preserves_nested_dictionary_field_identity() {
    use novarocks_connector_contract::{
        ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteInputShape,
        ConnectorWriteRecipeDraft,
    };
    #[allow(deprecated)]
    let nested_type = |dictionary_id| {
        DataType::Struct(
            vec![arrow_schema::Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                dictionary_id,
                false,
            )]
            .into(),
        )
    };
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
    )
    .unwrap();
    let original = &plan.fragments()[&FragmentId::new(722)];
    let mut nodes = original.nodes().clone();
    let NodeKind::TableWriter { target } = &mut nodes.get_mut(&original.root()).unwrap().kind
    else {
        unreachable!()
    };
    let input_value = target.target_fields[0].input;
    target.target_fields[0].ty.data_type = nested_type(1);
    let target = target.clone();
    let mut values = original.values().clone();
    values.get_mut(&input_value).unwrap().ty.data_type = nested_type(1);
    let fragment = Fragment::from(crate::plan::FragmentParts {
        id: original.id(),
        root: original.root(),
        values,
        expressions: original.expressions().clone(),
        nodes,
        sink: original.sink().clone(),
        dop_domain: original.dop_domain(),
        runtime_filters: original.runtime_filters().into(),
    });
    let mut input = package_input(fragment);
    input.cuts = fragment_cuts(&plan, original.id()).unwrap();
    for cut in &mut input.cuts.inbound {
        for import in &mut cut.imports {
            if import.destination == input_value {
                import.source.ty.data_type = nested_type(1);
            }
        }
    }
    let read_binding = connector_binding();
    let binding = ConnectorWriteBinding::new(
        read_binding.descriptor().clone(),
        read_binding.catalog_handle().clone(),
    );
    for id in [1, 2] {
        let fields = target
            .target_fields
            .iter()
            .map(|field| {
                ConnectorWriteFieldBinding::new(
                    field.token,
                    arrow_schema::Field::new(
                        field.provider_name.as_ref(),
                        nested_type(id),
                        field.ty.nullable,
                    ),
                )
            })
            .collect();
        let recipe = ConnectorWriteRecipeDraft::try_new(
            binding.clone(),
            target.handle.clone(),
            ConnectorWriteInputShape::Data { fields },
        )
        .unwrap();
        input.writes.insert(original.root(), recipe);
        let result = FragmentPackage::try_new(input.clone(), &Control);
        if id == 1 {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("input field occurrence")
            );
        }
    }
}
