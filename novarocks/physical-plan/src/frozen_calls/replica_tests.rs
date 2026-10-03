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
use crate::plan::FragmentParts;
use novarocks_type_contract::SemanticParameters;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct ReplicaControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for ReplicaControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original control refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn edit_fragment(fragment: &Fragment, edit: impl FnOnce(&mut FragmentParts)) -> Fragment {
    let mut parts = FragmentParts {
        id: fragment.id(),
        root: fragment.root(),
        values: fragment.values().clone(),
        expressions: fragment.expressions().clone(),
        nodes: fragment.nodes().clone(),
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
    };
    edit(&mut parts);
    Fragment::from(parts)
}
fn broadcast(fixture: &mut Fixture) {
    fixture.fragment = edit_fragment(&fixture.fragment, |parts| {
        let root = parts.nodes.get_mut(&parts.root).unwrap();
        root.output_properties.distribution = Distribution::Broadcast;
        root.output_properties.row_multiplicity = RowMultiplicity::Replicated;
    });
    // Distribution does not change actual root fields, demand or occurrences.
    fixture
        .uses
        .validate_fragment(&fixture.fragment, &Control::default())
        .unwrap();
}
fn check(fixture: &Fixture, control: &ReplicaControl) -> Result<(), FrozenCallError> {
    let calls = fixture.checked().unwrap();
    calls.validate_replica_equivalence(&fixture.fragment, &fixture.uses, control)
}
fn prefixes(
    call: impl Fn(&ReplicaControl) -> Result<(), FrozenCallError>,
    success: bool,
    all: bool,
) {
    let good = ReplicaControl::default();
    assert_eq!(call(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    let positions = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| {
            (all || at == 0 || at + 1 == trace.len() || *units == 256).then_some(at)
        })
        .collect::<Vec<_>>();
    for at in positions {
        for cause in CAUSES {
            let control = ReplicaControl {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(call(&control), Err(FrozenCallError::Control(actual)) if actual == cause),
                "refusal at {at}: {cause:?}"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn replica_equivalence_shared_definition_checks_each_sparse_use_and_domain() {
    let mut fixture = scalar_fixture(2, 0);
    broadcast(&mut fixture);
    let invocations = fixture.uses.flow().uses();
    assert_eq!(
        invocations[&ExpressionUseId::new(0)].definition,
        invocations[&ExpressionUseId::new(u32::MAX)].definition
    );
    assert_ne!(
        fixture.calls[0].context.domain,
        fixture.calls[1].context.domain
    );
    assert!(check(&fixture, &ReplicaControl::default()).is_ok());
    for unsafe_at in [0, 1] {
        fixture.calls[unsafe_at]
            .effects
            .observable_effects
            .rng_sampling = true;
        let site = fixture.calls[unsafe_at].site;
        assert_eq!(
            check(&fixture, &ReplicaControl::default()),
            Err(FrozenCallError::ReplicaEquivalence(site))
        );
        fixture.calls[unsafe_at].effects.observable_effects = ObservableEffects::NONE;
    }
}

#[test]
fn replica_equivalence_complete_claims_override_contradictory_legacy_volatility() {
    let mut fixture = scalar_fixture(1, u32::MAX);
    broadcast(&mut fixture);
    fixture.calls[0].effects.value_stability = FunctionVolatility::Volatile;
    let site = fixture.calls[0].site;
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    fixture.calls[0].effects.value_stability = FunctionVolatility::Immutable;
    fixture.fragment = edit_fragment(&fixture.fragment, |parts| {
        let nodes = parts
            .expressions
            .iter()
            .map(|(_, expression)| expression.clone())
            .collect::<Vec<_>>();
        for mut expression in nodes {
            if let ExprKind::FunctionCall { function, .. } = &mut expression.kind {
                function.volatility = FunctionVolatility::Volatile;
            }
            parts.expressions.insert(expression);
        }
    });
    assert!(check(&fixture, &ReplicaControl::default()).is_ok());
    fixture.calls[0].effects.value_stability = FunctionVolatility::Stable;
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
}

#[test]
fn replica_equivalence_immutable_state_and_each_observable_are_refused() {
    let mut fixture = scalar_fixture(1, 0);
    broadcast(&mut fixture);
    let site = fixture.calls[0].site;
    fixture.calls[0].effects.instance_state = FunctionInstanceState::ScalarInstance;
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    fixture.calls[0].effects.instance_state = FunctionInstanceState::None;
    for observable in [
        ObservableEffects {
            rng_sampling: true,
            warnings: false,
            controlled_wait: false,
        },
        ObservableEffects {
            rng_sampling: false,
            warnings: true,
            controlled_wait: false,
        },
        ObservableEffects {
            rng_sampling: false,
            warnings: false,
            controlled_wait: true,
        },
    ] {
        fixture.calls[0].effects.observable_effects = observable;
        assert_eq!(
            check(&fixture, &ReplicaControl::default()),
            Err(FrozenCallError::ReplicaEquivalence(site))
        );
    }
}

#[test]
fn replica_equivalence_keeps_same_occurrence_domain_proof_and_row_errors() {
    let mut fixture = scalar_fixture(2, 0);
    broadcast(&mut fixture);
    fixture.calls[0].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    fixture.calls[0].effects.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    fixture.calls[1].effects.proof_scope = CallProofScope::Unconditional;
    let calls = fixture.checked().unwrap();
    calls
        .validate_replica_equivalence(&fixture.fragment, &fixture.uses, &ReplicaControl::default())
        .unwrap();
    assert_eq!(
        calls.entries()[&fixture.calls[0].site]
            .effects
            .own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert_eq!(
        calls.entries()[&fixture.calls[0].site].effects.proof_scope,
        CallProofScope::Domain(fixture.calls[0].context.domain)
    );
    assert_eq!(
        calls.entries()[&fixture.calls[1].site].effects.proof_scope,
        CallProofScope::Unconditional
    );
}

#[test]
fn replica_equivalence_single_copy_does_not_invent_a_replay_requirement() {
    let mut fixture = scalar_fixture(2, 0);
    for call in &mut fixture.calls {
        call.effects.value_stability = FunctionVolatility::Volatile;
        call.effects.instance_state = FunctionInstanceState::ScalarInstance;
        call.effects.observable_effects = ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true,
        };
    }
    assert!(check(&fixture, &ReplicaControl::default()).is_ok());
    broadcast(&mut fixture);
    assert!(matches!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(_))
    ));
}

#[test]
fn replica_equivalence_revalidates_fragment_roots_and_context_before_claims() {
    let mut fixture = scalar_fixture(2, 0);
    broadcast(&mut fixture);
    let calls = fixture.checked().unwrap();
    let different = edit_fragment(&fixture.fragment, |parts| parts.id = FragmentId::new(7));
    assert!(matches!(
        calls.validate_replica_equivalence(&different, &fixture.uses, &ReplicaControl::default()),
        Err(FrozenCallError::WrongFragment | FrozenCallError::Roots(_))
    ));
    let changed = edit_fragment(&fixture.fragment, |parts| {
        let NodeKind::Values { rows } = &mut parts.nodes.get_mut(&parts.root).unwrap().kind else {
            panic!("values")
        };
        *rows = vec![rows[0].clone()].into_boxed_slice();
    });
    assert!(matches!(
        calls.validate_replica_equivalence(&changed, &fixture.uses, &ReplicaControl::default()),
        Err(FrozenCallError::Roots(_))
    ));
    // Fault injection into a private immutable table checks the public method's
    // defensive revalidation, rather than asserting a public mutation API.
    let mut invalid = calls.clone();
    Arc::make_mut(&mut invalid.entries)
        .get_mut(&fixture.calls[0].site)
        .unwrap()
        .context
        .domain = EvaluationDomainId::new(u32::MAX);
    assert_eq!(
        invalid.validate_replica_equivalence(
            &fixture.fragment,
            &fixture.uses,
            &ReplicaControl::default()
        ),
        Err(FrozenCallError::WrongContext)
    );
    prefixes(
        |control| invalid.validate_replica_equivalence(&fixture.fragment, &fixture.uses, control),
        false,
        true,
    );
}

#[test]
fn replica_equivalence_small_success_error_and_wide_quantum_preserve_all_control_causes() {
    let mut small = scalar_fixture(2, 0);
    broadcast(&mut small);
    prefixes(|control| check(&small, control), true, true);
    small.calls[1].effects.observable_effects.warnings = true;
    prefixes(|control| check(&small, control), false, true);
    let mut wide = scalar_fixture(320, u32::MAX);
    broadcast(&mut wide);
    let good = ReplicaControl::default();
    check(&wide, &good).unwrap();
    assert!(
        good.trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    prefixes(|control| check(&wide, control), true, false);
}

fn broadcast_table_fixture() -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = add_values(&mut builder, 1, true);
    // The input is an actual deterministic scalar occurrence. The table
    // argument reads its published value, rather than introducing a call
    // whose lifecycle state is inferred from legacy metadata.
    let input_value = ValueId::new(0);
    let table = builder.reserve_node_id().unwrap();
    let argument = builder
        .add_expression(table, boolean(), ExprKind::Value(input_value))
        .unwrap();
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let fields = function(FunctionKind::Table, integer());
    install_node(
        &mut builder,
        table,
        vec![input],
        vec![input_value, output],
        NodeKind::TableFunction {
            function: BoundTableFunction {
                function_id: fields.function_id,
                overload: fields.overload,
                argument_types: Box::from([FunctionArgumentType::Value(boolean())]),
                result_types: Box::from([integer()]),
                volatility: fields.volatility,
                argument_evaluation: fields.argument_evaluation,
                failure_behavior: fields.failure_behavior,
                intrinsic_row_error: fields.intrinsic_row_error,
                semantic_parameters: Box::default(),
            },
            arguments: Box::from([argument]),
            outputs: Box::from([
                TableFunctionOutput::PassThrough(input_value),
                TableFunctionOutput::FunctionResult {
                    result_ordinal: 0,
                    value: output,
                },
            ]),
            left_outer: false,
        },
    );
    let fragment = builder
        .finish_definition(table, FragmentSink::Noop, dop())
        .unwrap();
    let fragment = edit_fragment(&fragment, |parts| {
        for node in parts.nodes.values_mut() {
            node.output_properties.distribution = Distribution::Broadcast;
            node.output_properties.row_multiplicity = RowMultiplicity::Replicated;
            for required in &mut node.required_inputs {
                required.distribution = Distribution::Broadcast;
                required.row_multiplicity = RowMultiplicity::Replicated;
            }
        }
    });
    validate_fragment(&fragment, &FragmentCuts::default()).unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control::default()).unwrap();
    let mut bindings = Vec::new();
    let mut invocations = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        assert!(matches!(
            fragment.expressions().get(root.expr).unwrap().kind,
            ExprKind::Value(_) | ExprKind::FunctionCall { .. }
        ));
        let current = context(if ordinal == 0 { 0 } else { u32::MAX }, 0, root.demand);
        bindings.push((*site, current.use_id));
        invocations.push(ExpressionInvocation {
            context: current,
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    let flow = ExpressionControlFlow::try_new(
        vec![domain(0)],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::default()).unwrap();
    let scalar_context = uses
        .flow()
        .uses()
        .values()
        .find(|invocation| {
            matches!(
                fragment
                    .expressions()
                    .get(invocation.definition)
                    .unwrap()
                    .kind,
                ExprKind::FunctionCall { .. }
            )
        })
        .unwrap()
        .context;
    let table_context = context(1, 0, EvaluationDemand::Value);
    Fixture {
        fragment,
        uses,
        calls: vec![
            FrozenPhysicalCall {
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                site: PhysicalCallSite::Expression(scalar_context.use_id),
                context: scalar_context,
                effects: effects(FunctionKind::Scalar, scalar_context.domain),
            },
            FrozenPhysicalCall {
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                site: PhysicalCallSite::Table { node: table },
                context: table_context,
                effects: effects(FunctionKind::Table, table_context.domain),
            },
        ],
    }
}

#[test]
fn replica_equivalence_canonical_table_lifecycle_accepts_broadcast_and_refuses_exact_unsafe_site() {
    let mut fixture = broadcast_table_fixture();
    let site = fixture.calls[1].site;
    assert!(matches!(site, PhysicalCallSite::Table { .. }));
    assert_eq!(
        fixture.calls[1].effects.instance_state,
        FunctionInstanceState::TableInstance
    );
    assert!(fixture.calls[1].effects.observable_effects.is_empty());
    validate_fragment(&fixture.fragment, &FragmentCuts::default()).unwrap();
    check(&fixture, &ReplicaControl::default()).unwrap();

    fixture.calls[1].effects.observable_effects.warnings = true;
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    fixture.calls[1].effects.observable_effects = ObservableEffects::NONE;
    fixture.calls[1].effects.value_stability = FunctionVolatility::Volatile;
    assert_eq!(
        check(&fixture, &ReplicaControl::default()),
        Err(FrozenCallError::ReplicaEquivalence(site))
    );
    fixture.calls[1].effects.value_stability = FunctionVolatility::Immutable;
    fixture.calls[1].effects.instance_state = FunctionInstanceState::None;
    assert!(matches!(
        fixture.checked(),
        Err(FrozenCallError::InvalidEffects(_))
    ));
}

fn scalar_package_input(fixture: &Fixture) -> FragmentPackageInput {
    FragmentPackageInput {
        constants: ConstantPools::empty(),
        version: PlanVersionId::try_new([71; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment: fixture.fragment.clone(),
        expression_uses: fixture.uses.clone(),
        calls: fixture.checked().unwrap(),
        pruning: FrozenFragmentPruning::try_new(fixture.fragment.id(), vec![], &Control::default())
            .unwrap(),
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}

#[test]
fn replica_equivalence_package_rejects_one_unsafe_shared_scalar_occurrence() {
    let mut fixture = scalar_fixture(2, u32::MAX);
    broadcast(&mut fixture);
    assert_eq!(
        fixture.uses.flow().uses()[&ExpressionUseId::new(0)].definition,
        fixture.uses.flow().uses()[&ExpressionUseId::new(u32::MAX)].definition
    );
    FragmentPackage::try_new(scalar_package_input(&fixture), &ReplicaControl::default()).unwrap();
    let site = fixture.calls[1].site;
    fixture.calls[1].effects.instance_state = FunctionInstanceState::ScalarInstance;
    assert!(matches!(
        FragmentPackage::try_new(scalar_package_input(&fixture), &ReplicaControl::default()),
        Err(FragmentPackageError::Calls(FrozenCallError::ReplicaEquivalence(actual)))
            if actual == site
    ));
    fixture.calls[1].effects.instance_state = FunctionInstanceState::None;
    fixture.calls[1].effects.observable_effects.warnings = true;
    assert!(matches!(
        FragmentPackage::try_new(scalar_package_input(&fixture), &ReplicaControl::default()),
        Err(FragmentPackageError::Calls(FrozenCallError::ReplicaEquivalence(actual)))
            if actual == site
    ));
}
