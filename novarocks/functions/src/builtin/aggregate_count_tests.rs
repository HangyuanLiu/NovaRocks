// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::*;
use arrow_array::{DictionaryArray, Int32Array, StringArray, types::Int8Type};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, PureCompileControl, SemanticParameters,
};
use std::{mem::MaybeUninit, sync::Mutex, time::Duration};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after first refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("COUNT never waits")
    }
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(u32::MAX),
        demand: EvaluationDemand::Value,
    }
}
fn ty(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(arrow_schema::DataType::Int64, nullable)
}
struct Fixture {
    catalog: PureEngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    args: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
    state_input_type: FunctionValueType,
}
impl Fixture {
    fn new(types: &[FunctionValueType], policy: DecimalOverflowPolicy) -> Self {
        let original = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                original
                    .definition("count", FunctionKind::Aggregate)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        // Independent inventory of the one actually installed COUNT owner, not
        // an assertion that every builtin has a complete pure implementation.
        let catalog = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new("builtin.aggregate/count/v1").unwrap(),
                kind: FunctionKind::Aggregate,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new("builtin.aggregate/count/derived-v1")
                        .unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.aggregate/count/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::AggregateWindowV1,
                },
                aggregate_state_format: Some(
                    AggregateStateFormatIdentity::try_new("novarocks/count/state-v1").unwrap(),
                ),
            }])
            .unwrap();
        let args = types
            .iter()
            .map(|value_type| FunctionArgument::Value {
                value_type: value_type.clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let bound = catalog
            .metadata()
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: args.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        let state_input_type = ty(true);
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            uses: (0..args.len())
                .map(|n| Some(ExpressionUseId::new(n as u32 + 1)))
                .collect(),
            args,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
            state_input_type,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: self.args.len(),
                expected_result_type: None,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            context: context(),
            parameters: &self.parameters,
            environment: &[],
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn input_for_phase(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        let mut input = self.input();
        if !phase.consumes_logical_arguments() {
            // This fixture owns the actual nullable state carrier separately
            // from the selected logical signature. No production flow is inferred.
            input.argument_uses = crate::CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(u32::MAX),
                    domain: EvaluationDomainId::new(u32::MAX - 1),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &self.state_input_type,
            };
        }
        input
    }
    fn options(&self, phase: AggregateKernelPhase) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context()),
            options: AggregatePreparationOptions {
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: (!phase.consumes_logical_arguments())
                    .then(|| self.state_input_type.clone()),
            },
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog.prepare_fresh(
            self.input_for_phase(phase),
            self.selected.clone(),
            self.options(phase),
            control,
        )
    }
    fn kernel(&self, phase: AggregateKernelPhase) -> CountKernel {
        let prepared = self.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("actual aggregate handle")
        };
        CountKernel {
            contract: handle.contract().clone(),
        }
    }
}
fn result(array: &ArrayRef) -> Vec<i64> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
fn apply(kernel: &CountKernel, args: &[EvaluatedArgument<'_>], selection: Selection<'_>) -> i64 {
    let ctrl = RuntimeControl::default();
    let mut state = kernel.create_state(&ctrl).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, args, &[], &ctrl)
            .unwrap();
    let mut call = AggregateUpdateInvocation::try_new(kernel, input, &ctrl).unwrap();
    while call.next_selected_ordinal().is_some() {
        call.update_next(&mut state, &ctrl).unwrap();
    }
    state
}

#[test]
fn aggregate_count_installed_fresh_frozen_preserve_exact_lifecycle_and_policies() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for types in [vec![], vec![ty(true)], vec![ty(false)]] {
            let fixture = Fixture::new(&types, policy);
            for phase in [
                AggregateKernelPhase::Single,
                AggregateKernelPhase::Partial,
                AggregateKernelPhase::Intermediate,
                AggregateKernelPhase::Final,
            ] {
                let fresh = fixture.prepare(phase, &CompileControl::default()).unwrap();
                assert_eq!(fresh.source(), PurePreparationSource::Fresh);
                let frozen = fixture
                    .catalog
                    .prepare_frozen(
                        fixture.input_for_phase(phase),
                        fixture.selected.clone(),
                        fresh.call_contract().effects(),
                        fixture.options(phase),
                        &CompileControl::default(),
                    )
                    .unwrap();
                assert_eq!(frozen.source(), PurePreparationSource::Frozen);
                for actual in [&fresh, &frozen] {
                    assert_eq!(
                        actual.implementation().abi,
                        PureKernelAbi::AggregateWindowV1
                    );
                    assert_eq!(actual.call_contract().parameters(), &fixture.parameters);
                    assert!(std::ptr::eq(
                        actual.call_contract().selected(),
                        fixture.selected.as_ref()
                    ));
                    assert_eq!(actual.call_contract().decimal_overflow_policy(), policy);
                    assert_eq!(
                        actual.call_contract().effects().instance_state,
                        FunctionInstanceState::AggregateInstance
                    );
                    assert_eq!(
                        actual.call_contract().effects().null_behavior,
                        FunctionNullBehavior::CalledOnNull
                    );
                    assert_eq!(
                        actual.call_contract().effects().own_row_error,
                        FunctionIntrinsicRowError::NotRowEvaluated
                    );
                }
            }
        }
    }
}

#[test]
fn aggregate_count_closed_catalog_uses_one_exact_owner_for_aggregate_and_over() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for arguments in [vec![], vec![ty(true)]] {
            let fixture = Fixture::new(&arguments, policy);
            let aggregate = fixture
                .prepare(AggregateKernelPhase::Single, &CompileControl::default())
                .unwrap();
            for ignore_nulls in [false, true] {
                let options = || PureCallPreparation::AggregateWindow {
                    arguments: ScopedExpressionEffects::pure_value(context()),
                    options: AggregateWindowPreparationOptions {
                        aggregate: AggregatePreparationOptions {
                            phase: AggregateKernelPhase::Single,
                            distinct: false,
                            order_keys: Arc::from([]),
                            state_input_type: None,
                        },
                        window: WindowCallOptions::try_new(
                            None,
                            ignore_nulls,
                            &CompileControl::default(),
                        )
                        .unwrap(),
                    },
                };
                let fresh = fixture
                    .catalog
                    .prepare_fresh(
                        fixture.input(),
                        fixture.selected.clone(),
                        options(),
                        &CompileControl::default(),
                    )
                    .unwrap();
                let frozen = fixture
                    .catalog
                    .prepare_frozen(
                        fixture.input(),
                        fixture.selected.clone(),
                        fresh.call_contract().effects(),
                        options(),
                        &CompileControl::default(),
                    )
                    .unwrap();
                for actual in [&fresh, &frozen] {
                    assert_eq!(actual.implementation(), aggregate.implementation());
                    assert!(std::ptr::eq(
                        actual.call_contract().selected(),
                        fixture.selected.as_ref()
                    ));
                    assert_eq!(
                        actual.call_contract().effects(),
                        aggregate.call_contract().effects()
                    );
                    let PreparedPureKernel::Window(window) = actual.prepared() else {
                        panic!("COUNT AggregateWindowV1 must prepare its real window adapter")
                    };
                    let source = window.contract().aggregate().unwrap();
                    assert_eq!(source.phase(), AggregateKernelPhase::Single);
                    assert!(std::ptr::eq(source.call().as_ref(), actual.call_contract()));
                    assert_eq!(source.call().decimal_overflow_policy(), policy);
                }
            }
        }
    }
}

#[test]
fn aggregate_count_independent_count_star_nullable_encoded_and_selected_addresses() {
    let rows = [1, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let star =
        Fixture::new(&[], DecimalOverflowPolicy::OutputNull).kernel(AggregateKernelPhase::Single);
    assert_eq!(apply(&star, &[], selection), 2);
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let source: ArrayRef = Arc::new(
        Int64Array::from(vec![Some(999), None, Some(888), None, Some(7), Some(222)]).slice(0, 5),
    );
    assert_eq!(
        apply(&kernel, &[EvaluatedArgument::Column(&source)], selection),
        1
    );
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![Some(71)]));
    assert_eq!(
        apply(&kernel, &[EvaluatedArgument::Scalar(&scalar)], selection),
        2
    );
    let null: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    assert_eq!(
        apply(&kernel, &[EvaluatedArgument::Scalar(&null)], selection),
        0
    );
    let compact: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
    let compact = SelectedValues::try_new(
        selection,
        &arrow_schema::DataType::Int64,
        compact,
        Box::default(),
    )
    .unwrap();
    assert_eq!(
        apply(
            &kernel,
            &[EvaluatedArgument::SelectedColumn(&compact)],
            selection
        ),
        1
    );
    let keys = arrow_array::Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]);
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("not null"), None]));
    let dictionary: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    let f = Fixture::new(
        &[FunctionValueType::new(dictionary.data_type().clone(), true)],
        DecimalOverflowPolicy::OutputNull,
    );
    assert_eq!(
        apply(
            &f.kernel(AggregateKernelPhase::Single),
            &[EvaluatedArgument::Column(&dictionary)],
            Selection::all(5)
        ),
        2
    );
    let pool = ConstantPool::try_new(
        Arc::new(ty(true).try_to_field("original-count-source").unwrap()),
        ty(true),
        source.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    let value = pool.value(4).unwrap();
    let null_value = pool.value(1).unwrap();
    assert_eq!(
        apply(&kernel, &[EvaluatedArgument::Constant(&value)], selection),
        2
    );
    assert_eq!(
        apply(
            &kernel,
            &[EvaluatedArgument::Constant(&null_value)],
            selection
        ),
        0
    );
    assert_eq!(value.ordinal(), 4);
    assert!(value.pool().backing_identity() == pool.backing_identity());
}

#[test]
fn aggregate_count_split_update_merge_all_phases_signed_states_and_empty() {
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::ReportError);
    let partial = fixture.kernel(AggregateKernelPhase::Partial);
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(20), Some(30)]));
    let a = apply(
        &partial,
        &[EvaluatedArgument::Column(&source)],
        Selection::try_sparse(4, &[0, 1]).unwrap(),
    );
    let b = apply(
        &partial,
        &[EvaluatedArgument::Column(&source)],
        Selection::try_sparse(4, &[2, 3]).unwrap(),
    );
    assert_eq!((a, b), (1, 2));
    let ctrl = RuntimeControl::default();
    assert_eq!(
        result(
            &partial
                .build_intermediate([&a, &b].into_iter(), &ctrl)
                .unwrap()
        ),
        vec![1, 2]
    );
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let kernel = fixture.kernel(phase);
        let mut state = kernel.create_state(&ctrl).unwrap();
        let states: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-2), Some(2)]));
        let input = SelectedAggregateMergeInput::try_new(
            &kernel.contract,
            Selection::all(4),
            EvaluatedArgument::Column(&states),
            &ctrl,
        )
        .unwrap();
        let mut call = AggregateMergeInvocation::try_new(&kernel, input, &ctrl).unwrap();
        while call.next_selected_ordinal().is_some() {
            call.merge_next(&mut state, &ctrl).unwrap();
        }
        assert_eq!(state, 1);
        assert_eq!(
            result(&kernel.build_final([&state].into_iter(), &ctrl).unwrap()),
            vec![1]
        );
        assert_eq!(kernel.retained_bytes(&state), 0);
        assert!(result(&kernel.build_final(std::iter::empty(), &ctrl).unwrap()).is_empty());
    }
    let single = fixture.kernel(AggregateKernelPhase::Single);
    assert_eq!(
        apply(
            &single,
            &[EvaluatedArgument::Column(&source)],
            Selection::all(4)
        ),
        3
    );
    assert_eq!(
        apply(
            &single,
            &[EvaluatedArgument::Column(&source)],
            Selection::try_sparse(4, &[]).unwrap()
        ),
        0
    );
}

#[repr(align(8))]
struct Storage([MaybeUninit<u8>; 8]);
#[test]
fn aggregate_count_actual_erased_handle_sparse_repeated_group_mapping_and_foreign_owner() {
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::OutputNull);
    let prepared = fixture
        .prepare(AggregateKernelPhase::Single, &CompileControl::default())
        .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!()
    };
    let ctrl = RuntimeControl::default();
    let mut first = Storage([MaybeUninit::uninit(); 8]);
    let mut second = Storage([MaybeUninit::uninit(); 8]);
    let mut states = [
        handle.initialize_in(&mut first.0, &ctrl).unwrap(),
        handle.initialize_in(&mut second.0, &ctrl).unwrap(),
    ];
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(91),
        Some(1),
        None,
        Some(92),
        Some(2),
    ]));
    let args = [EvaluatedArgument::Column(&source)];
    let selected = Selection::try_sparse(5, &[1, 2, 4]).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(handle.contract(), selected, &args, &[], &ctrl)
            .unwrap();
    {
        let mut call = handle
            .prepare_update_batch(&mut states, &[0, 1, 0], input, &ctrl)
            .unwrap();
        call.run(&ctrl).unwrap();
        assert_eq!(call.rows_processed(), 3);
    }
    assert_eq!(
        result(&handle.emit(&states, &[1, 0, 0], 3, &ctrl).unwrap()),
        vec![0, 2, 2]
    );
    let foreign = fixture
        .prepare(AggregateKernelPhase::Single, &CompileControl::default())
        .unwrap();
    let PreparedPureKernel::Aggregate(foreign) = foreign.prepared() else {
        panic!()
    };
    assert!(matches!(
        foreign.emit(&states, &[0], 1, &ctrl),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn aggregate_count_exact_binding_distinct_order_state_and_required_children_reject() {
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::OutputNull);
    let options = PureCallPreparation::Aggregate {
        arguments: ScopedExpressionEffects::pure_value(context()),
        options: AggregatePreparationOptions {
            phase: AggregateKernelPhase::Single,
            distinct: true,
            order_keys: Arc::from([]),
            state_input_type: None,
        },
    };
    assert!(matches!(
        fixture.catalog.prepare_fresh(
            fixture.input(),
            fixture.selected.clone(),
            options,
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let options = PureCallPreparation::Aggregate {
        arguments: ScopedExpressionEffects::pure_value(context()),
        options: AggregatePreparationOptions {
            phase: AggregateKernelPhase::Single,
            distinct: false,
            order_keys: Arc::from([AggregateOrderKey {
                ascending: true,
                nulls_first: false,
            }]),
            state_input_type: None,
        },
    };
    assert!(
        fixture
            .catalog
            .prepare_fresh(
                fixture.input(),
                fixture.selected.clone(),
                options,
                &CompileControl::default()
            )
            .is_err()
    );
    let mut wrong = (*fixture.selected).clone();
    wrong.result_type = FunctionResultType::Scalar(ty(true));
    let wrong = Arc::new(wrong);
    let mut input = fixture.input();
    input.selected = &wrong;
    assert!(
        fixture
            .catalog
            .prepare_fresh(
                input,
                wrong.clone(),
                fixture.options(AggregateKernelPhase::Single),
                &CompileControl::default()
            )
            .is_err()
    );
    let options = PureCallPreparation::Aggregate {
        arguments: ScopedExpressionEffects::pure_value(context()),
        options: AggregatePreparationOptions {
            phase: AggregateKernelPhase::Final,
            distinct: false,
            order_keys: Arc::from([]),
            state_input_type: Some(FunctionValueType::new(arrow_schema::DataType::Int32, true)),
        },
    };
    assert!(
        fixture
            .catalog
            .prepare_fresh(
                fixture.input_for_phase(AggregateKernelPhase::Final),
                fixture.selected.clone(),
                options,
                &CompileControl::default()
            )
            .is_err()
    );

    let metadata_type = FunctionValueType::new(
        arrow_schema::DataType::List(Arc::new(
            arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                .with_metadata([("source".into(), "original".into())].into()),
        )),
        true,
    );
    let metadata_fixture = Fixture::new(&[metadata_type], DecimalOverflowPolicy::ReportError);
    assert!(
        metadata_fixture
            .prepare(AggregateKernelPhase::Single, &CompileControl::default())
            .is_ok()
    );
    let changed_type = FunctionValueType::new(
        arrow_schema::DataType::List(Arc::new(
            arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                .with_metadata([("source".into(), "different".into())].into()),
        )),
        true,
    );
    let changed_args = [FunctionArgument::Value {
        value_type: changed_type,
        constant: None,
    }];
    let mut changed_input = metadata_fixture.input();
    changed_input.request.arguments = &changed_args;
    assert!(
        metadata_fixture
            .catalog
            .prepare_fresh(
                changed_input,
                metadata_fixture.selected.clone(),
                metadata_fixture.options(AggregateKernelPhase::Single),
                &CompileControl::default()
            )
            .is_err()
    );
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let ctrl = RuntimeControl::default();
    let wrong_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
    let args = [EvaluatedArgument::Column(&wrong_array)];
    assert!(
        SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(1),
            &args,
            &[],
            &ctrl
        )
        .is_err()
    );
    let values: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let error = crate::RowDataError::new(0, "required COUNT child error");
    let selected = SelectedValues::try_new(
        Selection::all(1),
        &arrow_schema::DataType::Int64,
        values,
        vec![error].into_boxed_slice(),
    )
    .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&selected)];
    assert!(
        SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(1),
            &args,
            &[],
            &ctrl
        )
        .is_err()
    );
}

#[test]
fn aggregate_count_checked_overflow_has_no_state_mutation_and_invocation_latches() {
    let fixture = Fixture::new(&[], DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let ctrl = RuntimeControl::default();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, Selection::all(1), &[], &[], &ctrl)
            .unwrap();
    let mut call = AggregateUpdateInvocation::try_new(&kernel, input, &ctrl).unwrap();
    let mut state = i64::MAX;
    assert!(matches!(
        call.update_next(&mut state, &ctrl),
        Err(KernelFailure::Operational(_))
    ));
    assert_eq!(state, i64::MAX);
    let callbacks = ctrl.trace.lock().unwrap().len();
    assert!(matches!(
        call.update_next(&mut state, &ctrl),
        Err(KernelFailure::InstanceFailed)
    ));
    assert_eq!(ctrl.trace.lock().unwrap().len(), callbacks);
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Final);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1)]));
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(1),
        EvaluatedArgument::Column(&values),
        &ctrl,
    )
    .unwrap();
    let prepared = kernel.prepare_merge(input, &ctrl).unwrap();
    let mut state = i64::MIN;
    assert!(matches!(
        kernel.merge_row(&mut state, &prepared, 0, &ctrl),
        Err(KernelFailure::Operational(_))
    ));
    assert_eq!(state, i64::MIN);
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Internal(KernelDiagnostic::new("original")),
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original")),
        KernelFailure::Operational(KernelDiagnostic::new("original")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn aggregate_count_all_actual_compile_and_runtime_prefixes_preserve_first_cause() {
    let fixture = Fixture::new(&[], DecimalOverflowPolicy::ReportError);
    for distinct in [false, true] {
        let prepare = |control: &CompileControl| {
            let mut options = fixture.options(AggregateKernelPhase::Single);
            if let PureCallPreparation::Aggregate { options, .. } = &mut options {
                options.distinct = distinct;
            }
            fixture.catalog.prepare_fresh(
                fixture.input(),
                fixture.selected.clone(),
                options,
                control,
            )
        };
        let base = CompileControl::default();
        assert_eq!(prepare(&base).is_ok(), !distinct);
        let trace = base.trace.lock().unwrap().clone();
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let ctrl = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause)),
                };
                let result = prepare(&ctrl);
                assert!(
                    matches!(result,Err(FunctionSpecializationFailure::Control(actual)) if actual==cause)
                        || matches!(result,Err(FunctionSpecializationFailure::Kernel(ref actual)) if matches!((cause,actual),
                    (CompileControlError::Cancelled,KernelFailure::Cancelled)|(CompileControlError::DeadlineExceeded,KernelFailure::DeadlineExceeded)|(CompileControlError::ResourceExhausted,KernelFailure::ResourceExhausted)))
                );
                assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    for bad in [false, true] {
        let operation = |ctrl: &RuntimeControl| {
            kernel.build_final(
                [&1i64, &2i64].into_iter().take(if bad { 0 } else { 2 }),
                ctrl,
            )
        };
        let base = RuntimeControl::default();
        operation(&base).unwrap();
        let trace = base.trace.lock().unwrap().clone();
        for stop in 0..trace.len() {
            for cause in causes() {
                let ctrl = RuntimeControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(operation(&ctrl),Err(actual) if actual==cause));
                assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
    let values = [7i64; 320];
    let ctrl = RuntimeControl::default();
    assert_eq!(
        result(&kernel.build_final(values.iter(), &ctrl).unwrap()),
        vec![7; 320]
    );
    let trace = ctrl.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    let stops = [
        0,
        trace.iter().position(|u| *u == 256).unwrap(),
        trace.len() - 1,
    ];
    for stop in stops {
        for cause in causes() {
            let ctrl = RuntimeControl {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(kernel.build_final(values.iter(),&ctrl),Err(actual) if actual==cause));
            assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn aggregate_count_emission_layout_and_dishonest_extent_are_rejected_before_growth() {
    assert!(matches!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    ));
    struct Liar<'a> {
        values: std::slice::Iter<'a, i64>,
    }
    impl<'a> Iterator for Liar<'a> {
        type Item = &'a i64;
        fn next(&mut self) -> Option<Self::Item> {
            self.values.next()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (1, Some(1))
        }
    }
    impl ExactSizeIterator for Liar<'_> {
        fn len(&self) -> usize {
            1
        }
    }
    let fixture = Fixture::new(&[], DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let ctrl = RuntimeControl::default();
    assert!(matches!(
        kernel.build_final(
            Liar {
                values: [1, 2].iter()
            },
            &ctrl
        ),
        Err(KernelFailure::Internal(_))
    ));
    let trace = ctrl.trace.lock().unwrap().clone();
    assert!(trace.last().is_some_and(|u| *u > 0));
    for stop in 0..trace.len() {
        for cause in causes() {
            let ctrl = RuntimeControl {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(kernel.build_final(Liar{values:[1,2].iter()},&ctrl),Err(actual) if actual==cause)
            );
            assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn aggregate_count_update_merge_create_actual_prefixes_and_overflow_ordinary_tail() {
    let star =
        Fixture::new(&[], DecimalOverflowPolicy::OutputNull).kernel(AggregateKernelPhase::Single);
    let fixture = Fixture::new(&[ty(true)], DecimalOverflowPolicy::OutputNull);
    let merge = fixture.kernel(AggregateKernelPhase::Final);
    let setup = RuntimeControl::default();
    let update =
        SelectedAggregateUpdateInput::try_new(&star.contract, Selection::all(1), &[], &[], &setup)
            .unwrap();
    let update = star.prepare_update(update, &setup).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(-2)]));
    let input = SelectedAggregateMergeInput::try_new(
        &merge.contract,
        Selection::all(1),
        EvaluatedArgument::Column(&array),
        &setup,
    )
    .unwrap();
    let merge_input = merge.prepare_merge(input, &setup).unwrap();
    for action in 0..4 {
        let operation = |control: &RuntimeControl| -> Result<(), KernelFailure> {
            match action {
                0 => star.create_state(control).map(|_| ()),
                1 => {
                    let mut state = 0;
                    star.update_row(&mut state, &update, 0, control)
                }
                2 => {
                    let mut state = 3;
                    merge.merge_row(&mut state, &merge_input, 0, control)
                }
                _ => {
                    let mut state = i64::MAX;
                    star.update_row(&mut state, &update, 0, control)
                }
            }
        };
        let base = RuntimeControl::default();
        assert_eq!(operation(&base).is_ok(), action != 3);
        let trace = base.trace.lock().unwrap().clone();
        assert!(trace.last().is_some_and(|units| *units > 0));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = RuntimeControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(operation(&control), Err(actual) if actual == cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn aggregate_count_actual_erased_emission_all_callbacks_keep_seven_original_causes() {
    let fixture = Fixture::new(&[], DecimalOverflowPolicy::ReportError);
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let prepared = fixture.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("actual installed aggregate handle")
        };
        let foreign_prepared = fixture.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(foreign) = foreign_prepared.prepared() else {
            panic!("independently prepared aggregate handle")
        };
        for scenario in 0..5 {
            let emit = |control: &RuntimeControl| {
                // The borrowed states are freshly initialized for every attempt.
                // Setup uses an unrefused control and is outside the tested trace.
                let setup = RuntimeControl::default();
                let mut first = Storage([MaybeUninit::uninit(); 8]);
                let mut second = Storage([MaybeUninit::uninit(); 8]);
                let states = [
                    handle.initialize_in(&mut first.0, &setup).unwrap(),
                    handle.initialize_in(&mut second.0, &setup).unwrap(),
                ];
                match scenario {
                    0 => handle.emit(&states, &[1, 0, 0], 3, control),
                    1 => handle.emit(&states, &[], 0, control),
                    2 => handle.emit(&states, &[2], 1, control),
                    3 => foreign.emit(&states, &[0], 1, control),
                    _ => handle.emit(&states, &[0], 0, control),
                }
            };
            let baseline = RuntimeControl::default();
            let outcome = emit(&baseline);
            if scenario < 2 {
                assert_eq!(
                    result(&outcome.unwrap()),
                    vec![0; if scenario == 0 { 3 } else { 0 }]
                );
            } else if scenario == 4 {
                assert!(matches!(outcome, Err(KernelFailure::ResourceExhausted)));
            } else {
                assert!(matches!(outcome, Err(KernelFailure::InvalidProgram(_))));
            }
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace.first(), Some(&0));
            if scenario == 4 {
                // Exact source-derived prefix: mapping entry, its one completed
                // slot check, and typed emission entry. The direct row-grant
                // Resource must return before either state postcheck runs.
                assert_eq!(trace, vec![0, 1, 0]);
            }
            if scenario < 2 {
                // This trace includes the real typed emitter and both enclosing
                // emission postchecks, rather than only the direct COUNT builder.
                assert!(trace.len() > 4);
            }
            for stop in 0..trace.len() {
                for cause in causes() {
                    let control = RuntimeControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((stop, cause.clone())),
                    };
                    assert!(matches!(emit(&control), Err(actual) if actual == cause));
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                }
            }
        }
    }
}
