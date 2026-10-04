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

use crate::kernel_control::{internal, invalid};
use crate::*;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, StringArray, StructArray,
    types::Int8Type,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior, PureCompileControl,
    SemanticParameters,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const NAMES: [&str; 2] = ["lead", "lag"];
const ARGUMENT_USES: [Option<ExpressionUseId>; 3] = [
    Some(ExpressionUseId::new(11)),
    Some(ExpressionUseId::new(12)),
    Some(ExpressionUseId::new(13)),
];
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
            assert!(at <= stop, "compile callback after primary");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after primary");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("LEAD/LAG never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}
struct Fixture {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(name: &str, source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        Self::with_arguments(
            name,
            vec![FunctionArgument::Value {
                value_type: source,
                constant: None,
            }],
            policy,
        )
    }
    fn offset(
        name: &str,
        source: FunctionValueType,
        offset: ConstantValue,
        policy: DecimalOverflowPolicy,
    ) -> Self {
        Self::with_arguments(
            name,
            vec![
                FunctionArgument::Value {
                    value_type: source,
                    constant: None,
                },
                FunctionArgument::Value {
                    value_type: offset.value_type().clone(),
                    constant: Some(offset),
                },
            ],
            policy,
        )
    }
    fn with_arguments(
        name: &str,
        arguments: Vec<FunctionArgument>,
        policy: DecimalOverflowPolicy,
    ) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let resolved = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Window,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: context(),
            argument_uses: &ARGUMENT_USES[..self.arguments.len()],
            function_id: &self.function,
            kind: FunctionKind::Window,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn prepare(
        &self,
        options: WindowCallOptions,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let prepared = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            PureCallPreparation::Window {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options,
            },
            control,
        )?;
        assert_eq!(prepared.implementation().abi, PureKernelAbi::WindowV1);
        assert!(Arc::ptr_eq(
            prepared.call_contract().selected_owner(),
            &self.selected
        ));
        match prepared.into_prepared() {
            PreparedPureKernel::Window(value) => Ok(value),
            _ => panic!("actual Window attachment"),
        }
    }
}
fn options(ignore: bool) -> WindowCallOptions {
    WindowCallOptions::try_new(None, ignore, &CompileControl::default()).unwrap()
}
fn geometry<'a>(
    prepared: &'a Arc<dyn PreparedWindowKernel>,
    arguments: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let full = FullPartitionWindowInput::try_new(
        prepared.contract(),
        frames.len(),
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap()
}
fn ints(output: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(output.errors().is_empty());
    output
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn ty(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(array.data_type().clone(), nullable)
}
fn policy() -> ConstantPolicy {
    // Explicit finite test invoice; not a production profile or a host MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}
fn constant(array: ArrayRef, nullable: bool, ordinal: u32) -> ConstantValue {
    let source = ty(&array, nullable);
    ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap()),
        source,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn cv(value: Option<i64>, nullable: bool) -> ConstantValue {
    constant(Arc::new(Int64Array::from(vec![value])), nullable, 0)
}
fn whole_frames(rows: usize) -> Vec<WindowRowRange> {
    vec![
        WindowRowRange {
            start: 0,
            end: rows
        };
        rows
    ]
}
fn hand(name: &str, ignore: bool) -> [Option<i32>; 6] {
    // Input [NULL,10,NULL,30,40,NULL], offset one, in one partition.
    match (name, ignore) {
        ("lead", false) => [Some(10), None, Some(30), Some(40), None, None],
        ("lag", false) => [None, None, Some(10), None, Some(30), Some(40)],
        ("lead", true) => [Some(10), Some(30), Some(30), Some(40), None, None],
        ("lag", true) => [None, None, Some(10), Some(10), Some(30), Some(40)],
        _ => unreachable!("installed names"),
    }
}
fn compile_cause(error: FunctionSpecializationFailure) -> CompileControlError {
    match error {
        FunctionSpecializationFailure::Control(cause) => cause,
        FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
            CompileControlError::Cancelled
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
            CompileControlError::DeadlineExceeded
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
            CompileControlError::ResourceExhausted
        }
        other => panic!("unexpected original cause {other:?}"),
    }
}

#[test]
fn installed_lead_lag_default_one_preserve_hand_oracles_policies_and_split_selection() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = whole_frames(6);
    for name in NAMES {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for ignore in [false, true] {
                let fixture = Fixture::new(name, ty(&values, true), policy);
                let prepared = fixture
                    .prepare(options(ignore), &CompileControl::default())
                    .unwrap();
                assert_eq!(prepared.contract().call().decimal_overflow_policy(), policy);
                assert_eq!(
                    prepared.contract().call().effects().argument_control,
                    ArgumentControl::Window
                );
                assert_eq!(
                    prepared.contract().call().effects().instance_state,
                    FunctionInstanceState::WindowPartition
                );
                assert_eq!(
                    prepared.contract().call().effects().null_behavior,
                    FunctionNullBehavior::CalledOnNull
                );
                assert_eq!(
                    prepared.contract().call().effects().own_row_error,
                    FunctionIntrinsicRowError::NotRowEvaluated
                );
                let mut part = WindowEvaluationPartition::begin(
                    prepared.clone(),
                    geometry(&prepared, &arguments, &peers, &frames),
                    &Control::default(),
                )
                .unwrap();
                assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
                assert_eq!(
                    ints(
                        &part
                            .evaluate(Selection::all(6), 6, &Control::default())
                            .unwrap()
                    ),
                    hand(name, ignore)
                );
                let selected_rows = [1, 4, 5];
                let selection = Selection::try_sparse(6, &selected_rows).unwrap();
                let expected = selected_rows.map(|row| hand(name, ignore)[row]);
                for _ in 0..2 {
                    assert_eq!(
                        ints(&part.evaluate(selection, 3, &Control::default()).unwrap()),
                        expected
                    );
                }
                assert!(
                    part.evaluate(
                        Selection::try_sparse(6, &[]).unwrap(),
                        0,
                        &Control::default()
                    )
                    .unwrap()
                    .values()
                    .is_empty()
                );
                part.finish(&Control::default()).unwrap();
                let empty: ArrayRef = Arc::new(Int32Array::from(Vec::<Option<i32>>::new()));
                let empty_args = [EvaluatedArgument::Column(&empty)];
                let mut empty_part = WindowEvaluationPartition::begin(
                    prepared.clone(),
                    geometry(&prepared, &empty_args, &[], &[]),
                    &Control::default(),
                )
                .unwrap();
                assert!(
                    empty_part
                        .evaluate(Selection::all(0), 0, &Control::default())
                        .unwrap()
                        .values()
                        .is_empty()
                );
                empty_part.finish(&Control::default()).unwrap();
            }
        }
    }
}

#[test]
fn exact_offset_zero_two_max_and_nullable_non_null_cv_preserve_selected_ordinal() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let frames = whole_frames(6);
    let peers = [WindowRowRange { start: 0, end: 6 }];
    for name in NAMES {
        for ignore in [false, true] {
            for offset in [0, 2, i64::MAX] {
                let offset_cv = constant(
                    Arc::new(Int64Array::from(vec![Some(-1), Some(offset), None])),
                    true,
                    1,
                );
                let fixture = Fixture::offset(
                    name,
                    ty(&values, true),
                    offset_cv.clone(),
                    DecimalOverflowPolicy::ReportError,
                );
                let prepared = fixture
                    .prepare(options(ignore), &CompileControl::default())
                    .unwrap();
                assert_eq!(
                    prepared.contract().logical_argument_types().nth(1).unwrap(),
                    offset_cv.value_type()
                );
                let arguments = [
                    EvaluatedArgument::Column(&values),
                    EvaluatedArgument::Constant(&offset_cv),
                ];
                let mut part = WindowEvaluationPartition::begin(
                    prepared.clone(),
                    geometry(&prepared, &arguments, &peers, &frames),
                    &Control::default(),
                )
                .unwrap();
                let expected = match (offset, name, ignore) {
                    (0, _, _) => [None, Some(10), None, Some(30), Some(40), None],
                    (2, "lead", false) => [None, Some(30), Some(40), None, None, None],
                    (2, "lag", false) => [None, None, None, Some(10), None, Some(30)],
                    (2, "lead", true) => [Some(30), Some(40), Some(40), None, None, None],
                    (2, "lag", true) => [None, None, None, None, Some(10), Some(30)],
                    (i64::MAX, _, _) => [None; 6],
                    _ => unreachable!("explicit hand oracles"),
                };
                assert_eq!(
                    ints(
                        &part
                            .evaluate(Selection::all(6), 6, &Control::default())
                            .unwrap()
                    ),
                    expected
                );
            }
        }
    }
}

#[test]
fn sliced_scalar_nonzero_cv_and_dense_compact_sources_keep_original_addresses() {
    let backing: ArrayRef = Arc::new(Int32Array::from(vec![99, 7, 8, 9, 77]));
    let sliced = backing.slice(1, 3);
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![42]));
    let source_cv = constant(backing.clone(), false, 2);
    let compact = SelectedValues::try_new(
        Selection::all(3),
        sliced.data_type(),
        sliced.clone(),
        Box::default(),
    )
    .unwrap();
    let frames = whole_frames(3);
    let peers = [WindowRowRange { start: 0, end: 3 }];
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&backing, false), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(false), &CompileControl::default())
            .unwrap();
        for (argument, lead, lag) in [
            (
                EvaluatedArgument::Column(&sliced),
                [Some(8), None],
                [None, Some(8)],
            ),
            (
                EvaluatedArgument::SelectedColumn(&compact),
                [Some(8), None],
                [None, Some(8)],
            ),
            (
                EvaluatedArgument::Scalar(&scalar),
                [Some(42), None],
                [None, Some(42)],
            ),
            (
                EvaluatedArgument::Constant(&source_cv),
                [Some(8), None],
                [None, Some(8)],
            ),
        ] {
            let arguments = [argument];
            let mut part = WindowEvaluationPartition::begin(
                prepared.clone(),
                geometry(&prepared, &arguments, &peers, &frames),
                &Control::default(),
            )
            .unwrap();
            let rows = [0, 2];
            assert_eq!(
                ints(
                    &part
                        .evaluate(
                            Selection::try_sparse(3, &rows).unwrap(),
                            2,
                            &Control::default()
                        )
                        .unwrap()
                ),
                if name == "lead" { lead } else { lag }
            );
        }
    }
}

#[test]
fn physical_dictionary_null_rule_is_preserved_without_value_null_reinterpretation() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1)]),
            Arc::new(StringArray::from(vec![None, Some("visible")])),
        )
        .unwrap(),
    );
    assert!(!dictionary.is_null(0));
    let frames = whole_frames(4);
    let peers = [WindowRowRange { start: 0, end: 4 }];
    let arguments = [EvaluatedArgument::Column(&dictionary)];
    for name in NAMES {
        let fixture = Fixture::new(
            name,
            ty(&dictionary, true),
            DecimalOverflowPolicy::OutputNull,
        );
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &arguments, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        let out = part
            .evaluate(Selection::all(4), 4, &Control::default())
            .unwrap();
        let array = out
            .values()
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(
            array.keys().iter().collect::<Vec<_>>(),
            if name == "lead" {
                vec![Some(1), Some(1), Some(1), None]
            } else {
                vec![None, Some(0), Some(1), Some(1)]
            }
        );
        assert!(array.values().is_null(0));
    }
}

#[test]
fn exact_nested_source_fresh_and_frozen_keep_same_installed_attachment_and_metadata() {
    let field = Arc::new(
        Field::new("original", DataType::Int32, false)
            .with_metadata([("long".into(), "z".repeat(1300))].into()),
    );
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![field.clone()].into(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let offset = cv(Some(0), false);
    let arguments = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Constant(&offset),
    ];
    let frames = whole_frames(3);
    let peers = [WindowRowRange { start: 0, end: 3 }];
    for name in NAMES {
        let fixture = Fixture::offset(
            name,
            ty(&values, true),
            offset.clone(),
            DecimalOverflowPolicy::ReportError,
        );
        let options = options(true);
        let fresh = fixture
            .prepare(options, &CompileControl::default())
            .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                fixture
                    .catalog
                    .definition(name, FunctionKind::Window)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let signatures = [
            "(any<T>)->any<T>;strict;legacy",
            "(any<T>,i64)->any<T>;strict;legacy",
            "(any<T>,i64,any<D>)->any<T>;strict;legacy",
        ];
        let installed = signatures.map(|signature| InstalledPureKernel {
            function: fixture.function.clone(),
            kind: FunctionKind::Window,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!("builtin.window/{name}/{signature}"))
                    .unwrap(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.window/{name}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::WindowV1,
            },
            aggregate_state_format: None,
        });
        let subset = builder.seal_pure(installed).unwrap();
        let frozen = subset
            .prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                fresh.contract().call().effects(),
                PureCallPreparation::Window {
                    arguments: ScopedExpressionEffects::pure_value(context()),
                    options,
                },
                &CompileControl::default(),
            )
            .unwrap();
        assert_eq!(frozen.source(), PurePreparationSource::Frozen);
        let PreparedPureKernel::Window(frozen) = frozen.into_prepared() else {
            panic!("WindowV1")
        };
        assert!(Arc::ptr_eq(
            frozen.contract().call().selected_owner(),
            &fixture.selected
        ));
        assert_eq!(*frozen.contract().options(), options);
        let mut part = WindowEvaluationPartition::begin(
            frozen.clone(),
            geometry(&frozen, &arguments, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        let output = part
            .evaluate(Selection::all(3), 3, &Control::default())
            .unwrap();
        let output = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(novarocks_type_contract::arrow_data_types_exact(
            output.data_type(),
            values.data_type()
        ));
        assert_eq!(output.fields()[0].metadata(), field.metadata());
        assert_eq!(
            output.nulls().unwrap().iter().collect::<Vec<_>>(),
            [true, false, true]
        );
        let drift = [
            FunctionArgument::Value {
                value_type: FunctionValueType::new(
                    DataType::Struct(
                        vec![Arc::new(Field::new("drift", DataType::Int32, false))].into(),
                    ),
                    true,
                ),
                constant: None,
            },
            fixture.arguments[1].clone(),
        ];
        let mut input = fixture.input();
        input.request.arguments = &drift;
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    fixture.selected.clone(),
                    PureCallPreparation::Window {
                        arguments: ScopedExpressionEffects::pure_value(context()),
                        options
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
    }
}

#[test]
fn dynamic_null_negative_and_unsupported_defaults_refuse_before_partition_publication() {
    let source = FunctionValueType::new(DataType::Int32, true);
    for name in NAMES {
        let dynamic = Fixture::with_arguments(
            name,
            vec![
                FunctionArgument::Value {
                    value_type: source.clone(),
                    constant: None,
                },
                FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Int64, true),
                    constant: None,
                },
            ],
            DecimalOverflowPolicy::OutputNull,
        );
        for fixture in [
            dynamic,
            Fixture::offset(
                name,
                source.clone(),
                cv(None, true),
                DecimalOverflowPolicy::OutputNull,
            ),
            Fixture::offset(
                name,
                source.clone(),
                cv(Some(-1), false),
                DecimalOverflowPolicy::OutputNull,
            ),
        ] {
            assert!(matches!(
                fixture.prepare(options(false), &CompileControl::default()),
                Err(FunctionSpecializationFailure::Kernel(
                    KernelFailure::InvalidProgram(_)
                ))
            ));
        }
        for default in [
            FunctionValueType::new(DataType::Boolean, true),
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::LargeUtf8, true),
        ] {
            let offset = cv(Some(1), false);
            let fixture = Fixture::with_arguments(
                name,
                vec![
                    FunctionArgument::Value {
                        value_type: source.clone(),
                        constant: None,
                    },
                    FunctionArgument::Value {
                        value_type: offset.value_type().clone(),
                        constant: Some(offset),
                    },
                    FunctionArgument::Value {
                        value_type: default,
                        constant: None,
                    },
                ],
                DecimalOverflowPolicy::OutputNull,
            );
            assert!(matches!(
                fixture.prepare(options(false), &CompileControl::default()),
                Err(FunctionSpecializationFailure::Kernel(
                    KernelFailure::InvalidProgram(_)
                ))
            ));
        }
    }
}

#[test]
fn every_compile_callback_keeps_three_primary_causes_on_success_and_ordinary_offset_tails() {
    for name in NAMES {
        let source = FunctionValueType::new(DataType::Int32, true);
        for offset in [Some(1), Some(-1), None] {
            let fixture = Fixture::offset(
                name,
                source.clone(),
                cv(offset, true),
                DecimalOverflowPolicy::OutputNull,
            );
            let baseline = CompileControl::default();
            assert_eq!(
                fixture.prepare(options(true), &baseline).is_ok(),
                offset == Some(1)
            );
            let trace = baseline.trace.lock().unwrap().clone();
            assert!(trace.len() > 2);
            assert_eq!(trace.last().unwrap().1, 0, "original catalogue exit");
            if offset == Some(-1) {
                assert_eq!(
                    trace[trace.len() - 2].1,
                    1,
                    "negative-offset own ordinary tail"
                );
            }
            for at in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = CompileControl {
                        trace: Mutex::default(),
                        refusal: Some((at, cause)),
                    };
                    assert_eq!(
                        compile_cause(fixture.prepare(options(true), &control).unwrap_err()),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
        // An unsupported default carrier keeps its ordinary error tail and
        // original control rather than publishing a guessed conversion.
        let offset = cv(Some(1), false);
        let fixture = Fixture::with_arguments(
            name,
            vec![
                FunctionArgument::Value {
                    value_type: source.clone(),
                    constant: None,
                },
                FunctionArgument::Value {
                    value_type: offset.value_type().clone(),
                    constant: Some(offset),
                },
                FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Float64, true),
                    constant: None,
                },
            ],
            DecimalOverflowPolicy::OutputNull,
        );
        let baseline = CompileControl::default();
        assert!(fixture.prepare(options(false), &baseline).is_err());
        let trace = baseline.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause)),
                };
                assert_eq!(
                    compile_cause(fixture.prepare(options(false), &control).unwrap_err()),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn actual_window_wrapper_and_ordinary_leaf_tail_keep_seven_causes_and_failed_latch() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = whole_frames(6);
    let rows = [1, 4, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, &arguments, &peers, &frames);
        // Setup, selected output, empty output and finish all run through the
        // real lifecycle owner. Stage four exercises this leaf's ordinary tail.
        for stage in 0..5 {
            let baseline = Control::default();
            if stage == 0 {
                WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
            } else if stage == 4 {
                let mut part = prepared
                    .clone()
                    .begin_partition(input, &Control::default())
                    .unwrap();
                assert!(matches!(
                    part.evaluate(Selection::all(7), 7, &baseline),
                    Err(KernelFailure::InvalidProgram(_))
                ));
            } else {
                let mut part =
                    WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                        .unwrap();
                match stage {
                    1 => {
                        assert_eq!(
                            ints(&part.evaluate(selection, 3, &baseline).unwrap()),
                            rows.map(|row| hand(name, true)[row])
                        );
                    }
                    2 => {
                        assert!(
                            part.evaluate(Selection::try_sparse(6, &[]).unwrap(), 0, &baseline)
                                .unwrap()
                                .values()
                                .is_empty()
                        );
                    }
                    _ => part.finish(&baseline).unwrap(),
                }
            }
            let trace = baseline.trace.lock().unwrap().clone();
            assert!(trace.len() >= 2);
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    if stage == 0 {
                        assert_eq!(
                            WindowEvaluationPartition::begin(prepared.clone(), input, &control)
                                .map(|_| ())
                                .unwrap_err(),
                            cause
                        );
                    } else if stage == 4 {
                        let mut part = prepared
                            .clone()
                            .begin_partition(input, &Control::default())
                            .unwrap();
                        assert_eq!(
                            part.evaluate(Selection::all(7), 7, &control).unwrap_err(),
                            cause
                        );
                        let clean = Control::default();
                        assert_eq!(part.finish(&clean), Err(KernelFailure::InstanceFailed));
                        assert!(clean.trace.lock().unwrap().is_empty());
                    } else {
                        let mut part = WindowEvaluationPartition::begin(
                            prepared.clone(),
                            input,
                            &Control::default(),
                        )
                        .unwrap();
                        let result = match stage {
                            1 => part.evaluate(selection, 3, &control).map(|_| ()),
                            2 => part
                                .evaluate(Selection::try_sparse(6, &[]).unwrap(), 0, &control)
                                .map(|_| ()),
                            _ => part.finish(&control),
                        };
                        assert_eq!(result.unwrap_err(), cause);
                        let clean = Control::default();
                        assert_eq!(
                            part.evaluate(selection, 3, &clean).unwrap_err(),
                            KernelFailure::InstanceFailed
                        );
                        assert_eq!(part.finish(&clean), Err(KernelFailure::InstanceFailed));
                        assert!(clean.trace.lock().unwrap().is_empty());
                    }
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn wide_required_null_scan_and_selected_take_cross_real_quantum_with_sampled_causes() {
    let values: ArrayRef = Arc::new(Int32Array::from(
        (0..320)
            .map(|row| (row != 0).then_some(row))
            .collect::<Vec<_>>(),
    ));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 320 }];
    let frames = whole_frames(320);
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, &arguments, &peers, &frames);
        for setup in [true, false] {
            let baseline = Control::default();
            if setup {
                WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
            } else {
                let mut part =
                    WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                        .unwrap();
                let output = part.evaluate(Selection::all(320), 320, &baseline).unwrap();
                let expected: Vec<_> = (0..320)
                    .map(|row| {
                        if name == "lead" {
                            (row < 319).then_some(row + 1)
                        } else {
                            (row > 1).then_some(row - 1)
                        }
                    })
                    .collect();
                assert_eq!(ints(&output), expected);
            }
            let trace = baseline.trace.lock().unwrap().clone();
            let quantum = trace.iter().position(|units| *units == 256).unwrap();
            for at in [0, quantum, trace.len() - 1] {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    let outcome = if setup {
                        WindowEvaluationPartition::begin(prepared.clone(), input, &control)
                            .map(|_| ())
                    } else {
                        let mut part = WindowEvaluationPartition::begin(
                            prepared.clone(),
                            input,
                            &Control::default(),
                        )
                        .unwrap();
                        part.evaluate(Selection::all(320), 320, &control)
                            .map(|_| ())
                    };
                    assert_eq!(outcome.unwrap_err(), cause);
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
        assert_eq!(
            prepared.partition_retained_upper_bound(usize::MAX),
            Err(KernelFailure::ResourceExhausted)
        );
    }
}

#[test]
fn original_contract_full_input_capacity_and_unsupported_copy_refuse_without_fallback() {
    use arrow_array::UnionArray;
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::UnionFields;
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let fixture = Fixture::new(
        "lead",
        ty(&values, false),
        DecimalOverflowPolicy::OutputNull,
    );
    let a = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let b = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let args = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = whole_frames(2);
    assert!(matches!(
        a.clone()
            .begin_partition(geometry(&b, &args, &peers, &frames), &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let mut part = WindowEvaluationPartition::begin(
        a.clone(),
        geometry(&a, &args, &peers, &frames),
        &Control::default(),
    )
    .unwrap();
    assert!(matches!(
        part.evaluate(Selection::all(2), 1, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(
        part.finish(&Control::default()),
        Err(KernelFailure::InstanceFailed)
    );
    let wrong: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::Column(&wrong)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let null: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1)]));
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::Column(&null)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let error_values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1)]));
    let child = SelectedValues::try_new(
        Selection::all(2),
        &DataType::Int32,
        error_values,
        vec![RowDataError::new(0, "required child failure")].into(),
    )
    .unwrap();
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::SelectedColumn(&child)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let fields =
        UnionFields::try_new([0], [Field::new("original", DataType::Int32, true)]).unwrap();
    let union: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 0]),
            Some(ScalarBuffer::from(vec![0_i32, 1])),
            vec![Arc::new(Int32Array::from(vec![7, 8]))],
        )
        .unwrap(),
    );
    let fixture = Fixture::new("lead", ty(&union, true), DecimalOverflowPolicy::OutputNull);
    let prepared = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let arguments = [EvaluatedArgument::Column(&union)];
    // The original shared take author refuses padding a Union with a null
    // index. Required setup exposes this even before an empty output demand.
    assert!(matches!(
        WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &arguments, &peers, &frames),
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

fn default_fixture(
    name: &str,
    source: FunctionValueType,
    default: FunctionValueType,
    overflow: DecimalOverflowPolicy,
) -> Fixture {
    let offset = cv(Some(1), false);
    Fixture::with_arguments(
        name,
        vec![
            FunctionArgument::Value {
                value_type: source,
                constant: None,
            },
            FunctionArgument::Value {
                value_type: offset.value_type().clone(),
                constant: Some(offset),
            },
            FunctionArgument::Value {
                value_type: default,
                constant: None,
            },
        ],
        overflow,
    )
}

#[test]
fn three_arguments_convert_complete_defaults_and_only_replace_missing_targets() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(10), None, Some(30)]));
    let defaults: ArrayRef = Arc::new(StringArray::from(vec![
        Some("101"),
        Some("bad"),
        Some("303"),
        Some("404"),
    ]));
    let offset: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let peers = [WindowRowRange { start: 0, end: 4 }];
    let frames = whole_frames(4);
    let rows = [1, 3];
    for name in NAMES {
        for ignore in [false, true] {
            for overflow in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let fixture =
                    default_fixture(name, ty(&source, true), ty(&defaults, true), overflow);
                let prepared = fixture
                    .prepare(options(ignore), &CompileControl::default())
                    .unwrap();
                let args = [
                    EvaluatedArgument::Column(&source),
                    EvaluatedArgument::Scalar(&offset),
                    EvaluatedArgument::Column(&defaults),
                ];
                let mut part = WindowEvaluationPartition::begin(
                    prepared.clone(),
                    geometry(&prepared, &args, &peers, &frames),
                    &Control::default(),
                )
                .unwrap();
                let expected = match (name, ignore) {
                    ("lead", false) => vec![Some(10), None, Some(30), Some(404)],
                    ("lead", true) => vec![Some(10), Some(30), Some(30), Some(404)],
                    ("lag", false) => vec![Some(101), None, Some(10), None],
                    ("lag", true) => vec![Some(101), None, Some(10), Some(10)],
                    _ => unreachable!(),
                };
                let all = part
                    .evaluate(Selection::all(4), 4, &Control::default())
                    .unwrap();
                assert_eq!(
                    all.values()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected
                );
                let sparse = part
                    .evaluate(
                        Selection::try_sparse(4, &rows).unwrap(),
                        2,
                        &Control::default(),
                    )
                    .unwrap();
                assert_eq!(
                    sparse
                        .values()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![expected[1], expected[3]]
                );
                assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
                assert!(
                    part.evaluate(
                        Selection::try_sparse(4, &[]).unwrap(),
                        0,
                        &Control::default()
                    )
                    .unwrap()
                    .values()
                    .is_empty()
                );
                part.finish(&Control::default()).unwrap();
            }
        }
    }
    // I64-to-Utf8 conversion uses the original Arrow formatter. A real NULL
    // target remains NULL; the default only serves an absent target.
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("c"),
        Some("d"),
    ]));
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, 2, 3, i64::MAX]));
    let fixture = default_fixture(
        "lead",
        ty(&text, true),
        ty(&numbers, false),
        DecimalOverflowPolicy::OutputNull,
    );
    let prepared = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Scalar(&offset),
        EvaluatedArgument::Column(&numbers),
    ];
    let mut part = WindowEvaluationPartition::begin(
        prepared.clone(),
        geometry(&prepared, &args, &peers, &frames),
        &Control::default(),
    )
    .unwrap();
    let output = part
        .evaluate(Selection::all(4), 4, &Control::default())
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some("c"), Some("d"), Some("9223372036854775807")]
    );
}

#[test]
fn default_complete_setup_and_selected_copy_preserve_every_original_control_prefix() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let defaults: ArrayRef = Arc::new(StringArray::from(vec!["5", "bad", "9"]));
    let offset: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&offset),
        EvaluatedArgument::Column(&defaults),
    ];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = whole_frames(3);
    let rows = [0, 2];
    for name in NAMES {
        let fixture = default_fixture(
            name,
            ty(&source, false),
            ty(&defaults, false),
            DecimalOverflowPolicy::OutputNull,
        );
        let compilation = CompileControl::default();
        let prepared = fixture.prepare(options(false), &compilation).unwrap();
        let trace = compilation.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause)),
                };
                assert_eq!(
                    compile_cause(fixture.prepare(options(false), &control).unwrap_err()),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
        for stage in 0..4 {
            let baseline = Control::default();
            let input = geometry(&prepared, &args, &peers, &frames);
            if stage == 0 {
                WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
            } else {
                let mut part =
                    WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                        .unwrap();
                let selection = match stage {
                    1 => Selection::all(3),
                    2 => Selection::try_sparse(3, &rows).unwrap(),
                    _ => Selection::try_sparse(3, &[]).unwrap(),
                };
                part.evaluate(selection, 3, &baseline).unwrap();
            }
            let trace = baseline.trace.lock().unwrap().clone();
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    let outcome = if stage == 0 {
                        WindowEvaluationPartition::begin(prepared.clone(), input, &control)
                            .map(|_| ())
                    } else {
                        let mut part = WindowEvaluationPartition::begin(
                            prepared.clone(),
                            input,
                            &Control::default(),
                        )
                        .unwrap();
                        let selection = match stage {
                            1 => Selection::all(3),
                            2 => Selection::try_sparse(3, &rows).unwrap(),
                            _ => Selection::try_sparse(3, &[]).unwrap(),
                        };
                        let result = part.evaluate(selection, 3, &control).map(|_| ());
                        assert_eq!(
                            part.finish(&Control::default()),
                            Err(KernelFailure::InstanceFailed)
                        );
                        result
                    };
                    assert_eq!(outcome.unwrap_err(), cause);
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn complete_default_logical_addresses_and_required_child_errors_remain_original() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![5, 6, 7, 8]));
    let offset: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let default_pool: ArrayRef = Arc::new(StringArray::from(vec!["bad", "19", "bad"]));
    let default_cv = constant(default_pool, false, 1);
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["27"]));
    let sliced: ArrayRef =
        Arc::new(StringArray::from(vec!["bad", "31", "32", "33", "34", "bad"]).slice(1, 4));
    let compact_values: ArrayRef = Arc::new(StringArray::from(vec!["41", "42", "43", "44"]));
    let compact = SelectedValues::try_new(
        Selection::all(4),
        &DataType::Utf8,
        compact_values,
        Box::default(),
    )
    .unwrap();
    let peers = [WindowRowRange { start: 0, end: 4 }];
    let frames = whole_frames(4);
    for (argument, expected) in [
        (EvaluatedArgument::Constant(&default_cv), 19),
        (EvaluatedArgument::Scalar(&scalar), 27),
        (EvaluatedArgument::Column(&sliced), 34),
        (EvaluatedArgument::SelectedColumn(&compact), 44),
    ] {
        let fixture = default_fixture(
            "lead",
            ty(&source, false),
            FunctionValueType::new(DataType::Utf8, false),
            DecimalOverflowPolicy::OutputNull,
        );
        let prepared = fixture
            .prepare(options(false), &CompileControl::default())
            .unwrap();
        let args = [
            EvaluatedArgument::Column(&source),
            EvaluatedArgument::Scalar(&offset),
            argument,
        ];
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &args, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        let output = part
            .evaluate(Selection::all(4), 4, &Control::default())
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(6), Some(7), Some(8), Some(expected)]
        );
    }
    let nulls: ArrayRef = Arc::new(arrow_array::NullArray::new(4));
    let fixture = default_fixture(
        "lead",
        ty(&source, false),
        ty(&nulls, true),
        DecimalOverflowPolicy::ReportError,
    );
    let prepared = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&offset),
        EvaluatedArgument::Column(&nulls),
    ];
    let mut part = WindowEvaluationPartition::begin(
        prepared.clone(),
        geometry(&prepared, &args, &peers, &frames),
        &Control::default(),
    )
    .unwrap();
    let output = part
        .evaluate(Selection::all(4), 4, &Control::default())
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(6), Some(7), Some(8), None]
    );

    let bad_values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("1"),
        Some("2"),
        Some("3"),
        None,
    ]));
    let required = SelectedValues::try_new(
        Selection::all(4),
        &DataType::Utf8,
        bad_values,
        vec![RowDataError::new(3, "required default child")].into(),
    )
    .unwrap();
    let fixture = default_fixture(
        "lead",
        ty(&source, false),
        FunctionValueType::new(DataType::Utf8, true),
        DecimalOverflowPolicy::OutputNull,
    );
    let prepared = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    // Row three could be absent from every output demand. Required D is still
    // validated over the complete partition before any cursor is published.
    assert!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            4,
            &[
                EvaluatedArgument::Column(&source),
                EvaluatedArgument::Scalar(&offset),
                EvaluatedArgument::SelectedColumn(&required)
            ],
            &[],
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn three_argument_wide_required_setup_and_output_sample_real_quantum_without_type_retag() {
    let source: ArrayRef = Arc::new(StringArray::from(vec!["a"; 320]));
    let defaults: ArrayRef = Arc::new(Int64Array::from((0_i64..320).collect::<Vec<_>>()));
    let offset: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let fixture = default_fixture(
        "lead",
        ty(&source, false),
        ty(&defaults, false),
        DecimalOverflowPolicy::ReportError,
    );
    let prepared = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&offset),
        EvaluatedArgument::Column(&defaults),
    ];
    let peers = [WindowRowRange { start: 0, end: 320 }];
    let frames = whole_frames(320);
    let input = geometry(&prepared, &args, &peers, &frames);
    for stage in 0..2 {
        let baseline = Control::default();
        if stage == 0 {
            WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
        } else {
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            let output = part.evaluate(Selection::all(320), 320, &baseline).unwrap();
            let strings = output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(strings.value(0), "a");
            assert_eq!(strings.value(318), "a");
            assert_eq!(strings.value(319), "319");
            assert_eq!(strings.null_count(), 0);
            assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
            let selected = [0, 255, 319];
            let output = part
                .evaluate(
                    Selection::try_sparse(320, &selected).unwrap(),
                    3,
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some("a"), Some("a"), Some("319")]
            );
        }
        let trace = baseline.trace.lock().unwrap().clone();
        let quantum = trace
            .iter()
            .position(|units| *units == 256)
            .expect("actual required conversion or selected copy crosses its quantum");
        for at in [0, quantum, trace.len() - 1] {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                let outcome = if stage == 0 {
                    WindowEvaluationPartition::begin(prepared.clone(), input, &control).map(|_| ())
                } else {
                    let mut part = WindowEvaluationPartition::begin(
                        prepared.clone(),
                        input,
                        &Control::default(),
                    )
                    .unwrap();
                    part.evaluate(Selection::all(320), 320, &control)
                        .map(|_| ())
                };
                assert_eq!(outcome.unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
