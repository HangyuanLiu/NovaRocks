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

//! Installed basic owners over explicit frames, with independent expected
//! values and every compile/runtime checkpoint refusal kept primary.

use super::*;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters, WindowFrame,
};
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
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
        panic!("aggregate OVER never waits")
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
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}

impl Fixture {
    fn new(name: &str, source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        Self::new_multi(name, &[source], policy)
    }
    fn new_multi(name: &str, sources: &[FunctionValueType], policy: DecimalOverflowPolicy) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = sources
            .iter()
            .map(|source| FunctionArgument::Value {
                value_type: source.clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let resolved = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap_or_else(|error| panic!("{name} binds: {error}"));
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            uses: (0..sources.len())
                .map(|index| Some(ExpressionUseId::new(11 + index as u32)))
                .collect(),
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: context(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Aggregate,
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
    fn aggregate_options(distinct: bool) -> AggregatePreparationOptions {
        AggregatePreparationOptions {
            phase: AggregateKernelPhase::Single,
            distinct,
            order_keys: Arc::from([]),
            state_input_type: None,
        }
    }
    fn try_window(
        &self,
        frame: Option<WindowFrame<u64>>,
        ignore_nulls: bool,
        distinct: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let prepared = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            PureCallPreparation::AggregateWindow {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options: AggregateWindowPreparationOptions {
                    aggregate: Self::aggregate_options(distinct),
                    window: WindowCallOptions::try_new(
                        frame,
                        ignore_nulls,
                        &CompileControl::default(),
                    )
                    .unwrap(),
                },
            },
            control,
        )?;
        assert_eq!(
            prepared.implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
        match prepared.into_prepared() {
            PreparedPureKernel::Window(kernel) => Ok(kernel),
            _ => panic!("aggregate OVER prepares a window kernel"),
        }
    }
    fn window(&self) -> Arc<dyn PreparedWindowKernel> {
        self.try_window(None, false, false, &CompileControl::default())
            .unwrap_or_else(|error| panic!("aggregate OVER prepares: {error}"))
    }
}

fn ranges(values: &[(usize, usize)]) -> Vec<WindowRowRange> {
    values
        .iter()
        .map(|&(start, end)| WindowRowRange { start, end })
        .collect()
}

/// One peer group covering the partition: the adapter reads only frames.
fn whole(rows: usize) -> Vec<WindowRowRange> {
    if rows == 0 {
        Vec::new()
    } else {
        ranges(&[(0, rows)])
    }
}

fn begin<'a>(
    kernel: &'a Arc<dyn PreparedWindowKernel>,
    arguments: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
    control: &dyn KernelEvaluationControl,
) -> Result<WindowEvaluationPartition<'a>, KernelFailure> {
    let full = FullPartitionWindowInput::try_new(
        kernel.contract(),
        frames.len(),
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    let input = WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap();
    WindowEvaluationPartition::begin(kernel.clone(), input, control)
}

/// Every row's window value, or the partition's failure.
fn run_window(
    kernel: &Arc<dyn PreparedWindowKernel>,
    values: &ArrayRef,
    frames: &[WindowRowRange],
) -> Result<ArrayRef, KernelFailure> {
    let control = Control::default();
    let rows = frames.len();
    let arguments = [EvaluatedArgument::Column(values)];
    let peers = whole(rows);
    let mut partition = begin(kernel, &arguments, &peers, frames, &control)?;
    // One inline state per row and nothing else is retained.
    assert_eq!(
        partition.retained_bytes().unwrap(),
        partition.retained_upper_bound()
    );
    let output = partition.evaluate(Selection::all(rows), rows, &control)?;
    partition.finish(&control)?;
    let (_, values, errors) = output.into_parts();
    assert!(errors.is_empty());
    Ok(values)
}

#[test]
fn basic_window_average_frames_have_independent_expected_values() {
    let fixture = Fixture::new(
        "avg",
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
    );
    let window = fixture.window();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(3), None, Some(8)]));
    let frames = ranges(&[(0, 0), (0, 2), (0, 3), (1, 4)]);
    let output = run_window(&window, &values, &frames).unwrap();
    let expected = Float64Array::from(vec![None, Some(2.0), Some(2.0), Some(5.5)]);
    assert_eq!(output.as_ref(), &expected);
}
#[test]
fn basic_window_boolean_frames_distinguish_no_rows_from_null_rows() {
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![
        None,
        Some(true),
        Some(false),
        None,
    ]));
    let frames = ranges(&[(0, 0), (0, 1), (0, 2), (0, 3)]);
    for (name, expected) in [
        (
            "bool_or",
            BooleanArray::from(vec![None, Some(false), Some(true), Some(true)]),
        ),
        (
            "bool_and",
            BooleanArray::from(vec![None, Some(true), Some(true), Some(false)]),
        ),
    ] {
        let fixture = Fixture::new(
            name,
            FunctionValueType::new(DataType::Boolean, true),
            DecimalOverflowPolicy::OutputNull,
        );
        let output = run_window(&fixture.window(), &values, &frames).unwrap();
        assert_eq!(output.as_ref(), &expected);
    }
    let fixture = Fixture::new(
        "count_if",
        FunctionValueType::new(DataType::Boolean, true),
        DecimalOverflowPolicy::OutputNull,
    );
    let output = run_window(&fixture.window(), &values, &frames).unwrap();
    assert_eq!(output.as_ref(), &Int64Array::from(vec![0, 0, 1, 1]));
}
#[test]
fn basic_window_variance_and_std_frames_have_independent_expected_values() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None]));
    let frames = ranges(&[(0, 0), (0, 1), (0, 3), (1, 4)]);
    for (name, expected) in [
        (
            "var_pop",
            vec![None, Some(0.0), Some(2.0 / 3.0), Some(0.25)],
        ),
        ("var_samp", vec![None, None, Some(1.0), Some(0.5)]),
        (
            "stddev_pop",
            vec![None, Some(0.0), Some((2.0f64 / 3.0).sqrt()), Some(0.5)],
        ),
        (
            "stddev_samp",
            vec![None, None, Some(1.0), Some(0.5f64.sqrt())],
        ),
    ] {
        let fixture = Fixture::new(
            name,
            FunctionValueType::new(DataType::Int64, true),
            DecimalOverflowPolicy::OutputNull,
        );
        let output = run_window(&fixture.window(), &values, &frames).unwrap();
        assert_eq!(output.as_ref(), &Float64Array::from(expected));
    }
}
#[test]
fn basic_window_compile_and_runtime_refusals_remain_primary() {
    let fixture = Fixture::new(
        "avg",
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
    );
    let successful = CompileControl::default();
    fixture.try_window(None, false, false, &successful).unwrap();
    let calls = successful.trace.lock().unwrap().len();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..calls {
            let c = CompileControl {
                refusal: Some((stop, cause)),
                ..CompileControl::default()
            };
            assert!(fixture.try_window(None, false, false, &c).is_err());
        }
    }
    let window = fixture.window();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(3), None, Some(8)]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = whole(4);
    let frames = ranges(&[(0, 0), (0, 2), (0, 3), (1, 4)]);
    let successful = Control::default();
    drop(begin(&window, &arguments, &peers, &frames, &successful).unwrap());
    let calls = successful.trace.lock().unwrap().len();
    for cause in causes() {
        for stop in 0..calls {
            let c = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let result = begin(&window, &arguments, &peers, &frames, &c);
            match result {
                Err(error) => assert_eq!(error, cause),
                Ok(_) => panic!("expected original refusal"),
            }
        }
    }
}

#[test]
fn basic_window_pairwise_moments_use_only_complete_pairs() {
    let x: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(3)]));
    let y: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(4), Some(99), Some(6)]));
    let arguments = [EvaluatedArgument::Column(&x), EvaluatedArgument::Column(&y)];
    let frames = ranges(&[(0, 0), (0, 1), (0, 3), (0, 4)]);
    let peers = whole(4);
    for (name, expected) in [
        (
            "covar_pop",
            vec![None, Some(0.0), Some(0.5), Some(4.0 / 3.0)],
        ),
        ("covar_samp", vec![None, None, Some(1.0), Some(2.0)]),
        ("corr", vec![None, None, Some(1.0), Some(1.0)]),
    ] {
        let fixture = Fixture::new_multi(
            name,
            &[
                FunctionValueType::new(DataType::Int64, true),
                FunctionValueType::new(DataType::Int64, true),
            ],
            DecimalOverflowPolicy::OutputNull,
        );
        let window = fixture.window();
        let control = Control::default();
        let mut partition = begin(&window, &arguments, &peers, &frames, &control).unwrap();
        let (_, output, errors) = partition
            .evaluate(Selection::all(4), 4, &control)
            .unwrap()
            .into_parts();
        assert!(errors.is_empty());
        let actual = output.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, value) in expected.iter().enumerate() {
            match value {
                None => assert!(actual.is_null(row)),
                Some(value) => {
                    assert!(!actual.is_null(row));
                    assert!((actual.value(row) - value).abs() < 1e-14);
                }
            }
        }
        partition.finish(&control).unwrap();
    }
}

#[test]
fn shared_avg_core_accepts_legacy_state_carriers_and_preserves_decimal_state_scale() {
    let control = Control::default();
    let source = DataType::Float64;
    let output = DataType::Float64;
    let intermediate = DataType::Binary;
    let core = BasicComputation {
        operation: BasicOperation::Avg,
        domain: match source {
            DataType::Decimal128(..) => BasicStateDomain::Decimal128,
            DataType::Decimal256(..) => BasicStateDomain::Decimal256,
            _ => BasicStateDomain::Plain,
        },
        input_scale: match source {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(s),
            _ => None,
        },
        output_type: &output,
        intermediate_type: &intermediate,
    };
    let mut bytes = Vec::from(12.0f64.to_le_bytes());
    bytes.extend_from_slice(&3i64.to_le_bytes());
    let mut work = EvaluationCheckpoints::new(&control);
    let state = core.decode_bytes(&bytes, &mut work).unwrap();
    let result = core
        .build([Ok(state)].into_iter(), false, &control)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        4.0
    );
    let result = core.build([Ok(state)].into_iter(), true, &control).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        bytes
    );
    let decoded = core.decode_text("12,3", &mut work).unwrap();
    assert_eq!(decoded.sum, 12.0);
    assert_eq!(decoded.count, 3);

    let source = DataType::Decimal128(18, 2);
    let output = DataType::Decimal128(38, 6);
    let intermediate = DataType::Struct(
        vec![
            arrow_schema::Field::new("old_sum", DataType::Decimal128(38, 3), true),
            arrow_schema::Field::new("old_count", DataType::Int16, true),
        ]
        .into(),
    );
    let core = BasicComputation {
        operation: BasicOperation::Avg,
        domain: match source {
            DataType::Decimal128(..) => BasicStateDomain::Decimal128,
            DataType::Decimal256(..) => BasicStateDomain::Decimal256,
            _ => BasicStateDomain::Plain,
        },
        input_scale: match source {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(s),
            _ => None,
        },
        output_type: &output,
        intermediate_type: &intermediate,
    };
    let state = BasicState {
        decimal: 615,
        count: 2,
        ..Default::default()
    };
    let result = core
        .build([Ok(state)].into_iter(), false, &control)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        307500
    );
    let partial = core.build([Ok(state)].into_iter(), true, &control).unwrap();
    let partial = partial.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(partial.column(0).data_type(), &DataType::Decimal128(38, 3));
    assert_eq!(partial.column(1).data_type(), &DataType::Int64);
    assert_eq!(
        partial
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        615
    );
    assert_eq!(
        core.decode_optional(partial, 0, &mut work)
            .unwrap()
            .unwrap()
            .decimal,
        615
    );

    let source = DataType::Decimal256(60, 2);
    let output = source.clone();
    let intermediate = DataType::Struct(
        vec![
            arrow_schema::Field::new("sum", DataType::Decimal256(60, 3), true),
            arrow_schema::Field::new("count", DataType::Int64, true),
        ]
        .into(),
    );
    let core = BasicComputation {
        operation: BasicOperation::Avg,
        domain: match source {
            DataType::Decimal128(..) => BasicStateDomain::Decimal128,
            DataType::Decimal256(..) => BasicStateDomain::Decimal256,
            _ => BasicStateDomain::Plain,
        },
        input_scale: match source {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(s),
            _ => None,
        },
        output_type: &output,
        intermediate_type: &intermediate,
    };
    let result = core
        .build(
            [Ok(BasicState {
                wide: i256::from_i128(615),
                count: 2,
                ..Default::default()
            })]
            .into_iter(),
            false,
            &control,
        )
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap()
            .value(0),
        i256::from_i128(31)
    );
}

#[test]
fn shared_moment_core_retains_legacy_text_formats_and_diagnostic_messages() {
    let control = Control::default();
    let t = DataType::Float64;
    let state_type = DataType::Utf8;
    let core = BasicComputation {
        operation: BasicOperation::VarPop,
        domain: BasicStateDomain::Plain,
        input_scale: None,
        output_type: &t,
        intermediate_type: &state_type,
    };
    let mut work = EvaluationCheckpoints::new(&control);
    // The legacy variance parser deliberately ignores trailing fields.
    let state = core.decode_text("2,2,3,trailing", &mut work).unwrap();
    let result = core
        .build([Ok(state)].into_iter(), false, &control)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0 / 3.0
    );
    let partial = core.build([Ok(state)].into_iter(), true, &control).unwrap();
    assert_eq!(
        partial
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "2,2,3"
    );
    assert_eq!(
        core.decode_bytes(&[0; 8], &mut work)
            .unwrap_err()
            .into_kernel_failure(),
        failure("variance/stddev intermediate binary size mismatch: expected 24, got 8")
    );
    let avg = BasicComputation {
        operation: BasicOperation::Avg,
        domain: BasicStateDomain::Plain,
        input_scale: None,
        output_type: &t,
        intermediate_type: &state_type,
    };
    assert_eq!(
        avg.decode_text("hello,3", &mut work)
            .unwrap_err()
            .into_kernel_failure(),
        failure("invalid avg state sum 'hello': invalid float literal")
    );
    let long = "x".repeat(4096);
    assert_eq!(
        avg.decode_text(&long, &mut work)
            .unwrap_err()
            .into_legacy_message(),
        format!("invalid avg state '{}': missing ','", long)
    );
    let corr = BasicComputation {
        operation: BasicOperation::Corr,
        domain: BasicStateDomain::Plain,
        input_scale: None,
        output_type: &t,
        intermediate_type: &state_type,
    };
    let state = corr.decode_text("2,4,4,3,2,8", &mut work).unwrap();
    let result = corr
        .build([Ok(state)].into_iter(), false, &control)
        .unwrap();
    assert!(
        (result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            - 1.0)
            .abs()
            < 1e-15
    );
    assert_eq!(
        corr.decode_text("2,4,4,3,2,8,trailing", &mut work)
            .unwrap_err()
            .into_kernel_failure(),
        failure("corr utf8 state expects 6 parts")
    );
}

#[test]
fn basic_window_every_numeric_overload_and_alias_has_independent_values() {
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![1, 2, 3])),
        Arc::new(Int16Array::from(vec![1, 2, 3])),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
    ];
    let frames = ranges(&[(0, 3); 3]);
    let peers = whole(3);
    for input in &inputs {
        for (name, expected) in [
            ("avg", 2.0),
            ("variance", 2.0 / 3.0),
            ("variance_pop", 2.0 / 3.0),
            ("var_pop", 2.0 / 3.0),
            ("variance_samp", 1.0),
            ("var_samp", 1.0),
            ("stddev", (2.0f64 / 3.0).sqrt()),
            ("std", (2.0f64 / 3.0).sqrt()),
            ("stddev_pop", (2.0f64 / 3.0).sqrt()),
            ("stddev_samp", 1.0),
        ] {
            let f = Fixture::new(
                name,
                FunctionValueType::new(input.data_type().clone(), false),
                DecimalOverflowPolicy::OutputNull,
            );
            let result = run_window(&f.window(), input, &frames).unwrap();
            let a = result.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..3 {
                assert!(!a.is_null(row));
                assert!(
                    (a.value(row) - expected).abs() < 1e-15,
                    "{name} {:?}",
                    input.data_type()
                );
            }
        }
    }
    for x in &inputs {
        for y in &inputs {
            for (name, expected) in [("covar_pop", 2.0 / 3.0), ("covar_samp", 1.0), ("corr", 1.0)] {
                let f = Fixture::new_multi(
                    name,
                    &[
                        FunctionValueType::new(x.data_type().clone(), false),
                        FunctionValueType::new(y.data_type().clone(), false),
                    ],
                    DecimalOverflowPolicy::OutputNull,
                );
                let kernel = f.window();
                let control = Control::default();
                let args = [EvaluatedArgument::Column(x), EvaluatedArgument::Column(y)];
                let mut p = begin(&kernel, &args, &peers, &frames, &control).unwrap();
                let (_, out, errors) = p
                    .evaluate(Selection::all(3), 3, &control)
                    .unwrap()
                    .into_parts();
                assert!(errors.is_empty());
                let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
                for row in 0..3 {
                    assert!(
                        (out.value(row) - expected).abs() < 1e-15,
                        "{name} {:?}/{:?}",
                        x.data_type(),
                        y.data_type()
                    );
                }
                p.finish(&control).unwrap();
            }
        }
    }
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![100, 200, 300])
            .with_precision_and_scale(18, 2)
            .unwrap(),
    );
    let f = Fixture::new(
        "avg",
        FunctionValueType::new(values.data_type().clone(), false),
        DecimalOverflowPolicy::OutputNull,
    );
    let out = run_window(&f.window(), &values, &frames).unwrap();
    assert_eq!(out.data_type(), &DataType::Decimal128(38, 8));
    assert_eq!(
        out.as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(200_000_000); 3]
    );
    let values: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            i256::from_i128(100),
            i256::from_i128(200),
            i256::from_i128(300),
        ])
        .with_precision_and_scale(60, 2)
        .unwrap(),
    );
    let f = Fixture::new(
        "avg",
        FunctionValueType::new(values.data_type().clone(), false),
        DecimalOverflowPolicy::OutputNull,
    );
    let out = run_window(&f.window(), &values, &frames).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(i256::from_i128(200)); 3]
    );
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
    for (name, expected) in [
        ("bool_or", true),
        ("boolor_agg", true),
        ("bool_and", false),
        ("booland_agg", false),
    ] {
        let f = Fixture::new(
            name,
            FunctionValueType::new(DataType::Boolean, false),
            DecimalOverflowPolicy::OutputNull,
        );
        let out = run_window(&f.window(), &values, &frames).unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(expected); 3]
        );
    }
    let f = Fixture::new(
        "count_if",
        FunctionValueType::new(DataType::Boolean, false),
        DecimalOverflowPolicy::OutputNull,
    );
    let out = run_window(&f.window(), &values, &frames).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2); 3]
    );
}
