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
use crate::physical_binding_v2::finish;
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::{
    AggregatePhase, AggregateSequenceId, BoundFunction, LegacyBindingMetadata,
};
use novarocks_type_contract::{
    AggregateStateArgumentContract, CompileControlError, CompilePhase, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionOverloadId, FunctionValueType, FunctionVolatility, MAX_VALUE_TYPE_NODES,
    PureCompileControl, ValueLogicalType,
};
use std::{
    alloc::Layout,
    collections::HashMap,
    sync::{Arc, Mutex},
};

const SOURCE: usize = 4096;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    trace: Vec<u32>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn rejecting(at: usize, cause: CompileControlError) -> Self {
        Self(Mutex::new(State {
            trace: Vec::new(),
            stop: Some((at, cause)),
        }))
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut state = self.0.lock().unwrap();
        let at = state.trace.len();
        if let Some((stop, _)) = state.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        state.trace.push(units);
        match state.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 8,
        max_type_references: 4096,
        max_request_bytes: 4 * 1024 * 1024,
        max_allocation_requests: 8192,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 512 * 1024 * 1024,
    }
}
fn dictionary() -> FunctionValueType {
    FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )
}
fn fixture(phase: AggregatePhase) -> AggregateBinding {
    let field = Arc::new(
        Field::new("original", DataType::Utf8, true)
            .with_metadata(HashMap::from([("unknown".into(), "preserved".into())])),
    );
    AggregateBinding {
        function: BoundFunction::from_exact_signature(
            FunctionId::try_new("test/f").unwrap(),
            FunctionOverloadId::try_new("test/o").unwrap(),
            FunctionKind::Aggregate,
            vec![
                FunctionArgumentType::Value(dictionary()),
                FunctionArgumentType::Lambda {
                    parameter_types: vec![
                        FunctionValueType::new(DataType::Int64, false),
                        FunctionValueType::new(DataType::Struct(vec![field].into()), true),
                    ]
                    .into_boxed_slice(),
                    result_type: FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        false,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap(),
                },
            ]
            .into_boxed_slice(),
            FunctionValueType::new(DataType::Int64, true),
        ),
        phase,
        logical_argument_count: 1,
        intermediate_type: dictionary(),
        state_format: AggregateStateFormatId::try_new("state/v1").unwrap(),
        state_argument_contract: AggregateStateArgumentContract::ValueRootNullabilityIndependent,
    }
}
fn scope<T>(
    control: &Control,
    action: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, BindingCodecError>,
) -> Result<T, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = action(&mut work);
    finish(work, result)
}
fn run(
    source: &AggregateBinding,
    control: &Control,
) -> Result<AggregateBinding, BindingCodecError> {
    scope(control, |work| {
        let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
        preflight_aggregate_binding_copy(source, &mut model, limits(), work)?;
        copy_aggregate_binding_observed(source, work)
    })
}
fn copied_dictionary(original: &FunctionValueType, copied: &FunctionValueType) {
    assert_eq!(copied, original);
    let (DataType::Dictionary(a, b), DataType::Dictionary(c, d)) =
        (&original.data_type, &copied.data_type)
    else {
        panic!("Dictionary")
    };
    assert!(!std::ptr::eq(a.as_ref(), c.as_ref()));
    assert!(!std::ptr::eq(b.as_ref(), d.as_ref()));
}
fn prefixes(source: &AggregateBinding, emit: bool, successful: bool) {
    let operation = |control: &Control| {
        scope(control, |work| {
            if emit {
                copy_aggregate_binding_observed(source, work).map(|_| ())
            } else {
                let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
                preflight_aggregate_binding_copy(source, &mut model, limits(), work)
            }
        })
    };
    let baseline = Control::default();
    assert_eq!(operation(&baseline).is_ok(), successful);
    let trace = baseline.trace();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            assert!(
                matches!(operation(&control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn aggregate_copy_preserves_four_phases_sparse_sequences_and_full_owned_types() {
    let phases = [
        AggregatePhase::Single,
        AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(0),
        },
        AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
        AggregatePhase::Intermediate {
            sequence: AggregateSequenceId::new(0),
        },
        AggregatePhase::Intermediate {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
        AggregatePhase::Final {
            sequence: AggregateSequenceId::new(0),
        },
        AggregatePhase::Final {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
    ];
    for phase in phases {
        for contract in [
            AggregateStateArgumentContract::ExactSignature,
            AggregateStateArgumentContract::ValueRootNullabilityIndependent,
        ] {
            let mut source = fixture(phase);
            source.state_argument_contract = contract;
            let copied = run(&source, &Control::default()).unwrap();
            assert_eq!(copied.phase, phase);
            assert_eq!(copied.logical_argument_count, 1);
            assert_eq!(copied.state_argument_contract, contract);
            assert_eq!(copied.state_format.as_str(), "state/v1");
            assert!(!std::ptr::eq(
                source.state_format.as_str().as_ptr(),
                copied.state_format.as_str().as_ptr()
            ));
            assert!(copied.function.legacy_metadata.is_none());
            assert_eq!(copied.function.kind, FunctionKind::Aggregate);
            assert_eq!(copied.function.function_id.as_str(), "test/f");
            assert_eq!(copied.function.overload.as_str(), "test/o");
            assert_eq!(
                copied.function.result_type,
                FunctionValueType::new(DataType::Int64, true)
            );
            copied_dictionary(&source.intermediate_type, &copied.intermediate_type);
            let (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) = (
                &source.function.argument_types[0],
                &copied.function.argument_types[0],
            ) else {
                panic!("Value")
            };
            copied_dictionary(a, b);
            let (
                FunctionArgumentType::Lambda {
                    parameter_types: a,
                    result_type: r,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: b,
                    result_type: s,
                },
            ) = (
                &source.function.argument_types[1],
                &copied.function.argument_types[1],
            )
            else {
                panic!("Lambda")
            };
            assert_eq!(a, b);
            assert_eq!(r, s);
            assert_eq!(s.logical_type, ValueLogicalType::LargeInt);
            assert!(!std::ptr::eq(a.as_ptr(), b.as_ptr()));
            let (DataType::Struct(a), DataType::Struct(b)) = (&a[1].data_type, &b[1].data_type)
            else {
                panic!("Struct")
            };
            assert!(Arc::ptr_eq(&a[0], &b[0]));
            assert_eq!(b[0].name(), "original");
            assert_eq!(b[0].metadata()["unknown"], "preserved");
        }
    }
}

#[test]
fn aggregate_copy_requests_have_independent_layout_and_reference_oracles() {
    let source = fixture(AggregatePhase::Single);
    for count in [0, u32::MAX] {
        let mut raw = fixture(AggregatePhase::Single);
        raw.logical_argument_count = count;
        assert_eq!(
            run(&raw, &Control::default())
                .unwrap()
                .logical_argument_count,
            count
        );
    }
    let control = Control::default();
    let model = scope(&control, |work| {
        let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
        preflight_aggregate_binding_copy(&source, &mut model, limits(), work)?;
        Ok(model)
    })
    .unwrap();
    // Function/overload/state Boxes, args Vec+Box, Lambda parameters Vec+Box,
    // two independent Dictionaries with two DataType Boxes each; no outer Box.
    let bytes = 20
        + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(2).unwrap().size()
        + 4 * Layout::new::<DataType>().size();
    let retained = 20
        + Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + Layout::array::<FunctionValueType>(2).unwrap().size()
        + 4 * Layout::new::<DataType>().size();
    assert_eq!(model.facts.definition_count, 1);
    assert_eq!(model.facts.type_reference_count, 6);
    assert_eq!(model.facts.allocation_requests_upper_bound, 11);
    assert_eq!(model.facts.request_bytes_upper_bound, bytes);
    assert_eq!(model.retained, retained);
    assert_eq!(
        model.facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + bytes
    );
    assert_eq!(
        model.facts.cumulative_work_upper_bound,
        128 + 32 * 5 + 6 * (16 + 8 * MAX_VALUE_TYPE_NODES) + 4 * bytes + 11
    );
}

#[test]
fn repeated_aggregate_copies_accumulate_all_six_axes_and_keep_numeric_primary() {
    let source = fixture(AggregatePhase::Single);
    let model = scope(&Control::default(), |work| {
        let mut model = MaterializationModel::for_composition(2, 0, SOURCE, SOURCE);
        preflight_aggregate_binding_copy(&source, &mut model, limits(), work)?;
        let once = (model.facts.request_bytes_upper_bound, model.retained);
        preflight_aggregate_binding_copy(&source, &mut model, limits(), work)?;
        assert_eq!(model.facts.type_reference_count, 12);
        assert_eq!(model.facts.allocation_requests_upper_bound, 22);
        assert_eq!(model.facts.request_bytes_upper_bound, 2 * once.0);
        assert_eq!(model.retained, 2 * once.1);
        let first = copy_aggregate_binding_observed(&source, work)?;
        let second = copy_aggregate_binding_observed(&source, work)?;
        copied_dictionary(&first.intermediate_type, &second.intermediate_type);
        assert!(!std::ptr::eq(
            first.state_format.as_str().as_ptr(),
            second.state_format.as_str().as_ptr()
        ));
        Ok(model)
    })
    .unwrap();
    let f = model.facts;
    let tight = BindingProjectionLimits {
        max_definitions: f.definition_count,
        max_type_references: f.type_reference_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_request_bytes: f.request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    };
    let mut model = model;
    model.check(tight).unwrap();
    for axis in 0..6 {
        let mut under = tight;
        match axis {
            0 => under.max_definitions -= 1,
            1 => under.max_type_references -= 1,
            2 => under.max_allocation_requests -= 1,
            3 => under.max_request_bytes -= 1,
            4 => under.max_coexisting_source_and_request_bytes -= 1,
            5 => under.max_work -= 1,
            _ => unreachable!(),
        }
        for late in CAUSES {
            let control = Control::rejecting(1, late);
            let result = scope(&control, |work| {
                preflight_aggregate_binding_copy(&source, &mut model, under, work)
            });
            assert!(matches!(
                result,
                Err(BindingCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), [0]);
        }
    }
}

#[test]
fn aggregate_copy_success_and_ordinary_errors_preserve_every_original_callback_prefix() {
    let source = fixture(AggregatePhase::Single);
    let mut wrong = fixture(AggregatePhase::Single);
    wrong.function.kind = FunctionKind::Window;
    let mut legacy = fixture(AggregatePhase::Single);
    legacy.function.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: FunctionVolatility::Immutable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        semantic_parameters: Box::new([]),
    });
    for emit in [false, true] {
        prefixes(&source, emit, true);
        prefixes(&wrong, emit, false);
        prefixes(&legacy, emit, false);
    }
    assert!(matches!(
        run(&wrong, &Control::default()),
        Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an aggregate signature"
        ))
    ));
    assert!(matches!(
        run(&legacy, &Control::default()),
        Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an exact binding without legacy metadata"
        ))
    ));
}

#[test]
fn wide_aggregate_copy_has_real_prepare_quantum_and_sampled_emit_prefixes() {
    let mut source = fixture(AggregatePhase::Partial {
        sequence: AggregateSequenceId::new(u32::MAX),
    });
    source.function.argument_types = (0..320)
        .map(|i| FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, i % 2 == 1)))
        .collect();
    source.logical_argument_count = 320;
    let prepare = |control: &Control| {
        scope(control, |work| {
            let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
            preflight_aggregate_binding_copy(&source, &mut model, limits(), work)
        })
    };
    let control = Control::default();
    prepare(&control).unwrap();
    let baseline = control.trace();
    assert!(baseline.contains(&256));
    for at in [
        0,
        baseline.iter().position(|n| *n == 256).unwrap(),
        baseline.len() - 1,
    ] {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            assert!(
                matches!(prepare(&control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    let emit = |control: &Control| {
        scope(control, |work| {
            copy_aggregate_binding_observed(&source, work)
        })
    };
    let control = Control::default();
    let output = emit(&control).unwrap();
    assert_eq!(output.function.argument_types.len(), 320);
    assert_eq!(output.logical_argument_count, 320);
    assert_eq!(output.phase, source.phase);
    for (i, ty) in output.function.argument_types.iter().enumerate() {
        assert_eq!(
            *ty,
            FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, i % 2 == 1))
        );
    }
    let baseline = control.trace();
    for at in [0, baseline.len() / 2, baseline.len() - 1] {
        for cause in CAUSES {
            let control = Control::rejecting(at, cause);
            assert!(
                matches!(emit(&control), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    // Actual root/reference refusal precedes a late callback, not a fake scan.
    let mut under = limits();
    under.max_type_references = 1;
    for late in CAUSES {
        let control = Control::rejecting(1, late);
        let result = scope(&control, |work| {
            let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
            preflight_aggregate_binding_copy(&source, &mut model, under, work)
        });
        assert!(matches!(
            result,
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [0]);
    }
}

#[test]
fn staged_aggregate_counts_preserve_state_and_scalar_roots_before_type_walk() {
    let source = fixture(AggregatePhase::Single);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut model = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
    preflight_aggregate_binding_copy_counts(&source, &mut model, limits(), &mut work).unwrap();
    let prefix_bytes = 20
        + 2 * Layout::array::<FunctionArgumentType>(2).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(2).unwrap().size();
    assert_eq!(model.facts.type_reference_count, 6);
    assert_eq!(model.facts.allocation_requests_upper_bound, 7);
    assert_eq!(model.facts.request_bytes_upper_bound, prefix_bytes);
    assert_eq!(model.items, 4);
    preflight_aggregate_binding_copy_types(&source, &mut model, limits(), &mut work).unwrap();
    assert_eq!(model.facts.allocation_requests_upper_bound, 11);
    assert_eq!(
        model.facts.request_bytes_upper_bound,
        prefix_bytes + 4 * Layout::new::<DataType>().size()
    );
    work.finish().unwrap();
    let full = Control::default();
    let mut work = CompileCheckpoints::try_new(&full, CompilePhase::Decode).unwrap();
    let mut original = MaterializationModel::for_composition(1, 0, SOURCE, SOURCE);
    preflight_aggregate_binding_copy(&source, &mut original, limits(), &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(full.trace(), control.trace());
    assert_eq!(
        original.facts.request_bytes_upper_bound,
        model.facts.request_bytes_upper_bound
    );
}
