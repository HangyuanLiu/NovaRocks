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
use crate::builtin::window_default::DefaultValueRecipe;
use crate::kernel_control::internal;
use crate::{
    ConstantPolicy, ConstantPool, ConstantValue, EvaluatedArgument, KernelDiagnostic,
    KernelEvaluationControl, RowDataError, SelectedValues, Selection,
};
use arrow_array::{ArrayRef, NullArray, StringArray};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

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
        panic!("default conversion never waits")
    }
}
fn failures() -> [KernelFailure; 7] {
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
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}
fn recipe(source: &DataType, nullable: bool, target: &DataType) -> DefaultValueRecipe {
    DefaultValueRecipe::try_new(
        &ty(source.clone(), nullable),
        &ty(target.clone(), true),
        &CompileControl::default(),
    )
    .unwrap()
}
fn kinds() -> [DataType; 6] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ]
}
fn small_array(kind: &DataType) -> ArrayRef {
    match kind {
        DataType::Int8 => Arc::new(Int8Array::from(vec![Some(-7), Some(0), Some(7), None])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![Some(-7), Some(0), Some(7), None])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![Some(-7), Some(0), Some(7), None])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![Some(-7), Some(0), Some(7), None])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![
            Some(-7.0),
            Some(0.0),
            Some(7.0),
            None,
        ])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![
            Some(-7.0),
            Some(0.0),
            Some(7.0),
            None,
        ])),
        _ => panic!("numeric fixture"),
    }
}
fn ints(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("integer output"),
    }
}
fn f32_bits(array: &ArrayRef) -> Vec<Option<u32>> {
    array
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .iter()
        .map(|v| v.map(f32::to_bits))
        .collect()
}
fn f64_bits(array: &ArrayRef) -> Vec<Option<u64>> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|v| v.map(f64::to_bits))
        .collect()
}
fn run(source: &ArrayRef, target: &DataType) -> ArrayRef {
    recipe(source.data_type(), true, target)
        .evaluate_complete(
            EvaluatedArgument::Column(source),
            source.len(),
            &Control::default(),
        )
        .unwrap()
}
fn constant(array: &ArrayRef, ordinal: u32) -> ConstantValue {
    let value_type = ty(array.data_type().clone(), true);
    let policy = ConstantPolicy {
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
    };
    ConstantPool::try_new(
        Arc::new(value_type.try_to_field("original").unwrap()),
        value_type,
        array.to_data(),
        policy,
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}

#[test]
fn all_thirty_six_numeric_profiles_use_complete_original_safe_cast_with_hand_values() {
    for source in kinds() {
        let array = small_array(&source);
        for target in kinds() {
            let r = recipe(&source, true, &target);
            let out = r
                .evaluate_complete(EvaluatedArgument::Column(&array), 4, &Control::default())
                .unwrap();
            match target {
                DataType::Float32 => assert_eq!(
                    f32_bits(&out),
                    vec![Some(0xc0e00000), Some(0), Some(0x40e00000), None]
                ),
                DataType::Float64 => assert_eq!(
                    f64_bits(&out),
                    vec![
                        Some(0xc01c000000000000),
                        Some(0),
                        Some(0x401c000000000000),
                        None
                    ]
                ),
                _ => assert_eq!(ints(&out), vec![Some(-7), Some(0), Some(7), None]),
            }
            assert!(out.get_array_memory_size() <= r.retained_upper_bound(4).unwrap());
            let nonnull = array.slice(0, 3);
            let nonnull_recipe = recipe(&source, false, &target);
            let nonnull_out = nonnull_recipe
                .evaluate_complete(EvaluatedArgument::Column(&nonnull), 3, &Control::default())
                .unwrap();
            match target {
                DataType::Float32 => assert_eq!(
                    f32_bits(&nonnull_out),
                    vec![Some(0xc0e00000), Some(0), Some(0x40e00000)]
                ),
                DataType::Float64 => assert_eq!(
                    f64_bits(&nonnull_out),
                    vec![Some(0xc01c000000000000), Some(0), Some(0x401c000000000000)]
                ),
                _ => assert_eq!(ints(&nonnull_out), vec![Some(-7), Some(0), Some(7)]),
            }
        }
    }
}

#[test]
fn signed_narrowing_null_and_direct_source_float_rounding_keep_hand_bit_boundaries() {
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(-129),
        Some(-128),
        Some(127),
        Some(128),
        Some(-32769),
        Some(-32768),
        Some(32767),
        Some(32768),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ]));
    assert_eq!(
        ints(&run(&numbers, &DataType::Int8)),
        vec![
            None,
            Some(-128),
            Some(127),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None
        ]
    );
    assert_eq!(
        ints(&run(&numbers, &DataType::Int16)),
        vec![
            Some(-129),
            Some(-128),
            Some(127),
            Some(128),
            None,
            Some(-32768),
            Some(32767),
            None,
            None,
            None,
            None
        ]
    );
    assert_eq!(
        ints(&run(&numbers, &DataType::Int32)),
        vec![
            Some(-129),
            Some(-128),
            Some(127),
            Some(128),
            Some(-32769),
            Some(-32768),
            Some(32767),
            Some(32768),
            None,
            None,
            None
        ]
    );
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![
        16777217,
        9007199254740993,
        18014399583223809,
        i64::MAX,
        i64::MIN,
    ]));
    // Third row distinguishes direct I64->F32 from double rounding through F64.
    assert_eq!(
        f32_bits(&run(&wide, &DataType::Float32)),
        vec![
            Some(0x4b800000),
            Some(0x5a000000),
            Some(0x5a800001),
            Some(0x5f000000),
            Some(0xdf000000)
        ]
    );
    assert_eq!(
        f64_bits(&run(&wide, &DataType::Float64)),
        vec![
            Some(0x4170000010000000),
            Some(0x4340000000000000),
            Some(0x4350000010000000),
            Some(0x43e0000000000000),
            Some(0xc3e0000000000000)
        ]
    );
    for source in [DataType::Int8, DataType::Int16, DataType::Int32] {
        let array = small_array(&source);
        assert_eq!(
            f32_bits(&run(&array, &DataType::Float32)),
            vec![Some(0xc0e00000), Some(0), Some(0x40e00000), None]
        );
        assert_eq!(
            f64_bits(&run(&array, &DataType::Float64)),
            vec![
                Some(0xc01c000000000000),
                Some(0),
                Some(0x401c000000000000),
                None
            ]
        );
    }
}

#[test]
fn eight_float_to_signed_profiles_truncate_and_safe_null_nonfinite_and_outer_boundaries() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let (values, expected): (Vec<Option<f64>>, Vec<Option<i64>>) = match (&source, &target)
            {
                (_, DataType::Int8) => (
                    vec![Some(-129.0), Some(-128.75), Some(127.75), Some(128.0)],
                    vec![None, Some(-128), Some(127), None],
                ),
                (_, DataType::Int16) => (
                    vec![
                        Some(-32769.0),
                        Some(-32768.75),
                        Some(32767.75),
                        Some(32768.0),
                    ],
                    vec![None, Some(-32768), Some(32767), None],
                ),
                (DataType::Float64, DataType::Int32) => (
                    vec![
                        Some(-2147483649.0),
                        Some(-2147483648.75),
                        Some(2147483647.75),
                        Some(2147483648.0),
                    ],
                    vec![None, Some(i32::MIN as i64), Some(i32::MAX as i64), None],
                ),
                (DataType::Float32, DataType::Int32) => (
                    vec![
                        Some(-2147483648.0),
                        Some(2147483520.0),
                        Some(2147483648.0),
                        Some(-2147483904.0),
                    ],
                    vec![Some(i32::MIN as i64), Some(2147483520), None, None],
                ),
                (DataType::Float64, DataType::Int64) => (
                    vec![
                        Some(-9223372036854775808.0),
                        Some(9223372036854774784.0),
                        Some(9223372036854775808.0),
                        Some(-9223372036854777856.0),
                    ],
                    vec![Some(i64::MIN), Some(9223372036854774784), None, None],
                ),
                (DataType::Float32, DataType::Int64) => (
                    vec![
                        Some(-9223372036854775808.0),
                        Some(9223371487098961920.0),
                        Some(9223372036854775808.0),
                        Some(-9223373136366403584.0),
                    ],
                    vec![Some(i64::MIN), Some(9223371487098961920), None, None],
                ),
                _ => unreachable!(),
            };
            let mut values = values;
            values.extend([
                Some(-7.9),
                Some(7.9),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ]);
            let mut expected = expected;
            expected.extend([Some(-7), Some(7), None, None, None, None]);
            let array: ArrayRef = if source == DataType::Float32 {
                Arc::new(Float32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|x| x as f32))
                        .collect::<Vec<_>>(),
                ))
            } else {
                Arc::new(Float64Array::from(values))
            };
            let out = run(&array, &target);
            assert_eq!(ints(&out), expected, "{source:?}->{target:?}");
            // Regression reference to the unchanged library author; hand oracle above is independent.
            assert_eq!(
                ints(&out),
                ints(&arrow_cast::cast(array.as_ref(), &target).unwrap())
            );
        }
    }
}

#[test]
fn four_float_profiles_preserve_identity_bits_and_native_cross_width_facts() {
    let f32_words = [
        0, 0x80000000, 0x7f800000, 0xff800000, 1, 0x007fffff, 0x7f7fffff, 0x7fc12345, 0xff812345,
    ];
    let a: ArrayRef = Arc::new(Float32Array::from(f32_words.map(f32::from_bits).to_vec()));
    assert_eq!(
        f32_bits(&run(&a, &DataType::Float32)),
        f32_words.map(Some).to_vec()
    );
    let widened = run(&a, &DataType::Float64);
    let mut expected = vec![
        Some(0),
        Some(0x8000000000000000),
        Some(0x7ff0000000000000),
        Some(0xfff0000000000000),
        Some(0x36a0000000000000),
        Some(0x380fffffc0000000),
        Some(0x47efffffe0000000),
    ];
    // NaN payload conversion is the native same-platform fact, not a portable bit table.
    expected.extend(
        f32_words[7..]
            .iter()
            .map(|v| Some((f32::from_bits(*v) as f64).to_bits())),
    );
    assert_eq!(f64_bits(&widened), expected);
    let f64_words = [
        0,
        0x8000000000000000,
        0x7ff0000000000000,
        0xfff0000000000000,
        1,
        0x36a0000000000000,
        0x47efffffe0000000,
        0x7fefffffffffffff,
        0x7ff8123456789abc,
        0xfff0123456789abc,
    ];
    let b: ArrayRef = Arc::new(Float64Array::from(f64_words.map(f64::from_bits).to_vec()));
    assert_eq!(
        f64_bits(&run(&b, &DataType::Float64)),
        f64_words.map(Some).to_vec()
    );
    let mut expected = vec![
        Some(0),
        Some(0x80000000),
        Some(0x7f800000),
        Some(0xff800000),
        Some(0),
        Some(1),
        Some(0x7f7fffff),
        Some(0x7f800000),
    ];
    expected.extend(
        f64_words[8..]
            .iter()
            .map(|v| Some((f64::from_bits(*v) as f32).to_bits())),
    );
    assert_eq!(f32_bits(&run(&b, &DataType::Float32)), expected);
}

#[test]
fn numeric_complete_partition_keeps_slice_compact_scalar_and_nonzero_cv_address() {
    let backing: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(-128.75),
        Some(127.75),
        None,
        Some(f64::INFINITY),
    ]));
    let sliced = backing.slice(1, 3);
    let scalar: ArrayRef = Arc::new(Float64Array::from(vec![7.9]));
    let cv = constant(&backing, 2);
    let compact = SelectedValues::try_new(
        Selection::all(3),
        &DataType::Float64,
        sliced.clone(),
        Box::default(),
    )
    .unwrap();
    let r = recipe(&DataType::Float64, true, &DataType::Int8);
    for (input, rows, expected) in [
        (
            EvaluatedArgument::Column(&sliced),
            3,
            vec![Some(-128), Some(127), None],
        ),
        (
            EvaluatedArgument::SelectedColumn(&compact),
            3,
            vec![Some(-128), Some(127), None],
        ),
        (EvaluatedArgument::Scalar(&scalar), 4, vec![Some(7); 4]),
        (EvaluatedArgument::Constant(&cv), 4, vec![Some(127); 4]),
    ] {
        assert_eq!(
            ints(
                &r.evaluate_complete(input, rows, &Control::default())
                    .unwrap()
            ),
            expected
        );
    }
    let empty_source = backing.slice(0, 0);
    let empty = r
        .evaluate_complete(
            EvaluatedArgument::Column(&empty_source),
            0,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(empty.len(), 0);
}

#[test]
fn numeric_nominal_nonnullable_result_and_required_child_refuse_without_type_retagging() {
    let source = ty(DataType::Float64, true);
    assert!(
        DefaultValueRecipe::try_new(
            &source,
            &ty(DataType::Int64, false),
            &CompileControl::default()
        )
        .is_err()
    );
    for logical in [
        ValueLogicalType::Json,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Uuid,
    ] {
        let malformed = FunctionValueType {
            data_type: DataType::Float64,
            nullable: true,
            logical_type: logical,
        };
        assert!(
            DefaultValueRecipe::try_new(
                &malformed,
                &ty(DataType::Int64, true),
                &CompileControl::default()
            )
            .is_err()
        );
    }
    let null: ArrayRef = Arc::new(Float64Array::from(vec![None]));
    let r = recipe(&DataType::Float64, false, &DataType::Int64);
    assert!(
        r.evaluate_complete(EvaluatedArgument::Column(&null), 1, &Control::default())
            .is_err()
    );
    let required = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Float64,
        null,
        vec![RowDataError::new(0, "required child")].into(),
    )
    .unwrap();
    let r = recipe(&DataType::Float64, true, &DataType::Int64);
    assert!(
        r.evaluate_complete(
            EvaluatedArgument::SelectedColumn(&required),
            1,
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn actual_numeric_compile_success_and_ordinary_tails_keep_all_three_original_causes() {
    for target in [ty(DataType::Float32, true), ty(DataType::Float32, false)] {
        let source = ty(DataType::Int64, true);
        let baseline = CompileControl::default();
        let _ = DefaultValueRecipe::try_new(&source, &target, &baseline);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.last().unwrap().1 > 0);
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause)),
                };
                assert_eq!(
                    DefaultValueRecipe::try_new(&source, &target, &c).unwrap_err(),
                    compile_failure(cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn actual_numeric_runtime_success_and_ordinary_tails_keep_all_seven_original_causes() {
    let r = recipe(&DataType::Float64, true, &DataType::Int8);
    let valid: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-128.75),
        Some(f64::NAN),
        None,
    ]));
    let wrong: ArrayRef = Arc::new(StringArray::from(vec![Some("7"), None, Some("8")]));
    for array in [&valid, &wrong] {
        let baseline = Control::default();
        let _ = r.evaluate_complete(EvaluatedArgument::Column(array), 3, &baseline);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(trace.last().copied().unwrap() > 0);
        for stop in 0..trace.len() {
            for cause in failures() {
                let c = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    r.evaluate_complete(EvaluatedArgument::Column(array), 3, &c)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn wide_numeric_complete_rows_have_real_quantum_and_checked_individual_output_bounds() {
    for target in kinds() {
        let source: ArrayRef = Arc::new(Float64Array::from(
            (0..320)
                .map(|i| Some(f64::from(i) - 160.5))
                .collect::<Vec<_>>(),
        ));
        let r = recipe(&DataType::Float64, true, &target);
        let baseline = Control::default();
        let out = r
            .evaluate_complete(EvaluatedArgument::Column(&source), 320, &baseline)
            .unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        assert!(out.get_array_memory_size() <= r.retained_upper_bound(320).unwrap());
        // Sample entry, an actual completed 256-unit batch and final tail.
        for stop in [
            0,
            trace.iter().position(|u| *u == 256).unwrap(),
            trace.len() - 1,
        ] {
            for cause in failures() {
                let c = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    r.evaluate_complete(EvaluatedArgument::Column(&source), 320, &c)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
    let c = CompileControl::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::FunctionSpecialization).unwrap();
    let n = NumericDefaultRecipe::try_new(
        &ty(DataType::Float64, true),
        &ty(DataType::Float32, true),
        &mut work,
    )
    .unwrap()
    .unwrap();
    work.finish().unwrap();
    let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    assert!(n.source_class(a.as_ref()));
    assert!(!n.source_class(&StringArray::from(vec!["1"])));
    assert!(n.preflight_output(0).is_ok());
    assert_eq!(
        n.preflight_output(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        n.retained_upper_bound(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn typed_null_float_defaults_preserve_complete_rows_null_bitmap_and_returned_bound() {
    for rows in [0, 1, 255, 256, 257] {
        let source: ArrayRef = Arc::new(NullArray::new(rows));
        for target in [DataType::Float32, DataType::Float64] {
            let r = recipe(&DataType::Null, true, &target);
            let out = r
                .evaluate_complete(
                    EvaluatedArgument::Column(&source),
                    rows,
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(out.len(), rows);
            assert_eq!(out.null_count(), rows);
            match target {
                DataType::Float32 => assert_eq!(f32_bits(&out), vec![None; rows]),
                DataType::Float64 => assert_eq!(f64_bits(&out), vec![None; rows]),
                _ => unreachable!(),
            }
            let bound = NumericDefaultRecipe::retained_target_upper_bound(&target, rows).unwrap();
            assert!(out.get_array_memory_size() <= bound);
            assert_eq!(r.retained_upper_bound(rows).unwrap(), bound);
            assert!(NumericDefaultRecipe::preflight_target(&target, rows).is_ok());
        }
    }
}
