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

//! Exact temporal slice profiles, original control masks and selected-address proofs.
use super::super::calendar_slice_owner::{effects, operation, prepared_for_test_with_control};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, ConstantValue, EvaluatedArgument, FunctionValueType,
    KernelDiagnostic, ScalarEvaluationInstance,
    kernel_control::{internal, invalid},
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionIntrinsicRowError,
    FunctionNullBehavior, PureCompileControl,
};
use std::sync::Mutex;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after refusal");
        }
        trace.push(n);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: WaitDuration) -> Result<(), KernelFailure> {
        panic!("slice never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop);
        }
        trace.push(n);
        if let Some((stop, cause)) = self.refusal
            && at == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn dt(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn day(s: &str) -> i32 {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap()
        .num_days_from_ce()
        - 719163
}
fn temporal(date: bool, values: Vec<Option<&str>>) -> ArrayRef {
    if date {
        Arc::new(Date32Array::from(
            values.into_iter().map(|v| v.map(day)).collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(TimestampMicrosecondArray::from(
            values.into_iter().map(|v| v.map(dt)).collect::<Vec<_>>(),
        ))
    }
}
fn target(date: bool) -> DataType {
    if date {
        DataType::Date32
    } else {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }
}
fn prepare(
    date: bool,
    boundary: bool,
    nullable: bool,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn crate::PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    let mut types = vec![
        FunctionValueType::new(target(date), nullable),
        FunctionValueType::new(DataType::Int32, nullable),
        FunctionValueType::new(DataType::Utf8, nullable),
    ];
    if boundary {
        types.push(FunctionValueType::new(DataType::Utf8, nullable));
    }
    prepared_for_test_with_control(
        if date { "date_slice" } else { "time_slice" },
        &types,
        DecimalOverflowPolicy::OutputNull,
        control,
    )
}
fn instance(date: bool, boundary: bool, nullable: bool) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepare(date, boundary, nullable, &CompileControl::default()).unwrap(),
    )
    .unwrap()
}
#[test]
fn slice_all_four_profiles_preserve_original_unit_math_nulls_and_empty() {
    for date in [false, true] {
        for boundary in [false, true] {
            for (unit, floor, ceil) in [
                ("year", "2024-01-01 00:00:00", "2025-01-01 00:00:00"),
                ("quarter", "2024-04-01 00:00:00", "2024-07-01 00:00:00"),
                ("month", "2024-05-01 00:00:00", "2024-06-01 00:00:00"),
                ("week", "2024-05-13 00:00:00", "2024-05-20 00:00:00"),
                ("day", "2024-05-17 00:00:00", "2024-05-18 00:00:00"),
                ("hour", "2024-05-17 15:00:00", "2024-05-17 16:00:00"),
                ("minute", "2024-05-17 15:16:00", "2024-05-17 15:17:00"),
                ("second", "2024-05-17 15:16:17", "2024-05-17 15:16:18"),
                (
                    "millisecond",
                    "2024-05-17 15:16:17.123000",
                    "2024-05-17 15:16:17.124000",
                ),
                (
                    "microsecond",
                    "2024-05-17 15:16:17.123456",
                    "2024-05-17 15:16:17.123457",
                ),
            ] {
                if date
                    && matches!(
                        unit,
                        "hour" | "minute" | "second" | "millisecond" | "microsecond"
                    )
                {
                    continue;
                }
                let value = if date {
                    "2024-05-17"
                } else {
                    "2024-05-17 15:16:17.123456"
                };
                let source = temporal(date, vec![Some(value), Some(value), None]);
                let count = Arc::new(Int32Array::from(vec![1; 3])) as ArrayRef;
                let unit = Arc::new(StringArray::from(vec![unit; 3])) as ArrayRef;
                let bounds =
                    Arc::new(StringArray::from(vec!["floor", "CeIl", "floor"])) as ArrayRef;
                let mut args = vec![
                    EvaluatedArgument::Column(&source),
                    EvaluatedArgument::Column(&count),
                    EvaluatedArgument::Column(&unit),
                ];
                if boundary {
                    args.push(EvaluatedArgument::Column(&bounds));
                }
                let result = instance(date, boundary, true)
                    .evaluate(Selection::all(3), &args, &Control::default())
                    .unwrap();
                assert!(result.errors().is_empty());
                let f = if date { &floor[..10] } else { floor };
                let c = if date { &ceil[..10] } else { ceil };
                let expected = temporal(
                    date,
                    vec![Some(f), Some(if boundary { c } else { f }), None],
                );
                assert_eq!(result.values().to_data(), expected.to_data());
                let result = instance(date, boundary, true)
                    .evaluate(
                        Selection::try_sparse(3, &[]).unwrap(),
                        &args,
                        &Control::default(),
                    )
                    .unwrap();
                assert!(result.values().is_empty());
            }
        }
    }
}
#[test]
fn slice_called_on_null_controls_and_exact_declaration_contract_are_preserved() {
    assert_eq!(effects().null_behavior, FunctionNullBehavior::CalledOnNull);
    assert_eq!(effects().own_row_error, FunctionIntrinsicRowError::MayRaise);
    assert!(effects().environment_dependencies.is_empty());
    assert!(operation("date_slice").is_some());
    assert!(operation("time_slice").is_some());
    assert!(operation("date_floor").is_none());
    for date in [false, true] {
        for boundary in [false, true] {
            for nullable in [false, true] {
                let k = prepare(date, boundary, nullable, &CompileControl::default()).unwrap();
                assert_eq!(
                    k.contract().selected().argument_types.len(),
                    if boundary { 4 } else { 3 }
                );
                assert_eq!(k.contract().result_type().data_type, target(date));
            }
        }
    }
}
#[test]
fn slice_original_control_validation_order_before_temporal_null_is_row_attributed() {
    for date in [false, true] {
        let source = temporal(date, vec![None; 6]);
        let count = Arc::new(Int32Array::from(vec![
            None,
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
        ])) as ArrayRef;
        let unit = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("bad"),
            Some("unknown"),
            Some("day"),
            Some("day"),
        ])) as ArrayRef;
        let bounds = Arc::new(StringArray::from(vec![
            None,
            None,
            None,
            None,
            None,
            Some("bad"),
        ])) as ArrayRef;
        let args = [
            EvaluatedArgument::Column(&source),
            EvaluatedArgument::Column(&count),
            EvaluatedArgument::Column(&unit),
            EvaluatedArgument::Column(&bounds),
        ];
        let result = instance(date, true, true)
            .evaluate(Selection::all(6), &args, &Control::default())
            .unwrap();
        assert_eq!(result.errors().len(), 6);
        assert_eq!(result.values().null_count(), 6);
        let name = if date { "date_slice" } else { "time_slice" };
        let expected = [
            format!("{name} requires non-null interval"),
            format!("{name} requires non-null unit"),
            format!("{name} requires second parameter must be greater than 0"),
            "time_slice unsupported unit".into(),
            format!("{name} requires non-null boundary"),
            "time_slice expects boundary floor/ceil".into(),
        ];
        for (ordinal, (error, message)) in result.errors().iter().zip(expected).enumerate() {
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(error.message(), message);
        }
    }
}
#[test]
fn slice_sparse_compact_nonzero_slices_preserve_independent_addresses_and_error_masks() {
    for date in [false, true] {
        let source = temporal(
            date,
            if date {
                vec![
                    Some("1900-01-01"),
                    Some("2024-05-17"),
                    None,
                    Some("2024-05-01"),
                    Some("2000-01-01"),
                ]
            } else {
                vec![
                    Some("1900-01-01 00:00:00"),
                    Some("2024-05-17 12:34:56"),
                    None,
                    Some("2024-05-01 00:00:00"),
                    Some("2000-01-01 00:00:00"),
                ]
            },
        )
        .slice(1, 3);
        let count = Arc::new(Int32Array::from(vec![9, 1, 1, 1, 9]).slice(1, 3)) as ArrayRef;
        let unit =
            Arc::new(StringArray::from(vec!["bad", "month", "month", "month", "bad"]).slice(1, 3))
                as ArrayRef;
        let bounds =
            Arc::new(StringArray::from(vec!["bad", "ceil", "floor", "ceil", "bad"]).slice(1, 3))
                as ArrayRef;
        let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
        let args = [
            EvaluatedArgument::Column(&source),
            EvaluatedArgument::Column(&count),
            EvaluatedArgument::Column(&unit),
            EvaluatedArgument::Column(&bounds),
        ];
        let dense = instance(date, true, true)
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        let compact = temporal(
            date,
            if date {
                vec![Some("2024-05-17"), Some("2024-05-01")]
            } else {
                vec![Some("2024-05-17 12:34:56"), Some("2024-05-01 00:00:00")]
            },
        );
        let selected =
            SelectedValues::try_new(selection, &target(date), compact, Box::default()).unwrap();
        let args = [
            EvaluatedArgument::SelectedColumn(&selected),
            EvaluatedArgument::Column(&count),
            EvaluatedArgument::Column(&unit),
            EvaluatedArgument::Column(&bounds),
        ];
        let sparse = instance(date, true, true)
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(dense.values().to_data(), sparse.values().to_data());
        assert!(sparse.errors().is_empty());
        assert_eq!(
            sparse.values().to_data(),
            temporal(
                date,
                if date {
                    vec![Some("2024-06-01"); 2]
                } else {
                    vec![Some("2024-06-01 00:00:00"); 2]
                }
            )
            .to_data()
        );
    }
}
fn pool(array: ArrayRef, ordinal: u32) -> ConstantValue {
    let policy = ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 8,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1024,
        max_library_validation_work: 4096,
        max_library_validation_bytes: 8192,
    };
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("slice-control").unwrap()),
        ty,
        array.to_data(),
        policy,
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
#[test]
fn slice_scalar_and_nonzero_constant_controls_preserve_exact_floor_ceil() {
    for date in [false, true] {
        let source = temporal(
            date,
            vec![Some(if date {
                "2024-05-01"
            } else {
                "2024-05-01 00:00:00"
            })],
        );
        let count = pool(Arc::new(Int32Array::from(vec![9, 1])), 1);
        let unit = pool(Arc::new(StringArray::from(vec!["bad", "month"])), 1);
        let boundary = pool(Arc::new(StringArray::from(vec!["bad", "ceil"])), 1);
        let args = [
            EvaluatedArgument::Scalar(&source),
            EvaluatedArgument::Constant(&count),
            EvaluatedArgument::Constant(&unit),
            EvaluatedArgument::Constant(&boundary),
        ];
        let result = instance(date, true, true)
            .evaluate(Selection::all(5), &args, &Control::default())
            .unwrap();
        assert!(result.errors().is_empty());
        assert_eq!(
            result.values().to_data(),
            temporal(
                date,
                vec![
                    Some(if date {
                        "2024-06-01"
                    } else {
                        "2024-06-01 00:00:00"
                    });
                    5
                ]
            )
            .to_data()
        );
    }
}
#[test]
fn slice_nonnull_profiles_preserve_invalid_raw_payload_null_and_int32_max_overflow() {
    for date in [false, true] {
        for boundary in [false, true] {
            let source = if date {
                Arc::new(Date32Array::from(vec![
                    i32::MIN,
                    i32::MAX,
                    day("9999-12-31"),
                ])) as ArrayRef
            } else {
                Arc::new(TimestampMicrosecondArray::from(vec![
                    i64::MIN,
                    i64::MAX,
                    dt("9999-12-31 23:59:59"),
                ])) as ArrayRef
            };
            let count = Arc::new(Int32Array::from(vec![1, 1, i32::MAX])) as ArrayRef;
            let unit = Arc::new(StringArray::from(vec!["day"; 3])) as ArrayRef;
            let bounds = Arc::new(StringArray::from(vec!["ceil"; 3])) as ArrayRef;
            let mut args = vec![
                EvaluatedArgument::Column(&source),
                EvaluatedArgument::Column(&count),
                EvaluatedArgument::Column(&unit),
            ];
            if boundary {
                args.push(EvaluatedArgument::Column(&bounds));
            }
            let result = instance(date, boundary, false)
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap();
            assert!(result.errors().is_empty());
            assert!(result.values().is_null(0));
            assert!(result.values().is_null(1));
            if boundary {
                assert!(result.values().is_null(2));
            }
        }
    }
}
#[test]
fn slice_all_seven_runtime_and_three_compile_causes_survive_every_prefix() {
    let source = temporal(false, vec![Some("2024-05-17 15:16:17"), None]);
    let count = Arc::new(Int32Array::from(vec![1; 2])) as ArrayRef;
    let unit = Arc::new(StringArray::from(vec!["month", "day"])) as ArrayRef;
    let boundary = Arc::new(StringArray::from(vec!["floor", "floor"])) as ArrayRef;
    let args = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Column(&count),
        EvaluatedArgument::Column(&unit),
        EvaluatedArgument::Column(&boundary),
    ];
    let kernel = prepare(false, true, true, &CompileControl::default()).unwrap();
    let good = Control::default();
    ScalarEvaluationInstance::instantiate(kernel.clone())
        .unwrap()
        .evaluate(Selection::all(2), &args, &good)
        .unwrap();
    let count = good.trace.lock().unwrap().len();
    for stop in 0..count {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            invalid("original invalid"),
            internal("original internal"),
            KernelFailure::Operational(KernelDiagnostic::new("original operation")),
            KernelFailure::InstanceFailed,
        ] {
            let c = Control {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            };
            let mut instance = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
            assert_eq!(
                instance.evaluate(Selection::all(2), &args, &c).unwrap_err(),
                cause
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
    let good = CompileControl::default();
    prepare(false, true, true, &good).unwrap();
    let count = good.trace.lock().unwrap().len();
    for stop in 0..count {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = CompileControl {
                refusal: Some((stop, cause)),
                ..Default::default()
            };
            let error = prepare(false, true, true, &c).err().unwrap();
            let actual = match error {
                crate::FunctionSpecializationFailure::Binding(
                    crate::FunctionBindingError::Control(c),
                )
                | crate::FunctionSpecializationFailure::Control(c) => Some(c),
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                    Some(CompileControlError::Cancelled)
                }
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                    Some(CompileControlError::DeadlineExceeded)
                }
                crate::FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                    Some(CompileControlError::ResourceExhausted)
                }
                _ => None,
            };
            assert_eq!(actual, Some(cause));
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
