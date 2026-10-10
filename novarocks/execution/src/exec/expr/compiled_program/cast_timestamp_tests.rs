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
use arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;
use novarocks_functions::SelectedValues;

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn timestamp_type(unit: TimeUnit, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Timestamp(unit, None), nullable)
}
pub(super) fn timestamp_array(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
    }
}
fn timestamp_values(output: &SelectedValues<'_>) -> Vec<Option<i64>> {
    macro_rules! values {
        ($array:ty) => {
            output
                .values()
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .iter()
                .collect()
        };
    }
    match output.values().data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => values!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, None) => values!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, None) => values!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => values!(TimestampNanosecondArray),
        other => panic!("timestamp result lost its exact carrier: {other:?}"),
    }
}
fn timestamp_batch(
    program: &LocalProgram,
    values: &[Option<i64>],
    flags: &[Option<bool>],
) -> RecordBatch {
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let DataType::Timestamp(unit, None) = schema.field(0).data_type() else {
        panic!("actual frozen timestamp source")
    };
    let mut padded = vec![Some(99)];
    padded.extend_from_slice(values);
    padded.push(Some(-99));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            timestamp_array(*unit, padded).slice(1, values.len()),
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(flags.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn timestamp_cast_compiler_controller_all_sixteen_pairs_keep_exact_units_and_legacy_row_oracles() {
    let values = [
        Some(8),
        Some(-1001),
        Some(-1),
        Some(0),
        Some(1),
        Some(1001),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
        Some(9),
    ];
    let rows = [1, 2, 3, 4, 5, 6, 7, 8];
    let selection = Selection::try_sparse(values.len(), &rows).unwrap();
    for source in UNITS {
        for target in UNITS {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        timestamp_type(source, true),
                        timestamp_type(target, true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let batch = timestamp_batch(&program, &values, &[Some(true); 10]);
                    let mut evaluator = instance(&program);
                    let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
                    assert_eq!(output.selection(), selection);
                    assert_eq!(
                        output.values().data_type(),
                        &DataType::Timestamp(target, None)
                    );
                    let actual = timestamp_values(&output);
                    let mut expected_errors = Vec::new();
                    for (ordinal, &row) in rows.iter().enumerate() {
                        // The pre-existing full-carrier author is independent of
                        // this recipe/controller and its selected row mapping.
                        let array = timestamp_array(source, vec![values[row]]);
                        match crate::exec::expr::cast::cast_with_special_rules(
                            &array,
                            &DataType::Timestamp(target, None),
                        ) {
                            Ok(legacy) => {
                                let expected = SelectedValues::try_new(
                                    Selection::all(1),
                                    &DataType::Timestamp(target, None),
                                    legacy,
                                    Box::default(),
                                )
                                .unwrap();
                                assert_eq!(
                                    actual[ordinal],
                                    timestamp_values(&expected)[0],
                                    "{source:?}->{target:?}"
                                );
                            }
                            Err(error) => {
                                assert_eq!(
                                    (source, target),
                                    (TimeUnit::Microsecond, TimeUnit::Nanosecond)
                                );
                                assert!(error.contains("microsecond->nanosecond overflow"));
                                assert_eq!(actual[ordinal], None);
                                expected_errors.push(ordinal);
                            }
                        }
                    }
                    assert_eq!(errors(&output), expected_errors);
                    let empty = evaluator
                        .evaluate(
                            &batch,
                            Selection::try_sparse(values.len(), &[]).unwrap(),
                            &Control,
                        )
                        .unwrap();
                    assert!(empty.values().is_empty());
                    assert!(empty.errors().is_empty());
                    assert!(evaluator.instances.is_empty());
                }
            }
        }
    }
}

#[test]
fn timestamp_cast_nonnullable_contract_requires_only_five_successful_null_promises() {
    for source in UNITS {
        for target in UNITS {
            let safe_widen = matches!(
                (source, target),
                (
                    TimeUnit::Second,
                    TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
                ) | (
                    TimeUnit::Millisecond,
                    TimeUnit::Microsecond | TimeUnit::Nanosecond
                )
            );
            let (functions, package) = fixture(
                timestamp_type(source, false),
                timestamp_type(target, false),
                Source::Column,
                Wrap::Bare,
                DecimalOverflowPolicy::ReportError,
                false,
            );
            let result = compile(&functions, package, &Control);
            if safe_widen {
                assert!(result.unwrap_err().to_string().contains("successful-NULL"));
            } else {
                let program = result.unwrap();
                let batch =
                    timestamp_batch(&program, &[Some(i64::MAX), Some(-1)], &[Some(true); 2]);
                let output = instance(&program)
                    .evaluate(&batch, Selection::all(2), &Control)
                    .unwrap();
                assert_eq!(
                    errors(&output),
                    if (source, target) == (TimeUnit::Microsecond, TimeUnit::Nanosecond) {
                        vec![0]
                    } else {
                        vec![]
                    }
                );
            }
        }
    }
}

#[test]
fn timestamp_cast_literal_broadcast_and_batch_splits_keep_original_coefficients() {
    for source in UNITS {
        for target in UNITS {
            let program = compiled(
                timestamp_type(source, false),
                timestamp_type(target, true),
                Source::Constant,
                Wrap::Bare,
                DecimalOverflowPolicy::OutputNull,
                false,
            );
            let batch = timestamp_batch(&program, &[Some(i64::MAX); 5], &[Some(true); 5]);
            let array = timestamp_array(source, vec![Some(71)]);
            let legacy = crate::exec::expr::cast::cast_with_special_rules(
                &array,
                &DataType::Timestamp(target, None),
            )
            .unwrap();
            let expected = SelectedValues::try_new(
                Selection::all(1),
                &DataType::Timestamp(target, None),
                legacy,
                Box::default(),
            )
            .unwrap();
            let coefficient = timestamp_values(&expected)[0];
            let rows = [0, 2, 4];
            let mut evaluator = instance(&program);
            for _ in 0..2 {
                let output = evaluator
                    .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
                    .unwrap();
                assert_eq!(timestamp_values(&output), vec![coefficient; 3]);
                assert!(output.errors().is_empty());
            }
        }
    }
}

#[test]
fn timestamp_cast_guards_distinguish_successful_nulls_from_required_overflow_errors() {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for wrap in [Wrap::Bare, Wrap::IsNull, Wrap::If, Wrap::Coalesce] {
                let program = compiled(
                    timestamp_type(TimeUnit::Microsecond, true),
                    timestamp_type(TimeUnit::Nanosecond, true),
                    Source::Column,
                    wrap,
                    policy,
                    allow,
                );
                let batch = timestamp_batch(
                    &program,
                    &[Some(i64::MAX), None, Some(1), Some(i64::MAX)],
                    &[Some(true), Some(true), Some(true), Some(false)],
                );
                let output = instance(&program)
                    .evaluate(&batch, Selection::all(4), &Control)
                    .unwrap();
                assert_eq!(
                    errors(&output),
                    if matches!(wrap, Wrap::If) {
                        vec![0]
                    } else {
                        vec![0, 3]
                    }
                );
                if matches!(wrap, Wrap::IsNull) {
                    let values = output
                        .values()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    assert_eq!(
                        values.iter().collect::<Vec<_>>(),
                        vec![None, Some(true), Some(false), None]
                    );
                } else {
                    assert_eq!(
                        timestamp_values(&output),
                        match wrap {
                            Wrap::If => vec![None, None, Some(1000), Some(71)],
                            Wrap::Coalesce => vec![None, Some(71), Some(1000), None],
                            _ => vec![None, None, Some(1000), None],
                        }
                    );
                }
                for error in output.errors() {
                    assert!(error.message().contains("microsecond->nanosecond overflow"));
                }
            }
            let program = compiled(
                timestamp_type(TimeUnit::Second, true),
                timestamp_type(TimeUnit::Nanosecond, true),
                Source::Column,
                Wrap::Coalesce,
                policy,
                allow,
            );
            let batch =
                timestamp_batch(&program, &[Some(i64::MAX), None, Some(1)], &[Some(true); 3]);
            let output = instance(&program)
                .evaluate(&batch, Selection::all(3), &Control)
                .unwrap();
            assert_eq!(
                timestamp_values(&output),
                vec![Some(71), Some(71), Some(1000000000)]
            );
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn timestamp_cast_actual_runtime_callbacks_preserve_all_seven_causes_and_failed_latch() {
    let program = compiled(
        timestamp_type(TimeUnit::Microsecond, true),
        timestamp_type(TimeUnit::Nanosecond, true),
        Source::Column,
        Wrap::Coalesce,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let batch = timestamp_batch(
        &program,
        &[Some(i64::MAX), None, Some(-1)],
        &[Some(true); 3],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(3), &recorder)
        .unwrap();
    assert_eq!(errors(&output), vec![0]);
    let trace = recorder.trace.lock().unwrap().clone();
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            let mut evaluator = instance(&program);
            assert!(
                matches!(evaluator.evaluate(&batch, Selection::all(3), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(3), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
    let values = vec![Some(1); 320];
    let batch = timestamp_batch(&program, &values, &[Some(true); 320]);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(timestamp_values(&output), vec![Some(1000); 320]);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in (1..=trace.len()).filter(|&n| n == 1 || n == trace.len() || trace[n - 1] == 256)
    {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(instance(&program).evaluate(&batch, Selection::all(320), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
        }
    }
}
