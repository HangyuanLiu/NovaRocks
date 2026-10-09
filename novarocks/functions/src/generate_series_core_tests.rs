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
//! Sole-core data and actual work-event tests. Processor latching is a host obligation.
use super::*;
use crate::{KernelDiagnostic, KernelFailure};
fn values(a: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(a.to_vec()))
}
#[test]
fn generate_series_shared_core_keeps_raw_values_counts_null_and_return_conversions() {
    let a = values(&[Some(1), Some(4), None, Some(9)]);
    let b = values(&[Some(4), Some(1), Some(6), Some(8)]);
    let c = values(&[Some(2), Some(-2), Some(0), Some(1)]);
    let expansion = expand(&a, &b, Some(&c), 4, true).unwrap();
    assert_eq!(expansion.total_rows, 6);
    assert_eq!(expansion.row_counts, vec![2, 2, 1, 1]);
    assert_eq!(
        expansion.values,
        vec![Some(1), Some(3), Some(4), Some(2), None, None]
    );
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::FixedSizeBinary(16),
    ] {
        let out = result_column(expansion.values.clone(), &[ty.clone()]).unwrap();
        assert_eq!(out.data_type(), &ty);
        assert_eq!(
            (0..out.len())
                .map(
                    |row| integer_argument(&out, row, 0, IntegerDiagnosticContext::GenerateSeries)
                        .unwrap()
                )
                .collect::<Vec<_>>(),
            expansion.values
        );
    }
}
#[test]
fn generate_series_shared_core_keeps_whole_call_data_and_original_math_edges() {
    let a = values(&[Some(1)]);
    let b = values(&[Some(2)]);
    let zero = values(&[Some(0)]);
    match expand_observed(&a, &b, Some(&zero), 1, false, &mut |_| {
        Ok::<(), KernelFailure>(())
    }) {
        Err(SeriesFailure::Data(message)) => assert_eq!(
            message,
            "table function generate_series step size cannot equal zero"
        ),
        other => panic!("expected original whole-call Data, got {other:?}"),
    }
    assert_eq!(
        result_column(vec![Some(128)], &[DataType::Int8]).unwrap_err(),
        "table function generate_series value out of TINYINT range: 128"
    );
    assert_eq!(
        result_column(vec![Some(1)], &[]).unwrap_err(),
        "table function generate_series expects 1 return type, got 0"
    );
    let ty = DataType::FixedSizeBinary(16);
    let max = largeint::array_from_i128(&[Some(i128::MAX)]).unwrap();
    let step = largeint::array_from_i128(&[Some(1)]).unwrap();
    assert_eq!(
        expand(&max, &max, Some(&step), 1, false).unwrap_err(),
        format!(
            "table function generate_series value overflow: current={} step=1",
            i128::MAX
        )
    );
    assert_eq!(ty, DataType::FixedSizeBinary(16));
    let mut total = MAX_TABLE_FUNCTION_OUTPUT_ROWS;
    assert_eq!(
        checked_add_output_rows(&mut total, 1).unwrap_err(),
        "table function output too large"
    );
    assert_eq!(total, MAX_TABLE_FUNCTION_OUTPUT_ROWS + 1);
    let before = std::panic::catch_unwind(|| count(i128::MIN, i128::MAX, 1));
    if cfg!(debug_assertions) {
        assert!(before.is_err());
    } else {
        assert_eq!(before.unwrap().unwrap(), 0);
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("source invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("source internal")),
        KernelFailure::Operational(KernelDiagnostic::new("source operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn generate_series_shared_core_all_actual_expansion_callbacks_keep_seven_causes_and_no_tail() {
    for count in [4, 320] {
        let a = values(&[Some(1)]);
        let b = values(&[Some(count)]);
        let mut trace = vec![];
        expand_observed(&a, &b, None, 1, false, &mut |event| {
            trace.push(event);
            Ok::<(), KernelFailure>(())
        })
        .unwrap();
        assert_eq!(
            trace
                .iter()
                .filter(|&&e| e == SeriesObservation::Step)
                .count(),
            count as usize + 1
        );
        for at in 0..trace.len() {
            for cause in causes() {
                let mut seen = vec![];
                let result = expand_observed(&a, &b, None, 1, false, &mut |event| {
                    seen.push(event);
                    if seen.len() == at + 1 {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                match result {
                    Err(SeriesFailure::Control(actual)) => assert_eq!(actual, cause),
                    other => panic!("expected exact source control, got {other:?}"),
                }
                assert_eq!(seen, trace[..=at]);
            }
        }
    }
}
#[test]
fn generate_series_shared_core_all_result_conversion_callbacks_keep_seven_causes_and_no_tail() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::FixedSizeBinary(16),
    ] {
        let values = vec![Some(1), None, Some(2), Some(3)];
        let mut trace = vec![];
        result_column_observed(values.clone(), &[ty.clone()], &mut |event| {
            trace.push(event);
            Ok::<(), KernelFailure>(())
        })
        .unwrap();
        assert_eq!(
            trace
                .iter()
                .filter(|&&e| e == SeriesObservation::Step)
                .count(),
            values.len()
        );
        for at in 0..trace.len() {
            for cause in causes() {
                let mut seen = vec![];
                let result = result_column_observed(values.clone(), &[ty.clone()], &mut |event| {
                    seen.push(event);
                    if seen.len() == at + 1 {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                match result {
                    Err(SeriesFailure::Control(actual)) => assert_eq!(actual, cause),
                    other => panic!("expected exact conversion control, got {other:?}"),
                }
                assert_eq!(seen, trace[..=at]);
            }
        }
    }
}
