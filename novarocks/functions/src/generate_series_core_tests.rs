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
        let out = result_column(expansion.values.clone(), std::slice::from_ref(&ty)).unwrap();
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
fn generate_series_whole_call_causes_distinguish_data_contract_and_output_extent() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    let a = values(&[Some(1)]);
    let b = values(&[Some(2)]);
    let zero = values(&[Some(0)]);
    let cause = match expand_observed(&a, &b, Some(&zero), 1, false, &mut |_| {
        Ok::<(), KernelFailure>(())
    }) {
        Err(SeriesFailure::Cause(cause @ SeriesError::ZeroStep)) => cause,
        other => panic!("expected zero-step cause, got {other:?}"),
    };
    assert_eq!(cause.failure_fact().class(), FailureClass::Data);
    let narrow = result_column(vec![Some(128)], &[DataType::Int8]).unwrap_err();
    assert!(matches!(
        narrow,
        SeriesError::ValueOutOfRange {
            value: 128,
            data_type: DataType::Int8
        }
    ));
    assert_eq!(narrow.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        narrow.failure_fact(),
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_ne!(
        narrow.failure_fact().reason(),
        cause.failure_fact().reason()
    );
    let missing = result_column(vec![Some(1)], &[]).unwrap_err();
    assert!(matches!(missing, SeriesError::ReturnCount { actual: 0 }));
    assert_eq!(missing.failure_fact().class(), FailureClass::Invariant);
    let mut total = MAX_TABLE_FUNCTION_OUTPUT_ROWS;
    let extent = checked_add_output_rows(&mut total, 1).unwrap_err();
    assert_eq!(total, MAX_TABLE_FUNCTION_OUTPUT_ROWS);
    assert_eq!(extent.failure_fact().class(), FailureClass::Representation);
    let representation = extent.failure_fact().representation().unwrap();
    assert_eq!(
        representation.boundary,
        RepresentationBoundary::SingleOutput
    );
    assert_eq!(representation.required, Some(u64::from(u32::MAX) + 1));
    assert_eq!(representation.extent_limit, Some(u64::from(u32::MAX)));
    let extreme = count(i128::MIN, i128::MAX, 1).unwrap_err();
    assert!(matches!(
        extreme,
        SeriesError::OutputRows { required: None }
    ));
    assert_eq!(extreme.failure_fact().class(), FailureClass::Representation);
    assert_eq!(
        extreme.failure_fact().representation().unwrap().required,
        None
    );
    assert!(matches!(count(1, 2, 0), Err(SeriesError::ZeroStep)));
}

#[test]
fn generate_series_signed_extremes_do_not_require_an_unused_successor() {
    for (start, stop, step, expected) in [
        (i128::MAX, i128::MAX, 1, vec![Some(i128::MAX)]),
        (i128::MIN, i128::MIN, -1, vec![Some(i128::MIN)]),
        (0, i128::MIN, i128::MIN, vec![Some(0), Some(i128::MIN)]),
        (
            i128::MIN,
            i128::MAX,
            i128::MAX,
            vec![Some(i128::MIN), Some(-1), Some(i128::MAX - 1)],
        ),
        (
            i128::MAX,
            i128::MIN,
            i128::MIN,
            vec![Some(i128::MAX), Some(-1)],
        ),
    ] {
        let a = largeint::array_from_i128(&[Some(start)]).unwrap();
        let b = largeint::array_from_i128(&[Some(stop)]).unwrap();
        let c = largeint::array_from_i128(&[Some(step)]).unwrap();
        let expanded = expand(&a, &b, Some(&c), 1, false).unwrap();
        assert_eq!(expanded.values, expected);
        assert_eq!(expanded.total_rows, expected.len());
        assert_eq!(expanded.row_counts, vec![expected.len()]);
    }
    let argument = values(&[Some(1)]);
    assert!(matches!(
        integer_argument(&argument, 1, 2, IntegerDiagnosticContext::GenerateSeries),
        Err(SeriesError::ArgumentBounds {
            argument: 2,
            row: 1,
            rows: 1
        })
    ));
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
        result_column_observed(values.clone(), std::slice::from_ref(&ty), &mut |event| {
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
                let result = result_column_observed(
                    values.clone(),
                    std::slice::from_ref(&ty),
                    &mut |event| {
                        seen.push(event);
                        if seen.len() == at + 1 {
                            Err(cause.clone())
                        } else {
                            Ok(())
                        }
                    },
                );
                match result {
                    Err(SeriesFailure::Control(actual)) => assert_eq!(actual, cause),
                    other => panic!("expected exact conversion control, got {other:?}"),
                }
                assert_eq!(seen, trace[..=at]);
            }
        }
    }
}
