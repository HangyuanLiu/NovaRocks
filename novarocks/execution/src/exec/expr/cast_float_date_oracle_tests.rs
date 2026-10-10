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

//! Permanent full original float-to-DATE per-overload equality, including strict invalid rows.
use super::legacy_float_date_cast_baseline_tests::{actual, cases, days, input};
use arrow::array::{Array, ArrayRef, Date32Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("float DATE does not wait")
    }
}
fn recipe(
    wide: bool,
    nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(
            if wide {
                DataType::Float64
            } else {
                DataType::Float32
            },
            nullable,
        ),
        &FunctionValueType::new(DataType::Date32, result_nullable),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn compare_value(
    r: &PreparedCastRecipe,
    array: &ArrayRef,
    ordinal: usize,
    row: usize,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let old = actual(&array.slice(row, 1), policy, allow);
    let new = r
        .evaluate_row(EvaluatedArgument::Column(array), ordinal, row, &Control)
        .unwrap();
    match old {
        Ok(old) => {
            if old.is_null(0) {
                assert_eq!(new, CastRowResult::Null);
            } else {
                assert_eq!(
                    new,
                    CastRowResult::Signed(i64::from(
                        old.as_any().downcast_ref::<Date32Array>().unwrap().value(0)
                    ))
                );
            }
        }
        Err(message) => match new {
            CastRowResult::RowError(error) => {
                assert_eq!(error.selected_ordinal(), ordinal);
                assert_eq!(error.message(), message);
            }
            other => panic!("original strict error {message:?} was changed to {other:?}"),
        },
    }
}
fn complete(wide: bool) {
    let mut values = vec![
        Some(101.9),
        Some(690101.0),
        Some(700101.0),
        Some(991231.0),
        Some(10000101.0),
        Some(20240229.0),
        Some(20240101.0),
        Some(99991231.0),
        Some(101000000.0),
        Some(690101123456.0),
        Some(700101123456.0),
        Some(20240229123456.0),
        Some(99991231235959.0),
    ];
    values.extend(cases(wide).into_iter().map(|(value, _)| Some(value)));
    // Deliberately include every grammar region and precision boundary; no source shape is excluded.
    for value in [
        1.0,
        100.0,
        101.0,
        1231.0,
        691231.0,
        691232.0,
        700100.0,
        700101.0,
        991231.0,
        991232.0,
        10000100.0,
        10000101.0,
        99991231.0,
        99991232.0,
        101000000.0,
        6901235959.0,
        991231235959.0,
        10000101000000.0,
        99999999999999.0,
        100000000000000.0,
    ] {
        values.extend([
            Some(value),
            Some(value + 0.75),
            Some(value - 0.75),
            Some(-value),
        ]);
    }
    for source_nullable in [false, true] {
        let mut rows = values.clone();
        if source_nullable {
            rows.push(None);
        }
        let array = input(wide, rows);
        for result_nullable in [true, false] {
            if source_nullable && !result_nullable {
                continue;
            }
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = recipe(wide, source_nullable, result_nullable, policy, allow);
                    for row in 0..array.len() {
                        compare_value(&r, &array, row, row, policy, allow);
                    }
                    let slice = array.slice(1, array.len() - 1);
                    for row in 0..slice.len() {
                        compare_value(&r, &slice, row, row, policy, allow);
                    }
                    assert_eq!(actual(&array.slice(0, 0), policy, allow).unwrap().len(), 0);
                    let indices = [0, 2, array.len() - 1];
                    let selection = Selection::try_sparse(array.len(), &indices).unwrap();
                    for (ordinal, row) in selection.iter().enumerate() {
                        compare_value(&r, &array, ordinal, row, policy, allow);
                    }
                    // Nonzero Scalar source addresses are not logical-row lookups.
                    let scalar = array.slice(2, 1);
                    let old = actual(&scalar, policy, allow).unwrap();
                    assert_eq!(
                        old.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
                        days(1970, 1, 1)
                    );
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Scalar(&scalar), 33, 999, &Control)
                            .unwrap(),
                        CastRowResult::Signed(i64::from(days(1970, 1, 1)))
                    );
                }
            }
        }
    }
    let hidden: ArrayRef = if wide {
        Arc::new(arrow::array::Float64Array::new(
            vec![f64::NAN, 20240228.0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        ))
    } else {
        Arc::new(arrow::array::Float32Array::new(
            vec![f32::NAN, 20240228.0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        ))
    };
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let r = recipe(wide, true, true, policy, allow);
            compare_value(&r, &hidden, 0, 0, policy, allow);
            compare_value(&r, &hidden, 1, 1, policy, allow);
        }
    }
}
#[test]
fn float_date_oracle_complete_float32_date32_profile() {
    complete(false);
}
#[test]
fn float_date_oracle_complete_float64_date32_profile() {
    complete(true);
}
