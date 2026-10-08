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
//! Every actual admitted numeric shape of the complete dynamic-v1 overload.
use super::*;
use crate::exec::expr::legacy_array_difference_baseline_tests::list;
use arrow::array::*;
#[test]
fn pure_differential_array_difference_actual_numeric_result_shapes_null_nan_slices() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(i8::MAX),
            Some(0),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(i16::MAX),
            Some(0),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
            Some(0),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(0),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(f32::INFINITY),
            Some(f32::INFINITY),
            None,
            Some(-0.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            None,
            Some(-0.0),
        ])),
    ];
    for values in arrays {
        let a = list(
            values,
            vec![0, 3, 3, 4, 4],
            Some(vec![true, true, true, false]),
        );
        for a in [a.clone(), a.slice(1, 3), a.slice(0, 0)] {
            assert_scalar_matches_v1(ScalarDiffSpec::new("array_difference").column(a));
        }
    }
}
#[test]
fn pure_differential_array_difference_all_legal_decimal_precisions_signed_scales_and_extremes() {
    for precision in 1..=38 {
        for scale in i8::MIN..=precision as i8 {
            let values: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(1), None, Some(-2), Some(3)])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            assert_scalar_matches_v1(ScalarDiffSpec::new("array_difference").column(list(
                values,
                vec![0, 3, 3, 4],
                None,
            )));
        }
    }
    for scale in [i8::MIN, -1, 0, 38] {
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(i128::MIN), None, Some(i128::MAX)])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        assert_scalar_matches_v1(ScalarDiffSpec::new("array_difference").column(list(
            values,
            vec![0, 3],
            None,
        )));
    }
}
