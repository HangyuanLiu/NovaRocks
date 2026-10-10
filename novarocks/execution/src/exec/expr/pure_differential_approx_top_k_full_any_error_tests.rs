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
//! Preserve data failures of the original ANY profile through the real aggregate harness.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::{
    ArrayRef, Date64Array, DurationMicrosecondArray, Int64Array, LargeStringArray, UInt32Array,
    new_empty_array, new_null_array,
};
use std::sync::Arc;

#[test]
fn pure_differential_approx_top_k_any_profile_original_data_failures_are_not_admission_refusals() {
    let nonnull: [ArrayRef; 4] = [
        Arc::new(UInt32Array::from(vec![
            Some(17),
            None,
            Some(u32::MAX),
            Some(2),
        ])),
        Arc::new(LargeStringArray::from(vec![
            Some("hidden"),
            None,
            Some("é"),
            Some(""),
        ])),
        Arc::new(Date64Array::from(vec![
            Some(0),
            None,
            Some(-86_400_000),
            Some(1),
        ])),
        Arc::new(DurationMicrosecondArray::from(vec![
            Some(0),
            None,
            Some(i64::MIN),
            Some(1),
        ])),
    ];
    for input in nonnull {
        let data_type = input.data_type();
        for value in [
            Arc::clone(&input),
            input.slice(1, 2),
            new_null_array(data_type, 3),
            new_empty_array(data_type),
        ] {
            for arity in 1..=3 {
                let rows = value.len();
                let mut spec = AggregateDiffSpec::new("approx_top_k").column(Arc::clone(&value));
                for _ in 1..arity {
                    spec = spec.column(Arc::new(Int64Array::from(vec![3; rows])));
                }
                // The old binder accepts these ANY types. Update or final output
                // fails with original data text; an early Kernel refusal differs.
                assert_aggregate_matches_v1(
                    spec.grouped((0..rows).map(|row| row % 2).collect(), 3)
                        .partitions(2, 9303),
                );
            }
        }
    }
}
