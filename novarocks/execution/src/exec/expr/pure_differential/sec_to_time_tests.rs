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
//! Saturated TIME formatting over every actual registered profile and value domain.
use super::{ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{ArrayRef, Int64Array};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
#[test]
fn pure_differential_sec_to_time_actual_int64_profile_extremes_nulls_sparse_constants() {
    for nullable in [false, true] {
        let mut raw = vec![
            Some(i64::MIN),
            Some(-3024000),
            Some(-3023999),
            Some(-3600),
            Some(-61),
            Some(-1),
            Some(0),
            Some(1),
            Some(59),
            Some(60),
            Some(61),
            Some(3599),
            Some(3600),
            Some(3023999),
            Some(3024000),
            Some(i64::MAX),
        ];
        if nullable {
            raw.push(None);
        } else {
            raw.push(Some(0));
        }
        let values = Arc::new(Int64Array::from(raw)) as ArrayRef;
        let ty = FunctionValueType::new(values.data_type().clone(), nullable);
        let summary = assert_scalar_matches_v1(
            ScalarDiffSpec::new("sec_to_time")
                .typed_column(ty, values.clone())
                .sparse_selections(17, 2129),
        );
        assert_eq!(summary.legacy_batch_errors, 0);
        assert_eq!(summary.attributed_row_errors, 0);
        for index in 0..values.len() {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("sec_to_time")
                    .constant_rows(9)
                    .constant_array(values.slice(index, 1))
                    .sparse_selections(9, 2131),
            );
        }
    }
}
#[test]
fn pure_differential_sec_to_time_random_full_i64_domain() {
    let mut state = 2137u64;
    let mut values = Vec::new();
    for i in 0..513 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        values.push(if i % 17 == 0 {
            None
        } else {
            Some(state as i64)
        });
    }
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("sec_to_time")
            .column(Arc::new(Int64Array::from(values)))
            .sparse_selections(11, 2139),
    );
}
