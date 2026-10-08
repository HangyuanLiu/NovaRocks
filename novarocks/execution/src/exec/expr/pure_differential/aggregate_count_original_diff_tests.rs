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
//! Every actual COUNT logical signature, plus separately named original physical NULL counterexamples.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::{ArrayRef, DictionaryArray, Int8Array, Int64Array, NullArray, StringArray};
use arrow::datatypes::Int8Type;
use std::sync::Arc;
#[test]
fn pure_differential_count_original_star_signature_all_phases_grouped_empty() {
    for n in [0, 1, 5, 319] {
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new("count")
                .constant_rows(n)
                .grouped((0..n).map(|i| i % 3).collect(), 5)
                .partitions(4, 73),
        );
    }
}
#[test]
fn pure_differential_count_original_expr_signature_int_utf8_null_empty_and_slice() {
    for source in [
        Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(-5),
        ])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            Some("é\0"),
            None,
            Some(""),
            None,
            Some("tail"),
        ])) as ArrayRef,
    ] {
        for input in [source.clone(), source.slice(1, 3), source.slice(2, 0)] {
            let n = input.len();
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new("count")
                    .column(input)
                    .grouped((0..n).map(|i| i % 2).collect(), 4)
                    .partitions(3, 29),
            );
        }
    }
}
#[test]
fn pure_differential_count_original_bare_null_counterexample() {
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("count").column(Arc::new(NullArray::new(5))),
    );
}
#[test]
fn pure_differential_count_original_dictionary_value_null_counterexample() {
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        )
        .unwrap(),
    );
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("count")
            .column(input)
            .partitions(3, 29),
    );
}
