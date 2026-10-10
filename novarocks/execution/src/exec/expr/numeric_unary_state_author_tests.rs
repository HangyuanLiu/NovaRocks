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

//! Actual original request-author probes; new expectations are initially UNRUN.
use novarocks_sql::compiler::{
    SqlCompileControl, SqlPhysicalEmissionMode, ordinary_state_observe_for_test,
    ordinary_union_source_for_test,
};
fn check(sum: Option<(i64, i64, i64)>, intermediate: bool, mode: SqlPhysicalEmissionMode) {
    let control = SqlCompileControl::unbounded();
    let owner = ordinary_union_source_for_test(sum, intermediate, mode, &control).unwrap();
    let actual = ordinary_state_observe_for_test(&owner, &control).unwrap();
    assert_eq!(
        (actual.partials, actual.intermediates, actual.finals),
        (2, usize::from(intermediate), 1)
    );
    assert_eq!(actual.final_consumer_constant, sum.map(|(_, _, c)| c));
    let values = match (sum, intermediate) {
        (Some((a, b, c)), true) => vec![Some(c), Some(a), Some(b)],
        (Some((a, b, _)), false) => vec![Some(a), Some(b)],
        (None, true) => vec![None, None, None],
        (None, false) => vec![None, None],
    };
    assert_eq!(actual.final_producer_constants, values);
    assert_eq!(
        actual.final_independent_lineages,
        2 + usize::from(intermediate)
    );
}
#[test]
fn numeric_unary_original_state_author_count_union() {
    check(None, false, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_original_state_author_count_intermediate() {
    check(None, true, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_original_state_author_sum_distinct_cv() {
    check(
        Some((1, 2, 3)),
        true,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
}
#[test]
fn numeric_unary_exact_state_author_count_union() {
    check(
        None,
        false,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
#[test]
fn numeric_unary_exact_state_author_count_intermediate() {
    check(
        None,
        true,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
#[test]
fn numeric_unary_exact_state_author_sum_distinct_cv() {
    check(
        Some((1, 2, 3)),
        true,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
