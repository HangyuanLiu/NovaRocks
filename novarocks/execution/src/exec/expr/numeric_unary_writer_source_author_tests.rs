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

//! Independent original checked Writer source-route before probes (UNRUN).
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
};
use novarocks_functions::EngineFunctionCatalogBuilder;
use novarocks_sql::compiler::{
    SqlCompileControl, SqlPhysicalEmissionMode, writer_state_observe_for_test,
    writer_state_source_for_test,
};
use std::sync::Arc;
fn check(
    counts: &[usize],
    partials: usize,
    none: usize,
    independent: usize,
    mode: SqlPhysicalEmissionMode,
) {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(iceberg_theta_registration().unwrap().definition().clone())
        .unwrap();
    let functions = Arc::new(builder.seal_bound().unwrap());
    let owner = writer_state_source_for_test(
        ICEBERG_THETA_AGGREGATE_NAME,
        counts,
        &vec![false; counts.len()],
        functions,
        mode,
    );
    let observed = writer_state_observe_for_test(&owner, &SqlCompileControl::unbounded()).unwrap();
    eprintln!(
        "actual Writer request checks in {mode:?}: {:?}",
        observed.request_errors
    );
    if mode == SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration {
        assert!(
            observed.request_errors.is_empty(),
            "fresh exact request author must succeed"
        );
    }
    assert_eq!(
        (
            observed.partials,
            observed.no_contributions,
            observed.independent_lineages
        ),
        (partials, none, independent)
    );
}
#[test]
fn numeric_unary_original_writer_source_independent_two_targets_share_channel() {
    check(&[1, 1], 2, 0, 1, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_original_writer_source_positive_sparse_no_contribution() {
    check(&[1, 0], 1, 1, 0, SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn numeric_unary_original_writer_source_repeated_and_sparse_channel_occurrences() {
    check(&[2, 1], 3, 1, 1, SqlPhysicalEmissionMode::OriginalNativeV1);
}

#[test]
fn numeric_unary_exact_writer_source_independent_two_targets_share_channel() {
    check(
        &[1, 1],
        2,
        0,
        1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
#[test]
fn numeric_unary_exact_writer_source_positive_sparse_no_contribution() {
    check(
        &[1, 0],
        1,
        1,
        0,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
#[test]
fn numeric_unary_exact_writer_source_repeated_and_sparse_channel_occurrences() {
    check(
        &[2, 1],
        3,
        1,
        1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
}
