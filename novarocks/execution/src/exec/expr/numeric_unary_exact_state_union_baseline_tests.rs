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

//! Exact mode counterpart to frozen OriginalNativeV1 source-route before probes.
use novarocks_physical_plan::{AggregatePhase, NodeKind};
use novarocks_sql::compiler::{SqlCompileControl, SqlPhysicalEmissionMode};
fn check(sum: Option<(i64, i64, i64)>, intermediate: bool) {
    let control = SqlCompileControl::unbounded();
    let owner = novarocks_sql::compiler::ordinary_union_source_for_test(
        sum,
        intermediate,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &control,
    )
    .unwrap();
    let mut partial = 0;
    let mut middle = 0;
    let mut final_calls = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind {
                for call in calls {
                    match call.binding.phase {
                        AggregatePhase::Partial { .. } => partial += 1,
                        AggregatePhase::Intermediate { .. } => middle += 1,
                        AggregatePhase::Final { .. } => final_calls += 1,
                        _ => panic!("fixture emitted unexpected phase"),
                    }
                }
            }
        }
    }
    assert_eq!(
        (partial, middle, final_calls),
        (2, usize::from(intermediate), 1)
    );
}
#[test]
fn numeric_unary_exact_state_union_two_count_partials_keep_independent_sources() {
    check(None, false);
}
#[test]
fn numeric_unary_exact_state_union_count_intermediate_before_two_partials_is_valid() {
    check(None, true);
}
#[test]
fn numeric_unary_exact_state_union_sum_distinct_literal_constants_is_valid() {
    check(Some((1, 2, 3)), true);
}
