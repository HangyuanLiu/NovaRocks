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
//! Genuine original SQL binding author. Exact candidate presence is absent before owner install.
use super::approx_percentile_actual_sql_source_tests::approx_sql_source;
use novarocks_physical_plan::{ExprKind, PhysicalCallDefinition};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
const CORPUS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/aggregate/sql/agg_percentile_semantics.sql"
));
fn source(sql: &str, expected_arity: usize) {
    let owner = approx_sql_source(sql, SqlPhysicalEmissionMode::OriginalNativeV1);
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for (id, expr) in fragment.expressions().iter() {
            let ExprKind::FunctionCall { function, args } = &expr.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/percentile_hash/v1" {
                continue;
            }
            let request = fragment
                .call_requests()
                .get(PhysicalCallDefinition::Expression(*id))
                .unwrap();
            assert_eq!(request.logical_argument_count, expected_arity);
            assert_eq!(request.arguments.len(), expected_arity);
            assert_eq!(args.len(), expected_arity);
            assert_eq!(function.argument_types.len(), expected_arity);
            eprintln!(
                "percentile_hash actual ORIGINAL source definition={id:?} signature={function:?} request={request:?} ordered_children={args:?}"
            );
            count += 1;
        }
    }
    assert_eq!(count, 1);
}
#[test]
fn percentile_hash_original_corpus_actual_full_query_source() {
    let sql = CORPUS
        .split_once("-- query 5")
        .unwrap()
        .1
        .split_once("-- query 6")
        .unwrap()
        .0
        .replace("${case_db}", "fixture");
    source(sql.trim(), 1);
}
#[test]
fn percentile_hash_original_live_variadic_source_and_ignored_volatile_tail() {
    for sql in [
        "SELECT percentile_hash(v,w) FROM fixture.t_agg_percentile_semantics",
        "SELECT percentile_hash(v,w,id,v,w) FROM fixture.t_agg_percentile_semantics",
        "SELECT percentile_hash(v,CAST(sleep(0) AS INT)) FROM fixture.t_agg_percentile_semantics",
    ] {
        source(sql, if sql.contains("w,id") { 5 } else { 2 });
    }
}
