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

use super::*;
use crate::analysis::{ExprKind, QueryBody};
struct EmptyCatalog;
impl crate::catalog::PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Err(format!("table not found: {table}"))
    }
}
fn analyze(sql: &str, mode: &str, limit: Option<i64>) -> crate::analysis::ResolvedQuery {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query")
    };
    let mut settings = crate::sql_mode::SqlSemanticSettings::default()
        .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode));
    if let Some(raw) = limit {
        settings = settings.with_group_concat_max_len(raw);
    }
    crate::analyzer::analyze_with_function_catalog_and_sql_semantics(
        query,
        &EmptyCatalog,
        "default",
        crate::functions::builtin_sql_function_catalog(),
        &settings,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
    .0
}
fn facts(query: &crate::analysis::ResolvedQuery) -> &crate::binding::GroupConcatSourceFacts {
    let QueryBody::Select(select) = &query.body else {
        panic!("select")
    };
    let ExprKind::AggregateCall { resolved, .. } = &select.projection[0].expr.kind else {
        panic!("aggregate")
    };
    resolved.group_concat_source().unwrap()
}
#[test]
fn actual_gc_call_captures_lexical_hint_raw_limit_distinct_and_order() {
    let query = analyze(
        "select /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(distinct 'a' order by 1 desc nulls first separator '|')",
        "32",
        Some(-1),
    );
    let actual = facts(&query);
    assert!(actual.legacy);
    assert_eq!(actual.max_len, Some(-1));
    assert!(actual.state.distinct);
    assert_eq!(
        actual.state.order_keys.as_ref(),
        [novarocks_type_contract::AggregateStateOrderKey {
            ascending: false,
            nulls_first: true
        }]
    );
}
#[test]
fn explicit_separator_does_not_guess_mode_and_missing_limit_stays_absent() {
    for mode in ["32", "GROUP_CONCAT_LEGACY"] {
        let query = analyze("select group_concat('a' separator '|')", mode, None);
        assert_eq!(facts(&query).legacy, mode == "GROUP_CONCAT_LEGACY");
        assert_eq!(facts(&query).max_len, None);
        assert!(!facts(&query).state.distinct);
        assert!(facts(&query).state.order_keys.is_empty());
    }
}
