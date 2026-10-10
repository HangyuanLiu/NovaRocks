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
//! Original five-lifecycle SQL binding baseline; no owner preparation.
use super::sql_scalar_presence_original_tests::{EVALUATOR, SERIAL, fold_count, reset, session};
use novarocks_sql::compiler::{
    SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler, SqlPhysicalEmissionMode,
    SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlStatementInput,
    builtin_sql_function_catalog,
};

#[test]
fn e08s1_original_aggregate_window_and_aggregate_over_bind_without_presence_or_fold() {
    let _serial = SERIAL.lock().unwrap();
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&catalog);
    for sql in [
        "SELECT dict_merge('x', 1)",
        "SELECT session_number(1, 2) OVER (ORDER BY 1)",
        "SELECT max_by(1, 2) OVER ()",
        "SELECT count(1) OVER ()",
        "SELECT row_number() OVER ()",
    ] {
        reset();
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            session(),
            SqlPlanningEnvironment::Distributed,
            &tables,
            builtin_sql_function_catalog(),
            &EVALUATOR,
            None,
            super::pure_differential::constant_policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            SqlCompileControl::unbounded(),
        );
        SqlCompiler::analyze(request)
            .unwrap()
            .into_pending()
            .unwrap();
        assert_eq!(fold_count(), 0, "original analysis never evaluates {sql}");
    }
}
