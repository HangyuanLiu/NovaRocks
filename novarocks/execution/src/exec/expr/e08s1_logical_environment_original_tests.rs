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
//! Original public LogicalOnly analysis uses actual lexical bindings without folding.
use super::sql_scalar_presence_original_tests::{EVALUATOR, SERIAL, fold_count, reset, session};
use novarocks_sql::compiler::{
    SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent, SqlCompiler,
    SqlPhysicalEmissionMode, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlStatementInput,
    builtin_sql_function_catalog,
};
pub(super) fn logical(
    sql: &str,
    mode: SqlPhysicalEmissionMode,
    raw_limit: Option<i64>,
) -> Result<(), SqlCompileError> {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&catalog);
    let mut source_session = session();
    if let Some(raw) = raw_limit {
        source_session.sql_semantics = source_session.sql_semantics.with_group_concat_max_len(raw);
    }
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::LogicalOnly,
        source_session,
        SqlPlanningEnvironment::Distributed,
        &tables,
        builtin_sql_function_catalog(),
        &EVALUATOR,
        None,
        super::pure_differential::constant_policy(),
        mode,
        SqlCompileControl::unbounded(),
    ))?
    .into_complete()?;
    Ok(())
}
#[test]
fn e08s1_logical_environment_original_absent_source_does_not_add_admission() {
    let _serial = SERIAL.lock().unwrap();
    for sql in [
        "SELECT GROUP_CONCAT(REVERSE('abc'))",
        "SELECT FROM_UNIXTIME(CAST(ABS(CAST(1 AS BIGINT)) AS BIGINT))",
    ] {
        reset();
        logical(sql, SqlPhysicalEmissionMode::OriginalNativeV1, None).unwrap();
        assert_eq!(fold_count(), 0, "LogicalOnly does not run a calculator");
    }
}
