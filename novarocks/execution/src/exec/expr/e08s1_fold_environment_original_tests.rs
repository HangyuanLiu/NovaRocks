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
//! Original optimizer entrypoint, lexical source and actual fold-child observation.
use super::sql_scalar_presence_original_tests::{EVALUATOR, SERIAL, fold_count, reset, session};
use novarocks_sql::compiler::{
    SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent, SqlCompiler,
    SqlOptimizeRequest, SqlPhysicalEmissionMode, SqlPlannerTableSnapshot, SqlPlanningEnvironment,
    SqlStatementInput, builtin_sql_function_catalog,
};
pub(super) fn optimize(
    sql: &str,
    mode: SqlPhysicalEmissionMode,
    max_len: Option<i64>,
) -> Result<(), SqlCompileError> {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let tables = SqlPlannerTableSnapshot::new(&catalog);
    let mut source_session = session();
    if let Some(raw) = max_len {
        source_session.sql_semantics = source_session.sql_semantics.with_group_concat_max_len(raw);
    }
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        source_session,
        SqlPlanningEnvironment::Distributed,
        &tables,
        builtin_sql_function_catalog(),
        &EVALUATOR,
        None,
        super::pure_differential::constant_policy(),
        mode,
        control.clone(),
    ))?
    .into_pending()?;
    let statistics = novarocks_sql::planning::dml::DmlStatisticsSnapshot::empty();
    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics, control))?;
    Ok(())
}
#[test]
fn e08s1_fold_environment_original_concat_missing_limit_still_reaches_child_fold() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    optimize(
        "SELECT GROUP_CONCAT(REVERSE('abc'))",
        SqlPhysicalEmissionMode::OriginalNativeV1,
        None,
    )
    .unwrap();
    assert!(
        fold_count() > 0,
        "original child calculator is genuinely observed"
    );
}
#[test]
fn e08s1_fold_environment_original_unixtime_absent_zone_still_reaches_child_fold() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    optimize(
        "SELECT FROM_UNIXTIME(CAST(ABS(CAST(1 AS BIGINT)) AS BIGINT))",
        SqlPhysicalEmissionMode::OriginalNativeV1,
        None,
    )
    .unwrap();
    assert!(
        fold_count() > 0,
        "original child calculator is genuinely observed"
    );
}
