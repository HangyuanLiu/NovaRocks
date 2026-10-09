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
//! Original SQL analysis baseline. No support gate or replacement binding author.
use novarocks_sql::compiler::{
    FoldRequest, SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent,
    SqlCompiler, SqlConstantEvaluationError, SqlConstantEvaluator, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
};
use novarocks_type_contract::PureCompileControl;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) struct CountingEvaluator(AtomicUsize);
impl SqlConstantEvaluator for CountingEvaluator {
    fn eval_scalar(
        &self,
        _: &FoldRequest,
        _: &dyn PureCompileControl,
    ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}
pub(super) static SERIAL: Mutex<()> = Mutex::new(());
pub(super) static EVALUATOR: CountingEvaluator = CountingEvaluator(AtomicUsize::new(0));
pub(super) fn reset() {
    EVALUATOR.0.store(0, Ordering::Relaxed);
}
pub(super) fn fold_count() -> usize {
    EVALUATOR.0.load(Ordering::Relaxed)
}
pub(super) fn session() -> SqlSessionContext {
    SqlSessionContext {
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        current_catalog: None,
        current_database: "fixture".into(),
        optimizer_settings: SessionOptimizerSettings {
            enable_materialized_view_rewrite: Some(false),
            ..SessionOptimizerSettings::default()
        },
    }
}

fn analyze(sql: &str) {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let request = SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        &catalog,
        builtin_sql_function_catalog(),
        &EVALUATOR,
        None,
        super::pure_differential::constant_policy(),
        SqlCompileControl::unbounded(),
    );
    SqlCompiler::analyze(request)
        .unwrap()
        .into_pending()
        .unwrap();
}

#[test]
fn sql_presence_original_hour_literal_is_bound_without_fold_or_owner_admission() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    analyze("SELECT hour_from_unixtime(CAST(1700000000 AS BIGINT)) AS h");
    assert_eq!(fold_count(), 0);
}
#[test]
fn sql_presence_original_hour_projected_column_is_bound_without_fold() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    analyze("SELECT hour_from_unixtime(t.k) AS h FROM (SELECT CAST(1700000000 AS BIGINT) AS k) t");
    assert_eq!(fold_count(), 0);
}
#[test]
fn sql_presence_original_invalid_regexp_literal_is_not_a_binding_error() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    analyze("SELECT regexp_count('x', '[') AS n");
    assert_eq!(fold_count(), 0);
}
