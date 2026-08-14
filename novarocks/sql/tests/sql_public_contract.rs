// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::num::NonZeroUsize;

use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use novarocks_sql::compiler::{
    SessionOptimizerSettings, SqlCatalogSnapshot, SqlCompileControl, SqlCompileIntent,
    SqlCompileRequest, SqlCompiler, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_sql::planning::dml::DmlStatisticsSnapshot;

struct EmptyCatalog;

impl PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        _database: &str,
        _table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        Err("the public-contract query has no tables".to_string())
    }
}

impl SqlCatalogSnapshot for EmptyCatalog {
    fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
        self
    }
}

#[test]
fn external_sql_contract_compiles_and_reads_a_sealed_plan() {
    let catalog = EmptyCatalog;
    let statistics = DmlStatisticsSnapshot::empty();
    let request = SqlCompileRequest::new(
        SqlStatementInput::sql("SELECT 1"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            current_catalog: None,
            current_database: "default".to_string(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed {
            backend_count: NonZeroUsize::new(1).expect("one is non-zero"),
        },
        &catalog,
        &statistics,
        builtin_sql_function_catalog(),
        None,
        SqlCompileControl::unbounded(),
    );

    let plan = SqlCompiler::compile(request)
        .expect("public compiler request compiles")
        .into_distributed_plan()
        .expect("query intent produces a sealed distributed plan");
    assert!(!plan.fragments().is_empty());

    let _: Option<SqlTableBindingId> = None;
}
