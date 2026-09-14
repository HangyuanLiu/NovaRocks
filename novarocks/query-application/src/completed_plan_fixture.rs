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

//! One really completed plan, for tests of what happens to a plan afterwards.
//!
//! It is produced by the real completion driver rather than assembled, because
//! the things tested against it - pairing, activation, dispatch - are about a
//! plan that has actually been validated and paired with its capabilities.
//! A statement over literal rows needs no outside fact, so the fixture needs no
//! catalog, no provider and no statistics behind it.

use std::sync::Arc;

use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};
use novarocks_workload_control::{
    ResourceConfig, RootWork, WorkClass, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
};

use crate::preparation::{
    CompletedPlanWithAccess, FinalPlanCompletionDriver, ReadAccessSink, SqlCompletionFactSource,
};

struct NoFacts;

#[async_trait::async_trait]
impl SqlCompletionFactSource for NoFacts {
    type Access = ();

    async fn resolve(
        &self,
        _: &SqlNeedBatch,
        _: &ReadAccessSink<()>,
    ) -> Result<SqlFactBatch, String> {
        panic!("a VALUES plan asks for nothing")
    }
}

/// A completed plan over literal rows, carrying the given version.
pub(crate) async fn completed_values_plan(version: [u8; 16]) -> CompletedPlanWithAccess<()> {
    let (_root, scope) = query_scope();
    FinalPlanCompletionDriver::new(Arc::new(NoFacts))
        .complete(values_request(version), &scope)
        .await
        .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"))
}

fn query_scope() -> (RootWork, WorkScope) {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024,
            control_bytes: 128,
            per_scope_bytes: 896,
        },
    )
    .expect("workload control");
    control.mark_ready().expect("workload control ready");
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .expect("query root");
    let scope = root.owner.scope();
    (root, scope)
}

fn values_request(version: [u8; 16]) -> SqlFinalPlanCompileRequest {
    SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new(version).expect("plan version"),
        SqlStatementInput::sql("SELECT 1"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            current_catalog: Some("iceberg".to_string()),
            current_database: "db".to_string(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        noop_constant_evaluator(),
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: MAX_SCAN_BATCH_ROWS,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
}
