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

//! Connector-agnostic write and materialized-view backend traits.

use arrow::record_batch::RecordBatch;

use crate::engine::mv::lifecycle::{
    CreateMvRequest, DropMvRequest, ListMvsRequest, MvListRow, RefreshCtx, RefreshError,
    RefreshOutcome, RefreshPlan, RefreshRequest,
};
use crate::sql::parser::ast::Literal;
use novarocks_catalog::schema::ColumnDef;

/// Resolved table metadata returned by the connector metadata SPI. This is
/// the subset of table shape the engine layer needs in order to plan INSERTs
/// and to register the table with the in-memory logical catalog.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

/// Write-side: append rows or RecordBatches to a table. The INSERT
/// orchestration layer (`insert_flow.rs`, Phase 3) chooses between the two
/// depending on whether the source is literal VALUES or a pipeline result.
pub(crate) trait TableSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String>;
    fn append_batch(&self, table: &ResolvedTable, batch: RecordBatch) -> Result<(), String>;

    /// Whether this trait path supports INSERT SELECT materialized as a
    /// RecordBatch. FE-driven Iceberg pipeline sinks use
    /// `IcebergTableSinkFactory` directly and do not go through this trait.
    fn supports_pipeline_insert(&self) -> bool;
}

/// Materialized-view backend: CREATE / DROP / REFRESH / SHOW. Today only
/// StarRocks table implements this. Future backends (e.g. iceberg-as-MV-target)
/// plug in here.
pub(crate) trait MvBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String>;
    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String>;
    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String>;

    fn plan_refresh(
        &self,
        req: RefreshRequest,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<RefreshPlan, RefreshError>;
    fn execute_refresh(
        &self,
        plan: &RefreshPlan,
        ctx: &mut RefreshCtx,
    ) -> Result<RefreshOutcome, RefreshError>;
    fn commit_refresh(
        &self,
        outcome: &RefreshOutcome,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
    fn rollback_refresh(
        &self,
        outcome: Option<&RefreshOutcome>,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
}
