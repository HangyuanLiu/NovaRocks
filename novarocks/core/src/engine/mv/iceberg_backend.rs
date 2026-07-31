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

//! Iceberg-backed materialized-view backend.

use std::sync::{Arc, Weak};

use crate::connector::backend::MvBackend;
use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::{
    BackendRefreshOutcome, BackendRefreshPlan, CreateMvRequest, DropMvRequest, ListMvsRequest,
    MvListRow, RefreshCtx, RefreshError, RefreshOutcome, RefreshPlan, RefreshRequest,
};
use crate::mv::model::MvStorageEngine;

pub(crate) struct IcebergMvBackend {
    state: Weak<StandaloneState>,
}

impl IcebergMvBackend {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl MvBackend for IcebergMvBackend {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::create_iceberg_mv_with_connector_context(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::drop_iceberg_mv_with_connector_context(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
            &req.connector_context,
        )
        .map(|_| ())
    }

    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        let state = self.state()?;
        crate::engine::mv::analysis_adapter::list_mv_rows(
            &state,
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::Iceberg),
        )
    }

    fn plan_refresh(
        &self,
        req: RefreshRequest,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<RefreshPlan, RefreshError> {
        let state = self.state().map_err(RefreshError::pre_commit)?;
        crate::engine::mv::iceberg_refresh::plan_iceberg_mv_refresh_with_connector_context(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.statement,
            req.target,
            connector_context,
        )
    }

    fn execute_refresh(
        &self,
        plan: &RefreshPlan,
        ctx: &mut RefreshCtx,
    ) -> Result<RefreshOutcome, RefreshError> {
        let BackendRefreshPlan::Iceberg(plan_payload) = &plan.backend_plan else {
            return Err(RefreshError::user(
                "iceberg backend received non-iceberg refresh plan",
            ));
        };
        let state = self.state().map_err(RefreshError::pre_commit)?;
        let outcome =
            crate::engine::mv::iceberg_refresh::execute_iceberg_mv_refresh_with_connector_context(
                &state,
                plan_payload,
                &plan.contract,
                &ctx.connector_context,
            )?;
        Ok(RefreshOutcome {
            mv_id: plan.contract.mv_id,
            target: plan.contract.target.clone(),
            rows: None,
            base_snapshots: Default::default(),
            base_table_uuids: Default::default(),
            target_snapshot_id: None,
            backend_outcome: BackendRefreshOutcome::Iceberg(outcome),
        })
    }

    fn commit_refresh(
        &self,
        _outcome: &RefreshOutcome,
        _ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError> {
        Ok(())
    }

    fn rollback_refresh(
        &self,
        _outcome: Option<&RefreshOutcome>,
        _ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError> {
        Ok(())
    }
}
