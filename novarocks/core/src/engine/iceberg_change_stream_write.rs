// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Engine-owned execution adapter for planned Iceberg change-stream writes.
//!
//! Generic writer-report routing belongs to the Iceberg connector. This module
//! keeps only the standalone engine callback that builds and executes the
//! physical change-stream write plan inside the write transaction lifecycle.

use std::sync::{Arc, Mutex};

use crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan;
use crate::connector::iceberg::commit::{CleanupAttempt, CommitOutcome, CommitServiceError};
use crate::coordinator::execution::CoordinatedQueryResult;
use crate::engine::StandaloneState;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteTransactionExecutor, IcebergWriteTransactionSpec,
};
use crate::query_execution::write::WriteCommitInput;
use crate::runtime::query_options::QueryOptions;
use crate::sql::optimizer::OptimizedOperatorNode;
use crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec;

pub(crate) struct ChangeStreamPhysicalBuildInput {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) optimized_tree: OptimizedOperatorNode,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) query_opts: Option<QueryOptions>,
    pub(crate) mv_refresh_ctx:
        Option<Arc<crate::mv::refresh::execution_context::IcebergMvRefreshContext>>,
}

pub(crate) struct ChangeStreamWriteTransactionExecutor {
    build_input: Mutex<Option<ChangeStreamPhysicalBuildInput>>,
    commit_executor: IcebergWriteCommitExecutor,
    commit_plan: Mutex<Option<ChangeStreamWriterCommitPlan>>,
}

impl ChangeStreamWriteTransactionExecutor {
    pub(crate) fn new(
        build_input: ChangeStreamPhysicalBuildInput,
        commit_executor: IcebergWriteCommitExecutor,
    ) -> Self {
        Self {
            build_input: Mutex::new(Some(build_input)),
            commit_executor,
            commit_plan: Mutex::new(None),
        }
    }
}

impl IcebergWriteTransactionExecutor for ChangeStreamWriteTransactionExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut build_input = self
            .build_input
            .lock()
            .expect("change-stream build input lock poisoned")
            .take()
            .ok_or_else(|| "change-stream write build input was already consumed".to_string())?;
        let planned = crate::engine::build_physical_plan_as_iceberg_change_stream_write(
            &build_input.state,
            build_input.current_catalog.as_deref(),
            &build_input.current_database,
            &build_input.optimized_tree,
            &mut build_input.dag,
            build_input.mv_refresh_ctx.as_deref(),
            None,
        )?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            prepared,
            native_bundle,
            commit_plan,
            ..
        } = planned;
        *self
            .commit_plan
            .lock()
            .expect("change-stream commit plan lock poisoned") = Some(commit_plan);
        crate::engine::execute_planned_iceberg_change_stream_write(
            prepared,
            native_bundle,
            build_input.query_opts.clone(),
        )
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let guard = self
            .commit_plan
            .lock()
            .expect("change-stream commit plan lock poisoned");
        let plan = guard.as_ref().ok_or_else(|| {
            CommitServiceError::known_uncommitted(
                "change-stream commit plan is missing; coordinated write did not complete"
                    .to_string(),
                CleanupAttempt::not_attempted(),
            )
        })?;
        self.commit_executor
            .commit_change_stream_write_input(write_commit, plan)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}
