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

//! Frontend-owned table-maintenance parser, repository, and dispatch service.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks::connector::metadata_maintenance::MetadataMaintenanceIntent;
use novarocks::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceStatementResult, MaintenanceTarget, OptimizeSubmission, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_spi::connector::ConnectorMutationOperationId;
use novarocks_spi::state_store::StateStore;
use tokio::runtime::Handle;

use self::model::{
    MetadataMaintenanceExactOwner, MetadataMaintenanceOpaquePayload,
    MetadataMaintenanceOperationCreate, MetadataMaintenanceOperationKind,
    MetadataMaintenancePlanPayload, OptimizeJobCreate,
};
use self::parser::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, is_spark_maintenance_call,
    parse_maintenance_statement, parse_show_optimize,
};
use self::repository::{
    MetadataMaintenanceOperationRepository, OptimizeJobRepository, RepositoryErrorKind,
    metadata_maintenance_payload_digest,
};
use self::result::{action_result, optimize_jobs_result};
use self::worker::OptimizeWorker;

pub mod model;
pub mod parser;
pub mod repository;
pub mod result;
pub mod worker;

const OPTIMIZE_STATE_STORE_REQUIRED: &str = "ALTER TABLE OPTIMIZE requires frontend StateStore";
const SHOW_STATE_STORE_REQUIRED: &str = "SHOW ALTER TABLE OPTIMIZE requires frontend StateStore";
const AUTOMATIC_OPTIMIZE_STATE_STORE_REQUIRED: &str =
    "automatic optimize requires frontend StateStore";
const METADATA_MAINTENANCE_STATE_STORE_REQUIRED: &str =
    "connector metadata maintenance requires frontend StateStore";

enum WorkerLifecycle {
    NotStarted,
    Started(Option<OptimizeWorker>),
    Stopped(Result<(), String>),
}

// Design: ADR-0009 (docs/adr/ADR-0009-frontend-table-maintenance-owner.md)
pub struct FrontendTableMaintenanceService {
    repository: Option<Arc<OptimizeJobRepository>>,
    metadata_repository: Option<Arc<MetadataMaintenanceOperationRepository>>,
    worker: Mutex<WorkerLifecycle>,
    runtime: Handle,
}

impl FrontendTableMaintenanceService {
    pub async fn open(store: Option<Arc<dyn StateStore>>, runtime: Handle) -> Result<Self, String> {
        let (repository, metadata_repository) = match store {
            Some(store) => (
                Some(Arc::new(
                    OptimizeJobRepository::open(Arc::clone(&store))
                        .await
                        .map_err(|error| {
                            format!("open frontend optimize job repository failed: {error}")
                        })?,
                )),
                Some(Arc::new(
                    MetadataMaintenanceOperationRepository::open(store)
                        .await
                        .map_err(|error| {
                            format!("open frontend metadata maintenance repository failed: {error}")
                        })?,
                )),
            ),
            None => (None, None),
        };
        Ok(Self {
            repository,
            metadata_repository,
            worker: Mutex::new(WorkerLifecycle::NotStarted),
            runtime,
        })
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn execute_user_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        action: ParsedMaintenanceAction,
        spark_result: bool,
    ) -> Result<MaintenanceStatementResult, String> {
        engine.reject_user_action_on_mv(&target)?;
        if let ParsedMaintenanceAction::RewriteManifests {
            use_caching,
            spec_id,
        } = action.clone()
        {
            let outcome = self.execute_durable_metadata_action(
                engine,
                target,
                MetadataMaintenanceIntent::rewrite_metadata_layout(),
                MetadataMaintenanceOperationKind::RewriteMetadataLayout,
                use_caching,
                spec_id,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        if let ParsedMaintenanceAction::ExpireSnapshots {
            older_than_ms,
            retain_last,
        } = action.clone()
        {
            let outcome = self.execute_durable_metadata_action(
                engine,
                target,
                MetadataMaintenanceIntent::expire_table_versions(older_than_ms, retain_last),
                MetadataMaintenanceOperationKind::ExpireTableVersions,
                None,
                None,
            )?;
            return if spark_result {
                action_result(outcome)
            } else {
                Ok(MaintenanceStatementResult::Ok)
            };
        }
        let request = action.into_request(engine, target)?;
        let outcome = engine.execute_action(request)?;
        if spark_result {
            action_result(outcome)
        } else {
            Ok(MaintenanceStatementResult::Ok)
        }
    }

    fn execute_durable_metadata_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
        intent: MetadataMaintenanceIntent,
        kind: MetadataMaintenanceOperationKind,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    ) -> Result<MaintenanceActionOutcome, String> {
        if use_caching.is_some() {
            return Err(
                "rewrite_manifests `use_caching` is not implemented in NovaRocks yet".to_string(),
            );
        }
        if spec_id.is_some() {
            return Err(
                "rewrite_manifests `spec_id` is not implemented in NovaRocks yet".to_string(),
            );
        }
        let repository = self
            .metadata_repository
            .as_ref()
            .ok_or_else(|| METADATA_MAINTENANCE_STATE_STORE_REQUIRED.to_string())?;
        let operation_id = ConnectorMutationOperationId::new();
        let durable_id = uuid::Uuid::from_bytes(operation_id.to_bytes());
        let session = engine.plan_metadata_maintenance(&target, operation_id, intent)?;
        let plan = session.plan_ref();
        let request_payload = plan.provider_payload().to_vec();
        let request_payload_digest = metadata_maintenance_payload_digest(&request_payload);
        self.block_on(repository.create(MetadataMaintenanceOperationCreate {
            operation_id: durable_id,
            target: target.clone(),
            owner: MetadataMaintenanceExactOwner {
                instance_id: plan.owner().instance_id.as_str().to_string(),
                incarnation_id: uuid::Uuid::from_bytes(plan.owner().incarnation.to_bytes()),
            },
            kind,
            request_digest: plan.request_digest(),
            request_payload_digest,
            base_state_digest: plan.state_digest(),
            request_payload,
            created_at_ms: now_unix_millis(),
        }))
        .map_err(|error| {
            format!("persist metadata maintenance pending operation failed: {error}")
        })?;
        let plan_payload = plan.provider_payload().to_vec();
        self.block_on(repository.start(
            durable_id,
            MetadataMaintenancePlanPayload {
                plan_digest: plan.plan_digest(),
                payload_digest: metadata_maintenance_payload_digest(&plan_payload),
                payload: plan_payload,
            },
            now_unix_millis(),
        ))
        .map_err(|error| format!("persist metadata maintenance plan failed: {error}"))?;
        match engine.execute_planned_metadata_maintenance(session) {
            Ok(completed) => {
                let receipt = completed.receipt;
                self.block_on(repository.finish(
                    durable_id,
                    MetadataMaintenanceOpaquePayload {
                        digest: metadata_maintenance_payload_digest(receipt.provider_payload()),
                        payload: receipt.provider_payload().to_vec(),
                    },
                    now_unix_millis(),
                ))
                .map_err(|error| format!("persist metadata maintenance receipt failed: {error}"))?;
                let summary = receipt.summary();
                Ok(match kind {
                    MetadataMaintenanceOperationKind::RewriteMetadataLayout => {
                        MaintenanceActionOutcome::RewriteManifests {
                            rewritten_manifests_count: i32::try_from(summary.rewritten_items)
                                .map_err(|_| {
                                    "rewrite manifest count exceeds Spark result range".to_string()
                                })?,
                            added_manifests_count: i32::try_from(summary.added_items).map_err(
                                |_| "added manifest count exceeds Spark result range".to_string(),
                            )?,
                        }
                    }
                    MetadataMaintenanceOperationKind::ExpireTableVersions => {
                        MaintenanceActionOutcome::ExpireSnapshots {
                            deleted_data_files_count: None,
                            deleted_position_delete_files_count: None,
                            deleted_equality_delete_files_count: None,
                            deleted_manifest_files_count: None,
                            deleted_manifest_lists_count: None,
                            deleted_statistics_files_count: None,
                        }
                    }
                })
            }
            Err(error) => {
                // The engine-facing compatibility port deliberately does not
                // claim that a provider error happened before dispatch.
                // Preserve the operation fence and opaque evidence for an
                // exact-generation reconcile owner.
                let evidence = error.as_bytes().to_vec();
                self.block_on(repository.mark_reconcile_pending(
                    durable_id,
                    MetadataMaintenanceOpaquePayload {
                        digest: metadata_maintenance_payload_digest(&evidence),
                        payload: evidence,
                    },
                ))
                .map_err(|store| {
                    format!("record metadata maintenance reconcile state failed: {store}")
                })?;
                Err(error)
            }
        }
    }

    fn submit_user_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceStatementResult, String> {
        engine.reject_user_action_on_mv(&target)?;
        self.submit_user_optimize_inner(engine, target)?;
        Ok(MaintenanceStatementResult::Ok)
    }

    fn submit_user_optimize_inner(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<(), String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| OPTIMIZE_STATE_STORE_REQUIRED.to_string())?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        let request = OptimizeJobCreate {
            target: target.clone(),
            base_snapshot_id,
            created_at_ms: now_unix_millis(),
        };
        match self.block_on(repository.create(request)) {
            Ok(_) => {
                self.wakeup_worker()?;
                Ok(())
            }
            Err(error) if error.kind() == RepositoryErrorKind::AlreadyActive => Err(format!(
                "ALTER TABLE OPTIMIZE: create iceberg optimize job failed: {error}"
            )),
            Err(error) => Err(format!(
                "create frontend optimize job for {}.{}.{} failed: {error}",
                target.catalog, target.namespace, target.table
            )),
        }
    }

    fn submit_automatic_optimize_inner(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| AUTOMATIC_OPTIMIZE_STATE_STORE_REQUIRED.to_string())?;
        let base_snapshot_id = engine.current_snapshot_id(&target)?;
        match self.block_on(repository.create(OptimizeJobCreate {
            target,
            base_snapshot_id,
            created_at_ms: now_unix_millis(),
        })) {
            Ok(job) => {
                self.wakeup_worker()?;
                Ok(OptimizeSubmission::Submitted { job_id: job.job_id })
            }
            Err(error) if error.kind() == RepositoryErrorKind::AlreadyActive => {
                Ok(OptimizeSubmission::AlreadyActive)
            }
            Err(error) => Err(format!("submit automatic optimize failed: {error}")),
        }
    }

    fn show_optimize(
        &self,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        let repository = self
            .repository
            .as_ref()
            .ok_or_else(|| SHOW_STATE_STORE_REQUIRED.to_string())?;
        let statement = parse_show_optimize(sql)?;
        let mut jobs = self
            .block_on(repository.list())
            .map_err(|error| format!("show frontend optimize jobs failed: {error}"))?;
        let catalog_filter = statement.catalog.as_deref().or(context.current_catalog);
        let database_filter = statement
            .database
            .as_deref()
            .unwrap_or(context.current_database);
        if let Some(catalog) = catalog_filter {
            jobs.retain(|job| job.target.catalog == catalog);
        }
        jobs.retain(|job| job.target.namespace == database_filter);
        if let Some(table_name) = statement.table_name.as_deref() {
            jobs.retain(|job| job.target.table == table_name);
        }
        jobs.sort_by_key(|job| (job.created_at_ms, job.job_id));
        if statement.order_by_create_time_desc {
            jobs.reverse();
        }
        if let Some(limit) = statement.limit {
            jobs.truncate(limit);
        }
        optimize_jobs_result(jobs)
    }

    fn wakeup_worker(&self) -> Result<(), String> {
        let worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        if let WorkerLifecycle::Started(Some(worker)) = &*worker {
            worker.wakeup();
        }
        Ok(())
    }
}

impl TableMaintenanceService for FrontendTableMaintenanceService {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        match &*worker {
            WorkerLifecycle::NotStarted => {
                let optimize_worker = self
                    .repository
                    .as_ref()
                    .map(|repository| {
                        OptimizeWorker::start(
                            &self.runtime,
                            Arc::clone(repository),
                            Arc::downgrade(&engine),
                        )
                    })
                    .transpose()?;
                *worker = WorkerLifecycle::Started(optimize_worker);
                Ok(())
            }
            WorkerLifecycle::Started(_) => {
                Err("table maintenance service is already started".to_string())
            }
            WorkerLifecycle::Stopped(_) => {
                Err("table maintenance service cannot be restarted after shutdown".to_string())
            }
        }
    }

    fn try_handle_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String> {
        let Some(statement) = parse_maintenance_statement(sql, context)? else {
            return Ok(None);
        };
        let result = match statement {
            ParsedMaintenanceStatement::Execute { name_parts, action } => {
                let target = engine.resolve_target(&name_parts, context)?;
                self.execute_user_action(engine, target, action, is_spark_maintenance_call(sql))?
            }
            ParsedMaintenanceStatement::SubmitOptimize { name_parts } => {
                if self.repository.is_none() {
                    return Err(OPTIMIZE_STATE_STORE_REQUIRED.to_string());
                }
                let target = engine.resolve_target(&name_parts, context)?;
                self.submit_user_optimize(engine, target)?
            }
            ParsedMaintenanceStatement::ShowOptimize => self.show_optimize(sql, context)?,
        };
        Ok(Some(result))
    }

    fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        engine.execute_action(request)
    }

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        self.submit_automatic_optimize_inner(engine, target)
    }

    fn shutdown(&self) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        let lifecycle = std::mem::replace(&mut *worker, WorkerLifecycle::Stopped(Ok(())));
        drop(worker);

        let result = match lifecycle {
            WorkerLifecycle::NotStarted => Ok(()),
            WorkerLifecycle::Started(Some(mut worker)) => worker.shutdown(),
            WorkerLifecycle::Started(None) => Ok(()),
            WorkerLifecycle::Stopped(result) => result,
        };
        let mut worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        *worker = WorkerLifecycle::Stopped(result.clone());
        result
    }
}

impl ParsedMaintenanceAction {
    fn into_request(
        self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceActionRequest, String> {
        match self {
            Self::RewriteDataFiles {
                options,
                branch,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewriteDataFiles {
                base_snapshot_id: engine.current_snapshot_id(&target)?,
                target,
                job_id: None,
                options,
                branch,
                where_clause,
            }),
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => Ok(MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            }),
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => Ok(MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            }),
            Self::RemoveOrphanFiles { older_than_ms } => {
                Ok(MaintenanceActionRequest::RemoveOrphanFiles {
                    target,
                    older_than_ms,
                })
            }
            Self::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            }),
        }
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
