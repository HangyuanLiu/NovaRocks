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

//! Frontend adapter bound to the MV background runtime.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::mv::domain::dependency::refresh::build_upstream_refresh_steps_with_readiness;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::lifecycle::{RefreshError, RefreshErrorKind};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshPreparationRequest, MvRefreshPreparationService, PreparedMvRefresh,
};
use novarocks_mv_application::dependency::iceberg_mv_dependency_ref;
use novarocks_mv_application::persistence::codec::{PublicationInput, RelationOccurrence};
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::identity::{NativeDataVersion, ObjectIdentity};
use novarocks_mv_application::product::MvRefreshAttemptIdentity;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorRequestContext, ConnectorStopOwner,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_sql::planning::mv::MvRefreshStatement;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_table_maintenance::MaintenanceTarget;

use super::background::{MvBackgroundEngine, MvRefreshStep};
use novarocks_mv_application::maintenance::{
    MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
};

type SourceRevisionKey = (String, String, String, Vec<u8>);
type SourceRevision = (ObjectIdentity, NativeDataVersion);

fn background_connector_request_context() -> Result<ConnectorRequestContext, String> {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(300),
        ConnectorStopOwner::new().view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|context| {
        context.with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background)
    })
    .map_err(|error| error.to_string())
}

#[derive(Clone)]
/// FE-role adapter that binds MV background product callbacks to the native
/// query and connector capabilities of this process.
pub(crate) struct FrontendMvBackgroundEngine {
    ports: IcebergMvCorePorts,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    storage_observation: Arc<dyn novarocks_spi::connector::MvStorageObservationPort>,
}

impl FrontendMvBackgroundEngine {
    pub(crate) fn new_with_ports(
        ports: IcebergMvCorePorts,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        storage_observation: Arc<dyn novarocks_spi::connector::MvStorageObservationPort>,
    ) -> Self {
        Self {
            ports,
            connector_control,
            readiness,
            storage_observation,
        }
    }

    fn mv_id_for_target(&self, target: &MvTarget) -> Result<i64, MvBackgroundEngineError> {
        let projection = self
            .readiness
            .load_ready(target)
            .map_err(repository_error)?
            .ok_or_else(|| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TargetGone,
                    format!("MV target {} no longer exists", target.display_name()),
                )
            })?;
        Ok(projection.projection.mv_id)
    }
}

#[async_trait::async_trait]
impl MvBackgroundEngine for FrontendMvBackgroundEngine {
    fn resolve_refresh_steps(
        &self,
        target: &MvTarget,
    ) -> Result<Vec<MvRefreshStep>, MvBackgroundEngineError> {
        let requested = iceberg_mv_dependency_ref(
            target.catalog.as_deref().unwrap_or("default_catalog"),
            &target.database,
            &target.name,
        );
        let steps =
            build_upstream_refresh_steps_with_readiness(self.readiness.as_ref(), &requested)
                .map_err(|error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        error,
                    )
                })?;
        steps
            .into_iter()
            .map(|step| {
                if !step.is_iceberg() {
                    return Err(MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        format!(
                            "MV refresh step {} is not Iceberg-backed",
                            step.display_name()
                        ),
                    ));
                }
                let mv_id = self.mv_id_for_target(step.target())?;
                Ok(MvRefreshStep {
                    mv_id,
                    target: step.into_target(),
                })
            })
            .collect()
    }

    fn prepare_refresh_step(
        &self,
        step: &MvRefreshStep,
        attempt: MvRefreshAttemptIdentity,
        connector_context: &ConnectorRequestContext,
    ) -> Result<PreparedMvRefresh, MvBackgroundEngineError> {
        let statement = MvRefreshStatement {
            name_parts: vec![step.target.name.clone()],
            full: false,
        };
        let request = crate::mv::domain::application::MvRefreshRequest {
            name_parts: vec![step.target.name.clone()],
            full: false,
        };
        let service = crate::query_execution::mv_assembly::refresh_preparation::FrontendMvRefreshPreparationService::new_with_ports(
            &self.ports,
            step.target.catalog.as_deref(),
            &step.target.database,
            &request,
            connector_context,
        );
        service
            .prepare_step(MvRefreshPreparationRequest {
                statement,
                target: step.target.clone(),
                attempt,
            })
            .map_err(preparation_error)
    }

    fn current_source_inputs(
        &self,
        occurrences: &[RelationOccurrence],
    ) -> Result<Vec<PublicationInput>, MvBackgroundEngineError> {
        let connector_context = background_connector_request_context().map_err(|error| {
            MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
        })?;
        let mut revisions: BTreeMap<SourceRevisionKey, SourceRevision> = BTreeMap::new();
        occurrences
            .iter()
            .map(|occurrence| {
                let table_ref = novarocks_types::naming::TableIdentity {
                    catalog: occurrence.catalog_at_binding.clone(),
                    namespace: occurrence.namespace_at_binding.clone(),
                    table: occurrence.relation_at_binding.clone(),
                };
                let key = (
                    table_ref.catalog.clone(),
                    table_ref.namespace.clone(),
                    table_ref.table.clone(),
                    occurrence.object_id.as_bytes().to_vec(),
                );
                let (object_id, native_data_version) = if let Some(revision) = revisions.get(&key) {
                    revision.clone()
                } else {
                    let (_, revision) =
                        crate::mv::domain::refresh_io::observe_current_refresh_revision_with_ports(
                            self.connector_control.as_ref(),
                            self.storage_observation.as_ref(),
                            &table_ref,
                            &connector_context,
                        )
                        .map_err(|error| {
                            MvBackgroundEngineError::new(
                                MvBackgroundEngineErrorKind::TransientUnavailable,
                                error,
                            )
                        })?;
                    let revision =
                        persist_exact_connector_revision(&revision).map_err(|error| {
                            MvBackgroundEngineError::new(
                                MvBackgroundEngineErrorKind::InvalidDefinition,
                                error.to_string(),
                            )
                        })?;
                    revisions.insert(key, revision.clone());
                    revision
                };
                if object_id != occurrence.object_id {
                    return Err(MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        format!(
                            "MV source occurrence {} belongs to a replaced object",
                            occurrence.occurrence_id
                        ),
                    ));
                }
                Ok(PublicationInput {
                    relation_occurrence_id: occurrence.occurrence_id,
                    object_id,
                    native_data_version,
                })
            })
            .collect()
    }

    async fn maintenance_facts(
        &self,
        pending: &mut Option<crate::mv::domain::maintenance::stats::MaintenanceFactsState>,
        target: &MaintenanceTarget,
    ) -> Result<MvMaintenanceFacts, MvBackgroundEngineError> {
        if let Some(original) = pending.as_ref() {
            if !original.matches(target) {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::InvariantViolation,
                    "maintenance facts wait changes its original target",
                ));
            }
        } else {
            let projections = self
                .readiness
                .list_ready_projections_async()
                .await
                .map_err(repository_error)?
                .into_iter()
                .map(|loaded| loaded.projection)
                .collect();
            *pending = Some(
                crate::mv::domain::maintenance::stats::MaintenanceFactsState::try_new(
                    Arc::clone(&self.connector_control),
                    Arc::clone(&self.storage_observation),
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                    projections,
                )
                .map_err(|error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::TransientUnavailable,
                        error,
                    )
                })?,
            );
        }
        let stats = pending
            .as_mut()
            .expect("original facts installed before poll")
            .wait()
            .await
            .map_err(|error| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    error,
                )
            })?;
        Ok(maintenance_facts_from_stats(stats))
    }
}

pub(crate) fn maintenance_facts_from_stats(
    stats: crate::mv::domain::maintenance::stats::TableMaintenanceStats,
) -> MvMaintenanceFacts {
    MvMaintenanceFacts {
        current_snapshot_id: stats.current_snapshot_id,
        total_data_files: stats.total_data_files.map(|value| value as i64),
        max_compactable_data_files: stats.max_compactable_data_files.map(|value| value as i64),
        total_delete_files: stats.total_delete_files.map(|value| value as i64),
        total_files_size_bytes: stats.total_files_size_bytes.map(|value| value as i64),
        oldest_snapshot_timestamp_ms: stats
            .snapshots
            .iter()
            .map(|snapshot| snapshot.timestamp_ms)
            .min(),
        snapshot_count: stats.snapshots.len(),
        non_default_reference_count: stats.non_default_reference_count,
        downstream_floor_ts_ms: stats.downstream_floor_ts_ms,
        downstream_floor_unknown: stats.downstream_floor_unknown,
        maintenance_enabled: stats.maintenance_enabled,
        expire_max_snapshot_age_ms: stats.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: stats.expire_min_snapshots_to_keep,
        target_file_size_bytes: stats.target_file_size_bytes,
    }
}

fn preparation_error(error: RefreshError) -> MvBackgroundEngineError {
    let kind = match error.kind {
        RefreshErrorKind::PreCommitFailed => MvBackgroundEngineErrorKind::TransientUnavailable,
        RefreshErrorKind::UserError => MvBackgroundEngineErrorKind::InvalidDefinition,
    };
    MvBackgroundEngineError::new(kind, error.message.clone())
        .with_compile_control(error.compile_control_error())
}

fn repository_error(
    error: novarocks_mv_application::repository::MvRepositoryError,
) -> MvBackgroundEngineError {
    use novarocks_mv_application::repository::MvRepositoryErrorKind;

    let kind = match error.kind() {
        MvRepositoryErrorKind::NotFound => MvBackgroundEngineErrorKind::TargetGone,
        MvRepositoryErrorKind::Unavailable => MvBackgroundEngineErrorKind::TransientUnavailable,
        MvRepositoryErrorKind::Corruption => MvBackgroundEngineErrorKind::Corruption,
        MvRepositoryErrorKind::CommitUnknown => MvBackgroundEngineErrorKind::TerminalFailure,
        MvRepositoryErrorKind::InvalidRequest | MvRepositoryErrorKind::Conflict => {
            MvBackgroundEngineErrorKind::InvariantViolation
        }
    };
    MvBackgroundEngineError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::preparation_error;
    use crate::mv::domain::lifecycle::RefreshError;
    use novarocks_mv_application::maintenance::MvBackgroundEngineErrorKind;

    #[test]
    fn retryable_preparation_error_preserves_its_typed_disposition() {
        let error = preparation_error(RefreshError::pre_commit("connector is starting"));
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
    }

    #[test]
    fn definition_preparation_error_remains_blocked() {
        let error = preparation_error(RefreshError::user("stored MV contract is incompatible"));
        assert_eq!(error.kind(), MvBackgroundEngineErrorKind::InvalidDefinition);
    }
    #[test]
    fn background_preparation_terminal_retains_control_without_changing_policy_kind() {
        use novarocks_type_contract::CompileControlError;
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = preparation_error(RefreshError::from(
                novarocks_sql::compiler::SqlCompileError::from(control.clone()),
            ));
            assert_eq!(error.compile_control_error(), Some(control));
            assert_eq!(error.kind(), MvBackgroundEngineErrorKind::InvalidDefinition);
        }
    }
}

/// Same occurrence/revision author as the legacy method, with actual I/O in
/// one management pass and the selected Provider's original CPU capture.
pub(crate) async fn current_source_inputs_in(
    ports: &IcebergMvCorePorts,
    occurrences: &[RelationOccurrence],
    context: &ConnectorRequestContext,
    factory: &crate::mv::domain::staged_create::CurrentProjectionMetadataCpu,
    capture: &Arc<crate::mv::domain::staged_create::CurrentMetadataCapture>,
) -> Result<Vec<PublicationInput>, MvBackgroundEngineError> {
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector,
        ConnectorTableIdentity, ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
        ConnectorTableResolution,
    };
    let provider_error = |error: novarocks_spi::connector::ConnectorError| {
        let cause = crate::mv::domain::readiness::connector_failure_cause(error);
        MvBackgroundEngineError::new(
            MvBackgroundEngineErrorKind::TransientUnavailable,
            cause.detail(),
        )
        .with_original_failure(cause)
    };
    let mut revisions: BTreeMap<SourceRevisionKey, SourceRevision> = BTreeMap::new();
    let mut inputs = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(&occurrence.catalog_at_binding).map_err(
                |error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        error.to_string(),
                    )
                },
            )?,
            namespace: occurrence.namespace_at_binding.clone().into(),
            table: occurrence.relation_at_binding.clone().into(),
        };
        let key = (
            occurrence.catalog_at_binding.clone(),
            occurrence.namespace_at_binding.clone(),
            occurrence.relation_at_binding.clone(),
            occurrence.object_id.as_bytes().to_vec(),
        );
        let (object_id, native_data_version) = if let Some(revision) = revisions.get(&key) {
            revision.clone()
        } else {
            let lease = ConnectorControlResolver::acquire_current(
                ports.connector_control(),
                &table.instance_id,
            )
            .map_err(provider_error)?;
            let binding = capture
                .capture(
                    &lease,
                    factory,
                    ConnectorTableObjectCaptureRequest {
                        table: table.clone(),
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        selector: ConnectorTableObjectSelector::Current,
                        context: context.clone(),
                    },
                )
                .await
                .map_err(|failure| {
                    let error = failure.into_application();
                    let mut projected = MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::TransientUnavailable,
                        error.message(),
                    );
                    if let Some(cause) = error.original_failure() {
                        projected = projected.with_original_failure(cause.clone());
                    }
                    projected
                })?;
            let metadata = &binding.metadata;
            let observed = crate::mv::domain::storage_observation::observe_refresh_base(
                ports.storage_observation(),
                &lease,
                metadata,
                context.clone(),
            )
            .map_err(provider_error)?;
            if observed.table() != &metadata.identity {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    "MV scheduler source observation identity differs from its captured metadata",
                ));
            }
            let revision = lease
                .binding()
                .metadata()
                .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
                .map_err(provider_error)?;
            if let Some(snapshot) = observed.current_snapshot_id() {
                if lease
                    .binding()
                    .metadata()
                    .read_selector_from_exact_revision(&metadata.table, &revision)
                    .map_err(provider_error)?
                    != ConnectorReadSelector::SnapshotId(snapshot)
                {
                    return Err(MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::TransientUnavailable,
                        "MV scheduler source observation and exact revision disagree",
                    ));
                }
            }
            let revision = persist_exact_connector_revision(&revision).map_err(|error| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::InvalidDefinition,
                    error.to_string(),
                )
            })?;
            revisions.insert(key, revision.clone());
            revision
        };
        if object_id != occurrence.object_id {
            return Err(MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvalidDefinition,
                format!(
                    "MV source occurrence {} belongs to a replaced object",
                    occurrence.occurrence_id
                ),
            ));
        }
        inputs.push(PublicationInput {
            relation_occurrence_id: occurrence.occurrence_id,
            object_id,
            native_data_version,
        });
    }
    Ok(inputs)
}
