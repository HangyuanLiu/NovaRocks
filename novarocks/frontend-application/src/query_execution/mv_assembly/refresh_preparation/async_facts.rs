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

use super::io_gate::{MvRefreshPreparationIo, provider_error};
use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation,
};
use novarocks_spi::connector::*;
use novarocks_types::naming::TableIdentity;

/// Pure/domain failures keep their original category. Provider and repository
/// failures retain the actual typed cause through the same preparation result.
#[derive(Debug)]
pub(crate) struct PreparationError(pub(crate) MvApplicationError);
impl PreparationError {
    pub(crate) fn with_context(self, context: impl std::fmt::Display) -> Self {
        let mut error =
            MvApplicationError::new(self.0.kind(), format!("{context}: {}", self.0.message()))
                .with_compile_control(self.0.compile_control_error())
                .with_unavailable_implementation(self.0.unavailable_implementation().cloned());
        if let Some(cause) = self.0.original_failure() {
            error = error.with_original_failure(cause.clone());
        }
        Self(error)
    }
    pub(crate) fn user(message: impl Into<String>) -> Self {
        Self(MvApplicationError::new(
            MvApplicationErrorKind::InvalidRequest,
            message,
        ))
    }
}
impl std::fmt::Display for PreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for PreparationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl From<String> for PreparationError {
    fn from(e: String) -> Self {
        Self::user(e)
    }
}
impl From<&str> for PreparationError {
    fn from(e: &str) -> Self {
        Self::user(e)
    }
}
impl From<MvApplicationError> for PreparationError {
    fn from(e: MvApplicationError) -> Self {
        Self(e)
    }
}
impl From<ConnectorError> for PreparationError {
    fn from(e: ConnectorError) -> Self {
        Self(provider_error(e))
    }
}
impl From<novarocks_sql::compiler::SqlCompileError> for PreparationError {
    fn from(e: novarocks_sql::compiler::SqlCompileError) -> Self {
        Self(MvApplicationError::from_compile(e))
    }
}
impl From<crate::mv::domain::lifecycle::RefreshError> for PreparationError {
    fn from(e: crate::mv::domain::lifecycle::RefreshError) -> Self {
        let result = MvApplicationError::new(
            match e.kind {
                crate::mv::domain::lifecycle::RefreshErrorKind::UserError => {
                    MvApplicationErrorKind::InvalidRequest
                }
            },
            e.message.clone(),
        )
        .with_compile_control(e.compile_control_error())
        .with_unavailable_implementation(e.unavailable_implementation().cloned());
        Self(result)
    }
}

impl MvRefreshPreparationIo {
    pub(crate) async fn table_metadata(
        &self,
        controls: &dyn ConnectorControlResolver,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<(ConnectorControlPlanningLease, ConnectorTableMetadata), ConnectorError> {
        let lease = crate::connector::metadata_binding_typed(controls, &table.catalog)?;
        let metadata = self
            .metadata(
                &lease,
                context.clone(),
                &table.namespace,
                &table.table,
                ConnectorTableResolution::StrictBaseTable,
            )
            .await?;
        Ok((lease, metadata))
    }
    pub(crate) async fn target_binding_with_lease(
        &self,
        storage: &dyn MvStorageObservationPort,
        table: &TableIdentity,
        lease: ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<MvTargetBinding, ConnectorError> {
        crate::mv::domain::refresh::target_binding::validate_target_lease(table, &lease)?;
        let metadata = self
            .metadata(
                &lease,
                context.clone(),
                &table.namespace,
                &table.table,
                ConnectorTableResolution::StrictBaseTable,
            )
            .await?;
        crate::mv::domain::refresh::target_binding::complete_target_binding(
            storage, lease, metadata, context,
        )
    }
    pub(crate) async fn target_binding(
        &self,
        source: &IcebergMvCorePorts,
        target: &IcebergMvTarget,
        context: &ConnectorRequestContext,
    ) -> Result<MvTargetBinding, ConnectorError> {
        let table = TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        };
        let lease =
            crate::connector::metadata_binding_typed(source.connector_control(), &table.catalog)?;
        self.target_binding_with_lease(source.storage_observation(), &table, lease, context)
            .await
    }
    pub(crate) async fn schema(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, PreparationError> {
        let (lease, metadata) = self
            .table_metadata(source.connector_control(), table, context)
            .await?;
        let observed = crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            &lease,
            &metadata,
            context.clone(),
        )?;
        crate::mv::domain::refresh::observation::validate_observed_schema_locator(
            &observed,
            &metadata.identity,
        )?;
        Ok(observed)
    }
    pub(crate) async fn current_base(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        let (lease, metadata) = self
            .table_metadata(source.connector_control(), table, context)
            .await?;
        crate::mv::domain::refresh_io::observe_loaded_refresh_base(
            source.storage_observation(),
            &lease,
            &metadata,
            table,
            context,
        )
    }
    pub(crate) async fn current_revision(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<(MvRefreshBaseObservation, ConnectorExactSemanticRevision), ConnectorError> {
        let (lease, metadata) = self
            .table_metadata(source.connector_control(), table, context)
            .await?;
        let observed = crate::mv::domain::refresh_io::observe_loaded_refresh_base(
            source.storage_observation(),
            &lease,
            &metadata,
            table,
            context,
        )?;
        let revision = lease
            .binding()
            .metadata()
            .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)?;
        if let Some(snapshot) = observed.current_snapshot_id() {
            let selector = lease
                .binding()
                .metadata()
                .read_selector_from_exact_revision(&metadata.table, &revision)?;
            if selector != ConnectorReadSelector::SnapshotId(snapshot) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!(
                        "MV scheduler source observation and exact revision disagree for {}",
                        table.fqn()
                    ),
                ));
            }
        }
        Ok((observed, revision))
    }
    pub(crate) async fn snapshot_from_revision(
        &self,
        controls: &dyn ConnectorControlResolver,
        table: &TableIdentity,
        revision: &ConnectorExactSemanticRevision,
        context: &ConnectorRequestContext,
    ) -> Result<i64, ConnectorError> {
        let (lease, metadata) = self.table_metadata(controls, table, context).await?;
        match lease
            .binding()
            .metadata()
            .read_selector_from_exact_revision(&metadata.table, revision)?
        {
            ConnectorReadSelector::SnapshotId(id) => Ok(id),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "provider returned a non-snapshot MV read selector for {}",
                    table.fqn()
                ),
            )),
        }
    }
    pub(crate) async fn rebind_definition(
        &self,
        source: &IcebergMvCorePorts,
        projection:&novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        refs: &[TableIdentity],
        target: &IcebergMvTarget,
        retained: Option<&MvSchemaValidationObservation>,
        context: &ConnectorRequestContext,
    ) -> Result<
        (
            novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
            String,
        ),
        PreparationError,
    > {
        use crate::mv::domain::refresh::{observation, schema_contract};
        let loaded;
        let target_observation = match retained {
            Some(v) => v,
            None => {
                loaded = self
                    .schema(
                        source,
                        &TableIdentity {
                            catalog: target.catalog.clone(),
                            namespace: target.namespace.clone(),
                            table: target.table.clone(),
                        },
                        context,
                    )
                    .await?;
                &loaded
            }
        };
        schema_contract::validate_projection_target(projection, target_observation)?;
        let occurrences = &projection.facts.definition().relation_occurrences;
        if occurrences.len() != refs.len() {
            return Err(PreparationError::user(
                "MV refresh source references do not retain every D occurrence",
            ));
        }
        let mut renames = Vec::new();
        for (occurrence, table) in occurrences.iter().zip(refs) {
            observation::validate_occurrence_locator(occurrence, table)?;
            let observed = self.schema(source, table, context).await?;
            renames.extend(schema_contract::validate_relation_occurrence_schema(
                occurrence, &observed,
            )?);
        }
        observation::require_occurrence_rewriter(&renames)?;
        Ok((
            projection.clone(),
            projection.facts.definition().query.effective_sql.clone(),
        ))
    }
    pub(crate) async fn capture_pin(
        &self,
        source: &IcebergMvCorePorts,
        projection:&novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        context: &ConnectorRequestContext,
    ) -> Result<crate::mv::domain::refresh::pin::RefreshSnapshotPin, PreparationError> {
        let mut occurrences =
            Vec::with_capacity(projection.facts.definition().relation_occurrences.len());
        for relation in &projection.facts.definition().relation_occurrences {
            let table = TableIdentity {
                catalog: relation.catalog_at_binding.clone(),
                namespace: relation.namespace_at_binding.clone(),
                table: relation.relation_at_binding.clone(),
            };
            let (observed, revision) = self.current_revision(source, &table, context).await?;
            occurrences.push(
                crate::mv::domain::refresh_pin_adapter::finish_pin_occurrence(
                    relation.occurrence_id,
                    &relation.object_id,
                    table,
                    observed,
                    revision,
                )?,
            );
        }
        crate::mv::domain::refresh::pin::RefreshSnapshotPin::try_from_occurrences(occurrences)
            .map_err(Into::into)
    }
}
