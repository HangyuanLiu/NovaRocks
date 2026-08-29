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

//! Provider-neutral activation for the current frontend-owned MV refresh route.
//!
//! The adapter translates application publication facts into the generic
//! managed-publication intent. The exact connector generation owns physical
//! writer registration, provenance encoding and commit/reconcile machinery.

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorManagedPublicationEmptyInputDisposition,
    ConnectorManagedPublicationIntent, ConnectorManagedPublicationTarget,
    ConnectorManagedPublicationTechnique, ConnectorRequestContext,
    ConnectorStagedPublicationBaseFact, ConnectorTableIdentity, ConnectorTableResolution,
    ConnectorWriteActivationIntent, ConnectorWriteInputRequest, ConnectorWriteLease,
    ConnectorWriteOperationId,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::storage_observation::{MvLakePublishedProjection, observe_lake_package};
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvRefreshCommittedFacts, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
    MvStagedRefreshWriteMode, PreparedMvFirstRefreshWrite,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    PreparedMvRefreshWrite, PreparedMvRefreshWriteArtifact,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, PreparedMvNativeWriteAssembly,
};

/// Core-side provider adapter installed into the frontend composition.
///
/// It owns only the query-preparation kernel and MV leaf ports required to
/// bind an already admitted write. It cannot recover a state aggregate or
/// create a hidden all-in-one activation path.
pub struct IcebergMvRefreshProviderActivation {
    query_kernel: QueryPreparationKernel,
    ports: IcebergMvCorePorts,
}

impl IcebergMvRefreshProviderActivation {
    pub fn new(query_kernel: QueryPreparationKernel, ports: IcebergMvCorePorts) -> Self {
        Self {
            query_kernel,
            ports,
        }
    }
}

impl MvRefreshProviderActivation for IcebergMvRefreshProviderActivation {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, String> {
        match prepared.into_assembly_artifact() {
            PreparedMvRefreshWriteArtifact::FirstRefresh(prepared) => {
                super::first_refresh_staging::bind_prepared_mv_first_refresh_staging(
                    &self.query_kernel,
                    &self.ports,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
            PreparedMvRefreshWriteArtifact::Incremental(prepared) => {
                super::incremental_staging::bind_prepared_mv_incremental_staging(
                    &self.query_kernel,
                    &self.ports,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
        }
    }

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String> {
        MvRefreshCommittedFacts::from_write_receipt(intent, receipt)
    }

    fn observe_published_package(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        connector_context: &ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvLakePackageObservation, String> {
        if planning_lease.binding().descriptor().instance_id != table.instance_id {
            return Err(
                "MV publication observation table belongs to a different connector generation"
                    .to_string(),
            );
        }
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
            planning_lease,
            connector_context.clone(),
            table.namespace.as_ref(),
            table.table.as_ref(),
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(|error| format!("reload MV publication target metadata: {error}"))?;
        if metadata.identity != *table {
            return Err(
                "MV publication observation loaded metadata for a different target table"
                    .to_string(),
            );
        }
        let package = self
            .ports
            .storage_observation()
            .observe_lake_package(planning_lease, &metadata, connector_context.clone())
            .map_err(|error| format!("observe MV publication lake package: {error}"))?
            .ok_or_else(|| "MV publication target has no lake package observation".to_string())?;
        let local = crate::mv::domain::storage_observation::lake_package_from_spi(package.clone())
            .map_err(|error| format!("validate MV publication lake package: {error}"))?;
        if local.table != *table {
            return Err(
                "MV publication observer returned a package for a different target table"
                    .to_string(),
            );
        }
        let projection = local
            .published_projection()
            .map_err(|error| format!("project MV publication lake package: {error}"))?;
        require_exact_published_projection(projection, expected_snapshot_id)?;
        Ok(package)
    }
}

fn require_exact_published_projection(
    projection: MvLakePublishedProjection,
    expected_snapshot_id: i64,
) -> Result<MvLakePublishedProjection, String> {
    match &projection {
        MvLakePublishedProjection::Published {
            last_refreshed_iceberg_snapshot_id,
            ..
        } if *last_refreshed_iceberg_snapshot_id == expected_snapshot_id => Ok(projection),
        MvLakePublishedProjection::Published {
            last_refreshed_iceberg_snapshot_id,
            ..
        } => Err(format!(
            "MV publication lake snapshot {last_refreshed_iceberg_snapshot_id} does not match committed snapshot {expected_snapshot_id}"
        )),
        MvLakePublishedProjection::NeverPublished => {
            Err("MV publication committed but its lake package is never-published".to_string())
        }
    }
}

/// Activate a managed MV write from the exact provider-signed preparation.
/// No application caller reloads a catalog, constructs a physical collector,
/// encodes provenance, or registers a provider write service.
pub(crate) fn activate_first_refresh_connector_write(
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    if !exact_lease.matches_provider_binding_key(prepared.observed_binding()) {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    if !exact_lease.matches_provider_instance(prepared.target_table().owner()) {
        return Err(
            "MV first-refresh staging table belongs to a different connector instance".to_string(),
        );
    }
    let operation_id: ConnectorWriteOperationId = prepared.operation_id();
    let target = crate::catalog_application::resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    let intent = match prepared.write_mode() {
        MvStagedRefreshWriteMode::Append => novarocks_spi::connector::ConnectorWriteIntent::Append,
        MvStagedRefreshWriteMode::FullOverwrite => {
            novarocks_spi::connector::ConnectorWriteIntent::Overwrite
        }
    };
    let empty_input = match prepared.write_mode() {
        MvStagedRefreshWriteMode::Append => {
            ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit
        }
        MvStagedRefreshWriteMode::FullOverwrite => {
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
        }
    };
    let replacement = prepared
        .publication_intent()
        .partition_spec_replacement()
        .is_some();
    let target_ref = if replacement {
        "main"
    } else {
        prepared.staging_branch()
    };
    let input = ConnectorWriteInputRequest::Data {
        fields: prepared
            .write_input_fields()
            .iter()
            .map(|field| novarocks_spi::connector::ConnectorWriteFieldRequest::new(field.clone()))
            .collect(),
    };
    let purpose = novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh;
    let preparation = if replacement {
        crate::query_execution::dml::iceberg_writer::prepare_iceberg_connector_write_with_table(
            exact_lease,
            prepared.target_table().clone(),
            target_ref,
            intent,
            input,
            purpose,
            connector_context.clone(),
        )?
    } else {
        crate::query_execution::dml::iceberg_writer::prepare_iceberg_connector_write(
            exact_lease,
            &target,
            target_ref,
            intent,
            input,
            purpose,
            connector_context.clone(),
        )?
    };
    let managed_publication =
        managed_publication_activation_intent(prepared.publication_intent(), empty_input)?;
    crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared_with_intent(
        operation_id,
        preparation,
        ConnectorWriteActivationIntent::ManagedPublication(managed_publication),
        connector_context,
        exact_lease.clone(),
    )
    .map_err(|error| format!("activate exact Iceberg MV write generation: {error}"))
}

pub(crate) fn managed_publication_activation_intent(
    publication: &MvRefreshPublicationIntent,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
) -> Result<ConnectorManagedPublicationIntent, String> {
    let arguments = (
        publication.publication_id(),
        ConnectorManagedPublicationTarget::try_new(
            publication.target_object_id().clone(),
            publication.expected_target_snapshot_id(),
        )
        .map_err(|error| format!("build managed MV publication target: {error}"))?,
        match publication.technique() {
            MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
            MvRefreshPublicationTechnique::Incremental => {
                ConnectorManagedPublicationTechnique::Incremental
            }
            MvRefreshPublicationTechnique::MetadataOnly => {
                return Err(
                    "metadata-only MV refresh must use the catalog staging operation".to_string(),
                );
            }
        },
        publication
            .bases()
            .iter()
            .map(|base| ConnectorStagedPublicationBaseFact {
                table: base.table_fqn().into(),
                object_id: base.table_object_id().clone(),
                from_version: base.from_snapshot(),
                to_version: base.to_snapshot(),
            })
            .collect(),
        publication.definition_fingerprint(),
        empty_input,
        publication.descriptor_properties().clone(),
    );
    match publication.partition_spec_replacement() {
        Some(replacement) => {
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                replacement.clone(),
                publication
                    .expected_committed_partitioning()
                    .cloned()
                    .ok_or_else(|| {
                        "managed MV partition replacement is missing its exact preview partitioning"
                            .to_string()
                    })?,
                arguments.6,
            )
        }
        None => ConnectorManagedPublicationIntent::try_new(
            arguments.0,
            arguments.1,
            arguments.2,
            arguments.3,
            arguments.4,
            arguments.5,
            arguments.6,
        ),
    }
    .map_err(|error| format!("build managed MV publication activation intent: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{MvLakePublishedProjection, require_exact_published_projection};

    fn published(snapshot_id: i64) -> MvLakePublishedProjection {
        MvLakePublishedProjection::Published {
            last_refresh_ms: 1_700_000_010_000,
            last_refresh_rows: 7,
            last_refreshed_iceberg_snapshot_id: snapshot_id,
            base_snapshots: BTreeMap::new(),
            base_table_object_ids: BTreeMap::new(),
        }
    }

    #[test]
    fn exact_published_projection_retains_the_lake_timestamp() {
        assert_eq!(
            require_exact_published_projection(published(99), 99)
                .expect("exact snapshot is accepted"),
            published(99)
        );
    }

    #[test]
    fn advanced_published_projection_fails_closed() {
        let error = require_exact_published_projection(published(100), 99)
            .expect_err("advanced lake head must not finalize an older publication");

        assert!(error.contains("does not match committed snapshot 99"));
    }
}
