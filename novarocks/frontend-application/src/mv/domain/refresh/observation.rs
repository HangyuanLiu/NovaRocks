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

//! Refresh schema observations from retained provider metadata and canonical D/L.

use crate::mv::domain::refresh::schema_contract::{
    MvOccurrenceFieldRebind, validate_projection_target, validate_relation_occurrence_schema,
};
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation,
};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorRequestContext, ConnectorTableResolution,
    MvStorageObservationPort,
};
use novarocks_types::naming::TableIdentity;

/// No second Current capture is allowed: object, metadata version, schema and
/// fields all come from the provider's same retained handle.
pub(crate) fn observe_schema_validation_for_table(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    let lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observed = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(|error| format!("observe exact MV schema for {}: {error}", table.fqn()))?;
    validate_observed_schema_locator(&observed, &metadata.identity)?;
    Ok(observed)
}

/// The load awaits the actual Provider; the schema inspector is pure over
/// that exact retained metadata generation.
pub(crate) async fn observe_schema_validation_for_table_async(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, crate::mv::domain::application::MvApplicationError> {
    let lease = crate::connector::metadata_binding_typed(connector_control, &table.catalog)
        .map_err(schema_provider_error)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease_async(
        &lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )
    .await
    .map_err(schema_provider_error)?;
    let observed = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(schema_provider_error)?;
    validate_observed_schema_locator(&observed, &metadata.identity)
        .map_err(schema_contract_error)?;
    Ok(observed)
}

pub(crate) fn validate_observed_schema_locator(
    observed: &MvSchemaValidationObservation,
    identity: &novarocks_spi::connector::ConnectorTableIdentity,
) -> Result<(), String> {
    if observed.table() != identity {
        return Err(
            "MV schema observation does not match the retained metadata locator".to_string(),
        );
    }
    Ok(())
}

fn schema_provider_error(
    error: novarocks_spi::connector::ConnectorError,
) -> crate::mv::domain::application::MvApplicationError {
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    let kind = if error.kind() == novarocks_spi::connector::ConnectorErrorKind::Unavailable {
        MvApplicationErrorKind::Unavailable
    } else {
        MvApplicationErrorKind::Engine
    };
    MvApplicationError::from_connector(kind, error)
}

fn schema_contract_error(message: String) -> crate::mv::domain::application::MvApplicationError {
    crate::mv::domain::application::MvApplicationError::new(
        crate::mv::domain::application::MvApplicationErrorKind::InvalidRequest,
        message,
    )
}

pub(crate) fn observe_current_refresh_base(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvRefreshBaseObservation, String> {
    crate::mv::domain::refresh_io::observe_current_refresh_base_with_ports(
        connector_control,
        storage_observation,
        table,
        connector_context,
    )
}

/// Collects exact target runtime bindings and every occurrence-local rename.
/// The caller owns SQL rewriting; duplicate physical inputs are never coalesced.
pub(crate) fn observe_refresh_schema(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<(MvRuntimeBindings, Vec<MvOccurrenceFieldRebind>), String> {
    let loaded_target;
    let target_observation = match retained_target_observation {
        Some(observation) => observation,
        None => {
            loaded_target = observe_schema_validation_for_table(
                connector_control,
                storage_observation,
                &TableIdentity {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                connector_context,
            )?;
            &loaded_target
        }
    };
    let bindings = validate_projection_target(projection, target_observation)?;
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != base_refs.len() {
        return Err("MV refresh source references do not retain every D occurrence".to_string());
    }
    let mut renames = Vec::new();
    for (occurrence, table) in occurrences.iter().zip(base_refs) {
        validate_occurrence_locator(occurrence, table)?;
        let observed = observe_schema_validation_for_table(
            connector_control,
            storage_observation,
            table,
            connector_context,
        )?;
        renames.extend(validate_relation_occurrence_schema(occurrence, &observed)?);
    }
    Ok((bindings, renames))
}

pub(crate) async fn observe_refresh_schema_async(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<
    (MvRuntimeBindings, Vec<MvOccurrenceFieldRebind>),
    crate::mv::domain::application::MvApplicationError,
> {
    let loaded_target;
    let target_observation = match retained_target_observation {
        Some(observation) => observation,
        None => {
            loaded_target = observe_schema_validation_for_table_async(
                connector_control,
                storage_observation,
                &TableIdentity {
                    catalog: target.catalog.clone(),
                    namespace: target.namespace.clone(),
                    table: target.table.clone(),
                },
                connector_context,
            )
            .await?;
            &loaded_target
        }
    };
    let bindings = validate_projection_target(projection, target_observation)
        .map_err(schema_contract_error)?;
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != base_refs.len() {
        return Err(schema_contract_error(
            "MV refresh source references do not retain every D occurrence".to_string(),
        ));
    }
    let mut renames = Vec::new();
    for (occurrence, table) in occurrences.iter().zip(base_refs) {
        validate_occurrence_locator(occurrence, table).map_err(schema_contract_error)?;
        let observed = observe_schema_validation_for_table_async(
            connector_control,
            storage_observation,
            table,
            connector_context,
        )
        .await?;
        renames.extend(
            validate_relation_occurrence_schema(occurrence, &observed)
                .map_err(schema_contract_error)?,
        );
    }
    Ok((bindings, renames))
}

pub(crate) fn validate_occurrence_locator(
    occurrence: &novarocks_mv_application::persistence::codec::RelationOccurrence,
    table: &TableIdentity,
) -> Result<(), String> {
    if occurrence.catalog_at_binding != table.catalog
        || occurrence.namespace_at_binding != table.namespace
        || occurrence.relation_at_binding != table.table
    {
        return Err(format!(
            "MV refresh source locator does not match occurrence {}",
            occurrence.occurrence_id
        ));
    }
    Ok(())
}

pub(crate) async fn rebind_mv_definition_before_refresh_derivation_async(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<(StoredKeyedMvProjection, String), crate::mv::domain::application::MvApplicationError> {
    let (_, renames) = observe_refresh_schema_async(
        connector_control,
        storage_observation,
        projection,
        base_refs,
        target,
        retained_target_observation,
        connector_context,
    )
    .await?;
    require_occurrence_rewriter(&renames).map_err(schema_contract_error)?;
    Ok((
        projection.clone(),
        projection.facts.definition().query.effective_sql.clone(),
    ))
}

/// Until the SQL occurrence-aware rewriter is connected, renamed fields fail
/// closed. The canonical document is never changed to pretend a rebind occurred.
pub(crate) fn rebind_mv_definition_before_refresh_derivation(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &ConnectorRequestContext,
) -> Result<(StoredKeyedMvProjection, String), String> {
    let (_, renames) = observe_refresh_schema(
        connector_control,
        storage_observation,
        projection,
        base_refs,
        target,
        retained_target_observation,
        connector_context,
    )?;
    require_occurrence_rewriter(&renames)?;
    Ok((
        projection.clone(),
        projection.facts.definition().query.effective_sql.clone(),
    ))
}

pub(crate) fn require_occurrence_rewriter(
    renames: &[MvOccurrenceFieldRebind],
) -> Result<(), String> {
    if renames.is_empty() {
        Ok(())
    } else {
        Err("MV refresh requires occurrence-aware SQL field rebinding".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::identity::FieldIdentity;

    #[test]
    fn renamed_fields_cannot_fall_back_to_the_stored_sql() {
        let rename = MvOccurrenceFieldRebind {
            occurrence_id: 7,
            field_id: FieldIdentity::try_new(vec![0xff, 1]).unwrap(),
            qualifier_at_binding: "left_input".to_string(),
            name_at_binding: "old_name".to_string(),
            current_name: "new_name".to_string(),
        };
        assert!(require_occurrence_rewriter(&[]).is_ok());
        assert_eq!(
            require_occurrence_rewriter(&[rename]).unwrap_err(),
            "MV refresh requires occurrence-aware SQL field rebinding"
        );
    }
}
