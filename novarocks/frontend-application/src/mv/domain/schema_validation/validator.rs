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

//! Canonical D/L schema validation against one exact provider observation.
//!
//! Opaque provider identities are compared byte-for-byte by their domain
//! types. This module never decodes them into connector-specific numeric IDs
//! and never reconstructs the retired `MvSchemaContract`.

use crate::mv::domain::refresh::schema_contract::{
    MvOccurrenceFieldRebind, validate_projection_target, validate_relation_occurrence_schema,
};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_mv_application::persistence::codec::RelationOccurrence;
use novarocks_mv_application::persistence::projection::StoredMvProjection;

/// Validate one D source occurrence and the exact target generation.
pub(crate) fn validate_schema_contract(
    projection: &StoredMvProjection,
    occurrence: &RelationOccurrence,
    current_base: &MvSchemaValidationObservation,
    current_target: &MvSchemaValidationObservation,
) -> Result<Vec<MvOccurrenceFieldRebind>, String> {
    require_projection_occurrence(projection, occurrence)?;
    require_row_lineage("base", current_base)?;
    require_row_lineage("target", current_target)?;
    validate_projection_target(projection, current_target)?;
    validate_relation_occurrence_schema(occurrence, current_base)
}

/// Validate every ordered D occurrence used by a join or UNION definition.
/// Repeated physical objects remain separate occurrences.
pub(crate) fn validate_join_schema_contract(
    projection: &StoredMvProjection,
    bases: &[(&RelationOccurrence, &MvSchemaValidationObservation)],
    current_target: &MvSchemaValidationObservation,
) -> Result<Vec<MvOccurrenceFieldRebind>, String> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != bases.len() {
        return Err(format!(
            "MV source observation count {} does not match D occurrence count {}",
            bases.len(),
            occurrences.len()
        ));
    }
    require_row_lineage("target", current_target)?;
    validate_projection_target(projection, current_target)?;

    let mut renames = Vec::new();
    for (expected, (occurrence, observation)) in occurrences.iter().zip(bases) {
        if expected != *occurrence {
            return Err(format!(
                "MV source observations are not in D occurrence order at occurrence {}",
                expected.occurrence_id
            ));
        }
        require_row_lineage("base", observation)?;
        renames.extend(validate_relation_occurrence_schema(
            occurrence,
            observation,
        )?);
    }
    Ok(renames)
}

/// Validate that all branch bindings are materializable from the exact target
/// schema. `reconstruct_runtime_bindings`, reached through
/// `validate_projection_target`, checks the opaque field identity, type and
/// nullability of each branch field.
pub(crate) fn validate_branch_id_field(
    projection: &StoredMvProjection,
    current_target: &MvSchemaValidationObservation,
) -> Result<(), String> {
    if projection.facts.interpretation().branches.is_empty() {
        return Err("MV definition has no branch interpretation".to_string());
    }
    require_row_lineage("target", current_target)?;
    let bindings = validate_projection_target(projection, current_target)?;
    if bindings.branches.len() != projection.facts.interpretation().branches.len() {
        return Err("MV target schema does not retain every branch binding".to_string());
    }
    Ok(())
}

fn require_projection_occurrence(
    projection: &StoredMvProjection,
    occurrence: &RelationOccurrence,
) -> Result<(), String> {
    if projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .any(|stored| stored == occurrence)
    {
        Ok(())
    } else {
        Err(format!(
            "source occurrence {} is not part of the canonical MV definition",
            occurrence.occurrence_id
        ))
    }
}

fn require_row_lineage(
    role: &str,
    observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    if !observation.is_format_v3() {
        return Err(format!("MV {role} table is not Iceberg format v3"));
    }
    if !observation.stored_row_lineage_enabled() {
        return Err(format!(
            "MV {role} table does not retain stored row lineage"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::codec::PhysicalFieldLogicalIdentity;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorCommittedVersion, ConnectorInstanceId,
        ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectId,
        MvObservedSourceField, MvSchemaValidationObservation as SpiObservation,
    };
    use std::collections::BTreeSet;
    use std::sync::Arc;

    struct Active;

    impl ConnectorCancellation for Active {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            Arc::new(Active),
            4096,
            64 * 1024,
        )
        .unwrap()
    }

    fn projection() -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .unwrap(),
        }
    }

    fn convert(observation: SpiObservation) -> MvSchemaValidationObservation {
        let context = context();
        crate::mv::domain::storage_observation::schema_validation_from_spi(observation, &context)
            .unwrap()
    }

    fn source_observation(
        occurrence: &RelationOccurrence,
        rename_amount: bool,
    ) -> MvSchemaValidationObservation {
        let context = context();
        let fields = occurrence
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                (
                    ordinal as u32,
                    MvObservedSourceField::try_new(
                        Bytes::copy_from_slice(field.field_id.as_bytes()),
                        if rename_amount && field.name_at_binding == "amount" {
                            "amount_now".to_string()
                        } else {
                            field.name_at_binding.clone()
                        },
                        field.type_signature.clone(),
                        field.nullable,
                    )
                    .unwrap(),
                )
            })
            .collect();
        convert(
            SpiObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse(&occurrence.catalog_at_binding)
                        .unwrap(),
                    namespace: Arc::from(occurrence.namespace_at_binding.as_str()),
                    table: Arc::from(occurrence.relation_at_binding.as_str()),
                },
                novarocks_mv_application::persistence::exact_revision::restore_persisted_object(
                    &occurrence.object_id,
                )
                .unwrap(),
                ConnectorCommittedVersion::try_new(Bytes::from_static(b"source-metadata"), Some(9))
                    .unwrap(),
                Bytes::copy_from_slice(occurrence.schema_version.as_bytes()),
                Bytes::from_static(b"source-partition"),
                true,
                true,
                fields,
                &context,
            )
            .unwrap(),
        )
    }

    fn target_observation_with_metadata(
        projection: &StoredMvProjection,
        metadata_version: ConnectorCommittedVersion,
    ) -> MvSchemaValidationObservation {
        let context = context();
        let facts = &projection.facts;
        let mut seen = BTreeSet::new();
        let fields = facts
            .interpretation()
            .target
            .fields
            .iter()
            .filter(|field| seen.insert(field.target_field_id.clone()))
            .enumerate()
            .map(|(ordinal, field)| {
                let prefix = match &field.logical_identity {
                    PhysicalFieldLogicalIdentity::Output(_) => "output",
                    PhysicalFieldLogicalIdentity::State(_) => "state",
                    PhysicalFieldLogicalIdentity::ApplyKey(_) => "apply",
                    PhysicalFieldLogicalIdentity::Branch(_) => "branch",
                };
                (
                    ordinal as u32,
                    MvObservedSourceField::try_new(
                        Bytes::copy_from_slice(field.target_field_id.as_bytes()),
                        format!("{prefix}_{ordinal}"),
                        field.type_signature.clone(),
                        field.nullable,
                    )
                    .unwrap(),
                )
            })
            .collect();
        convert(
            SpiObservation::try_new(
                facts.source_revision().target.clone(),
                facts.source_revision().target_object_id.clone(),
                metadata_version,
                Bytes::copy_from_slice(facts.interpretation().target.schema_version.as_bytes()),
                Bytes::copy_from_slice(
                    facts
                        .interpretation()
                        .target
                        .partition_spec_version
                        .as_bytes(),
                ),
                true,
                true,
                fields,
                &context,
            )
            .unwrap(),
        )
    }

    fn target_observation(projection: &StoredMvProjection) -> MvSchemaValidationObservation {
        target_observation_with_metadata(projection, projection.facts.metadata_version().clone())
    }

    #[test]
    fn occurrence_validation_preserves_opaque_identity_and_reports_rename() {
        let projection = projection();
        let occurrence = &projection.facts.definition().relation_occurrences[0];
        let renames = validate_schema_contract(
            &projection,
            occurrence,
            &source_observation(occurrence, true),
            &target_observation(&projection),
        )
        .unwrap();
        assert_eq!(renames.len(), 1);
        assert_eq!(renames[0].field_id.as_bytes(), &[2]);
        assert_eq!(renames[0].current_name, "amount_now");
    }

    #[test]
    fn exact_target_generation_drift_is_rejected() {
        let projection = projection();
        let occurrence = &projection.facts.definition().relation_occurrences[0];
        let target = target_observation_with_metadata(
            &projection,
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"other-generation"), Some(12))
                .unwrap(),
        );
        let error = validate_schema_contract(
            &projection,
            occurrence,
            &source_observation(occurrence, false),
            &target,
        )
        .unwrap_err();
        assert!(error.contains("exact document generation"), "{error}");
    }
}
