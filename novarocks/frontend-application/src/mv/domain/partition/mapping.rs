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

use crate::mv::domain::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_mv_application::persistence::codec::{ExpressionKind, TargetPartitionTransform};
use novarocks_mv_application::persistence::projection::StoredMvProjection;
use novarocks_mv_application::persistence::schema::{
    MvPartitionContract, MvPartitionTransformContract,
};
use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionTransform, ConnectorChangePartitionValue,
};

/// Derive one target key only when D's output lineage and L's exact partition
/// binding prove that each target field is the same source field and transform
/// as the provider's per-file impact. Any weaker relation requires a full scan.
pub(crate) fn map_connector_partition_to_mv_key(
    projection: &StoredMvProjection,
    source_occurrence_id: u32,
    source_observation: &MvSchemaValidationObservation,
    target_partition: &MvPartitionContract,
    connector_partition: &ConnectorChangePartition,
) -> Result<Option<MvPartitionKey>, String> {
    let schema = source_observation.exact_schema();
    let occurrence = projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .find(|occurrence| occurrence.occurrence_id == source_occurrence_id)
        .ok_or("connector partition impact names no D source occurrence")?;
    if occurrence.catalog_at_binding != source_observation.table().instance_id.as_str()
        || !novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            &occurrence.object_id,
            &schema.object_id,
        )
        .map_err(|error| error.to_string())?
    {
        return Err(
            "connector partition impact does not belong to an exact D source occurrence"
                .to_string(),
        );
    }
    if connector_partition.partition_spec_identity().as_ref()
        != schema.partition_spec_version.as_bytes()
    {
        return Err("connector partition impact uses a different source partition spec".into());
    }
    for field in connector_partition.fields() {
        if !schema
            .fields
            .iter()
            .any(|observed| observed.field_id.as_bytes() == field.source_field_identity().as_ref())
        {
            return Err(format!(
                "connector partition impact references source field {} outside the exact schema observation",
                field.source_column()
            ));
        }
    }
    let definition = projection.facts.definition();
    let interpretation = projection.facts.interpretation();
    let canonical = &interpretation.target.partition_fields;
    if canonical.len() != target_partition.fields.len() || canonical.is_empty() {
        return Err("target partition observation disagrees with canonical L".into());
    }
    let mut fields = Vec::with_capacity(canonical.len());
    for (binding, observed) in canonical.iter().zip(&target_partition.fields) {
        if !target_transform_matches(&binding.transform, &observed.transform) {
            return Err("target partition transform disagrees with canonical L".into());
        }
        let output_binding = interpretation
            .outputs
            .iter()
            .find(|output| output.target_field_id == binding.source_target_field_id)
            .ok_or("target partition source has no output binding in canonical L")?;
        let output = definition
            .outputs
            .iter()
            .find(|output| output.output_id == output_binding.output_id)
            .ok_or("target partition source has no output in canonical D")?;
        if output.expression.kind != ExpressionKind::Field
            || output.expression.source_fields.len() != 1
            || output.expression.source_fields[0].occurrence_id != source_occurrence_id
        {
            return Err("target partition output is not one direct source field".into());
        }
        let source_id = &output.expression.source_fields[0].field_id;
        let source_field = schema
            .fields
            .iter()
            .find(|field| field.field_id == *source_id)
            .ok_or("direct target partition source is absent from the exact observation")?;
        if source_field.type_signature != output.type_signature
            || output_binding.type_signature != output.type_signature
        {
            return Err("direct target partition source changes its value type".into());
        }
        let impact = connector_partition
            .fields()
            .iter()
            .find(|field| {
                field.source_field_identity().as_ref() == source_id.as_bytes()
                    && impact_transform_matches(&binding.transform, field.transform())
            })
            .ok_or("source partition impact cannot derive the target transform")?;
        fields.push(MvPartitionKeyField::new(
            binding.partition_field_id.clone(),
            match impact.value() {
                ConnectorChangePartitionValue::Null => MvPartitionValue::Null,
                ConnectorChangePartitionValue::String(value) => {
                    MvPartitionValue::String(value.to_string())
                }
            },
        ));
    }
    Ok(Some(MvPartitionKey::new(
        interpretation.target.partition_spec_version.clone(),
        fields,
    )))
}

fn target_transform_matches(
    binding: &TargetPartitionTransform,
    observed: &MvPartitionTransformContract,
) -> bool {
    match (binding, observed) {
        (TargetPartitionTransform::Identity, MvPartitionTransformContract::Identity)
        | (TargetPartitionTransform::Year, MvPartitionTransformContract::Year)
        | (TargetPartitionTransform::Month, MvPartitionTransformContract::Month)
        | (TargetPartitionTransform::Day, MvPartitionTransformContract::Day)
        | (TargetPartitionTransform::Hour, MvPartitionTransformContract::Hour)
        | (TargetPartitionTransform::Void, MvPartitionTransformContract::Void) => true,
        (
            TargetPartitionTransform::Bucket { num_buckets: left },
            MvPartitionTransformContract::Bucket { num_buckets: right },
        ) => left == right,
        (
            TargetPartitionTransform::Truncate { width: left },
            MvPartitionTransformContract::Truncate { width: right },
        ) => left == right,
        _ => false,
    }
}

fn impact_transform_matches(
    binding: &TargetPartitionTransform,
    impact: ConnectorChangePartitionTransform,
) -> bool {
    match (binding, impact) {
        (TargetPartitionTransform::Identity, ConnectorChangePartitionTransform::Identity)
        | (TargetPartitionTransform::Year, ConnectorChangePartitionTransform::Year)
        | (TargetPartitionTransform::Month, ConnectorChangePartitionTransform::Month)
        | (TargetPartitionTransform::Day, ConnectorChangePartitionTransform::Day)
        | (TargetPartitionTransform::Hour, ConnectorChangePartitionTransform::Hour) => true,
        (
            TargetPartitionTransform::Bucket { num_buckets },
            ConnectorChangePartitionTransform::Bucket { buckets },
        ) => *num_buckets == buckets.get(),
        (
            TargetPartitionTransform::Truncate { width },
            ConnectorChangePartitionTransform::Truncate {
                width: impact_width,
            },
        ) => *width == impact_width.get(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::codec::{
        ApplyKeyKind, ExpressionShape, PhysicalFieldLogicalIdentity, SourceFieldReference,
        TargetPartitionFieldBinding,
    };
    use novarocks_mv_application::persistence::identity::FieldIdentity;
    use novarocks_mv_application::persistence::projection::StoredMvProjection;
    use novarocks_mv_application::persistence::schema::MvPartitionFieldContract;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorChangePartition, ConnectorChangePartitionField,
        ConnectorChangePartitionTransform, ConnectorChangePartitionValue,
        ConnectorCommittedVersion, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableObjectId, MvObservedSourceField,
        MvSchemaValidationObservation as SpiObservation,
    };
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
            1024,
            16 * 1024,
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

    fn observation(object: &'static [u8]) -> MvSchemaValidationObservation {
        let context = context();
        crate::mv::domain::storage_observation::schema_validation_from_spi(
            SpiObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                    namespace: Arc::from("sales"),
                    table: Arc::from("orders"),
                },
                ConnectorTableObjectId::try_new(Bytes::from_static(object)).unwrap(),
                ConnectorCommittedVersion::try_new(
                    Bytes::from_static(b"source-metadata"),
                    Some(11),
                )
                .unwrap(),
                Bytes::from_static(&[1]),
                Bytes::from_static(&[4]),
                true,
                true,
                vec![(
                    0,
                    MvObservedSourceField::try_new(
                        Bytes::from_static(&[1]),
                        "order_id".to_string(),
                        "bigint".to_string(),
                        false,
                    )
                    .unwrap(),
                )],
                vec![],
                &context,
            )
            .unwrap(),
            &context,
        )
        .unwrap()
    }

    fn partition() -> ConnectorChangePartition {
        ConnectorChangePartition::try_new(
            Bytes::from_static(&[4]),
            vec![
                ConnectorChangePartitionField::try_new(
                    Bytes::from_static(&[1]),
                    "order_id",
                    ConnectorChangePartitionTransform::Identity,
                    ConnectorChangePartitionValue::String("42".into()),
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    fn partitioned_projection() -> StoredMvProjection {
        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), None);
        fixture.definition.relation_occurrences.truncate(1);
        fixture.definition.outputs[0].name = "order_id".into();
        fixture.definition.outputs[0].type_signature = "bigint".into();
        fixture.definition.outputs[0].nullable = false;
        fixture.definition.outputs[0].expression = ExpressionShape {
            kind: ExpressionKind::Field,
            function_identity: None,
            source_fields: vec![SourceFieldReference {
                occurrence_id: 7,
                field_id: FieldIdentity::try_new(vec![1]).unwrap(),
            }],
        };
        fixture.interpretation.outputs[0].type_signature = "bigint".into();
        fixture.interpretation.outputs[0].nullable = false;
        fixture.interpretation.aggregates.clear();
        fixture.interpretation.state_slots.clear();
        fixture.interpretation.branches.clear();
        fixture.interpretation.apply_key.kind = ApplyKeyKind::BaseRowId;
        fixture.interpretation.target.fields.retain(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::Output(_) | PhysicalFieldLogicalIdentity::ApplyKey(_)
            )
        });
        fixture.interpretation.target.fields[0].type_signature = "bigint".into();
        fixture.interpretation.target.fields[0].nullable = false;
        fixture.interpretation.target.partition_fields = vec![TargetPartitionFieldBinding {
            partition_field_id: FieldIdentity::try_new(vec![90]).unwrap(),
            source_target_field_id: fixture.interpretation.outputs[0].target_field_id.clone(),
            transform: TargetPartitionTransform::Identity,
        }];
        StoredMvProjection {
            mv_id: 1,
            facts: fixture.build().unwrap(),
        }
    }

    fn target_partition() -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: 4,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1000,
                partition_field_name: "order_id".into(),
                source_target_field_id: 31,
                source_column_name: "order_id".into(),
                transform: MvPartitionTransformContract::Identity,
            }],
        }
    }

    #[test]
    fn exact_partition_impact_derives_direct_bound_target_key() {
        let projection = partitioned_projection();
        assert_eq!(
            map_connector_partition_to_mv_key(
                &projection,
                7,
                &observation(&[11]),
                &target_partition(),
                &partition(),
            )
            .unwrap(),
            Some(MvPartitionKey::new(
                projection
                    .facts
                    .interpretation()
                    .target
                    .partition_spec_version
                    .clone(),
                vec![MvPartitionKeyField::new(
                    FieldIdentity::try_new(vec![90]).unwrap(),
                    MvPartitionValue::String("42".into()),
                )],
            ))
        );

        let changed_spec = ConnectorChangePartition::try_new(
            Bytes::from_static(&[5]),
            partition().fields().to_vec(),
        )
        .unwrap();
        let error = map_connector_partition_to_mv_key(
            &projection,
            7,
            &observation(&[11]),
            &target_partition(),
            &changed_spec,
        )
        .unwrap_err();
        assert!(error.contains("different source partition spec"), "{error}");
    }

    #[test]
    fn exact_partition_impact_never_reconstructs_a_retired_numeric_mapping() {
        let projection = projection();
        let target_partition = MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        };
        let error = map_connector_partition_to_mv_key(
            &projection,
            7,
            &observation(&[11]),
            &target_partition,
            &partition(),
        )
        .unwrap_err();
        assert!(error.contains("canonical L"), "{error}");

        let error = map_connector_partition_to_mv_key(
            &projection,
            7,
            &observation(&[12]),
            &target_partition,
            &partition(),
        )
        .unwrap_err();
        assert!(error.contains("exact D source occurrence"), "{error}");
    }
}
