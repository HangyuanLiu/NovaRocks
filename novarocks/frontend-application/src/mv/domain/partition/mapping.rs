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

use crate::mv::domain::model::MvPartitionKey;
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_mv_application::persistence::projection::StoredMvProjection;

/// Validates the exact source object carried by a connector partition impact.
///
/// Canonical D/L currently retain the target partition-spec version but not the
/// typed transform/source-field binding needed to turn provider partition facts
/// into an MV partition key. Decoding opaque identities or falling back to the
/// retired numeric schema contract would create a second authority, so exact
/// partition impacts fail closed until that binding is part of canonical L and
/// the provider observation.
pub(crate) fn map_connector_partition_to_mv_key(
    projection: &StoredMvProjection,
    observation: &MvSchemaValidationObservation,
    connector_partition: &novarocks_spi::connector::ConnectorChangePartition,
) -> Result<Option<MvPartitionKey>, String> {
    let schema = observation.exact_schema();
    let source_matches = projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .any(|occurrence| {
            occurrence.catalog_at_binding == observation.table().instance_id.as_str()
                && occurrence.object_id.as_bytes() == schema.object_id.as_bytes().as_ref()
        });
    if !source_matches {
        return Err(
            "connector partition impact does not belong to an exact D source occurrence"
                .to_string(),
        );
    }

    for field in connector_partition.fields() {
        if !schema
            .fields
            .iter()
            .any(|observed| observed.name.eq_ignore_ascii_case(field.source_column()))
        {
            return Err(format!(
                "connector partition impact references source column {} outside the exact schema observation",
                field.source_column()
            ));
        }
    }

    Err(
        "affected partition mapping requires a canonical typed partition transform/source-field binding"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::projection::StoredMvProjection;
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
                &context,
            )
            .unwrap(),
            &context,
        )
        .unwrap()
    }

    fn partition() -> ConnectorChangePartition {
        ConnectorChangePartition::try_new(vec![
            ConnectorChangePartitionField::try_new(
                "order_id",
                ConnectorChangePartitionTransform::Identity,
                ConnectorChangePartitionValue::String("42".into()),
            )
            .unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn exact_partition_impact_never_reconstructs_a_retired_numeric_mapping() {
        let projection = projection();
        let error =
            map_connector_partition_to_mv_key(&projection, &observation(&[11]), &partition())
                .unwrap_err();
        assert!(error.contains("canonical typed partition"), "{error}");

        let error =
            map_connector_partition_to_mv_key(&projection, &observation(&[12]), &partition())
                .unwrap_err();
        assert!(error.contains("exact D source occurrence"), "{error}");
    }
}
