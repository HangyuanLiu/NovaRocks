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

//! Consumer-owned observation boundary for MV storage facts.
//!
//! This is intentionally a Core-internal port.  Consumers retain an exact
//! connector planning lease while an adapter reads provider-specific storage,
//! then receive only validated neutral values.  It is not a Connector SPI
//! capability and must not expose concrete table handles or catalog entries.

use std::collections::HashSet;

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorTableIdentity,
};

use crate::mv::persistence::{descriptor::MvDescriptorV1, schema::MvPartitionContract};

/// Exact target-schema facts observed immediately after CREATE/bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvTargetCreationObservation {
    pub(crate) table: ConnectorTableIdentity,
    pub(crate) table_uuid: String,
    pub(crate) schema_id: i32,
    pub(crate) fields: Vec<MvObservedTargetField>,
    pub(crate) partition: MvPartitionContract,
}

impl MvTargetCreationObservation {
    pub(crate) fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        schema_id: i32,
        fields: Vec<MvObservedTargetField>,
        partition: MvPartitionContract,
    ) -> Result<Self, ConnectorError> {
        validate_table_identity(&table, "created MV target")?;
        require_non_empty(&table_uuid, "created MV target UUID")?;
        if fields.is_empty() {
            return corrupt("created MV target observation has no schema fields");
        }

        let mut field_ids = HashSet::with_capacity(fields.len());
        let mut field_names = HashSet::with_capacity(fields.len());
        for field in &fields {
            require_non_empty(&field.name, "created MV target field name")?;
            require_non_empty(&field.type_signature, "created MV target field type")?;
            if !field_ids.insert(field.field_id) {
                return corrupt(format!(
                    "created MV target observation has duplicate field ID {}",
                    field.field_id
                ));
            }
            if !field_names.insert(field.name.as_str()) {
                return corrupt(format!(
                    "created MV target observation has duplicate field name `{}`",
                    field.name
                ));
            }
        }
        validate_partition_contract(&partition, &fields)?;

        Ok(Self {
            table,
            table_uuid,
            schema_id,
            fields,
            partition,
        })
    }
}

/// One field in an observed target schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvObservedTargetField {
    pub(crate) field_id: i32,
    pub(crate) name: String,
    pub(crate) type_signature: String,
    pub(crate) nullable: bool,
}

/// A discovered MV lake package, including its current publication state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvLakePackageObservation {
    pub(crate) table: ConnectorTableIdentity,
    pub(crate) descriptor: MvDescriptorV1,
    pub(crate) publication: MvLakePublication,
}

impl MvLakePackageObservation {
    pub(crate) fn try_new(
        table: ConnectorTableIdentity,
        descriptor: MvDescriptorV1,
        publication: MvLakePublication,
    ) -> Result<Self, ConnectorError> {
        validate_table_identity(&table, "MV lake package")?;
        validate_descriptor(&descriptor)?;
        if let MvLakePublication::Published(facts) = &publication {
            facts.validate()?;
        }
        Ok(Self {
            table,
            descriptor,
            publication,
        })
    }
}

/// The only publication states meaningful to lake recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvLakePublication {
    NeverPublished,
    Published(MvPublishedLakeFacts),
}

/// Persisted refresh facts observed together with the lake package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvPublishedLakeFacts {
    pub(crate) target_snapshot_id: i64,
    pub(crate) refresh_id: i64,
    pub(crate) mv_id: i64,
    pub(crate) token: String,
    pub(crate) technique: MvPublishedRefreshTechnique,
    pub(crate) bases: Vec<MvPublishedBaseFact>,
    pub(crate) definition_fingerprint: String,
    pub(crate) rows: i64,
    pub(crate) provenance_hash: String,
    pub(crate) waterline_hash: String,
}

impl MvPublishedLakeFacts {
    pub(crate) fn try_new(
        target_snapshot_id: i64,
        refresh_id: i64,
        mv_id: i64,
        token: String,
        technique: MvPublishedRefreshTechnique,
        bases: Vec<MvPublishedBaseFact>,
        definition_fingerprint: String,
        rows: i64,
        provenance_hash: String,
        waterline_hash: String,
    ) -> Result<Self, ConnectorError> {
        let facts = Self {
            target_snapshot_id,
            refresh_id,
            mv_id,
            token,
            technique,
            bases,
            definition_fingerprint,
            rows,
            provenance_hash,
            waterline_hash,
        };
        facts.validate()?;
        Ok(facts)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.target_snapshot_id < 0 {
            return corrupt("published MV lake facts have a negative target snapshot ID");
        }
        if self.refresh_id < 0 {
            return corrupt("published MV lake facts have a negative refresh ID");
        }
        if self.mv_id < 0 {
            return corrupt("published MV lake facts have a negative MV ID");
        }
        require_non_empty(&self.token, "published MV refresh token")?;
        require_non_empty(
            &self.definition_fingerprint,
            "published MV definition fingerprint",
        )?;
        require_non_empty(&self.provenance_hash, "published MV provenance hash")?;
        require_non_empty(&self.waterline_hash, "published MV waterline hash")?;
        if self.rows < 0 {
            return corrupt("published MV lake facts have a negative row count");
        }

        let mut base_fqns = HashSet::with_capacity(self.bases.len());
        let mut base_uuids = HashSet::with_capacity(self.bases.len());
        for base in &self.bases {
            require_non_empty(&base.table_fqn, "published MV base table FQN")?;
            require_non_empty(&base.table_uuid, "published MV base table UUID")?;
            if !base_fqns.insert(base.table_fqn.as_str())
                || !base_uuids.insert(base.table_uuid.as_str())
            {
                return corrupt(format!(
                    "published MV lake facts have duplicate base identity `{}` ({})",
                    base.table_fqn, base.table_uuid
                ));
            }
            if base.to_snapshot < 0 || base.from_snapshot.is_some_and(|snapshot| snapshot < 0) {
                return corrupt(format!(
                    "published MV lake facts have a negative watermark for base `{}`",
                    base.table_fqn
                ));
            }
        }
        Ok(())
    }
}

/// One base-table identity and refresh watermark from MV provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvPublishedBaseFact {
    pub(crate) table_fqn: String,
    pub(crate) table_uuid: String,
    pub(crate) from_snapshot: Option<i64>,
    pub(crate) to_snapshot: i64,
}

/// The published refresh technique, detached from provider provenance types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvPublishedRefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

/// Observation port implemented by the sole concrete Iceberg adapter.
pub(crate) trait MvStorageObservation: Send + Sync {
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError>;

    fn discover_lake_packages(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<Vec<MvLakePackageObservation>, ConnectorError>;

    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError>;
}

fn validate_table_identity(
    table: &ConnectorTableIdentity,
    subject: &str,
) -> Result<(), ConnectorError> {
    if table.namespace.trim().is_empty() || table.table.trim().is_empty() {
        return corrupt(format!("{subject} has an empty table identity"));
    }
    Ok(())
}

fn validate_descriptor(descriptor: &MvDescriptorV1) -> Result<(), ConnectorError> {
    require_non_empty(&descriptor.package_id, "MV descriptor package ID")?;
    require_non_empty(&descriptor.logical_sql, "MV descriptor logical SQL")?;
    require_non_empty(&descriptor.dialect, "MV descriptor dialect")?;
    descriptor
        .to_canonical_json()
        .map_err(|err| ConnectorError::new(ConnectorErrorKind::CorruptData, err))?;
    descriptor
        .content_hash()
        .map_err(|err| ConnectorError::new(ConnectorErrorKind::CorruptData, err))?;
    Ok(())
}

fn validate_partition_contract(
    partition: &MvPartitionContract,
    fields: &[MvObservedTargetField],
) -> Result<(), ConnectorError> {
    let mut field_ids = HashSet::with_capacity(fields.len());
    for field in fields {
        field_ids.insert(field.field_id);
    }

    let mut partition_ids = HashSet::with_capacity(partition.fields.len());
    let mut partition_names = HashSet::with_capacity(partition.fields.len());
    for field in &partition.fields {
        require_non_empty(&field.partition_field_name, "MV partition field name")?;
        require_non_empty(&field.source_column_name, "MV partition source column name")?;
        if !partition_ids.insert(field.partition_field_id) {
            return corrupt(format!(
                "created MV target partition contract has duplicate partition field ID {}",
                field.partition_field_id
            ));
        }
        if !partition_names.insert(field.partition_field_name.as_str()) {
            return corrupt(format!(
                "created MV target partition contract has duplicate partition field name `{}`",
                field.partition_field_name
            ));
        }
        if !field_ids.contains(&field.source_target_field_id) {
            return corrupt(format!(
                "created MV target partition contract references missing target field ID {}",
                field.source_target_field_id
            ));
        }
        match &field.transform {
            crate::mv::persistence::schema::MvPartitionTransformContract::Bucket {
                num_buckets,
            } if *num_buckets == 0 => {
                return corrupt("created MV target partition contract has zero buckets");
            }
            crate::mv::persistence::schema::MvPartitionTransformContract::Truncate { width }
                if *width == 0 =>
            {
                return corrupt("created MV target partition contract has zero truncate width");
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_non_empty(value: &str, subject: &str) -> Result<(), ConnectorError> {
    if value.trim().is_empty() {
        return corrupt(format!("{subject} is empty"));
    }
    Ok(())
}

fn corrupt<T>(message: impl Into<String>) -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        message,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};

    use super::{
        MvLakePackageObservation, MvLakePublication, MvObservedTargetField, MvPublishedBaseFact,
        MvPublishedLakeFacts, MvPublishedRefreshTechnique, MvTargetCreationObservation,
    };
    use crate::mv::persistence::{
        descriptor::MvDescriptorV1,
        schema::{MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract},
    };

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("iceberg.rest").unwrap(),
            namespace: Arc::from("db"),
            table: Arc::from("mv_target"),
        }
    }

    fn descriptor() -> MvDescriptorV1 {
        MvDescriptorV1 {
            descriptor_version: 1,
            package_id: "package-1".to_string(),
            logical_sql: "select 1".to_string(),
            dialect: "novarocks".to_string(),
            visible_columns: vec!["c1".to_string()],
            hidden_columns: vec![],
            base_dependencies: vec![],
            schema_contract: None,
            refresh_contract: None,
            created_at_ms: 1,
        }
    }

    fn target_fields() -> Vec<MvObservedTargetField> {
        vec![MvObservedTargetField {
            field_id: 1,
            name: "c1".to_string(),
            type_signature: "int".to_string(),
            nullable: false,
        }]
    }

    fn published_facts() -> MvPublishedLakeFacts {
        MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::Incremental,
            vec![MvPublishedBaseFact {
                table_fqn: "iceberg.db.base".to_string(),
                table_uuid: "base-uuid".to_string(),
                from_snapshot: Some(8),
                to_snapshot: 9,
            }],
            "definition-fingerprint".to_string(),
            1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn target_observation_rejects_duplicate_schema_fields_and_invalid_partition_reference() {
        let mut duplicated = target_fields();
        duplicated.push(MvObservedTargetField {
            field_id: 1,
            name: "c2".to_string(),
            type_signature: "bigint".to_string(),
            nullable: true,
        });
        let err = MvTargetCreationObservation::try_new(
            table(),
            "target-uuid".to_string(),
            0,
            duplicated,
            MvPartitionContract {
                target_spec_id: 0,
                fields: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = MvTargetCreationObservation::try_new(
            table(),
            "target-uuid".to_string(),
            0,
            target_fields(),
            MvPartitionContract {
                target_spec_id: 0,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "day_c1".to_string(),
                    source_target_field_id: 99,
                    source_column_name: "c1".to_string(),
                    transform: MvPartitionTransformContract::Day,
                }],
            },
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn published_facts_reject_duplicate_bases_negative_rows_and_missing_hashes() {
        let duplicate_base = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::Full,
            vec![
                MvPublishedBaseFact {
                    table_fqn: "iceberg.db.base".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    from_snapshot: None,
                    to_snapshot: 9,
                },
                MvPublishedBaseFact {
                    table_fqn: "iceberg.db.base_renamed".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    from_snapshot: None,
                    to_snapshot: 10,
                },
            ],
            "definition-fingerprint".to_string(),
            1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            duplicate_base.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let negative_rows = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::MetadataOnly,
            vec![],
            "definition-fingerprint".to_string(),
            -1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            negative_rows.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let missing_hash = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::MetadataOnly,
            vec![],
            "definition-fingerprint".to_string(),
            0,
            "".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            missing_hash.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn lake_package_accepts_never_published_and_validates_the_descriptor() {
        let observed = MvLakePackageObservation::try_new(
            table(),
            descriptor(),
            MvLakePublication::NeverPublished,
        )
        .unwrap();
        assert_eq!(observed.descriptor.package_id, "package-1");

        let observed = MvLakePackageObservation::try_new(
            table(),
            descriptor(),
            MvLakePublication::Published(published_facts()),
        )
        .unwrap();
        assert_eq!(observed.table.table.as_ref(), "mv_target");

        let mut invalid = descriptor();
        invalid.package_id.clear();
        let err =
            MvLakePackageObservation::try_new(table(), invalid, MvLakePublication::NeverPublished)
                .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }
}
