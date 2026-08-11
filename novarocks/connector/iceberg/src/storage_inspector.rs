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

//! Server-composition-only projection of Iceberg MV storage facts.
//!
//! The inspector interprets an opaque table handle only while the caller
//! retains the exact control generation that loaded it. Its outputs contain
//! no Iceberg values, catalog clients, or application-owned MV types.

use std::collections::BTreeMap;
use std::time::Instant;

use serde::Deserialize;

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorTableMetadata,
};

use crate::commit::{MvProvenanceV1, RefreshTechnique};
use crate::iceberg::spec::{FormatVersion, TableMetadata, Transform};
use crate::scan_model::IcebergTableInfo;

pub(crate) const MV_DESCRIPTOR_PACKAGE_ID_PROP: &str = "novarocks.mv.descriptor.package-id";
const MV_DESCRIPTOR_HASH_PROP: &str = "novarocks.mv.descriptor.hash";
const MV_DESCRIPTOR_INLINE_PROP: &str = "novarocks.mv.descriptor.inline";
const MAX_TARGET_FIELDS: usize = 4_096;
const MAX_PARTITION_FIELDS: usize = 4_096;
const MAX_PROVENANCE_BASES: usize = 16_384;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageTargetObservation {
    pub table_uuid: String,
    pub schema_id: i32,
    pub format_v3: bool,
    pub explicit_row_lineage_enabled: bool,
    pub fields: Vec<IcebergStorageTargetField>,
    pub partition: IcebergStoragePartitionContract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageTargetField {
    pub field_id: i32,
    pub name: String,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePartitionContract {
    pub target_spec_id: i32,
    pub fields: Vec<IcebergStoragePartitionField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePartitionField {
    pub partition_field_id: i32,
    pub partition_field_name: String,
    pub source_target_field_id: i32,
    pub source_column_name: String,
    pub transform: IcebergStoragePartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergStoragePartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageLakePackageObservation {
    pub descriptor_properties: BTreeMap<String, String>,
    pub publication: IcebergStorageLakePublication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergStorageLakePublication {
    NeverPublished,
    Published(IcebergStoragePublishedFacts),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePublishedFacts {
    pub target_snapshot_id: i64,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
    pub technique: IcebergStorageRefreshTechnique,
    pub bases: Vec<IcebergStoragePublishedBaseFact>,
    pub definition_fingerprint: String,
    pub rows: i64,
    pub provenance_hash: String,
    pub waterline_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePublishedBaseFact {
    pub table_fqn: String,
    pub table_uuid: String,
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergStorageRefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

/// Stateless inspector installed only by the Server composition root.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergStorageInspector;

impl IcebergStorageInspector {
    pub fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<IcebergStorageTargetObservation, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        target_observation(&table, &context)
    }

    pub fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<IcebergStorageLakePackageObservation>, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        lake_package_observation(&table, &context)
    }
}

fn target_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<IcebergStorageTargetObservation, ConnectorError> {
    let schema = table.current_schema();
    if schema.as_struct().fields().len() > MAX_TARGET_FIELDS {
        return Err(exhausted(
            "Iceberg MV target schema exceeds the inspection field limit",
        ));
    }
    let mut budget = 0_usize;
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            reserve(context, &mut budget, &field.name)?;
            let type_signature = field.field_type.to_string();
            reserve(context, &mut budget, &type_signature)?;
            Ok(IcebergStorageTargetField {
                field_id: field.id,
                name: field.name.clone(),
                type_signature,
                nullable: !field.required,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let spec = table.default_partition_spec();
    if spec.fields().len() > MAX_PARTITION_FIELDS {
        return Err(exhausted(
            "Iceberg MV target partition spec exceeds the inspection field limit",
        ));
    }
    let mut partition_fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg MV target partition field {} references missing target field ID {}",
                field.name, field.source_id
            ))
        })?;
        reserve(context, &mut budget, &field.name)?;
        reserve(context, &mut budget, &source.name)?;
        partition_fields.push(IcebergStoragePartitionField {
            partition_field_id: field.field_id,
            partition_field_name: field.name.clone(),
            source_target_field_id: field.source_id,
            source_column_name: source.name.clone(),
            transform: partition_transform(&field.transform)?,
        });
    }
    let table_uuid = table.uuid().to_string();
    reserve(context, &mut budget, &table_uuid)?;
    validate_context(context)?;
    Ok(IcebergStorageTargetObservation {
        table_uuid,
        schema_id: table.current_schema_id(),
        format_v3: matches!(table.format_version(), FormatVersion::V3),
        explicit_row_lineage_enabled: table
            .properties()
            .get("write.row-lineage")
            .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        fields,
        partition: IcebergStoragePartitionContract {
            target_spec_id: spec.spec_id(),
            fields: partition_fields,
        },
    })
}

fn lake_package_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<Option<IcebergStorageLakePackageObservation>, ConnectorError> {
    let properties = table.properties();
    if !properties.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP) {
        return Ok(None);
    }
    let mut budget = 0_usize;
    let mut descriptor_properties = BTreeMap::new();
    for key in [
        MV_DESCRIPTOR_PACKAGE_ID_PROP,
        MV_DESCRIPTOR_HASH_PROP,
        MV_DESCRIPTOR_INLINE_PROP,
    ] {
        if let Some(value) = properties.get(key) {
            reserve(context, &mut budget, key)?;
            reserve(context, &mut budget, value)?;
            descriptor_properties.insert(key.to_string(), value.clone());
        }
    }
    if !descriptor_properties.contains_key(MV_DESCRIPTOR_INLINE_PROP) {
        return Err(corrupt(
            "Iceberg MV package is missing its inline descriptor property",
        ));
    }
    let publication = match table.current_snapshot() {
        None => IcebergStorageLakePublication::NeverPublished,
        Some(snapshot) => match MvProvenanceV1::from_snapshot_summary(snapshot).map_err(corrupt)? {
            None => IcebergStorageLakePublication::NeverPublished,
            Some(provenance) => IcebergStorageLakePublication::Published(published_facts(
                snapshot.snapshot_id(),
                provenance,
                context,
                &mut budget,
            )?),
        },
    };
    validate_context(context)?;
    Ok(Some(IcebergStorageLakePackageObservation {
        descriptor_properties,
        publication,
    }))
}

#[derive(Deserialize)]
struct TableHandlePayload {
    namespace: String,
    table: String,
    table_info: Option<IcebergTableInfo>,
}

fn decoded_table(
    exact_lease: &ConnectorControlPlanningLease,
    metadata: &ConnectorTableMetadata,
    context: &ConnectorRequestContext,
) -> Result<TableMetadata, ConnectorError> {
    validate_context(context)?;
    if exact_lease.binding().descriptor().instance_id != metadata.identity.instance_id
        || metadata.table.owner() != &metadata.identity.instance_id
    {
        return Err(invalid(
            "Iceberg storage inspection metadata does not belong to the retained generation",
        ));
    }
    let payload: TableHandlePayload =
        serde_json::from_slice(metadata.table.payload()).map_err(|error| {
            corrupt(format!(
                "decode Iceberg table handle for storage inspection: {error}"
            ))
        })?;
    if payload.namespace != metadata.identity.namespace.as_ref()
        || payload.table != metadata.identity.table.as_ref()
    {
        return Err(corrupt(
            "Iceberg storage inspection table handle identity does not match loaded metadata",
        ));
    }
    let table_info = payload
        .table_info
        .ok_or_else(|| corrupt("Iceberg storage inspection handle has no frozen table metadata"))?;
    if table_info.namespace != payload.namespace || table_info.table != payload.table {
        return Err(corrupt(
            "Iceberg storage inspection frozen table identity is inconsistent",
        ));
    }
    let serialized = table_info.serialized_metadata.ok_or_else(|| {
        corrupt("Iceberg storage inspection handle has no serialized table metadata")
    })?;
    if serialized.len() > context.max_total_payload_bytes() {
        return Err(exhausted(
            "Iceberg storage inspection metadata exceeds the request payload budget",
        ));
    }
    serde_json::from_str(&serialized).map_err(|error| {
        corrupt(format!(
            "decode Iceberg storage inspection metadata: {error}"
        ))
    })
}

fn published_facts(
    target_snapshot_id: i64,
    provenance: MvProvenanceV1,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<IcebergStoragePublishedFacts, ConnectorError> {
    if provenance.bases.len() > MAX_PROVENANCE_BASES {
        return Err(exhausted(
            "Iceberg MV provenance exceeds the inspection base limit",
        ));
    }
    reserve(context, budget, &provenance.token)?;
    reserve(context, budget, &provenance.definition_fingerprint)?;
    let provenance_hash = provenance.content_hash().map_err(corrupt)?;
    let waterline_hash = provenance.waterline_hash().map_err(corrupt)?;
    reserve(context, budget, &provenance_hash)?;
    reserve(context, budget, &waterline_hash)?;
    let bases = provenance
        .bases
        .iter()
        .map(|base| {
            reserve(context, budget, &base.table_fqn)?;
            reserve(context, budget, &base.uuid)?;
            Ok(IcebergStoragePublishedBaseFact {
                table_fqn: base.table_fqn.clone(),
                table_uuid: base.uuid.clone(),
                from_snapshot: base.from_snapshot,
                to_snapshot: base.to_snapshot,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(IcebergStoragePublishedFacts {
        target_snapshot_id,
        refresh_id: provenance.refresh_id,
        mv_id: provenance.mv_id,
        token: provenance.token,
        technique: match provenance.technique {
            RefreshTechnique::Incremental => IcebergStorageRefreshTechnique::Incremental,
            RefreshTechnique::Full => IcebergStorageRefreshTechnique::Full,
            RefreshTechnique::MetadataOnly => IcebergStorageRefreshTechnique::MetadataOnly,
        },
        bases,
        definition_fingerprint: provenance.definition_fingerprint,
        rows: provenance.rows,
        provenance_hash,
        waterline_hash,
    })
}

fn partition_transform(
    transform: &Transform,
) -> Result<IcebergStoragePartitionTransform, ConnectorError> {
    match transform {
        Transform::Identity => Ok(IcebergStoragePartitionTransform::Identity),
        Transform::Year => Ok(IcebergStoragePartitionTransform::Year),
        Transform::Month => Ok(IcebergStoragePartitionTransform::Month),
        Transform::Day => Ok(IcebergStoragePartitionTransform::Day),
        Transform::Hour => Ok(IcebergStoragePartitionTransform::Hour),
        Transform::Bucket(num_buckets) => Ok(IcebergStoragePartitionTransform::Bucket {
            num_buckets: *num_buckets,
        }),
        Transform::Truncate(width) => {
            Ok(IcebergStoragePartitionTransform::Truncate { width: *width })
        }
        Transform::Void => Ok(IcebergStoragePartitionTransform::Void),
        Transform::Unknown => Err(corrupt(
            "Iceberg storage inspection cannot project an unknown partition transform",
        )),
    }
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg storage inspection request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg storage inspection request deadline elapsed",
        ));
    }
    Ok(())
}

fn reserve(
    context: &ConnectorRequestContext,
    budget: &mut usize,
    value: &str,
) -> Result<(), ConnectorError> {
    *budget = budget
        .checked_add(value.len())
        .ok_or_else(|| exhausted("Iceberg storage inspection payload accounting overflowed"))?;
    if *budget > context.max_total_payload_bytes() {
        return Err(exhausted(
            "Iceberg storage inspection facts exceed the request payload budget",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_spi::connector::{ConnectorCancellation, ConnectorErrorKind};

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context(max_total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            Arc::new(NeverCancelled),
            max_total_payload_bytes.min(1024),
            max_total_payload_bytes,
        )
        .expect("context")
    }

    fn metadata(properties: HashMap<String, String>) -> TableMetadata {
        metadata_with_format(FormatVersion::V2, properties)
    }

    fn metadata_with_format(
        format_version: FormatVersion,
        properties: HashMap<String, String>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///storage-inspector-test".to_string(),
            format_version,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn target_projection_preserves_field_identity_and_nullability() {
        let observed = target_observation(&metadata(HashMap::new()), &context(4096))
            .expect("target observation");
        assert_eq!(observed.fields.len(), 2);
        assert_eq!(observed.fields[0].field_id, 1);
        assert_eq!(observed.fields[0].name, "id");
        assert!(!observed.fields[0].nullable);
        assert_eq!(observed.fields[1].name, "name");
        assert!(observed.fields[1].nullable);
        assert!(observed.partition.fields.is_empty());
        assert!(!observed.format_v3);
        assert!(!observed.explicit_row_lineage_enabled);
    }

    #[test]
    fn schema_validation_projection_requires_explicit_row_lineage_property() {
        let implicit = target_observation(
            &metadata_with_format(FormatVersion::V3, HashMap::new()),
            &context(4096),
        )
        .expect("implicit row lineage observation");
        assert!(implicit.format_v3);
        assert!(!implicit.explicit_row_lineage_enabled);

        let explicit = target_observation(
            &metadata_with_format(
                FormatVersion::V3,
                HashMap::from([("write.row-lineage".to_string(), "TRUE".to_string())]),
            ),
            &context(4096),
        )
        .expect("explicit row lineage observation");
        assert!(explicit.explicit_row_lineage_enabled);
    }

    #[test]
    fn lake_projection_is_absent_for_ordinary_table() {
        assert_eq!(
            lake_package_observation(&metadata(HashMap::new()), &context(4096))
                .expect("lake observation"),
            None
        );
    }

    #[test]
    fn lake_projection_requires_inline_descriptor() {
        let error = lake_package_observation(
            &metadata(HashMap::from([(
                MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                "analytics.mv_orders".to_string(),
            )])),
            &context(4096),
        )
        .expect_err("missing inline descriptor");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn lake_projection_is_bounded_by_request_context() {
        let error = lake_package_observation(
            &metadata(HashMap::from([
                (
                    MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                    "analytics.mv_orders".to_string(),
                ),
                (MV_DESCRIPTOR_INLINE_PROP.to_string(), "x".repeat(1024)),
            ])),
            &context(64),
        )
        .expect_err("payload limit");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn unknown_partition_transform_fails_closed() {
        let error = partition_transform(&Transform::Unknown).expect_err("unknown transform");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
