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

//! The sole concrete implementation of the MV storage observation port.
//!
//! The adapter is deliberately limited to catalog-storage observations.  It
//! owns neither SQLite repositories nor lifecycle/scheduler policy.  A caller
//! retains the connector control lease that admitted a target while the adapter
//! projects provider table state into neutral, validated MV observations.

use std::sync::{Arc, RwLock};
use std::time::Instant;

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_connector_iceberg::commit::{MvProvenanceV1, RefreshTechnique};
use novarocks_connector_iceberg::iceberg::spec::Transform;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlRegistry, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceId, ConnectorListNamespacesRequest, ConnectorListTablesRequest,
    ConnectorNamespaceIdentity, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableMetadata,
};

use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogRegistry, IcebergLoadedTable, load_table,
};
use crate::mv::persistence::{
    descriptor::{MV_DESCRIPTOR_PACKAGE_ID_PROP, MvDescriptorV1},
    schema::{MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract},
};
use crate::mv::storage_observation::{
    MvLakePackageObservation, MvLakePublication, MvObservedTargetField, MvPublishedBaseFact,
    MvPublishedLakeFacts, MvPublishedRefreshTechnique, MvStorageObservation,
    MvTargetCreationObservation,
};

/// Concrete Iceberg storage observer retained in Core until provider-owned MV
/// implementation moves behind a later boundary.  It accepts the existing
/// catalog registry only as an implementation detail; no consumer receives a
/// catalog entry or loaded table from this type.
pub(crate) struct IcebergMvStorageObservationAdapter {
    catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
    controls: Arc<dyn ConnectorControlRegistry>,
}

impl IcebergMvStorageObservationAdapter {
    pub(crate) fn new(
        catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
        controls: Arc<dyn ConnectorControlRegistry>,
    ) -> Self {
        Self { catalogs, controls }
    }

    fn exact_entry(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
    ) -> Result<crate::connector::iceberg::catalog::registry::IcebergCatalogEntry, ConnectorError>
    {
        validate_exact_lease(exact_lease, table)?;
        self.catalog_entry(table.instance_id.as_str())
    }

    fn catalog_entry(
        &self,
        catalog: &str,
    ) -> Result<crate::connector::iceberg::catalog::registry::IcebergCatalogEntry, ConnectorError>
    {
        self.catalogs
            .read()
            .map_err(|error| internal(format!("Iceberg catalog registry read lock: {error}")))?
            .get(catalog)
            .map_err(internal)
    }

    fn observe_loaded_package(
        &self,
        table: ConnectorTableIdentity,
        loaded: &IcebergLoadedTable,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        let properties = loaded.table.metadata().properties();
        if !properties.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP) {
            return Ok(None);
        }

        let descriptor = MvDescriptorV1::from_storage_properties(properties).map_err(corrupt)?;
        let expected_package_id = format!("{}.{}", table.namespace, table.table);
        if descriptor.package_id != expected_package_id {
            return Err(corrupt(format!(
                "Iceberg MV descriptor package id mismatch for {}.{}.{}: expected {expected_package_id}, got {}",
                table.instance_id.as_str(),
                table.namespace,
                table.table,
                descriptor.package_id
            )));
        }

        let publication = match loaded.table.metadata().current_snapshot() {
            None => MvLakePublication::NeverPublished,
            Some(snapshot) => {
                match MvProvenanceV1::from_snapshot_summary(snapshot).map_err(corrupt)? {
                    None => MvLakePublication::NeverPublished,
                    Some(provenance) => MvLakePublication::Published(published_facts(
                        snapshot.snapshot_id(),
                        provenance,
                    )?),
                }
            }
        };
        MvLakePackageObservation::try_new(table, descriptor, publication).map(Some)
    }
}

impl MvStorageObservation for IcebergMvStorageObservationAdapter {
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError> {
        validate_context(&context)?;
        let table = &metadata.identity;
        validate_loaded_metadata(exact_lease, metadata)?;
        let entry = self.exact_entry(exact_lease, table)?;
        entry.invalidate_table_cache(&table.namespace, &table.table);
        let loaded = load_table(&entry, &table.namespace, &table.table).map_err(internal)?;
        validate_context(&context)?;
        creation_observation(table.clone(), &loaded)
    }

    fn discover_lake_packages(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<Vec<MvLakePackageObservation>, ConnectorError> {
        validate_context(&context)?;
        let catalog_names = self
            .catalogs
            .read()
            .map_err(|error| internal(format!("Iceberg catalog registry read lock: {error}")))?
            .catalog_names();
        let mut budget = 0_usize;
        let mut packages = Vec::new();

        for catalog in catalog_names {
            validate_context(&context)?;
            reserve_payload(&context, &mut budget, &catalog)?;
            let instance_id = ConnectorInstanceId::parse(&catalog)
                .map_err(|error| internal(error.to_string()))?;
            // Retain one current generation per catalog so namespace/table
            // enumeration and every package observation are fenced together.
            let exact_lease = self.controls.acquire_current(&instance_id)?;
            if exact_lease.binding().descriptor().instance_id != instance_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector control lease instance does not match catalog discovery instance",
                ));
            }
            let entry = self.catalog_entry(&catalog)?;
            let mut namespaces = exact_lease.binding().metadata().list_namespaces(
                ConnectorListNamespacesRequest {
                    instance_id: instance_id.clone(),
                    context: context.clone(),
                },
            )?;
            namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
            namespaces.dedup_by(|left, right| left.namespace == right.namespace);

            for namespace in namespaces {
                validate_namespace_owner(&namespace, &instance_id)?;
                if let Err(error) = normalize_identifier(namespace.namespace.as_ref()) {
                    tracing::warn!(
                        catalog,
                        namespace = %namespace.namespace,
                        error,
                        "skip Iceberg namespace outside the Native identifier contract during lake MV discovery"
                    );
                    continue;
                }
                reserve_payload(&context, &mut budget, namespace.namespace.as_ref())?;
                let mut tables =
                    exact_lease
                        .binding()
                        .metadata()
                        .list_tables(ConnectorListTablesRequest {
                            namespace: namespace.clone(),
                            context: context.clone(),
                        })?;
                tables.sort_by(|left, right| left.table.cmp(&right.table));
                tables.dedup_by(|left, right| left.table == right.table);

                for table in tables {
                    validate_table_owner(&table, &instance_id, &namespace)?;
                    reserve_payload(&context, &mut budget, table.table.as_ref())?;
                    validate_context(&context)?;
                    entry.invalidate_table_cache(&table.namespace, &table.table);
                    let loaded = match load_table(&entry, &table.namespace, &table.table) {
                        Ok(loaded) => loaded,
                        Err(error) => {
                            tracing::warn!(
                                catalog,
                                namespace = %table.namespace,
                                table = %table.table,
                                error,
                                "skip unreadable Iceberg table during lake MV discovery"
                            );
                            continue;
                        }
                    };
                    if let Some(package) = self.observe_loaded_package(table, &loaded)? {
                        packages.push(package);
                    }
                }
            }
        }
        packages.sort_by(|left, right| {
            left.table
                .instance_id
                .as_str()
                .cmp(right.table.instance_id.as_str())
                .then(left.table.namespace.cmp(&right.table.namespace))
                .then(left.table.table.cmp(&right.table.table))
        });
        Ok(packages)
    }

    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        validate_context(&context)?;
        let table = &metadata.identity;
        validate_loaded_metadata(exact_lease, metadata)?;
        let entry = self.exact_entry(exact_lease, table)?;
        entry.invalidate_table_cache(&table.namespace, &table.table);
        let loaded = match load_table(&entry, &table.namespace, &table.table) {
            Ok(loaded) => loaded,
            Err(error) => {
                tracing::warn!(
                    catalog = %table.instance_id.as_str(),
                    namespace = %table.namespace,
                    table = %table.table,
                    error,
                    "skip unreadable named Iceberg table during targeted lake MV discovery"
                );
                return Ok(None);
            }
        };
        validate_context(&context)?;
        self.observe_loaded_package(table.clone(), &loaded)
    }
}

fn validate_loaded_metadata(
    exact_lease: &ConnectorControlPlanningLease,
    metadata: &ConnectorTableMetadata,
) -> Result<(), ConnectorError> {
    validate_exact_lease(exact_lease, &metadata.identity)?;
    if metadata.table.owner() != &metadata.identity.instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV storage observation metadata handle does not match its table identity",
        ));
    }
    Ok(())
}

fn validate_exact_lease(
    exact_lease: &ConnectorControlPlanningLease,
    table: &ConnectorTableIdentity,
) -> Result<(), ConnectorError> {
    if exact_lease.binding().descriptor().instance_id != table.instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV storage observation table does not belong to the retained connector generation",
        ));
    }
    Ok(())
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "MV storage observation request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "MV storage observation request deadline elapsed",
        ));
    }
    Ok(())
}

fn reserve_payload(
    context: &ConnectorRequestContext,
    used: &mut usize,
    value: &str,
) -> Result<(), ConnectorError> {
    *used = used.checked_add(value.len()).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV lake discovery payload accounting overflowed",
        )
    })?;
    if *used > context.max_total_payload_bytes() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV lake discovery names exceed the connector request payload budget",
        ));
    }
    Ok(())
}

fn validate_namespace_owner(
    namespace: &ConnectorNamespaceIdentity,
    instance_id: &ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if &namespace.instance_id != instance_id || namespace.namespace.trim().is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector metadata returned an invalid namespace during MV lake discovery",
        ));
    }
    Ok(())
}

fn validate_table_owner(
    table: &ConnectorTableIdentity,
    instance_id: &ConnectorInstanceId,
    namespace: &ConnectorNamespaceIdentity,
) -> Result<(), ConnectorError> {
    if &table.instance_id != instance_id
        || table.namespace != namespace.namespace
        || table.table.trim().is_empty()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector metadata returned an invalid table during MV lake discovery",
        ));
    }
    Ok(())
}

fn creation_observation(
    table: ConnectorTableIdentity,
    loaded: &IcebergLoadedTable,
) -> Result<MvTargetCreationObservation, ConnectorError> {
    let metadata = loaded.table.metadata();
    let schema = metadata.current_schema();
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| MvObservedTargetField {
            field_id: field.id,
            name: field.name.clone(),
            type_signature: field.field_type.to_string(),
            nullable: !field.required,
        })
        .collect();
    MvTargetCreationObservation::try_new(
        table,
        metadata.uuid().to_string(),
        metadata.current_schema_id(),
        fields,
        partition_contract(loaded)?,
    )
}

fn partition_contract(loaded: &IcebergLoadedTable) -> Result<MvPartitionContract, ConnectorError> {
    let metadata = loaded.table.metadata();
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg MV target partition field {} references missing target field ID {}",
                field.name, field.source_id
            ))
        })?;
        fields.push(MvPartitionFieldContract {
            partition_field_id: field.field_id,
            partition_field_name: field.name.clone(),
            source_target_field_id: field.source_id,
            source_column_name: source.name.clone(),
            transform: partition_transform(&field.transform)?,
        });
    }
    Ok(MvPartitionContract {
        target_spec_id: spec.spec_id(),
        fields,
    })
}

fn partition_transform(
    transform: &Transform,
) -> Result<MvPartitionTransformContract, ConnectorError> {
    match transform {
        Transform::Identity => Ok(MvPartitionTransformContract::Identity),
        Transform::Year => Ok(MvPartitionTransformContract::Year),
        Transform::Month => Ok(MvPartitionTransformContract::Month),
        Transform::Day => Ok(MvPartitionTransformContract::Day),
        Transform::Hour => Ok(MvPartitionTransformContract::Hour),
        Transform::Bucket(num_buckets) => Ok(MvPartitionTransformContract::Bucket {
            num_buckets: *num_buckets,
        }),
        Transform::Truncate(width) => Ok(MvPartitionTransformContract::Truncate { width: *width }),
        Transform::Void => Ok(MvPartitionTransformContract::Void),
        Transform::Unknown => Err(corrupt(
            "Iceberg MV target partition contract cannot persist an unknown transform",
        )),
    }
}

fn published_facts(
    target_snapshot_id: i64,
    provenance: MvProvenanceV1,
) -> Result<MvPublishedLakeFacts, ConnectorError> {
    let technique = match provenance.technique {
        RefreshTechnique::Incremental => MvPublishedRefreshTechnique::Incremental,
        RefreshTechnique::Full => MvPublishedRefreshTechnique::Full,
        RefreshTechnique::MetadataOnly => MvPublishedRefreshTechnique::MetadataOnly,
    };
    let provenance_hash = provenance.content_hash().map_err(corrupt)?;
    let waterline_hash = provenance.waterline_hash().map_err(corrupt)?;
    let bases = provenance
        .bases
        .iter()
        .map(|base| MvPublishedBaseFact {
            table_fqn: base.table_fqn.clone(),
            table_uuid: base.uuid.clone(),
            from_snapshot: base.from_snapshot,
            to_snapshot: base.to_snapshot,
        })
        .collect();
    MvPublishedLakeFacts::try_new(
        target_snapshot_id,
        provenance.refresh_id,
        provenance.mv_id,
        provenance.token,
        technique,
        bases,
        provenance.definition_fingerprint,
        provenance.rows,
        provenance_hash,
        waterline_hash,
    )
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorErrorKind, ConnectorRequestContext,
    };

    use super::{reserve_payload, validate_context};

    struct Cancellation(bool);

    impl ConnectorCancellation for Cancellation {
        fn is_cancelled(&self) -> bool {
            self.0
        }
    }

    fn context(max_total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(Cancellation(false)),
            1,
            max_total_payload_bytes,
        )
        .unwrap()
    }

    #[test]
    fn discovery_payload_budget_is_cumulative() {
        let context = context(3);
        let mut used = 0;
        reserve_payload(&context, &mut used, "ab").unwrap();
        let error = reserve_payload(&context, &mut used, "cd").unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn context_preserves_cancelled_category() {
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(Cancellation(true)),
            1,
            1,
        )
        .unwrap();
        let error = validate_context(&context).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }
}
