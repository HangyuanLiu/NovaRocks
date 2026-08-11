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

//! Pure reconstruction of an MV's SQLite definition-create inputs from its
//! lake package observation (descriptor + publication facts).
//!
//! This is the core of W4's "SQLite as a rebuildable cache" property: given
//! nothing but a validated lake package observation,
//! [`rebuild_mv_definition_from_lake`] reproduces exactly the
//! inputs `create_iceberg_mv` would have persisted into SQLite at CREATE
//! time, plus the refresh watermark a completed refresh would have recorded.
//! M3 calls this at startup for MVs discovered on the lake but missing from
//! SQLite. No catalog I/O happens here — every input is already in memory.

use std::collections::BTreeMap;
use std::sync::{Arc, atomic::AtomicBool};

use crate::engine::StandaloneState;
use crate::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::CreateMvDefinitionRequest;
use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::descriptor::DescriptorDependency;
use crate::mv::storage_observation::{
    MvLakePackageObservation, MvLakePublication, discover_mv_lake_packages,
};

/// Output of [`rebuild_mv_definition_from_lake`]: the definition-create
/// request `create_iceberg_mv` would have issued, plus the refresh watermark
/// maps `StoredMvDefinition` separately tracks (`CreateMvDefinitionRequest`
/// has no watermark fields — a freshly-created MV has never refreshed).
pub(crate) struct RebuiltMvDefinition {
    pub create_request: CreateMvDefinitionRequest,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
}

/// Reconstruct an MV's SQLite definition-create inputs purely from its lake
/// package (descriptor + optional current-snapshot provenance). Pure: no I/O.
///
pub(crate) fn rebuild_mv_definition_from_lake(
    package: &MvLakePackageObservation,
) -> Result<RebuiltMvDefinition, String> {
    let descriptor = &package.descriptor;

    let base_table_refs = descriptor
        .base_dependencies
        .iter()
        .map(|dep| format!("{}.{}.{}", dep.catalog, dep.namespace, dep.name))
        .collect();

    let schema_contract = descriptor.schema_contract_typed()?;
    let partition_spec = schema_contract
        .as_ref()
        .and_then(|contract| contract.target.partition.clone());

    let create_request = CreateMvDefinitionRequest {
        select_sql: descriptor.logical_sql.clone(),
        base_table_refs,
        // W1 descriptors carry no primary-key metadata; a rebuilt definition
        // is indistinguishable from one created without `PRIMARY KEY (...)`.
        primary_key_columns: Vec::new(),
        storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
        target_catalog: Some(package.table.instance_id.as_str().to_string()),
        target_namespace: Some(package.table.namespace.to_string()),
        target_table: Some(package.table.table.to_string()),
        schema_contract,
        partition_spec,
        created_at_ms: descriptor.created_at_ms,
    };

    let (last_refresh_snapshots, last_refresh_table_uuids) = match &package.publication {
        MvLakePublication::Published(facts) => {
            let mut snapshots = BTreeMap::new();
            let mut table_uuids = BTreeMap::new();
            for base in &facts.bases {
                snapshots.insert(base.table_fqn.clone(), base.to_snapshot);
                table_uuids.insert(base.table_fqn.clone(), base.table_uuid.clone());
            }
            (snapshots, table_uuids)
        }
        MvLakePublication::NeverPublished => (BTreeMap::new(), BTreeMap::new()),
    };

    Ok(RebuiltMvDefinition {
        create_request,
        last_refresh_snapshots,
        last_refresh_table_uuids,
    })
}

/// Rebuild any lake-native Iceberg MV definitions that are present on the lake
/// but missing from SQLite, making them visible and refreshable on a
/// fresh-`[metadata]` cluster.
///
/// This is the integration that fulfills W4 statelessness: SQLite is treated as
/// a rebuildable cache over the lake. For every registered Iceberg catalog we
/// enumerate its namespaces, discover the MV packages each namespace carries
/// (MV-table inline descriptor — never SQLite), and for each MV whose target is
/// not already recorded in SQLite we
/// reconstruct its definition-create inputs with
/// [`rebuild_mv_definition_from_lake`] and persist them (definition + refresh
/// watermark + dependencies) through the repository's ordinary create path.
///
/// Idempotent: MVs already present in SQLite (cache hit, matched by target
/// `catalog.namespace.table`) are skipped, so calling this at startup
/// on an already-populated cluster is a no-op.
///
pub(crate) fn rebuild_imv_cache_from_lake(state: &Arc<StandaloneState>) -> Result<(), String> {
    // No metadata provider means SQLite is not the runtime authority (e.g.
    // FE-compatible mode or a metadata-less test state); there is nothing to
    // rebuild a cache into.
    if state.metadata_provider.is_none() {
        return Ok(());
    }

    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?;
    let read = state
        .metadata_provider
        .as_ref()
        .expect("metadata provider checked above")
        .begin_read()
        .map_err(|error| format!("open catalog attachment read transaction failed: {error}"))?;
    let instance_ids = state
        .catalog_attachment_repo
        .list(read.as_ref())
        .map_err(|error| format!("list catalog attachments for MV rebuild failed: {error}"))?
        .into_iter()
        .filter(|attachment| {
            attachment.properties.properties.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case("type") && value.eq_ignore_ascii_case("iceberg")
            })
        })
        .map(|attachment| {
            novarocks_spi::connector::ConnectorInstanceId::parse(&attachment.catalog).map_err(
                |error| {
                    format!(
                        "parse Iceberg catalog attachment `{}` for MV rebuild: {error}",
                        attachment.catalog
                    )
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let packages = discover_mv_lake_packages(
        state.connector_control.as_ref(),
        instance_ids,
        state.mv_storage_observation.as_ref(),
        context,
    )
    .map_err(|error| format!("discover lake MV packages failed: {error}"))?;
    for package in packages {
        rebuild_one_lake_package_if_missing(state, &package)?;
    }
    Ok(())
}

/// Persist a single observed lake package's definition into SQLite if it is not already
/// present. The MV is keyed by its target `catalog.namespace.table`
/// (the same key the create path registers via `find_by_target`).
///
/// Exposed to the crate so the W0 stateless-rebuild harness
/// (`stateless_rebuild::execute_request`) can drive a *targeted* single-MV
/// rebuild for the `full` level, instead of sweeping every registered catalog
/// through [`rebuild_imv_cache_from_lake`].
pub(crate) fn rebuild_one_lake_package_if_missing(
    state: &Arc<StandaloneState>,
    package: &MvLakePackageObservation,
) -> Result<(), String> {
    // Cache-hit check: skip MVs already recorded in SQLite. The rebuilt target
    // maps to (discovered.catalog, discovered.namespace, discovered.table).
    let existing = state
        .mv_repository
        .find_by_target(&crate::mv::model::MvTarget {
            catalog: Some(package.table.instance_id.as_str().to_string()),
            database: package.table.namespace.to_string(),
            name: package.table.table.to_string(),
        })
        .map_err(|e| format!("look up MV definition during lake rebuild failed: {e}"))?;
    if existing.is_some() {
        return Ok(());
    }

    let rebuilt = rebuild_mv_definition_from_lake(package)?;
    let created_at_ms = rebuilt.create_request.created_at_ms;
    let dependencies =
        dependency_requests_from_descriptor(&package.descriptor.base_dependencies, created_at_ms)?;

    let definition = state
        .mv_repository
        .create(
            uuid::Uuid::new_v4(),
            crate::mv::repository::CreateMvRepositoryRequest {
                definition: rebuilt.create_request,
                refresh: Default::default(),
                dependencies: dependencies.clone(),
            },
        )
        .map_err(|e| format!("rebuild iceberg MV repository metadata failed: {e}"))?;
    state
        .mv_repository
        .set_rebuilt_refresh_watermark(
            definition.mv_id,
            rebuilt.last_refresh_snapshots,
            rebuilt.last_refresh_table_uuids,
        )
        .map_err(|e| format!("stamp rebuilt iceberg MV refresh watermark failed: {e}"))?;
    Ok(())
}

/// Map the descriptor's `base_dependencies` back into the repository
/// `CreateMvDependencyRequest` shape used by `replace_dependencies_for_mv`.
/// This is the inverse of `iceberg_refresh::descriptor_dependency_from_request`.
fn dependency_requests_from_descriptor(
    dependencies: &[DescriptorDependency],
    created_at_ms: i64,
) -> Result<Vec<CreateMvDependencyRequest>, String> {
    dependencies
        .iter()
        .map(|dep| {
            Ok(CreateMvDependencyRequest {
                upstream: MvDependencyObjectRef {
                    catalog: (!dep.catalog.is_empty()).then(|| dep.catalog.clone()),
                    database_or_namespace: dep.namespace.clone(),
                    name: dep.name.clone(),
                    object_type: parse_dependency_object_type(&dep.object_type)?,
                    storage_engine: parse_dependency_storage_engine(&dep.storage_engine)?,
                },
                created_at_ms,
            })
        })
        .collect()
}

fn parse_dependency_object_type(value: &str) -> Result<MvDependencyObjectType, String> {
    match value {
        "table" => Ok(MvDependencyObjectType::Table),
        "materialized_view" => Ok(MvDependencyObjectType::MaterializedView),
        other => Err(format!(
            "unknown MV descriptor dependency object type `{other}`"
        )),
    }
}

fn parse_dependency_storage_engine(value: &str) -> Result<MvDependencyStorageEngine, String> {
    match value {
        "starrocks" => Ok(MvDependencyStorageEngine::StarRocks),
        "iceberg" => Ok(MvDependencyStorageEngine::Iceberg),
        "external_table" => Ok(MvDependencyStorageEngine::ExternalTable),
        other => Err(format!(
            "unknown MV descriptor dependency storage engine `{other}`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::descriptor::{DescriptorDependency, MvDescriptorV1};
    use crate::mv::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };
    use crate::mv::storage_observation::{
        MvLakePackageObservation, MvLakePublication, MvPublishedBaseFact, MvPublishedLakeFacts,
        MvPublishedRefreshTechnique,
    };
    use crate::sql::planner::vocabulary::ApplyKeySource;
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};
    use std::sync::Arc;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_uuid: "uuid-orders".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "uuid-mv".to_string(),
                schema_id_at_create: 1,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 99,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 0,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 1000,
                        partition_field_name: "id_bucket".to_string(),
                        source_target_field_id: 1,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Bucket { num_buckets: 4 },
                    }],
                }),
            },
        }
    }

    fn sample_package(publication: MvLakePublication) -> MvLakePackageObservation {
        let mut descriptor = MvDescriptorV1 {
            descriptor_version: 1,
            package_id: "analytics.mv_orders".to_string(),
            logical_sql: "SELECT id FROM ice.sales.orders".to_string(),
            dialect: "starrocks".to_string(),
            visible_columns: vec!["id".to_string()],
            hidden_columns: vec!["__nova_base_row_id".to_string()],
            base_dependencies: vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }],
            schema_contract: None,
            refresh_contract: None,
            created_at_ms: 123,
        };
        descriptor
            .set_schema_contract(&sample_contract())
            .expect("set schema contract");
        MvLakePackageObservation::try_new(
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
                namespace: Arc::from("analytics"),
                table: Arc::from("mv_orders"),
            },
            descriptor,
            publication,
        )
        .expect("valid lake package")
    }

    fn sample_publication() -> MvLakePublication {
        MvLakePublication::Published(
            MvPublishedLakeFacts::try_new(
                300,
                7,
                1,
                "token-7".to_string(),
                MvPublishedRefreshTechnique::Incremental,
                vec![MvPublishedBaseFact {
                    table_fqn: "ice.sales.orders".to_string(),
                    table_uuid: "uuid-orders".to_string(),
                    from_snapshot: Some(100),
                    to_snapshot: 200,
                }],
                "fp-abc".to_string(),
                42,
                "provenance-hash".to_string(),
                "waterline-hash".to_string(),
            )
            .expect("valid published facts"),
        )
    }

    #[test]
    fn rebuild_maps_descriptor_and_provenance() {
        let package = sample_package(sample_publication());

        let rebuilt = rebuild_mv_definition_from_lake(&package).expect("rebuild succeeds");

        let request = &rebuilt.create_request;
        assert_eq!(request.select_sql, "SELECT id FROM ice.sales.orders");
        assert_eq!(
            request.base_table_refs,
            vec!["ice.sales.orders".to_string()]
        );
        assert!(request.primary_key_columns.is_empty());
        assert_eq!(
            request.storage_engine,
            MvStorageEngine::Iceberg.as_sql_str()
        );
        assert_eq!(request.target_catalog.as_deref(), Some("ice"));
        assert_eq!(request.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(request.target_table.as_deref(), Some("mv_orders"));
        assert_eq!(request.created_at_ms, 123);

        let contract = request
            .schema_contract
            .as_ref()
            .expect("schema contract present");
        assert_eq!(contract, &sample_contract());

        let partition = request.partition_spec.as_ref().expect("partition spec");
        assert_eq!(partition.target_spec_id, 0);
        assert_eq!(partition.fields.len(), 1);
        assert_eq!(partition.fields[0].partition_field_name, "id_bucket");

        assert_eq!(
            rebuilt.last_refresh_snapshots.get("ice.sales.orders"),
            Some(&200)
        );
        assert_eq!(
            rebuilt.last_refresh_table_uuids.get("ice.sales.orders"),
            Some(&"uuid-orders".to_string())
        );
    }

    #[test]
    fn rebuild_never_published_has_empty_watermark() {
        let package = sample_package(MvLakePublication::NeverPublished);

        let rebuilt = rebuild_mv_definition_from_lake(&package).expect("rebuild succeeds");

        assert!(rebuilt.last_refresh_snapshots.is_empty());
        assert!(rebuilt.last_refresh_table_uuids.is_empty());
        // The create request is still fully valid even with no refresh history.
        assert_eq!(
            rebuilt.create_request.select_sql,
            "SELECT id FROM ice.sales.orders"
        );
        assert!(rebuilt.create_request.schema_contract.is_some());
    }
}
