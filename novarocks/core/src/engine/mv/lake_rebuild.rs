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
//! lake package (W1 descriptor + optional W3a provenance).
//!
//! This is the core of W4's "SQLite as a rebuildable cache" property: given
//! nothing but the descriptor already inlined on the MV table (and,
//! optionally, the provenance record carried by the MV table's current
//! snapshot), [`rebuild_mv_definition_from_lake`] reproduces exactly the
//! inputs `create_iceberg_mv` would have persisted into SQLite at CREATE
//! time, plus the refresh watermark a completed refresh would have recorded.
//! M3 calls this at startup for MVs discovered on the lake but missing from
//! SQLite. No catalog I/O happens here — every input is already in memory.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::connector::iceberg::commit::mv_provenance::MvProvenanceV1;
use crate::engine::StandaloneState;
use crate::engine::mv::iceberg_discovery::{DiscoveredIcebergMv, discover_iceberg_mvs_from_entry};
use crate::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::CreateMvDefinitionRequest;
use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::descriptor::DescriptorDependency;

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
/// `provenance` should be the [`MvProvenanceV1`] read from the MV table's
/// current snapshot summary, if any. `None` means the MV was created
/// but has never completed a refresh, matching a brand-new `StoredMvDefinition`
/// whose watermark maps are empty.
pub(crate) fn rebuild_mv_definition_from_lake(
    discovered: &DiscoveredIcebergMv,
    provenance: Option<&MvProvenanceV1>,
) -> Result<RebuiltMvDefinition, String> {
    let descriptor = &discovered.descriptor;

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
        target_catalog: Some(discovered.catalog.clone()),
        target_namespace: Some(discovered.namespace.clone()),
        target_table: Some(discovered.table.clone()),
        schema_contract,
        partition_spec,
        created_at_ms: descriptor.created_at_ms,
    };

    let (last_refresh_snapshots, last_refresh_table_uuids) = match provenance {
        Some(provenance) => {
            let mut snapshots = BTreeMap::new();
            let mut table_uuids = BTreeMap::new();
            for base in &provenance.bases {
                snapshots.insert(base.table_fqn.clone(), base.to_snapshot);
                table_uuids.insert(base.table_fqn.clone(), base.uuid.clone());
            }
            (snapshots, table_uuids)
        }
        None => (BTreeMap::new(), BTreeMap::new()),
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
/// ## Enumeration scope (documented W4 limitation)
///
/// Catalogs are enumerated from the live registry
/// (`IcebergCatalogRegistry::catalog_names`) — i.e. catalogs already
/// re-registered from config / SQLite by `restore_iceberg_catalogs`, which runs
/// before this. Namespaces are enumerated per catalog from the lake itself
/// (`registry::list_namespaces`), so every namespace physically present in the
/// warehouse / REST catalog is swept, not merely those SQLite happens to know.
/// The remaining gap is catalog *discovery*: a catalog that exists on the lake
/// but was never declared to this cluster (no config entry, no `CREATE EXTERNAL
/// CATALOG`) is not enumerated, because NovaRocks has no warehouse URI /
/// credentials to reach it. Rebuild is therefore bounded by the set of
/// registered catalogs, which matches how every other lake operation resolves a
/// catalog by name.
pub(crate) fn rebuild_imv_cache_from_lake(state: &Arc<StandaloneState>) -> Result<(), String> {
    // No metadata provider means SQLite is not the runtime authority (e.g.
    // FE-compatible mode or a metadata-less test state); there is nothing to
    // rebuild a cache into.
    if state.metadata_provider.is_none() {
        return Ok(());
    }

    let catalog_names = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.catalog_names()
    };

    for catalog in catalog_names {
        let entry = {
            let catalogs = state
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            catalogs.get(&catalog)?
        };
        let namespaces = crate::connector::iceberg::catalog::registry::list_namespaces(&entry)?;
        for namespace in namespaces {
            let discovered = discover_iceberg_mvs_from_entry(&entry, &catalog, &namespace)?;
            for mv in discovered {
                rebuild_one_discovered_mv_if_missing(state, &entry, &mv)?;
            }
        }
    }
    Ok(())
}

/// Persist a single discovered MV's definition into SQLite if it is not already
/// present. The MV is keyed by its target `catalog.namespace.table`
/// (the same key the create path registers via `find_by_target`).
///
/// Exposed to the crate so the W0 stateless-rebuild harness
/// (`stateless_rebuild::execute_request`) can drive a *targeted* single-MV
/// rebuild for the `full` level, instead of sweeping every registered catalog
/// through [`rebuild_imv_cache_from_lake`].
pub(crate) fn rebuild_one_discovered_mv_if_missing(
    state: &Arc<StandaloneState>,
    entry: &crate::connector::iceberg::catalog::registry::IcebergCatalogEntry,
    mv: &DiscoveredIcebergMv,
) -> Result<(), String> {
    // Cache-hit check: skip MVs already recorded in SQLite. The rebuilt target
    // maps to (discovered.catalog, discovered.namespace, discovered.table).
    let existing = state
        .mv_repository
        .find_by_target(&crate::mv::model::MvTarget {
            catalog: Some(mv.catalog.clone()),
            database: mv.namespace.clone(),
            name: mv.table.clone(),
        })
        .map_err(|e| format!("look up MV definition during lake rebuild failed: {e}"))?;
    if existing.is_some() {
        return Ok(());
    }

    // Read the MV table's current-snapshot provenance for the refresh watermark.
    // Absent provenance (created-but-never-refreshed MV) yields empty watermark
    // maps, matching a freshly created definition.
    let loaded =
        crate::connector::iceberg::catalog::registry::load_table(entry, &mv.namespace, &mv.table)?;
    let provenance = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|current| MvProvenanceV1::from_snapshot_summary(current))
        .transpose()?
        .flatten();

    let rebuilt = rebuild_mv_definition_from_lake(mv, provenance.as_ref())?;
    let created_at_ms = rebuilt.create_request.created_at_ms;
    let dependencies =
        dependency_requests_from_descriptor(&mv.descriptor.base_dependencies, created_at_ms)?;

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
    use crate::connector::iceberg::commit::mv_provenance::{ProvenanceBase, RefreshTechnique};
    use crate::mv::persistence::descriptor::{DescriptorDependency, MvDescriptorV1};
    use crate::mv::persistence::schema::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

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

    fn sample_discovered() -> DiscoveredIcebergMv {
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
        DiscoveredIcebergMv {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            public_name: "mv_orders".to_string(),
            table: "mv_orders".to_string(),
            descriptor,
        }
    }

    fn sample_provenance() -> MvProvenanceV1 {
        MvProvenanceV1 {
            provenance_version: 1,
            refresh_id: 7,
            mv_id: 1,
            token: "token-7".to_string(),
            technique: RefreshTechnique::Incremental,
            bases: vec![ProvenanceBase {
                table_fqn: "ice.sales.orders".to_string(),
                uuid: "uuid-orders".to_string(),
                from_snapshot: Some(100),
                to_snapshot: 200,
            }],
            definition_fingerprint: "fp-abc".to_string(),
            rows: 42,
        }
    }

    #[test]
    fn rebuild_maps_descriptor_and_provenance() {
        let discovered = sample_discovered();
        let provenance = sample_provenance();

        let rebuilt = rebuild_mv_definition_from_lake(&discovered, Some(&provenance))
            .expect("rebuild succeeds");

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
    fn rebuild_without_provenance_has_empty_watermark() {
        let discovered = sample_discovered();

        let rebuilt = rebuild_mv_definition_from_lake(&discovered, None).expect("rebuild succeeds");

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
