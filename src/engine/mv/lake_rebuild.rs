//! Pure reconstruction of an MV's SQLite definition-create inputs from its
//! lake package (W1 descriptor + optional W3a provenance).
//!
//! This is the core of W4's "SQLite as a rebuildable cache" property: given
//! nothing but the descriptor already inlined on the storage table (and,
//! optionally, the provenance record carried by the storage table's current
//! snapshot), [`rebuild_mv_definition_from_lake`] reproduces exactly the
//! inputs `create_iceberg_mv` would have persisted into SQLite at CREATE
//! time, plus the refresh watermark a completed refresh would have recorded.
//! M3 calls this at startup for MVs discovered on the lake but missing from
//! SQLite. No catalog I/O happens here — every input is already in memory.

use std::collections::BTreeMap;

use crate::connector::iceberg::commit::mv_provenance::MvProvenanceV1;
use crate::connector::starrocks::table::model::StarRocksMvStorageEngine;
use crate::engine::mv::iceberg_discovery::DiscoveredIcebergMv;
use crate::meta::repository::mv::CreateMvDefinitionRequest;

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
/// `provenance` should be the [`MvProvenanceV1`] read from the storage
/// table's current snapshot summary, if any. `None` means the MV was created
/// but has never completed a refresh, matching a brand-new `StoredMvDefinition`
/// whose watermark maps are empty.
pub(crate) fn rebuild_mv_definition_from_lake(
    discovered: &DiscoveredIcebergMv,
    provenance: Option<&MvProvenanceV1>,
) -> Result<RebuiltMvDefinition, String> {
    let descriptor = &discovered.descriptor;
    let (storage_namespace, storage_table) =
        split_storage_table_pointer(&descriptor.storage_table)?;

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
        storage_engine: StarRocksMvStorageEngine::Iceberg.as_sql_str().to_string(),
        target_catalog: Some(discovered.catalog.clone()),
        target_namespace: Some(storage_namespace),
        target_table: Some(storage_table),
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

/// Split a descriptor `storage_table` pointer (`"namespace.table"`, W1's
/// single-level-namespace convention — see
/// `iceberg_discovery::parse_storage_table_pointer`) into its two parts.
fn split_storage_table_pointer(pointer: &str) -> Result<(String, String), String> {
    let mut parts = pointer.split('.');
    let namespace = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| format!("invalid MV descriptor storage_table pointer `{pointer}`"))?;
    let table = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| format!("invalid MV descriptor storage_table pointer `{pointer}`"))?;
    if parts.next().is_some() {
        return Err(format!(
            "invalid MV descriptor storage_table pointer `{pointer}`; W1 supports single-level namespaces"
        ));
    }
    Ok((namespace.to_string(), table.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::mv_provenance::{ProvenanceBase, RefreshTechnique};
    use crate::engine::mv::iceberg_discovery::IcebergMvDiscoverySource;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };
    use crate::meta::repository::mv_descriptor::{DescriptorDependency, MvDescriptorV1};

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
                table_fqn: "ice.analytics.__nr_mv_mv_orders".to_string(),
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
            public_view: "analytics.mv_orders".to_string(),
            storage_table: "analytics.__nr_mv_mv_orders".to_string(),
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
            storage_table: "__nr_mv_mv_orders".to_string(),
            descriptor,
            source: IcebergMvDiscoverySource::StorageTable,
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
            StarRocksMvStorageEngine::Iceberg.as_sql_str()
        );
        assert_eq!(request.target_catalog.as_deref(), Some("ice"));
        assert_eq!(request.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(request.target_table.as_deref(), Some("__nr_mv_mv_orders"));
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
