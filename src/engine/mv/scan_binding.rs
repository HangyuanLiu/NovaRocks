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

use std::collections::{BTreeMap, HashMap};

use crate::engine::mv::refresh_context::IcebergMvRefreshContext;
use crate::exec::node::iceberg_delta_scan::{
    DeltaScanDeleteSidePayload, IcebergDeltaDataColumnPayload,
};
use crate::sql::catalog::{IcebergTableInfo, ScanSource};
use crate::sql::codegen::scan::binding::{
    ResolvedIcebergDeltaScan, ResolvedIcebergFileScan, ResolvedScanExecution, ScanBindingResolver,
};
use crate::sql::codegen::scan::iceberg_delta::IcebergDeltaScanRuntimePlan;
use crate::sql::planner::payload::PlanScanNode;

impl ScanBindingResolver for IcebergMvRefreshContext {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        resolve_scan_source(
            node_id,
            &scan.table.source,
            |table, snapshot_id| self.version_scan_source(table, snapshot_id),
            |target_scan| self.target_state_scan_source(target_scan),
            |target_scan| self.target_locator_scan_source(target_scan),
            |table, from_snapshot_id, to_snapshot_id| {
                build_iceberg_delta_scan_runtime_plan(table, from_snapshot_id, to_snapshot_id, self)
            },
        )
    }
}

fn resolve_scan_source<V, S, L, D>(
    node_id: i32,
    source: &ScanSource,
    version: V,
    target_state: S,
    target_locator: L,
    delta: D,
) -> Result<Option<ResolvedScanExecution>, String>
where
    V: FnOnce(&IcebergTableInfo, i64) -> Result<ScanSource, String>,
    S: FnOnce(&crate::sql::catalog::IcebergMvTargetStateScan) -> Result<ScanSource, String>,
    L: FnOnce(&crate::sql::catalog::IcebergMvTargetLocatorScan) -> Result<ScanSource, String>,
    D: FnOnce(&IcebergTableInfo, i64, i64) -> Result<IcebergDeltaScanRuntimePlan, String>,
{
    let (kind, resolved) = match source {
        ScanSource::IcebergVersionTable { table, snapshot_id } => {
            let resolved = version(table, *snapshot_id).and_then(|source| {
                resolve_file_scan(node_id, "IcebergVersionTable", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergVersionTable", resolved)
        }
        ScanSource::IcebergMvTargetState(scan) => {
            let resolved = target_state(scan).and_then(|source| {
                resolve_file_scan(node_id, "IcebergMvTargetState", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergMvTargetState", resolved)
        }
        ScanSource::IcebergMvTargetLocator(scan) => {
            let resolved = target_locator(scan).and_then(|source| {
                resolve_file_scan(node_id, "IcebergMvTargetLocator", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergMvTargetLocator", resolved)
        }
        ScanSource::IcebergDeltaTable {
            table,
            from_snapshot_id,
            to_snapshot_id,
        } => {
            let resolved = delta(table, *from_snapshot_id, *to_snapshot_id).map(|runtime_plan| {
                Some(ResolvedScanExecution::IcebergDelta(
                    ResolvedIcebergDeltaScan { runtime_plan },
                ))
            });
            ("IcebergDeltaTable", resolved)
        }
        _ => return Ok(None),
    };
    resolved.map_err(|err| format!("resolve scan binding node_id={node_id} source={kind}: {err}"))
}

fn resolve_file_scan(
    node_id: i32,
    source_kind: &str,
    source: ScanSource,
) -> Result<ResolvedIcebergFileScan, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        cloud_properties,
        binding,
    } = source
    else {
        return Err(format!(
            "internal scan binding contract violation: node_id={node_id} source={source_kind} resolver must return IcebergDataFiles"
        ));
    };
    Ok(ResolvedIcebergFileScan {
        table,
        files,
        cloud_properties,
        binding,
    })
}

pub(crate) fn build_iceberg_delta_scan_runtime_plan(
    table: &IcebergTableInfo,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    refresh_ctx: &IcebergMvRefreshContext,
) -> Result<IcebergDeltaScanRuntimePlan, String> {
    let catalog_key = crate::engine::catalog::normalize_identifier(&table.catalog)?;
    let entry = refresh_ctx
        .base_catalog_entries
        .get(&catalog_key)
        .ok_or_else(|| {
            format!(
                "Iceberg delta scan requires base catalog {} in MV refresh context",
                table.catalog
            )
        })?;
    let ident = iceberg::TableIdent::from_strs([table.namespace.as_str(), table.table.as_str()])
        .map_err(|e| {
            format!(
                "build iceberg table ident for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)
        .map_err(|e| {
            format!(
                "build iceberg catalog for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        catalog.load_table(&ident).await
    })
    .map_err(|e| format!("load iceberg table for delta scan runtime failed: {e}"))?
    .map_err(|e| {
        format!(
            "load iceberg table for delta scan {}.{}.{}: {e}",
            table.catalog, table.namespace, table.table
        )
    })?;

    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded,
        from_snapshot_id,
        Some(to_snapshot_id),
        &[],
    )
    .map_err(|e| {
        format!(
            "ivm-a1 scan binding delta-scan: plan_changes failed for {}.{}.{} from_snapshot={} to_snapshot={}: {e}",
            table.catalog, table.namespace, table.table, from_snapshot_id, to_snapshot_id
        )
    })?;
    let equality_targets_by_delete_file =
        crate::connector::iceberg::changes::equality_delete_targets_at(
            &loaded,
            batch.current_snapshot_id,
            &batch.equality_deletes,
        )
        .map_err(|e| {
            format!(
                "ivm-a1 scan binding delta-scan: plan equality-delete targets failed for {}.{}.{} at snapshot {}: {e}",
                table.catalog, table.namespace, table.table, batch.current_snapshot_id
            )
        })?;
    let change_files =
        crate::connector::iceberg::changes::delta_source_files_from_change_batch_with_equality_targets(
            &batch,
            &equality_targets_by_delete_file,
        )?;
    let has_delete = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_delete {
        let object_store_factory = crate::connector::iceberg::changes::build_factory_for_table(
            &loaded,
            entry.object_store_config(),
        )?;
        let object_store_factory = std::sync::Arc::new(object_store_factory);
        let expected_object_store_bucket =
            crate::connector::iceberg::changes::expected_object_store_bucket_for_table(&loaded)?;
        let base_data_file_lineage =
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                &loaded,
                batch.current_snapshot_id,
            )?;
        let previous_data_file_lineage = if !batch.deleted_data_files.is_empty() {
            crate::connector::iceberg::changes::previous_snapshot_data_file_lineage_index(
                &loaded,
                batch.previous_snapshot_id,
            )?
        } else {
            HashMap::new()
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let touched_referenced_data_files: std::collections::HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|delete| delete.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions_per_file = if !touched_referenced_data_files.is_empty() {
            crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
                &loaded,
                batch.previous_snapshot_id,
                object_store_factory.as_ref(),
                &|path: &str| {
                    crate::connector::iceberg::changes::normalize_delete_projection_path(
                        path,
                        entry.object_store_config(),
                        expected_object_store_bucket.as_deref(),
                    )
                },
                |data_file_path: &str| touched_referenced_data_files.contains(data_file_path),
            )
            .map_err(|e| {
                format!(
                    "ivm-a1 scan binding delta-scan: preload previous deleted positions failed for {}.{}.{} at snapshot {}: {e}",
                    table.catalog, table.namespace, table.table, batch.previous_snapshot_id
                )
            })?
            .into_iter()
            .map(|(path, bitmap)| (path, bitmap.iter().collect::<Vec<_>>()))
            .collect()
        } else {
            HashMap::new()
        };
        let previous_delete_visibility_data_files =
            crate::connector::iceberg::changes::delete_visibility_data_files_at(
                &loaded,
                batch.previous_snapshot_id,
            )?;
        Some(DeltaScanDeleteSidePayload {
            base_data_file_lineage,
            previous_data_file_lineage,
            previous_delete_visibility_data_files,
            previously_deleted_positions_per_file,
            deleted_data_file_paths,
        })
    } else {
        None
    };
    let current_schema = loaded.metadata().current_schema();
    let data_columns = current_schema
        .as_ref()
        .as_struct()
        .fields()
        .iter()
        .map(|field| IcebergDeltaDataColumnPayload {
            name: field.name.clone(),
            field_id: field.id,
        })
        .collect();
    Ok(IcebergDeltaScanRuntimePlan {
        table_location: loaded.metadata().location().to_string(),
        data_columns,
        cloud_properties: entry.cloud_properties_map(),
        change_files,
        delete_side,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::sql::catalog::{
        IcebergDataFileBinding, IcebergDataFileInfo, IcebergMvTargetLocatorScan,
        IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter,
        IcebergMvTargetStateScan, IcebergSchemaDef,
    };

    fn table_info(catalog: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(99),
            schema_id: 1,
            location: format!("s3://bucket/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn resolved_files(table: IcebergTableInfo) -> ScanSource {
        ScanSource::IcebergDataFiles {
            table,
            files: vec![IcebergDataFileInfo {
                path: "s3://bucket/data.parquet".to_string(),
                size: 10,
                row_count: Some(2),
                column_stats: None,
                partition_spec_id: None,
                partition_key: None,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                delete_files: Vec::new(),
                manifest_path: None,
                partition_values: Vec::new(),
            }],
            cloud_properties: BTreeMap::from([("endpoint".to_string(), "minio".to_string())]),
            binding: IcebergDataFileBinding::ExplicitFiles,
        }
    }

    fn panic_version(_: &IcebergTableInfo, _: i64) -> Result<ScanSource, String> {
        panic!("unexpected version resolver call")
    }

    fn panic_state(_: &IcebergMvTargetStateScan) -> Result<ScanSource, String> {
        panic!("unexpected target-state resolver call")
    }

    fn panic_locator(_: &IcebergMvTargetLocatorScan) -> Result<ScanSource, String> {
        panic!("unexpected target-locator resolver call")
    }

    fn panic_delta(
        _: &IcebergTableInfo,
        _: i64,
        _: i64,
    ) -> Result<IcebergDeltaScanRuntimePlan, String> {
        panic!("unexpected delta resolver call")
    }

    #[test]
    fn iceberg_mv_refresh_context_implements_scan_binding_resolver() {
        fn assert_impl<T: ScanBindingResolver>() {}
        assert_impl::<IcebergMvRefreshContext>();
    }

    #[test]
    fn version_dispatch_preserves_explicit_snapshot_and_narrows_files() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let resolved = resolve_scan_source(
            7,
            &source,
            |table, snapshot_id| {
                assert_eq!(table.catalog, "ice");
                assert_eq!(snapshot_id, 42);
                Ok(resolved_files(table.clone()))
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect("version binding")
        .expect("binding required");
        let ResolvedScanExecution::IcebergFiles(files) = resolved else {
            panic!("expected file binding");
        };
        assert_eq!(files.table.current_snapshot_id, Some(99));
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.binding, IcebergDataFileBinding::ExplicitFiles);
    }

    #[test]
    fn target_state_dispatch_preserves_projection_contract() {
        let scan = IcebergMvTargetStateScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-mv".to_string(),
            target_snapshot_id: Some(77),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["sum_state".to_string()],
            physical_column_names: vec!["k".to_string(), "sum_state".to_string()],
            row_id_column_name: "k".to_string(),
            row_filter: IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "k".to_string(),
                branch_scope: None,
            },
            partition_constraint: IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        };
        let source = ScanSource::IcebergMvTargetState(scan);
        let resolved = resolve_scan_source(
            8,
            &source,
            panic_version,
            |scan| {
                assert_eq!(scan.target_snapshot_id, Some(77));
                assert_eq!(scan.physical_column_names, ["k", "sum_state"]);
                Ok(resolved_files(table_info("tgt", "mv")))
            },
            panic_locator,
            panic_delta,
        )
        .expect("target-state binding");
        assert!(matches!(
            resolved,
            Some(ResolvedScanExecution::IcebergFiles(_))
        ));
    }

    #[test]
    fn target_locator_dispatch_preserves_apply_key_projection() {
        let source = ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-mv".to_string(),
            target_snapshot_id: Some(77),
            apply_key_column: "__apply_key".to_string(),
            branch_id_column: Some("__branch_id".to_string()),
        });
        let resolved = resolve_scan_source(
            9,
            &source,
            panic_version,
            panic_state,
            |scan| {
                assert_eq!(scan.apply_key_column, "__apply_key");
                assert_eq!(scan.branch_id_column.as_deref(), Some("__branch_id"));
                Ok(resolved_files(table_info("tgt", "mv")))
            },
            panic_delta,
        )
        .expect("target-locator binding");
        assert!(matches!(
            resolved,
            Some(ResolvedScanExecution::IcebergFiles(_))
        ));
    }

    #[test]
    fn delta_dispatch_returns_fully_materialized_neutral_payload() {
        let source = ScanSource::IcebergDeltaTable {
            table: table_info("ice", "base"),
            from_snapshot_id: 10,
            to_snapshot_id: 20,
        };
        let resolved = resolve_scan_source(
            10,
            &source,
            panic_version,
            panic_state,
            panic_locator,
            |table, from, to| {
                assert_eq!(table.table, "base");
                assert_eq!((from, to), (10, 20));
                Ok(IcebergDeltaScanRuntimePlan {
                    table_location: "s3://bucket/base".to_string(),
                    data_columns: vec![IcebergDeltaDataColumnPayload {
                        name: "k".to_string(),
                        field_id: 1,
                    }],
                    cloud_properties: BTreeMap::new(),
                    change_files: Vec::new(),
                    delete_side: None,
                })
            },
        )
        .expect("delta binding")
        .expect("binding required");
        let ResolvedScanExecution::IcebergDelta(delta) = resolved else {
            panic!("expected delta binding");
        };
        assert_eq!(delta.runtime_plan.table_location, "s3://bucket/base");
        assert_eq!(delta.runtime_plan.data_columns[0].field_id, 1);
    }

    #[test]
    fn ordinary_source_does_not_require_refresh_binding() {
        let source = resolved_files(table_info("ice", "ordinary"));
        let resolved = resolve_scan_source(
            11,
            &source,
            panic_version,
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect("ordinary source");
        assert!(resolved.is_none());
    }

    #[test]
    fn resolver_errors_retain_node_source_and_catalog_table_context() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("missing_catalog", "missing_table"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            12,
            &source,
            |table, snapshot_id| {
                Err(format!(
                    "load {}.{}.{} snapshot {}: table not found",
                    table.catalog, table.namespace, table.table, snapshot_id
                ))
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("missing context must fail");
        assert!(err.contains("node_id=12"), "{err}");
        assert!(err.contains("IcebergVersionTable"), "{err}");
        assert!(
            err.contains("missing_catalog.db.missing_table snapshot 42"),
            "{err}"
        );
    }

    #[test]
    fn missing_pinned_snapshot_never_falls_back_to_current_snapshot() {
        let calls = Cell::new(0);
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            13,
            &source,
            |_, snapshot_id| {
                calls.set(calls.get() + 1);
                assert_eq!(snapshot_id, 42);
                Err("snapshot 42 not found".to_string())
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("missing pinned snapshot must fail");
        assert_eq!(calls.get(), 1);
        assert!(err.contains("snapshot 42 not found"), "{err}");
    }

    #[test]
    fn file_resolver_rejects_non_file_variant_as_internal_contract_error() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            14,
            &source,
            |table, snapshot_id| {
                Ok(ScanSource::IcebergVersionTable {
                    table: table.clone(),
                    snapshot_id,
                })
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("semantic source must not escape adapter");
        assert!(
            err.contains("internal scan binding contract violation"),
            "{err}"
        );
        assert!(err.contains("node_id=14"), "{err}");
    }
}
