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

//! Application-owned admission and lookup for frozen Iceberg delta scans.
//!
//! The runtime plan is derived while the refresh already owns its loaded,
//! pinned table.  Preparation only looks it up by the SQL binding token and
//! snapshot window; it never reopens a catalog or connector generation.

use std::collections::{HashMap, HashSet};

use crate::connector::iceberg::catalog::IcebergCatalogEntry;
use crate::connector::iceberg::delta::{DeltaDataColumn, DeltaScanDeleteSide};
use crate::connector::iceberg::scan_model::IcebergTableInfo;
use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::query_execution::preparation::scan::{
    IcebergDeltaScanRuntimePlan, ResolvedIcebergDeltaScan, ResolvedScanExecution,
    ScanBindingResolver,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::{ScanSource, SqlScanKind};

/// Freeze every physical input for one delta snapshot window while admission
/// still holds the exact table object.  The returned value contains no catalog
/// client or connector control handle and is safe to retain in the query-local
/// binding store.
pub(crate) fn freeze_iceberg_delta_runtime_plan(
    table: &IcebergTableInfo,
    entry: &IcebergCatalogEntry,
    loaded: &novarocks_connector_iceberg::iceberg::table::Table,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> Result<IcebergDeltaScanRuntimePlan, String> {
    let metadata = loaded.metadata();
    if table.current_snapshot_id != Some(to_snapshot_id)
        || metadata.current_snapshot_id() != Some(to_snapshot_id)
    {
        return Err(format!(
            "Iceberg delta admission for {}.{}.{} did not retain frozen to_snapshot_id {to_snapshot_id}",
            table.catalog, table.namespace, table.table
        ));
    }
    let frozen_table_uuid = metadata.uuid().to_string();
    if table.table_uuid.as_deref() != Some(frozen_table_uuid.as_str())
        || table.location != metadata.location()
    {
        return Err(format!(
            "Iceberg delta admission for {}.{}.{} drifted from its frozen table identity",
            table.catalog, table.namespace, table.table
        ));
    }

    let batch = crate::connector::iceberg::changes::plan_changes(
        loaded,
        from_snapshot_id,
        Some(to_snapshot_id),
        &[],
    )
    .map_err(|error| {
        format!(
            "freeze Iceberg delta scan for {}.{}.{} from_snapshot={} to_snapshot={}: {error}",
            table.catalog, table.namespace, table.table, from_snapshot_id, to_snapshot_id
        )
    })?;
    let equality_targets = crate::connector::iceberg::changes::equality_delete_targets_at(
        loaded,
        batch.current_snapshot_id,
        &batch.equality_deletes,
    )
    .map_err(|error| {
        format!(
            "freeze Iceberg equality-delete targets for {}.{}.{} at snapshot {}: {error}",
            table.catalog, table.namespace, table.table, batch.current_snapshot_id
        )
    })?;
    let change_files = crate::connector::iceberg::changes::delta_source_files_from_change_batch_with_equality_targets(
        &batch,
        &equality_targets,
    )?;
    let has_delete = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_delete {
        let factory = crate::connector::iceberg::changes::build_factory_for_table(
            loaded,
            entry.object_store_config(),
        )?;
        let expected_bucket =
            crate::connector::iceberg::changes::expected_object_store_bucket_for_table(loaded)?;
        let base_data_file_lineage =
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                loaded,
                batch.current_snapshot_id,
            )?;
        let previous_data_file_lineage = if batch.deleted_data_files.is_empty() {
            HashMap::new()
        } else {
            crate::connector::iceberg::changes::previous_snapshot_data_file_lineage_index(
                loaded,
                batch.previous_snapshot_id,
            )?
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let touched_referenced_data_files: HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|delete| delete.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions_per_file = if touched_referenced_data_files.is_empty() {
            HashMap::new()
        } else {
            crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
                loaded,
                batch.previous_snapshot_id,
                &factory,
                &|path| {
                    crate::connector::iceberg::changes::normalize_delete_projection_path(
                        path,
                        entry.object_store_config(),
                        expected_bucket.as_deref(),
                    )
                },
                |data_file_path| touched_referenced_data_files.contains(data_file_path),
            )
            .map_err(|error| {
                format!(
                    "freeze prior Iceberg delete positions for {}.{}.{} at snapshot {}: {error}",
                    table.catalog, table.namespace, table.table, batch.previous_snapshot_id
                )
            })?
            .into_iter()
            .map(|(path, bitmap)| (path, bitmap.iter().collect()))
            .collect()
        };
        let previous_delete_visibility_data_files =
            crate::connector::iceberg::changes::delete_visibility_data_files_at(
                loaded,
                batch.previous_snapshot_id,
            )?;
        Some(DeltaScanDeleteSide {
            base_data_file_lineage,
            previous_data_file_lineage,
            previous_delete_visibility_data_files,
            previously_deleted_positions_per_file,
            deleted_data_file_paths,
        })
    } else {
        None
    };
    let data_columns = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| DeltaDataColumn {
            name: field.name.clone(),
            field_id: field.id,
        })
        .collect();
    Ok(IcebergDeltaScanRuntimePlan {
        table_location: metadata.location().to_string(),
        data_columns,
        change_files,
        delete_side,
    })
}

/// Exact query-local delta lookup.  It intentionally accepts neither a
/// refresh context nor a catalog/registry, so it cannot reacquire metadata or
/// a newer connector generation after compilation.
pub(crate) struct QueryTableBindingScanResolver<'a> {
    bindings: &'a QueryTableBindingStore,
}

impl<'a> QueryTableBindingScanResolver<'a> {
    pub(crate) fn new(bindings: &'a QueryTableBindingStore) -> Self {
        Self { bindings }
    }
}

impl ScanBindingResolver for QueryTableBindingScanResolver<'_> {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        let ScanSource::Sql(source) = &scan.table.source;
        let SqlScanKind::Delta {
            from_snapshot_id,
            to_snapshot_id,
        } = source.kind
        else {
            return Ok(None);
        };
        let binding = self.bindings.binding(source.binding)?;
        let runtime_plan = binding
            .delta_runtime_plans
            .get(&(from_snapshot_id, to_snapshot_id))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "SQL delta scan binding for '{}.{}.{}' has no admitted runtime plan from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                    source.table.catalog, source.table.namespace, source.table.table
                )
            })?;
        Ok(Some(ResolvedScanExecution::IcebergDelta(
            ResolvedIcebergDeltaScan { runtime_plan },
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{QueryTableBindingScanResolver, ScanBindingResolver};
    use crate::engine::query_planning::bindings::{
        QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };
    use crate::query_execution::preparation::scan::{
        IcebergDeltaScanRuntimePlan, ResolvedScanExecution,
    };
    use crate::sql::catalog::ResolvedAnalyzerTable;
    use crate::sql::planner::payload::PlanScanNode;
    use crate::sql::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    fn delta_plan() -> IcebergDeltaScanRuntimePlan {
        IcebergDeltaScanRuntimePlan {
            table_location: "file:///warehouse/sales/orders".to_string(),
            data_columns: vec![],
            change_files: vec![],
            delete_side: None,
        }
    }

    fn delta_scan(
        binding: crate::sql::binding::SqlTableBindingId,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    ) -> PlanScanNode {
        let source = ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            },
        ));
        PlanScanNode {
            database: "sales".to_string(),
            table: TableDef {
                name: "orders".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source,
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn binding_with_delta(binding: crate::sql::binding::SqlTableBindingId) -> QueryTableBinding {
        let source = ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id: 10,
                to_snapshot_id: 20,
            },
        ));
        QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(
                Some("ice"),
                "sales",
                TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source,
                },
            ),
            statistics_pin: None,
            planning_lease: None,
            scan_materialization: None,
            frozen_snapshot_files: BTreeMap::new(),
            delta_runtime_plans: BTreeMap::from([((10, 20), delta_plan())]),
        }
    }

    #[test]
    fn sqlx2_preparation_delta_resolves_only_its_admitted_window() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let binding = bindings
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("ice", "sales", "orders", 20),
                |binding| Ok(binding_with_delta(binding)),
            )
            .expect("admit binding");
        let resolver = QueryTableBindingScanResolver::new(&bindings);

        let resolved = resolver
            .resolve_scan(7, &delta_scan(binding, 10, 20))
            .expect("resolve admitted delta")
            .expect("delta scan execution");
        let ResolvedScanExecution::IcebergDelta(delta) = resolved else {
            panic!("expected Iceberg delta execution");
        };
        assert_eq!(
            delta.runtime_plan.table_location,
            "file:///warehouse/sales/orders"
        );

        let error = resolver
            .resolve_scan(7, &delta_scan(binding, 9, 20))
            .expect_err("unadmitted window must fail before submission");
        assert!(error.contains("no admitted runtime plan"), "error={error}");
    }

    #[test]
    fn sqlx2_preparation_delta_rejects_cross_request_token() {
        let first = QueryTableBindingStore::try_new().expect("first binding store");
        let second = QueryTableBindingStore::try_new().expect("second binding store");
        let binding = second
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("ice", "sales", "orders", 20),
                |binding| Ok(binding_with_delta(binding)),
            )
            .expect("admit second binding");

        let error = QueryTableBindingScanResolver::new(&first)
            .resolve_scan(8, &delta_scan(binding, 10, 20))
            .expect_err("cross-request token must fail before connector preparation");
        assert!(error.contains("different request"), "error={error}");
    }
}
