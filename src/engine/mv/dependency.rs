use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_ddl::ResolvedTableRef;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::{
    CreateMvDependencyRequest, MvDependencyObjectRef, MvDependencyObjectType,
    MvDependencyStorageEngine, StoredMvDefinition,
};

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<IcebergTableRef>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn iceberg_table_dependency_ref(base: &IcebergTableRef) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(base.catalog.clone()),
        database_or_namespace: base.namespace.clone(),
        name: base.table.clone(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn iceberg_mv_dependency_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_mv_dependency_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::ManagedLake,
    }
}

pub(crate) fn stored_definition_dependency_ref(
    definition: &StoredMvDefinition,
    managed_name: Option<(&str, &str)>,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        let catalog = definition
            .target_catalog
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target catalog".to_string())?;
        let namespace = definition
            .target_namespace
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target namespace".to_string())?;
        let table = definition
            .target_table
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target table".to_string())?;
        return Ok(iceberg_mv_dependency_ref(catalog, namespace, table));
    }
    let (database, table) = managed_name.ok_or_else(|| {
        "managed-lake MV definition requires database/table name for dependency ref".to_string()
    })?;
    Ok(managed_mv_dependency_ref(database, table))
}

pub(crate) fn resolve_create_mv_dependencies(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "materialized view dependency resolution requires metadata provider".to_string()
    })?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency metadata read transaction failed: {e}"))?;

    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let base = IcebergTableRef {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some()
                {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::ManagedLake { database, table } => {
                let managed = state
                    .managed_lake
                    .read()
                    .expect("standalone managed lake read lock");
                let runtime = managed.table(database, table).map_err(|err| {
                    format!("resolve managed-lake MV dependency {database}.{table} failed: {err}")
                })?;
                if runtime.table.kind
                    != crate::connector::starrocks::managed::model::ManagedTableKind::MaterializedView
                {
                    return Err(format!(
                        "materialized view base tables must be Iceberg tables or materialized views; found managed lake table `{database}.{table}`"
                    ));
                }
                return Err(format!(
                    "managed-lake MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                ));
            }
        }
    }
    if base_refs.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(ResolvedCreateMvDependencies {
        base_refs,
        dependencies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_ref_display_distinguishes_table_and_mv() {
        let table = iceberg_table_dependency_ref(&IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        });
        let mv = iceberg_mv_dependency_ref("ice", "sales", "orders_mv");

        assert_eq!(table.display_name(), "ice.sales.orders");
        assert_eq!(mv.display_name(), "mv:ice.sales.orders_mv");
    }
}
